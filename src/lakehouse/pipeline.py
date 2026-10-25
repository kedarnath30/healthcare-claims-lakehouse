"""CLI entry point: run one layer (plus its data-quality gate) or the whole medallion pipeline.

    python -m src.lakehouse.pipeline --layer bronze          # raw CSV -> bronze Delta + bronze DQ
    python -m src.lakehouse.pipeline --layer silver          # bronze -> silver + silver DQ
    python -m src.lakehouse.pipeline --layer gold            # silver -> gold marts + gold DQ + CSV exports
    python -m src.lakehouse.pipeline --layer all --strict    # everything; exit 1 if any error-level check fails

Airflow (dags/claims_lakehouse_dag.py) calls exactly these commands, one task per layer.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path

from . import bronze, config, dq, gold, silver
from .io import get_spark, table_format


def _raw_data_lines(table: str) -> int:
    n = 0
    for f in config.SOURCES[table]:
        with open(Path(config.RAW) / f, "rb") as fh:
            n += sum(1 for _ in fh) - 1  # minus header
    return n


def _merge_reports() -> dict:
    rows = []
    for layer in ["bronze", "silver", "gold"]:
        p = config.RESULTS / f"dq_{layer}.csv"
        if p.exists():
            rows += list(csv.DictReader(open(p)))
    return dq.write_report(rows, "dq_report")


def run_layer(spark, layer: str, tables: list[str] | None) -> dict:
    t0 = time.time()
    if layer == "bronze":
        info = bronze.run(spark, tables or list(config.SOURCES) + ["icd9_dx_reference"])
        checks = dq.bronze_checks(spark, {t: _raw_data_lines(t) for t in config.SOURCES})
    elif layer == "silver":
        info = silver.run(spark, tables)
        checks = dq.silver_checks(spark)
    elif layer == "gold":
        info = gold.run(spark)
        checks = dq.gold_checks(spark)
    else:
        raise ValueError(layer)
    summary = dq.write_report(checks, f"dq_{layer}")
    rec = {"layer": layer, "format": table_format(), "seconds": round(time.time() - t0, 1),
           "tables": info, "dq": summary}
    config.RESULTS.mkdir(parents=True, exist_ok=True)
    (config.RESULTS / f"run_{layer}.json").write_text(json.dumps(rec, indent=2, default=str))
    print(json.dumps(rec, indent=2, default=str))
    return rec


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", choices=["bronze", "silver", "gold", "all"], required=True)
    ap.add_argument("--tables", nargs="*", help="bronze: source tables; silver: members/claims/reference")
    ap.add_argument("--format", choices=["delta", "parquet"], default=None)
    ap.add_argument("--strict", action="store_true", help="exit 1 when an error-level DQ check fails")
    args = ap.parse_args(argv)
    spark = get_spark(fmt=args.format)
    layers = ["bronze", "silver", "gold"] if args.layer == "all" else [args.layer]
    failed = 0
    for layer in layers:
        rec = run_layer(spark, layer, args.tables if args.layer != "all" else None)
        failed += rec["dq"]["fail"]
        if failed and args.strict:
            print(f"DQ gate failed after {layer}: {failed} error-level checks", file=sys.stderr)
            break
    print("combined DQ:", _merge_reports())
    return 1 if (failed and args.strict) else 0


if __name__ == "__main__":
    sys.exit(main())
