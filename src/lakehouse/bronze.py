"""Bronze layer: raw CMS CSVs -> typed Delta tables, with schema enforcement.

Enforcement happens in three places:
1. Header contract: the CSV header must match the expected column list exactly (name and order), otherwise the
   load fails with a SchemaDriftError before any data is written.
2. Explicit StructType: no schema inference. Values that don't fit the declared type (e.g. text in an amount
   column, wrong column count) land in `_corrupt_record`; those rows are quarantined to bronze/_quarantine_<table>
   instead of silently becoming NULLs in the main table.
3. Delta's own write-time schema enforcement rejects appends whose schema differs from the table
   (exercised by a check in dq.py).
"""
from __future__ import annotations

import re
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config
from .io import write_table


class SchemaDriftError(ValueError):
    pass


def read_header(path: Path) -> list[str]:
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        first = fh.readline().strip()
    return [c.strip().strip('"') for c in first.split(",")]


def check_header(table: str, path: Path) -> None:
    expected, actual = config.COLUMNS[table], read_header(path)
    if actual != expected:
        missing = [c for c in expected if c not in actual]
        extra = [c for c in actual if c not in expected]
        raise SchemaDriftError(
            f"{path.name}: header does not match the {table} contract "
            f"(missing={missing[:5]}, unexpected={extra[:5]}, n_expected={len(expected)}, n_actual={len(actual)})"
        )


def read_raw(spark: SparkSession, table: str, paths: list[Path]) -> DataFrame:
    for p in paths:
        check_header(table, p)
    df = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("enforceSchema", True)
        .schema(config.bronze_schema(table))
        .csv([str(p) for p in paths])
    )
    df = df.withColumn("_source_file", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1)) \
           .withColumn("_ingest_ts", F.current_timestamp())
    if table == "beneficiary":
        df = df.withColumn("bene_year", F.regexp_extract("_source_file", r"DE1_0_(\d{4})_", 1).cast("int"))
    return df


def ingest(spark: SparkSession, table: str, raw_dir: Path | None = None) -> dict:
    raw_dir = Path(raw_dir or config.RAW)
    paths = [raw_dir / f for f in config.SOURCES[table]]
    missing = [str(p) for p in paths if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Run scripts/download_data.py first; missing {missing}")
    # Spark requires the parsed frame to be persisted before filtering on _corrupt_record; DISK_ONLY keeps the
    # 1.2 GB carrier file from blowing the driver heap on a small machine.
    df = read_raw(spark, table, paths).persist(StorageLevel.DISK_ONLY)
    bad = df.filter(F.col("_corrupt_record").isNotNull())
    good = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
    n_bad = bad.count()
    write_table(good, "bronze", table)
    write_table(bad.select("_source_file", "_ingest_ts", "_corrupt_record"), "bronze", f"_quarantine_{table}")
    n_good = good.count()
    df.unpersist()
    return {"table": table, "files": [p.name for p in paths], "rows_loaded": n_good, "rows_quarantined": n_bad}


def ingest_icd9_reference(spark: SparkSession, raw_dir: Path | None = None) -> dict:
    """CMS ICD-9-CM v32 (FY2015) long descriptions: '<code> <description>' per line, latin-1."""
    raw_dir = Path(raw_dir or config.RAW)
    path = raw_dir / "CMS32_DESC_LONG_DX.txt"
    rows = []
    with open(path, "r", encoding="latin-1") as fh:
        for line in fh:
            m = re.match(r"^(\S+)\s+(.*)$", line.rstrip("\r\n"))
            if m:
                rows.append((m.group(1).strip(), m.group(2).strip()))
    df = spark.createDataFrame(rows, "icd9_code string, description string")
    write_table(df, "bronze", "icd9_dx_reference")
    return {"table": "icd9_dx_reference", "files": [path.name], "rows_loaded": len(rows), "rows_quarantined": 0}


def run(spark: SparkSession, tables: list[str] | None = None) -> list[dict]:
    tables = tables or list(config.SOURCES)
    out = []
    for t in tables:
        out.append(ingest_icd9_reference(spark) if t == "icd9_dx_reference" else ingest(spark, t))
    return out
