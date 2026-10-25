"""Data-quality checks between layers. Every check returns a row for results/dq_report.csv.

Severity: `error` checks gate the pipeline (pipeline.py --strict exits non-zero); `warn` checks are reported.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config
from .io import read_table, table_format, table_path


def _row(layer, table, check, metric, value, threshold, passed, severity="error", detail=""):
    return {"layer": layer, "table": table, "check": check, "metric": metric,
            "value": value if not isinstance(value, float) else round(value, 6),
            "threshold": threshold, "status": "PASS" if passed else ("FAIL" if severity == "error" else "WARN"),
            "severity": severity, "detail": detail}


# ---------------------------------------------------------------- generic checks
def row_count(df: DataFrame, layer: str, table: str, min_rows: int = 1) -> dict:
    n = df.count()
    return _row(layer, table, "row_count", "rows", n, f">= {min_rows}", n >= min_rows)


def null_rate(df: DataFrame, layer: str, table: str, cols: list[str], max_rate: float = 0.0,
              severity: str = "error") -> list[dict]:
    total = df.count()
    aggs = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first().asDict()
    out = []
    for c in cols:
        rate = (aggs[c] or 0) / total if total else 0.0
        out.append(_row(layer, table, "null_rate", c, rate, f"<= {max_rate}", rate <= max_rate, severity))
    return out


def duplicates(df: DataFrame, layer: str, table: str, keys: list[str], severity: str = "error",
               expected_zero: bool = True) -> dict:
    n_dup = df.groupBy(*keys).count().filter("count > 1").count()
    passed = (n_dup == 0) if expected_zero else True
    return _row(layer, table, "duplicate_keys", "+".join(keys), n_dup, "= 0" if expected_zero else "info",
                passed, severity)


def referential_integrity(child: DataFrame, parent: DataFrame, layer: str, table: str, key: str,
                          parent_name: str, max_orphan_rate: float = 0.0, severity: str = "error") -> dict:
    total = child.count()
    orphans = child.select(key).join(parent.select(key).distinct(), key, "left_anti").count()
    rate = orphans / total if total else 0.0
    return _row(layer, table, "referential_integrity", f"{key} -> {parent_name}", rate,
                f"orphan rate <= {max_orphan_rate}", rate <= max_orphan_rate, severity,
                f"{orphans:,} orphan rows of {total:,}")


def reconciliation(n_source: int, n_target: int, n_explained: int, layer: str, table: str,
                   detail: str) -> dict:
    """n_source - n_target must be fully explained by documented drops (dedupe, segment roll-up, rejects)."""
    gap = n_source - n_target - n_explained
    return _row(layer, table, "row_reconciliation", "unexplained_row_gap", gap, "= 0", gap == 0, "error",
                f"source={n_source:,} target={n_target:,} explained_drops={n_explained:,}; {detail}")


# ---------------------------------------------------------------- layer suites
def bronze_checks(spark: SparkSession, raw_line_counts: dict[str, int] | None = None) -> list[dict]:
    out = []
    keys = {"beneficiary": ["DESYNPUF_ID", "bene_year"], "inpatient": ["CLM_ID", "SEGMENT"],
            "outpatient": ["CLM_ID", "SEGMENT"], "carrier": ["CLM_ID"]}
    for t, k in keys.items():
        if not os.path.exists(table_path("bronze", t)):
            out.append(_row("bronze", t, "row_count", "rows", "table missing", ">= 1", False))
            continue
        df = read_table(spark, "bronze", t)
        q = read_table(spark, "bronze", f"_quarantine_{t}").count()
        n = df.count()
        out.append(_row("bronze", t, "row_count", "rows", n, ">= 1", n >= 1))
        out.append(_row("bronze", t, "quarantined_rows", "rows", q, "= 0", q == 0, "warn"))
        if raw_line_counts and t in raw_line_counts:
            out.append(reconciliation(raw_line_counts[t], n, q, "bronze", t, "raw CSV data lines vs loaded+quarantined"))
        out += null_rate(df, "bronze", t, k[:1], 0.0)
        out.append(duplicates(df, "bronze", t, k))
    out.append(schema_enforcement_probe(spark))
    return out


def schema_enforcement_probe(spark: SparkSession) -> dict:
    """Try to append a frame with an unexpected column to a bronze Delta table; Delta must reject it."""
    if table_format() != "delta":
        return _row("bronze", "inpatient", "schema_enforcement", "delta_rejects_drift", "skipped (parquet)",
                    "rejected", True, "warn")
    df = read_table(spark, "bronze", "inpatient").limit(1).withColumn("UNEXPECTED_COL", F.lit("x"))
    try:
        df.write.format("delta").mode("append").save(table_path("bronze", "inpatient"))
        rejected = False
        msg = "append with extra column was ACCEPTED"
    except Exception as e:  # AnalysisException: A schema mismatch detected when writing to the Delta table
        rejected = True
        msg = str(e).split("\n")[0][:160]
    return _row("bronze", "inpatient", "schema_enforcement", "delta_rejects_drift", rejected, "True", rejected,
                "error", msg)


def silver_checks(spark: SparkSession) -> list[dict]:
    out = []
    my, mem, mm = (read_table(spark, "silver", t) for t in ["member_year", "member", "member_month"])
    ch, dx, rej = (read_table(spark, "silver", t) for t in ["claim_header", "claim_diagnosis", "_rejects_claims"])
    b_bene = read_table(spark, "bronze", "beneficiary")

    # members
    out.append(reconciliation(b_bene.count(), my.count(),
                              b_bene.count() - b_bene.dropDuplicates(["DESYNPUF_ID", "bene_year"]).count(),
                              "silver", "member_year", "dropped = duplicate member-year rows"))
    out.append(duplicates(my, "silver", "member_year", ["member_id", "coverage_year"]))
    out.append(duplicates(mem, "silver", "member", ["member_id"]))
    out.append(duplicates(mm, "silver", "member_month", ["member_id", "month_start"]))
    out += null_rate(my, "silver", "member_year", ["member_id", "member_key", "birth_dt", "sex"], 0.0)

    # claims: bronze rows -> silver claims must reconcile exactly
    for ctype, src in [("IP", "inpatient"), ("OP", "outpatient"), ("CAR", "carrier")]:
        b = read_table(spark, "bronze", src)
        biz = [c for c in b.columns if not c.startswith("_")]
        n_b = b.count()
        n_exact_dup = n_b - b.dropDuplicates(biz).count()
        n_claims = b.select("CLM_ID").distinct().count()
        n_collapsed = (n_b - n_exact_dup) - n_claims  # extra segments (or carrier re-sends) folded into one claim
        n_rej = rej.filter(F.col("claim_type") == ctype).count()
        n_s = ch.filter(F.col("claim_type") == ctype).count()
        out.append(reconciliation(n_b, n_s, n_exact_dup + n_collapsed + n_rej, "silver", f"claim_header[{ctype}]",
                                  f"exact_dups={n_exact_dup:,}, segments_collapsed={n_collapsed:,}, rejects={n_rej:,}"))
        # paid amount must be conserved through dedupe + roll-up (ignoring rejected claims)
        if ctype != "CAR":
            paid_b = b.dropDuplicates(biz).join(rej.filter(F.col("claim_type") == ctype)
                                                .select(F.col("claim_id").alias("CLM_ID")), "CLM_ID", "left_anti") \
                      .agg(F.sum("CLM_PMT_AMT")).first()[0] or 0
            paid_s = ch.filter(F.col("claim_type") == ctype).agg(F.sum("paid_amt")).first()[0] or 0
            out.append(_row("silver", f"claim_header[{ctype}]", "amount_reconciliation", "paid_bronze - paid_silver",
                            float(paid_b - paid_s), "= 0", float(paid_b - paid_s) == 0.0, "error",
                            f"bronze={float(paid_b):,.2f} silver={float(paid_s):,.2f}"))
    out.append(duplicates(ch, "silver", "claim_header", ["claim_id", "claim_type"]))
    out += null_rate(ch, "silver", "claim_header", ["claim_id", "member_id", "claim_from_dt", "paid_amt"], 0.0)
    out += null_rate(ch, "silver", "claim_header", ["provider_id", "principal_dx"], 0.05, "warn")
    out.append(referential_integrity(ch, my.select("member_id"), "silver", "claim_header", "member_id",
                                     "member_year.member_id", 0.0))
    out.append(referential_integrity(dx.select("claim_id"), ch.select("claim_id"), "silver", "claim_diagnosis",
                                     "claim_id", "claim_header.claim_id", 0.0))
    n_neg = ch.filter(F.col("paid_amt") < 0).count()
    out.append(_row("silver", "claim_header", "value_range", "paid_amt < 0 rows", n_neg, "= 0", n_neg == 0, "warn"))
    bad_dt = ch.filter((F.col("claim_from_dt") < "2007-01-01") | (F.col("claim_from_dt") > config.DATA_END_DATE)).count()
    out.append(_row("silver", "claim_header", "value_range", "from_dt outside 2007-01-01..2010-12-31", bad_dt,
                    "= 0", bad_dt == 0, "warn"))
    total_dx = dx.count()
    invalid = dx.filter(~F.col("is_valid_code")).count()
    out.append(_row("silver", "claim_diagnosis", "icd9_validity", "invalid_code_rate", invalid / total_dx,
                    "<= 0.01", invalid / total_dx <= 0.01, "warn", f"{invalid:,} of {total_dx:,}"))
    ref = read_table(spark, "silver", "icd9_dx").select("icd9_code")
    unmatched = dx.filter("is_valid_code").select("icd9_code").join(ref, "icd9_code", "left_anti").count()
    out.append(_row("silver", "claim_diagnosis", "referential_integrity", "icd9_code -> CMS ICD-9 v32 reference",
                    unmatched / total_dx, "<= 0.10", unmatched / total_dx <= 0.10, "warn",
                    f"{unmatched:,} of {total_dx:,} codes not in the FY2015 billable list (3-digit/retired codes)"))
    # eligibility coverage: claims in months the member had no Part A/B coverage
    outside = ch.join(mm.withColumnRenamed("month_start", "service_month"), ["member_id", "service_month"],
                      "left_anti").count()
    out.append(_row("silver", "claim_header", "eligibility_coverage", "claims outside covered member-months",
                    outside / ch.count(), "<= 0.05", outside / ch.count() <= 0.05, "warn",
                    f"{outside:,} claims (synthetic data perturbs coverage months)"))
    return out


def gold_checks(spark: SparkSession) -> list[dict]:
    out = []
    ch = read_table(spark, "silver", "claim_header")
    pm = read_table(spark, "gold", "pmpm_monthly")
    silver_paid = float(ch.filter(F.col("service_month").isin([r[0] for r in pm.select("service_month").collect()]))
                        .agg(F.sum("paid_amt")).first()[0])
    gold_paid = float(pm.agg(F.sum("paid_total")).first()[0])
    out.append(_row("gold", "pmpm_monthly", "amount_reconciliation", "silver_paid - gold_paid",
                    round(silver_paid - gold_paid, 2), "= 0", abs(silver_paid - gold_paid) < 0.01, "error",
                    f"silver={silver_paid:,.2f} gold={gold_paid:,.2f}"))
    out.append(duplicates(pm, "gold", "pmpm_monthly", ["service_month"]))
    out += null_rate(pm, "gold", "pmpm_monthly", ["member_months", "pmpm_total"], 0.0)
    idx = read_table(spark, "gold", "readmission_index_stays")
    n_ip = ch.filter("claim_type = 'IP'").count()
    out.append(_row("gold", "readmission_index_stays", "row_reconciliation", "ip_claims - index_rows",
                    n_ip - idx.count(), "= 0", n_ip == idx.count()))
    rate = idx.filter("has_full_followup").agg(F.avg(F.col("readmit_30d").cast("int"))).first()[0]
    out.append(_row("gold", "readmission_index_stays", "plausibility", "readmit_rate_30d", float(rate),
                    "between 0.02 and 0.40", 0.02 <= rate <= 0.40, "warn"))
    pu = read_table(spark, "gold", "provider_utilization")
    out.append(duplicates(pu, "gold", "provider_utilization", ["claim_type", "provider_id"]))
    td = read_table(spark, "gold", "top_diagnoses_by_cost")
    out += null_rate(td, "gold", "top_diagnoses_by_cost", ["description"], 0.2, "warn")
    return out


def write_report(rows: list[dict], name: str = "dq_report") -> dict:
    import csv
    config.RESULTS.mkdir(parents=True, exist_ok=True)
    cols = ["layer", "table", "check", "metric", "value", "threshold", "status", "severity", "detail"]
    with open(config.RESULTS / f"{name}.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "checks": len(rows),
        "pass": sum(r["status"] == "PASS" for r in rows),
        "warn": sum(r["status"] == "WARN" for r in rows),
        "fail": sum(r["status"] == "FAIL" for r in rows),
    }
    with open(config.RESULTS / f"{name}.json", "w") as fh:
        json.dump({"summary": summary, "checks": rows}, fh, indent=2, default=str)
    return summary
