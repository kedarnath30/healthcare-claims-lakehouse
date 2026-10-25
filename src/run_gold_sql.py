"""Run sql/gold_layer.sql in Spark SQL on the silver tables and check each mart against the PySpark gold output.

    python -m src.run_gold_sql      -> results/sql_parity.csv
"""
from __future__ import annotations

import csv
import re
import sys

from pyspark.sql import functions as F

from src.lakehouse import config
from src.lakehouse.io import get_spark, read_table, register_views

CHECKS = {  # mart -> (key columns, numeric columns compared)
    "pmpm_monthly": (["service_month"], ["member_months", "paid_total", "pmpm_total"]),
    "pmpm_by_risk_segment": (["coverage_year", "risk_segment"], ["members", "member_months", "paid_total", "pmpm"]),
    "readmission_index_stays": (["claim_id"], ["days_to_next_admit"]),
    "readmission_summary": (["dimension", "value"], ["index_stays", "readmissions_30d", "readmit_rate_30d"]),
    "top_diagnoses_by_cost": (["cost_rank"], ["claims", "members", "paid_total"]),
    "provider_utilization": (["claim_type", "provider_id"], ["claims", "members", "paid_total", "readmit_rate_30d"]),
}


def split_statements(sql: str) -> list[str]:
    no_comments = re.sub(r"--[^\n]*", "", sql)
    return [s.strip() for s in no_comments.split(";") if s.strip()]


def main() -> int:
    spark = get_spark("gold-sql")
    register_views(spark, {"silver": ["claim_header", "member_month", "member_year", "icd9_dx"]})
    statements = split_statements((config.ROOT / "sql" / "gold_layer.sql").read_text())
    for s in statements:
        spark.sql(s)
    rows = []
    for mart, (keys, nums) in CHECKS.items():
        sql_df = spark.table(f"gold_{mart}")
        py_df = read_table(spark, "gold", mart)
        s = sql_df.select(*[F.col(k).alias(f"sk_{k}") for k in keys], *[F.col(c).alias(f"s_{c}") for c in nums])
        p = py_df.select(*[F.col(k).alias(f"pk_{k}") for k in keys], *[F.col(c).alias(f"p_{c}") for c in nums])
        # null-safe key match: e.g. the readmission row for stays with no principal diagnosis has value = NULL
        cond = [F.col(f"sk_{k}").eqNullSafe(F.col(f"pk_{k}")) for k in keys]
        j = s.join(p, cond, "full_outer")
        # a row mismatches when one side is NULL and the other is not, or the values differ by > 0.005
        mismatch = j.filter(" OR ".join(
            f"(((s_{c} IS NULL) <> (p_{c} IS NULL)) OR abs(s_{c} - p_{c}) > 0.005)" for c in nums)).count()
        rows.append({"mart": mart, "rows_sql": sql_df.count(), "rows_pyspark": py_df.count(),
                     "mismatched_rows": mismatch, "status": "PASS" if mismatch == 0 else "FAIL"})
        print(rows[-1])
    with open(config.RESULTS / "sql_parity.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    return 0 if all(r["status"] == "PASS" for r in rows) else 1


if __name__ == "__main__":
    sys.exit(main())
