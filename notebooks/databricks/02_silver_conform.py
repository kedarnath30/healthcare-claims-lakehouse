# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: clean, de-duplicate, conform
# MAGIC * Members: one row per member-year, decoded sex/race, chronic-condition flags, SHA-256 `member_key`,
# MAGIC   and a member-month eligibility table (PMPM denominator)
# MAGIC * Claims: inpatient + outpatient + carrier → one `claim_header` schema; exact duplicates dropped,
# MAGIC   multi-segment institutional claims rolled up, carrier line payments summed
# MAGIC * Diagnoses: ICD-9 codes normalized (`428.0` / `4280` / ` 4280 ` → `4280`), validated, chaptered
# MAGIC * Rejects (no member id / unusable dates) are kept in `_rejects_claims` with a reason

# COMMAND ----------

dbutils.widgets.text("lakehouse_root", "/Volumes/main/claims_raw/lakehouse")
dbutils.widgets.text("catalog", "main")
import os, sys
sys.path.insert(0, os.path.abspath("../.."))
os.environ["LAKEHOUSE_ROOT"] = dbutils.widgets.get("lakehouse_root")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

import importlib
from src.lakehouse import config, silver, dq, io
importlib.reload(config)
spark = io.get_spark()

# COMMAND ----------

display(spark.createDataFrame(silver.run(spark)))

# COMMAND ----------

for t in ["member_year", "member", "member_month", "claim_header", "claim_diagnosis", "icd9_dx", "_rejects_claims"]:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.claims_silver.{t.lstrip('_')} USING DELTA LOCATION '{io.table_path('silver', t)}'")
# Z-order the claim fact on the columns the gold marts filter / join on (Databricks Delta)
spark.sql(f"OPTIMIZE {catalog}.claims_silver.claim_header ZORDER BY (member_id, service_month)")

# COMMAND ----------

checks = dq.silver_checks(spark)
display(spark.createDataFrame([{k: str(v) for k, v in c.items()} for c in checks]))
assert not [c for c in checks if c["status"] == "FAIL"], "silver DQ gate failed"
