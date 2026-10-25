# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold marts
# MAGIC PMPM (overall and by chronic-condition risk segment), 30-day all-cause readmissions, top diagnoses by
# MAGIC cost, provider utilization. Built with PySpark here; `sql/gold_layer.sql` is the same logic in Spark SQL
# MAGIC (run in the last cell for a parity check).

# COMMAND ----------

dbutils.widgets.text("lakehouse_root", "/Volumes/main/claims_raw/lakehouse")
dbutils.widgets.text("catalog", "main")
import os, sys
sys.path.insert(0, os.path.abspath("../.."))
os.environ["LAKEHOUSE_ROOT"] = dbutils.widgets.get("lakehouse_root")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

import importlib
from src.lakehouse import config, gold, dq, io
importlib.reload(config)
spark = io.get_spark()

# COMMAND ----------

display(spark.createDataFrame(gold.run(spark, export_csv=False)))

# COMMAND ----------

for t in ["pmpm_monthly", "pmpm_by_risk_segment", "readmission_index_stays", "readmission_summary",
          "top_diagnoses_by_cost", "provider_utilization"]:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.claims_gold.{t} USING DELTA LOCATION '{io.table_path('gold', t)}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT service_month, member_months, paid_total, pmpm_total
# MAGIC FROM main.claims_gold.pmpm_monthly ORDER BY service_month

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cost_rank, icd9_display, description, claims, members, paid_total, share_of_total_paid
# MAGIC FROM main.claims_gold.top_diagnoses_by_cost ORDER BY cost_rank LIMIT 15

# COMMAND ----------

checks = dq.gold_checks(spark)
display(spark.createDataFrame([{k: str(v) for k, v in c.items()} for c in checks]))
assert not [c for c in checks if c["status"] == "FAIL"], "gold DQ gate failed"
