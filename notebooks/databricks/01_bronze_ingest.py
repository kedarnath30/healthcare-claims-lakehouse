# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: raw CMS CSV → Delta with schema enforcement
# MAGIC * Header contract check (fails on column drift before anything is written)
# MAGIC * Explicit `StructType`, no inference; malformed rows go to `_quarantine_<table>`
# MAGIC * Ingest metadata: `_source_file`, `_ingest_ts`, `bene_year`

# COMMAND ----------

dbutils.widgets.text("lakehouse_root", "/Volumes/main/claims_raw/lakehouse")
dbutils.widgets.text("raw_root", "/Volumes/main/claims_raw/raw")
dbutils.widgets.text("catalog", "main")

import os, sys
sys.path.insert(0, os.path.abspath("../.."))            # repo root (Databricks Git folder)
os.environ["LAKEHOUSE_ROOT"] = dbutils.widgets.get("lakehouse_root")
os.environ["LAKEHOUSE_RAW"] = dbutils.widgets.get("raw_root")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

import importlib
from src.lakehouse import config, bronze, dq, io
importlib.reload(config)                                  # pick up the widget paths
spark = io.get_spark()                                    # returns the notebook's active session

# COMMAND ----------

ingest_log = bronze.run(spark, list(config.SOURCES) + ["icd9_dx_reference"])
display(spark.createDataFrame(ingest_log))

# COMMAND ----------

# Register the Delta folders as Unity Catalog external tables so analysts can query them by name
for t in list(config.SOURCES) + ["icd9_dx_reference"]:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.claims_bronze.{t} USING DELTA LOCATION '{io.table_path('bronze', t)}'")

# COMMAND ----------

checks = dq.bronze_checks(spark)
display(spark.createDataFrame([{k: str(v) for k, v in c.items()} for c in checks]))
assert not [c for c in checks if c["status"] == "FAIL"], "bronze DQ gate failed"
