# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold in Spark SQL + parity check
# MAGIC Runs `sql/gold_layer.sql` against the silver tables and compares every mart with the PySpark build.

# COMMAND ----------

dbutils.widgets.text("lakehouse_root", "/Volumes/main/claims_raw/lakehouse")
import os, sys
sys.path.insert(0, os.path.abspath("../.."))
os.environ["LAKEHOUSE_ROOT"] = dbutils.widgets.get("lakehouse_root")

# COMMAND ----------

import importlib
from src.lakehouse import config
importlib.reload(config)
from src import run_gold_sql
importlib.reload(run_gold_sql)
exit_code = run_gold_sql.main()
display(spark.read.option("header", True).csv(f"file:{config.RESULTS / 'sql_parity.csv'}"))
assert exit_code == 0, "Spark SQL gold layer disagrees with the PySpark gold layer"
