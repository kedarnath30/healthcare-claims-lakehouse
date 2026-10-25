# Databricks notebook source
# MAGIC %md
# MAGIC # Healthcare Claims Lakehouse: setup
# MAGIC Run once per workspace. Creates the Unity Catalog schemas and a Volume for the raw CMS DE-SynPUF files,
# MAGIC then downloads Sample 1 into the Volume. Clone this repo into Databricks Repos (Git folders) first so the
# MAGIC `src/lakehouse` package is importable from these notebooks.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("volume_root", "/Volumes/main/claims_raw/raw")
catalog = dbutils.widgets.get("catalog")
volume_root = dbutils.widgets.get("volume_root")

# COMMAND ----------

for schema in ["claims_raw", "claims_bronze", "claims_silver", "claims_gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.claims_raw.raw")

# COMMAND ----------

# Download the public CMS files straight into the Volume (same URLs as scripts/download_data.py)
import os, subprocess, sys
repo_root = os.path.abspath("../..")
os.environ["LAKEHOUSE_RAW"] = volume_root
subprocess.run([sys.executable, f"{repo_root}/scripts/download_data.py"], check=True)
display(dbutils.fs.ls(volume_root.replace("/Volumes", "dbfs:/Volumes")))
