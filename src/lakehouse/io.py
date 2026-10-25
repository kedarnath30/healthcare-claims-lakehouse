"""SparkSession factory and table read/write helpers.

Tables are written as Delta Lake (delta-spark) when it is available, else plain Parquet. Each table lives at
<LAKE>/<layer>/<name>; on Databricks the same functions work against a Unity Catalog Volume path.
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession

from . import config

_FORMAT = os.environ.get("LAKEHOUSE_FORMAT", "delta")


def get_spark(app: str = "claims-lakehouse", fmt: str | None = None) -> SparkSession:
    global _FORMAT
    if fmt:
        _FORMAT = fmt
    active = SparkSession.getActiveSession()
    if active is not None:  # Databricks / notebooks already have one
        return active
    builder = (
        SparkSession.builder.appName(app)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.local.dir", os.environ.get("SPARK_LOCAL_DIRS", "/tmp"))
    )
    if _FORMAT == "delta":
        try:
            from delta import configure_spark_with_delta_pip

            builder = builder.config(
                "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
            ).config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            builder = configure_spark_with_delta_pip(builder)
        except ImportError:
            _FORMAT = "parquet"
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def table_format() -> str:
    return _FORMAT


def table_path(layer: str, name: str) -> str:
    return str(config.LAKE / layer / name)


def write_table(df: DataFrame, layer: str, name: str, partition_by: list[str] | None = None,
                mode: str = "overwrite") -> str:
    path = table_path(layer, name)
    w = df.write.format(_FORMAT).mode(mode)
    if _FORMAT == "delta":
        w = w.option("overwriteSchema", "true")
    if partition_by:
        w = w.partitionBy(*partition_by)
    w.save(path)
    return path


def read_table(spark: SparkSession, layer: str, name: str) -> DataFrame:
    return spark.read.format(_FORMAT).load(table_path(layer, name))


def register_views(spark: SparkSession, tables: dict[str, list[str]]) -> None:
    """Expose lakehouse tables as temp views named <layer>_<table> for Spark SQL."""
    for layer, names in tables.items():
        for n in names:
            read_table(spark, layer, n).createOrReplaceTempView(f"{layer}_{n}")
