"""Gold layer: business-facing marts built from silver.

  pmpm_monthly             paid amount, eligible member months and PMPM by service month and claim type
  pmpm_by_risk_segment     PMPM by coverage year x chronic-condition-count bucket
  readmission_index_stays  every inpatient stay with its 30-day all-cause readmission flag
  readmission_summary      readmission rate by discharge year and by principal-diagnosis chapter
  top_diagnoses_by_cost    principal ICD-9 diagnoses ranked by total paid, with CMS descriptions
  provider_utilization     claims, members, paid, LOS and readmission rate per provider and claim type
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import config
from . import transforms as T
from .io import read_table, write_table


def pmpm_monthly(claims: DataFrame, member_month: DataFrame) -> DataFrame:
    mm = member_month.groupBy(F.col("month_start").alias("service_month")).agg(
        F.countDistinct("member_id").alias("member_months"))
    paid = claims.groupBy("service_month").pivot("claim_type", ["IP", "OP", "CAR"]).agg(
        F.sum("paid_amt")).na.fill(0)
    paid = paid.withColumnRenamed("IP", "paid_ip").withColumnRenamed("OP", "paid_op") \
               .withColumnRenamed("CAR", "paid_carrier")
    df = mm.join(paid, "service_month", "left").na.fill(0, ["paid_ip", "paid_op", "paid_carrier"])
    df = df.withColumn("paid_total", F.col("paid_ip") + F.col("paid_op") + F.col("paid_carrier"))
    for c in ["ip", "op", "carrier", "total"]:
        df = df.withColumn(f"pmpm_{c}", F.round(F.col(f"paid_{c}") / F.col("member_months"), 2))
    return df.orderBy("service_month")


def pmpm_by_risk_segment(claims: DataFrame, member_year: DataFrame) -> DataFrame:
    seg = member_year.withColumn(
        "risk_segment",
        F.when(F.col("chronic_condition_count") == 0, "0 chronic conditions")
         .when(F.col("chronic_condition_count") <= 2, "1-2 chronic conditions")
         .when(F.col("chronic_condition_count") <= 4, "3-4 chronic conditions")
         .otherwise("5+ chronic conditions"))
    paid = claims.groupBy("member_id", F.col("service_year").alias("coverage_year")).agg(
        F.sum("paid_amt").alias("paid"))
    df = seg.filter("member_months > 0").join(paid, ["member_id", "coverage_year"], "left") \
            .na.fill(0, ["paid"])
    out = df.groupBy("coverage_year", "risk_segment").agg(
        F.countDistinct("member_id").alias("members"),
        F.sum("member_months").alias("member_months"),
        F.sum("paid").alias("paid_total"))
    return out.withColumn("pmpm", F.round(F.col("paid_total") / F.col("member_months"), 2)) \
              .orderBy("coverage_year", "risk_segment")


def readmission_index_stays(claims: DataFrame) -> DataFrame:
    stays = claims.filter("claim_type = 'IP'").select(
        "member_id", "claim_id", "admit_dt", "discharge_dt", "provider_id", "principal_dx",
        "principal_dx_chapter", "drg_cd", "paid_amt", "length_of_stay")
    return T.flag_readmissions(stays, 30, config.DATA_END_DATE) \
        .withColumn("discharge_year", F.year("discharge_dt"))


def readmission_summary(index_stays: DataFrame) -> DataFrame:
    eligible = index_stays.filter("has_full_followup")
    def agg(df, dim_name, dim_col):
        return df.groupBy(F.lit(dim_name).alias("dimension"), F.col(dim_col).cast("string").alias("value")).agg(
            F.count("*").alias("index_stays"),
            F.sum(F.col("readmit_30d").cast("int")).alias("readmissions_30d"))
    overall = eligible.groupBy(F.lit("overall").alias("dimension"), F.lit("all").alias("value")).agg(
        F.count("*").alias("index_stays"), F.sum(F.col("readmit_30d").cast("int")).alias("readmissions_30d"))
    df = overall.unionByName(agg(eligible, "discharge_year", "discharge_year")) \
                .unionByName(agg(eligible, "principal_dx_chapter", "principal_dx_chapter"))
    return df.withColumn("readmit_rate_30d", F.round(F.col("readmissions_30d") / F.col("index_stays"), 4)) \
             .orderBy("dimension", F.desc("index_stays"))


def top_diagnoses_by_cost(claims: DataFrame, icd9: DataFrame, top_n: int = 50) -> DataFrame:
    total = claims.agg(F.sum("paid_amt")).first()[0]
    by_dx = claims.filter(F.col("principal_dx").isNotNull()).groupBy("principal_dx").agg(
        F.count("*").alias("claims"),
        F.countDistinct("member_id").alias("members"),
        F.sum("paid_amt").alias("paid_total"),
        F.sum(F.when(F.col("claim_type") == "IP", F.col("paid_amt")).otherwise(0)).alias("paid_ip"),
        F.sum(F.when(F.col("claim_type") == "OP", F.col("paid_amt")).otherwise(0)).alias("paid_op"),
        F.sum(F.when(F.col("claim_type") == "CAR", F.col("paid_amt")).otherwise(0)).alias("paid_carrier"))
    w = Window.orderBy(F.desc("paid_total"), "principal_dx")
    df = by_dx.withColumn("cost_rank", F.row_number().over(w)) \
              .withColumn("share_of_total_paid", F.round(F.col("paid_total") / F.lit(total), 4)) \
              .withColumn("paid_per_claim", F.round(F.col("paid_total") / F.col("claims"), 2))
    ref = icd9.select(F.col("icd9_code").alias("principal_dx"), "icd9_display", "description", "icd9_chapter")
    return df.filter(F.col("cost_rank") <= top_n).join(F.broadcast(ref), "principal_dx", "left") \
             .orderBy("cost_rank")


def provider_utilization(claims: DataFrame, index_stays: DataFrame) -> DataFrame:
    base = claims.filter(F.col("provider_id").isNotNull() & (F.col("provider_id") != ""))
    util = base.groupBy("claim_type", "provider_id").agg(
        F.count("*").alias("claims"),
        F.countDistinct("member_id").alias("members"),
        F.sum("paid_amt").alias("paid_total"),
        F.round(F.avg("length_of_stay"), 2).alias("avg_length_of_stay"))
    readm = index_stays.filter("has_full_followup").groupBy("provider_id").agg(
        F.count("*").alias("index_stays"),
        F.round(F.avg(F.col("readmit_30d").cast("int")), 4).alias("readmit_rate_30d")) \
        .withColumn("claim_type", F.lit("IP"))
    df = util.join(readm, ["claim_type", "provider_id"], "left")
    df = df.withColumn("paid_per_claim", F.round(F.col("paid_total") / F.col("claims"), 2)) \
           .withColumn("paid_per_member", F.round(F.col("paid_total") / F.col("members"), 2))
    w = Window.partitionBy("claim_type").orderBy(F.desc("paid_total"), "provider_id")
    return df.withColumn("paid_rank_in_type", F.row_number().over(w))


EXPORTS = {  # small gold tables also exported as CSV into results/ for the README
    "pmpm_monthly": None, "pmpm_by_risk_segment": None, "readmission_summary": None,
    "top_diagnoses_by_cost": 25, "provider_utilization": None,
}


def run(spark: SparkSession, export_csv: bool = True) -> list[dict]:
    claims = read_table(spark, "silver", "claim_header").cache()
    member_month = read_table(spark, "silver", "member_month")
    member_year = read_table(spark, "silver", "member_year")
    icd9 = read_table(spark, "silver", "icd9_dx")

    write_table(pmpm_monthly(claims, member_month), "gold", "pmpm_monthly")
    write_table(pmpm_by_risk_segment(claims, member_year), "gold", "pmpm_by_risk_segment")
    write_table(readmission_index_stays(claims), "gold", "readmission_index_stays")
    idx = read_table(spark, "gold", "readmission_index_stays")
    write_table(readmission_summary(idx), "gold", "readmission_summary")
    write_table(top_diagnoses_by_cost(claims, icd9), "gold", "top_diagnoses_by_cost")
    write_table(provider_utilization(claims, idx), "gold", "provider_utilization")
    claims.unpersist()

    names = ["pmpm_monthly", "pmpm_by_risk_segment", "readmission_index_stays", "readmission_summary",
             "top_diagnoses_by_cost", "provider_utilization"]
    out = [{"table": n, "rows": read_table(spark, "gold", n).count()} for n in names]
    if export_csv:
        config.RESULTS.mkdir(parents=True, exist_ok=True)
        for n, limit in EXPORTS.items():
            df = read_table(spark, "gold", n)
            if n == "provider_utilization":
                df = df.filter("paid_rank_in_type <= 10").orderBy("claim_type", "paid_rank_in_type")
            elif limit:
                df = df.orderBy("cost_rank").limit(limit)
            df.toPandas().to_csv(config.RESULTS / f"gold_{n}.csv", index=False)
    return out
