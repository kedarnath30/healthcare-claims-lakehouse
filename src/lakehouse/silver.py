"""Silver layer: cleaned, de-duplicated, conformed tables.

Tables
  member_year      one row per member per coverage year (2008-2010), decoded demographics and chronic flags
  member           one row per member (latest coverage year wins), surrogate member_key
  member_month     one row per member per covered month (the PMPM denominator)
  claim_header     IP + OP + carrier claims conformed to one schema, one row per claim
  claim_diagnosis  long table: one row per claim per diagnosis position, ICD-9 normalized
  icd9_dx          ICD-9-CM reference with display code and chapter
  _rejects_claims  claims dropped from claim_header and the reason (missing member / unusable dates)
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config
from . import transforms as T
from .io import read_table, write_table

SEX = {1: "Male", 2: "Female"}
RACE = {1: "White", 2: "Black", 3: "Other", 5: "Hispanic"}


def _decode(col: str, mapping: dict) -> F.Column:
    expr = None
    for k, v in mapping.items():
        expr = (F.when(F.col(col) == k, v) if expr is None else expr.when(F.col(col) == k, v))
    return expr.otherwise("Unknown")


def build_member_year(bene: DataFrame) -> DataFrame:
    df = T.drop_exact_duplicates(bene)
    df = df.withColumn("member_id", F.upper(F.trim("DESYNPUF_ID")))
    df = T.latest_per_key(df, ["member_id", "bene_year"], "_ingest_ts")
    cc = [F.when(F.col(src) == 1, True).when(F.col(src) == 2, False).alias(dst)
          for src, dst in config.CHRONIC_FLAGS.items()]
    out = df.select(
        "member_id",
        T.member_key(F.col("member_id")).alias("member_key"),
        F.col("bene_year").alias("coverage_year"),
        T.parse_yyyymmdd(F.col("BENE_BIRTH_DT")).alias("birth_dt"),
        T.parse_yyyymmdd(F.col("BENE_DEATH_DT")).alias("death_dt"),
        _decode("BENE_SEX_IDENT_CD", SEX).alias("sex"),
        _decode("BENE_RACE_CD", RACE).alias("race"),
        F.col("SP_STATE_CODE").alias("state_code"),
        F.col("BENE_COUNTY_CD").alias("county_code"),
        (F.col("BENE_ESRD_IND") == "Y").alias("esrd"),
        F.col("BENE_HI_CVRAGE_TOT_MONS").alias("part_a_months"),
        F.col("BENE_SMI_CVRAGE_TOT_MONS").alias("part_b_months"),
        F.col("BENE_HMO_CVRAGE_TOT_MONS").alias("hmo_months"),
        F.col("PLAN_CVRG_MOS_NUM").alias("part_d_months"),
        # Member months for medical PMPM: months with Part A or Part B fee-for-service coverage
        F.greatest(F.coalesce("BENE_HI_CVRAGE_TOT_MONS", F.lit(0)),
                   F.coalesce("BENE_SMI_CVRAGE_TOT_MONS", F.lit(0))).alias("member_months"),
        *cc,
    )
    out = out.withColumn("age_at_year_end",
                         F.floor(F.months_between(F.make_date("coverage_year", F.lit(12), F.lit(31)),
                                                  "birth_dt") / 12).cast("int"))
    out = out.withColumn("chronic_condition_count",
                         sum(F.col(c).cast("int") for c in config.CHRONIC_FLAGS.values()))
    return out


def build_member(member_year: DataFrame) -> DataFrame:
    latest = T.latest_per_key(member_year, ["member_id"], "coverage_year")
    return latest.select("member_key", "member_id", "birth_dt", "death_dt", "sex", "race", "state_code",
                         "county_code", "esrd", F.col("coverage_year").alias("last_coverage_year"))


def _institutional(df: DataFrame, claim_type: str) -> tuple[DataFrame, DataFrame]:
    df = T.drop_exact_duplicates(df)
    df = T.rollup_segments(df)
    is_ip = claim_type == "IP"
    from_dt, thru_dt = T.parse_yyyymmdd(F.col("CLM_FROM_DT")), T.parse_yyyymmdd(F.col("CLM_THRU_DT"))
    hdr = df.select(
        F.col("CLM_ID").alias("claim_id"),
        F.upper(F.trim("DESYNPUF_ID")).alias("member_id"),
        F.lit(claim_type).alias("claim_type"),
        from_dt.alias("claim_from_dt"),
        thru_dt.alias("claim_thru_dt"),
        (F.coalesce(T.parse_yyyymmdd(F.col("CLM_ADMSN_DT")), from_dt) if is_ip else F.lit(None).cast("date"))
        .alias("admit_dt"),
        (F.coalesce(T.parse_yyyymmdd(F.col("NCH_BENE_DSCHRG_DT")), thru_dt) if is_ip else F.lit(None).cast("date"))
        .alias("discharge_dt"),
        F.trim("PRVDR_NUM").alias("provider_id"),
        F.trim("AT_PHYSN_NPI").alias("attending_npi"),
        F.col("CLM_PMT_AMT").cast("decimal(14,2)").alias("paid_amt"),
        F.col("NCH_PRMRY_PYR_CLM_PD_AMT").cast("decimal(14,2)").alias("primary_payer_paid_amt"),
        (F.trim("CLM_DRG_CD") if is_ip else F.lit(None).cast("string")).alias("drg_cd"),
        (F.col("CLM_UTLZTN_DAY_CNT") if is_ip else F.lit(None).cast("int")).alias("utilization_days"),
        F.col("segment_count"),
        F.col("_source_file"),
    )
    dx_cols = [f"ICD9_DGNS_CD_{i}" for i in range(1, 11)]
    return hdr, df.select("CLM_ID", "DESYNPUF_ID", *dx_cols)


def _carrier(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = T.drop_exact_duplicates(df)
    df = T.latest_per_key(df, ["CLM_ID"], "_ingest_ts")
    pay = [f"LINE_NCH_PMT_AMT_{i}" for i in range(1, 14)]
    lines = sum(F.when(F.col(f"HCPCS_CD_{i}").isNotNull() | (F.coalesce(F.col(p), F.lit(0)) != 0), 1)
                .otherwise(0) for i, p in enumerate(pay, start=1))
    hdr = df.select(
        F.col("CLM_ID").alias("claim_id"),
        F.upper(F.trim("DESYNPUF_ID")).alias("member_id"),
        F.lit("CAR").alias("claim_type"),
        T.parse_yyyymmdd(F.col("CLM_FROM_DT")).alias("claim_from_dt"),
        T.parse_yyyymmdd(F.col("CLM_THRU_DT")).alias("claim_thru_dt"),
        F.lit(None).cast("date").alias("admit_dt"),
        F.lit(None).cast("date").alias("discharge_dt"),
        F.trim("PRF_PHYSN_NPI_1").alias("provider_id"),
        F.trim("PRF_PHYSN_NPI_1").alias("attending_npi"),
        T.sum_columns(pay).alias("paid_amt"),
        T.sum_columns([f"LINE_BENE_PRMRY_PYR_PD_AMT_{i}" for i in range(1, 14)]).alias("primary_payer_paid_amt"),
        F.lit(None).cast("string").alias("drg_cd"),
        F.lit(None).cast("int").alias("utilization_days"),
        lines.cast("int").alias("segment_count"),  # carrier: number of populated service lines
        F.col("_source_file"),
    )
    dx_cols = [f"ICD9_DGNS_CD_{i}" for i in range(1, 9)]
    return hdr, df.select("CLM_ID", "DESYNPUF_ID", *dx_cols)


def explode_diagnoses(dx_wide: DataFrame, claim_type: str) -> DataFrame:
    dx_cols = [c for c in dx_wide.columns if c.startswith("ICD9_DGNS_CD_")]
    arr = F.array(*[F.col(c) for c in dx_cols])
    df = dx_wide.select(F.col("CLM_ID").alias("claim_id"), F.upper(F.trim("DESYNPUF_ID")).alias("member_id"),
                        F.posexplode(arr).alias("pos", "raw_code"))
    df = df.withColumn("icd9_code", T.normalize_icd9(F.col("raw_code"))).filter(F.col("icd9_code").isNotNull())
    return df.select(
        "claim_id", F.lit(claim_type).alias("claim_type"), "member_id",
        (F.col("pos") + 1).alias("dx_position"), "icd9_code",
        T.icd9_display(F.col("icd9_code")).alias("icd9_display"),
        T.icd9_category(F.col("icd9_code")).alias("icd9_category"),
        T.icd9_chapter(F.col("icd9_code")).alias("icd9_chapter"),
        T.icd9_is_valid(F.col("icd9_code")).alias("is_valid_code"),
    )


def build_claims(ip: DataFrame, op: DataFrame, car: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    parts = [_institutional(ip, "IP"), _institutional(op, "OP"), _carrier(car)]
    headers = parts[0][0].unionByName(parts[1][0]).unionByName(parts[2][0])
    dx = explode_diagnoses(parts[0][1], "IP").unionByName(explode_diagnoses(parts[1][1], "OP")) \
        .unionByName(explode_diagnoses(parts[2][1], "CAR"))
    principal = dx.filter("dx_position = 1").select("claim_id", "claim_type",
                                                   F.col("icd9_code").alias("principal_dx"),
                                                   F.col("icd9_chapter").alias("principal_dx_chapter"))
    dx_counts = dx.groupBy("claim_id", "claim_type").agg(F.count("*").alias("dx_count"))
    headers = headers.join(principal, ["claim_id", "claim_type"], "left") \
                     .join(dx_counts, ["claim_id", "claim_type"], "left") \
                     .withColumn("dx_count", F.coalesce("dx_count", F.lit(0)))
    reason = (F.when(F.col("member_id").isNull() | (F.col("member_id") == ""), "missing_member_id")
              .when(F.col("claim_from_dt").isNull(), "missing_or_invalid_from_date")
              .when(F.col("claim_thru_dt").isNull(), "missing_or_invalid_thru_date")
              .when(F.col("claim_thru_dt") < F.col("claim_from_dt"), "thru_before_from"))
    headers = headers.withColumn("_reject_reason", reason)
    rejects = headers.filter(F.col("_reject_reason").isNotNull())
    clean = headers.filter(F.col("_reject_reason").isNull()).drop("_reject_reason")
    clean = clean.withColumn("service_month", F.trunc("claim_from_dt", "month")) \
                 .withColumn("service_year", F.year("claim_from_dt"))
    clean = clean.withColumn(
        "length_of_stay",
        F.when(F.col("claim_type") == "IP",
               F.greatest(F.datediff("discharge_dt", "admit_dt"), F.lit(0))))
    dx = dx.join(clean.select("claim_id", "claim_type"), ["claim_id", "claim_type"], "left_semi")
    return clean, dx, rejects


def build_icd9_reference(ref: DataFrame) -> DataFrame:
    code = T.normalize_icd9(F.col("icd9_code"))
    return ref.select(code.alias("icd9_code"), "description") \
        .withColumn("icd9_display", T.icd9_display(F.col("icd9_code"))) \
        .withColumn("icd9_chapter", T.icd9_chapter(F.col("icd9_code"))) \
        .dropDuplicates(["icd9_code"])


def run(spark: SparkSession, steps: list[str] | None = None) -> list[dict]:
    steps = steps or ["members", "claims", "reference"]
    out = []
    if "members" in steps:
        my = build_member_year(read_table(spark, "bronze", "beneficiary"))
        write_table(my, "silver", "member_year")
        my = read_table(spark, "silver", "member_year")
        write_table(build_member(my), "silver", "member")
        write_table(T.expand_member_months(my), "silver", "member_month")
        out += [{"table": t, "rows": read_table(spark, "silver", t).count()}
                for t in ["member_year", "member", "member_month"]]
    if "claims" in steps:
        clean, dx, rejects = build_claims(read_table(spark, "bronze", "inpatient"),
                                          read_table(spark, "bronze", "outpatient"),
                                          read_table(spark, "bronze", "carrier"))
        write_table(dx, "silver", "claim_diagnosis", partition_by=["claim_type"])
        write_table(clean, "silver", "claim_header", partition_by=["claim_type"])
        write_table(rejects, "silver", "_rejects_claims")
        out += [{"table": t, "rows": read_table(spark, "silver", t).count()}
                for t in ["claim_header", "claim_diagnosis", "_rejects_claims"]]
    if "reference" in steps:
        write_table(build_icd9_reference(read_table(spark, "bronze", "icd9_dx_reference")), "silver", "icd9_dx")
        out.append({"table": "icd9_dx", "rows": read_table(spark, "silver", "icd9_dx").count()})
    return out
