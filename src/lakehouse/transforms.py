"""Pure DataFrame / Column transforms shared by the silver and gold layers (and unit-tested in tests/)."""
from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------- ICD-9-CM diagnosis codes
ICD9_VALID = r"^([0-9]{3,5}|V[0-9]{2,4}|E[0-9]{3,4})$"

# ICD-9-CM chapters by 3-digit numeric category (standard chapter ranges)
ICD9_CHAPTERS = [
    (1, 139, "Infectious & parasitic"), (140, 239, "Neoplasms"),
    (240, 279, "Endocrine, nutritional & metabolic"), (280, 289, "Blood & blood-forming organs"),
    (290, 319, "Mental disorders"), (320, 389, "Nervous system & sense organs"),
    (390, 459, "Circulatory system"), (460, 519, "Respiratory system"), (520, 579, "Digestive system"),
    (580, 629, "Genitourinary system"), (630, 679, "Pregnancy & childbirth"),
    (680, 709, "Skin & subcutaneous tissue"), (710, 739, "Musculoskeletal & connective tissue"),
    (740, 759, "Congenital anomalies"), (760, 779, "Perinatal conditions"),
    (780, 799, "Symptoms & ill-defined conditions"), (800, 999, "Injury & poisoning"),
]


def normalize_icd9(col: Column) -> Column:
    """Upper-case, strip dots/whitespace, empty -> NULL. Codes are stored without the decimal point."""
    c = F.upper(F.regexp_replace(F.trim(col), r"[.\s]", ""))
    return F.when((c.isNull()) | (c == ""), F.lit(None)).otherwise(c)


def icd9_is_valid(code: Column) -> Column:
    return code.rlike(ICD9_VALID)


def icd9_display(code: Column) -> Column:
    """'4280' -> '428.0', 'V5841' -> 'V58.41', 'E9330' -> 'E933.0', '250' -> '250'. Invalid -> NULL."""
    e_code = code.startswith("E")
    head = F.when(e_code, code.substr(1, 4)).otherwise(code.substr(1, 3))
    tail = F.when(e_code, code.substr(5, 10)).otherwise(code.substr(4, 10))
    return F.when(~icd9_is_valid(code), F.lit(None)).when(tail == "", head).otherwise(
        F.concat(head, F.lit("."), tail))


def icd9_category(code: Column) -> Column:
    return F.when(~icd9_is_valid(code), F.lit(None)).when(code.startswith("E"), code.substr(1, 4)) \
        .otherwise(code.substr(1, 3))


def icd9_chapter(code: Column) -> Column:
    num = code.substr(1, 3).cast("int")
    expr = F.when(code.startswith("V"), F.lit("Supplementary (V codes)")) \
        .when(code.startswith("E"), F.lit("External causes (E codes)"))
    for lo, hi, name in ICD9_CHAPTERS:
        expr = expr.when(num.between(lo, hi), F.lit(name))
    return F.when(~icd9_is_valid(code), F.lit("Invalid / missing")).otherwise(expr.otherwise("Invalid / missing"))


# ---------------------------------------------------------------- dates, keys, money
DATE_8 = r"^(19|20)[0-9]{2}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])$"


def parse_yyyymmdd(col: Column) -> Column:
    """'20090725' -> date. Blank or malformed strings -> NULL instead of an exception."""
    s = F.trim(col)
    return F.to_date(F.when(s.rlike(DATE_8), s), "yyyyMMdd")


def member_key(col: Column) -> Column:
    """Stable surrogate key: first 16 hex chars of SHA-256 of the trimmed, upper-cased member id."""
    return F.substring(F.sha2(F.upper(F.trim(col)), 256), 1, 16)


def sum_columns(cols: list[str]) -> Column:
    """Null-safe row-wise sum of amount columns (carrier lines 1..13)."""
    return reduce(lambda a, b: a + b, [F.coalesce(F.col(c), F.lit(0)) for c in cols]).cast("decimal(14,2)")


# ---------------------------------------------------------------- dedupe / segment roll-up
def drop_exact_duplicates(df: DataFrame, ignore: list[str] | None = None) -> DataFrame:
    """Drop rows identical on every business column (ingest metadata such as _ingest_ts is ignored)."""
    ignore = set(ignore or []) | {c for c in df.columns if c.startswith("_")}
    return df.dropDuplicates([c for c in df.columns if c not in ignore])


def latest_per_key(df: DataFrame, keys: list[str], order_col: str) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def rollup_segments(df: DataFrame) -> DataFrame:
    """Institutional claims can arrive in two SEGMENTs under one CLM_ID. Collapse to one claim:
    sum payments/days, min from-date, max thru-date, keep segment-1 values for everything else."""
    sums = {"CLM_PMT_AMT", "NCH_PRMRY_PYR_CLM_PD_AMT", "CLM_UTLZTN_DAY_CNT"}
    w = Window.partitionBy("CLM_ID").orderBy("SEGMENT")
    first = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    agg_exprs = [F.count("*").alias("segment_count"), F.min("CLM_FROM_DT").alias("_from"),
                 F.max("CLM_THRU_DT").alias("_thru")]
    agg_exprs += [F.sum(c).alias(f"_sum_{c}") for c in sums if c in df.columns]
    agg = df.groupBy("CLM_ID").agg(*agg_exprs)
    out = first.join(agg, "CLM_ID")
    out = out.withColumn("CLM_FROM_DT", F.col("_from")).withColumn("CLM_THRU_DT", F.col("_thru"))
    for c in sums:
        if c in df.columns:
            out = out.withColumn(c, F.col(f"_sum_{c}"))
    return out.drop("_from", "_thru", *[f"_sum_{c}" for c in sums])


# ---------------------------------------------------------------- eligibility
def expand_member_months(member_year: DataFrame) -> DataFrame:
    """One row per member per covered month. The beneficiary file only carries a count of covered months per
    year, so months are assumed contiguous from January and capped at the month of death."""
    months = F.sequence(F.lit(1), F.greatest(F.col("member_months"), F.lit(0)))
    df = member_year.filter("member_months > 0").select(
        "member_id", "coverage_year", "death_dt", F.explode(months).alias("m"))
    df = df.withColumn("month_start", F.make_date("coverage_year", "m", F.lit(1)))
    df = df.filter(F.col("death_dt").isNull() | (F.col("month_start") <= F.trunc("death_dt", "month")))
    return df.select("member_id", "month_start")


# ---------------------------------------------------------------- readmissions
def flag_readmissions(stays: DataFrame, window_days: int = 30, data_end: str | None = None) -> DataFrame:
    """All-cause 30-day readmission flag on inpatient index stays.

    stays: member_id, claim_id, admit_dt, discharge_dt (one row per inpatient claim).
    A stay is a readmission trigger for the previous stay if it is admitted 1..window_days after that stay's
    discharge. Admissions on the discharge day are treated as transfers (same episode), not readmissions.
    Index stays discharged within window_days of data_end are flagged `has_full_followup = false`.
    """
    w = Window.partitionBy("member_id").orderBy("admit_dt", "discharge_dt", "claim_id")
    df = stays.withColumn("next_admit_dt", F.lead("admit_dt").over(w)) \
              .withColumn("next_claim_id", F.lead("claim_id").over(w))
    df = df.withColumn("days_to_next_admit", F.datediff("next_admit_dt", "discharge_dt"))
    df = df.withColumn(
        "readmit_30d",
        F.coalesce((F.col("days_to_next_admit") >= 1) & (F.col("days_to_next_admit") <= window_days),
                   F.lit(False)))
    if data_end:
        df = df.withColumn("has_full_followup",
                           F.datediff(F.to_date(F.lit(data_end)), F.col("discharge_dt")) >= window_days)
    else:
        df = df.withColumn("has_full_followup", F.lit(True))
    return df
