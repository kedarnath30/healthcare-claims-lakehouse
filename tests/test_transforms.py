"""Unit tests for the silver/gold transforms on small hand-built frames (no CMS data needed)."""
import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from src.lakehouse import config, silver
from src.lakehouse import transforms as T
from src.lakehouse.bronze import SchemaDriftError, check_header


# ------------------------------------------------------------------ ICD-9
@pytest.mark.parametrize("raw, code, display, category, chapter, valid", [
    ("4280", "4280", "428.0", "428", "Circulatory system", True),
    (" 428.0 ", "4280", "428.0", "428", "Circulatory system", True),
    ("v5841", "V5841", "V58.41", "V58", "Supplementary (V codes)", True),
    ("E9330", "E9330", "E933.0", "E933", "External causes (E codes)", True),
    ("250", "250", "250", "250", "Endocrine, nutritional & metabolic", True),
    ("OTHER", "OTHER", None, None, "Invalid / missing", False),
])
def test_icd9_normalization(spark, raw, code, display, category, chapter, valid):
    df = spark.createDataFrame([(raw,)], "raw string").withColumn("c", T.normalize_icd9(F.col("raw")))
    r = df.select("c", T.icd9_display(F.col("c")).alias("d"), T.icd9_category(F.col("c")).alias("cat"),
                  T.icd9_chapter(F.col("c")).alias("ch"), T.icd9_is_valid(F.col("c")).alias("v")).first()
    assert (r.c, r.d, r.cat, r.ch, r.v) == (code, display, category, chapter, valid)


def test_icd9_blank_is_null(spark):
    df = spark.createDataFrame([("",), ("  ",), (None,)], "raw string")
    assert df.select(T.normalize_icd9(F.col("raw")).alias("c")).filter("c is not null").count() == 0


# ------------------------------------------------------------------ dates / keys / sums
def test_parse_yyyymmdd(spark):
    df = spark.createDataFrame([("20090725",), ("20100229",), ("2009-07-25",), ("",), (None,)], "s string")
    got = [r.d for r in df.select(T.parse_yyyymmdd(F.col("s")).alias("d")).collect()]
    assert got == [dt.date(2009, 7, 25), None, None, None, None]  # 2010 was not a leap year


def test_member_key_stable_and_case_insensitive(spark):
    df = spark.createDataFrame([("00013D2EFD8E45D1",), (" 00013d2efd8e45d1 ",)], "id string")
    keys = [r.k for r in df.select(T.member_key(F.col("id")).alias("k")).collect()]
    assert keys[0] == keys[1] and len(keys[0]) == 16


def test_sum_columns_null_safe(spark):
    df = spark.createDataFrame([(Decimal("50.00"), None, Decimal("10.50"))], "a decimal(12,2), b decimal(12,2), c decimal(12,2)")
    assert df.select(T.sum_columns(["a", "b", "c"]).alias("s")).first().s == Decimal("60.50")


# ------------------------------------------------------------------ dedupe / segments
def test_drop_exact_duplicates_ignores_ingest_metadata(spark):
    df = spark.createDataFrame([("c1", 10, "f1"), ("c1", 10, "f2"), ("c1", 11, "f1")],
                               "CLM_ID string, amt int, _source_file string")
    assert T.drop_exact_duplicates(df).count() == 2


def test_rollup_segments_sums_payments_and_spans_dates(spark):
    df = spark.createDataFrame(
        [("c1", 1, "20090101", "20090105", Decimal("1000.00"), Decimal("0.00"), 4, "P1"),
         ("c1", 2, "20090106", "20090110", Decimal("500.00"), Decimal("0.00"), 4, "P2"),
         ("c2", 1, "20090301", "20090302", Decimal("80.00"), Decimal("0.00"), 1, "P3")],
        "CLM_ID string, SEGMENT int, CLM_FROM_DT string, CLM_THRU_DT string, CLM_PMT_AMT decimal(12,2), "
        "NCH_PRMRY_PYR_CLM_PD_AMT decimal(12,2), CLM_UTLZTN_DAY_CNT int, PRVDR_NUM string")
    out = {r.CLM_ID: r for r in T.rollup_segments(df).collect()}
    assert len(out) == 2
    c1 = out["c1"]
    assert c1.CLM_PMT_AMT == Decimal("1500.00") and c1.CLM_UTLZTN_DAY_CNT == 8
    assert (c1.CLM_FROM_DT, c1.CLM_THRU_DT, c1.PRVDR_NUM, c1.segment_count) == ("20090101", "20090110", "P1", 2)


def test_latest_per_key(spark):
    df = spark.createDataFrame([("m1", 2008, 1), ("m1", 2008, 2), ("m2", 2008, 1)], "id string, y int, ts int")
    out = T.latest_per_key(df, ["id", "y"], "ts").orderBy("id").collect()
    assert [(r.id, r.ts) for r in out] == [("m1", 2), ("m2", 1)]


# ------------------------------------------------------------------ eligibility
def test_expand_member_months_caps_at_death(spark):
    my = spark.createDataFrame(
        [("m1", 2009, 12, None), ("m2", 2009, 12, dt.date(2009, 3, 15)), ("m3", 2009, 0, None),
         ("m4", 2009, 4, None)],
        "member_id string, coverage_year int, member_months int, death_dt date")
    counts = {r.member_id: r["count"] for r in T.expand_member_months(my).groupBy("member_id").count().collect()}
    assert counts == {"m1": 12, "m2": 3, "m4": 4}


# ------------------------------------------------------------------ readmissions
def test_flag_readmissions(spark):
    d = dt.date
    stays = spark.createDataFrame([
        ("m1", "a", d(2009, 1, 1), d(2009, 1, 5)),    # next admit 2009-01-20 -> 15 days -> readmit
        ("m1", "b", d(2009, 1, 20), d(2009, 1, 22)),  # next admit 2009-03-30 -> 67 days -> no
        ("m1", "c", d(2009, 3, 30), d(2009, 4, 2)),   # last stay -> no
        ("m2", "d", d(2009, 6, 1), d(2009, 6, 3)),    # next admit same day as discharge -> transfer -> no
        ("m2", "e", d(2009, 6, 3), d(2009, 6, 9)),    # next admit 2009-07-09 -> exactly 30 days -> readmit
        ("m2", "f", d(2009, 7, 9), d(2009, 7, 10)),
        ("m3", "g", d(2010, 12, 10), d(2010, 12, 12)),  # discharged < 30 days before data end
    ], "member_id string, claim_id string, admit_dt date, discharge_dt date")
    out = {r.claim_id: r for r in T.flag_readmissions(stays, 30, "2010-12-31").collect()}
    assert {k for k, r in out.items() if r.readmit_30d} == {"a", "e"}
    assert out["d"].days_to_next_admit == 0 and not out["d"].readmit_30d
    assert out["g"].has_full_followup is False and out["a"].has_full_followup is True


# ------------------------------------------------------------------ silver builders
def test_explode_diagnoses_positions_and_blanks(spark):
    wide = spark.createDataFrame([("c1", "m1", "4280", None, "V5841")],
                                 "CLM_ID string, DESYNPUF_ID string, ICD9_DGNS_CD_1 string, "
                                 "ICD9_DGNS_CD_2 string, ICD9_DGNS_CD_3 string")
    rows = silver.explode_diagnoses(wide, "IP").orderBy("dx_position").collect()
    assert [(r.dx_position, r.icd9_display) for r in rows] == [(1, "428.0"), (3, "V58.41")]


def test_member_year_decodes_and_counts_conditions(spark):
    cols = config.BENEFICIARY_COLS
    vals = {c: None for c in cols}
    vals.update({"DESYNPUF_ID": "abc", "BENE_BIRTH_DT": "19230501", "BENE_SEX_IDENT_CD": 2, "BENE_RACE_CD": 1,
                 "BENE_ESRD_IND": "0", "BENE_HI_CVRAGE_TOT_MONS": 12, "BENE_SMI_CVRAGE_TOT_MONS": 10})
    for c in config.CHRONIC_FLAGS:
        vals[c] = 2
    vals["SP_CHF"] = 1
    vals["SP_DIABETES"] = 1
    schema = config.bronze_schema("beneficiary", with_corrupt=False)
    row = tuple(vals[c] for c in cols)
    bene = spark.createDataFrame([row, row], schema) \
        .withColumn("bene_year", F.lit(2009)).withColumn("_ingest_ts", F.current_timestamp())
    r = silver.build_member_year(bene).collect()
    assert len(r) == 1  # exact duplicate dropped
    r = r[0]
    assert (r.member_id, r.sex, r.race, r.member_months, r.chronic_condition_count, r.cc_chf, r.age_at_year_end) == \
           ("ABC", "Female", "White", 12, 2, True, 86)


# ------------------------------------------------------------------ bronze schema contract
def test_header_contract(tmp_path):
    good = tmp_path / "ip.csv"
    good.write_text(",".join(f'"{c}"' for c in config.INPATIENT_COLS) + "\n")
    check_header("inpatient", good)  # no exception
    bad = tmp_path / "ip_bad.csv"
    bad.write_text(",".join(config.INPATIENT_COLS[:-1] + ["NEW_COL"]) + "\n")
    with pytest.raises(SchemaDriftError):
        check_header("inpatient", bad)


def test_bronze_schema_types():
    s = {f.name: f.dataType.simpleString() for f in config.bronze_schema("carrier")}
    assert s["LINE_NCH_PMT_AMT_1"] == "decimal(12,2)"
    assert s["CLM_ID"] == "string" and s["CLM_FROM_DT"] == "string"
    assert "_corrupt_record" in s
    assert len(config.CARRIER_COLS) == 142
