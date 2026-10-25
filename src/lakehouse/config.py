"""Paths, source-file layout and explicit bronze schemas for the CMS DE-SynPUF Sample 1 files.

Every column list below mirrors the header of the published CSV exactly, in order. Bronze ingest
compares the file header against these lists and refuses to load a file whose layout drifted.
"""
from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType

ROOT = Path(__file__).resolve().parents[2]
# On Databricks set LAKEHOUSE_ROOT to a Unity Catalog Volume, e.g. /Volumes/main/claims/lakehouse
DATA = Path(os.environ.get("LAKEHOUSE_DATA", ROOT / "data"))
RAW = Path(os.environ.get("LAKEHOUSE_RAW", DATA / "raw"))
LAKE = Path(os.environ.get("LAKEHOUSE_ROOT", DATA / "lakehouse"))
RESULTS = ROOT / "results"
FIGURES = ROOT / "figures"

# Last date covered by the claim files (used to drop index stays without a full 30-day follow-up window)
DATA_END_DATE = "2010-12-31"

SOURCES = {
    "beneficiary": [
        "DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv",
        "DE1_0_2009_Beneficiary_Summary_File_Sample_1.csv",
        "DE1_0_2010_Beneficiary_Summary_File_Sample_1.csv",
    ],
    "inpatient": ["DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv"],
    "outpatient": ["DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.csv"],
    "carrier": ["DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.csv"],
}


def _n(prefix: str, n: int, start: int = 1) -> list[str]:
    return [f"{prefix}_{i}" for i in range(start, n + 1)]


BENEFICIARY_COLS = [
    "DESYNPUF_ID", "BENE_BIRTH_DT", "BENE_DEATH_DT", "BENE_SEX_IDENT_CD", "BENE_RACE_CD",
    "BENE_ESRD_IND", "SP_STATE_CODE", "BENE_COUNTY_CD", "BENE_HI_CVRAGE_TOT_MONS",
    "BENE_SMI_CVRAGE_TOT_MONS", "BENE_HMO_CVRAGE_TOT_MONS", "PLAN_CVRG_MOS_NUM",
    "SP_ALZHDMTA", "SP_CHF", "SP_CHRNKIDN", "SP_CNCR", "SP_COPD", "SP_DEPRESSN", "SP_DIABETES",
    "SP_ISCHMCHT", "SP_OSTEOPRS", "SP_RA_OA", "SP_STRKETIA",
    "MEDREIMB_IP", "BENRES_IP", "PPPYMT_IP", "MEDREIMB_OP", "BENRES_OP", "PPPYMT_OP",
    "MEDREIMB_CAR", "BENRES_CAR", "PPPYMT_CAR",
]

INPATIENT_COLS = [
    "DESYNPUF_ID", "CLM_ID", "SEGMENT", "CLM_FROM_DT", "CLM_THRU_DT", "PRVDR_NUM", "CLM_PMT_AMT",
    "NCH_PRMRY_PYR_CLM_PD_AMT", "AT_PHYSN_NPI", "OP_PHYSN_NPI", "OT_PHYSN_NPI", "CLM_ADMSN_DT",
    "ADMTNG_ICD9_DGNS_CD", "CLM_PASS_THRU_PER_DIEM_AMT", "NCH_BENE_IP_DDCTBL_AMT",
    "NCH_BENE_PTA_COINSRNC_LBLTY_AM", "NCH_BENE_BLOOD_DDCTBL_LBLTY_AM", "CLM_UTLZTN_DAY_CNT",
    "NCH_BENE_DSCHRG_DT", "CLM_DRG_CD",
] + _n("ICD9_DGNS_CD", 10) + _n("ICD9_PRCDR_CD", 6) + _n("HCPCS_CD", 45)

OUTPATIENT_COLS = [
    "DESYNPUF_ID", "CLM_ID", "SEGMENT", "CLM_FROM_DT", "CLM_THRU_DT", "PRVDR_NUM", "CLM_PMT_AMT",
    "NCH_PRMRY_PYR_CLM_PD_AMT", "AT_PHYSN_NPI", "OP_PHYSN_NPI", "OT_PHYSN_NPI",
    "NCH_BENE_BLOOD_DDCTBL_LBLTY_AM",
] + _n("ICD9_DGNS_CD", 10) + _n("ICD9_PRCDR_CD", 6) + [
    "NCH_BENE_PTB_DDCTBL_AMT", "NCH_BENE_PTB_COINSRNC_AMT", "ADMTNG_ICD9_DGNS_CD",
] + _n("HCPCS_CD", 45)

CARRIER_COLS = (
    ["DESYNPUF_ID", "CLM_ID", "CLM_FROM_DT", "CLM_THRU_DT"]
    + _n("ICD9_DGNS_CD", 8) + _n("PRF_PHYSN_NPI", 13) + _n("TAX_NUM", 13) + _n("HCPCS_CD", 13)
    + _n("LINE_NCH_PMT_AMT", 13) + _n("LINE_BENE_PTB_DDCTBL_AMT", 13)
    + _n("LINE_BENE_PRMRY_PYR_PD_AMT", 13) + _n("LINE_COINSRNC_AMT", 13)
    + _n("LINE_ALOWD_CHRG_AMT", 13) + _n("LINE_PRCSG_IND_CD", 13) + _n("LINE_ICD9_DGNS_CD", 13)
)

COLUMNS = {
    "beneficiary": BENEFICIARY_COLS,
    "inpatient": INPATIENT_COLS,
    "outpatient": OUTPATIENT_COLS,
    "carrier": CARRIER_COLS,
}

_INT_COLS = {
    "SEGMENT", "BENE_SEX_IDENT_CD", "BENE_RACE_CD", "BENE_HI_CVRAGE_TOT_MONS",
    "BENE_SMI_CVRAGE_TOT_MONS", "BENE_HMO_CVRAGE_TOT_MONS", "PLAN_CVRG_MOS_NUM",
    "CLM_UTLZTN_DAY_CNT",
}
_AMOUNT_PREFIXES = ("MEDREIMB_", "BENRES_", "PPPYMT_", "LINE_NCH_PMT_AMT", "LINE_BENE_",
                    "LINE_COINSRNC_AMT", "LINE_ALOWD_CHRG_AMT")


def _type_for(col: str):
    if col in _INT_COLS or (col.startswith("SP_") and col not in {"SP_STATE_CODE"}):
        return IntegerType()
    if col.endswith("_AMT") or col.endswith("_AM") or col.startswith(_AMOUNT_PREFIXES):
        return DecimalType(12, 2)
    # Identifiers, codes and raw yyyyMMdd dates stay strings in bronze; silver parses them.
    return StringType()


def bronze_schema(table: str, with_corrupt: bool = True) -> StructType:
    fields = [StructField(c, _type_for(c), True) for c in COLUMNS[table]]
    if with_corrupt:
        fields.append(StructField("_corrupt_record", StringType(), True))
    return StructType(fields)


# Chronic-condition flags in the beneficiary file (1 = yes, 2 = no)
CHRONIC_FLAGS = {
    "SP_ALZHDMTA": "cc_alzheimers", "SP_CHF": "cc_chf", "SP_CHRNKIDN": "cc_ckd",
    "SP_CNCR": "cc_cancer", "SP_COPD": "cc_copd", "SP_DEPRESSN": "cc_depression",
    "SP_DIABETES": "cc_diabetes", "SP_ISCHMCHT": "cc_ischemic_heart", "SP_OSTEOPRS": "cc_osteoporosis",
    "SP_RA_OA": "cc_ra_oa", "SP_STRKETIA": "cc_stroke_tia",
}
