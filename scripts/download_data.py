"""Download CMS DE-SynPUF Sample 1 (synthetic Medicare claims) + the CMS ICD-9-CM v32 descriptions.

Source: CMS 2008-2010 Data Entrepreneurs' Synthetic Public Use File (DE-SynPUF), Sample 1 of 20
https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files/cms-2008-2010-data-entrepreneurs-synthetic-public-use-file-de-synpuf/de10-sample-1
DE-SynPUF is fully synthetic: it keeps the structure of real Medicare FFS claims but no real beneficiary is in it.
Free, public, no login. Raw files (~1.4 GB unzipped) are git-ignored and never committed.

Files: beneficiary summary 2008/2009/2010, inpatient + outpatient claims 2008-2010, carrier claims 2008-2010
(part 1A of 1A/1B; use --carrier-b to add 1B), ICD-9-CM v32 (FY2015) long diagnosis descriptions.

Note: on the CMS page the 2010 beneficiary link points to a file named ..._sample_20.zip; its contents are the
Sample 1 file (DE1_0_2010_Beneficiary_Summary_File_Sample_1.csv). This script uses that link.

Usage: python scripts/download_data.py [--carrier-b]
"""
import argparse
import os
import pathlib
import urllib.request
import zipfile

RAW = pathlib.Path(os.environ.get("LAKEHOUSE_RAW", pathlib.Path(__file__).resolve().parents[1] / "data" / "raw"))
SYN = "https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/"
FILES = {
    "DE1_0_2008_Beneficiary_Summary_File_Sample_1.zip": SYN + "DE1_0_2008_Beneficiary_Summary_File_Sample_1.zip",
    "DE1_0_2009_Beneficiary_Summary_File_Sample_1.zip": SYN + "DE1_0_2009_Beneficiary_Summary_File_Sample_1.zip",
    "DE1_0_2010_Beneficiary_Summary_File_Sample_1.zip":
        "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/synpufs/downloads/"
        "de1_0_2010_beneficiary_summary_file_sample_20.zip",
    "DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.zip": SYN + "DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.zip",
    "DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.zip": SYN + "DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.zip",
    "DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.zip":
        "https://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.zip",
    "icd-9-cm-v32-master-descriptions.zip":
        "https://www.cms.gov/medicare/coding/icd9providerdiagnosticcodes/downloads/icd-9-cm-v32-master-descriptions.zip",
}
CARRIER_B = {"DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.zip":
             "https://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.zip"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--carrier-b", action="store_true", help="also fetch carrier part 1B (~1.2 GB unzipped)")
    args = ap.parse_args()
    RAW.mkdir(parents=True, exist_ok=True)
    files = dict(FILES, **(CARRIER_B if args.carrier_b else {}))
    for name, url in files.items():
        z = RAW / name
        if not z.exists():
            print(f"downloading {name}")
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (claims-lakehouse)"})
            with urllib.request.urlopen(req) as r, open(z, "wb") as fh:
                while chunk := r.read(1 << 20):
                    fh.write(chunk)
        if not zipfile.is_zipfile(z):
            z.unlink()
            raise SystemExit(f"{name}: not a zip (CMS may have moved the file); see the source page in this docstring")
        with zipfile.ZipFile(z) as zf:
            for member in zf.namelist():
                if member.endswith((".csv", "CMS32_DESC_LONG_DX.txt")) and not (RAW / member).exists():
                    zf.extract(member, RAW)
        print(f"ok {name}")
    print("raw files:", sorted(p.name for p in RAW.iterdir() if p.suffix in {".csv", ".txt"}))


if __name__ == "__main__":
    main()
