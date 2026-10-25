"""Render the README figures from the gold CSV exports in results/ (no Spark needed).

    python -m src.make_figures      -> figures/pmpm_trend.png, figures/top_diagnoses_by_cost.png,
                                       figures/readmission_by_chapter.png
"""
from __future__ import annotations

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

from src.lakehouse import config  # noqa: E402

COLORS = {"IP": "#1f4e79", "OP": "#4f9fd6", "CAR": "#f2a541", "total": "#222222"}
SOURCE = "Source: CMS DE-SynPUF Sample 1 (2008-2010), synthetic Medicare claims; carrier = Sample 1A file"


def _style(ax, title: str, subtitle: str | None = None) -> None:
    ax.set_title(title, loc="left", fontsize=13, fontweight="bold", pad=22 if subtitle else 8)
    if subtitle:
        ax.text(0, 1.02, subtitle, transform=ax.transAxes, fontsize=9, color="#555555")
    for s in ["top", "right"]:
        ax.spines[s].set_visible(False)
    ax.grid(axis="y", alpha=0.25)


def pmpm_trend() -> str:
    df = pd.read_csv(config.RESULTS / "gold_pmpm_monthly.csv", parse_dates=["service_month"])
    df = df[(df["service_month"] >= "2008-01-01") & (df["service_month"] <= "2010-12-01")].sort_values("service_month")
    fig, ax = plt.subplots(figsize=(10, 5))
    x = df["service_month"]
    ax.stackplot(x, df["pmpm_ip"], df["pmpm_op"], df["pmpm_carrier"],
                 labels=["Inpatient", "Outpatient", "Carrier (physician/supplier)"],
                 colors=[COLORS["IP"], COLORS["OP"], COLORS["CAR"]], alpha=0.9)
    ax.plot(x, df["pmpm_total"], color=COLORS["total"], lw=1.2, label="Total PMPM")
    ax.set_ylabel("Paid per member per month (USD)")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"${v:,.0f}"))
    _style(ax, "Medicare paid PMPM by claim type, 2008-2010",
           "Paid amount / eligible Part A or B member-months, by month of service")
    ax.legend(loc="upper right", frameon=False, fontsize=9)
    fig.text(0.01, 0.01, SOURCE, fontsize=7, color="#777777")
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    out = config.FIGURES / "pmpm_trend.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return str(out)


def top_diagnoses(n: int = 15) -> str:
    df = pd.read_csv(config.RESULTS / "gold_top_diagnoses_by_cost.csv").sort_values("cost_rank").head(n)

    def label(r):
        desc = str(r["description"]) if pd.notna(r["description"]) else "(3-digit category / not in CMS v32 list)"
        desc = desc if len(desc) <= 48 else desc[:45] + "..."
        code = r["icd9_display"] if pd.notna(r["icd9_display"]) else r["principal_dx"]
        return f"{code}  {desc}"

    df["label"] = df.apply(label, axis=1)
    df = df.iloc[::-1]
    fig, ax = plt.subplots(figsize=(10, 6.5))
    left = 0
    for col, name, c in [("paid_ip", "Inpatient", COLORS["IP"]), ("paid_op", "Outpatient", COLORS["OP"]),
                         ("paid_carrier", "Carrier", COLORS["CAR"])]:
        vals = df[col] / 1e6
        ax.barh(df["label"], vals, left=left, color=c, label=name)
        left = left + vals
    for y, (tot, share) in enumerate(zip(df["paid_total"] / 1e6, df["share_of_total_paid"])):
        ax.text(tot + 0.3, y, f"${tot:,.1f}M  ({share:.1%})", va="center", fontsize=8)
    ax.set_xlabel("Total Medicare paid (USD millions), 2008-2010")
    ax.set_xlim(0, (df["paid_total"].max() / 1e6) * 1.22)
    ax.tick_params(axis="y", labelsize=8)
    ax.set_title(f"Top {n} principal diagnoses by total paid", loc="left", fontsize=13, fontweight="bold")
    for s in ["top", "right"]:
        ax.spines[s].set_visible(False)
    ax.grid(axis="x", alpha=0.25)
    ax.legend(loc="lower right", frameon=False, fontsize=9)
    fig.text(0.01, 0.01, SOURCE, fontsize=7, color="#777777")
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    out = config.FIGURES / "top_diagnoses_by_cost.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return str(out)


def readmission_by_chapter(min_stays: int = 200) -> str:
    df = pd.read_csv(config.RESULTS / "gold_readmission_summary.csv")
    overall = float(df.loc[df["dimension"] == "overall", "readmit_rate_30d"].iloc[0])
    ch = df[(df["dimension"] == "principal_dx_chapter") & (df["index_stays"] >= min_stays)]
    ch = ch[ch["value"].notna() & (ch["value"] != "Invalid / missing")].sort_values("readmit_rate_30d")
    fig, ax = plt.subplots(figsize=(10, 6.5))
    colors = [COLORS["CAR"] if r > overall else COLORS["IP"] for r in ch["readmit_rate_30d"]]
    ax.barh(ch["value"], ch["readmit_rate_30d"] * 100, color=colors)
    for y, (r, n) in enumerate(zip(ch["readmit_rate_30d"], ch["index_stays"])):
        ax.text(max(r, overall) * 100 + 0.25, y, f"{r:.1%}  (n={n:,})", va="center", fontsize=8)
    ax.axvline(overall * 100, color="#222222", ls="--", lw=1)
    ax.text(overall * 100, len(ch) - 0.4, f" overall {overall:.1%}", fontsize=8, va="bottom")
    ax.set_xlabel("30-day all-cause readmission rate (%)")
    ax.set_xlim(0, ch["readmit_rate_30d"].max() * 100 * 1.3)
    ax.tick_params(axis="y", labelsize=9)
    ax.set_title("30-day readmission rate by principal-diagnosis chapter", loc="left", fontsize=13,
                 fontweight="bold", pad=22)
    ax.text(0, 1.02, f"Index stays with a full 30-day follow-up; chapters with >= {min_stays} stays; orange = above overall rate",
            transform=ax.transAxes, fontsize=9, color="#555555")
    for s in ["top", "right"]:
        ax.spines[s].set_visible(False)
    ax.grid(axis="x", alpha=0.25)
    fig.text(0.01, 0.01, SOURCE, fontsize=7, color="#777777")
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    out = config.FIGURES / "readmission_by_chapter.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return str(out)


def main() -> None:
    config.FIGURES.mkdir(parents=True, exist_ok=True)
    for f in (pmpm_trend(), top_diagnoses(), readmission_by_chapter()):
        print("wrote", f)


if __name__ == "__main__":
    main()
