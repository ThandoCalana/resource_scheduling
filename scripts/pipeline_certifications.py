"""
pipeline_certifications.py
Slipstream Intelligence Platform — Certifications & Skills Data Pipeline

What this script does:
  1. Reads the certifications Excel file
  2. Collapses 382,614 rows (one per calendar day per cert) down to
     82 rows (one per person per certification — the current snapshot)
  3. Cleans, validates, and enriches every field
  4. Computes days_until_expiry from today so Cortex Analyst can answer
     "which certs are expiring soon?" accurately
  5. Saves FACT_EMPLOYEE_CERTIFICATION.csv ready for Snowflake load

Why the collapse:
  The source file stores a daily time-series for every cert — 4,449 dates
  × 82 unique person+cert combinations = 382,614 rows. For the chatbot we
  only need the current state of each certification. We take the row with
  the most recent Record Date + Last Updated per person+cert combination.

Run:
    python pipeline_certifications.py --input cleaned_cert.xlsx
"""

import pandas as pd
import numpy as np
import os
import logging
import argparse
from datetime import date, datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

TODAY = date.today()

# Partner tier decode — based on observed values in source data
PARTNER_TIER = {
    0.0: "No Partner Tier",
    2.0: "Silver Partner",
    6.0: "Gold Partner",
}

# Status normalisation — map source values to clean labels
STATUS_MAP = {
    "Active":            "Active",
    "Expired":           "Expired",
    "Planned":           "Planned",
    "Projected Active":  "Projected Active",
    "Projected Expired": "Projected Expired",
}

# Expiry category thresholds (days)
def expiry_category(days: float) -> str:
    if pd.isna(days) or days < 0:
        return "Expired"
    if days <= 90:
        return "Expiring Within 90 Days"
    if days <= 180:
        return "Expiring Within 6 Months"
    if days <= 365:
        return "Expiring Within 1 Year"
    return "Valid"


# ─────────────────────────────────────────────────────────────────
# STEP 1 — READ
# ─────────────────────────────────────────────────────────────────

def read_raw(path: str) -> pd.DataFrame:
    log.info(f"Reading: {path}")
    df = pd.read_excel(path)
    log.info(f"Raw rows: {len(df):,}  |  Columns: {df.columns.tolist()}")
    return df


# ─────────────────────────────────────────────────────────────────
# STEP 2 — COLLAPSE to one row per person + cert
# ─────────────────────────────────────────────────────────────────

def collapse(df: pd.DataFrame) -> pd.DataFrame:
    """
    Source has one row per calendar day per cert (daily time-series).
    We want the single most-current snapshot for each person+cert.
    Strategy: sort by Record Date DESC + Last Updated DESC, dedup on Name+Cert.
    """
    log.info("Collapsing daily time-series to current snapshot...")

    df = df.sort_values(
        ["Name", "Certification Name", "Record Date", "Last Updated"],
        ascending=[True, True, False, False]
    )
    df = df.drop_duplicates(subset=["Name", "Certification Name"], keep="first").copy()

    log.info(f"Rows after collapse: {len(df):,}")
    return df


# ─────────────────────────────────────────────────────────────────
# STEP 3 — CLEAN & ENRICH
# ─────────────────────────────────────────────────────────────────

def clean(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Cleaning and enriching...")

    # Drop the source row-index column
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # ── Rename columns to clean snake_case ──
    df = df.rename(columns={
        "Date":                      "snapshot_date",
        "Name":                      "full_name",
        "Certification Name":        "certification_name",
        "Technology":                "technology",
        "Status":                    "status",
        "Expiration Date":           "expiration_date",
        "Expiration Date Indicator": "expiry_indicator",
        "Validity (Years)":          "validity_years",
        "Cost ($)":                  "cost_usd",
        "Record Date":               "record_date",
        "Last Updated":              "last_updated",
        "Latest Flag":               "latest_flag",
        "Employment Status":         "employment_status",
        "Partner":                   "partner_code",
        "validityNow":               "validity_now_days",
    })

    # ── Dates ──
    for col in ["snapshot_date", "expiration_date", "record_date", "last_updated"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # ── Text cleaning ──
    df["full_name"]          = df["full_name"].str.strip().str.title()
    df["certification_name"] = df["certification_name"].str.strip()
    df["technology"]         = df["technology"].str.strip()
    df["employment_status"]  = df["employment_status"].str.strip()

    # ── Status normalise ──
    df["status"] = df["status"].str.strip().map(STATUS_MAP).fillna("Unknown")

    # ── Expiry indicator ──
    df["expiry_indicator"] = df["expiry_indicator"].str.strip()

    # ── Partner ──
    df["partner_code"]  = df["partner_code"].fillna(0.0)
    df["partner_tier"]  = df["partner_code"].map(PARTNER_TIER).fillna("Unknown")

    # ── Latest flag to boolean ──
    df["is_latest"] = df["latest_flag"].astype(bool)

    # ── Days until expiry — recompute from today ──
    # Source's validityNow was computed from a fixed date, not today.
    # We recompute from TODAY so "expiring in 90 days" is always accurate.
    def days_until(exp_date):
        if pd.isna(exp_date):
            return None
        delta = (exp_date - TODAY).days
        return delta

    df["days_until_expiry"] = df["expiration_date"].apply(days_until)

    # ── Expiry category (for plain-English Cortex Analyst queries) ──
    # Don't apply to Planned certs — they haven't been obtained yet
    def expiry_cat(row):
        if row["status"] in ("Planned", "Projected Active"):
            return "Planned / Not Yet Active"
        if row["expiry_indicator"] == "Projected" and row["expiration_date"] == date(1972, 1, 1):
            return "Projected Expired (No Real Date)"
        return expiry_category(row["days_until_expiry"])

    df["expiry_category"] = df.apply(expiry_cat, axis=1)

    # ── Cost: source values look like exam fees only (max $200) ──
    # Mark zero-cost certs explicitly
    df["cost_usd"]     = df["cost_usd"].fillna(0).astype(int)
    df["is_free_cert"] = df["cost_usd"] == 0

    # ── Validity years: fill nulls with 0 (unknown) ──
    df["validity_years"] = df["validity_years"].fillna(0).astype(int)

    # ── First name (useful for chatbot name matching) ──
    df["first_name"] = df["full_name"].str.split().str[0]

    # ── Year and month of expiry (for trend queries) ──
    df["expiry_year"]  = pd.to_datetime(df["expiration_date"], errors="coerce").dt.year
    df["expiry_month"] = pd.to_datetime(df["expiration_date"], errors="coerce").dt.month_name()

    # ── Load date ──
    df["load_date"] = TODAY

    log.info("Cleaning complete.")
    return df


# ─────────────────────────────────────────────────────────────────
# STEP 4 — VALIDATE
# ─────────────────────────────────────────────────────────────────

def validate(df: pd.DataFrame):
    log.info("Running validation checks...")
    errors = []

    # No nulls on required fields
    for col in ["full_name", "certification_name", "technology", "status", "employment_status"]:
        if df[col].isnull().any():
            errors.append(f"Null values found in required column: {col}")

    # No duplicate person+cert
    dupes = df.duplicated(subset=["full_name", "certification_name"])
    if dupes.any():
        errors.append(f"Duplicate person+cert rows: {dupes.sum()}")

    # Status values are all known
    unknown_status = df[df["status"] == "Unknown"]
    if len(unknown_status) > 0:
        errors.append(f"Unknown status values in {len(unknown_status)} rows")

    # Cost never negative
    if (df["cost_usd"] < 0).any():
        errors.append("Negative cost_usd values found")

    if errors:
        for e in errors:
            log.error(f"VALIDATION FAILED: {e}")
        raise ValueError("Pipeline failed validation.")
    else:
        log.info("All validation checks passed.")


# ─────────────────────────────────────────────────────────────────
# STEP 5 — SELECT FINAL COLUMNS & SAVE
# ─────────────────────────────────────────────────────────────────

FINAL_COLUMNS = [
    "full_name", "first_name", "employment_status", "partner_code", "partner_tier",
    "certification_name", "technology", "status", "expiry_indicator",
    "expiration_date", "expiry_year", "expiry_month",
    "days_until_expiry", "expiry_category",
    "validity_years", "cost_usd", "is_free_cert",
    "record_date", "last_updated", "is_latest",
    "load_date",
]

def save(df: pd.DataFrame, output_dir: str = "outputs") -> str:
    os.makedirs(output_dir, exist_ok=True)
    out = df[FINAL_COLUMNS].sort_values(["full_name", "technology", "certification_name"])
    path = os.path.join(output_dir, "fact_employee_certification.csv")
    out.to_csv(path, index=False)
    log.info(f"Saved: {path}  ({len(out):,} rows)")
    return path


def upload_to_snowflake(path: str):
    try:
        import snowflake.connector
    except ImportError:
        log.warning("snowflake-connector-python not installed. Skipping upload.")
        return

    conn = snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database  = "SKILLS_DB",
        schema    = "PUBLIC",
    )
    cur = conn.cursor()
    log.info("Uploading to @SKILLS_DB.PUBLIC.RAW_STAGE...")
    cur.execute(f"PUT file://{path} @SKILLS_DB.PUBLIC.RAW_STAGE OVERWRITE = TRUE AUTO_COMPRESS = TRUE")
    cur.execute("EXECUTE TASK SKILLS_DB.PUBLIC.TASK_LOAD_CERTIFICATIONS")
    cur.close()
    conn.close()
    log.info("Snowflake upload complete.")


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────

def run(input_path: str, output_dir: str = "outputs", upload: bool = False):
    log.info("=" * 60)
    log.info("Slipstream Certifications Pipeline — starting")
    log.info(f"Today: {TODAY}")
    log.info("=" * 60)

    raw       = read_raw(input_path)
    collapsed = collapse(raw)
    cleaned   = clean(collapsed)

    validate(cleaned)

    # Print summary before saving
    log.info("--- Output summary ---")
    log.info(f"Total certs:          {len(cleaned)}")
    log.info(f"Active employees:     {(cleaned['employment_status']=='Active').sum()}")
    log.info(f"Active certs:         {(cleaned['status']=='Active').sum()}")
    log.info(f"Planned certs:        {(cleaned['status']=='Planned').sum()}")
    log.info(f"Expired certs:        {cleaned['status'].isin(['Expired','Projected Expired']).sum()}")
    log.info(f"Expiring within 90d:  {(cleaned['expiry_category']=='Expiring Within 90 Days').sum()}")
    log.info(f"Technologies:         {sorted(cleaned['technology'].unique())}")

    path = save(cleaned, output_dir)

    if upload:
        upload_to_snowflake(path)

    log.info("Pipeline complete.")
    return cleaned


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  required=True)
    parser.add_argument("--output", default="outputs")
    parser.add_argument("--upload", action="store_true")
    args = parser.parse_args()
    run(args.input, args.output, args.upload)
