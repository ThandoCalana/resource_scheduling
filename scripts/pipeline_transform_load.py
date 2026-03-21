"""
pipeline_transform_load.py
Slipstream Intelligence — Schedule Transform & Load Pipeline

Takes the raw output.csv (backtick-delimited, time-slot exploded) produced
by the Outlook pull stage and transforms it into the two Snowflake fact tables:
  - FACT_SCHEDULE_MEETINGS  (one row per unique meeting per employee)
  - FACT_SCHEDULE_DAILY     (one row per employee per day, aggregated)

Then loads both directly into Snowflake via the connector.

Run:
    python pipeline_transform_load.py --input output.csv
    python pipeline_transform_load.py --input output.csv --dry-run
"""

import os
import argparse
from datetime import date
# from dotenv import load_dotenv

# load_dotenv()

import pandas as pd
import numpy as np

MAX_MEETING_MINS = 480


# NAME NORMALISATION
# Fixes the inconsistent email formats coming out of the Outlook pull
# e.g. Azabenathi.Pupuma@slipstreamdata.co.za → azabenathi.pupuma@...
# and derives full_name from the email local part
def normalise_email(email: str) -> str:
    return email.strip().lower()

def email_to_fullname(email: str) -> str:
    local = email.split("@")[0].lower()
    parts = local.split(".")
    return " ".join(p.title() for p in parts) if len(parts) > 1 else local.title()


def read_raw(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, sep=",", engine="python")
    # Drop any phantom columns from trailing delimiters
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:

    # Normalise email and derive full_name
    df["user_email"] = df["user_email"].apply(normalise_email)
    df["full_name"]  = df["user_email"].apply(email_to_fullname)
    df["first_name"] = df['full_name'].str.split(' ').str[0]

    # Parse dates and times
    df["date"]       = pd.to_datetime(df["date"], errors="coerce")
    df["start_time"] = pd.to_datetime(df["start_dt"], errors="coerce")
    df["end_time"]   = pd.to_datetime(df["end_dt"],   errors="coerce")

    # Drop rows with unparseable dates
    bad = df["date"].isnull() | df["start_time"].isnull() | df["end_time"].isnull()
    if bad.sum():
        df = df[~bad].copy()

    # Duration from actual start/end (ignore the slot-level dur)
    df["duration_mins"] = (df["end_time"] - df["start_time"]).dt.total_seconds() / 60

    # Flag all-day / OOO blocks
    df["is_all_day_block"] = df["duration_mins"] > MAX_MEETING_MINS

    # Clean meeting subject
    df["meeting_subject"] = (
        df["subject"]
        .fillna("[No Subject / Private]")
        .str.replace(r"\s{2,}", " ", regex=True)
        .str.strip()
    )

    # Load percentage: source is decimal (0.72) → convert to integer (72)
    df["load_pct"]    = (df["load_pct"] * 100).round(0).astype(int)
    df["load_pct"]    = df["load_pct"].clip(upper=100)
    df["has_overlap"] = df["load_pct"] > 100

    # Calendar helpers
    df["day_of_week"] = df["date"].dt.day_name()
    df["week_number"] = df["date"].dt.isocalendar().week.astype(int)
    df["year"]        = df["date"].dt.isocalendar().year.astype(int)
    df["month"]       = df["date"].dt.month_name()
    df["week_start"]  = (df["date"] - pd.to_timedelta(df["date"].dt.dayofweek, unit="D")).dt.date
    df["date"]        = df["date"].dt.date

    return df


def build_meetings(df: pd.DataFrame) -> pd.DataFrame:

    meetings = df[~df["is_all_day_block"]].copy()

    # Deduplicate — one row per unique meeting per person
    meetings = meetings.drop_duplicates(
        subset=["user_email", "start_time", "end_time", "meeting_subject"]
    ).copy()

    meetings = meetings[[
        "user_email", "full_name", "first_name",
        "date", "week_start", "week_number", "year", "month", "day_of_week",
        "meeting_subject", "start_time", "end_time",
        "duration_mins", "load_pct", "has_overlap",
    ]].sort_values(["user_email", "start_time"]).reset_index(drop=True)

    meetings["meeting_id"] = range(1, len(meetings) + 1)

    return meetings


def build_daily(meetings: pd.DataFrame, df_full: pd.DataFrame) -> pd.DataFrame:

    agg = meetings.groupby(["user_email", "full_name", "first_name", "date"]).agg(
        meeting_count         = ("meeting_id",    "count"),
        total_booked_mins     = ("duration_mins", "sum"),
        longest_meeting_mins  = ("duration_mins", "max"),
        shortest_meeting_mins = ("duration_mins", "min"),
        first_meeting_start   = ("start_time",    "min"),
        last_meeting_end      = ("end_time",       "max"),
        has_overlap           = ("has_overlap",    "any"),
    ).reset_index()

    # Bring in load_pct from daily grain
    daily_load = df_full.groupby(["user_email", "date"])["load_pct"].first().reset_index()
    agg = agg.merge(daily_load, on=["user_email", "date"], how="left")

    # Calendar helpers
    agg["date"]        = pd.to_datetime(agg["date"])
    agg["day_of_week"] = agg["date"].dt.day_name()
    agg["week_number"] = agg["date"].dt.isocalendar().week.astype(int)
    agg["year"]        = agg["date"].dt.isocalendar().year.astype(int)
    agg["month"]       = agg["date"].dt.month_name()
    agg["week_start"]  = (agg["date"] - pd.to_timedelta(agg["date"].dt.dayofweek, unit="D")).dt.date
    agg["date"]        = agg["date"].astype(str)

    def load_category(pct):
        if pd.isna(pct): return "Unknown"
        if pct >= 90: return "Very Heavy"
        if pct >= 70: return "Heavy"
        if pct >= 40: return "Moderate"
        return "Light"

    agg["load_category"] = agg["load_pct"].apply(load_category)
    agg["free_mins"]     = (480 - agg["total_booked_mins"]).clip(lower=0)

    agg = agg.sort_values(["user_email", "date"]).reset_index(drop=True)
    agg["daily_id"] = range(1, len(agg) + 1)

    return agg


def validate(meetings: pd.DataFrame, daily: pd.DataFrame):
    errors = []
    if meetings["meeting_id"].duplicated().any():
        errors.append("Duplicate meeting_ids")
    if meetings["user_email"].isnull().any():
        errors.append("Null user_email in meetings")
    if (meetings["duration_mins"] <= 0).any():
        errors.append("Zero or negative duration")
    if (daily["meeting_count"] < 1).any():
        errors.append("Daily rows with 0 meeting count")
    if errors:
        raise ValueError("Validation failed: " + " | ".join(errors))


def load_to_snowflake(meetings: pd.DataFrame, daily: pd.DataFrame):
    try:
        import snowflake.connector
        from snowflake.connector.pandas_tools import write_pandas
    except ImportError:
        raise ImportError("Run: pip install snowflake-connector-python")

    conn = snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database  = "SCHEDULE_DB",
        schema    = "PUBLIC",
    )
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE SCHEDULE_DB.PUBLIC.FACT_SCHEDULE_MEETINGS_TEST")

    success, nchunks, nrows, _ = write_pandas(
        conn, meetings.rename(columns=str.upper), "FACT_SCHEDULE_MEETINGS_TEST",
        database="SCHEDULE_DB", schema="PUBLIC", auto_create_table=False,use_logical_type=True
    )

    cur.execute("TRUNCATE TABLE SCHEDULE_DB.PUBLIC.FACT_SCHEDULE_TEST")

    success, nchunks, nrows, _ = write_pandas(
        conn, daily.rename(columns=str.upper), "FACT_SCHEDULE_TEST",
        database="SCHEDULE_DB", schema="PUBLIC", auto_create_table=False, use_logical_type=True
    )

    cur.close()
    conn.close()


def run(input_path: str, dry_run: bool = False):

    raw      = read_raw(input_path)
    cleaned  = clean(raw)
    meetings = build_meetings(cleaned)
    daily    = build_daily(meetings, cleaned)

    validate(meetings, daily)


    if dry_run:
        meetings.to_csv("fact_schedule_meetings_preview.csv", index=False)
        daily.to_csv("fact_schedule_daily_preview.csv", index=False)
    else:
        load_to_snowflake(meetings, daily)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",   required=True, help="Path to raw output.csv")
    parser.add_argument("--dry-run", action="store_true", help="Transform only, skip Snowflake load")
    args = parser.parse_args()
    run(args.input, args.dry_run)
    print("Snowflake tables updated")
