import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import pyodbc

# Load environment variables from .env file
load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_DRIVER = os.getenv("SQL_DRIVER")

RAW_INPUT_FILE = os.getenv("RAW_INPUT_FILE")
TRANSFORMED_OUTPUT_FILE = os.getenv("TRANSFORMED_OUTPUT_FILE")

CSV_SEP = "`"
TABLE_NAME = "meetings"


# Check that all required environment variables are set before proceeding
def validate_env():
    missing = []

    if not RAW_INPUT_FILE:
        missing.append("RAW_INPUT_FILE")
    if not TRANSFORMED_OUTPUT_FILE:
        missing.append("RAW_OUTPUT_FILE")
    if not SQL_SERVER:
        missing.append("SQL_SERVER")
    if not SQL_DATABASE:
        missing.append("SQL_DATABASE")
    if not SQL_DRIVER:
        missing.append("SQL_DRIVER")

    # Fail fast if anything is missing to avoid cryptic errors later
    if missing:
        raise ValueError(
            f"Missing the following required .env variables: {', '.join(missing)}"
        )

validate_env()


# Create SQLAlchemy connection string with proper escaping for the ODBC driver
def build_sql_engine():
    encoded_driver = SQL_DRIVER.replace(" ", "+")
    conn_str = (
        f"mssql+pyodbc://@{SQL_SERVER}/{SQL_DATABASE}"
        f"?driver={encoded_driver}&Trusted_Connection=yes;TrustServerCertificate=yes;"
    )
    return create_engine(conn_str)


# Connect directly to master database to create the target database if it doesn't exist yet
print(f"Connecting to SQL Server {SQL_SERVER} (master DB)...")

conn_str = f"DRIVER={{{SQL_DRIVER}}};SERVER={SQL_SERVER};DATABASE=master;Trusted_Connection=yes;TrustServerCertificate=yes;"

with pyodbc.connect(conn_str, autocommit=True) as conn:
    cursor = conn.cursor()
    print(f"Checking if database '{SQL_DATABASE}' exists...")
    # Use SQL conditional to safely create database only if missing
    cursor.execute(
        f"IF DB_ID('{SQL_DATABASE}') IS NULL CREATE DATABASE [{SQL_DATABASE}]"
    )
    print(f"Database '{SQL_DATABASE}' is ready!")


# Parse raw CSV and enrich with calculated fields for analysis
def transform_meeting_data():
    print(f"Loading raw CSV: {RAW_INPUT_FILE}")

    df = pd.read_csv(RAW_INPUT_FILE, sep=CSV_SEP)

    # Convert string columns to proper datetime objects for time calculations
    for col in ["date", "start_time", "end_time"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # Fill missing subjects with a default value rather than leaving them null
    df["meeting_subject"].fillna("Unspecified Activity", inplace=True)

    # Calculate meeting duration in minutes from start and end times
    df["duration_minutes"] = (
        (df["end_time"] - df["start_time"])
        .dt.total_seconds()
        .fillna(0) / 60
    )

    # Extract temporal features to enable grouping and trend analysis
    df["weekday"] = df["date"].dt.day_name()
    df["month"] = df["date"].dt.month_name()
    df["year"] = df["date"].dt.year

    # Categorize meetings into time-of-day buckets for workload insights
    df["time_of_day"] = df["start_time"].dt.hour.apply(
        lambda h: "Morning" if h < 12 else "Afternoon" if h < 17 else "Evening"
        if not pd.isna(h) else "Unknown"
    )

    # Map numeric load percentage to human-readable workload categories
    def load_band(p):
        if pd.isna(p): return "Unknown"
        p = float(p)
        if p >= 80: return "Very Busy"
        if p >= 50: return "Busy"
        if p >= 20: return "Moderate"
        return "Light"

    df["load_band"] = df["load_percentage"].apply(load_band)

    # Convert structured meeting data into natural language summaries for the chatbot
    def record_to_sentence(row):
        name = row.get("first_name", "Someone")
        date = row["date"].strftime("%Y-%m-%d") if not pd.isna(row["date"]) else "Unknown"
        start = row["start_time"].strftime("%H:%M") if not pd.isna(row["start_time"]) else "??:??"
        end = row["end_time"].strftime("%H:%M") if not pd.isna(row["end_time"]) else "??:??"
        duration = int(row.get("duration_minutes", 0))
        subject = row.get("meeting_subject", "Unspecified")
        load_pct = row.get("load_percentage", "Unknown")
        band = row.get("load_band", "Unknown")

        return (
            f"{name} has '{subject}' on {date} from {start} to {end}, "
            f"lasting {duration} minutes. Workload load: {load_pct}% ({band})."
        )

    df["summary_sentence"] = df.apply(record_to_sentence, axis=1)

    # Remove columns that won't be needed downstream to reduce storage and noise
    cols_to_drop = ["content", "week_number", "year", "time_slot"] 
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

    print(f"Saving transformed CSV → {TRANSFORMED_OUTPUT_FILE}")
    df.to_csv(TRANSFORMED_OUTPUT_FILE, sep=CSV_SEP, index=False)

    return df


# Push the enriched dataset into SQL Server for persistence and downstream analytics
def upload_to_sql(df):
    print("Uploading data to SQL Server...")
    engine = build_sql_engine()
    # Use replace mode to overwrite the table with fresh transformed data each run
    df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)
    print("SQL upload complete.")


# Run the full ETL pipeline from raw CSV through transformation to database storage
if __name__ == "__main__":
    df = transform_meeting_data()
    upload_to_sql(df)
    print("Pipeline complete!")
