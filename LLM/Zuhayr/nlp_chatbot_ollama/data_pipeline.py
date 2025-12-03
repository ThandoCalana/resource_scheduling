
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import pyodbc

# Load environment variables from .env file
load_dotenv()

DB_SERVER = os.getenv("SQL_SERVER")
DB_NAME = os.getenv("SQL_DATABASE")
DB_DRIVER = os.getenv("SQL_DRIVER")
TABLE_NAME = "meetings"

RAW_INPUT_FILE = os.getenv("RAW_INPUT_FILE", "output.csv")
TRANSFORMED_OUTPUT_FILE = os.getenv("RAW_OUTPUT_FILE", "data_transformed.csv")

CSV_SEP = "`"
SQL_TABLE = TABLE_NAME

# Build connection string for SQLAlchemy
def build_sql_engine():
    driver_encoded = DB_DRIVER.replace(" ", "+")
    conn_str = (
        f"mssql+pyodbc://@{DB_SERVER}/{DB_NAME}"
        f"?driver={driver_encoded}&trusted_connection=yes"
    )
    return create_engine(conn_str)

# Create database if it doesn't exist
print(f"Connecting to SQL Server {DB_SERVER} (master DB)...")

conn_str = f"DRIVER={{{DB_DRIVER}}};SERVER={DB_SERVER};DATABASE=master;Trusted_Connection=yes"

with pyodbc.connect(conn_str, autocommit=True) as conn:
    cursor = conn.cursor()
    print(f"Checking if database '{DB_NAME}' exists...")
    cursor.execute(f"IF DB_ID('{DB_NAME}') IS NULL CREATE DATABASE [{DB_NAME}]")
    print(f"Database '{DB_NAME}' is ready!")

# Transform and clean meeting data
def transform_meeting_data():
    print("📥 Loading raw CSV:", RAW_INPUT_FILE)
    df = pd.read_csv(RAW_INPUT_FILE, sep=CSV_SEP)

    # Convert date/time columns to datetime
    for col in ["date", "start_time", "end_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Fill missing subjects with default value
    if "meeting_subject" in df.columns:
        df["meeting_subject"].fillna("Unspecified Activity", inplace=True)

    # Calculate meeting duration
    if "start_time" in df.columns and "end_time" in df.columns:
        df["duration_minutes"] = (
            (df["end_time"] - df["start_time"])
            .dt.total_seconds()
            .fillna(0) / 60
        )
    else:
        df["duration_minutes"] = 0

    # Extract day info from date
    if "date" in df.columns:
        df["weekday"] = df["date"].dt.day_name()
        df["month"] = df["date"].dt.month_name()
        df["year"] = df["date"].dt.year
    else:
        df["weekday"] = ""
        df["month"] = ""
        df["year"] = ""

    # Categorize time of day
    def time_of_day_safe(val):
        if pd.isna(val):
            return "Unknown"
        hour = int(val)
        if hour < 12:
            return "Morning"
        elif hour < 17:
            return "Afternoon"
        return "Evening"

    if "start_time" in df.columns:
        df["time_of_day"] = df["start_time"].dt.hour.apply(time_of_day_safe)
    else:
        df["time_of_day"] = "Unknown"

    # Categorize workload bands
    def load_band(p):
        if pd.isna(p):
            return "Unknown"
        p = float(p)
        if p >= 80:
            return "Very Busy"
        if p >= 50:
            return "Busy"
        if p >= 20:
            return "Moderate"
        return "Light"

    if "load_percentage" in df.columns:
        df["load_band"] = df["load_percentage"].apply(load_band)
    else:
        df["load_band"] = "Unknown"

    # Create summary sentence for each record
    def record_to_sentence(row):
        name = row.get("first_name", "Someone")

        date_value = (
            row["date"].strftime("%Y-%m-%d")
            if "date" in row and not pd.isna(row["date"])
            else "Unknown date"
        )

        start = (
            row["start_time"].strftime("%H:%M")
            if "start_time" in row and not pd.isna(row["start_time"])
            else "??:??"
        )

        end = (
            row["end_time"].strftime("%H:%M")
            if "end_time" in row and not pd.isna(row["end_time"])
            else "??:??"
        )

        subject = row.get("meeting_subject", "Unspecified Activity")
        duration = int(row.get("duration_minutes", 0))
        load_pct = row.get("load_percentage", "Unknown")
        band = row.get("load_band", "Unknown")

        return (
            f"{name} has '{subject}' on {date_value} "
            f"from {start} to {end}, lasting {duration} minutes. "
            f"Workload load: {load_pct}% ({band})."
        )

    df["summary_sentence"] = df.apply(record_to_sentence, axis=1)

    # Drop unnecessary columns
    cols_to_drop = ["content", "week_number", "year", "time_slot"]
    df_clean = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

    # Save transformed data to CSV
    print("💾 Saving transformed CSV →", TRANSFORMED_OUTPUT_FILE)
    df_clean.to_csv(TRANSFORMED_OUTPUT_FILE, sep=CSV_SEP, index=False, encoding="utf-8")

    return df_clean

# Upload transformed data to SQL Server
def upload_to_sql(df):
    print("📡 Uploading to SQL Server…")
    engine = build_sql_engine()
    df.to_sql(SQL_TABLE, engine, if_exists="replace", index=False)
    print("✅ Upload complete.")
