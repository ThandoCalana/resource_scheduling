from __future__ import annotations

import os
from typing import Any

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine


# ---------------------------------------------------------------------------
# Config (all sourced from environment variables / GitHub Actions secrets)
# ---------------------------------------------------------------------------

CLICKUP_API_TOKEN = os.environ["CLICKUP_API_TOKEN"]
CLICKUP_LIST_ID   = os.environ["CLICKUP_LIST_ID"]
BASE_URL          = f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task"

SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ROLE      = os.environ["SNOWFLAKE_ROLE"]
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DATABASE  = os.environ["SNOWFLAKE_DATABASE"]
SNOWFLAKE_SCHEMA    = os.environ["SNOWFLAKE_SCHEMA"]

RESIGNED_NAMES = ["Jenny Wrench", "Lynn Carelse", "Carl Brink"]

OUTPUT_COLUMNS = [
    "Name",
    "Certification Name",
    "Status",
    "Technology",
    "Record Date",
    "Expiration Date",
    "Employment Status",
]


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def fetch_tasks() -> list[dict[str, Any]]:
    headers  = {"Authorization": CLICKUP_API_TOKEN}
    response = requests.get(
        BASE_URL,
        headers=headers,
        params={"subtasks": "true"},
        timeout=60,
    )
    response.raise_for_status()
    tasks = response.json().get("tasks", [])
    print(f"[extract] Tasks fetched: {len(tasks)}")
    return tasks


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def parse_epoch_or_iso(series: pd.Series) -> pd.Series:
    """
    ClickUp returns dates as epoch-ms strings (e.g. "1693612800000") or
    occasionally as ISO strings. This handles both safely.
    """
    # Try numeric first (epoch ms as string or int)
    numeric = pd.to_numeric(series, errors="coerce")
    parsed_epoch = pd.to_datetime(numeric, unit="ms", errors="coerce")

    # For any that failed numeric conversion, try ISO string parsing
    parsed_iso = pd.to_datetime(series, errors="coerce")

    # Use epoch result where available, fall back to ISO
    return parsed_epoch.where(parsed_epoch.notna(), parsed_iso)


def normalize_tasks(tasks: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for task in tasks:
        assignees  = task.get("assignees") or []
        tags       = task.get("tags") or []
        first      = assignees[0] if assignees else None
        first_tag  = tags[0] if tags else None
        custom     = task.get("custom_fields") or []
        expire_val = custom[1].get("value") if len(custom) > 1 else None

        rows.append({
            "Name":               first.get("username") if first else None,
            "Certification Name": task.get("name"),
            "Status":             (task.get("status") or {}).get("status"),
            "Tag":                first_tag.get("name") if first_tag else None,
            "date_created":       task.get("date_created"),
            "due_date":           task.get("due_date"),
            "Expire Date":        expire_val,
        })

    df = pd.DataFrame(rows)

    df["date_created"] = parse_epoch_or_iso(df["date_created"])
    df["due_date"]     = parse_epoch_or_iso(df["due_date"]).dt.normalize()
    df["Expire Date"]  = parse_epoch_or_iso(df["Expire Date"]).dt.normalize()

    # Strip whitespace from text columns only (leave datetime columns alone)
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].fillna("").astype(str).str.strip()

    return df


def add_employment_status(df: pd.DataFrame) -> pd.DataFrame:
    resigned = {n.strip() for n in RESIGNED_NAMES}
    df["Employment Status"] = np.where(df["Name"].isin(resigned), "Resigned", "Active")
    return df


def add_technology(df: pd.DataFrame) -> pd.DataFrame:
    cert = df["Certification Name"].fillna("")
    df["Technology"] = np.select(
        [
            cert.str.contains("Tableau",           case=False),
            cert.str.contains("Alteryx|Trifacta",  case=False),
            cert.str.contains("Google",            case=False),
            cert.str.contains("AWS",               case=False),
            cert.str.contains("Snowflake|SnowPro", case=False),
            cert.str.contains("wherescape",        case=False),
            cert.str.contains("Data Vault",        case=False),
            cert.str.contains("Design Kit",        case=False),
            cert.str.contains("Salesforce",        case=False),
            cert.str.contains("Matillion",         case=False),
        ],
        ["Tableau", "Alteryx", "Google", "AWS", "Snowflake",
         "Wherescape", "Data Vault", "Design Kit", "Salesforce", "Matillion"],
        default="Other",
    )
    return df


def build_planned(df: pd.DataFrame) -> pd.DataFrame:
    planned = df[
        df["Tag"].eq("cert") &
        df["Status"].ne("Done") &
        (df["Name"].ne("") | df["due_date"].notna())
    ].copy()

    planned["Record Date"]     = planned["date_created"].dt.normalize()
    planned["Expiration Date"] = planned["due_date"] + pd.DateOffset(years=2)
    planned["Status"]          = "Planned"

    return planned[OUTPUT_COLUMNS]


def build_completed(df: pd.DataFrame) -> pd.DataFrame:
    done = df[
        df["Tag"].eq("cert") &
        df["Status"].eq("Done") &
        (df["Name"].ne("") | df["due_date"].notna())
    ].copy()

    done["Record Date"]     = done["date_created"].dt.normalize()
    done["Expiration Date"] = np.where(
        done["Expire Date"].isna(),
        pd.Timestamp.today().normalize() + pd.DateOffset(years=10),
        done["Expire Date"],
    )
    done["Expiration Date"] = pd.to_datetime(done["Expiration Date"], errors="coerce").dt.normalize()
    done["Status"] = np.where(
        pd.Timestamp.today().normalize() > done["Expiration Date"], "Expired", "Active"
    )

    return done[OUTPUT_COLUMNS]


def transform(tasks: list[dict[str, Any]]) -> pd.DataFrame:
    df = normalize_tasks(tasks)
    df = add_employment_status(df)
    df = add_technology(df)

    result = pd.concat([build_completed(df), build_planned(df)], ignore_index=True)
    result = result[result["Certification Name"].str.strip().ne("")]

    # Ensure clean date types for Snowflake
    result["Record Date"]     = pd.to_datetime(result["Record Date"],     errors="coerce").dt.date
    result["Expiration Date"] = pd.to_datetime(result["Expiration Date"], errors="coerce").dt.date

    print(f"[transform] Rows after transform: {len(result)}")
    return result


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def push_to_snowflake(df: pd.DataFrame) -> None:
    conn_str = (
        f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}"
        f"@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}"
        f"?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}"
    )
    engine = create_engine(conn_str)
    with engine.connect() as conn:
        df.to_sql(
            "certifications",
            con=conn,
            index=False,
            if_exists="replace",
            chunksize=10_000,
        )
    print(f"[load] {len(df)} rows written to Snowflake → {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.certifications")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    tasks  = fetch_tasks()
    result = transform(tasks)
    push_to_snowflake(result)


if __name__ == "__main__":
    main()
