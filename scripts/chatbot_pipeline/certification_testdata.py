from __future__ import annotations

import os
from pathlib import Path
from typing import Any
import datetime as dt

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


CLICKUP_API_TOKEN = get_required_env("CLICKUP_API_TOKEN")
CLICKUP_LIST_ID = get_required_env("CLICKUP_LIST_ID")
BASE_URL = f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task"
PARAMS = {"subtasks": "true"}

START_DATE = dt.date(2023, 1, 1)
END_DATE = dt.date(2026, 12, 31)
OUTPUT_DIR = Path("outputs")
OUTPUT_FILE = OUTPUT_DIR / "certifications.csv"

# Replace this later with a proper reference file if you have one.
RESIGNED_NAMES = pd.DataFrame(
    {
        "Name": [
            "Jenny Wrench",
            "Lynn Carelse",
            "Carl Brink",
        ]
    }
)


def epoch_ms_to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, unit="ms", errors="coerce")


def epoch_ms_to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, unit="ms", errors="coerce").dt.normalize()


def clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    text_cols = out.select_dtypes(include=["object", "string"]).columns
    for col in text_cols:
        out[col] = out[col].fillna("").astype(str).str.strip()
    return out


def first_or_none(value: Any) -> Any:
    if isinstance(value, list) and value:
        return value[0]
    return None


def fetch_clickup_tasks(api_token: str, base_url: str, params: dict[str, str]) -> list[dict[str, Any]]:
    headers = {"Authorization": api_token}
    response = requests.get(base_url, headers=headers, params=params, timeout=60)
    response.raise_for_status()
    payload = response.json()
    return payload.get("tasks", [])


def extract_custom_field_value(task: dict[str, Any], field_index: int = 1) -> Any:
    custom_fields = task.get("custom_fields", []) or []
    if len(custom_fields) > field_index:
        return custom_fields[field_index].get("value")
    return None


def normalize_tasks(tasks: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for task in tasks:
        assignees = task.get("assignees", []) or []
        tags = task.get("tags", []) or []

        first_assignee = first_or_none(assignees)
        first_tag = first_or_none(tags)

        rows.append(
            {
                "Name": first_assignee.get("username") if first_assignee else None,
                "Expire Date": extract_custom_field_value(task, 1),
                "date_created": task.get("date_created"),
                "Update Date": task.get("date_updated"),
                "due_date": task.get("due_date"),
                "Certification Name": task.get("name"),
                "Status": (task.get("status") or {}).get("status"),
                "Tag": first_tag.get("name") if first_tag else None,
            }
        )

    return pd.DataFrame(rows)


def apply_employment_status(df: pd.DataFrame, resigned_df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    resigned_set = set(resigned_df["Name"].dropna().astype(str).str.strip())
    out["Employment Status"] = np.where(
        out["Name"].fillna("").astype(str).str.strip().isin(resigned_set),
        "Resigned",
        "Active",
    )
    return out


def build_completed_branch(df: pd.DataFrame) -> pd.DataFrame:
    completed = df.copy()
    completed = completed[(completed["Name"].notna()) | (completed["due_date"].notna())]
    completed = completed[completed["Status"].eq("Done")]
    completed = completed[completed["Tag"].eq("cert")]

    completed["Record Date"] = completed["date_created"].dt.normalize()
    completed["Last Updated"] = completed["Update Date"].dt.normalize()
    completed["Expiration Date"] = np.where(
        completed["Expire Date"].isna(),
        pd.Timestamp.today().normalize() + pd.DateOffset(years=10),
        completed["Expire Date"],
    )
    completed["Expiration Date"] = pd.to_datetime(completed["Expiration Date"], errors="coerce").dt.normalize()
    completed["Status"] = np.where(
        pd.Timestamp.today().normalize() > completed["Expiration Date"],
        "Expired",
        "Active",
    )
    completed["Latest Flag"] = "1"

    max_record = (
        completed.groupby(["Name", "Certification Name"], dropna=False)["Record Date"]
        .max()
        .rename("Max_Record_Date")
        .reset_index()
    )
    completed = completed.merge(max_record, on=["Name", "Certification Name"], how="left")
    completed["Latest Flag"] = np.where(completed["Record Date"].eq(completed["Max_Record_Date"]), "1", "0")
    completed = completed.drop(columns=["Max_Record_Date"])

    return completed[
        [
            "Name",
            "Record Date",
            "Last Updated",
            "Latest Flag",
            "Certification Name",
            "Expiration Date",
            "Status",
            "Employment Status",
        ]
    ].copy()



def build_planned_branch(df: pd.DataFrame) -> pd.DataFrame:
    planned = df.copy()
    planned = planned[(planned["Name"].notna()) | (planned["due_date"].notna())]
    planned = planned[planned["Status"].ne("Done")]
    planned = planned[planned["Tag"].eq("cert")]

    planned["Planned Exam Date"] = planned["due_date"]
    planned["Record Date"] = planned["date_created"].dt.normalize()
    planned["Last Updated"] = planned["Update Date"].dt.normalize()
    planned["Status"] = "Planned"
    planned["Latest Flag"] = "1"

    return planned[
        [
            "Name",
            "Record Date",
            "Last Updated",
            "Latest Flag",
            "Certification Name",
            "Planned Exam Date",
            "Status",
            "Employment Status",
        ]
    ].copy()



def standardise_certification_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Certification Name"] = out["Certification Name"].replace(
        {
            "Tableau Desktop Specialist (no expiry date)": "Tableau Desktop Specialist",
            "DESIGN KIT: HUMAN-CENTERED DESIGN": "Design Kit: Human-Centered Design",
            "DESIGN KIT: PROTOTYPING": "Design Kit: Prototyping",
            "AWS Certified Cloud Practitioner": "AWS Cloud Practitioner",
        }
    )

    out.loc[
        out["Certification Name"].fillna("").str.contains("SnowPro Core", case=False, na=False),
        "Certification Name",
    ] = "SnowPro Core"

    cert_name = out["Certification Name"].fillna("")
    out["Technology"] = np.select(
        [
            cert_name.str.contains("Tableau", case=False, na=False),
            cert_name.str.contains("Alteryx", case=False, na=False),
            cert_name.str.contains("Google", case=False, na=False),
            cert_name.str.contains("AWS", case=False, na=False),
            cert_name.str.contains("Snowflake", case=False, na=False),
            cert_name.str.contains("SnowPro", case=False, na=False),
            cert_name.str.contains("wherescape", case=False, na=False),
            cert_name.str.contains("Trifacta", case=False, na=False),
            cert_name.str.contains("Data Vault", case=False, na=False),
            cert_name.str.contains("Design Kit", case=False, na=False),
            cert_name.str.contains("Salesforce", case=False, na=False),
            cert_name.str.contains("Matillion", case=False, na=False),
        ],
        [
            "Salesforce",
            "Alteryx",
            "Google",
            "AWS",
            "Snowflake",
            "Snowflake",
            "Wherescape",
            "Alteryx",
            "Data Vault",
            "Design Kit",
            "Salesforce",
            "Matillion",
        ],
        default="Other",
    )

    name_col = out["Name"].fillna("") if "Name" in out.columns else pd.Series(dtype=str)
    if not name_col.empty:
        out.loc[name_col.str.contains("Thembani", case=False, na=False), "Name"] = "Thembani Faleni"
        out.loc[name_col.str.contains("Flora", case=False, na=False), "Name"] = "Flora Kundaeli"

    return out



def classify_hierarchy(cert: str) -> str:
    cert_lower = str(cert).lower()
    if "practitioner" in cert_lower:
        return "Entry"
    if "core" in cert_lower:
        return "Core"
    if "associate" in cert_lower or "developer 1" in cert_lower:
        return "Associate"
    if "specialist" in cert_lower:
        return "Specialist"
    if "advanced" in cert_lower or "advance" in cert_lower:
        return "Advanced"
    if "professional" in cert_lower:
        return "Professional"
    if "administration" in cert_lower or "administrator" in cert_lower:
        return "Administrator"
    if cert_lower in ["aws", "snowflake", "salesforce", "alteryx", "pyspark", "data cloud"]:
        return "Technology Only"
    if "study path" in cert_lower or "learning path" in cert_lower or "find out which certs" in cert_lower:
        return "Planning / Research"
    if cert_lower == "other":
        return "Other"
    return "Unspecified"



def prepare_glossary(glossary_df: pd.DataFrame) -> pd.DataFrame:
    out = glossary_df.copy()

    for col, default in {
        "Prerequisites": "",
        "Cost": "0",
        "Currency": "USD",
        "Validity (Years)": pd.NA,
        "Partner": 0,
    }.items():
        if col not in out.columns:
            out[col] = default

    out["Cost"] = out["Cost"].fillna("").astype(str).str.replace("$", "", regex=False)
    out["Currency"] = "USD"
    out["Cost"] = np.where(out["Cost"].isin(["Free", ""]), "0", out["Cost"])
    out["Partner"] = pd.to_numeric(out["Partner"], errors="coerce").fillna(0).astype(int)
    out["Cost"] = pd.to_numeric(out["Cost"], errors="coerce").fillna(0.0)
    out["Validity (Years)"] = pd.to_numeric(out["Validity (Years)"], errors="coerce")

    return out[
        [
            "Certification Name",
            "Technology",
            "Prerequisites",
            "Cost",
            "Currency",
            "Validity (Years)",
            "Partner",
        ]
    ].copy()



def build_date_spine(start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    return pd.DataFrame({"Date": pd.date_range(start=start_date, end=end_date, freq="D")})



def expand_by_date(base_df: pd.DataFrame, date_df: pd.DataFrame) -> pd.DataFrame:
    left = base_df.copy()
    right = date_df.copy()
    left["__key"] = 1
    right["__key"] = 1
    return left.merge(right, on="__key", how="outer").drop(columns="__key")



def recompute_status_over_time(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["Status"] = np.select(
        [
            (out["Expiration Date"] > out["Date"]) & (out["Planned Exam Date"].isna()),
            (out["Expiration Date"] <= out["Date"]) & (out["Planned Exam Date"].isna()),
            (out["Planned Exam Date"].notna()) & (out["Planned Exam Date"] > out["Date"]),
            (out["Planned Exam Date"].notna()) & (out["Planned Exam Date"] <= out["Date"]),
        ],
        [
            "Active",
            "Expired",
            "Planned",
            "Planned - Past Exam Date",
        ],
        default=out["Status"],
    )

    out["Status"] = np.where(
        (out["Status"] == "Planned") & (out["Planned Exam Date"].isna()),
        "Planned - No Exam Date",
        out["Status"],
    )

    out["Expiration Date"] = np.where(
        out["Status"].astype(str).str.contains("Planned", na=False),
        out["Planned Exam Date"] + pd.to_timedelta((out["Validity (Years)"].fillna(0) * 365.25), unit="D"),
        out["Expiration Date"],
    )
    out["Expiration Date"] = pd.to_datetime(out["Expiration Date"], errors="coerce").dt.normalize()

    out["Expiration Date Indicator"] = np.where(
        out["Status"].astype(str).str.contains("Planned", na=False),
        "Projected",
        "Actual",
    )
    out["Expiration Date Indicator"] = np.where(
        out["Expiration Date"].isna(), pd.NA, out["Expiration Date Indicator"]
    )

    out["Status"] = np.where(
        out["Status"].astype(str).str.contains("Planned", na=False) & (out["Date"] >= out["Planned Exam Date"]),
        "Projected Active",
        out["Status"],
    )
    out["Status"] = np.where(
        (out["Status"] == "Projected Active") & (out["Date"] >= out["Expiration Date"]),
        "Projected Expired",
        out["Status"],
    )

    return out



def finalise_output(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Cost ($)"] = out["Cost"]
    keep = [
        "Date",
        "Name",
        "Certification Name",
        "Technology",
        "Status",
        "Expiration Date",
        "Expiration Date Indicator",
        "Planned Exam Date",
        "Validity (Years)",
        "Cost ($)",
        "Record Date",
        "Last Updated",
        "Latest Flag",
        "Employment Status",
        "Partner",
    ]
    existing = [c for c in keep if c in out.columns]
    out = out[existing].copy()
    return out.sort_values(
        by=["Date", "Name", "Status", "Expiration Date", "Planned Exam Date"],
        ascending=[True, True, True, True, True],
        na_position="last",
    ).reset_index(drop=True)



def build_glossary_from_tasks(tasks: list[dict[str, Any]]) -> pd.DataFrame:
    if not tasks:
        return pd.DataFrame(columns=["Certification Name", "Technology", "Prerequisites", "Cost", "Currency", "Validity (Years)", "Partner"])

    df_api = pd.json_normalize(tasks)
    if "name" in df_api.columns and "Certification Name" not in df_api.columns:
        df_api = df_api.rename(columns={"name": "Certification Name"})
    if "Name" not in df_api.columns:
        df_api["Name"] = ""

    df_api = standardise_certification_fields(df_api)
    return prepare_glossary(df_api)



def update_expiry_by_highest_hierarchy(final_df: pd.DataFrame) -> pd.DataFrame:
    out = final_df.copy()
    out["hierarchy"] = out["Certification Name"].apply(classify_hierarchy)

    temp = pd.DataFrame(
        {
            "employee_name": out["Name"],
            "certification": out["Certification Name"],
            "obtained_date": pd.to_datetime(out["Record Date"], errors="coerce"),
            "technology": out["Technology"],
            "hierarchy": out["hierarchy"],
            "Date": out["Date"],
        }
    )
    temp["expiry_date"] = temp["obtained_date"] + pd.DateOffset(years=2)

    hierarchy_rank = {
        "Technology Only": 0,
        "Entry": 1,
        "Core": 2,
        "Associate": 3,
        "Administrator": 4,
        "Specialist": 5,
        "Advanced": 6,
        "Professional": 7,
        "Other": -1,
        "Planning / Research": -1,
        "Unspecified": -1,
    }
    temp["hierarchy_rank"] = temp["hierarchy"].map(hierarchy_rank).fillna(-1)

    sorted_temp = temp.sort_values(
        by=["employee_name", "technology", "hierarchy_rank", "obtained_date"],
        ascending=[True, True, False, False],
    )

    highest_cert = (
        sorted_temp.groupby(["employee_name", "technology"], as_index=False)
        .first()[["employee_name", "technology", "expiry_date"]]
        .rename(columns={"expiry_date": "group_expiry_date"})
    )

    temp = temp.merge(highest_cert, on=["employee_name", "technology"], how="left")
    temp["expiry_date"] = temp["group_expiry_date"]

    merge_cols = temp[["Date", "employee_name", "certification", "hierarchy", "expiry_date"]].copy()
    merge_cols = merge_cols.rename(columns={"hierarchy": "hierarchy_temp", "expiry_date": "expiry_date_temp"})

    if "Expiration Date" in out.columns:
        out = out.drop(columns=["Expiration Date"])

    out = out.merge(
        merge_cols,
        left_on=["Date", "Name", "Certification Name"],
        right_on=["Date", "employee_name", "certification"],
        how="left",
    )

    out["hierarchy"] = out["hierarchy_temp"]
    out["Expiration Date"] = out["expiry_date_temp"]
    out = out.drop(columns=["employee_name", "certification", "hierarchy_temp", "expiry_date_temp"])
    return out



def push_to_snowflake(df: pd.DataFrame) -> None:
    account = get_required_env("SNOWFLAKE_ACCOUNT")
    user = get_required_env("SNOWFLAKE_USER")
    password = get_required_env("SNOWFLAKE_PASSWORD")
    role = get_required_env("SNOWFLAKE_ROLE")
    warehouse = get_required_env("SNOWFLAKE_WAREHOUSE")
    database = get_required_env("SNOWFLAKE_DATABASE")
    schema = get_required_env("SNOWFLAKE_SCHEMA")

    conn_str = (
        f"snowflake://{user}:{password}@{account}/{database}/{schema}"
        f"?warehouse={warehouse}&role={role}"
    )

    engine = create_engine(conn_str)
    with engine.connect() as conn:
        df.to_sql("certifications", con=conn, index=False, if_exists="replace", chunksize=10000)



def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)

    tasks = fetch_clickup_tasks(CLICKUP_API_TOKEN, BASE_URL, PARAMS)
    print(f"Tasks fetched: {len(tasks)}")

    raw_df = normalize_tasks(tasks)
    raw_df = apply_employment_status(raw_df, RESIGNED_NAMES)
    raw_df = clean_text_columns(raw_df)

    raw_df["Expire Date"] = epoch_ms_to_date(raw_df["Expire Date"])
    raw_df["date_created"] = epoch_ms_to_datetime(raw_df["date_created"])
    raw_df["Update Date"] = epoch_ms_to_datetime(raw_df["Update Date"])
    raw_df["due_date"] = epoch_ms_to_date(raw_df["due_date"])

    completed_df = build_completed_branch(raw_df)
    planned_df = build_planned_branch(raw_df)

    base_cert_df = pd.concat([completed_df, planned_df], ignore_index=True, sort=False)
    base_cert_df = standardise_certification_fields(base_cert_df)
    base_cert_df = base_cert_df[
        base_cert_df["Certification Name"].notna()
        & (base_cert_df["Certification Name"].astype(str).str.strip() != "")
    ]

    glossary_prepared = build_glossary_from_tasks(tasks)

    matched = base_cert_df.merge(
        glossary_prepared,
        on=["Technology", "Certification Name"],
        how="left",
    )

    date_spine = build_date_spine(START_DATE, END_DATE)
    expanded = expand_by_date(matched, date_spine)

    final_df = recompute_status_over_time(expanded)
    final_df = finalise_output(final_df)
    final_df = update_expiry_by_highest_hierarchy(final_df)

    final_df["Status"] = final_df["Status"].replace("Projected Expired", "Active")
    final_df = final_df[final_df["Status"] != "Planned - No Exam Date"]

    desired_df = final_df.drop_duplicates(subset=["Name", "Technology"])
    desired_df = desired_df[
        [
            "Name",
            "Certification Name",
            "Technology",
            "Status",
            "Employment Status",
            "Expiration Date",
            "Record Date",
        ]
    ].copy()

    desired_df.to_csv(OUTPUT_FILE, index=False)
    print(f"CSV written to {OUTPUT_FILE}")

    push_to_snowflake(desired_df)
    print("Snowflake load completed.")


if __name__ == "__main__":
    main()
