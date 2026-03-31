import pandas as pd
from datetime import datetime

# ----------------- User Settings -----------------
SOURCE_FILE = "./Three_Month_Team_Schedule.xlsx"
OUTPUT_FILE = "./Aggregated_Hours.xlsx"

REPORT_START = datetime(2026, 1, 1).date()
REPORT_END   = datetime(2026, 5, 31).date()
# -------------------------------------------------

# ---------------- Load Data ----------------
df = pd.read_csv(SOURCE_FILE)

# Standardise columns
df["date"] = pd.to_datetime(df["date"]).dt.date
df["time"] = pd.to_datetime(df["time"], format="%H:%M").dt.time
df["user"] = df["user"].astype(str)
df["subject"] = df["subject"].fillna("").astype(str)
df["is_busy"] = df["is_busy"].astype(int)

# Filter date range
df = df[(df["date"] >= REPORT_START) & (df["date"] <= REPORT_END)]

# Exclude weekends
df = df[pd.to_datetime(df["date"]).dt.weekday < 5]

# ---------------- Atomic fact table ----------------
# Only keep busy slots → each = 0.5 hours
fact_df = df[df["is_busy"] == 1].copy()
fact_df["Hours"] = 0.5

fact_df.rename(columns={
    "date": "Date",
    "user": "Team Member",
    "subject": "Task"
}, inplace=True)

fact_df["Date"] = pd.to_datetime(fact_df["Date"])

team_members = sorted(fact_df["Team Member"].unique())
team_members_df = pd.DataFrame({"Team Member": team_members})

# ---------------- Sheet 2: Daily Loads ----------------
daily = (
    fact_df
    .groupby(["Date", "Team Member"])["Hours"]
    .sum()
    .reset_index()
)

daily_pivot = (
    daily
    .pivot(index="Date", columns="Team Member", values="Hours")
    .fillna(0)
    .reindex(columns=team_members, fill_value=0)
    .sort_index()
)

# ---------------- Sheet 3: Monthly Aggregation ----------------
fact_df["MonthDate"] = fact_df["Date"].dt.to_period("M").dt.to_timestamp()

monthly = (
    fact_df
    .groupby(["Team Member", "MonthDate"])["Hours"]
    .sum()
    .reset_index()
)

monthly_pivot = (
    monthly
    .pivot(index="Team Member", columns="MonthDate", values="Hours")
    .fillna(0)
    .reindex(index=team_members, fill_value=0)
    .sort_index(axis=1)
)

monthly_pivot.columns = [d.strftime("%B %Y") for d in monthly_pivot.columns]

# -------- Per-member task monthly aggregation sheets --------
member_task_monthly = (
    fact_df
    .groupby(["Team Member", "Task", "MonthDate"])["Hours"]
    .sum()
    .reset_index()
)

# ---------------- Write to Excel ----------------
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    team_members_df.to_excel(writer, sheet_name="Names", index=False)
    daily_pivot.to_excel(writer, sheet_name="Daily Loads")
    monthly_pivot.to_excel(writer, sheet_name="Monthly Aggregation")

    for member in team_members:
        member_df = member_task_monthly[
            member_task_monthly["Team Member"] == member
        ]

        if member_df.empty:
            continue

        pivot = (
            member_df
            .pivot(index="Task", columns="MonthDate", values="Hours")
            .fillna(0)
            .sort_index(axis=1)
        )

        pivot.columns = [d.strftime("%B %Y") for d in pivot.columns]

        pivot.to_excel(writer, sheet_name=member[:31])

print(f"Output written to {OUTPUT_FILE}")
