import pandas as pd
from datetime import datetime

SOURCE_FILE = "./data/calendar_flat.csv"
OUTPUT_FILE = "./data/Aggregated_Hours.xlsx"

df = pd.read_csv(SOURCE_FILE)

# --- Clean + standardize ---
df["date"] = pd.to_datetime(df["date"]).dt.date
df["time"] = pd.to_datetime(df["time"], format="%H:%M").dt.time
df["user"] = df["user"].astype(str)
df["subject"] = df["subject"].fillna("").astype(str)
df["is_busy"] = df["is_busy"].astype(int)

# --- Filter ---
df = df[(df["date"] >= df["date"].min()) & (df["date"] <= df["date"].max())]
df = df[pd.to_datetime(df["date"]).dt.weekday < 5]

# --- Fact table ---
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

# --- Daily ---
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

# --- Monthly ---
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

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    # --- Core sheets ---
    team_members_df.to_excel(writer, sheet_name="Names", index=False)
    daily_pivot.to_excel(writer, sheet_name="Daily Loads")
    monthly_pivot.to_excel(writer, sheet_name="Monthly Aggregation")

    # ------------------------------
    # Per - person drill down
    # ------------------------------
    for member in team_members:
        member_df = fact_df[fact_df["Team Member"] == member]

        if member_df.empty:
            continue

        # Aggregate total hours per task
        task_summary = (
            member_df
            .groupby("Task")["Hours"]
            .sum()
            .reset_index()
            .sort_values(by="Hours", ascending=False)
        )

        # Optional: clean blank subjects
        task_summary["Task"] = task_summary["Task"].replace("", "No Subject")
        
        sheet_name = member[:31]

        task_summary.to_excel(writer, sheet_name=sheet_name, index=False)

<<<<<<< HEAD
print(f"Output written → {OUTPUT_FILE}")
=======
print(f"Output written to {OUTPUT_FILE}")
>>>>>>> f019d005e0522b3738dc9ec5334affe7e5b26e97
