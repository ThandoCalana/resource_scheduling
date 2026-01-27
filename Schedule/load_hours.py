import pandas as pd
from openpyxl import load_workbook
from datetime import datetime

# ----------------- User Settings -----------------
SOURCE_FILE = "Three_Month_Team_Schedule.xlsx"
OUTPUT_FILE = "Aggregated_Hours.xlsx"

# Dynamic reporting period
REPORT_START = datetime(2026, 1, 1).date()
REPORT_END   = datetime(2026, 3, 31).date()
# -------------------------------------------------

# Load workbook and sheet names
wb = load_workbook(SOURCE_FILE, data_only=True)
day_sheets = [s for s in wb.sheetnames if s not in ["Names", "Daily Loads", "Monthly Averages"]]

# --- Extract team member names from first sheet row 1, column B onward ---
first_sheet = wb[day_sheets[0]]
team_members = []
col = 2
while first_sheet.cell(row=1, column=col).value:
    team_members.append(first_sheet.cell(row=1, column=col).value)
    col += 1

team_members_df = pd.DataFrame({"Team Member": team_members})

# --- Collect logs (weekdays only) ---
all_logs = []

for sheet_name in day_sheets:
    # Read sheet
    df = pd.read_excel(SOURCE_FILE, sheet_name=sheet_name, header=None)
    
    # Extract date from sheet name
    date_part = sheet_name.split(" ")[-1]
    try:
        sheet_date = datetime.fromisoformat(date_part).date()
    except:
        continue

    # Skip dates outside reporting period
    if sheet_date < REPORT_START or sheet_date > REPORT_END:
        continue

    # Skip weekends (Saturday=5, Sunday=6)
    if sheet_date.weekday() >= 5:
        continue

    # Rows 2-21 (30-min slots)
    time_data = df.iloc[1:21, 1:]
    existing_cols = df.iloc[0, 1:].tolist()
    
    # Keep only columns that match team_members and exist in this sheet
    cols_to_use = [col for col in team_members if col in existing_cols]
    time_data_filtered = pd.DataFrame({col: time_data.iloc[:, existing_cols.index(col)] for col in cols_to_use})

    # Calculate hours: 0.5 per non-empty slot
    hours_logged = (time_data_filtered.notna().astype(int) * 0.5).sum()

    # Append to logs
    temp_df = pd.DataFrame({
        "Team Member": hours_logged.index,
        "Hours": hours_logged.values,
        "Date": pd.to_datetime(sheet_date)  # ensure datetime64[ns]
    })
    all_logs.append(temp_df)

# Combine all logs into one DataFrame
full_log = pd.concat(all_logs)

# --- Sheet 2: Daily Loads ---
daily_pivot = full_log.pivot(index="Date", columns="Team Member", values="Hours").fillna(0)
daily_pivot = daily_pivot.reindex(columns=team_members, fill_value=0).sort_index()

# --- Sheet 3: Monthly Aggregation ---
monthly_agg = full_log.groupby([
    "Team Member",
    pd.Grouper(key="Date", freq="ME")
])["Hours"].sum().reset_index()

monthly_pivot = monthly_agg.pivot(index="Team Member", columns="Date", values="Hours").fillna(0)
monthly_pivot.columns = [d.strftime("%B %Y") for d in monthly_pivot.columns]
monthly_pivot = monthly_pivot.reindex(index=team_members, fill_value=0).sort_index(axis=1)

# --- Write to Excel (3 sheets only) ---
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    team_members_df.to_excel(writer, sheet_name="Names", index=False)
    daily_pivot.to_excel(writer, sheet_name="Daily Loads")
    monthly_pivot.to_excel(writer, sheet_name="Monthly Aggregation")

print(f"Aggregated report saved to {OUTPUT_FILE} (weekends excluded)")
