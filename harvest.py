import requests
import datetime
from dotenv import load_dotenv
import os
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# --- CONFIG ---
load_dotenv()

GOOGLE_CREDS_JSON = os.getenv('GOOGLE_CREDS_JSON')
SPREADSHEET_NAME = os.getenv('HARVEST_SPREADSHEET_NAME')
HARVEST_ACCOUNT_ID = os.getenv('HARVEST_ACCOUNT_ID')
HARVEST_TOKEN = os.getenv('HARVEST_TOKEN')


# --- Initialize Google Sheets client ---
def init_google_sheet(spreadsheet_name):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_JSON, scope)
    client = gspread.authorize(creds)
    return client.open(spreadsheet_name)

# --- Get current working week dates (Monday to Friday) ---
def get_week_dates():
    today = datetime.date.today()
    start = today - datetime.timedelta(days=today.weekday())  # Monday
    return [(start + datetime.timedelta(days=i)) for i in range(5)]

# --- Fetch Harvest time entries for the current week ---
def fetch_harvest_entries(account_id, token, week_dates):
    headers = {
        "Harvest-Account-Id": account_id,
        "Authorization": f"Bearer {token}",
        "User-Agent": "Harvest Weekly Tracker"
    }
    start = week_dates[0].strftime("%Y-%m-%d")
    end = week_dates[-1].strftime("%Y-%m-%d")
    url = f"https://api.harvestapp.com/v2/time_entries?from={start}&to={end}"

    response = requests.get(url, headers=headers)
    tasks = []

    if response.ok:
        for entry in response.json().get("time_entries", []):
            task_date_str = entry.get("spent_date")
            # Convert string to date object for comparison if needed
            task_date = datetime.datetime.strptime(task_date_str, "%Y-%m-%d").date() if task_date_str else None

            # Build a descriptive name (notes or task name fallback)
            name = entry.get("notes") or entry.get("task", {}).get("name", "No Name")

            # Harvest user who logged the time
            assignee = entry.get("user", {}).get("name", "")

            if task_date in week_dates:
                tasks.append({
                    "name": name,
                    "assignees": assignee,
                    "due_date": task_date,
                    "status": "Logged",
                    "priority": "",
                    "platform": "Harvest",
                    "url": ""  # Harvest API does not provide direct URL for time entries
                })
    else:
        print(f"Error fetching Harvest entries: {response.status_code} - {response.text}")

    return tasks

# --- Write tasks to sheets by due date ---
def write_tasks_to_sheets(sheet, tasks, week_dates):
    worksheets = {}
    for date in week_dates:
        date_str = date.strftime("%Y-%m-%d")
        try:
            ws = sheet.worksheet(date_str)
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws = sheet.add_worksheet(title=date_str, rows="100", cols="20")
        worksheets[date_str] = ws
        ws.append_row(["Task Name", "Assignees", "Due Date", "Status", "Priority", "Platform", "URL"])

    for task in tasks:
        due = task["due_date"]
        if due and due.strftime("%Y-%m-%d") in worksheets:
            ws = worksheets[due.strftime("%Y-%m-%d")]
            ws.append_row([
                task["name"],
                task["assignees"],
                due.strftime("%Y-%m-%d"),
                task["status"],
                task["priority"],
                task["platform"],
                task["url"]
            ])

# --- MAIN ---
def main():
    sheet = init_google_sheet(SPREADSHEET_NAME)
    week_dates = get_week_dates()
    harvest_tasks = fetch_harvest_entries(HARVEST_ACCOUNT_ID, HARVEST_TOKEN, week_dates)
    write_tasks_to_sheets(sheet, harvest_tasks, week_dates)
    print("Harvest tasks written to weekly sheets!")

if __name__ == "__main__":
    main()

