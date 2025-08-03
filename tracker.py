import requests
import datetime
import os
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# --- CONFIG ---
load_dotenv()
CLICKUP_API_KEY = os.getenv('CLICKUP_API_TOKEN')
CLICKUP_TEAM_ID = os.getenv('CLICKUP_TEAM_ID')
SPREADSHEET_NAME = os.getenv('COMBINED_SPREADSHEET_NAME')
GOOGLE_CREDS_JSON = os.getenv('GOOGLE_CREDS_JSON')
HARVEST_ACCOUNT_ID = os.getenv('HARVEST_ACCOUNT_ID')
HARVEST_TOKEN = os.getenv('HARVEST_TOKEN')

# --- Google Sheets ---
def init_google_sheet(spreadsheet_name):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_JSON, scope)
    client = gspread.authorize(creds)
    return client.open(spreadsheet_name)

# --- Date Utilities ---
def get_week_dates():
    today = datetime.date.today()
    start = today - datetime.timedelta(days=today.weekday())  # Monday
    return [(start + datetime.timedelta(days=i)) for i in range(5)]

# --- ClickUp Tasks ---
def fetch_clickup_tasks(api_key, team_id):
    headers = {"Authorization": api_key}
    url = f"https://api.clickup.com/api/v2/team/{team_id}/task"
    response = requests.get(url, headers=headers, params={"archived": "false"})
    tasks = []

    if not response.ok:
        print(f"Error fetching ClickUp tasks: {response.status_code} - {response.text}")
        return tasks

    week_dates = set(get_week_dates())
    for task in response.json().get("tasks", []):
        due_ts = task.get("due_date")
        due_date = datetime.datetime.fromtimestamp(int(due_ts)/1000, datetime.UTC).date() if due_ts else None
        status = task.get("status", {}).get("status", "").upper()

        include = False
        if due_date and due_date in week_dates:
            include = True
        elif not due_date and status == "IN PROGRESS":
            include = True

        if include:
            tasks.append({
                "name": task.get("name", ""),
                "due_date": due_date,
                "assignees": ", ".join([a.get("username", "") for a in task.get("assignees", [])]),
                "status": status,
                "priority": task.get("priority", {}).get("priority", "") if task.get("priority") else "",
                "platform": "ClickUp",
                "url": task.get("url", "")
            })
    return tasks

# --- Harvest Tasks ---
def fetch_harvest_entries(account_id, token, week_dates):
    headers = {
        "Harvest-Account-Id": account_id,
        "Authorization": f"Bearer {token}",
        "User-Agent": "Harvest Weekly Tracker"
    }
    url = f"https://api.harvestapp.com/v2/time_entries?from={week_dates[0]}&to={week_dates[-1]}"
    response = requests.get(url, headers=headers)
    tasks = []

    if not response.ok:
        print(f"Error fetching Harvest entries: {response.status_code} - {response.text}")
        return tasks

    for entry in response.json().get("time_entries", []):
        date_str = entry.get("spent_date")
        task_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else None

        if task_date in week_dates:
            tasks.append({
                "name": entry.get("notes") or entry.get("task", {}).get("name", "No Name"),
                "assignees": entry.get("user", {}).get("name", ""),
                "due_date": task_date,
                "status": "Logged",
                "priority": "",
                "platform": "Harvest",
                "url": ""
            })
    return tasks

# --- Write Tasks ---
def write_tasks_to_sheets(sheet, tasks, week_dates):
    worksheets = {}
    headers = ["Task Name", "Assignees", "Due Date", "Status", "Priority", "Platform", "URL"]

    for date in week_dates:
        title = date.strftime("%Y-%m-%d")
        try:
            ws = sheet.worksheet(title)
        except gspread.exceptions.WorksheetNotFound:
            ws = sheet.add_worksheet(title=title, rows="100", cols="20")
            ws.append_row(headers)
        worksheets[title] = ws

    try:
        no_due_ws = sheet.worksheet("No Due Date - In Progress")
    except gspread.exceptions.WorksheetNotFound:
        no_due_ws = sheet.add_worksheet(title="No Due Date - In Progress", rows="100", cols="20")
        no_due_ws.append_row(headers)

    for task in tasks:
        due_date = task["due_date"]
        row = [
            task["name"], task["assignees"],
            due_date.strftime("%Y-%m-%d") if due_date else "",
            task["status"], task["priority"],
            task["platform"], task["url"]
        ]
        if due_date:
            date_str = due_date.strftime("%Y-%m-%d")
            if date_str in worksheets:
                worksheets[date_str].append_row(row)
        else:
            no_due_ws.append_row(row)

# --- Main ---
def main():
    sheet = init_google_sheet(SPREADSHEET_NAME)
    week_dates = get_week_dates()
    clickup_tasks = fetch_clickup_tasks(CLICKUP_API_KEY, CLICKUP_TEAM_ID)
    harvest_tasks = fetch_harvest_entries(HARVEST_ACCOUNT_ID, HARVEST_TOKEN, week_dates)
    all_tasks = clickup_tasks + harvest_tasks
    write_tasks_to_sheets(sheet, all_tasks, week_dates)
    print("All tasks written to weekly sheets!")

if __name__ == "__main__":
    main()
