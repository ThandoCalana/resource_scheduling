import requests
import datetime
import os
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# --- CONFIG ---

load_dotenv()

CLICKUP_API_KEY = os.getenv('CLICKUP_TOKEN')
CLICKUP_TEAM_ID = os.getenv('SS_CLICKUP_TEAM_ID')
SPREADSHEET_NAME = os.getenv('CLICKUP_SPREADSHEET_NAME')
GOOGLE_CREDS_JSON = os.getenv('GOOGLE_CREDS_JSON')  

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

# --- Fetch ClickUp tasks using your logic ---
def fetch_clickup_tasks(api_key, team_id):
    headers = {"Authorization": api_key}
    url = f"https://api.clickup.com/api/v2/team/{team_id}/task"
    params = {"archived": "false"}
    response = requests.get(url, headers=headers, params=params)
    tasks = []

    if response.ok:
        for task in response.json().get("tasks", []):
            due = task.get("due_date")
            due_date = (datetime.datetime.utcfromtimestamp(int(due)/1000).date() if due else None)
            assignees = [a.get("username", "") for a in task.get("assignees", [])]
            status = task.get("status", {}).get("status", "").upper()
            priority = task.get("priority", {}).get("priority", "") if task.get("priority") else ""
            now = datetime.datetime.utcnow().date()

            # Filter logic:
            # 1) Task due date in current week (Mon-Fri)
            # 2) OR No due date AND status is IN PROGRESS AND NOT overdue (i.e. today or future)
            include_task = False
            if due_date:
                week_dates = get_week_dates()
                if due_date in week_dates:
                    include_task = True
            else:
                if status == "IN PROGRESS":
                    # Without due date, consider not overdue if today or in future (but no due date means not overdue)
                    # So just accept tasks without due date and status IN PROGRESS
                    include_task = True

            if include_task:
                tasks.append({
                    "name": task.get("name", ""),
                    "due_date": due_date,
                    "assignees": ", ".join(assignees),
                    "status": status,
                    "priority": priority,
                    "platform": "ClickUp",
                    "url": task.get("url", "")
                })
    else:
        print(f"Error fetching ClickUp tasks: {response.status_code} - {response.text}")
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

    # Special sheet for tasks with no due date but IN PROGRESS
    no_due_sheet_name = "No Due Date - In Progress"
    try:
        no_due_ws = sheet.worksheet(no_due_sheet_name)
        no_due_ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        no_due_ws = sheet.add_worksheet(title=no_due_sheet_name, rows="100", cols="20")
    no_due_ws.append_row(["Task Name", "Assignees", "Due Date", "Status", "Priority", "Platform", "URL"])

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
        else:
            # Tasks without due date but included (IN PROGRESS)
            no_due_ws.append_row([
                task["name"],
                task["assignees"],
                "",  # No due date
                task["status"],
                task["priority"],
                task["platform"],
                task["url"]
            ])

# --- MAIN ---
def main():
    sheet = init_google_sheet(SPREADSHEET_NAME)
    week_dates = get_week_dates()
    clickup_tasks = fetch_clickup_tasks(CLICKUP_API_KEY, CLICKUP_TEAM_ID)
    write_tasks_to_sheets(sheet, clickup_tasks, week_dates)
    print("ClickUp tasks written to weekly sheets!")

if __name__ == "__main__":
    main()
