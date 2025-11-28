import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# --- Load environment variables ---
load_dotenv()
CLICKUP_API_TOKEN = os.getenv("CLICKUP_TOKEN")
SPACE_ID = os.getenv("CLICKUP_SPACE_ID")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")
SPREADSHEET_NAME = os.getenv("COMBINED_SPREADSHEET_NAME")
HARVEST_ACCOUNT_ID = os.getenv("HARVEST_ACCOUNT_ID")
HARVEST_TOKEN = os.getenv("HARVEST_TOKEN")

CLICKUP_HEADERS = {"Authorization": CLICKUP_API_TOKEN}
HARVEST_HEADERS = {
    "Authorization": f"Bearer {HARVEST_TOKEN}",
    "Harvest-Account-ID": HARVEST_ACCOUNT_ID,
    "User-Agent": "IntegrationScript"
}

# --- Initialize Google Sheets client ---
def init_google_sheet():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_JSON, scope)
    client = gspread.authorize(creds)
    return client.open(SPREADSHEET_NAME)

# --- ClickUp functions ---
def get_folders(space_id):
    return requests.get(f"https://api.clickup.com/api/v2/space/{space_id}/folder", headers=CLICKUP_HEADERS).json().get("folders", [])

def get_lists_in_folder(folder_id):
    return requests.get(f"https://api.clickup.com/api/v2/folder/{folder_id}/list", headers=CLICKUP_HEADERS).json().get("lists", [])

def get_lists_directly_in_space(space_id):
    return requests.get(f"https://api.clickup.com/api/v2/space/{space_id}/list", headers=CLICKUP_HEADERS).json().get("lists", [])

def get_tasks(list_id):
    return requests.get(f"https://api.clickup.com/api/v2/list/{list_id}/task", headers=CLICKUP_HEADERS).json().get("tasks", [])

# --- Harvest functions ---
def get_harvest_entries():
    start_date = get_week_dates()[0].isoformat()
    end_date = get_week_dates()[-1].isoformat()
    url = f"https://api.harvestapp.com/v2/time_entries?from={start_date}&to={end_date}"
    response = requests.get(url, headers=HARVEST_HEADERS)
    entries = response.json().get("time_entries", [])

    formatted = []
    for e in entries:
        entry_date = datetime.fromisoformat(e["spent_date"])
        formatted.append({
            "name": e.get("notes", "No description"),
            "status": "Logged",
            "due_date": e["spent_date"],
            "assignees": e["user"]["name"],
            "project": e["project"]["name"],
            "sheet_date": entry_date.date()
        })
    return formatted

# --- Get current working week dates (Mon-Fri) ---
def get_week_dates():
    today = datetime.now(timezone.utc).date()
    monday = today - timedelta(days=today.weekday())
    return [monday + timedelta(days=i) for i in range(5)]

# --- Filter and format ClickUp tasks ---
def filter_clickup_tasks(all_tasks, statuses):
    today = datetime.now(timezone.utc).date()
    filtered = []
    weekdays = get_week_dates()

    for task in all_tasks:
        task_status = task.get("status", {}).get("status", "").upper()
        if task_status not in statuses:
            continue

        due_timestamp = task.get("due_date")
        if due_timestamp:
            due_date = datetime.fromtimestamp(int(due_timestamp)/1000, tz=timezone.utc).date()
            if due_date < today:
                continue
            due_date_str = due_date.isoformat()
        else:
            due_date_str = "No due date"

        assignees = ", ".join([a.get("username", "") for a in task.get("assignees", [])]) or "Unassigned"

        folder = task.get("folder")
        lst = task.get("list")
        project_name = (folder["name"] if folder else (lst["name"] if lst else "Unknown"))

        if project_name in {"Certification", "Product Management"}:
            continue

        for day in weekdays:
            filtered.append({
                "name": task['name'],
                "status": task_status,
                "due_date": due_date_str,
                "assignees": assignees,
                "project": project_name,
                "sheet_date": day
            })

    return filtered

# --- Write to Google Sheets ---
def write_tasks_to_sheets(sheet, tasks):
    weekdays = get_week_dates()
    headers = ["Task Name", "Status", "Due Date", "Assignees", "Project/Folder"]

    for day in weekdays:
        sheet_name = day.strftime("%A")
        try:
            ws = sheet.worksheet(sheet_name)
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws = sheet.add_worksheet(title=sheet_name, rows="100", cols="20")

        rows_to_write = [headers] + [
            [t['name'], t['status'], t['due_date'], t['assignees'], t['project']]
            for t in tasks if t['sheet_date'] == day
        ]

        if rows_to_write:
            ws.update(values=rows_to_write, range_name=f"A1:E{len(rows_to_write)}")

# --- Main ---
def main():
    sheet = init_google_sheet()

    # --- ClickUp ---
    all_tasks_clickup = []
    for folder in get_folders(SPACE_ID):
        for lst in get_lists_in_folder(folder["id"]):
            tasks = get_tasks(lst["id"])
            for t in tasks:
                t["folder"] = folder
            all_tasks_clickup.extend(tasks)

    for lst in get_lists_directly_in_space(SPACE_ID):
        tasks = get_tasks(lst["id"])
        for t in tasks:
            t["folder"] = None
            t["list"] = lst
        all_tasks_clickup.extend(tasks)

    filtered_clickup = filter_clickup_tasks(all_tasks_clickup, statuses={"IN PROGRESS", "REVIEW"})

    # --- Harvest ---
    harvest_entries = get_harvest_entries()

    # --- Merge and write ---
    combined_tasks = filtered_clickup + harvest_entries
    write_tasks_to_sheets(sheet, combined_tasks)

    print("ClickUp & Harvest data written to Google Sheets (Mon–Fri).")

if __name__ == "__main__":
    main()
