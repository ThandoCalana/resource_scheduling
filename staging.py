import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from gspread import Cell

# --- Load environment variables ---
load_dotenv()
CLICKUP_API_TOKEN = os.getenv("CLICKUP_TOKEN")
SPACE_IDS = [s.strip() for s in os.getenv("CLICKUP_SPACE_IDS", "").split(",") if s.strip()]
ASSIGNEES = [a.strip() for a in os.getenv("CLICKUP_ASSIGNEES", "").split(",") if a.strip()]
ASSIGNEES_WITH_UNASSIGNED = ASSIGNEES + ["Unassigned"]
HARVEST_ACCOUNT_ID = os.getenv("HARVEST_ACCOUNT_ID")
HARVEST_TOKEN = os.getenv("HARVEST_TOKEN")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")
SPREADSHEET_NAME = os.getenv("COMBINED_SPREADSHEET_NAME")

CLICKUP_HEADERS = {"Authorization": CLICKUP_API_TOKEN}
HARVEST_HEADERS = {
    "Authorization": f"Bearer {HARVEST_TOKEN}",
    "Harvest-Account-ID": HARVEST_ACCOUNT_ID,
    "User-Agent": "IntegrationScript"
}

# --- Google Sheets client ---
def init_google_sheet():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_JSON, scope)
    client = gspread.authorize(creds)
    return client.open(SPREADSHEET_NAME)

# --- Current work week dates ---
def get_week_dates():
    today = datetime.now(timezone.utc).date()
    monday = today - timedelta(days=today.weekday())
    return [monday + timedelta(days=i) for i in range(5)]

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
    week_start = get_week_dates()[0].isoformat()
    week_end = get_week_dates()[-1].isoformat()
    url = f"https://api.harvestapp.com/v2/time_entries?from={week_start}&to={week_end}"
    response = requests.get(url, headers=HARVEST_HEADERS)
    response.raise_for_status()
    entries = response.json().get("time_entries", [])

    formatted_entries = []
    for e in entries:
        assignee = e.get("user", {}).get("name", "Unassigned")
        assignee = assignee if assignee in ASSIGNEES_WITH_UNASSIGNED else "Unassigned"
        entry_date = datetime.fromisoformat(e["spent_date"]).date()
        formatted_entries.append({
            "id": f"harvest_{e.get('id')}",
            "name": f"[Harvest] {e.get('notes', 'No description')}",
            "assignee": assignee,
            "sheet_dates": [entry_date]  # single day for harvest
        })
    return formatted_entries

# --- Main ---
def main():
    sheet = init_google_sheet()
    now = datetime.now(timezone.utc)
    excluded_lists = {"certification", "product management"}
    weekdays = get_week_dates()

    task_dict = {assignee: [] for assignee in ASSIGNEES_WITH_UNASSIGNED}
    seen_task_ids_per_assignee = {assignee: set() for assignee in ASSIGNEES_WITH_UNASSIGNED}

    def add_task(t, allowed_statuses, list_name, restrict_overdue_unassigned=False):
        task_id = t["id"]
        task_status = t.get("status", {}).get("status", "").upper()
        if task_status not in allowed_statuses:
            return

        due_date_str = t.get("due_date")
        due_date = datetime.fromtimestamp(int(due_date_str)/1000, tz=timezone.utc).date() if due_date_str else None
        task_assignees = [a.get("username", "") for a in t.get("assignees", [])] or ["Unassigned"]

        if restrict_overdue_unassigned:
            if "Unassigned" in task_assignees or (due_date and due_date < now.date()):
                return

        sheet_dates = [due_date] if due_date else weekdays
        task_name = f"[{list_name}] {t['name']}"
        task_link = t.get("url")  # store ClickUp URL

        for assignee in task_assignees:
            target = assignee if assignee in ASSIGNEES_WITH_UNASSIGNED else "Unassigned"
            if task_id not in seen_task_ids_per_assignee[target]:
                task_dict[target].append({"name": task_name, "sheet_dates": sheet_dates, "id": task_id, "link": task_link})
                seen_task_ids_per_assignee[target].add(task_id)

    # --- Pull ClickUp tasks across multiple spaces ---
    for space_id in SPACE_IDS:
        for folder in get_folders(space_id):
            for lst in get_lists_in_folder(folder["id"]):
                list_name = lst.get("name", "")
                if list_name.lower() in excluded_lists:
                    continue
                tasks = get_tasks(lst["id"])
                for t in tasks:
                    if list_name.lower() == "freshdesk":
                        add_task(t, {"IN PROGRESS", "TO DO", "REVIEW"}, list_name)
                    elif list_name.lower() == "internal projects":
                        add_task(t, {"IN PROGRESS", "REVIEW"}, list_name)
                    else:
                        add_task(t, {"IN PROGRESS", "REVIEW"}, list_name, restrict_overdue_unassigned=True)

        for lst in get_lists_directly_in_space(space_id):
            list_name = lst.get("name", "")
            if list_name.lower() in excluded_lists:
                continue
            tasks = get_tasks(lst["id"])
            for t in tasks:
                if list_name.lower() == "freshdesk":
                    add_task(t, {"IN PROGRESS", "TO DO", "REVIEW"}, list_name)
                elif list_name.lower() == "internal projects":
                    add_task(t, {"IN PROGRESS", "REVIEW"}, list_name)
                else:
                    add_task(t, {"IN PROGRESS", "REVIEW"}, list_name, restrict_overdue_unassigned=True)

    # --- Pull Harvest tasks ---
    for entry in get_harvest_entries():
        assignee = entry["assignee"]
        if entry["id"] not in seen_task_ids_per_assignee[assignee]:
            task_dict[assignee].append(entry)
            seen_task_ids_per_assignee[assignee].add(entry["id"])

    # --- Write to Google Sheets ---
    for day in weekdays:
        sheet_name = day.strftime("%A")
        try:
            ws = sheet.worksheet(sheet_name)
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws = sheet.add_worksheet(title=sheet_name, rows="100", cols=str(len(ASSIGNEES_WITH_UNASSIGNED)))

        header_row = [Cell(1, col_idx, assignee) for col_idx, assignee in enumerate(ASSIGNEES_WITH_UNASSIGNED, start=1)]
        ws.update_cells(header_row)



        max_tasks = max(len([t for t in task_dict[a] if day in t["sheet_dates"]]) for a in ASSIGNEES_WITH_UNASSIGNED)
        cells_to_update = []
        for i in range(max_tasks):
            for col_idx, assignee in enumerate(ASSIGNEES_WITH_UNASSIGNED, start=1):
                tasks_for_day = [t for t in task_dict[assignee] if day in t["sheet_dates"]]
                row_index = i + 2  # +2 because row 1 is headers
                if i < len(tasks_for_day):
                    t_obj = tasks_for_day[i]
                    if "link" in t_obj and t_obj["link"]:  # ClickUp task
                        text = t_obj["name"]
                        link = t_obj["link"]
                        safe_text = text.replace('"', '""')
                        formula = f'=HYPERLINK("{link}", "{safe_text}")'
                        cells_to_update.append(Cell(row_index, col_idx, formula))
                    else:
                        cells_to_update.append(Cell(row_index, col_idx, t_obj["name"]))
                else:
                    cells_to_update.append(Cell(row_index, col_idx, ""))

        # Update the worksheet with formulas evaluated
        ws.update_cells(cells_to_update, value_input_option="USER_ENTERED")



    print("ClickUp (multi-space) + Harvest tasks written to Google Sheets (hyperlinks only for ClickUp tasks).")

if __name__ == "__main__":
    main()
