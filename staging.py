import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from oauth2client.service_account import ServiceAccountCredentials
import gspread

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

# --- Current work week dates (Mon-Fri) ---
def get_week_dates():
    today = datetime.now(timezone.utc).date()
    monday = today - timedelta(days=today.weekday())
    return [monday + timedelta(days=i) for i in range(5)]

# --- ClickUp API ---
def get_folders(space_id):
    return requests.get(
        f"https://api.clickup.com/api/v2/space/{space_id}/folder",
        headers=CLICKUP_HEADERS
    ).json().get("folders", [])

def get_lists_in_folder(folder_id):
    return requests.get(
        f"https://api.clickup.com/api/v2/folder/{folder_id}/list",
        headers=CLICKUP_HEADERS
    ).json().get("lists", [])

def get_lists_directly_in_space(space_id):
    return requests.get(
        f"https://api.clickup.com/api/v2/space/{space_id}/list",
        headers=CLICKUP_HEADERS
    ).json().get("lists", [])

def get_tasks(list_id):
    # include subtasks=true so parents with subtasks are fully represented
    return requests.get(
        f"https://api.clickup.com/api/v2/list/{list_id}/task?subtasks=true",
        headers=CLICKUP_HEADERS
    ).json().get("tasks", [])

def get_subtasks(task_id):
    return requests.get(
        f"https://api.clickup.com/api/v2/task/{task_id}/subtask",
        headers=CLICKUP_HEADERS
    ).json().get("tasks", [])

# --- Harvest ---
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
    excluded_lists = {"certification", "product management"}  # case-insensitive compare below
    weekdays = get_week_dates()

    # per-assignee buckets
    task_dict = {assignee: [] for assignee in ASSIGNEES_WITH_UNASSIGNED}
    seen_task_ids_per_assignee = {assignee: set() for assignee in ASSIGNEES_WITH_UNASSIGNED}

    def build_sheet_dates(due_date, allow_overdue):
        """
        Return weekday dates up to and including the due date.
        If there's no due date -> all weekdays.
        If due_date is before week start and allow_overdue (e.g., Freshdesk) -> show on Monday only.
        """
        if not due_date:
            return weekdays
        days = [d for d in weekdays if d <= due_date]
        if days:
            return days
        # overdue before Monday: only show on Monday if we're allowing overdue display
        return [weekdays[0]] if allow_overdue else []

    def push_task_to_buckets(task_id, task_name, task_link, assignees, sheet_dates):
        for assignee in assignees:
            target = assignee if assignee in ASSIGNEES_WITH_UNASSIGNED else "Unassigned"
            if task_id not in seen_task_ids_per_assignee[target]:
                task_dict[target].append({
                    "name": task_name,
                    "sheet_dates": sheet_dates,
                    "id": task_id,
                    "link": task_link
                })
                seen_task_ids_per_assignee[target].add(task_id)

    def add_task(t, allowed_statuses, list_name, restrict_overdue_unassigned=False, is_subtask=False, parent_due_date=None):
        """
        Adds a task if it matches allowed statuses.
        Always recurses into subtasks (so subtasks can be included even if parent is filtered out).
        Subtasks inherit the parent's due date when missing.
        Tasks repeat across weekdays up to their due date (or all week if no due date).
        """
        task_id = t.get("id")
        status = (t.get("status", {}) or {}).get("status", "")
        task_status = status.upper()

        # --- Due date (inherit from parent if missing) ---
        due_date_str = t.get("due_date")
        if due_date_str:
            due_date = datetime.fromtimestamp(int(due_date_str) / 1000, tz=timezone.utc).date()
        else:
            due_date = parent_due_date  # may still be None

        # --- Assignees (default to Unassigned) ---
        task_assignees = [a.get("username", "") for a in t.get("assignees", [])] or ["Unassigned"]

        # --- Allow-overdue flag (Freshdesk lists pass False to restrict_overdue_unassigned)
        allow_overdue = not restrict_overdue_unassigned

        # --- If restricting, skip unassigned & overdue ---
        if restrict_overdue_unassigned:
            if "Unassigned" in task_assignees:
                # skip unassigned when restricting
                pass_flag = False
            else:
                pass_flag = True
            if due_date and due_date < now.date():
                pass_flag = False
            if not pass_flag:
                # still recurse into subtasks (they might be valid)
                for sub in get_subtasks(task_id):
                    add_task(sub, allowed_statuses, list_name, restrict_overdue_unassigned, is_subtask=True, parent_due_date=due_date)
                return

        # --- Only add THIS task if status is allowed ---
        if task_status in allowed_statuses:
            # Compute sheet dates with overdue handling
            sheet_dates = build_sheet_dates(due_date, allow_overdue)
            if sheet_dates:  # May be [] if restricted & overdue before Monday
                # Name/Link
                task_name = t.get("name", "Untitled Task")
                if is_subtask:
                    task_name = f"(Subtask) {task_name}"
                else:
                    task_name = f"[{list_name}] {task_name}"
                task_link = t.get("url")

                push_task_to_buckets(task_id, task_name, task_link, task_assignees, sheet_dates)

        # --- Recurse into subtasks regardless of parent inclusion ---
        for sub in get_subtasks(task_id):
            add_task(sub, allowed_statuses, list_name, restrict_overdue_unassigned, is_subtask=True, parent_due_date=due_date)

    # --- Pull ClickUp tasks across all provided spaces ---
    for space_id in SPACE_IDS:
        # Folders and their lists
        for folder in get_folders(space_id):
            for lst in get_lists_in_folder(folder["id"]):
                list_name = lst.get("name", "") or ""
                if list_name.lower() in excluded_lists:
                    continue
                tasks = get_tasks(lst["id"])
                for t in tasks:
                    if list_name.lower() == "freshdesk":
                        # Freshdesk: include TO DO + IN PROGRESS + REVIEW; allow overdue + unassigned
                        add_task(t, {"IN PROGRESS", "TO DO", "REVIEW"}, list_name, restrict_overdue_unassigned=False)
                    elif list_name.lower() == "internal projects":
                        add_task(t, {"IN PROGRESS", "REVIEW"}, list_name, restrict_overdue_unassigned=True)
                    else:
                        add_task(t, {"IN PROGRESS", "REVIEW"}, list_name, restrict_overdue_unassigned=True)

        # Lists directly under space
        for lst in get_lists_directly_in_space(space_id):
            list_name = lst.get("name", "") or ""
            if list_name.lower() in excluded_lists:
                continue
            tasks = get_tasks(lst["id"])
            for t in tasks:
                if list_name.lower() == "freshdesk":
                    add_task(t, {"IN PROGRESS", "TO DO", "REVIEW"}, list_name, restrict_overdue_unassigned=False)
                elif list_name.lower() == "internal projects":
                    add_task(t, {"IN PROGRESS", "REVIEW"}, list_name, restrict_overdue_unassigned=True)
                else:
                    add_task(t, {"IN PROGRESS", "REVIEW"}, list_name, restrict_overdue_unassigned=True)

    # --- Pull Harvest tasks ---
    for entry in get_harvest_entries():
        assignee = entry["assignee"]
        if entry["id"] not in seen_task_ids_per_assignee[assignee]:
            task_dict[assignee].append(entry)
            seen_task_ids_per_assignee[assignee].add(entry["id"])

    # --- Write to Google Sheets (one batch update per sheet) ---
    for day in weekdays:
        sheet_name = day.strftime("%A")
        try:
            ws = sheet.worksheet(sheet_name)
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws = sheet.add_worksheet(title=sheet_name, rows="200", cols=str(len(ASSIGNEES_WITH_UNASSIGNED)))

        # Build columns per assignee for this day
        per_col_values = []
        for assignee in ASSIGNEES_WITH_UNASSIGNED:
            tasks_for_day = [t for t in task_dict[assignee] if day in t["sheet_dates"]]
            col_cells = []
            for t_obj in tasks_for_day:
                if "link" in t_obj and t_obj["link"]:
                    text = t_obj["name"].replace('"', '""')
                    link = t_obj["link"]
                    col_cells.append(f'=HYPERLINK("{link}", "{text}")')
                else:
                    col_cells.append(t_obj["name"])
            per_col_values.append(col_cells)

        # Determine max rows and build a single 2D matrix
        max_rows = max([len(col) for col in per_col_values] + [0])
        rows = []
        # Header
        rows.append(ASSIGNEES_WITH_UNASSIGNED)
        # Body
        for i in range(max_rows):
            row = []
            for col in per_col_values:
                row.append(col[i] if i < len(col) else "")
            rows.append(row)

        # Batch update once
        ws.update(range_name="A1", values=rows, value_input_option="USER_ENTERED")

    print("Tasks written to Google Sheets.")

if __name__ == "__main__":
    main()
