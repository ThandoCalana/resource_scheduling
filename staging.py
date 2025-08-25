import os
import requests
from dotenv import load_dotenv

# --- Load environment variables ---
load_dotenv()
CLICKUP_API_TOKEN = os.getenv("CLICKUP_TOKEN")
SPACE_ID = os.getenv("CLICKUP_SPACE_ID")  # now defined in your .env

CLICKUP_HEADERS = {"Authorization": CLICKUP_API_TOKEN}

# --- Configurable assignees ---
ASSIGNEES = ["Mark Gelman", "Thembani Faleni", "Robin April"]
ASSIGNEES_WITH_UNASSIGNED = ASSIGNEES + ["Unassigned"]

# --- ClickUp functions ---
def get_folders(space_id):
    return requests.get(f"https://api.clickup.com/api/v2/space/{space_id}/folder", headers=CLICKUP_HEADERS).json().get("folders", [])

def get_lists_in_folder(folder_id):
    return requests.get(f"https://api.clickup.com/api/v2/folder/{folder_id}/list", headers=CLICKUP_HEADERS).json().get("lists", [])

def get_lists_directly_in_space(space_id):
    return requests.get(f"https://api.clickup.com/api/v2/space/{space_id}/list", headers=CLICKUP_HEADERS).json().get("lists", [])

def get_tasks(list_id):
    return requests.get(f"https://api.clickup.com/api/v2/list/{list_id}/task", headers=CLICKUP_HEADERS).json().get("tasks", [])

# --- Main ---
def main():
    task_dict = {assignee: [] for assignee in ASSIGNEES_WITH_UNASSIGNED}
    seen_task_ids_per_assignee = {assignee: set() for assignee in ASSIGNEES_WITH_UNASSIGNED}

    # --- Helper to add task ---
    def add_task(t, allowed_statuses):
        task_id = t["id"]
        task_status = t.get("status", {}).get("status", "").upper()
        if task_status not in allowed_statuses:
            return

        task_assignees = [a.get("username", "") for a in t.get("assignees", [])]
        if not task_assignees:
            task_assignees = ["Unassigned"]

        for assignee in task_assignees:
            target = assignee if assignee in ASSIGNEES_WITH_UNASSIGNED else "Unassigned"
            if task_id not in seen_task_ids_per_assignee[target]:
                task_dict[target].append(t["name"])
                seen_task_ids_per_assignee[target].add(task_id)

    # --- Folders ---
    for folder in get_folders(SPACE_ID):
        for lst in get_lists_in_folder(folder["id"]):
            list_name_lower = lst.get("name", "").lower()
            tasks = get_tasks(lst["id"])
            for t in tasks:
                if list_name_lower == "freshdesk":
                    add_task(t, allowed_statuses={"IN PROGRESS", "TO DO", "REVIEW"})
                else:
                    add_task(t, allowed_statuses={"IN PROGRESS", "REVIEW"})

    # --- Lists directly in space ---
    for lst in get_lists_directly_in_space(SPACE_ID):
        list_name_lower = lst.get("name", "").lower()
        tasks = get_tasks(lst["id"])
        for t in tasks:
            if list_name_lower == "freshdesk":
                add_task(t, allowed_statuses={"IN PROGRESS", "TO DO", "REVIEW"})
            else:
                add_task(t, allowed_statuses={"IN PROGRESS", "REVIEW"})

    # --- Align rows for TXT ---
    max_rows = max(len(v) for v in task_dict.values()) if task_dict else 0
    for k in task_dict:
        task_dict[k] += [""] * (max_rows - len(task_dict[k]))

    # --- Write TXT ---
    with open("tasks_output.txt", "w", encoding="utf-8") as f:
        f.write("\t".join(ASSIGNEES_WITH_UNASSIGNED) + "\n")
        for i in range(max_rows):
            row = [task_dict[assignee][i] for assignee in ASSIGNEES_WITH_UNASSIGNED]
            f.write("\t".join(row) + "\n")

    print("All ClickUp tasks (Freshdesk + other lists) written to tasks_output.txt.")

if __name__ == "__main__":
    main()
