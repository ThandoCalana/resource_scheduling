import os
import requests
from dotenv import load_dotenv

# --- Load environment variables ---
load_dotenv()
CLICKUP_API_KEY = os.getenv('CLICKUP_TOKEN')
TEAM_ID = os.getenv('CLICKUP_TEAM_ID')
SPACE_ID = os.getenv('CLICKUP_SPACE_ID')  # Add this to your .env

HEADERS = {"Authorization": CLICKUP_API_KEY}

# --- Get all folders in the space ---
folders_url = f"https://api.clickup.com/api/v2/space/{SPACE_ID}/folder"
folders_resp = requests.get(folders_url, headers=HEADERS)
print("=== FOLDERS ===")
print(folders_resp.json())

# --- Get lists in each folder ---
if folders_resp.ok:
    for folder in folders_resp.json().get("folders", []):
        folder_id = folder["id"]
        lists_url = f"https://api.clickup.com/api/v2/folder/{folder_id}/list"
        lists_resp = requests.get(lists_url, headers=HEADERS)
        print(f"=== LISTS in Folder {folder_id} ===")
        print(lists_resp.json())

# --- Get lists directly in the space (not in folders) ---
space_lists_url = f"https://api.clickup.com/api/v2/space/{SPACE_ID}/list"
space_lists_resp = requests.get(space_lists_url, headers=HEADERS)
print("=== LISTS directly in Space ===")
print(space_lists_resp.json())

# --- Get tasks in each list (both from folders and space) ---
all_list_ids = []

# From folders
if folders_resp.ok:
    for folder in folders_resp.json().get("folders", []):
        folder_id = folder["id"]
        lists_url = f"https://api.clickup.com/api/v2/folder/{folder_id}/list"
        lists_resp = requests.get(lists_url, headers=HEADERS)
        if lists_resp.ok:
            for lst in lists_resp.json().get("lists", []):
                all_list_ids.append(lst["id"])

# From space directly
if space_lists_resp.ok:
    for lst in space_lists_resp.json().get("lists", []):
        all_list_ids.append(lst["id"])

# Fetch tasks for each list
for list_id in all_list_ids:
    tasks_url = f"https://api.clickup.com/api/v2/list/{list_id}/task"
    tasks_resp = requests.get(tasks_url, headers=HEADERS)
    print(f"=== TASKS in List {list_id} ===")
    print(tasks_resp.json())


import requests
headers = {"Authorization": "YOUR_TOKEN"}
url = "https://api.clickup.com/api/v2/team"
r = requests.get(url, headers=headers)
print(r.json())