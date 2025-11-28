import os
import csv
from datetime import datetime, timedelta, timezone
from msal import PublicClientApplication
from dotenv import load_dotenv
import requests

# --- Load environment variables ---
load_dotenv()
CLIENT_ID = os.getenv("CLIENT_ID")
TENANT_ID = os.getenv("TENANT_ID")

# --- MSAL Public Client (Delegated) ---
app = PublicClientApplication(
    client_id=CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}"
)

# --- Device Code Flow ---
scopes = ["Calendars.Read"]
flow = app.initiate_device_flow(scopes=scopes)
print(flow["message"])  # follow instructions to sign in

result = app.acquire_token_by_device_flow(flow)
if "access_token" not in result:
    raise Exception("Failed to obtain access token.")

access_token = result["access_token"]
headers = {"Authorization": f"Bearer {access_token}"}

# --- Calculate current week's Monday → Friday ---
today = datetime.now(timezone.utc).date()
monday = today - timedelta(days=today.weekday())
friday = monday + timedelta(days=4)

start_dt = datetime.combine(monday, datetime.min.time()).isoformat() + "Z"
end_dt = datetime.combine(friday, datetime.max.time()).isoformat() + "Z"

# --- Pull events ---
url = f"https://graph.microsoft.com/v1.0/me/calendarview?startDateTime={start_dt}&endDateTime={end_dt}"
response = requests.get(url, headers=headers)
response.raise_for_status()
events = response.json().get("value", [])

# --- Write CSV ---
csv_filename = "outlook_calendar_week.csv"
with open(csv_filename, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["Subject", "Start", "End", "Organizer", "Location"])
    for e in events:
        subject = e.get("subject", "No title")
        start = e.get("start", {}).get("dateTime", "")
        end = e.get("end", {}).get("dateTime", "")
        organizer = e.get("organizer", {}).get("emailAddress", {}).get("address", "")
        location = e.get("location", {}).get("displayName", "")
        writer.writerow([subject, start, end, organizer, location])

print(f"Signed-in email: {result.get('id_token_claims', {}).get('preferred_username', 'Unknown')}")
print(f"{len(events)} events written to {csv_filename}")
