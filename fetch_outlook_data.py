import os
import requests
from datetime import datetime, timedelta, timezone, time
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# --- Load environment variables ---
load_dotenv()
SPREADSHEET_NAME = os.getenv("OUTLOOK_SPREADSHEET_NAME")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")

raw_emails = os.getenv("OUTLOOK_USER_EMAIL")
if not raw_emails:
    raise ValueError("OUTLOOK_USER_EMAILS is not set in .env")
OUTLOOK_USER_EMAILS = [e.strip() for e in raw_emails.split(",")]

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# --- Google Sheets setup ---
def init_google_sheet():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_JSON, scope)
    client = gspread.authorize(creds)
    return client.open(SPREADSHEET_NAME)

# --- Generate 30-minute time slots (08:00–18:00) ---
def generate_time_slots(start_hour=8, end_hour=18):
    slots = []
    current = datetime.combine(datetime.today(), time(start_hour, 0))
    end = datetime.combine(datetime.today(), time(end_hour, 0))
    while current < end:
        slots.append(current.time())
        current += timedelta(minutes=30)
    return slots

# --- Get current week Mon–Fri ---
def get_week_dates():
    today = datetime.now(timezone.utc).date()
    monday = today - timedelta(days=today.weekday())
    return [monday + timedelta(days=i) for i in range(5)]

# --- Convert email to "Full Name" ---
def email_to_name(email):
    local = email.split("@")[0]  # "thando.calana"
    parts = local.split(".")     # ["thando", "calana"]
    return " ".join(p.capitalize() for p in parts)

# --- Fetch Outlook events for a given user ---
def get_outlook_events(user_email):
    # Get access token
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    token_data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials"
    }
    token_resp = requests.post(token_url, data=token_data)
    token_resp.raise_for_status()
    access_token = token_resp.json()["access_token"]

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Prefer": 'outlook.timezone="UTC"'
    }

    weekdays = get_week_dates()
    start_str = weekdays[0].isoformat() + "T00:00:00Z"
    end_str = weekdays[-1].isoformat() + "T23:59:59Z"

    url = f"https://graph.microsoft.com/v1.0/users/{user_email}/calendarview?startDateTime={start_str}&endDateTime={end_str}"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    events = resp.json().get("value", [])

    formatted = []
    for ev in events:
        start_dt = datetime.fromisoformat(ev["start"]["dateTime"])
        end_dt = datetime.fromisoformat(ev["end"]["dateTime"])
        formatted.append({
            "subject": ev.get("subject", "No subject"),
            "date": start_dt.date(),
            "start_time": start_dt.time(),
            "end_time": end_dt.time()
        })
    return formatted

# --- Write calendar-style sheet for all users ---
def write_calendar_sheet(sheet, all_events, users):
    weekdays = get_week_dates()
    time_slots = generate_time_slots()

    for day in weekdays:
        sheet_name = day.strftime("%A")
        try:
            ws = sheet.worksheet(sheet_name)
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws = sheet.add_worksheet(title=sheet_name, rows="200", cols=str(len(users) + 1))

        # Header row: Time + each user's name
        rows_to_write = [["Time"] + [email_to_name(u) for u in users]]

        # Populate slots
        for slot in time_slots:
            row = [slot.strftime("%H:%M")]
            for user in users:
                row_events = []
                for ev in all_events[user]:
                    if ev["date"] == day and ev["start_time"] <= slot < ev["end_time"]:
                        row_events.append(ev["subject"])
                row.append(", ".join(row_events))
            rows_to_write.append(row)

        ws.update(values=rows_to_write, range_name=f"A1:{chr(65+len(users))}{len(rows_to_write)}")

# --- Main ---
def main():
    sheet = init_google_sheet()
    all_events = {}
    for user in OUTLOOK_USER_EMAILS:
        all_events[user] = get_outlook_events(user)
    write_calendar_sheet(sheet, all_events, OUTLOOK_USER_EMAILS)
    print("All Outlook calendars written in calendar-style format.")

if __name__ == "__main__":
    main()
