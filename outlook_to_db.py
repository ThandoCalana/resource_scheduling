import os
import requests
import pyodbc
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# --- Load environment variables ---
load_dotenv()
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
OUTLOOK_USER_EMAILS = [e.strip() for e in os.getenv("OUTLOOK_USER_EMAIL").split(",")]

SQL_SERVER = os.getenv("SQL_SERVER")  # e.g., "localhost\\SQLEXPRESS"
SQL_DB = os.getenv("SQL_DB")          # database name
LOCAL_TZ = os.getenv("LOCAL_TZ", "Africa/Johannesburg")

# --- Helpers ---
def get_week_dates():
    today = datetime.now(timezone.utc).date()
    monday = today - timedelta(days=today.weekday())
    return [monday + timedelta(days=i) for i in range(5)]

# --- Fetch Outlook events for a user ---
def get_outlook_events(user_email):
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
        "Prefer": f'outlook.timezone="{LOCAL_TZ}"'
    }

    weekdays = get_week_dates()
    start_str = weekdays[0].isoformat() + "T00:00:00"
    end_str = weekdays[-1].isoformat() + "T23:59:59"

    url = (
        f"https://graph.microsoft.com/v1.0/users/{user_email}/calendarview"
        f"?startDateTime={start_str}&endDateTime={end_str}&$top=1000"
    )
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    events = resp.json().get("value", [])

    formatted = []
    slot_minutes = 30  # 30-min intervals

    for ev in events:
        status = ev.get("showAs", "busy").lower()
        # Include meetings that are accepted, tentative, or marked busy
        if status not in ["busy", "tentative", "oof"]:
            continue

        start_dt = datetime.fromisoformat(ev["start"]["dateTime"])
        end_dt = datetime.fromisoformat(ev["end"]["dateTime"])
        subject = ev.get("subject", "No subject")

        # Expand across 30-min intervals
        current = start_dt
        while current < end_dt:
            formatted.append({
                "subject": subject,
                "date": current.date(),
                "time_slot": current.time(),
                "start_time": start_dt,
                "end_time": end_dt
            })
            current += timedelta(minutes=slot_minutes)

    return formatted

# --- Write to SQL Server ---
def write_to_db(all_events):
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DB};"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate=yes;"
    )
    cursor = conn.cursor()

    # Clear current week’s data for these users
    weekdays = get_week_dates()
    for user in OUTLOOK_USER_EMAILS:
        cursor.execute(
            "DELETE FROM dbo.OutlookCalendar WHERE user_email=? AND date BETWEEN ? AND ?",
            user, weekdays[0], weekdays[-1]
        )
    conn.commit()

    # Insert events
    for user, events in all_events.items():
        for ev in events:
            cursor.execute(
                """INSERT INTO dbo.OutlookCalendar
                   (user_email, date, time_slot, meeting_subject, start_time, end_time)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                user, ev["date"], ev["time_slot"], ev["subject"], ev["start_time"], ev["end_time"]
            )
    conn.commit()
    cursor.close()
    conn.close()
    print("Outlook calendar events written to SQL Server.")

# --- Main ---
def main():
    all_events = {}
    for user in OUTLOOK_USER_EMAILS:
        all_events[user] = get_outlook_events(user)
    write_to_db(all_events)

if __name__ == "__main__":
    main()
