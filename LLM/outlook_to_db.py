import os
import requests
import pyodbc
from datetime import datetime, timedelta, time, timezone
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

# --- Load environment variables ---
load_dotenv()
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
OUTLOOK_USER_EMAILS = [e.strip() for e in os.getenv("OUTLOOK_USER_EMAIL").split(",")]

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DB = os.getenv("SQL_DB")
LOCAL_TZ = os.getenv("LOCAL_TZ", "Africa/Johannesburg")

# --- Load sentence-transformer model ---
model = SentenceTransformer("all-MiniLM-L6-v2")

# --- Working hours ---
WORK_START = time(8, 0)
WORK_END   = time(17, 0)
SLOT_MINUTES = 30
TOTAL_SLOTS_PER_DAY = int((WORK_END.hour * 60 + WORK_END.minute - WORK_START.hour * 60) / SLOT_MINUTES)

# --- Helpers ---
def get_week_dates():
    today = datetime.now(timezone.utc).date()
    monday = today - timedelta(days=today.weekday())
    return [monday + timedelta(days=i) for i in range(5)]

def extract_first_name(user_email: str, display_name: str = None) -> str:
    if display_name:
        return display_name.strip().split()[0].capitalize()
    local_part = user_email.split("@")[0]
    return local_part.split(".")[0].capitalize() if "." in local_part else local_part.capitalize()

def is_within_work_hours(dt: datetime) -> bool:
    t = dt.time()
    return WORK_START <= t < WORK_END

# --- Fetch Outlook events for a user ---
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
        "Prefer": f'outlook.timezone="{LOCAL_TZ}"'
    }

    # Get display name
    user_info = requests.get(f"https://graph.microsoft.com/v1.0/users/{user_email}", headers=headers)
    display_name = user_info.json().get("displayName") if user_info.status_code == 200 else None
    first_name = extract_first_name(user_email, display_name)

    weekdays = get_week_dates()
    start_str = weekdays[0].isoformat() + "T00:00:00"
    end_str   = weekdays[-1].isoformat() + "T23:59:59"

    url = (
        f"https://graph.microsoft.com/v1.0/users/{user_email}/calendarview"
        f"?startDateTime={start_str}&endDateTime={end_str}&$top=1000"
    )
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    events = resp.json().get("value", [])

    formatted = []

    for ev in events:
        status = ev.get("showAs", "busy").lower()
        if status not in ["busy", "tentative", "oof"]:
            continue

        start_dt = datetime.fromisoformat(ev["start"]["dateTime"])
        end_dt   = datetime.fromisoformat(ev["end"]["dateTime"])
        subject  = ev.get("subject", "No subject")

        # Expand across 30-min slots but only within working hours
        current = start_dt
        while current < end_dt:
            if is_within_work_hours(current):
                formatted.append({
                    "subject": subject,
                    "date": current.date(),
                    "time_slot": current.time().replace(microsecond=0),
                    "start_time": start_dt.replace(microsecond=0),
                    "end_time": end_dt.replace(microsecond=0),
                })
            current += timedelta(minutes=SLOT_MINUTES)

    # --- Compute load percentage accounting for concurrency within working hours ---
    slots_by_date = {}
    for ev in formatted:
        date = ev["date"]
        slot = ev["time_slot"]
        slots_by_date.setdefault(date, set()).add(slot)

    for ev in formatted:
        date = ev["date"]
        unique_slots = len(slots_by_date[date])
        ev["load_percentage"] = round(min(unique_slots / TOTAL_SLOTS_PER_DAY, 1.0), 2)

    return formatted, first_name

# --- Helper to get embedding ---
def get_embedding(text: str) -> list:
    return model.encode(text).tolist()

# --- Write to SQL Server ---
def write_to_db(all_events):
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};DATABASE={SQL_DB};"
        f"Trusted_Connection=yes;TrustServerCertificate=yes;"
    )
    cursor = conn.cursor()

    weekdays = get_week_dates()
    for user in OUTLOOK_USER_EMAILS:
        cursor.execute(
            "DELETE FROM dbo.OutlookCalendar WHERE user_email=? AND date BETWEEN ? AND ?",
            user, weekdays[0], weekdays[-1]
        )
    conn.commit()

    insert_sql = """
        INSERT INTO dbo.OutlookCalendar
        (user_email, first_name, date, time_slot, meeting_subject, start_time, end_time, load_percentage, content, embedding)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for user, data in all_events.items():
        events, first_name = data
        for ev in events:
            content_text = f"{ev['subject']}, {ev['start_time'].strftime('%H:%M')} - {ev['end_time'].strftime('%H:%M')}"
            embedding = get_embedding(content_text)
            cursor.execute(insert_sql, (
                user,
                first_name,
                ev["date"],
                ev["time_slot"],
                ev["subject"],
                ev["start_time"],
                ev["end_time"],
                ev["load_percentage"],
                content_text,
                str(embedding)
            ))

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
