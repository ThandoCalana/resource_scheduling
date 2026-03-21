import os
import csv
import requests
import pytz
from datetime import datetime, timedelta, time
# from dotenv import load_dotenv

# --- Load environment variables ---
# load_dotenv()

TENANT_ID = os.environ["TENANT_ID"]
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
OUTLOOK_USER_EMAIL = [u.strip() for u in os.environ["OUTLOOK_USER_EMAIL"].split(",") if u.strip()]

# --- Config ---
OUTPUT_DIR = "./Schedule"
EVENTS_FILE = os.path.join(OUTPUT_DIR, "outlook_events.csv")

LOCAL_TZ = pytz.timezone("Africa/Johannesburg")

# --- Auth ---
def get_access_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]

# --- Fetch events ---
def fetch_events(token, user_email, start_dt, end_dt):
    url = f"https://graph.microsoft.com/v1.0/users/{user_email}/calendarView"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "startDateTime": start_dt.isoformat(),
        "endDateTime": end_dt.isoformat(),
        "$top": 1000,
    }

    events = []
    while url:
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()
        events.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
        params = None

    return events

# --- Normalize event ---
def parse_event(ev):
    start_dt = datetime.fromisoformat(ev["start"]["dateTime"]).astimezone(LOCAL_TZ)
    end_dt   = datetime.fromisoformat(ev["end"]["dateTime"]).astimezone(LOCAL_TZ)

    # Skip events with zero or negative duration
    if end_dt <= start_dt:
        return None

    return {
        "date": start_dt.date(),
        "start_dt": start_dt,
        "end_dt": end_dt,
        "subject": ev.get("subject", "") or ""
    }

# --- Main ---
def main():

    token = get_access_token()

    start_dt = datetime.now(LOCAL_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt   = start_dt + timedelta(days=7)

    event_rows = []

    for user in OUTLOOK_USER_EMAIL:
            raw_events = fetch_events(token, user, start_dt, end_dt)
            parsed_events = [ev for ev in (parse_event(e) for e in raw_events) if ev is not None]

            # --- Build per-user, per-day aggregation for load percentage ---
            daily_durations = {}
            for ev in parsed_events:
                date = ev["date"]
                duration = (ev["end_dt"] - ev["start_dt"]).total_seconds() / 60  # minutes
                daily_durations[date] = daily_durations.get(date, 0) + duration

            # Define total available minutes per day (8:00–16:30)
            WORK_START = time(8, 0)
            WORK_END   = time(16, 30)
            TOTAL_MINUTES = ((datetime.combine(datetime.today(), WORK_END) -
                            datetime.combine(datetime.today(), WORK_START)).total_seconds() / 60)

            for ev in parsed_events:
                date = ev["date"]
                load_pct = min(round(daily_durations[date] / TOTAL_MINUTES * 100), 100)
                event_rows.append([
                    user,
                    date.isoformat(),
                    ev["start_dt"],
                    ev["end_dt"],
                    ev["subject"],
                    load_pct
                ])

    # --- WRITE EVENTS CSV ---
    with open(EVENTS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["user_email", "date", "start_dt", "end_dt", "subject", "load_pct"])
        writer.writerows(event_rows)

    print(f"Events written to: {EVENTS_FILE}")

# --- Run ---
if __name__ == "__main__":
    main()
