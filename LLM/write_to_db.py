import os
import requests
import pyodbc
from datetime import datetime, timedelta, time, timezone
from dotenv import load_dotenv


# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
OUTLOOK_USER_EMAILS = [e.strip() for e in os.getenv("OUTLOOK_USER_EMAIL").split(",")]

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DB = os.getenv("SQL_DB")
LOCAL_TZ = os.getenv("LOCAL_TZ", "Africa/Johannesburg")

WORK_START = time(8, 0)
WORK_END = time(17, 0)
SLOT_MINUTES = 30
TOTAL_SLOTS_PER_DAY = int((WORK_END.hour * 60 + WORK_END.minute -
                           (WORK_START.hour * 60 + WORK_START.minute)) / SLOT_MINUTES)


# -----------------------------
# Helpers
# -----------------------------
def get_week_dates():
    today = datetime.now(timezone.utc).date()
    monday = today - timedelta(days=today.weekday())
    return [monday + timedelta(days=i) for i in range(5)]


def extract_first_name(user_email: str, display_name: str = None):
    if display_name:
        return display_name.strip().split()[0].capitalize()

    local_part = user_email.split("@")[0]
    return (local_part.split(".")[0] if "." in local_part else local_part).capitalize()


def is_within_work_hours(dt: datetime) -> bool:
    t = dt.time()
    return WORK_START <= t < WORK_END


def compute_free_blocks(busy_blocks):
    busy_blocks = sorted(busy_blocks, key=lambda b: b[0])
    free_blocks = []
    current_start = WORK_START

    for start, end in busy_blocks:
        if start > current_start:
            free_blocks.append((current_start, start))
        current_start = max(current_start, end)

    if current_start < WORK_END:
        free_blocks.append((current_start, WORK_END))

    return free_blocks


def merge_busy_blocks_with_context(events):
    events_sorted = sorted(events, key=lambda e: e["start_time"])
    blocks = []

    for ev in events_sorted:
        s, e, subj = ev["start_time"], ev["end_time"], ev["subject"]

        if not blocks:
            blocks.append({
                "start": s,
                "end": e,
                "subjects": [subj],
                "details": [(subj, s, e)],
            })
            continue

        last = blocks[-1]
        if s <= last["end"]:
            last["end"] = max(last["end"], e)
            last["subjects"].append(subj)
            last["details"].append((subj, s, e))
        else:
            blocks.append({
                "start": s,
                "end": e,
                "subjects": [subj],
                "details": [(subj, s, e)],
            })

    return blocks


def build_rag_summary(first_name, date, merged_busy, free_blocks, load):
    busy_entries = []
    for block in merged_busy:
        for subj, s, e in block["details"]:
            busy_entries.append({
                "start": s.strftime('%H:%M'),
                "end": e.strftime('%H:%M'),
                "subject": subj
            })

    busy_str = (
        "\n".join([f"- {b['start']}-{b['end']} | {b['subject']}" for b in busy_entries])
        if busy_entries else "None"
    )

    free_str = (
        "\n".join([f"- {s.strftime('%H:%M')}-{e.strftime('%H:%M')}" for s, e in free_blocks])
        if free_blocks else "None"
    )

    total_free_hours = sum(
        (datetime.combine(date, e) - datetime.combine(date, s)).total_seconds() / 3600
        for s, e in free_blocks
    )
    total_busy_hours = sum(
        (e - s).total_seconds() / 3600
        for block in merged_busy
        for _, s, e in block["details"]
    ) if merged_busy else 0.0

    morning_free = any(s.hour < 12 for s, _ in free_blocks)
    afternoon_free = any(s.hour >= 12 for s, _ in free_blocks)

    summary = f"""
EMPLOYEE: {first_name}
DATE: {date}
LOAD_PERCENTAGE: {round(load * 100, 2)}
BUSY_SLOTS:
{busy_str}
FREE_SLOTS:
{free_str}
META:
TOTAL_FREE_HOURS: {round(total_free_hours, 2)}
TOTAL_BUSY_HOURS: {round(total_busy_hours, 2)}
FREE_MORNING: {str(morning_free)}
FREE_AFTERNOON: {str(afternoon_free)}
""".strip()

    return summary



# -----------------------------
# Outlook Graph API Handling
# -----------------------------
def get_outlook_events(user_email):
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

    token_data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }

    token_resp = requests.post(token_url, data=token_data)
    token_resp.raise_for_status()
    access_token = token_resp.json()["access_token"]

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Prefer": f'outlook.timezone="{LOCAL_TZ}"',
    }

    # Get First Name
    user_info = requests.get(f"https://graph.microsoft.com/v1.0/users/{user_email}", headers=headers)
    display_name = user_info.json().get("displayName") if user_info.status_code == 200 else None
    first_name = extract_first_name(user_email, display_name)

    # Fetch events for current week
    weekdays = get_week_dates()
    start_str = f"{weekdays[0]}T00:00:00"
    end_str = f"{weekdays[-1]}T23:59:59"

    url = (
        f"https://graph.microsoft.com/v1.0/users/{user_email}/calendarview"
        f"?startDateTime={start_str}&endDateTime={end_str}&$top=2000"
    )
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    events = resp.json().get("value", [])

    formatted = []

    for ev in events:
        status = ev.get("showAs", "busy").lower()
        if status not in {"busy", "tentative", "oof"}:
            continue

        start_dt = datetime.fromisoformat(ev["start"]["dateTime"])
        end_dt = datetime.fromisoformat(ev["end"]["dateTime"])
        subject = ev.get("subject", "No subject")

        current = start_dt
        while current < end_dt:
            if is_within_work_hours(current):
                formatted.append({
                    "subject": subject,
                    "date": current.date(),
                    "start_time": start_dt.replace(microsecond=0),
                    "end_time": end_dt.replace(microsecond=0),
                })
            current += timedelta(minutes=SLOT_MINUTES)

    # Load %
    slots_by_date = {}
    for ev in formatted:
        slots_by_date.setdefault(ev["date"], 0)
        slots_by_date[ev["date"]] += 1

    for ev in formatted:
        ev["load_percentage"] = round(slots_by_date[ev["date"]] / TOTAL_SLOTS_PER_DAY, 2)

    return formatted, first_name


# -----------------------------
# SQL Server Writing
# -----------------------------
def write_to_db(all_events):
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};DATABASE={SQL_DB};"
        f"Trusted_Connection=yes;TrustServerCertificate=yes;"
    )
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE OutlookCalendarEvents")
    cursor.execute("TRUNCATE TABLE OutlookCalendarAvailability")
    cursor.execute("TRUNCATE TABLE OutlookCalendarSummary")
    conn.commit()

    insert_event = """
        INSERT INTO dbo.OutlookCalendarEvents
        (user_email, first_name, date, meeting_subject, start_time, end_time, load_percentage, content)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    insert_availability = """
        INSERT INTO dbo.OutlookCalendarAvailability
        (user_email, first_name, date, block_type, start_time, end_time, duration_minutes,
         meeting_subject, meeting_start, meeting_end, context)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    insert_summary = """
        INSERT INTO dbo.OutlookCalendarSummary
        (user_email, first_name, date, load_percentage, summary_text)
        VALUES (?, ?, ?, ?, ?)
    """

    for user, data in all_events.items():
        events, first_name = data

        # Insert events
        for ev in events:
            content_text = f"{ev['subject']} {ev['start_time'].strftime('%H:%M')}–{ev['end_time'].strftime('%H:%M')}"
            cursor.execute(insert_event, (
                user,
                first_name,
                ev["date"],
                ev["subject"],
                ev["start_time"],
                ev["end_time"],
                ev["load_percentage"],
                content_text,
            ))

        # By date
        events_by_date = {}
        for ev in events:
            events_by_date.setdefault(ev["date"], []).append(ev)

        # Availability + summary
        for date, evs in events_by_date.items():
            merged_busy = merge_busy_blocks_with_context(evs)
            busy_pairs = [(b["start"].time(), b["end"].time()) for b in merged_busy]
            free_blocks = compute_free_blocks(busy_pairs)
            load = evs[0]["load_percentage"]

            for s, e in free_blocks:
                dur = int((datetime.combine(date, e) - datetime.combine(date, s)).total_seconds() / 60)
                cursor.execute(insert_availability, (
                    user, first_name, date, "FREE", s, e, dur,
                    None, None, None,
                    f"Free block {s.strftime('%H:%M')}–{e.strftime('%H:%M')}"
                ))

            for block in merged_busy:
                dur = int((block["end"] - block["start"]).total_seconds() / 60)
                context_text = "; ".join([f"{subj} {s.strftime('%H:%M')}–{e.strftime('%H:%M')}" for subj, s, e in block["details"]])
                cursor.execute(insert_availability, (
                    user,
                    first_name,
                    date,
                    "USED",
                    block["start"].time(),
                    block["end"].time(),
                    dur,
                    "; ".join(block["subjects"]),
                    block["start"],
                    block["end"],
                    context_text,
                ))

            summary_text = build_rag_summary(first_name, date, merged_busy, free_blocks, load)
            cursor.execute(insert_summary, (
                user, first_name, date, load, summary_text
            ))

    conn.commit()
    cursor.close()
    conn.close()
    print("Events, availability blocks, and summaries written to SQL Server.")


# -----------------------------
# Main
# -----------------------------
def main():
    all_events = {user: get_outlook_events(user) for user in OUTLOOK_USER_EMAILS}
    write_to_db(all_events)


if __name__ == "__main__":
    main()
