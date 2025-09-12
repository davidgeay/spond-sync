# attendance_sync.py
# Sync Spond attendance -> Google Sheets (one row per member per event)
#
# pip install spond gspread google-auth pandas
# env: SPOND_USERNAME, SPOND_PASSWORD, GOOGLE_SERVICE_ACCOUNT_JSON, SHEET_ID
#
# Sheet tab must be named: Attendance

import os, json, asyncio
from datetime import datetime, timedelta, timezone
import gspread
from google.oauth2.service_account import Credentials
from spond import spond  # unofficial package

SHEET_ID = os.environ["SHEET_ID"]
TAB_NAME = "Attendance"
EVENT_NAME_FILTER = "Istrening U18B"

# Hard cutoff: only events strictly after July 1, 2025 (UTC)
EVENT_START_MIN = datetime(2025, 7, 1, 0, 0, 0, tzinfo=timezone.utc)

# Window for fetching from Spond API (can be wide; we'll still hard-filter by date)
PAST_DAYS = int(os.environ.get("PAST_DAYS", "365"))
FUTURE_DAYS = int(os.environ.get("FUTURE_DAYS", "365"))

HEADER = [
    "EventID","EventName","EventStartIso","MemberID","MemberName",
    "Status","AbsenceReason","CheckedIn","ResponseAtIso",
    "Source","LastSyncedAtIso","ManualOverride"
]

TRUTHY = {"true","yes","1","y","ja","t"}

def sheets_client_from_env():
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)

def get_ws(gc):
    sh = gc.open_by_key(SHEET_ID)
    try:
        return sh.worksheet(TAB_NAME)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(TAB_NAME, rows=2000, cols=20)

def ensure_header(ws):
    existing = ws.row_values(1)
    if existing != HEADER:
        if existing:
            ws.clear()
        ws.update("A1", [HEADER])

def index_existing(ws):
    data = ws.get_all_records()
    idx = {}
    for i, row in enumerate(data, start=2):
        k = (str(row.get("EventID","")).strip(), str(row.get("MemberID","")).strip())
        mo = str(row.get("ManualOverride","")).strip().lower()
        idx[k] = (i, mo)
    return idx

def is_truthy(x) -> bool:
    return str(x).strip().lower() in TRUTHY

def _parse_event_start_iso(ev) -> tuple[str, datetime | None]:
    # Prefer explicit UTC if present
    raw = ev.get("startTimeUtc") or ev.get("startTime") or ""
    if not raw:
        return "", None
    try:
        # Accept e.g. "2025-09-12T18:00:00Z" or with offset
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        iso = dt.isoformat().replace("+00:00", "Z")
        return iso, dt
    except Exception:
        return raw, None

async def fetch_attendance_rows():
    client = spond.SpondClient()
    await client.login(os.environ["SPOND_USERNAME"], os.environ["SPOND_PASSWORD"])

    now = datetime.now(timezone.utc)
    after = now - timedelta(days=PAST_DAYS)
    before = now + timedelta(days=FUTURE_DAYS)

    events = await client.get_events(after=after.isoformat(), before=before.isoformat())

    rows = []
    for ev in events:
        title = (ev.get("title") or "").strip()
        if title != EVENT_NAME_FILTER:
            continue

        start_iso, start_dt = _parse_event_start_iso(ev)
        # Hard date filter: only events strictly after 2025-07-01T00:00:00Z
        if not start_dt or start_dt <= EVENT_START_MIN:
            continue

        event_id = ev["id"]

        details = await client.get_event(event_id)
        participants = details.get("participants") or details.get("members") or []

        for p in participants:
            member_id = str(p.get("memberId") or p.get("id") or "")
            member_name = p.get("name") or f"ID:{member_id}"
            status = (p.get("status") or p.get("attendance") or "").strip()
            reason = (p.get("absenceReason") or p.get("comment") or "").strip()
            checked_in = bool(p.get("checkedIn") or p.get("isCheckedIn") or False)
            response_at = p.get("respondedAt") or p.get("updatedAt") or ""

            rows.append([
                event_id, title, start_iso, member_id, member_name,
                status, reason, "TRUE" if checked_in else "FALSE", response_at,
                "spond-sync",
                datetime.utcnow().isoformat(timespec="seconds") + "Z",
                ""  # ManualOverride (blank by default)
            ])

    await client.close()
    return rows

def upsert(ws, new_rows):
    idx = index_existing(ws)
    updates = []
    appends = []

    for r in new_rows:
        key = (r[0], r[3])  # (EventID, MemberID)
        if key in idx:
            row_index, manual_override_val = idx[key]
            if is_truthy(manual_override_val):
                continue  # keep user's manual edits
            current_mo = ws.cell(row_index, HEADER.index("ManualOverride")+1).value or ""
            r_to_write = r.copy()
            r_to_write[-1] = current_mo
            # Range like A{row}:L{row}
            end_col_idx = len(HEADER)  # 12 -> L
            end_col_letter = chr(ord('A') + end_col_idx - 1)
            rng = f"A{row_index}:{end_col_letter}{row_index}"
            updates.append({"range": rng, "values": [r_to_write]})
        else:
            appends.append(r)

    if updates:
        ws.batch_update(updates)
    if appends:
        ws.append_rows(appends, value_input_option="RAW")

async def main():
    rows = await fetch_attendance_rows()
    gc = sheets_client_from_env()
    ws = get_ws(gc)
    ensure_header(ws)
    upsert(ws, rows)

if __name__ == "__main__":
    asyncio.run(main())
