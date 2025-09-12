# attendance_sync.py
# pip install: spond gspread google-auth pandas openpyxl

import os, json, asyncio, io, traceback
from datetime import datetime, timezone, timedelta
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from spond import spond

SHEET_ID = os.environ["SHEET_ID"]
TAB_NAME = "Attendance"
EVENT_NAME_FILTER = "Istrening U18B"
# Only take events strictly AFTER 2025-07-01
EVENT_START_MIN = datetime(2025, 7, 1, 0, 0, 0, tzinfo=timezone.utc)

HEADER = [
    "EventID","EventName","EventStartIso","MemberID","MemberName",
    "Status","AbsenceReason","CheckedIn","ResponseAtIso",
    "Source","LastSyncedAtIso","ManualOverride"
]
TRUTHY = {"true","yes","1","y","ja","t"}

def log(msg): print(f"[spond-sync] {msg}", flush=True)

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
        return sh.add_worksheet(title=TAB_NAME, rows=2000, cols=20)

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

def parse_event_start_iso(ev):
    raw = ev.get("startTimeUtc") or ev.get("startTime") or ""
    if not raw: return "", None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        return dt.isoformat().replace("+00:00","Z"), dt
    except Exception:
        return raw, None

async def fetch_attendance_rows():
    username = os.environ["SPOND_USERNAME"]
    password = os.environ["SPOND_PASSWORD"]
    s = spond.Spond(username=username, password=password)

    # ✅ FIX: pass datetimes, not strings
    min_start = EVENT_START_MIN            # inclusive on API side; we still hard-filter below
    max_start = datetime.now(timezone.utc) + timedelta(days=365)

    log(f"Fetching events (min_start={min_start.isoformat()}, max_start={max_start.isoformat()}) ...")
    events = await s.get_events(min_start=min_start, max_start=max_start)

    rows = []
    for ev in events or []:
        title = (ev.get("title") or "").strip()
        if title != EVENT_NAME_FILTER:
            continue
        start_iso, start_dt = parse_event_start_iso(ev)
        # Hard filter: strictly after 2025-07-01
        if not start_dt or start_dt <= EVENT_START_MIN:
            continue

        event_id = ev["id"]
        log(f"Event {event_id} | {title} | {start_iso}")

        # Try official XLSX first
        try:
            xlsx_bytes = await s.get_event_attendance_xlsx(event_id)
            df = pd.read_excel(io.BytesIO(xlsx_bytes))
        except Exception as e:
            log(f"WARNING: XLSX not available for {event_id}: {e}")
            details = await s.get_event(event_id) if hasattr(s, "get_event") else {}
            participants = (details.get("participants") or details.get("members") or [])
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
                    "spond-sync", datetime.utcnow().isoformat(timespec="seconds")+"Z", ""
                ])
            continue

        # Normalize XLSX columns
        cols = {c.lower(): c for c in df.columns}
        def col(*names):
            for n in names:
                if n in cols: return cols[n]
            return None
        name_col = col("name","member","member name")
        status_col = col("status","attendance")
        reason_col = col("reason","absence reason","comment","comments")
        checkin_col = col("checked in","checked_in","checkedin")
        response_col = col("response at","responded at","updated at")
        member_id_col = col("member id","member_id","id")

        for _, r in df.iterrows():
            member_name = str(r.get(name_col,"") if name_col else "").strip()
            member_id = str(r.get(member_id_col,"") if member_id_col else member_name).strip()
            status = str(r.get(status_col,"") if status_col else "").strip()
            reason = str(r.get(reason_col,"") if reason_col else "").strip()
            checked_in = str(r.get(checkin_col,"") if checkin_col else "").strip().lower() in TRUTHY
            response_at = str(r.get(response_col,"") if response_col else "").strip()
            rows.append([
                event_id, title, start_iso, member_id or member_name, member_name or f"ID:{member_id}",
                status, reason, "TRUE" if checked_in else "FALSE", response_at,
                "spond-sync", datetime.utcnow().isoformat(timespec="seconds")+"Z", ""
            ])

    # ✅ Ensure HTTP session is closed even if something errors
    if hasattr(s, "close"):
        await s.close()
    elif hasattr(s, "clientsession") and s.clientsession:
        await s.clientsession.close()

    log(f"Prepared {len(rows)} rows.")
    return rows

def upsert(ws, new_rows):
    idx = index_existing(ws)
    updates, appends = [], []
    for r in new_rows:
        key = (r[0], r[3])  # (EventID, MemberID)
        if key in idx:
            row_index, manual_override_val = idx[key]
            if is_truthy(manual_override_val):
                continue
            current_mo = ws.cell(row_index, HEADER.index("ManualOverride")+1).value or ""
            r_to_write = r.copy()
            r_to_write[-1] = current_mo
            end_col_letter = chr(ord('A') + len(HEADER) - 1)  # L
            updates.append({"range": f"A{row_index}:{end_col_letter}{row_index}", "values": [r_to_write]})
        else:
            appends.append(r)
    if updates:
        log(f"Updating {len(updates)} existing rows ...")
        ws.batch_update(updates)
    if appends:
        log(f"Appending {len(appends)} new rows ...")
        ws.append_rows(appends, value_input_option="RAW")

async def main():
    try:
        rows = await fetch_attendance_rows()
        gc = sheets_client_from_env()
        ws = get_ws(gc)
        ensure_header(ws)
        upsert(ws, rows)
        log("Sync complete.")
    except Exception as e:
        log("ERROR:\n" + "".join(traceback.format_exception(e)))
        raise

if __name__ == "__main__":
    asyncio.run(main())
