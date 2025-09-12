# attendance_sync.py
# Sync Spond attendance -> Google Sheets (one row per member per event)
#
# pip install: spond gspread google-auth pandas openpyxl
# Secrets: SPOND_USERNAME, SPOND_PASSWORD, GOOGLE_SERVICE_ACCOUNT_JSON, SHEET_ID

import os, json, asyncio, io, traceback
from datetime import datetime, timezone, timedelta
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from spond import spond

# ---------- SETTINGS ----------
SHEET_ID = os.environ["SHEET_ID"]
TAB_NAME = "Attendance"

# Filters
EVENT_NAME_FILTER = os.environ.get("EVENT_NAME_FILTER", "Istrening U18B")
GROUP_NAME_FILTER = os.environ.get("GROUP_NAME_FILTER", "IHKS G2008b/G2009b")

# Include events ON/AFTER this timestamp (inclusive)
EVENT_START_MIN = datetime(2025, 7, 1, 0, 0, 0, tzinfo=timezone.utc)

# Title/group matching modes: exact | iexact | contains | startswith
MATCH_MODE = os.environ.get("MATCH_MODE", "iexact")
GROUP_MATCH_MODE = os.environ.get("GROUP_MATCH_MODE", "iexact")
# ------------------------------

HEADER = [
    "EventID","EventName","EventStartIso","MemberID","MemberName",
    "Status","AbsenceReason","CheckedIn","ResponseAtIso",
    "Source","LastSyncedAtIso","ManualOverride"
]
TRUTHY = {"true","yes","1","y","ja","t"}

def log(msg: str): print(f"[spond-sync] {msg}", flush=True)

def _norm(s: str) -> str:
    # collapse inner spaces, strip ends, lower
    return " ".join((s or "").split()).strip().lower()

def matches(title: str, pattern: str, mode: str) -> bool:
    if mode == "exact":       return (title or "") == (pattern or "")
    if mode == "iexact":      return _norm(title) == _norm(pattern)
    if mode == "contains":    return _norm(pattern) in _norm(title)
    if mode == "startswith":  return _norm(title).startswith(_norm(pattern))
    return _norm(title) == _norm(pattern)

def sheets_client_from_env():
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

def get_ws(gc):
    sh = gc.open_by_key(SHEET_ID)
    # 1) Try exact
    try:
        return sh.worksheet(TAB_NAME)
    except gspread.WorksheetNotFound:
        pass
    # 2) Tolerant search
    wanted = TAB_NAME.strip().lower()
    for ws in sh.worksheets():
        if ws.title == TAB_NAME or ws.title.strip().lower() == wanted:
            return ws
    # 3) Create if missing; if API says exists, return it
    try:
        return sh.add_worksheet(title=TAB_NAME, rows=2000, cols=20)
    except gspread.exceptions.APIError as e:
        if "already exists" in str(e).lower():
            for ws in sh.worksheets():
                if ws.title.strip().lower() == wanted:
                    return ws
        raise

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
    if not raw:
        return "", None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        return dt.isoformat().replace("+00:00", "Z"), dt
    except Exception:
        return raw, None

async def pick_group(s: spond.Spond):
    """Return (group_id, group_name) for the requested GROUP_NAME_FILTER, or (None, None)."""
    try:
        groups = await s.get_groups()
    except Exception as e:
        log(f"WARNING: could not list groups: {e}")
        groups = []

    log(f"Found {len(groups)} groups on account.")
    for g in groups[:20]:
        log(f"  - {g.get('id')} | {g.get('name') or g.get('title')}")

    # Try to match by name first
    for g in groups:
        gname = g.get("name") or g.get("title") or ""
        if matches(gname, GROUP_NAME_FILTER, GROUP_MATCH_MODE):
            return (g.get("id"), gname)

    # No direct name match; return None and we'll filter by name on events
    return (None, None)

def event_in_group(ev, wanted_group_id, wanted_group_name):
    """Check group by id or by name on the event object."""
    if ev is None:
        return False
    gid = ev.get("groupId") or (ev.get("group") or {}).get("id") or ev.get("group_id")
    gname = (ev.get("group") or {}).get("name") or ev.get("groupName") or ev.get("group_title") or ""
    if wanted_group_id and gid and str(gid) == str(wanted_group_id):
        return True
    if wanted_group_name and matches(gname, wanted_group_name, GROUP_MATCH_MODE):
        return True
    return False

async def fetch_attendance_rows():
    username = os.environ["SPOND_USERNAME"]
    password = os.environ["SPOND_PASSWORD"]
    s = spond.Spond(username=username, password=password)

    # Select the group
    wanted_gid, matched_gname = await pick_group(s)
    if matched_gname:
        log(f"Using group: {matched_gname} (id={wanted_gid})")
    else:
        log(f"Group '{GROUP_NAME_FILTER}' not found by name; will filter by name on events.")

    # Ask wide; still hard-filter locally
    min_start = EVENT_START_MIN  # inclusive
    max_start = datetime.now(timezone.utc) + timedelta(days=365)

    log(f"Fetching events (min_start={min_start.isoformat()}, max_start={max_start.isoformat()}) ...")
    try:
        # Some library versions support group_id; if not, we'll fall back
        events = await s.get_events(min_start=min_start, max_start=max_start, group_id=wanted_gid) if wanted_gid else await s.get_events(min_start=min_start, max_start=max_start)
    except TypeError:
        # Fallback: call without group filter and filter locally
        events = await s.get_events(min_start=min_start, max_start=max_start)

    total = len(events or [])
    log(f"Fetched {total} events total.")
    for ev in (events or [])[:25]:
        raw_start = ev.get("startTimeUtc") or ev.get("startTime") or ""
        gname = (ev.get("group") or {}).get("name") or ev.get("groupName") or ""
        log(f"  - {ev.get('id')} | {ev.get('title')} | {gname} | {raw_start}")

    # Group filter (ID or name)
    events = [ev for ev in (events or []) if event_in_group(ev, wanted_gid, GROUP_NAME_FILTER)]
    log(f"After group filter '{GROUP_NAME_FILTER}': {len(events)} events")

    # Title filter
    title_matches = [ev for ev in events if matches(ev.get('title',''), EVENT_NAME_FILTER, MATCH_MODE)]
    log(f"Title matches for '{EVENT_NAME_FILTER}' ({MATCH_MODE}): {len(title_matches)}")

    rows = []
    for ev in title_matches:
        title = (ev.get("title") or "").strip()
        start_iso, start_dt = parse_event_start_iso(ev)
        # Inclusive cutoff: start >= EVENT_START_MIN
        if not start_dt:
            log(f"Skip {ev.get('id')}: no start time parsed.")
            continue
        if start_dt < EVENT_START_MIN:
            log(f"Skip {ev.get('id')}: {start_dt.isoformat()} < cutoff {EVENT_START_MIN.isoformat()}")
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

    # Close HTTP session
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
