# attendance_sync.py
# Requirements installed by the workflow: spond gspread google-auth pandas openpyxl
#
# Secrets required in the repo:
#   SPOND_USERNAME, SPOND_PASSWORD, GOOGLE_SERVICE_ACCOUNT_JSON, SHEET_ID
#
# Optional env (already set in the workflow below):
#   TIMEZONE  -> local tz for weekday/time filtering (default Europe/Oslo)

import os, io, json, asyncio
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from spond import spond

# ---------- CONFIG ----------
GROUP_NAME_FILTER = "IHKS G2008b/G2009b"   # exact group to sync
LOCAL_TZ = ZoneInfo(os.getenv("TIMEZONE", "Europe/Oslo"))

# Date window: from 1 Aug 2025 (local midnight) up to now
DATE_MIN_LOCAL = datetime(2025, 8, 1, 0, 0, 0, tzinfo=LOCAL_TZ)
DATE_MIN_UTC = DATE_MIN_LOCAL.astimezone(timezone.utc)
DATE_MAX_UTC = datetime.now(timezone.utc)

# Time-of-day window in LOCAL_TZ (inclusive)
WINDOW_START = time(19, 0)   # 19:00
WINDOW_END   = time(21, 30)  # 21:30

# Weekdays allowed (Mon=0 ... Sun=6)
ALLOWED_WEEKDAYS = {0, 1, 2, 3, 4}

SHEET_TAB = "Attendance"

ALL_COLUMNS = [
    "Event ID", "Event Title", "Event Start (UTC)",
    "Member", "Status", "Raw Status", "Raw Reason",
    "Override Status", "Override Reason",
]
# ---------------------------

def log(msg: str): print(f"[spond-sync] {msg}", flush=True)

def auth_sheets() -> gspread.Client:
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)

def get_ws(gc: gspread.Client):
    sh = gc.open_by_key(os.environ["SHEET_ID"])
    try:
        return sh.worksheet(SHEET_TAB)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=SHEET_TAB, rows=2000, cols=20)
    except gspread.exceptions.APIError as e:
        # If API races: fall back to fetching it
        if "already exists" in str(e).lower():
            return sh.worksheet(SHEET_TAB)
        raise

def _extract(d: Dict[str, Any], *keys):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

def parse_event_start(any_dict: Dict[str, Any]) -> Optional[datetime]:
    raw = _extract(any_dict,
        "startTimeUtc", "start_time_utc", "startTime", "start",
        "startAt", "start_at", "startDateTime", "start_datetime"
    )
    if not raw:
        return None
    try:
        s = str(raw).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def within_rules(start_utc: datetime) -> bool:
    """Apply weekday + time window (in LOCAL_TZ) and the date window."""
    if start_utc.tzinfo is None:
        start_utc = start_utc.replace(tzinfo=timezone.utc)
    # Date window (UTC, then reconfirm in local just to be safe)
    if not (DATE_MIN_UTC <= start_utc <= DATE_MAX_UTC):
        return False

    local = start_utc.astimezone(LOCAL_TZ)
    if local.weekday() not in ALLOWED_WEEKDAYS:
        return False

    # Inclusive time window
    t = local.time()
    return (t >= WINDOW_START) and (t <= WINDOW_END)

def normalize_status(raw: str) -> str:
    r = (raw or "").strip().lower()
    if r in {"yes", "attending", "accepted", "present"}:
        return "Present"
    if r in {"no", "declined", "absent"}:
        return "Absent"
    if "late" in r:
        return "Late"
    if r in {"unknown", "maybe", "no response"}:
        return "No response"
    return (raw or "").strip()

def read_attendance_xlsx(xlsx_bytes: bytes) -> pd.DataFrame:
    # Let pandas infer the table; Spond exports vary a bit by locale/template
    df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=0)

    # Normalize column names
    df.columns = [str(c).strip() for c in df.columns]

    def pick(*opts):
        for o in opts:
            if o in df.columns:
                return o
        return None

    name_col   = pick("Name", "Member name", "Member", "Participant", "Navn", "Deltaker")
    status_col = pick("Status", "Response", "Attending", "Svar", "Attendance")
    reason_col = pick("Note", "Reason", "Absence reason", "Kommentar", "Notes")

    keep = [c for c in (name_col, status_col, reason_col) if c]
    if not keep:
        return pd.DataFrame(columns=["Member", "Raw Status", "Raw Reason"])

    slim = df[keep].copy()
    slim.rename(columns={name_col: "Member"} if name_col else {}, inplace=True)
    if status_col:
        slim.rename(columns={status_col: "Raw Status"}, inplace=True)
    else:
        slim["Raw Status"] = ""
    if reason_col:
        slim.rename(columns={reason_col: "Raw Reason"}, inplace=True)
    else:
        slim["Raw Reason"] = ""

    for c in ["Member", "Raw Status", "Raw Reason"]:
        if c in slim.columns:
            slim[c] = slim[c].astype(str).fillna("")

    return slim[["Member", "Raw Status", "Raw Reason"]]

async def fetch_group_id(sp: spond.Spond) -> Optional[str]:
    groups = await sp.get_groups()
    log(f"Found {len(groups)} groups.")
    gid = None
    for g in groups:
        gname = _extract(g, "name", "title", "groupName") or ""
        this_id = _extract(g, "id", "groupId", "uid")
        log(f"  - {this_id} | {gname}")
        if gname.strip().lower() == GROUP_NAME_FILTER.strip().lower():
            gid = this_id
    if gid:
        log(f"Using group: {GROUP_NAME_FILTER} (id={gid})")
    else:
        log(f"ERROR: Group '{GROUP_NAME_FILTER}' not found.")
    return gid

async def fetch_rows() -> List[Dict[str, Any]]:
    username = os.environ["SPOND_USERNAME"]
    password = os.environ["SPOND_PASSWORD"]
    sp = spond.Spond(username=username, password=password)

    gid = await fetch_group_id(sp)
    if not gid:
        await sp.clientsession.close()
        return []

    log(f"Fetching events (UTC window {DATE_MIN_UTC.isoformat()} → {DATE_MAX_UTC.isoformat()}) ...")
    try:
        events = await sp.get_events(
            group_id=gid,
            min_start=DATE_MIN_UTC,
            max_start=DATE_MAX_UTC,
            include_scheduled=True,
            max_events=500
        )
    except TypeError:
        # Older lib: no group_id param on get_events; fetch all then filter
        events = await sp.get_events(min_start=DATE_MIN_UTC, max_start=DATE_MAX_UTC)

    log(f"Fetched {len(events)} events (list view may be minimal).")

    out_rows: List[Dict[str, Any]] = []
    checked = 0

    for ev in events:
        eid = _extract(ev, "id", "eventId", "uid")
        if not eid:
            continue

        # Always fetch full details to get reliable start time
        try:
            details = await sp.get_event(eid)
        except Exception as e:
            log(f"WARNING: get_event failed for {eid}: {e}")
            continue

        checked += 1
        title = _extract(details, "title", "name", "eventName", "subject") or ""
        start_utc = parse_event_start(details)
        start_disp = start_utc.isoformat() if start_utc else "NO-START"
        log(f"  • {eid} | {title or 'NO-TITLE'} | {start_disp}")

        if not start_utc or not within_rules(start_utc):
            continue

        # Pull attendance XLSX (preferred)
        try:
            xlsx = await sp.get_event_attendance_xlsx(eid)
            table = read_attendance_xlsx(xlsx)
        except Exception as e:
            log(f"WARNING: XLSX not available for {eid}: {e}")
            table = pd.DataFrame(columns=["Member", "Raw Status", "Raw Reason"])
            # Fallback: participants structure if present
            participants = details.get("participants") or details.get("members") or []
            for p in participants:
                table.loc[len(table)] = [
                    p.get("name") or f"ID:{p.get('memberId') or p.get('id') or ''}",
                    str(p.get("status") or p.get("attendance") or ""),
                    str(p.get("absenceReason") or p.get("comment") or "")
                ]

        if table.empty:
            continue

        table["Status"] = table["Raw Status"].map(normalize_status)

        # Assemble final columns
        table.insert(0, "Event ID", eid)
        table.insert(1, "Event Title", title)
        table.insert(2, "Event Start (UTC)", start_utc.isoformat())

        if "Override Status" not in table.columns:
            table["Override Status"] = ""
        if "Override Reason" not in table.columns:
            table["Override Reason"] = ""

        table = table[[
            "Event ID", "Event Title", "Event Start (UTC)",
            "Member", "Status", "Raw Status", "Raw Reason",
            "Override Status", "Override Reason",
        ]]

        out_rows.extend(table.to_dict(orient="records"))

    await sp.clientsession.close()
    log(f"Checked details for {checked} events.")
    log(f"Prepared {len(out_rows)} rows.")
    return out_rows

def upsert_to_sheet(ws, rows: List[Dict[str, Any]]):
    """Write full header always; keep your manual overrides when re-syncing."""
    existing = ws.get_all_records()
    existing_df = pd.DataFrame(existing) if existing else pd.DataFrame(columns=ALL_COLUMNS)
    new_df = pd.DataFrame(rows)

    if new_df.empty:
        new_df = pd.DataFrame(columns=ALL_COLUMNS)
    else:
        for col in ALL_COLUMNS:
            if col not in new_df.columns:
                new_df[col] = ""
        new_df = new_df[ALL_COLUMNS]

        # Preserve overrides by (Event ID + Member)
        key = ["Event ID", "Member"]
        for col in ["Override Status", "Override Reason"]:
            if col in existing_df.columns:
                merged = pd.merge(
                    new_df,
                    existing_df[key + [col]],
                    on=key,
                    how="left",
                    suffixes=("", "_old"),
                )
                merged[col] = merged[col].where(
                    merged[col].astype(str).str.len() > 0, merged[f"{col}_old"]
                )
                new_df = merged.drop(columns=[f"{col}_old"])

    # Sort by date then member (nice to read)
    if "Event Start (UTC)" in new_df.columns:
        new_df["_dt"] = pd.to_datetime(new_df["Event Start (UTC)"], errors="coerce", utc=True)
        new_df.sort_values(by=["_dt", "Member"], inplace=True)
        new_df.drop(columns=["_dt"], inplace=True)

    ws.clear()
    ws.update([ALL_COLUMNS] + new_df.fillna("").values.tolist())
    log("Sheet updated.")

async def main():
    gc = auth_sheets()
    ws = get_ws(gc)
    rows = await fetch_rows()
    upsert_to_sheet(ws, rows)

if __name__ == "__main__":
    asyncio.run(main())
