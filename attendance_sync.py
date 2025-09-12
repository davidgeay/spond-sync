# attendance_sync.py
# Requirements: spond gspread google-auth pandas openpyxl
#
# Secrets required (GitHub > Settings > Secrets and variables > Actions):
#   SPOND_USERNAME, SPOND_PASSWORD, GOOGLE_SERVICE_ACCOUNT_JSON, SHEET_ID
#
# Rules:
#   - Only Spond group named "IHKS G2008b/G2009b"
#   - Date range: from 2025-08-01 up to time of sync
#   - Weekdays only: Monday–Friday
#   - Local time window: 19:00–23:00 (inclusive), using TIMEZONE (default Europe/Oslo)
#   - No filtering by event title

import os, io, json, asyncio
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from spond import spond

# -------- Configuration --------
GROUP_NAME = "IHKS G2008b/G2009b"
TIMEZONE = ZoneInfo(os.getenv("TIMEZONE", "Europe/Oslo"))

DATE_MIN_LOCAL = datetime(2025, 8, 1, 0, 0, 0, tzinfo=TIMEZONE)
DATE_MIN_UTC = DATE_MIN_LOCAL.astimezone(timezone.utc)
DATE_MAX_UTC = datetime.now(timezone.utc)

WINDOW_START = time(19, 0)    # 19:00 (inclusive)
WINDOW_END   = time(23, 0)    # 23:00 (inclusive)

ALLOWED_WEEKDAYS = {0, 1, 2, 3, 4}  # Mon=0 ... Fri=4

ATT_TAB  = "Attendance"
DBG_TAB  = "Debug"

ATT_COLUMNS = [
    "Event ID", "Event Title", "Event Start (UTC)",
    "Member", "Status", "Raw Status", "Raw Reason",
    "Override Status", "Override Reason",
]
# --------------------------------

def log(msg: str): print(f"[spond-sync] {msg}", flush=True)

# ---------- Google Sheets ----------
def sheets_client() -> gspread.Client:
    svc_json = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(
        svc_json, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)

def open_spreadsheet(gc: gspread.Client):
    return gc.open_by_key(os.environ["SHEET_ID"])

def get_or_create_ws(sh, title: str):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=2000, cols=20)
    except gspread.exceptions.APIError as e:
        if "already exists" in str(e).lower():
            return sh.worksheet(title)
        raise

# ---------- Spond helpers ----------
def _pick(d: Dict[str, Any], *keys):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

def parse_start_utc(any_dict: Dict[str, Any]) -> Optional[datetime]:
    raw = _pick(any_dict,
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

def within_rules(start_utc: datetime) -> Tuple[bool, Dict[str, Any]]:
    """Return (included?, debug_fields)."""
    if start_utc.tzinfo is None:
        start_utc = start_utc.replace(tzinfo=timezone.utc)

    # Date window check (UTC)
    in_date = DATE_MIN_UTC <= start_utc <= DATE_MAX_UTC

    local = start_utc.astimezone(TIMEZONE)
    wd = local.weekday()
    in_weekday = wd in ALLOWED_WEEKDAYS

    t = local.time()
    in_time = (t >= WINDOW_START) and (t <= WINDOW_END)

    included = in_date and in_weekday and in_time
    window_label = f"Time {WINDOW_START.strftime('%H:%M')}-{WINDOW_END.strftime('%H:%M')} OK"
    debug = {
        "Start UTC": start_utc.isoformat(),
        "Start Local": local.isoformat(),
        "Local Weekday (0=Mon)": wd,
        "Local Time": local.strftime("%H:%M"),
        "In Date Window": "Yes" if in_date else "No",
        "Weekday OK": "Yes" if in_weekday else "No",
        window_label: "Yes" if in_time else "No",
        "Included": "Yes" if included else "No",
    }
    return included, debug

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
    # Read first sheet; Spond formats vary a bit
    df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=0)
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
    if name_col:   slim.rename(columns={name_col: "Member"}, inplace=True)
    if status_col: slim.rename(columns={status_col: "Raw Status"}, inplace=True)
    else:          slim["Raw Status"] = ""
    if reason_col: slim.rename(columns={reason_col: "Raw Reason"}, inplace=True)
    else:          slim["Raw Reason"] = ""

    for c in ["Member", "Raw Status", "Raw Reason"]:
        if c in slim.columns:
            slim[c] = slim[c].astype(str).fillna("")

    return slim[["Member", "Raw Status", "Raw Reason"]]

async def resolve_group_id(sp: spond.Spond) -> Optional[str]:
    groups = await sp.get_groups()
    log(f"Found {len(groups)} groups.")
    gid = None
    for g in groups:
        gname = _pick(g, "name", "title", "groupName") or ""
        this_id = _pick(g, "id", "groupId", "uid")
        log(f"  - {this_id} | {gname}")
        if gname.strip().lower() == GROUP_NAME.strip().lower():
            gid = this_id
    if gid:
        log(f"Using group: {GROUP_NAME} (id={gid})")
    else:
        log(f"ERROR: Group '{GROUP_NAME}' not found.")
    return gid

# ---------- Fetch & Build Rows ----------
async def fetch_rows() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    username = os.environ["SPOND_USERNAME"]
    password = os.environ["SPOND_PASSWORD"]
    sp = spond.Spond(username=username, password=password)

    gid = await resolve_group_id(sp)
    if not gid:
        await sp.clientsession.close()
        return [], []

    log(f"Fetching events (UTC {DATE_MIN_UTC.isoformat()} → {DATE_MAX_UTC.isoformat()}) ...")
    try:
        events = await sp.get_events(
            group_id=gid,
            min_start=DATE_MIN_UTC,
            max_start=DATE_MAX_UTC,
            include_scheduled=True,
            max_events=500
        )
    except TypeError:
        # Older lib: no group_id on get_events
        events = await sp.get_events(min_start=DATE_MIN_UTC, max_start=DATE_MAX_UTC)

    log(f"Fetched {len(events)} events (list view may be minimal).")

    att_rows: List[Dict[str, Any]] = []
    dbg_rows: List[Dict[str, Any]] = []

    for ev in events:
        eid = _pick(ev, "id", "eventId", "uid")
        if not eid:
            continue

        # Full details for reliable fields
        try:
            details = await sp.get_event(eid)
        except Exception as e:
            log(f"WARNING: get_event failed for {eid}: {e}")
            continue

        title = _pick(details, "title", "name", "eventName", "subject") or ""
        start_utc = parse_start_utc(details)
        start_disp = start_utc.isoformat() if start_utc else "NO-START"
        log(f"  • {eid} | {title or 'NO-TITLE'} | {start_disp}")

        if start_utc:
            include, dbg = within_rules(start_utc)
        else:
            include = False
            dbg = {
                "Start UTC": "NO-START",
                "Start Local": "",
                "Local Weekday (0=Mon)": "",
                "Local Time": "",
                "In Date Window": "No",
                "Weekday OK": "No",
                f"Time {WINDOW_START.strftime('%H:%M')}-{WINDOW_END.strftime('%H:%M')} OK": "No",
                "Included": "No",
            }

        dbg_rows.append({
            "Event ID": eid,
            "Event Title": title,
            **dbg
        })

        if not include:
            continue

        # Attendance table (prefer XLSX)
        try:
            xlsx = await sp.get_event_attendance_xlsx(eid)
            table = read_attendance_xlsx(xlsx)
        except Exception as e:
            log(f"WARNING: XLSX not available for {eid}: {e}")
            table = pd.DataFrame(columns=["Member", "Raw Status", "Raw Reason"])
            # Fallback: participants structure, if any
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
        table.insert(0, "Event ID", eid)
        table.insert(1, "Event Title", title)
        table.insert(2, "Event Start (UTC)", start_utc.isoformat())

        if "Override Status" not in table.columns:
            table["Override Status"] = ""
        if "Override Reason" not in table.columns:
            table["Override Reason"] = ""

        table = table[ATT_COLUMNS]
        att_rows.extend(table.to_dict(orient="records"))

    # Close http session
    if hasattr(sp, "clientsession") and sp.clientsession:
        await sp.clientsession.close()

    log(f"Prepared {len(att_rows)} attendance rows.")
    return att_rows, dbg_rows

# ---------- Write to Sheets ----------
def upsert_attendance(ws, rows: List[Dict[str, Any]]):
    """Write full header always; preserve override columns by (Event ID + Member)."""
    existing = ws.get_all_records()
    existing_df = pd.DataFrame(existing) if existing else pd.DataFrame(columns=ATT_COLUMNS)
    new_df = pd.DataFrame(rows)

    if new_df.empty:
        new_df = pd.DataFrame(columns=ATT_COLUMNS)
    else:
        for col in ATT_COLUMNS:
            if col not in new_df.columns:
                new_df[col] = ""
        new_df = new_df[ATT_COLUMNS]

        # Preserve overrides
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

    # Sort by date then member
    if "Event Start (UTC)" in new_df.columns:
        new_df["_dt"] = pd.to_datetime(new_df["Event Start (UTC)"], errors="coerce", utc=True)
        new_df.sort_values(by=["_dt", "Member"], inplace=True)
        new_df.drop(columns=["_dt"], inplace=True)

    ws.clear()
    ws.update([ATT_COLUMNS] + new_df.fillna("").values.tolist())
    log("Attendance sheet updated.")

def write_debug(ws_dbg, dbg_rows: List[Dict[str, Any]]):
    if not dbg_rows:
        # Write header with a hint row
        cols = [
            "Event ID","Event Title","Start UTC","Start Local","Local Weekday (0=Mon)",
            "Local Time","In Date Window","Weekday OK",f"Time {WINDOW_START.strftime('%H:%M')}-{WINDOW_END.strftime('%H:%M')} OK","Included"
        ]
        ws_dbg.clear()
        ws_dbg.update([cols, ["(no events found in API window)", "", "", "", "", "", "", "", "", ""]])
        log("Debug sheet updated (no events).")
        return

    # Build a stable column order
    all_keys = [
        "Event ID","Event Title","Start UTC","Start Local","Local Weekday (0=Mon)",
        "Local Time","In Date Window","Weekday OK",f"Time {WINDOW_START.strftime('%H:%M')}-{WINDOW_END.strftime('%H:%M')} OK","Included"
    ]
    rows = []
    for r in dbg_rows:
        rows.append([r.get(k, "") for k in all_keys])

    ws_dbg.clear()
    ws_dbg.update([all_keys] + rows)
    log("Debug sheet updated.")

# ---------- Entrypoint ----------
async def main():
    gc = sheets_client()
    sh = open_spreadsheet(gc)
    ws_att = get_or_create_ws(sh, ATT_TAB)
    ws_dbg = get_or_create_ws(sh, DBG_TAB)

    att_rows, dbg_rows = await fetch_rows()
    upsert_attendance(ws_att, att_rows)
    write_debug(ws_dbg, dbg_rows)
    log("Sync complete.")

if __name__ == "__main__":
    asyncio.run(main())
