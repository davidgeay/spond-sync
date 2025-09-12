# attendance_sync.py
# Secrets needed: SPOND_USERNAME, SPOND_PASSWORD, GOOGLE_SERVICE_ACCOUNT_JSON, SHEET_ID
# Requires: spond gspread google-auth pandas openpyxl python-dateutil

import os, io, json, asyncio, re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dateutil import parser as dtparser
import gspread
from google.oauth2.service_account import Credentials
from spond import spond

# ---------------- Config ----------------
GROUP_NAME = "IHKS G2008b/G2009b"
TIMEZONE = ZoneInfo(os.getenv("TIMEZONE", "Europe/Oslo"))

# Date window: from 2025-08-01 inclusive up to run time (UTC)
DATE_MIN_LOCAL = datetime(2025, 8, 1, 0, 0, 0, tzinfo=TIMEZONE)
DATE_MIN_UTC = DATE_MIN_LOCAL.astimezone(timezone.utc)
DATE_MAX_UTC = datetime.now(timezone.utc)

ATT_TAB = "Attendance"
DBG_TAB = "Debug"

ATT_COLUMNS = [
    "Event ID", "Event Title", "Event Start (UTC)",
    "Member", "Status", "Raw Status", "Raw Reason",
    "Override Status", "Override Reason",
]

# Only the word "istrening" (any case). Matches "Istrening U18B", etc.
ISTR_PAT = re.compile(r"\bistrening\b", re.IGNORECASE)

def log(msg: str): print(f"[spond-sync] {msg}", flush=True)

# ---------------- Google Sheets ----------------
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

# ---------------- Utilities ----------------
def _pick(d: Dict[str, Any], *keys):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

def parse_start_utc(d: Dict[str, Any]) -> Optional[datetime]:
    """Try common start keys; tolerate strings or epoch seconds."""
    raw = _pick(d,
        "startTimeUtc", "start_time_utc", "startTime", "start",
        "startAt", "start_at", "startDateTime", "start_datetime",
        "utcStart", "utc_start", "startTimestamp", "start_timestamp"
    )
    if raw is None:
        return None
    try:
        # epoch?
        if isinstance(raw, (int, float)) and raw > 1_000_000_000:
            dt = datetime.fromtimestamp(float(raw), tz=timezone.utc)
            return dt
        # string ISO?
        s = str(raw).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def to_flat_text(value: Any) -> str:
    """Flatten dict/list/bytes/etc into a single searchable string."""
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="ignore")
        except Exception:
            return ""
    if isinstance(value, dict):
        parts = []
        for k, v in value.items():
            parts.append(to_flat_text(k))
            parts.append(to_flat_text(v))
        return " | ".join([p for p in parts if p])
    if isinstance(value, (list, tuple, set)):
        return " | ".join([to_flat_text(v) for v in value if v is not None])
    return str(value)

def contains_istrening(*values: Any) -> bool:
    for v in values:
        text = to_flat_text(v)
        if text and ISTR_PAT.search(text):
            return True
    return False

def parse_datetime_from_text(text: str) -> Optional[datetime]:
    try:
        dt = dtparser.parse(text, fuzzy=True, dayfirst=True)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TIMEZONE)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def xlsx_all_text(xlsx_bytes: bytes, max_rows_per_sheet: int = 200) -> str:
    """Collect text from the first ~200 rows of each sheet (broader than just header)."""
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(xlsx_bytes), data_only=True)
        chunks = []
        for ws in wb.worksheets:
            rows = min(ws.max_row or 0, max_rows_per_sheet)
            if rows == 0:
                continue
            for r in ws.iter_rows(min_row=1, max_row=rows, values_only=True):
                for cell in r:
                    if isinstance(cell, str):
                        c = cell.strip()
                        if c:
                            chunks.append(c)
        return "\n".join(chunks)
    except Exception:
        return ""

def read_attendance_xlsx(xlsx_bytes: bytes) -> pd.DataFrame:
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

# ---------------- Core logic ----------------
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

async def fetch_rows() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (attendance_rows, debug_rows)
    """
    username = os.environ["SPOND_USERNAME"]
    password = os.environ["SPOND_PASSWORD"]
    sp = spond.Spond(username=username, password=password)

    try:
        gid = await resolve_group_id(sp)
        if not gid:
            return [], []

        log(f"Fetching events (UTC {DATE_MIN_UTC.isoformat()} → {DATE_MAX_UTC.isoformat()}) ...")
        # Server-side windowing ensures we only get events from 2025-08-01 → now.
        try:
            events = await sp.get_events(
                group_id=gid,
                min_start=DATE_MIN_UTC,
                max_start=DATE_MAX_UTC,
                include_scheduled=True,
                max_events=500
            )
        except TypeError:
            events = await sp.get_events(min_start=DATE_MIN_UTC, max_start=DATE_MAX_UTC)

        log(f"Fetched {len(events)} events (list may be minimal).")

        att_rows: List[Dict[str, Any]] = []
        dbg_rows: List[Dict[str, Any]] = []

        for ev in events:
            eid = _pick(ev, "id", "eventId", "uid")
            if not eid:
                continue

            # Always fetch full details
            try:
                details = await sp.get_event(eid)
            except Exception as e:
                log(f"WARNING: get_event failed for {eid}: {e}")
                continue

            title = _pick(details, "title", "name", "eventName", "subject")
            start_utc = parse_start_utc(details)
            start_disp = start_utc.isoformat() if start_utc else "NO-START"
            log(f"  • {eid} | {(title if isinstance(title,str) else 'NO-TITLE') or 'NO-TITLE'} | {start_disp}")

            matched_source = ""
            included = False
            resolved_start = start_utc

            # 1) Search ALL event data text (flattened)
            if contains_istrening(details):
                matched_source = "details"
                included = True

            # 2) If not matched yet, scan the entire attendance XLSX text
            xlsx_text = ""
            if not included:
                try:
                    xlsx = await sp.get_event_attendance_xlsx(eid)
                    xlsx_text = xlsx_all_text(xlsx)
                    if contains_istrening(xlsx_text):
                        matched_source = "xlsx"
                        included = True
                    # If no start found, attempt to infer from XLSX text
                    if not resolved_start:
                        guess = parse_datetime_from_text(xlsx_text)
                        if guess:
                            resolved_start = guess
                            start_disp = resolved_start.isoformat()
                except Exception as e:
                    xlsx_text = f"(xlsx unavailable: {e})"

            # 3) Cut-off date rule:
            #    If we couldn't resolve a start, still accept — get_events already applied the window.
            date_ok = True if resolved_start is None else (DATE_MIN_UTC <= resolved_start <= DATE_MAX_UTC)
            included = included and date_ok

            dbg_rows.append({
                "Event ID": eid,
                "Event Title": to_flat_text(title)[:200],
                "Start UTC": start_disp,
                "Matched (details/xlsx)": matched_source or "no",
                "Cutoff Date OK": "Yes" if date_ok else "No",
                "Included": "Yes" if included else "No",
            })

            if not included:
                continue

            # Attendance table (prefer XLSX)
            table = pd.DataFrame(columns=["Member", "Raw Status", "Raw Reason"])
            try:
                xlsx = await sp.get_event_attendance_xlsx(eid)
                table = read_attendance_xlsx(xlsx)
            except Exception as e:
                log(f"WARNING: XLSX not available for {eid}: {e}")
                participants = details.get("participants") or details.get("members") or []
                for p in participants:
                    table.loc[len(table)] = [
                        to_flat_text(p.get("name")) or f"ID:{p.get('memberId') or p.get('id') or ''}",
                        str(p.get("status") or p.get("attendance") or ""),
                        to_flat_text(p.get("absenceReason") or p.get("comment") or "")
                    ]

            if table.empty:
                continue

            table["Status"] = table["Raw Status"].map(normalize_status)
            table.insert(0, "Event ID", eid)
            table.insert(1, "Event Title", to_flat_text(title) or "(no title)")
            table.insert(2, "Event Start (UTC)", (resolved_start or start_utc or DATE_MIN_UTC).isoformat())

            if "Override Status" not in table.columns:
                table["Override Status"] = ""
            if "Override Reason" not in table.columns:
                table["Override Reason"] = ""

            table = table[ATT_COLUMNS]
            att_rows.extend(table.to_dict(orient="records"))

        log(f"Prepared {len(att_rows)} attendance rows.")
        return att_rows, dbg_rows

    finally:
        try:
            if sp and getattr(sp, "clientsession", None):
                await sp.clientsession.close()
        except Exception:
            pass

# ---------------- Write to Sheets ----------------
def upsert_attendance(ws, rows: List[Dict[str, Any]]):
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

    if "Event Start (UTC)" in new_df.columns:
        new_df["_dt"] = pd.to_datetime(new_df["Event Start (UTC)"], errors="coerce", utc=True)
        new_df.sort_values(by=["_dt", "Member"], inplace=True)
        new_df.drop(columns=["_dt"], inplace=True)

    ws.clear()
    ws.update([ATT_COLUMNS] + new_df.fillna("").values.tolist())
    log("Attendance sheet updated.")

def write_debug(ws_dbg, dbg_rows: List[Dict[str, Any]]):
    cols = ["Event ID", "Event Title", "Start UTC", "Matched (details/xlsx)", "Cutoff Date OK", "Included"]
    if not dbg_rows:
        ws_dbg.clear()
        ws_dbg.update([cols, ["(no events in API window)", "", "", "", "", ""]])
        log("Debug sheet updated (no events).")
        return
    rows = [[r.get(c, "") for c in cols] for r in dbg_rows]
    ws_dbg.clear()
    ws_dbg.update([cols] + rows)
    log("Debug sheet updated.")

# ---------------- Entrypoint ----------------
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
