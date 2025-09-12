import os
import io
import json
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
from spond import spond

TAB_NAME = "Attendance"

# -------- Helpers: env & matching --------

def env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}

EVENT_NAME_FILTER = os.getenv("EVENT_NAME_FILTER", "Istrening U18B").strip()
GROUP_NAME_FILTER = os.getenv("GROUP_NAME_FILTER", "").strip()
MATCH_MODE = os.getenv("MATCH_MODE", "iexact").strip().lower()  # iexact | icontains | equals | contains
FALLBACK_TITLE_FROM_XLSX = env_bool("FALLBACK_TITLE_FROM_XLSX", True)

# Date window
MIN_START_UTC_STR = os.getenv("MIN_START_UTC", "2025-07-01T00:00:00Z").strip()

def parse_iso_z(dt: str) -> datetime:
    # Accept forms with or without 'Z'
    s = dt.replace("Z", "+00:00")
    return datetime.fromisoformat(s).astimezone(timezone.utc)

MIN_START_UTC = parse_iso_z(MIN_START_UTC_STR)
MAX_START_UTC = datetime.now(timezone.utc)  # up to "now"

def match_text(value: str, needle: str, mode: str) -> bool:
    a = (value or "").strip()
    b = (needle or "").strip()
    if mode == "iexact":
        return a.lower() == b.lower()
    if mode == "icontains":
        return b.lower() in a.lower()
    if mode == "equals":
        return a == b
    if mode == "contains":
        return b in a
    # default strict
    return a.lower() == b.lower()

# -------- Google Sheets setup --------

def get_ws(gc: gspread.Client):
    sh = gc.open_by_key(os.environ["SHEET_ID"])
    try:
        return sh.worksheet(TAB_NAME)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=TAB_NAME, rows=2000, cols=20)
    except gspread.exceptions.APIError as e:
        # If API says the sheet exists but the index was stale
        if "already exists" in str(e):
            return sh.worksheet(TAB_NAME)
        raise

def auth_gsheets() -> gspread.Client:
    svc_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    data = json.loads(svc_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(data, scopes=scopes)
    return gspread.authorize(creds)

# -------- Spond helpers --------

def _extract_from_candidates(d: Dict[str, Any], *names: str) -> Optional[Any]:
    for n in names:
        if n in d and d[n] is not None:
            return d[n]
    return None

def _parse_event_times(any_dict: Dict[str, Any]) -> Optional[datetime]:
    # Try a bunch of likely keys
    candidates = _extract_from_candidates(
        any_dict,
        "startTimeUtc", "start_time_utc", "startTime", "start",
        "startAt", "start_at", "startDateTime", "start_datetime"
    )
    if isinstance(candidates, str):
        try:
            return parse_iso_z(candidates)
        except Exception:
            pass
    return None

def _parse_event_title(any_dict: Dict[str, Any]) -> Optional[str]:
    return _extract_from_candidates(
        any_dict,
        "title", "name", "eventName", "activityTitle", "subject"
    )

async def _get_event_title_from_xlsx(sp: spond.Spond, event_id: str) -> Optional[str]:
    """
    Download the event attendance XLSX and try to read the title from
    the first few rows (Spond usually prints meta like event name/date at the top).
    """
    try:
        xlsx_bytes: bytes = await sp.get_event_attendance_xlsx(event_id)
    except Exception:
        return None

    try:
        # Read the first sheet without assuming headers
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(xlsx_bytes), data_only=True)
        ws = wb.active
        # Look through top 10 rows for a string cell that looks like a title line
        lines = []
        for r in ws.iter_rows(min_row=1, max_row=10, values_only=True):
            for cell in r:
                if isinstance(cell, str):
                    lines.append(cell.strip())
        # Heuristic: find the longest non-empty line
        best = max((ln for ln in lines if ln), key=len, default=None)
        return best
    except Exception:
        return None

async def _event_title_matches(sp: spond.Spond, ev: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Return (matches?, resolved_title)
    Tries list fields; if missing, optionally tries XLSX header text.
    """
    # Try direct list fields
    list_title = _parse_event_title(ev)
    if list_title:
        return (match_text(list_title, EVENT_NAME_FILTER, MATCH_MODE), list_title)

    # Try details endpoint (not documented; may or may not exist)
    resolved_title = None
    if hasattr(sp, "get_event"):
        try:
            details = await sp.get_event(ev["id"])
            resolved_title = _parse_event_title(details)
            if resolved_title:
                return (match_text(resolved_title, EVENT_NAME_FILTER, MATCH_MODE), resolved_title)
        except Exception:
            pass

    # Fallback: scan XLSX header
    if FALLBACK_TITLE_FROM_XLSX:
        resolved_title = await _get_event_title_from_xlsx(sp, ev["id"])
        if resolved_title:
            return (match_text(resolved_title, EVENT_NAME_FILTER, MATCH_MODE), resolved_title)

    return (False, resolved_title)

def _read_attendance_table(xlsx_bytes: bytes) -> pd.DataFrame:
    """
    Read the Spond attendance XLSX into a normalized dataframe.
    """
    # Try pandas first; if header rows exist, pandas will still usually find the table.
    df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=0)

    # Normalize columns
    cols = {c: str(c).strip() for c in df.columns}
    df.rename(columns=cols, inplace=True)

    def pick(*options):
        for o in options:
            if o in df.columns:
                return o
        return None

    name_col   = pick("Name", "Member name", "Participant", "Navn", "Member", "Deltaker")
    status_col = pick("Status", "Response", "Attending", "Svar", "Attendance")
    reason_col = pick("Note", "Reason", "Absence reason", "Kommentar", "Notes")

    # Keep only the columns we need
    keep = [c for c in [name_col, status_col, reason_col] if c]
    if not keep:
        # Nothing recognizable: return empty frame
        return pd.DataFrame(columns=["Member", "Raw Status", "Raw Reason"])

    slim = df[keep].copy()
    # Rename to our schema
    if name_col:   slim.rename(columns={name_col: "Member"}, inplace=True)
    if status_col: slim.rename(columns={status_col: "Raw Status"}, inplace=True)
    else:          slim["Raw Status"] = ""
    if reason_col: slim.rename(columns={reason_col: "Raw Reason"}, inplace=True)
    else:          slim["Raw Reason"] = ""

    # Coerce to string
    for c in ["Member", "Raw Status", "Raw Reason"]:
        if c in slim.columns:
            slim[c] = slim[c].astype(str).fillna("")

    return slim

def _normalize_status(raw: str) -> str:
    r = (raw or "").strip().lower()
    # Common Spond words
    if r in {"yes", "attending", "accepted", "present"}:
        return "Present"
    if r in {"no", "declined", "absent"}:
        return "Absent"
    if "late" in r:
        return "Late"
    if r in {"unknown", "maybe", "no response"}:
        return "No response"
    # leave as-is if unknown
    return raw.strip() or ""

# -------- Main fetch & write --------

async def fetch_attendance_rows() -> List[Dict[str, Any]]:
    username = os.environ["SPOND_USERNAME"]
    password = os.environ["SPOND_PASSWORD"]

    sp = spond.Spond(username=username, password=password)

    # Resolve group filter
    group_id: Optional[str] = None
    groups = await sp.get_groups()
    print(f"[spond-sync] Found {len(groups)} groups on account.")
    for g in groups:
        gid = _extract_from_candidates(g, "id", "groupId", "uid")
        gname = _parse_event_title(g) or g.get("name") or g.get("groupName") or ""
        print(f"[spond-sync]   - {gid} | {gname}")
        if GROUP_NAME_FILTER and match_text(gname, GROUP_NAME_FILTER, "iexact"):
            group_id = gid

    if GROUP_NAME_FILTER and not group_id:
        await sp.clientsession.close()
        print(f"[spond-sync] ERROR: Group '{GROUP_NAME_FILTER}' not found.")
        return []

    if group_id:
        print(f"[spond-sync] Using group: {GROUP_NAME_FILTER} (id={group_id})")

    # Fetch events
    print(f"[spond-sync] Fetching events (min_start={MIN_START_UTC.isoformat()}, max_start={MAX_START_UTC.isoformat()}) ...")
    events = await sp.get_events(
        group_id=group_id,
        min_start=MIN_START_UTC,
        max_start=MAX_START_UTC,
        include_scheduled=True,
        max_events=500
    )

    print(f"[spond-sync] Fetched {len(events)} events total (list view may have minimal fields).")

    rows: List[Dict[str, Any]] = []
    matches = 0

    for ev in events:
        ev_id = _extract_from_candidates(ev, "id", "eventId", "uid")
        if not ev_id:
            continue

        # Time filter (if list has it; otherwise we trust get_events params already did)
        ev_start = _parse_event_times(ev)
        # If API didn’t include a start, we won’t try to re-derive here.

        matched, resolved_title = await _event_title_matches(sp, ev)
        if not matched:
            continue

        matches += 1

        # Pull attendance XLSX (also gives us the actual attendee table)
        try:
            xlsx_bytes: bytes = await sp.get_event_attendance_xlsx(ev_id)
        except Exception as e:
            print(f"[spond-sync]   - {ev_id} | skipped (failed to download XLSX: {e})")
            continue

        table = _read_attendance_table(xlsx_bytes)
        if table.empty:
            print(f"[spond-sync]   - {ev_id} | '{resolved_title or 'UNKNOWN'}' | no recognizable rows")
            continue

        # Add normalized fields & identifiers
        table["Status"] = table["Raw Status"].map(_normalize_status)
        table.insert(0, "Event ID", ev_id)
        table.insert(1, "Event Title", resolved_title or EVENT_NAME_FILTER)
        if ev_start is not None:
            table.insert(2, "Event Start (UTC)", ev_start.isoformat())
        else:
            table.insert(2, "Event Start (UTC)", "")

        # Provide manual override columns (preserved on subsequent runs)
        if "Override Status" not in table.columns:
            table["Override Status"] = ""
        if "Override Reason" not in table.columns:
            table["Override Reason"] = ""

        # Final shape/order
        table = table[[
            "Event ID", "Event Title", "Event Start (UTC)",
            "Member", "Status", "Raw Status", "Raw Reason",
            "Override Status", "Override Reason",
        ]]

        rows.extend(table.to_dict(orient="records"))

    await sp.clientsession.close()
    print(f"[spond-sync] Title matches for '{EVENT_NAME_FILTER}' ({MATCH_MODE}): {matches}")
    print(f"[spond-sync] Prepared {len(rows)} rows.")
    return rows

def upsert_to_sheet(ws, rows: List[Dict[str, Any]]):
    """
    Rebuild the sheet with fresh data, but preserve manual override columns
    from any existing rows (matched by Event ID + Member).
    """
    # Read existing
    existing = ws.get_all_records()
    existing_df = pd.DataFrame(existing) if existing else pd.DataFrame()

    new_df = pd.DataFrame(rows)

    # Ensure columns exist
    for col in ["Override Status", "Override Reason"]:
        if col not in new_df.columns:
            new_df[col] = ""

    if not existing_df.empty:
        for col in ["Override Status", "Override Reason"]:
            if col in existing_df.columns:
                # Build a map by keys
                key_cols = ["Event ID", "Member"]
                merged = pd.merge(
                    new_df,
                    existing_df[key_cols + [col]],
                    on=key_cols,
                    how="left",
                    suffixes=("", "_old")
                )
                # If old override value exists and new is empty, keep old
                merged[col] = merged[col].where(merged[col].astype(str).str.len() > 0, merged[f"{col}_old"])
                new_df = merged.drop(columns=[f"{col}_old"])

    # Sort (optional): by date then member
    if "Event Start (UTC)" in new_df.columns:
        # Missing dates will sort last
        new_df["_sort_dt"] = pd.to_datetime(new_df["Event Start (UTC)"], errors="coerce")
        new_df.sort_values(by=["_sort_dt", "Member"], inplace=True)
        new_df.drop(columns=["_sort_dt"], inplace=True)

    # Write
    ws.clear()
    ws.update([new_df.columns.tolist()] + new_df.fillna("").values.tolist())
    print("[spond-sync] Sheet updated.")

# -------- Entrypoint --------

async def main():
    gc = auth_gsheets()
    ws = get_ws(gc)
    rows = await fetch_attendance_rows()
    upsert_to_sheet(ws, rows)

if __name__ == "__main__":
    asyncio.run(main())
