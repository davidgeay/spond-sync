import os
import asyncio
from datetime import datetime, timezone, timedelta
import json
import re
from typing import Any, Dict, List, Tuple, Optional

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

# ----------------- CONFIG / CONSTANTS -----------------

GROUP_NAME = "IHKS G2008b/G2009b"
CUTOFF_LOCAL = "2025-08-01T00:00:00"  # local midnight of Aug 1, 2025
TAB_NAME = "Attendance"
DEBUG_TAB = "Debug"

# Colors
GREEN = {"red": 0.8, "green": 0.94, "blue": 0.8}
RED = {"red": 0.99, "green": 0.80, "blue": 0.80}
PURPLE = {"red": 0.90, "green": 0.85, "blue": 0.98}
HEADER_GRAY = {"red": 0.93, "green": 0.93, "blue": 0.93}

# Status precedence (higher wins when multiple clues exist)
STATUS_RANK = {
    "Present": 3,
    "Absent": 2,          # Absent — <reason>
    "No response": 0,
}

# Normalize “istrening” detection was requested earlier; we now ignore title filtering.
ISTR_PAT = re.compile(r"\bistrening\b", re.I)  # unused in this version but kept for reference

# ----------------- HELPER: TIMEZONE -----------------

def local_to_utc(dt_local_str: str, tz_name: str) -> datetime:
    """Convert local ISO-like string to aware UTC."""
    # We don't import zoneinfo (not guaranteed on runner images);
    # Spond timestamps work fine if we offset using the OS env TZ assumption.
    # We'll accept a best-effort: if TIMEZONE not provided, treat as UTC.
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(tz_name)
        dt = datetime.fromisoformat(dt_local_str)
        return dt.replace(tzinfo=tz).astimezone(timezone.utc)
    except Exception:
        # Fallback: treat string as naive UTC
        return datetime.fromisoformat(dt_local_str).replace(tzinfo=timezone.utc)

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

# ----------------- LOGIN / SHEETS -----------------

def get_gspread_client() -> gspread.Client:
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sa_json:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON env var missing.")

    info = json.loads(sa_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def get_ws(gc: gspread.Client, sheet_id: str, title: str) -> gspread.Worksheet:
    sh = gc.open_by_key(sheet_id)
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=2000, cols=200)

def clear_and_set_header(ws: gspread.Worksheet, headers: List[str]) -> None:
    ws.clear()
    ws.update("A1", [headers])
    # Light gray header
    ws.format("1:1", {"backgroundColor": HEADER_GRAY, "textFormat": {"bold": True}})

def apply_conditional_formatting(ws: gspread.Worksheet, n_rows: int, n_cols: int, start_col_idx: int) -> None:
    """Add three simple text-based rules over the data grid."""
    # Data region (from row 2)
    start_row = 2
    end_row = n_rows + 1
    start_col_letter = gspread.utils.rowcol_to_a1(1, start_col_idx)[0]
    end_cell = gspread.utils.rowcol_to_a1(end_row, n_cols)
    rng = f"{start_col_letter}{start_row}:{end_cell}"

    rules = [
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws.id, "startRowIndex": start_row - 1, "endRowIndex": end_row,
                                "startColumnIndex": start_col_idx - 1, "endColumnIndex": n_cols}],
                    "booleanRule": {
                        "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "Present"}]},
                        "format": {"backgroundColor": GREEN}
                    }
                },
                "index": 0
            }
        },
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws.id, "startRowIndex": start_row - 1, "endRowIndex": end_row,
                                "startColumnIndex": start_col_idx - 1, "endColumnIndex": n_cols}],
                    "booleanRule": {
                        "condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "Absent"}]},
                        "format": {"backgroundColor": RED}
                    }
                },
                "index": 0
            }
        },
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws.id, "startRowIndex": start_row - 1, "endRowIndex": end_row,
                                "startColumnIndex": start_col_idx - 1, "endColumnIndex": n_cols}],
                    "booleanRule": {
                        "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "No response"}]},
                        "format": {"backgroundColor": PURPLE}
                    }
                },
                "index": 0
            }
        },
    ]
    ws.spreadsheet.batch_update({"requests": rules})

# ----------------- SPOND API -----------------

# We use the async 'spond' package.
from spond import Spond


def norm(s: Optional[str]) -> str:
    return "" if s is None else " ".join(str(s).split()).strip()


def best_name(d: Dict[str, Any]) -> Optional[str]:
    """Try to extract a meaningful member/player name from any dict shape."""
    # direct
    for k in ("memberName", "name", "displayName", "fullName"):
        if k in d and isinstance(d[k], str) and d[k].strip():
            return d[k].strip()

    # nested member
    m = d.get("member")
    if isinstance(m, dict):
        for k in ("name", "memberName", "displayName", "fullName"):
            if k in m and isinstance(m[k], str) and m[k].strip():
                return m[k].strip()
        fn, ln = m.get("firstName"), m.get("lastName")
        if isinstance(fn, str) and isinstance(ln, str):
            nm = f"{fn} {ln}".strip()
            if nm:
                return nm

    # separate first/last on the same node
    fn, ln = d.get("firstName"), d.get("lastName")
    if isinstance(fn, str) and isinstance(ln, str):
        nm = f"{fn} {ln}".strip()
        if nm:
            return nm

    return None


def normalize_player_name(s: str) -> str:
    # collapse whitespace + lowercase + strip; keep diacritics to preserve distinct names
    return re.sub(r"\s+", " ", s or "").strip().casefold()


def status_from_fields(d: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    """
    Decide one status from a dict fragment.
    Returns: (status_label, reason_or_None)
    """
    reason = None

    # Collect any reason-like field
    for rk in ("absenceReason", "reason", "note", "comment", "absence_reason"):
        if isinstance(d.get(rk), str) and d[rk].strip():
            reason = d[rk].strip()
            break

    # Boolean check-ins (attendance)
    if d.get("checkedIn") is True or d.get("checked_in") is True:
        return "Present", None

    # Generic status strings common in Spond shapes
    for key in ("attendanceStatus", "status", "response", "rsvp", "reply", "answer"):
        val = d.get(key)
        if isinstance(val, str):
            v = val.strip().lower()

            # Common "present" variants
            if v in {"present", "attended", "checked_in", "yes", "accepted", "attending", "going"}:
                return "Present", None

            # Common "absent/declined" variants
            if v in {"absent", "no", "declined", "not_attending", "cant_come", "cannot"}:
                return "Absent", reason

            # Explicit "no response / pending"
            if v in {"unknown", "pending", "unanswered", "no_response"}:
                return "No response", None

    # Some shapes encode booleans like { attending: true } etc.
    if d.get("attending") is True:
        return "Present", None
    if d.get("declined") is True or d.get("absent") is True:
        return "Absent", reason

    # Nothing clear
    return "No response", None


def choose_better(current: Tuple[str, Optional[str]], new: Tuple[str, Optional[str]]) -> Tuple[str, Optional[str]]:
    """Pick by STATUS_RANK, prefer having a reason when ranks tie on Absent."""
    (cs, cr), (ns, nr) = current, new
    if STATUS_RANK.get(ns, -1) > STATUS_RANK.get(cs, -1):
        return (ns, nr)
    if ns == cs == "Absent":
        # Prefer one that has a reason
        if (nr and not cr):
            return (ns, nr)
    return (cs, cr)


def walk_dicts(obj: Any):
    """Yield all dicts within any nested structure."""
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from walk_dicts(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from walk_dicts(item)


async def fetch_group_and_events(sp: Spond, cutoff_utc: datetime) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    # find the group
    groups = await sp.get_groups()
    target_group = None
    for g in groups:
        if norm(g.get("name")).casefold() == GROUP_NAME.casefold():
            target_group = g
            break
    if not target_group:
        raise RuntimeError(f"Group '{GROUP_NAME}' not found on your account.")

    # fetch events (Spond returns minimal fields here)
    events = await sp.get_events(min_start=cutoff_utc, max_start=now_utc())
    # keep only this group's events and >= cutoff
    evs = []
    for e in events:
        gid = e.get("groupId") or (e.get("group") or {}).get("id")
        if gid and str(gid) == str(target_group.get("id")):
            # only after cutoff; be tolerant with fields
            start = e.get("startAt") or e.get("start") or e.get("startTime") or (e.get("time") or {}).get("start")
            # We will re-validate after we fetch full details anyway
            evs.append(e)

    # hydrate each event with full details (title/start/status live here)
    detailed = []
    for e in evs:
        try:
            full = await sp.get_event(e["id"])
            detailed.append(full)
        except Exception:
            # keep a stub if fetching fails
            detailed.append(e)
    return target_group, detailed


def event_header(ev: Dict[str, Any], tz_name: str) -> str:
    # Build a readable column header from event start
    # Try common fields; most detailed event payloads have ISO in "startAt"
    raw = ev.get("startAt") or ev.get("start") or ev.get("startTime") or (ev.get("time") or {}).get("start")
    title = norm(ev.get("title") or ev.get("name") or "")
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(tz_name)
        if isinstance(raw, str):
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        elif isinstance(raw, (int, float)):
            dt = datetime.fromtimestamp(raw, tz=timezone.utc)
        else:
            dt = None
        if dt is not None and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if dt is not None:
            local = dt.astimezone(tz)
            return f"{local:%Y-%m-%d %H:%M}"
    except Exception:
        pass
    # Fallback to title or ID
    return title or str(ev.get("id"))


def build_player_map(allowlist_raw: str) -> List[str]:
    # Player list comes from the secret PLAYER_ALLOWLIST (one name per line)
    names = [normalize_player_name(x) for x in allowlist_raw.splitlines() if x.strip()]
    # Preserve original display names too (we’ll reconstruct a capitalized version)
    display = [x.strip() for x in allowlist_raw.splitlines() if x.strip()]
    # Keep both but our matching uses normalized set
    return display  # ordered list, already user-specified


def to_norm_set(names: List[str]) -> set:
    return {normalize_player_name(n) for n in names}


def extract_statuses_for_event(ev: Dict[str, Any], players_norm: set) -> Dict[str, Tuple[str, Optional[str]]]:
    """
    Scan the entire event payload and collect a best status for any player name we recognize.
    We match by the *player's name* (so guardian replies count for the player).
    """
    result: Dict[str, Tuple[str, Optional[str]]] = {}  # normalized name -> (status, reason)

    for d in walk_dicts(ev):
        nm = best_name(d)
        if not nm:
            continue
        nm_norm = normalize_player_name(nm)
        if nm_norm not in players_norm:
            continue  # not a tracked player

        status = status_from_fields(d)  # (label, reason)
        if nm_norm not in result:
            result[nm_norm] = status
        else:
            result[nm_norm] = choose_better(result[nm_norm], status)

    return result


async def main():
    # --------- ENV ---------
    tz_name = os.environ.get("TIMEZONE", "Europe/Oslo")
    spond_user = os.environ.get("SPOND_USERNAME")
    spond_pass = os.environ.get("SPOND_PASSWORD")
    sheet_id = os.environ.get("SHEET_ID")
    allowlist_blob = os.environ.get("PLAYER_ALLOWLIST", "").strip()

    if not (spond_user and spond_pass and sheet_id and allowlist_blob):
        raise RuntimeError("Missing one of SPOND_USERNAME, SPOND_PASSWORD, SHEET_ID, PLAYER_ALLOWLIST.")

    cutoff_utc = local_to_utc(CUTOFF_LOCAL, tz_name)

    # --------- SPOND ---------
    print("[spond-sync] Logging in and loading group + events…")
    async with Spond(username=spond_user, password=spond_pass) as sp:
        group, events = await fetch_group_and_events(sp, cutoff_utc)

    print(f"[spond-sync] Using group: {group.get('name')} (id={group.get('id')})")
    print(f"[spond-sync] Found {len(events)} candidate events since cutoff.")

    # Build player list
    players_display = build_player_map(allowlist_blob)
    players_norm = to_norm_set(players_display)

    # Prepare event headers and per-event status maps
    tz_name = os.environ.get("TIMEZONE", "Europe/Oslo")
    event_headers: List[str] = []
    per_event_status: List[Dict[str, Tuple[str, Optional[str]]]] = []

    for ev in events:
        # keep only events that start after cutoff (double-check with detailed field)
        raw = ev.get("startAt") or ev.get("start") or ev.get("startTime") or (ev.get("time") or {}).get("start")
        ok_time = True
        try:
            if isinstance(raw, str):
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                ok_time = dt >= cutoff_utc
        except Exception:
            pass
        if not ok_time:
            continue

        col = event_header(ev, tz_name)
        event_headers.append(col)

        st_map = extract_statuses_for_event(ev, players_norm)
        per_event_status.append(st_map)

    print(f"[spond-sync] Events to write: {len(event_headers)}")

    # --------- BUILD MATRIX ---------
    # Columns: Player | Total Present | Total Missed | Total Unanswered | [event columns...]
    rows: List[List[str]] = []
    for disp_name in players_display:
        nm_norm = normalize_player_name(disp_name)
        statuses_row: List[str] = []
        present_ct = 0
        absent_ct = 0
        unanswered_ct = 0

        for stmap in per_event_status:
            status, reason = stmap.get(nm_norm, ("No response", None))
            if status == "Present":
                present_ct += 1
                statuses_row.append("Present")
            elif status == "Absent":
                absent_ct += 1
                cell = "Absent"
                if reason:
                    cell = f"Absent — {reason}"
                statuses_row.append(cell)
            else:
                unanswered_ct += 1
                statuses_row.append("No response")

        total_missed = absent_ct + unanswered_ct
        row = [disp_name, str(present_ct), str(total_missed), str(unanswered_ct)] + statuses_row
        rows.append(row)

    # --------- WRITE SHEET ---------
    gc = get_gspread_client()
    ws = get_ws(gc, sheet_id, TAB_NAME)

    headers = ["Player", "Total Present", "Total Missed", "Total Unanswered"] + event_headers
    clear_and_set_header(ws, headers)

    if rows:
        ws.update(f"A2", rows, value_input_option="RAW")

    # Conditional formatting on the grid only (from first event column)
    total_rows = len(rows) + 1  # + header
    total_cols = len(headers)
    first_event_col_index = 5  # A=1, B=2, C=3, D=4, so events start at E=5
    try:
        # Clear old rules by recreating sheet rules (optional). We'll just add rules anew.
        apply_conditional_formatting(ws, n_rows=len(rows), n_cols=total_cols, start_col_idx=first_event_col_index)
    except Exception as e:
        print(f"[spond-sync] Warning: could not apply conditional formatting: {e}")

    # --------- DEBUG SHEET (optional, helps verify) ---------
    dbg = get_ws(gc, sheet_id, DEBUG_TAB)
    dbg.clear()
    dbg_headers = ["Event ID", "Event Title", "Start UTC", "Matched (details)", "Cutoff OK", "Included?"]
    dbg_rows = []
    for ev, col_title in zip(events, event_headers):
        start_raw = ev.get("startAt") or ev.get("start") or ev.get("startTime") or (ev.get("time") or {}).get("start")
        cutoff_ok = "Yes"
        try:
            dt = datetime.fromisoformat((start_raw or "").replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            cutoff_ok = "Yes" if dt >= cutoff_utc else "No"
        except Exception:
            pass

        # Count how many of our players we found statuses for
        stmap = extract_statuses_for_event(ev, players_norm)
        found = sum(1 for k in stmap.keys() if k in players_norm)
        dbg_rows.append([
            str(ev.get("id")),
            norm(ev.get("title") or ev.get("name") or ""),
            (start_raw or ""),
            f"details",
            cutoff_ok,
            "Yes" if col_title else "No",
        ])
    dbg.update("A1", [dbg_headers])
    if dbg_rows:
        dbg.update("A2", dbg_rows)

    print("[spond-sync] Done. Sheet updated.")


if __name__ == "__main__":
    asyncio.run(main())
