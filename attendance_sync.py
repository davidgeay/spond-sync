import os
import asyncio
from datetime import datetime, timezone
import json
import re
from typing import Any, Dict, List, Tuple, Optional

import gspread
from google.oauth2.service_account import Credentials

# ----------------- CONFIG / CONSTANTS -----------------

GROUP_NAME = "IHKS G2008b/G2009b"
CUTOFF_LOCAL = "2025-08-01T00:00:00"  # local time start
TAB_NAME = "Attendance"
DEBUG_TAB = "Debug"

# Colors
GREEN = {"red": 0.8, "green": 0.94, "blue": 0.8}
RED = {"red": 0.99, "green": 0.80, "blue": 0.80}
PURPLE = {"red": 0.90, "green": 0.85, "blue": 0.98}
HEADER_GRAY = {"red": 0.93, "green": 0.93, "blue": 0.93}

STATUS_RANK = {"Present": 3, "Absent": 2, "No response": 0}

# ----------------- HELPER: TIMEZONE -----------------

def local_to_utc(dt_local_str: str, tz_name: str) -> datetime:
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(tz_name)
        dt = datetime.fromisoformat(dt_local_str)
        return dt.replace(tzinfo=tz).astimezone(timezone.utc)
    except Exception:
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
    ws.format("1:1", {"backgroundColor": HEADER_GRAY, "textFormat": {"bold": True}})

def apply_conditional_formatting(ws: gspread.Worksheet, n_rows: int, n_cols: int, start_col_idx: int) -> None:
    start_row = 2
    end_row = n_rows + 1
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

# Robust import for different package layouts
try:
    from spond.api import Spond  # most reliable path
except Exception:
    from spond import Spond  # fallback if top-level exports it

def norm(s: Optional[str]) -> str:
    return "" if s is None else " ".join(str(s).split()).strip()

def best_name(d: Dict[str, Any]) -> Optional[str]:
    for k in ("memberName", "name", "displayName", "fullName"):
        if k in d and isinstance(d[k], str) and d[k].strip():
            return d[k].strip()
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
    fn, ln = d.get("firstName"), d.get("lastName")
    if isinstance(fn, str) and isinstance(ln, str):
        nm = f"{fn} {ln}".strip()
        if nm:
            return nm
    return None

def normalize_player_name(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip().casefold()

def status_from_fields(d: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    reason = None
    for rk in ("absenceReason", "reason", "note", "comment", "absence_reason"):
        if isinstance(d.get(rk), str) and d[rk].strip():
            reason = d[rk].strip()
            break
    if d.get("checkedIn") is True or d.get("checked_in") is True:
        return "Present", None
    for key in ("attendanceStatus", "status", "response", "rsvp", "reply", "answer"):
        val = d.get(key)
        if isinstance(val, str):
            v = val.strip().lower()
            if v in {"present", "attended", "checked_in", "yes", "accepted", "attending", "going"}:
                return "Present", None
            if v in {"absent", "no", "declined", "not_attending", "cant_come", "cannot"}:
                return "Absent", reason
            if v in {"unknown", "pending", "unanswered", "no_response"}:
                return "No response", None
    if d.get("attending") is True:
        return "Present", None
    if d.get("declined") is True or d.get("absent") is True:
        return "Absent", reason
    return "No response", None

def choose_better(current: Tuple[str, Optional[str]], new: Tuple[str, Optional[str]]) -> Tuple[str, Optional[str]]:
    (cs, cr), (ns, nr) = current, new
    if STATUS_RANK.get(ns, -1) > STATUS_RANK.get(cs, -1):
        return (ns, nr)
    if ns == cs == "Absent":
        if (nr and not cr):
            return (ns, nr)
    return (cs, cr)

def walk_dicts(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from walk_dicts(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from walk_dicts(item)

async def fetch_group_and_events(sp: Spond, cutoff_utc: datetime) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    groups = await sp.get_groups()
    target_group = None
    for g in groups:
        if norm(g.get("name")).casefold() == GROUP_NAME.casefold():
            target_group = g
            break
    if not target_group:
        raise RuntimeError(f"Group '{GROUP_NAME}' not found on your account.")
    events = await sp.get_events(min_start=cutoff_utc, max_start=now_utc())
    evs = []
    for e in events:
        gid = e.get("groupId") or (e.get("group") or {}).get("id")
        if gid and str(gid) == str(target_group.get("id")):
            evs.append(e)
    detailed = []
    for e in evs:
        try:
            full = await sp.get_event(e["id"])
            detailed.append(full)
        except Exception:
            detailed.append(e)
    return target_group, detailed

def event_header(ev: Dict[str, Any], tz_name: str) -> str:
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
    return title or str(ev.get("id"))

def build_player_map(allowlist_raw: str) -> List[str]:
    return [x.strip() for x in allowlist_raw.splitlines() if x.strip()]

def to_norm_set(names: List[str]) -> set:
    return {normalize_player_name(n) for n in names}

def extract_statuses_for_event(ev: Dict[str, Any], players_norm: set) -> Dict[str, Tuple[str, Optional[str]]]:
    result: Dict[str, Tuple[str, Optional[str]]] = {}
    for d in walk_dicts(ev):
        nm = best_name(d)
        if not nm:
            continue
        nm_norm = normalize_player_name(nm)
        if nm_norm not in players_norm:
            continue
        status = status_from_fields(d)
        if nm_norm not in result:
            result[nm_norm] = status
        else:
            result[nm_norm] = choose_better(result[nm_norm], status)
    return result

async def main():
    tz_name = os.environ.get("TIMEZONE", "Europe/Oslo")
    spond_user = os.environ.get("SPOND_USERNAME")
    spond_pass = os.environ.get("SPOND_PASSWORD")
    sheet_id = os.environ.get("SHEET_ID")
    allowlist_blob = os.environ.get("PLAYER_ALLOWLIST", "").strip()
    if not (spond_user and spond_pass and sheet_id and allowlist_blob):
        raise RuntimeError("Missing one of SPOND_USERNAME, SPOND_PASSWORD, SHEET_ID, PLAYER_ALLOWLIST.")
    cutoff_utc = local_to_utc(CUTOFF_LOCAL, tz_name)

    print("[spond-sync] Logging in and loading group + events…")
    async with Spond(username=spond_user, password=spond_pass) as sp:
        group, events = await fetch_group_and_events(sp, cutoff_utc)
    print(f"[spond-sync] Using group: {group.get('name')} (id={group.get('id')})")
    print(f"[spond-sync] Found {len(events)} candidate events since cutoff.")

    players_display = build_player_map(allowlist_blob)
    players_norm = to_norm_set(players_display)

    event_headers: List[str] = []
    per_event_status: List[Dict[str, Tuple[str, Optional[str]]]] = []
    for ev in events:
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

        event_headers.append(event_header(ev, tz_name))
        per_event_status.append(extract_statuses_for_event(ev, players_norm))

    print(f"[spond-sync] Events to write: {len(event_headers)}")

    # Build matrix
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
                statuses_row.append(f"Absent — {reason}" if reason else "Absent")
            else:
                unanswered_ct += 1
                statuses_row.append("No response")
        total_missed = absent_ct + unanswered_ct
        rows.append([disp_name, str(present_ct), str(total_missed), str(unanswered_ct)] + statuses_row)

    # Write sheets
    gc = get_gspread_client()
    ws = get_ws(gc, sheet_id, TAB_NAME)
    headers = ["Player", "Total Present", "Total Missed", "Total Unanswered"] + event_headers
    clear_and_set_header(ws, headers)
    if rows:
        ws.update("A2", rows, value_input_option="RAW")

    try:
        apply_conditional_formatting(ws, n_rows=len(rows), n_cols=len(headers), start_col_idx=5)
    except Exception as e:
        print(f"[spond-sync] Warning: could not apply conditional formatting: {e}")

    # Debug sheet
    dbg = get_ws(gc, sheet_id, DEBUG_TAB)
    dbg.clear()
    dbg_headers = ["Event ID", "Event Title", "Start UTC", "Cutoff OK", "Included?"]
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
        dbg_rows.append([
            str(ev.get("id")),
            norm(ev.get("title") or ev.get("name") or ""),
            (start_raw or ""),
            cutoff_ok,
            "Yes" if col_title else "No",
        ])
    dbg.update("A1", [dbg_headers])
    if dbg_rows:
        dbg.update("A2", dbg_rows)

    print("[spond-sync] Done. Sheet updated.")

if __name__ == "__main__":
    asyncio.run(main())

