# attendance_sync.py
# Secrets required: SPOND_USERNAME, SPOND_PASSWORD, GOOGLE_SERVICE_ACCOUNT_JSON, SHEET_ID
# pip deps: spond gspread google-auth pandas openpyxl python-dateutil

import os, io, json, asyncio, re
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
from dateutil import parser as dtparser
import gspread
from google.oauth2.service_account import Credentials
from spond import spond

# ---------------- Settings you asked for ----------------
GROUP_NAME = "IHKS G2008b/G2009b"
TIMEZONE = ZoneInfo(os.getenv("TIMEZONE", "Europe/Oslo"))
CUTOFF_LOCAL = datetime(2025, 8, 1, 0, 0, tzinfo=TIMEZONE)   # include events on/after this date
CUTOFF_UTC = CUTOFF_LOCAL.astimezone(timezone.utc)
NOW_UTC = datetime.now(timezone.utc)

SHEET_ATT = "Attendance"
SHEET_DBG = "Debug"

# wide sheet layout
FIXED_COLS = ["Player", "Total Present", "Total Missed", "Total Unanswered"]

# keyword match
ISTR_PAT = re.compile(r"\bistrening\b", re.IGNORECASE)

def log(msg: str): print(f"[spond-sync] {msg}", flush=True)

# ---------------- Google Sheets helpers ----------------
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
        return sh.add_worksheet(title=title, rows=2000, cols=26)
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

def to_text(v: Any) -> str:
    if v is None: return ""
    if isinstance(v, (str, int, float, bool)): return str(v)
    if isinstance(v, dict):
        return " | ".join([to_text(k)+": "+to_text(x) for k, x in v.items() if x is not None])
    if isinstance(v, (list, tuple, set)):
        return " | ".join([to_text(x) for x in v if x is not None])
    return str(v)

def contains_istrening(*vals: Any) -> bool:
    for v in vals:
        if ISTR_PAT.search(to_text(v)): return True
    return False

def parse_start_utc(d: Dict[str, Any]) -> Optional[datetime]:
    raw = _pick(d, "startTimeUtc", "start_time_utc", "startTime", "start",
                "startAt", "start_at", "startDateTime", "start_datetime",
                "utcStart", "utc_start", "startTimestamp", "start_timestamp")
    if raw is None: return None
    try:
        if isinstance(raw, (int, float)) and raw > 1_000_000_000:
            return datetime.fromtimestamp(float(raw), tz=timezone.utc)
        s = str(raw).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def parse_datetime_from_text(text: str) -> Optional[datetime]:
    try:
        dt = dtparser.parse(text, fuzzy=True, dayfirst=True)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=TIMEZONE)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

# ---------------- XLSX parsing (all sheets, exclude leaders) ----------------
ROLE_WORDS = {"leader", "leder", "coach", "trener", "admin"}

NAME_CANDS   = ["Name", "Member name", "Member", "Participant", "Navn", "Deltaker", "Spiller", "Player"]
STATUS_CANDS = ["Status", "Response", "Attending", "Svar", "Svarstatus", "Attendance",
                "Kommer", "Deltar", "Deltar ikke", "Kommer ikke", "Påmeldt"]
REASON_CANDS = ["Note", "Reason", "Absence reason", "Kommentar", "Begrunnelse", "Fraværsgrunn",
                "Årsak", "Notes", "Message", "Kommentarer"]
ROLE_CANDS   = ["Type", "Role", "Rolle", "Kategori", "Group", "Gruppe", "Category"]

def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = [str(c).strip() for c in df.columns]
    for c in candidates:
        if c in cols: return c
    # loose match
    low = [c.lower() for c in cols]
    for cand in candidates:
        cl = cand.lower()
        for i, l in enumerate(low):
            if cl in l: return cols[i]
    return None

def _looks_like_leaders(df: pd.DataFrame, sheet_name: str) -> bool:
    if "leder" in sheet_name.lower() or "leader" in sheet_name.lower():
        return True
    role_col = _pick_col(df, ROLE_CANDS)
    if role_col:
        if df[role_col].astype(str).str.lower().str.contains("|".join(ROLE_WORDS)).any():
            return True
    # sometimes name has "(Leder)" etc.
    name_col = _pick_col(df, NAME_CANDS)
    if name_col:
        if df[name_col].astype(str).str.lower().str.contains(r"\(.*(leder|coach|admin).*\)").any():
            return True
    return False

def read_attendance_xlsx_all(xlsx_bytes: bytes) -> pd.DataFrame:
    """Return table with Member, Raw Status, Raw Reason — from ALL sheets, excluding leaders/admins."""
    try:
        sheets = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=None)
    except Exception:
        return pd.DataFrame(columns=["Member", "Raw Status", "Raw Reason"])

    frames: List[pd.DataFrame] = []
    for sheet_name, df in sheets.items():
        if df is None or df.empty:
            continue
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        if _looks_like_leaders(df, sheet_name):
            continue

        name_col   = _pick_col(df, NAME_CANDS)
        status_col = _pick_col(df, STATUS_CANDS)
        reason_col = _pick_col(df, REASON_CANDS)

        if not (name_col or status_col or reason_col):
            continue

        slim = pd.DataFrame()
        if name_col:   slim["Member"] = df[name_col].astype(str)
        if status_col: slim["Raw Status"] = df[status_col].astype(str)
        if reason_col: slim["Raw Reason"] = df[reason_col].astype(str)
        for c in ["Member", "Raw Status", "Raw Reason"]:
            if c not in slim.columns: slim[c] = ""
        frames.append(slim[["Member", "Raw Status", "Raw Reason"]])

    if not frames:
        return pd.DataFrame(columns=["Member", "Raw Status", "Raw Reason"])
    out = pd.concat(frames, ignore_index=True).fillna("")
    # clean obvious junk
    out = out[out["Member"].astype(str).str.len().between(1, 80)]
    return out

# ---------------- Status normalization & coloring ----------------
def normalize_status(raw: str) -> str:
    r = (raw or "").strip().lower()
    # English
    if r in {"yes", "attending", "accepted", "present"}: return "Present"
    if r in {"no", "declined", "absent"}: return "Absent"
    if "late" in r: return "Present"  # treat late as present for totals
    if r in {"unknown", "maybe", "no response"}: return "No response"
    # Norwegian
    if r in {"kommer", "deltar", "påmeldt", "ja"}: return "Present"
    if r in {"kommer ikke", "deltar ikke", "nei", "fravær"}: return "Absent"
    if r in {"usikker", "kanskje"}: return "No response"
    if r in {"ubesvart", "ingen svar"}: return "No response"
    return "No response" if not r else raw.strip()

def status_to_cell(status: str, reason: str) -> Tuple[str, str]:
    """Return (display_text, color_code) where color_code in {'present','absent','noresp'}"""
    st = normalize_status(status)
    if st == "Present":
        return ("Present", "present")
    if st == "Absent":
        txt = "Absent" + (f" — {reason.strip()}" if str(reason).strip() else "")
        return (txt, "absent")
    # default no answer
    return ("No response", "noresp")

# ---------------- Spond helpers ----------------
async def resolve_group_id(sp: spond.Spond) -> Optional[str]:
    groups = await sp.get_groups()
    log(f"Found {len(groups)} groups.")
    gid = None
    for g in groups:
        nm = _pick(g, "name", "title", "groupName") or ""
        gid_candidate = _pick(g, "id", "groupId", "uid")
        log(f"  - {gid_candidate} | {nm}")
        if nm.strip().lower() == GROUP_NAME.strip().lower():
            gid = gid_candidate
    if gid:
        log(f"Using group: {GROUP_NAME} (id={gid})")
    else:
        log(f"ERROR: Group '{GROUP_NAME}' not found.")
    return gid

def xlsx_all_text(xlsx_bytes: bytes) -> str:
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(xlsx_bytes), data_only=True)
        parts = []
        for ws in wb.worksheets:
            for r in ws.iter_rows(min_row=1, max_row=min(ws.max_row or 0, 200), values_only=True):
                for c in r:
                    if isinstance(c, str) and c.strip():
                        parts.append(c.strip())
        return "\n".join(parts)
    except Exception:
        return ""

# ---------------- Core: build wide matrix ----------------
async def collect_events_and_attendance() -> Tuple[List[str], Dict[str, Dict[str, Tuple[str, str]]], List[Dict[str, Any]]]:
    """
    Returns:
      - event_headers: List[str] for sheet columns (E → ...), e.g. '2025-09-01 19:00 — Title'
      - per_member: { member_name: { event_header: (display_text, color_code) } }
      - debug_rows: rows for Debug tab
    """
    username = os.environ["SPOND_USERNAME"]; password = os.environ["SPOND_PASSWORD"]
    sp = spond.Spond(username=username, password=password)

    try:
        gid = await resolve_group_id(sp)
        if not gid:
            return [], {}, []

        log(f"Fetching events (UTC {CUTOFF_UTC.isoformat()} → {NOW_UTC.isoformat()}) ...")
        try:
            events = await sp.get_events(group_id=gid, min_start=CUTOFF_UTC, max_start=NOW_UTC,
                                         include_scheduled=True, max_events=500)
        except TypeError:
            events = await sp.get_events(min_start=CUTOFF_UTC, max_start=NOW_UTC)

        log(f"Fetched {len(events)} events (list may be minimal).")

        # gather event details that match keyword (details or XLSX)
        included: List[Tuple[str, str, Optional[datetime]]] = []  # (event_id, header, start_utc)
        debug_rows: List[Dict[str, Any]] = []

        for le in events:
            eid = _pick(le, "id", "eventId", "uid")
            if not eid: continue

            try:
                d = await sp.get_event(eid)
            except Exception as e:
                log(f"WARNING: get_event failed for {eid}: {e}")
                continue

            title = _pick(d, "title", "name", "eventName", "subject") or ""
            start_utc = parse_start_utc(d)
            start_disp = start_utc.isoformat() if start_utc else "NO-START"

            matched_src = ""
            if contains_istrening(d):
                matched_src = "details"
            else:
                try:
                    xbytes = await sp.get_event_attendance_xlsx(eid)
                    if contains_istrening(xlsx_all_text(xbytes)):
                        matched_src = "xlsx"
                    if not start_utc:  # try to guess from xlsx text
                        guess = parse_datetime_from_text(xlsx_all_text(xbytes))
                        if guess: start_utc = guess
                except Exception:
                    pass

            date_ok = True if start_utc is None else (start_utc >= CUTOFF_UTC)
            included_flag = (matched_src != "") and date_ok

            debug_rows.append({
                "Event ID": eid,
                "Event Title": title[:200],
                "Start UTC": start_disp,
                "Matched (details/xlsx)": matched_src or "no",
                "Cutoff Date OK": "Yes" if date_ok else "No",
                "Included": "Yes" if included_flag else "No",
            })

            if not included_flag:
                continue

            # make column header (local time + short title)
            if start_utc:
                start_local = start_utc.astimezone(TIMEZONE)
                header = f"{start_local:%Y-%m-%d %H:%M} — {title or '(no title)'}"
            else:
                header = f"(no time) — {title or '(no title)'}"
            included.append((eid, header, start_utc))

        # sort events by time
        included.sort(key=lambda t: (t[2] or CUTOFF_UTC))
        event_headers = [h for _, h, _ in included]

        # per-member attendance per event
        per_member: Dict[str, Dict[str, Tuple[str, str]]] = {}

        for (eid, header, _) in included:
            # read XLSX (all sheets, exclude leaders/admins)
            xbytes = None
            table = pd.DataFrame()
            try:
                xbytes = await sp.get_event_attendance_xlsx(eid)
                table = read_attendance_xlsx_all(xbytes)
            except Exception as e:
                log(f"WARNING: XLSX not available for {eid}: {e}")

            # If table is empty, there might be zero responses; we still want to keep the column
            member_status: Dict[str, Tuple[str, str]] = {}

            if not table.empty:
                # normalize and fill text+color
                for _, row in table.iterrows():
                    name   = str(row.get("Member", "")).strip()
                    raw_st = str(row.get("Raw Status", "")).strip()
                    raw_re = str(row.get("Raw Reason", "")).strip()
                    if not name: continue
                    txt, color = status_to_cell(raw_st, raw_re)
                    member_status[name] = (txt, color)

            # fold into per_member structure
            # (we'll fill missing later as "No response")
            for name, val in member_status.items():
                per_member.setdefault(name, {})[header] = val

        return event_headers, per_member, debug_rows

    finally:
        try:
            if sp and getattr(sp, "clientsession", None):
                await sp.clientsession.close()
        except Exception:
            pass

# ---------------- Write wide sheet + colors ----------------
def build_matrix(event_headers: List[str], per_member: Dict[str, Dict[str, Tuple[str, str]]]) -> Tuple[List[List[str]], List[List[str]]]:
    """
    Returns (values_matrix, color_matrix) without header row.
    color_matrix holds 'present' | 'absent' | 'noresp' for only the event cells.
    """
    # roster: all members seen anywhere, excluding obvious admins/coaches by name tag (belt-and-braces)
    def is_admin_like(name: str) -> bool:
        ln = name.lower()
        return any(w in ln for w in ("coach", "trener", "leder", "admin"))
    members = sorted([m for m in per_member.keys() if m and not is_admin_like(m)])

    values: List[List[str]] = []
    colors: List[List[str]] = []  # only for event columns

    for m in members:
        row_values = [m, "", "", ""]  # B,C,D totals filled after loop
        row_colors: List[str] = []

        present = 0
        noresp = 0
        absent = 0

        status_map = per_member.get(m, {})
        for eh in event_headers:
            if eh in status_map:
                text, code = status_map[eh]
            else:
                text, code = ("No response", "noresp")
            row_values.append(text)
            row_colors.append(code)
            if code == "present": present += 1
            elif code == "absent": absent += 1
            else: noresp += 1

        missed = absent + noresp
        row_values[1] = str(present)
        row_values[2] = str(missed)
        row_values[3] = str(noresp)

        values.append(row_values)
        colors.append(row_colors)

    return values, colors

def write_wide_sheet(sh, ws_att, event_headers: List[str], values: List[List[str]], colors: List[List[str]]):
    # header
    header = FIXED_COLS + event_headers

    # wipe + write values
    ws_att.clear()
    ws_att.update([header] + values)

    # Freeze header + first column for usability
    try:
        sh.batch_update({
            "requests": [
                {"updateSheetProperties": {
                    "properties": {"sheetId": ws_att.id, "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 1}},
                    "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"
                }}
            ]
        })
    except Exception:
        pass

    # Apply background colors for event cells
    # We'll build repeatCell requests in chunks.
    color_map = {
        "present": {"red": 0.85, "green": 0.95, "blue": 0.85},  # light green
        "absent":  {"red": 0.98, "green": 0.85, "blue": 0.85},  # light red
        "noresp":  {"red": 0.93, "green": 0.86, "blue": 0.97},  # light purple
    }

    requests = []
    n_rows = len(values)
    n_events = len(event_headers)
    if n_rows > 0 and n_events > 0:
        # event cells start at row 2, column 5 (E)
        start_row = 1  # zero-based index for API; row 2 in UI
        start_col = 4  # column E
        for r in range(n_rows):
            for c in range(n_events):
                code = colors[r][c]
                rgb = color_map.get(code)
                if not rgb: continue
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": ws_att.id,
                            "startRowIndex": start_row + r,
                            "endRowIndex":   start_row + r + 1,
                            "startColumnIndex": start_col + c,
                            "endColumnIndex":   start_col + c + 1,
                        },
                        "cell": {"userEnteredFormat": {"backgroundColor": rgb}},
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })
        # batch in chunks to avoid request size limits
        for i in range(0, len(requests), 5000):
            sh.batch_update({"requests": requests[i:i+5000]})

    log("Attendance sheet updated (wide grid).")

def write_debug(ws_dbg, dbg_rows: List[Dict[str, Any]]):
    cols = ["Event ID", "Event Title", "Start UTC", "Matched (details/xlsx)", "Cutoff Date OK", "Included"]
    ws_dbg.clear()
    if not dbg_rows:
        ws_dbg.update([cols, ["(no events in window)", "", "", "", "", ""]])
    else:
        ws_dbg.update([cols] + [[r.get(c, "") for c in cols] for r in dbg_rows])
    log("Debug sheet updated.")

# ---------------- Entrypoint ----------------
async def main():
    gc = sheets_client()
    sh = open_spreadsheet(gc)
    ws_att = get_or_create_ws(sh, SHEET_ATT)
    ws_dbg = get_or_create_ws(sh, SHEET_DBG)

    event_headers, per_member, dbg_rows = await collect_events_and_attendance()

    # If we have no events at all, still render headers
    values, colors = build_matrix(event_headers, per_member)
    write_wide_sheet(sh, ws_att, event_headers, values, colors)
    write_debug(ws_dbg, dbg_rows)

    log("Sync complete.")

if __name__ == "__main__":
    asyncio.run(main())
