# attendance_sync.py
# Secrets needed: SPOND_USERNAME, SPOND_PASSWORD, GOOGLE_SERVICE_ACCOUNT_JSON, SHEET_ID
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

# ---------------- Settings ----------------
GROUP_NAME = "IHKS G2008b/G2009b"
TIMEZONE = ZoneInfo(os.getenv("TIMEZONE", "Europe/Oslo"))
CUTOFF_LOCAL = datetime(2025, 8, 1, 0, 0, tzinfo=TIMEZONE)     # include events on/after
CUTOFF_UTC = CUTOFF_LOCAL.astimezone(timezone.utc)
NOW_UTC = datetime.now(timezone.utc)

SHEET_ATT = "Attendance"
SHEET_DBG = "Debug"
FIXED_COLS = ["Player", "Total Present", "Total Missed", "Total Unanswered"]

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
        return sh.add_worksheet(title=title, rows=2000, cols=26)
    except gspread.exceptions.APIError as e:
        if "already exists" in str(e).lower():
            return sh.worksheet(title)
        raise

# ---------------- Utils ----------------
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

# ---------------- XLSX parsing (robust) ----------------
ROLE_WORDS_EXCLUDE = {"leader", "leder", "coach", "trener", "admin"}   # tutors/guardians stay

NAME_CANDS   = ["Name", "Member name", "Member", "Participant", "Navn", "Deltaker", "Spiller", "Player"]
STATUS_CANDS = ["Status", "Response", "Svar", "Svarstatus", "Attendance", "Attending", "RSVP"]
REASON_CANDS = ["Note", "Reason", "Absence reason", "Kommentar", "Begrunnelse", "Fraværsgrunn",
                "Årsak", "Notes", "Message", "Kommentarer"]
ROLE_CANDS   = ["Type", "Role", "Rolle", "Kategori", "Group", "Gruppe", "Category"]

# headers that might be boolean one-hots for yes/no
YES_HDR_HINTS = re.compile(r"(attend|kommer|deltar|påmeld|present|going|ja)", re.I)
NO_HDR_HINTS  = re.compile(r"(declin|kommer\s*ikke|deltar\s*ikke|nei|absent|not\s*going|kan\s*ikke)", re.I)
NORES_HDR_HINTS = re.compile(r"(no\s*resp|ubesvar|ingen\s*svar|not\s*respond)", re.I)

def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = [str(c).strip() for c in df.columns]
    for c in candidates:
        if c in cols: return c
    low = [c.lower() for c in cols]
    for cand in candidates:
        cl = cand.lower()
        for i, l in enumerate(low):
            if cl in l: return cols[i]
    return None

def _leaders_only_sheet_name(sheet_name: str) -> bool:
    s = sheet_name.lower()
    return any(x in s for x in ("leder", "leader", "coach", "trener", "admin"))

def _truthy(v: Any) -> bool:
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "ja", "x", "✓", "check", "checked", "present"}

def _falsy(v: Any) -> bool:
    s = str(v).strip().lower()
    return s in {"0", "false", "no", "nei", "x", "✗", "not present"}

def infer_status_from_row(row: pd.Series, name_col: str, status_col: Optional[str]) -> Tuple[str, str]:
    """Return (raw_status_text, raw_reason_text). If status_col empty, infer from the rest of the row."""
    reason = ""
    if status_col:
        raw = str(row.get(status_col, "")).strip()
        if raw:
            return raw, reason

    # Try boolean / one-hot columns
    for col in row.index:
        if col == name_col: 
            continue
        val = row[col]
        if YES_HDR_HINTS.search(str(col)) and _truthy(val):
            return "Present", ""
        if NO_HDR_HINTS.search(str(col)) and _truthy(val):
            return "Absent", str(val)
        if NORES_HDR_HINTS.search(str(col)) and _truthy(val):
            return "No response", ""

    # Fallback: scan entire row text for keywords
    blob = " ".join([str(v) for k, v in row.items() if k != name_col and pd.notna(v)]).lower()
    if re.search(r"(accepted|attending|present|going|kommer|deltar|påmeldt|ja|møter)", blob):
        return "Present", ""
    if re.search(r"(declin|kommer\s*ikke|deltar\s*ikke|nei|fravær|kan\s*ikke)", blob):
        return "Absent", ""
    if re.search(r"(no\s*response|ubesvart|ingen\s*svar|not\s*respond)", blob):
        return "No response", ""

    # Nothing obvious
    return "", ""

def read_attendance_xlsx_all(xlsx_bytes: bytes) -> pd.DataFrame:
    """Rows: Member, Raw Status, Raw Reason. Keeps tutors/guardians, drops only coaches/admins."""
    try:
        sheets = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=None)
    except Exception:
        return pd.DataFrame(columns=["Member", "Raw Status", "Raw Reason"])

    frames: List[pd.DataFrame] = []
    for sheet_name, df in sheets.items():
        if df is None or df.empty: 
            continue
        if _leaders_only_sheet_name(sheet_name):  # a dedicated leaders tab
            continue

        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]

        name_col   = _pick_col(df, NAME_CANDS)
        status_col = _pick_col(df, STATUS_CANDS)
        reason_col = _pick_col(df, REASON_CANDS)
        role_col   = _pick_col(df, ROLE_CANDS)

        # filter out coaches/admins only
        if role_col:
            mask_excl = df[role_col].astype(str).str.lower().str.contains(
                r"\b(" + "|".join(ROLE_WORDS_EXCLUDE) + r")\b"
            )
            df = df[~mask_excl]

        # also remove names that explicitly contain (leder/coach/admin)
        if name_col:
            mask_name_leader = df[name_col].astype(str).str.lower().str.contains(r"\(.*(leder|coach|admin).*\)")
            df = df[~mask_name_leader]

        if not name_col:
            continue
        if df.empty:
            continue

        members, statuses, reasons = [], [], []
        for _, r in df.iterrows():
            name = str(r.get(name_col, "")).strip()
            if not name:
                continue
            raw_status, raw_reason = infer_status_from_row(r, name_col, status_col)
            if not raw_status and status_col:   # final fallback to the raw cell
                raw_status = str(r.get(status_col, "")).strip()
            if not raw_reason and reason_col:
                raw_reason = str(r.get(reason_col, "")).strip()

            members.append(name)
            statuses.append(raw_status)
            reasons.append(raw_reason)

        if members:
            frames.append(pd.DataFrame({"Member": members, "Raw Status": statuses, "Raw Reason": reasons}))

    if not frames:
        return pd.DataFrame(columns=["Member", "Raw Status", "Raw Reason"])
    return pd.concat(frames, ignore_index=True).fillna("")

# ---------------- Status normalization & coloring ----------------
def normalize_status(raw: str) -> str:
    if raw is None: return "No response"
    r_norm = re.sub(r"[^a-zA-ZæøåÆØÅ\s]", " ", str(raw).strip().lower())

    if any(w in r_norm for w in [
        "yes","accepted","attending","present","going","kommer","deltar","påmeldt","ja","møter"
    ]): return "Present"

    if any(w in r_norm for w in [
        "no ","declin","absent","kommer ikke","deltar ikke","nei","fravær","kan ikke","not going"
    ]): return "Absent"

    if any(w in r_norm for w in [
        "no response","unknown","maybe","usikker","kanskje","ubesvart","ingen svar","not responded","har ikke svart"
    ]): return "No response"

    if str(raw).strip() in {"", "-", "—", "nan"}: return "No response"
    return "No response"

def status_to_cell(status: str, reason: str) -> Tuple[str, str]:
    st = normalize_status(status)
    if st == "Present":
        return ("Present", "present")
    if st == "Absent":
        txt = "Absent" + (f" — {str(reason).strip()}" if str(reason).strip() else "")
        return (txt, "absent")
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

def xlsx_all_text(xbytes: bytes) -> str:
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(xbytes), data_only=True)
        parts = []
        for ws in wb.worksheets:
            for r in ws.iter_rows(min_row=1, max_row=min(ws.max_row or 0, 200), values_only=True):
                for c in r:
                    if isinstance(c, str) and c.strip():
                        parts.append(c.strip())
        return "\n".join(parts)
    except Exception:
        return ""

# ---------------- Collect + build matrix ----------------
async def collect_events_and_attendance() -> Tuple[List[str], Dict[str, Dict[str, Tuple[str, str]]], List[Dict[str, Any]]]:
    sp = spond.Spond(username=os.environ["SPOND_USERNAME"], password=os.environ["SPOND_PASSWORD"])
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

        included: List[Tuple[str, str, Optional[datetime]]] = []
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
                    xb = await sp.get_event_attendance_xlsx(eid)
                    if contains_istrening(xlsx_all_text(xb)):
                        matched_src = "xlsx"
                    if not start_utc:
                        guess = parse_datetime_from_text(xlsx_all_text(xb))
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

            if start_utc:
                start_local = start_utc.astimezone(TIMEZONE)
                header = f"{start_local:%Y-%m-%d %H:%M} — {title or '(no title)'}"
            else:
                header = f"(no time) — {title or '(no title)'}"
            included.append((eid, header, start_utc))

        included.sort(key=lambda t: (t[2] or CUTOFF_UTC))
        event_headers = [h for _, h, _ in included]

        per_member: Dict[str, Dict[str, Tuple[str, str]]] = {}
        for (eid, header, _) in included:
            table = pd.DataFrame()
            try:
                xbytes = await sp.get_event_attendance_xlsx(eid)
                table = read_attendance_xlsx_all(xbytes)
            except Exception as e:
                log(f"WARNING: XLSX not available for {eid}: {e}")

            if not table.empty:
                for _, row in table.iterrows():
                    name   = str(row.get("Member", "")).strip()
                    raw_st = str(row.get("Raw Status", "")).strip()
                    raw_re = str(row.get("Raw Reason", "")).strip()
                    if not name: 
                        continue
                    txt, code = status_to_cell(raw_st, raw_re)
                    per_member.setdefault(name, {})[header] = (txt, code)

        return event_headers, per_member, debug_rows

    finally:
        try:
            if sp and getattr(sp, "clientsession", None):
                await sp.clientsession.close()
        except Exception:
            pass

def build_matrix(event_headers: List[str], per_member: Dict[str, Dict[str, Tuple[str, str]]]) -> Tuple[List[List[str]], List[List[str]]]:
    def is_admin_like(name: str) -> bool:
        ln = name.lower()
        return any(w in ln for w in ("coach", "trener", "leder", "admin"))

    members = sorted([m for m in per_member.keys() if m and not is_admin_like(m)])

    values: List[List[str]] = []
    colors: List[List[str]] = []

    for m in members:
        row_values = [m, "", "", ""]
        row_colors: List[str] = []
        present = noresp = absent = 0

        for eh in event_headers:
            text, code = per_member.get(m, {}).get(eh, ("No response", "noresp"))
            row_values.append(text); row_colors.append(code)
            if code == "present": present += 1
            elif code == "absent": absent += 1
            else: noresp += 1

        missed = absent + noresp
        row_values[1] = str(present)
        row_values[2] = str(missed)
        row_values[3] = str(noresp)

        values.append(row_values); colors.append(row_colors)

    return values, colors

def write_wide_sheet(sh, ws_att, event_headers: List[str], values: List[List[str]], colors: List[List[str]]):
    header = FIXED_COLS + event_headers
    ws_att.clear()
    ws_att.update([header] + values)

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

    color_map = {
        "present": {"red": 0.85, "green": 0.95, "blue": 0.85},
        "absent":  {"red": 0.98, "green": 0.85, "blue": 0.85},
        "noresp":  {"red": 0.93, "green": 0.86, "blue": 0.97},
    }

    requests = []
    n_rows, n_events = len(values), len(event_headers)
    if n_rows > 0 and n_events > 0:
        for r in range(n_rows):
            for c in range(n_events):
                code = colors[r][c]
                rgb = color_map.get(code)
                if not rgb: continue
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": ws_att.id,
                            "startRowIndex": 1 + r,
                            "endRowIndex":   2 + r,
                            "startColumnIndex": 4 + c,
                            "endColumnIndex":   5 + c,
                        },
                        "cell": {"userEnteredFormat": {"backgroundColor": rgb}},
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })
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
    values, colors = build_matrix(event_headers, per_member)

    write_wide_sheet(sh, ws_att, event_headers, values, colors)
    write_debug(ws_dbg, dbg_rows)

    log("Sync complete.")

if __name__ == "__main__":
    asyncio.run(main())
