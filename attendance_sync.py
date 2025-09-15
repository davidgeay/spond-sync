# attendance_sync.py
# Secrets required: SPOND_USERNAME, SPOND_PASSWORD, GOOGLE_SERVICE_ACCOUNT_JSON, SHEET_ID
# Optional: TIMEZONE (default Europe/Oslo), PLAYER_ALLOWLIST (JSON array or comma list)
# Deps: spond gspread google-auth pandas openpyxl python-dateutil

import os, io, json, asyncio, re
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
from dateutil import parser as dtparser
import gspread
from google.oauth2.service_account import Credentials
from spond import spond

# ---------------- Config ----------------
GROUP_NAME = "IHKS G2008b/G2009b"

TIMEZONE = ZoneInfo(os.getenv("TIMEZONE", "Europe/Oslo"))
CUTOFF_LOCAL = datetime(2025, 8, 1, 0, 0, tzinfo=TIMEZONE)
CUTOFF_UTC = CUTOFF_LOCAL.astimezone(timezone.utc)
NOW_UTC = datetime.now(timezone.utc)

SHEET_ATT = "Attendance"
SHEET_DBG = "Debug"
FIXED_COLS = ["Player", "Total Present", "Total Missed", "Total Unanswered"]

ISTR_PAT = re.compile(r"\bistrening\b", re.IGNORECASE)

def log(msg: str): print(f"[spond-sync] {msg}", flush=True)

# ---------------- Sheets helpers ----------------
def sheets_client() -> gspread.Client:
    svc = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(
        svc, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)

def open_spreadsheet(gc: gspread.Client):
    return gc.open_by_key(os.environ["SHEET_ID"])

def get_or_create_ws(sh, title: str):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=2000, cols=50)
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
        return " ".join([f"{k}: {to_text(x)}" for k, x in v.items() if x is not None])
    if isinstance(v, (list, tuple, set)):
        return " ".join([to_text(x) for x in v if x is not None])
    return str(v)

def contains_istrening(*vals: Any) -> bool:
    for v in vals:
        if ISTR_PAT.search(to_text(v)): return True
    return False

def key_name(name: str) -> str:
    if not name: return ""
    s = re.sub(r"\(.*?\)", "", name)           # drop parenthesis content
    s = re.sub(r"[|,;/].*$", "", s)            # drop trailing after separators
    s = re.sub(r"\s+", " ", s).strip().casefold()
    return s

def parse_start_utc(d: Dict[str, Any]) -> Optional[datetime]:
    raw = _pick(d, "startTimeUtc","start_time_utc","startTime","start","startAt",
                "start_at","startDateTime","start_datetime","utcStart","utc_start",
                "startTimestamp","start_timestamp")
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

# ---------------- Allowlist ----------------
def load_allowlist() -> Dict[str, str]:
    """
    Returns dict of normalized_key -> canonical_display_name
    If no allowlist provided, returns empty dict (meaning 'not enforced').
    """
    raw = os.getenv("PLAYER_ALLOWLIST", "").strip()
    if not raw:
        return {}
    names: List[str] = []
    try:
        arr = json.loads(raw)
        if isinstance(arr, list):
            names = [str(x).strip() for x in arr if str(x).strip()]
    except Exception:
        names = [x.strip() for x in re.split(r",|\n", raw) if x.strip()]
    out = {}
    for n in names:
        out[key_name(n)] = n  # normalized -> canonical
    return out

# ---------------- Attendance parsing ----------------
NAME_CANDS    = ["Name","Navn","Member name","Member","Participant","Deltaker","Spiller","Player"]
STATUS_CANDS  = ["Response","Svar","Status","Attendance","Attending","RSVP","Svarstatus"]
REASON_CANDS  = ["Note","Reason","Absence reason","Kommentar","Begrunnelse","Fraværsgrunn","Årsak","Notes","Message"]
CHECKIN_CANDS = ["Checked in","Innsjekket","Oppmøte","Møtt"]

YES_TOKENS = ("accepted","attending","present","going","kommer","deltar","påmeldt","ja","møter","checked in","check","✓","ok")
NO_TOKENS  = ("declined","absent","kommer ikke","deltar ikke","nei","fravær","kan ikke","not going","rejected","✗","avslått","nei takk")
NR_TOKENS  = ("no response","unknown","maybe","usikker","kanskje","ubesvart","ingen svar","pending","not responded","har ikke svart")

YES_HDR_HINTS    = re.compile(r"(attend|kommer|deltar|påmeld|present|going|ja|møter|check)", re.I)
NO_HDR_HINTS     = re.compile(r"(declin|kommer\s*ikke|deltar\s*ikke|nei|absent|not\s*going|kan\s*ikke|fravær|avslå)", re.I)
NORES_HDR_HINTS  = re.compile(r"(no\s*resp|ubesvar|ingen\s*svar|not\s*respond|pending)", re.I)

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

def _truthy(v: Any) -> bool:
    s = str(v).strip().lower()
    return s in {"1","true","yes","ja","x","✓","present","checked","check","kommer","deltar","påmeldt","going","ok"}

def normalize_status(raw: Any) -> str:
    s = str(raw or "").strip().lower()
    if any(t in s for t in YES_TOKENS): return "Present"
    if any(t in s for t in NO_TOKENS):  return "Absent"
    if s == "" or any(t in s for t in NR_TOKENS):  return "No response"
    # boolean-like fallback
    if s in {"1","true","yes","ja","✓","ok"}: return "Present"
    if s in {"0","false","no","nei","✗"}:     return "Absent"
    return "No response"

def infer_status_from_row(row: pd.Series, name_col: str,
                          status_col: Optional[str],
                          checkin_col: Optional[str]) -> Tuple[str, str]:
    # explicit status
    if status_col:
        st = str(row.get(status_col, "")).strip()
        if st:
            return normalize_status(st), ""
    # explicit check-in
    if checkin_col and _truthy(row.get(checkin_col, "")):
        return "Present",""
    # header inference
    for col in row.index:
        if col == name_col: continue
        val = row[col]
        if YES_HDR_HINTS.search(str(col)) and _truthy(val):
            return "Present", ""
        if NO_HDR_HINTS.search(str(col)) and _truthy(val):
            return "Absent", ""
        if NORES_HDR_HINTS.search(str(col)) and _truthy(val):
            return "No response", ""
    # text scan
    blob = " ".join([str(v) for k, v in row.items() if k != name_col and pd.notna(v)]).lower()
    if any(t in blob for t in YES_TOKENS): return "Present",""
    if any(t in blob for t in NO_TOKENS):  return "Absent",""
    if any(t in blob for t in NR_TOKENS):  return "No response",""
    return "No response",""

def read_attendance_xlsx(xbytes: bytes) -> pd.DataFrame:
    """
    Returns a tidy frame: columns = Member | Status | Reason
    """
    try:
        sheets = pd.read_excel(io.BytesIO(xbytes), sheet_name=None)
    except Exception:
        return pd.DataFrame(columns=["Member","Status","Reason"])

    frames: List[pd.DataFrame] = []
    for _, df in (sheets or {}).items():
        if df is None or df.empty: 
            continue
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]

        name_col    = _pick_col(df, NAME_CANDS)
        status_col  = _pick_col(df, STATUS_CANDS)
        reason_col  = _pick_col(df, REASON_CANDS)
        checkin_col = _pick_col(df, CHECKIN_CANDS)
        if not name_col:
            continue

        members, statuses, reasons = [], [], []
        for _, r in df.iterrows():
            name = str(r.get(name_col, "")).strip()
            if not name: continue
            st, _ = infer_status_from_row(r, name_col, status_col, checkin_col)
            rsn = str(r.get(reason_col, "")).strip() if reason_col else ""
            members.append(name); statuses.append(st); reasons.append(rsn)

        if members:
            frames.append(pd.DataFrame({"Member": members, "Status": statuses, "Reason": reasons}))

    if not frames:
        return pd.DataFrame(columns=["Member","Status","Reason"])

    return pd.concat(frames, ignore_index=True).fillna("")

def extract_statuses_from_json(ev: Dict[str,Any]) -> Dict[str,str]:
    """
    Return norm_name -> 'Present' | 'Absent' | 'No response'
    Scans common arrays and fields.
    """
    by_name: Dict[str,str] = {}

    arrays = []
    for k in ("participants","members","invites","responses","attendances","attendance","rsvps"):
        v = ev.get(k)
        if isinstance(v, list): arrays.append(v)

    def get_stat(p: Dict[str,Any]) -> Optional[str]:
        # look through common keys
        for k in ("status","response","rsvpStatus","attendanceStatus","answer","state"):
            if k in p and p[k] is not None:
                st = normalize_status(p[k])
                return st
        # boolean hints
        if str(p.get("isAttending","")).lower() in {"true","1"}: return "Present"
        if str(p.get("declined","")).lower() in {"true","1"}:    return "Absent"
        return None

    for arr in arrays:
        for p in arr:
            name = _pick(p, "name","fullName","memberName","title","displayName") or ""
            name = str(name).strip()
            if not name: continue
            st = get_stat(p)
            if st is None:
                # sometimes nested like {"member": {"name": ...}, "status": ...}
                m = p.get("member") or p.get("participant")
                if isinstance(m, dict):
                    nm2 = str(_pick(m, "name","fullName","memberName","title","displayName") or "").strip()
                    if nm2: name = nm2
                st = get_stat(p) or "No response"
            by_name[key_name(name)] = st

    return by_name

def status_to_cell(status: str, reason: str) -> Tuple[str, str]:
    st = normalize_status(status)
    if st == "Present":
        return ("Present", "present")
    if st == "Absent":
        txt = "Absent" + (f" — {str(reason).strip()}" if str(reason).strip() else "")
        return (txt, "absent")
    return ("No response", "noresp")

# ---------------- Spond flows ----------------
async def resolve_group_id(sp: spond.Spond) -> Optional[str]:
    groups = await sp.get_groups()
    log(f"Found {len(groups)} groups.")
    gid = None
    for g in groups:
        nm = _pick(g, "name","title","groupName") or ""
        gid_candidate = _pick(g, "id","groupId","uid")
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
                    if c is None: continue
                    cs = str(c).strip()
                    if cs: parts.append(cs)
        return "\n".join(parts)
    except Exception:
        return ""

def xlsx_contains_istrening(xbytes: bytes) -> bool:
    return contains_istrening(xlsx_all_text(xbytes))

# ---------------- Collect ----------------
async def collect_events_and_attendance() -> Tuple[List[str], Dict[str, Dict[str, Tuple[str, str]]], List[Dict[str, Any]], Dict[str, Any]]:
    sp = spond.Spond(username=os.environ["SPOND_USERNAME"], password=os.environ["SPOND_PASSWORD"])
    roster = load_allowlist()  # norm -> canonical
    try:
        gid = await resolve_group_id(sp)
        if not gid:
            return [], {}, [], {}

        log(f"Fetching events (UTC {CUTOFF_UTC.isoformat()} → {NOW_UTC.isoformat()}) ...")
        try:
            events = await sp.get_events(group_id=gid, min_start=CUTOFF_UTC, max_start=NOW_UTC,
                                         include_scheduled=True, max_events=500)
        except TypeError:
            events = await sp.get_events(min_start=CUTOFF_UTC, max_start=NOW_UTC)

        log(f"Fetched {len(events)} events (list may be minimal).")

        included: List[Tuple[str, str, Optional[datetime]]] = []
        dbg_events: List[Dict[str, Any]] = []

        for le in events:
            eid = _pick(le, "id","eventId","uid")
            if not eid: 
                continue
            try:
                ev = await sp.get_event(eid)
            except Exception as e:
                log(f"WARNING get_event {eid}: {e}")
                continue

            title = _pick(ev, "title","name","eventName","subject") or ""
            start_utc = parse_start_utc(ev)
            start_disp = start_utc.isoformat() if start_utc else "NO-START"

            matched = ""
            if contains_istrening(ev):
                matched = "details"
            else:
                try:
                    xb = await sp.get_event_attendance_xlsx(eid)
                    if xlsx_contains_istrening(xb):
                        matched = "xlsx"
                    if not start_utc:
                        guess = parse_datetime_from_text(xlsx_all_text(xb))
                        if guess: start_utc = guess
                except Exception:
                    pass

            date_ok = True if start_utc is None else (start_utc >= CUTOFF_UTC)
            include = (matched != "") and date_ok

            dbg_events.append({
                "Event ID": eid,
                "Event Title": title[:200],
                "Start UTC": start_disp,
                "Matched (details/xlsx)": matched or "no",
                "Cutoff Date OK": "Yes" if date_ok else "No",
                "Included": "Yes" if include else "No",
            })
            if not include:
                continue

            header = (f"{start_utc.astimezone(TIMEZONE):%Y-%m-%d %H:%M} — {title or '(no title)'}"
                      if start_utc else f"(no time) — {title or '(no title)'}")
            included.append((eid, header, start_utc))

        included.sort(key=lambda t: (t[2] or CUTOFF_UTC))
        event_headers = [h for _, h, _ in included]

        # Build rows per canonical roster name
        per_member: Dict[str, Dict[str, Tuple[str, str]]] = {canon: {} for canon in (roster.values() or [])}
        name_hits = {"matched": {}, "unmatched": set()}  # for debug

        def map_to_roster(display_name: str) -> Optional[str]:
            nk = key_name(display_name)
            if roster:
                canon = roster.get(nk)
                if canon:
                    name_hits["matched"][canon] = name_hits["matched"].get(canon, 0) + 1
                else:
                    name_hits["unmatched"].add(display_name)
                return canon
            # If no roster provided, fall back to using cleaned name
            return display_name

        for (eid, header, _) in included:
            # 1) JSON statuses
            json_status_by_norm: Dict[str,str] = {}
            try:
                ev = await sp.get_event(eid)
                json_status_by_norm = extract_statuses_from_json(ev)
            except Exception:
                pass

            # 2) XLSX statuses
            xdf = pd.DataFrame()
            try:
                xb = await sp.get_event_attendance_xlsx(eid)
                xdf = read_attendance_xlsx(xb)
            except Exception:
                pass

            # Build a temp map display_name -> (status, reason)
            tmp: Dict[str, Tuple[str,str]] = {}

            # Prefer XLSX (more reliable), then JSON fill-ins
            if not xdf.empty:
                for _, r in xdf.iterrows():
                    nm = str(r["Member"]).strip()
                    st = str(r["Status"]).strip()
                    rs = str(r.get("Reason","")).strip()
                    tmp[nm] = status_to_cell(st, rs)
            # Fill from JSON if missing
            for n_norm, st in json_status_by_norm.items():
                # We don't know the exact display form here, so store only if we don't already have XLSX
                # This will be mapped via roster below.
                # Keep reason empty for JSON.
                tmp.setdefault(n_norm, status_to_cell(st, ""))

            # Map tmp -> canonical roster names
            for name_like, (txt, code) in tmp.items():
                canon = map_to_roster(name_like)
                if not canon:
                    # If roster enforced and this name isn't on it, skip
                    continue
                per_member.setdefault(canon, {})
                per_member[canon][header] = (txt, code)

        # If roster is enforced, ensure everyone appears even if no data
        if roster:
            for canon in roster.values():
                per_member.setdefault(canon, {})

        # Build relevance/debug info
        dbg_info = {
            "Roster size": len(roster) if roster else "(no roster enforced)",
            "Matched names": len(name_hits["matched"]),
            "Unmatched samples": ", ".join(sorted(list(name_hits["unmatched"]))[:10]) if name_hits["unmatched"] else "",
        }

        return event_headers, per_member, dbg_events, dbg_info

    finally:
        try:
            if sp and getattr(sp, "clientsession", None):
                await sp.clientsession.close()
        except Exception:
            pass

# ---------------- Build sheet ----------------
def build_matrix(event_headers: List[str], per_member: Dict[str, Dict[str, Tuple[str, str]]]) -> Tuple[List[List[str]], List[List[str]]]:
    members = sorted(per_member.keys(), key=lambda s: s.lower())
    values: List[List[str]] = []
    colors: List[List[str]] = []

    for m in members:
        row_vals = [m, "", "", ""]
        row_cols: List[str] = []
        present = absent = noresp = 0

        for eh in event_headers:
            text, code = per_member.get(m, {}).get(eh, ("No response","noresp"))
            row_vals.append(text)
            row_cols.append(code)
            if code == "present": present += 1
            elif code == "absent": absent += 1
            else: noresp += 1

        missed = absent + noresp
        row_vals[1] = str(present)
        row_vals[2] = str(missed)
        row_vals[3] = str(noresp)

        values.append(row_vals)
        colors.append(row_cols)

    return values, colors

def write_wide_sheet(sh, ws_att, event_headers: List[str], values: List[List[str]], colors: List[List[str]]):
    header = FIXED_COLS + event_headers
    ws_att.clear()
    ws_att.update([header] + values)

    # Freeze header + name column
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
        "present": {"red": 0.85, "green": 0.95, "blue": 0.85},  # green-ish
        "absent":  {"red": 0.98, "green": 0.85, "blue": 0.85},  # red-ish
        "noresp":  {"red": 0.93, "green": 0.86, "blue": 0.97},  # purple-ish
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

    log("Attendance sheet updated.")

def write_debug(ws_dbg, dbg_rows: List[Dict[str, Any]], dbg_info: Dict[str, Any]):
    cols = ["Event ID","Event Title","Start UTC","Matched (details/xlsx)","Cutoff Date OK","Included"]
    ws_dbg.clear()
    data = [cols] + [[r.get(c,"") for c in cols] for r in dbg_rows] if dbg_rows else [cols, ["(no events)", "", "", "", "", ""]]
    ws_dbg.update(data)

    # Append summary block
    ws_dbg.append_row([])
    ws_dbg.append_row(["Summary"])
    for k, v in (dbg_info or {}).items():
        ws_dbg.append_row([str(k), str(v)])

    log("Debug sheet updated.")

# ---------------- Main ----------------
async def main():
    gc = sheets_client()
    sh = open_spreadsheet(gc)
    ws_att = get_or_create_ws(sh, SHEET_ATT)
    ws_dbg = get_or_create_ws(sh, SHEET_DBG)

    headers, per_member, dbg_events, dbg_info = await collect_events_and_attendance()
    values, colors = build_matrix(headers, per_member)

    write_wide_sheet(sh, ws_att, headers, values, colors)
    write_debug(ws_dbg, dbg_events, dbg_info)
    log("Sync complete.")

if __name__ == "__main__":
    asyncio.run(main())
