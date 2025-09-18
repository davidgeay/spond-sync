# attendance_sync_summary.py
# Deps: spond gspread google-auth pandas openpyxl python-dateutil
# Secrets required: SPOND_USERNAME, SPOND_PASSWORD, GOOGLE_SERVICE_ACCOUNT_JSON, SHEET_ID

import os, io, json, asyncio, re
from typing import Any, Dict, List, Tuple, Optional
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from dateutil import parser as dtparser
from spond import spond

# ---------------- Config ----------------
GROUP_NAME = "IHKS G2008b/G2009b"
TIMEZONE = ZoneInfo(os.getenv("TIMEZONE", "Europe/Oslo"))
CUTOFF_LOCAL = datetime(2025, 7, 1, 0, 0, tzinfo=TIMEZONE)   # July 1 cutoff
CUTOFF_UTC = CUTOFF_LOCAL.astimezone(timezone.utc)
NOW_UTC = datetime.now(timezone.utc)

SHEET_ATT = "Attendance"

# ---------------- Allowlist ----------------
ALLOWLIST = [
    "Adrian Jekteberg","Albert Hetland","Arnt Olav Gunstead","Balder Skjervheim Kvandal",
    "Bartlomiej Lapinski","Christian Jakobsen","David Janson Lund","Ewan Shirlaw",
    "Falk Bruland Austefjord","Filip Kamecki","Henry Orrell","Isac Rosseland",
    "Jesper Abercrombie","Kevin Wilford Norheim Tjelta","Max Berntsen","Norberts Ozols",
    "Ole Elvebakk","Rudi Hauge Risa","Sava Durdevic","Torjus Line Bjelland",
    "Tristan Bjarnisson","Trym Haug Bjørnestad","Ulrik Reitan Seidel","Vetle Sele",
    "William Madsen","Zakhar Avdeienko"
]
ALLOWSET = {n.casefold(): n for n in ALLOWLIST}

# ---------------- Utils ----------------
def log(msg: str): print(f"[spond-sync] {msg}", flush=True)

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
        return sh.add_worksheet(title=title, rows=2000, cols=200)
    except gspread.exceptions.APIError as e:
        if "already exists" in str(e).lower():
            return sh.worksheet(title)
        raise

def parse_start_utc(ev: Dict[str, Any]) -> Optional[datetime]:
    raw = ev.get("startTimeUtc") or ev.get("start") or ev.get("startAt")
    if not raw: return None
    try:
        s = str(raw).replace("Z","+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

# ---------------- Status normalization ----------------
def normalize_status(status: str, reason: str) -> Tuple[str,str]:
    """Return (display_text, code)"""
    s = (status or "").lower()
    r = (reason or "").strip()
    if "accept" in s or "present" in s or "attending" in s or "going" in s or "ja" in s:
        return ("Present","present")
    if "declin" in s or "absent" in s or "nei" in s or "not going" in s:
        if r:
            return (f"Absent — {r}","absent_reason")
        else:
            return ("?","absent_noreason")
    if "no response" in s or "pending" in s or "unknown" in s or not s:
        return ("No response","noresp")
    return ("No response","noresp")

# ---------------- Collect ----------------
async def resolve_group_id(sp: spond.Spond) -> Optional[str]:
    groups = await sp.get_groups()
    for g in groups:
        nm = g.get("name") or g.get("title") or ""
        gid = g.get("id")
        if nm.strip().lower() == GROUP_NAME.strip().lower():
            return gid
    return None

async def collect_events(sp: spond.Spond):
    gid = await resolve_group_id(sp)
    if not gid: return []

    events = await sp.get_events(group_id=gid, min_start=CUTOFF_UTC, max_start=NOW_UTC, max_events=500)
    out = []
    for e in events:
        eid = e.get("id")
        if not eid: continue
        ev = await sp.get_event(eid)
        start = parse_start_utc(ev)
        if not start: continue
        local = start.astimezone(TIMEZONE)

        # --- filters ---
        if start < CUTOFF_UTC: continue
        if local.weekday() >= 5: continue    # skip Sat/Sun
        if local.hour < 17: continue         # skip before 17:00

        title = ev.get("title") or "(no title)"
        header = f"{local:%Y-%m-%d %H:%M} — {title}"
        out.append((eid, header, start))
    return sorted(out, key=lambda t: t[2])

async def collect_attendance(sp: spond.Spond, events):
    per_member: Dict[str, Dict[str, Tuple[str,str]]] = {n:{} for n in ALLOWLIST}
    for eid, header, _ in events:
        ev = await sp.get_event(eid)
        arrs = []
        for k in ("participants","members","responses","attendance","rsvps"):
            if isinstance(ev.get(k),list):
                arrs += ev[k]
        for p in arrs:
            name = p.get("member",{}).get("name") or p.get("name") or ""
            reason = p.get("reason") or p.get("note") or ""
            status = p.get("status") or p.get("response") or ""
            disp, code = normalize_status(str(status), str(reason))
            if name.casefold() in ALLOWSET:
                canon = ALLOWSET[name.casefold()]
                per_member[canon][header] = (disp,code)
    return per_member

# ---------------- Build matrix ----------------
def build_matrix(events, per_member):
    total_events = len(events)
    members = sorted(ALLOWLIST,key=str.lower)
    values = []
    colors = []

    for m in members:
        row = [m,"","", ""]
        rowcols = []
        present=absent=noresp=0

        for _, header, _ in events:
            disp, code = per_member.get(m,{}).get(header,("No response","noresp"))
            # --- summary counts ---
            if code=="present":
                present+=1
            elif code=="absent_noreason":
                absent+=1
            elif code in ("absent_reason","noresp"):
                noresp+=1

            row.append(disp)
            rowcols.append(code)

        # summary columns
        def pct(n): return f"{n} ({(n/total_events*100):.0f}%)" if total_events else "0 (0%)"
        row[1] = pct(present)
        row[2] = pct(absent)
        row[3] = pct(noresp)

        values.append(row)
        colors.append(rowcols)
    return values, colors

# ---------------- Write ----------------
def write_sheet(sh, ws, events, values, colors):
    headers = ["Player","Attended","Declined","Unanswered"]+[h for _,h,_ in events]
    old = ws.get_all_values()
    ws.clear()
    ws.update([headers]+values)

    # Freeze header + name col
    sh.batch_update({"requests":[{"updateSheetProperties":{
        "properties":{"sheetId":ws.id,"gridProperties":{"frozenRowCount":1,"frozenColumnCount":1}},
        "fields":"gridProperties.frozenRowCount,gridProperties.frozenColumnCount"}}]})

    # Colors
    color_map = {
        "present":{"red":0.85,"green":0.95,"blue":0.85},     # green
        "absent_reason":{"red":0.98,"green":0.85,"blue":0.85}, # red
        "absent_noreason":{"red":0.93,"green":0.86,"blue":0.97}, # purple
        "noresp":{"red":0.93,"green":0.86,"blue":0.97},      # purple
    }

    reqs=[]
    for r,rowcols in enumerate(colors):
        for c,code in enumerate(rowcols):
            rgb=color_map.get(code)
            if not rgb: continue
            cell_val = values[r][4+c]  # offset 4 fixed cols
            # --- override: if user manually set "?" keep purple ---
            if old and len(old)>r+1 and len(old[r+1])>c+4:
                prev=old[r+1][c+4]
                if prev.strip()=="?":
                    rgb=color_map["absent_noreason"]
                    values[r][4+c]="?"
            reqs.append({"repeatCell":{
                "range":{"sheetId":ws.id,"startRowIndex":1+r,"endRowIndex":2+r,
                         "startColumnIndex":4+c,"endColumnIndex":5+c},
                "cell":{"userEnteredFormat":{"backgroundColor":rgb}},
                "fields":"userEnteredFormat.backgroundColor"}})
    for i in range(0,len(reqs),5000):
        sh.batch_update({"requests":reqs[i:i+5000]})

# ---------------- Main ----------------
async def main():
    gc=sheets_client()
    sh=open_spreadsheet(gc)
    ws=get_or_create_ws(sh,SHEET_ATT)

    sp=spond.Spond(username=os.environ["SPOND_USERNAME"],password=os.environ["SPOND_PASSWORD"])
    events=await collect_events(sp)
    per_member=await collect_attendance(sp,events)
    values,colors=build_matrix(events,per_member)
    write_sheet(sh,ws,events,values,colors)
    log("Sync complete.")
    if getattr(sp,"clientsession",None):
        await sp.clientsession.close()

if __name__=="__main__":
    asyncio.run(main())
