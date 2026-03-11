#!/usr/bin/env python3
"""
SGA Billing Scorecard — Data Fetcher
Fetches worklog data from Jira and capacity planning from Airtable.
Run locally or via GitHub Actions every 5 minutes.

Requirements: pip install requests
"""

import os, json, time, requests
from datetime import datetime, date, timedelta
from collections import defaultdict

# ─── CONFIG ─────────────────────────────────────────────────────────────────────────────
JIRA_BASE_URL  = "https://shopsys.atlassian.net"
JIRA_EMAIL     = os.environ["JIRA_EMAIL"]
JIRA_API_TOKEN = os.environ["JIRA_API_TOKEN"]
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE  = "appUYbP0DH662mQbA"
AIRTABLE_TABLE = "tblEYS1jZYjIADagB"   # Capacity Planning

WEEKLY_BILLABLE_TARGET = 356
BILLABILITY_TARGET     = 0.80

# ─── TEAM MEMBERS ──────────────────────────────────────────────────────────────────────────
TEAM_MEMBERS = [
    {"name": "Adam Duris",        "accountId": "712020:4488b5a7-b088-4a5a-9a78-7a94b2e82ffd", "team": "Patrik Team",  "target": 0.80},
    {"name": "Ahsan Javed",       "accountId": "712020:ebc61b18-0416-4683-b091-51ea2863e827", "team": "Apps Team",    "target": 0.60},
    {"name": "Dan Dulgerian",     "accountId": "712020:1d8f40be-0dfb-4ccd-b939-6a46d6ce25a2", "team": "Miro Team",    "target": 0.85},
    {"name": "David Gomola",      "accountId": "712020:8279b16f-cb5c-4c1b-8811-ee3afafc9814", "team": "Miro Team",    "target": 0.85},
    {"name": "David Kalla",       "accountId": "712020:75e42ba2-3da7-4ef9-8518-b2e43eb3f673", "team": "Miro Team",    "target": 0.80},
    {"name": "David Simoes",      "accountId": "5d09fca3c6c8340c472d2a43",                    "team": "Maintenance",  "target": 0.0},
    {"name": "Eva Martincova",    "accountId": "712020:35972894-7915-4f2c-beb7-5fb6d30e428b", "team": "Management",   "target": 0.0},
    {"name": "Frantisek Seifried","accountId": "712020:8501e5fc-65fc-4811-8963-1384b53baad9", "team": "Maintenance",  "target": 0.80},
    {"name": "Jakub Schubert",    "accountId": "712020:e3e48e9c-83f6-4920-9ef7-d61d13adc21f", "team": "Management",   "target": 0.0},
    {"name": "Joao Antunes",      "accountId": "5cfa31b59943230e77ad44e0",                    "team": "Independent",  "target": 0.75},
    {"name": "Marek Casnocha",    "accountId": "712020:a24c7e3c-0020-4dd0-ac06-1e6692faa6ed", "team": "Independent",  "target": 0.80},
    {"name": "Michal Srnicek",    "accountId": "5b700762e72afd064c8a45aa",                    "team": "Patrik Team",  "target": 0.80},
    {"name": "Miroslav Straka",   "accountId": "712020:d3e66acf-45af-44db-8ca4-c3f7bb6fcbd0", "team": "Miro Team",    "target": 0.80},
    {"name": "Patrik Bajer",      "accountId": "5e624b356e8fdd0cd8178257",                    "team": "Patrik Team",  "target": 0.80},
    {"name": "Roman Filenko",     "accountId": "712020:ba534d73-c28c-4548-abef-bbfadc02b076", "team": "Patrik Team",  "target": 0.80},
    {"name": "Ronalds Nordmanis", "accountId": "712020:04ef5dbe-3670-4f4e-b631-535b48c9874c", "team": "Independent",  "target": 0.80},
]

DISPLAY_NAMES = {
    "Adam Duris":         "Adam \u010euriš",
    "David Simoes":       "David Sim\u00f5es",
    "Eva Martincova":     "Eva Martincov\u00e1",
    "Frantisek Seifried": "Franti\u0161ek Seifried",
    "Joao Antunes":       "Jo\u00e3o Antunes",
    "Marek Casnocha":     "Marek \u010casnocha",
    "Michal Srnicek":     "Michal Srn\u00ed\u010dek",
}

ACCOUNT_MAP = {m["accountId"]: m for m in TEAM_MEMBERS}
ACCOUNT_IDS = set(m["accountId"] for m in TEAM_MEMBERS)

BILLABLE_PROJECT_IDS = {
    10237,10064,10121,10081,10169,10864,11029,10111,10130,10197,11392,10175,
    10147,11260,10732,10122,10080,11062,10271,10666,10104,10155,10232,10059,
    10141,10112,10123,10124,11359,10184,10633,11128,10115,10084,10085,10188,
    10234,10235,10187,10109,10196,10189,10369,10207,11326,10114,10205,10144,
    10217,10174,10224,10043,11095,10086,10154,10336,10116,10180,11293,10194,
    10996,10209,10930,10963,10168,10068,10054,10195,10198,10501,10798,10164,
    10236,10127,10158,10029,10143,10110,10062,10221,10140,10146,10567,11194,
    10106,10179,10076,10170,10139,10214,10092,10204,10177,10044,10063,10142,
    10100,10058,10156,10098,10233,10061,10145,10176,10094,11161,10213,10303,
    10162,10073,10200,10218,10231,10118,10071,10055,10119,10120,10225,10077,
    10468,10199,10131,10036,10013,10097,10215,10402,10600,11227,10160,10101,
    10831,10069,10435,10067,10096,10148,10075,10765,10028,10534,10128,10219,
    10079,10163,
}

# ─── DATE HELPERS ──────────────────────────────────────────────────────────────────────────
def week_bounds(d):
    monday = d - timedelta(days=d.weekday())
    return monday, monday + timedelta(days=6)

def month_bounds(d):
    first = d.replace(day=1)
    last  = (d.replace(month=d.month % 12 + 1, day=1) - timedelta(days=1)) if d.month < 12 else d.replace(day=31)
    return first, last

def ytd_bounds(d):
    return date(d.year, 1, 1), d

def working_days(start, end):
    count, cur = 0, start
    while cur <= end:
        if cur.weekday() < 5:
            count += 1
        cur += timedelta(days=1)
    return count

def date_to_ms(d):
    return int(datetime(d.year, d.month, d.day).timestamp() * 1000)

# ─── JIRA AUTH ─────────────────────────────────────────────────────────────────────────────
JIRA_AUTH = (JIRA_EMAIL, JIRA_API_TOKEN)

def jira_get_with_retry(url, params=None, max_retries=6):
    for attempt in range(max_retries):
        resp = requests.get(url, auth=JIRA_AUTH, params=params)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", min(2 ** attempt, 60)))
            print(f"  Rate limited. Waiting {wait}s...")
            time.sleep(wait)
            continue
        return resp
    return resp

# ─── BULK WORKLOG FETCH ────────────────────────────────────────────────────────────────────────
def fetch_worklog_ids_since(since_ms):
    """
    GET /rest/api/3/worklog/updated?since={ms}
    Returns all worklog IDs updated since the given epoch-ms timestamp.
    """
    ids, since = [], since_ms
    while True:
        resp = jira_get_with_retry(
            f"{JIRA_BASE_URL}/rest/api/3/worklog/updated",
            params={"since": since},
        )
        resp.raise_for_status()
        data = resp.json()
        ids.extend(v["worklogId"] for v in data["values"])
        if data["lastPage"]:
            break
        since = data["until"]
    print(f"  Found {len(ids)} worklog IDs updated since YTD start")
    return ids


def fetch_worklogs_by_ids(worklog_ids):
    """
    POST /rest/api/3/worklog/list  (max 1000 per request)
    Returns full worklog objects: issueId, author, started, timeSpentSeconds.
    """
    all_worklogs = []
    batches = -(-len(worklog_ids) // 1000)   # ceiling division
    for i in range(0, len(worklog_ids), 1000):
        batch = worklog_ids[i : i + 1000]
        resp = requests.post(
            f"{JIRA_BASE_URL}/rest/api/3/worklog/list",
            auth=JIRA_AUTH,
            json={"ids": batch},
        )
        resp.raise_for_status()
        all_worklogs.extend(resp.json())
    print(f"  Fetched {len(all_worklogs)} worklog objects ({batches} batch(es))")
    return all_worklogs


def fetch_issue_projects(issue_ids):
    """
    Batch-fetch project IDs for a set of Jira issue IDs via JQL.
    Returns dict: issue_id (str) -> project_id (int)
    """
    issue_project = {}
    ids_list = list(issue_ids)
    for i in range(0, len(ids_list), 200):
        batch      = ids_list[i : i + 200]
        jql        = f"id in ({','.join(batch)})"
        next_token = None
        while True:
            params = {"jql": jql, "fields": "project", "maxResults": 200}
            if next_token:
                params["nextPageToken"] = next_token
            resp = jira_get_with_retry(
                f"{JIRA_BASE_URL}/rest/api/3/search/jql", params=params
            )
            resp.raise_for_status()
            data = resp.json()
            for issue in data.get("issues", []):
                issue_project[issue["id"]] = int(issue["fields"]["project"]["id"])
            next_token = data.get("nextPageToken")
            if not next_token:
                break
    print(f"  Resolved projects for {len(issue_project)} unique issues")
    return issue_project


# ─── COMPUTE ───────────────────────────────────────────────────────────────────────────────────
def compute_totals(all_worklogs, start_date, end_date):
    totals = defaultdict(lambda: {"total_seconds": 0, "billable_seconds": 0})
    for wl in all_worklogs:
        wl_date = wl["started"][:10]
        if start_date <= wl_date <= end_date:
            aid = wl["author"]["accountId"]
            if aid in ACCOUNT_IDS:
                secs = wl["timeSpentSeconds"]
                totals[aid]["total_seconds"] += secs
                if wl["_is_billable"]:
                    totals[aid]["billable_seconds"] += secs
    return dict(totals)


# ─── AIRTABLE ────────────────────────────────────────────────────────────────────────────────────
def fetch_capacity_planning():
    print("\nFetching Airtable Capacity Planning...")
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    url     = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}"
    records, offset = [], None
    while True:
        params = {"fields[]": ["Team Member", "Month", "Available Hours"], "pageSize": 100}
        if offset:
            params["offset"] = offset
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        records.extend(data["records"])
        offset = data.get("offset")
        if not offset:
            break
    result = {}
    for rec in records:
        f         = rec.get("fields", {})
        month_raw = f.get("Month", "")
        members   = f.get("Team Member", [])
        avail     = f.get("Available Hours", 0) or 0
        if month_raw and members:
            name = members[0] if isinstance(members[0], str) else members[0].get("name", "")
            result[f"{name}|{month_raw[:7]}"] = avail
    print(f"  Loaded {len(result)} entries.")
    return result


# ─── METRICS ──────────────────────────────────────────────────────────────────────────────────────
def secs_to_h(s):
    return round(s / 3600, 2)

def billability(bill, total):
    return round(bill / total, 4) if total > 0 else None

def build_period_metrics(worklogs, capacity, start, end, label):
    months = set()
    d = start.replace(day=1)
    while d <= end:
        months.add(d.strftime("%Y-%m"))
        d = (d.replace(month=d.month % 12 + 1, day=1) if d.month < 12
             else d.replace(year=d.year + 1, month=1, day=1))

    def prorate(month_str):
        y, m = int(month_str[:4]), int(month_str[5:7])
        ms = date(y, m, 1)
        me = (date(y, m % 12 + 1, 1) - timedelta(days=1)) if m < 12 else date(y, 12, 31)
        ov_s, ov_e = max(start, ms), min(end, me)
        if ov_s > ov_e:
            return 0.0
        wd_t = working_days(ms, me)
        return working_days(ov_s, ov_e) / wd_t if wd_t > 0 else 0.0

    members_out = []
    team_totals = defaultdict(lambda: {"total_h": 0, "billable_h": 0, "planned_h": 0})
    for m in TEAM_MEMBERS:
        aid       = m["accountId"]
        wl        = worklogs.get(aid, {"total_seconds": 0, "billable_seconds": 0})
        total_h   = secs_to_h(wl["total_seconds"])
        bill_h    = secs_to_h(wl["billable_seconds"])
        planned_h = round(sum(
            (capacity.get(f"{m['name']}|{mo}", 0) or 0) * prorate(mo)
            for mo in months
        ), 2)
        gap     = round(bill_h - planned_h, 2) if planned_h > 0 else None
        display = DISPLAY_NAMES.get(m["name"], m["name"])
        members_out.append({
            "name": display, "team": m["team"], "target": m["target"],
            "total_h": total_h, "billable_h": bill_h,
            "planned_h": planned_h, "gap_h": gap,
            "billability": billability(bill_h, total_h) if m["target"] > 0 else None,
        })
        t = team_totals[m["team"]]
        t["total_h"]    += total_h
        t["billable_h"] += bill_h
        t["planned_h"]  += planned_h

    teams_out = {
        tn: {
            "total_h":     round(t["total_h"], 2),
            "billable_h":  round(t["billable_h"], 2),
            "planned_h":   round(t["planned_h"], 2),
            "gap_h":       round(t["billable_h"] - t["planned_h"], 2) if t["planned_h"] > 0 else None,
            "billability": billability(t["billable_h"], t["total_h"]),
        }
        for tn, t in team_totals.items()
    }

    total_h_all   = sum(m["total_h"]   for m in members_out)
    bill_h_all    = sum(m["billable_h"] for m in members_out)
    planned_h_all = sum(m["planned_h"]  for m in members_out)
    return {
        "label": label, "start": start.isoformat(), "end": end.isoformat(),
        "total_h":    round(total_h_all, 2),
        "billable_h": round(bill_h_all, 2),
        "planned_h":  round(planned_h_all, 2),
        "gap_h":      round(bill_h_all - planned_h_all, 2) if planned_h_all > 0 else None,
        "billability": billability(bill_h_all, total_h_all),
        "members": members_out,
        "teams":   teams_out,
    }


# ─── ENTRY POINT ──────────────────────────────────────────────────────────────────────────────
def main():
    today = date.today()
    ws, we = week_bounds(today)
    ms, me = month_bounds(today)
    ys, ye = ytd_bounds(today)

    capacity = fetch_capacity_planning()

    print(f"\nFetching Jira worklogs (YTD: {ys} \u2192 {ye})")

    # Step 1: Get all worklog IDs updated since Jan 1 (~few pagination calls)
    worklog_ids = fetch_worklog_ids_since(date_to_ms(ys))

    # Step 2: Bulk-fetch full worklog details (1000 per request)
    raw_worklogs = fetch_worklogs_by_ids(worklog_ids)

    # Step 3: Filter to team members within YTD date range
    ys_str, ye_str = ys.isoformat(), ye.isoformat()
    team_worklogs = [
        wl for wl in raw_worklogs
        if wl["author"]["accountId"] in ACCOUNT_IDS
        and ys_str <= wl["started"][:10] <= ye_str
    ]
    print(f"  Team worklogs in YTD: {len(team_worklogs)}")

    # Step 4: Resolve project for each unique issue (for billability)
    issue_ids     = {wl["issueId"] for wl in team_worklogs}
    issue_project = fetch_issue_projects(issue_ids)

    # Step 5: Attach billability flag
    for wl in team_worklogs:
        pid = issue_project.get(wl["issueId"])
        wl["_is_billable"] = (pid in BILLABLE_PROJECT_IDS) if pid else False

    # Step 6: Slice into periods entirely in-memory — no more API calls
    print("\nComputing period metrics...")
    wl_w = compute_totals(team_worklogs, ws.isoformat(), we.isoformat())
    wl_m = compute_totals(team_worklogs, ms.isoformat(), me.isoformat())
    wl_y = compute_totals(team_worklogs, ys.isoformat(), ye.isoformat())

    week_data  = build_period_metrics(wl_w, capacity, ws, we, "week")
    month_data = build_period_metrics(wl_m, capacity, ms, me, "month")
    ytd_data   = build_period_metrics(wl_y, capacity, ys, ye, "ytd")
    week_data["team_weekly_target"] = WEEKLY_BILLABLE_TARGET

    output = {
        "lastUpdated":          datetime.utcnow().isoformat() + "Z",
        "generatedDate":        today.isoformat(),
        "weeklyBillableTarget": WEEKLY_BILLABLE_TARGET,
        "billabilityTarget":    BILLABILITY_TARGET,
        "week":  week_data,
        "month": month_data,
        "ytd":   ytd_data,
    }

    out_path = os.path.join(os.path.dirname(__file__), "..", "data.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nDone! data.json written.")
    print(f"  Week:  {week_data['billable_h']}h billable")
    print(f"  Month: {month_data['billable_h']}h billable")
    print(f"  YTD:   {ytd_data['billable_h']}h billable")


if __name__ == "__main__":
    main()
