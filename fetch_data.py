#!/usr/bin/env python3
"""
SGA Billing Scorecard — Data Fetcher
Fetches worklog data from Jira and capacity planning from Airtable.
Run locally or via GitHub Actions every 30 minutes.

Requirements: pip install requests python-dateutil
"""

import os
import json
import time
import requests
from datetime import datetime, date, timedelta
from collections import defaultdict

# ─── CONFIG ────────────────────────────────────────────────────────────────────

JIRA_BASE_URL   = "https://shopsys.atlassian.net"
JIRA_EMAIL      = os.environ["JIRA_EMAIL"]
JIRA_API_TOKEN  = os.environ["JIRA_API_TOKEN"]
AIRTABLE_TOKEN  = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE   = "appUYbP0DH662mQbA"
AIRTABLE_TABLE  = "tblEYS1jZYjIADagB"   # Capacity Planning

WEEKLY_BILLABLE_TARGET = 356             # team-wide weekly billable hours target
BILLABILITY_TARGET     = 0.80            # 80%
CURRENT_YEAR           = 2026

# ─── TEAM MEMBERS ──────────────────────────────────────────────────────────────

TEAM_MEMBERS = [
    {"name": "Adam Ďuriš",       "accountId": "712020:4488b5a7-b088-4a5a-9a78-7a94b2e82ffd", "team": "Patrik Team",    "target": 0.80},
    {"name": "Ahsan Javed",       "accountId": "712020:ebc61b18-0416-4683-b091-51ea2863e827", "team": "Apps Team",      "target": 0.60},
    {"name": "Dan Dulgerian",     "accountId": "712020:1d8f40be-0dfb-4ccd-b939-6a46d6ce25a2", "team": "Miro Team",      "target": 0.85},
    {"name": "David Gomola",      "accountId": "712020:8279b16f-cb5c-4c1b-8811-ee3afafc9814", "team": "Miro Team",      "target": 0.85},
    {"name": "David Kalla",       "accountId": "712020:75e42ba2-3da7-4ef9-8518-b2e43eb3f673", "team": "Miro Team",      "target": 0.80},
    {"name": "David Simões",      "accountId": "5d09fca3c6c8340c472d2a43",                    "team": "Maintenance",    "target": 0.0},
    {"name": "Eva Martincová",    "accountId": "712020:35972894-7915-4f2c-beb7-5fb6d30e428b", "team": "Management",     "target": 0.0},
    {"name": "František Seifried","accountId": "712020:8501e5fc-65fc-4811-8963-1384b53baad9", "team": "Maintenance",    "target": 0.80},
    {"name": "Jakub Schubert",    "accountId": "712020:e3e48e9c-83f6-4920-9ef7-d61d13adc21f", "team": "Management",     "target": 0.0},
    {"name": "João Antunes",      "accountId": "5cfa31b59943230e77ad44e0",                    "team": "Independent",    "target": 0.75},
    {"name": "Marek Časnocha",    "accountId": "712020:a24c7e3c-0020-4dd0-ac06-1e6692faa6ed", "team": "Independent",    "target": 0.80},
    {"name": "Michal Srníček",    "accountId": "5b700762e72afd064c8a45aa",                    "team": "Patrik Team",    "target": 0.80},
    {"name": "Miroslav Straka",   "accountId": "712020:d3e66acf-45af-44db-8ca4-c3f7bb6fcbd0", "team": "Miro Team",      "target": 0.80},
    {"name": "Patrik Bajer",      "accountId": "5e624b356e8fdd0cd8178257",                    "team": "Patrik Team",    "target": 0.80},
    {"name": "Roman Filenko",     "accountId": "712020:ba534d73-c28c-4548-abef-bbfadc02b076", "team": "Patrik Team",    "target": 0.80},
    {"name": "Ronalds Nordmanis", "accountId": "712020:04ef5dbe-3670-4f4e-b631-535b48c9874c", "team": "Independent",    "target": 0.80},
]

ACCOUNT_MAP = {m["accountId"]: m for m in TEAM_MEMBERS}
ACCOUNT_IDS = set(m["accountId"] for m in TEAM_MEMBERS)

# Billable project IDs (from Shopify People - Billable Clockwork report)
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
    10079,10163
}

# ─── DATE HELPERS ──────────────────────────────────────────────────────────────

def week_bounds(d: date):
    monday = d - timedelta(days=d.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday

def month_bounds(d: date):
    first = d.replace(day=1)
    if d.month == 12:
        last = d.replace(day=31)
    else:
        last = (d.replace(month=d.month+1, day=1) - timedelta(days=1))
    return first, last

def ytd_bounds(d: date):
    return date(d.year, 1, 1), d

def working_days(start: date, end: date) -> int:
    count = 0
    current = start
    while current <= end:
        if current.weekday() < 5:
            count += 1
        current += timedelta(days=1)
    return count

# ─── JIRA HELPERS ──────────────────────────────────────────────────────────────

JIRA_AUTH = (JIRA_EMAIL, JIRA_API_TOKEN)

def jira_get(path: str, params: dict = None):
    url = f"{JIRA_BASE_URL}{path}"
    resp = requests.get(url, auth=JIRA_AUTH, params=params)
    resp.raise_for_status()
    return resp.json()

def search_issues_with_worklogs(start_date: str, end_date: str) -> list[dict]:
    """Return all issues that have worklogs by our users in the date range."""
    user_list = ",".join(f'"{uid}"' for uid in ACCOUNT_IDS)
    jql = (
        f'worklogAuthor in ({user_list}) '
        f'AND worklogDate >= "{start_date}" '
        f'AND worklogDate <= "{end_date}"'
    )
    issues = []
    start_at = 0
    while True:
        data = jira_get("/rest/api/3/search", {
            "jql": jql,
            "fields": "id,key,project",
            "maxResults": 100,
            "startAt": start_at,
        })
        issues.extend(data["issues"])
        print(f"  Fetched {len(issues)}/{data['total']} issues...")
        if start_at + 100 >= data["total"]:
            break
        start_at += 100
        time.sleep(0.2)   # be polite to the API
    return issues

def get_issue_worklogs(issue_key: str, start_date: str, end_date: str) -> list[dict]:
    """Return worklogs for an issue filtered by date range."""
    result = []
    start_at = 0
    while True:
        data = jira_get(f"/rest/api/3/issue/{issue_key}/worklog", {
            "startAt": start_at,
            "maxResults": 100,
        })
        for wl in data["worklogs"]:
            wl_date = wl["started"][:10]
            if start_date <= wl_date <= end_date:
                result.append(wl)
        if start_at + 100 >= data["total"]:
            break
        start_at += 100
    return result

# ─── MAIN FETCH ────────────────────────────────────────────────────────────────

def fetch_jira_worklogs(start_date: str, end_date: str) -> dict:
    """
    Returns dict keyed by accountId with:
      { "total_seconds": int, "billable_seconds": int }
    """
    print(f"\nFetching Jira worklogs {start_date} → {end_date}")
    issues = search_issues_with_worklogs(start_date, end_date)
    print(f"  Found {len(issues)} issues with worklogs. Fetching details...")

    totals = defaultdict(lambda: {"total_seconds": 0, "billable_seconds": 0})

    for i, issue in enumerate(issues):
        project_id = int(issue["fields"]["project"]["id"])
        is_billable = project_id in BILLABLE_PROJECT_IDS

        worklogs = get_issue_worklogs(issue["key"], start_date, end_date)
        for wl in worklogs:
            author_id = wl["author"]["accountId"]
            if author_id in ACCOUNT_IDS:
                secs = wl["timeSpentSeconds"]
                totals[author_id]["total_seconds"] += secs
                if is_billable:
                    totals[author_id]["billable_seconds"] += secs

        if (i + 1) % 20 == 0:
            print(f"  Processed {i+1}/{len(issues)} issues...")
            time.sleep(0.3)

    return dict(totals)

# ─── AIRTABLE FETCH ────────────────────────────────────────────────────────────

def fetch_capacity_planning() -> dict:
    """
    Returns dict keyed by "Name - YYYY-MM" with available_hours.
    """
    print("\nFetching Airtable Capacity Planning...")
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}"

    records = []
    offset = None
    while True:
        params = {
            "fields[]": ["ID", "Team Member", "Month", "Available Hours"],
            "pageSize": 100,
        }
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
        f = rec.get("fields", {})
        month_raw = f.get("Month", "")         # e.g. "2026-01-01"
        members   = f.get("Team Member", [])   # list of linked record names
        avail     = f.get("Available Hours", 0) or 0
        if month_raw and members:
            month_key = month_raw[:7]           # "2026-01"
            name      = members[0] if isinstance(members[0], str) else members[0].get("name", "")
            result[f"{name}|{month_key}"] = avail

    print(f"  Loaded {len(result)} capacity planning entries.")
    return result

# ─── METRICS BUILDER ───────────────────────────────────────────────────────────

def secs_to_hours(s): return round(s / 3600, 2)

def billability(billable_h, total_h):
    if total_h == 0:
        return None   # no data — don't show a fake 0%
    return round(billable_h / total_h, 4)

def build_period_metrics(worklogs: dict, capacity: dict, start: date, end: date, label: str):
    start_str = start.isoformat()
    end_str   = end.isoformat()
    months    = set()
    d = start.replace(day=1)
    while d <= end:
        months.add(d.strftime("%Y-%m"))
        if d.month == 12:
            d = d.replace(year=d.year+1, month=1)
        else:
            d = d.replace(month=d.month+1)

    # Working-days prorate factor per month
    def month_prorate(month_str: str) -> float:
        y, m = int(month_str[:4]), int(month_str[5:7])
        month_start = date(y, m, 1)
        if m == 12:
            month_end = date(y, 12, 31)
        else:
            month_end = date(y, m+1, 1) - timedelta(days=1)
        overlap_start = max(start, month_start)
        overlap_end   = min(end, month_end)
        if overlap_start > overlap_end:
            return 0.0
        wd_overlap = working_days(overlap_start, overlap_end)
        wd_total   = working_days(month_start, month_end)
        return wd_overlap / wd_total if wd_total > 0 else 0.0

    members_out = []
    team_totals = defaultdict(lambda: {"total_h": 0, "billable_h": 0, "planned_h": 0, "members": 0})

    for m in TEAM_MEMBERS:
        aid  = m["accountId"]
        wl   = worklogs.get(aid, {"total_seconds": 0, "billable_seconds": 0})
        total_h    = secs_to_hours(wl["total_seconds"])
        billable_h = secs_to_hours(wl["billable_seconds"])

        # Planned hours from Airtable (prorated if period spans partial months)
        planned_h = 0.0
        for month in months:
            avail = capacity.get(f"{m['name']}|{month}", 0) or 0
            planned_h += avail * month_prorate(month)
        planned_h = round(planned_h, 2)

        gap       = round(billable_h - planned_h, 2) if planned_h > 0 else None
        bill_pct  = billability(billable_h, total_h) if m["target"] > 0 else None

        entry = {
            "name":         m["name"],
            "team":         m["team"],
            "target":       m["target"],
            "total_h":      total_h,
            "billable_h":   billable_h,
            "planned_h":    planned_h,
            "gap_h":        gap,
            "billability":  bill_pct,
        }
        members_out.append(entry)

        # Aggregate by team
        t = team_totals[m["team"]]
        t["total_h"]    += total_h
        t["billable_h"] += billable_h
        t["planned_h"]  += planned_h
        if m["target"] > 0:
            t["members"] += 1

    # Team summaries
    teams_out = {}
    for team_name, t in team_totals.items():
        teams_out[team_name] = {
            "total_h":    round(t["total_h"], 2),
            "billable_h": round(t["billable_h"], 2),
            "planned_h":  round(t["planned_h"], 2),
            "gap_h":      round(t["billable_h"] - t["planned_h"], 2) if t["planned_h"] > 0 else None,
            "billability": billability(t["billable_h"], t["total_h"]),
        }

    # Overall totals
    total_h_all    = sum(m["total_h"] for m in members_out)
    billable_h_all = sum(m["billable_h"] for m in members_out)
    planned_h_all  = sum(m["planned_h"] for m in members_out)

    return {
        "label":        label,
        "start":        start_str,
        "end":          end_str,
        "total_h":      round(total_h_all, 2),
        "billable_h":   round(billable_h_all, 2),
        "planned_h":    round(planned_h_all, 2),
        "gap_h":        round(billable_h_all - planned_h_all, 2) if planned_h_all > 0 else None,
        "billability":  billability(billable_h_all, total_h_all),
        "members":      members_out,
        "teams":        teams_out,
    }

# ─── ENTRY POINT ───────────────────────────────────────────────────────────────

def main():
    today = date.today()

    week_start,  week_end  = week_bounds(today)
    month_start, month_end = month_bounds(today)
    ytd_start,   ytd_end   = ytd_bounds(today)

    # Fetch all worklogs YTD once (includes week + month data too)
    capacity = fetch_capacity_planning()

    # We fetch three separate windows to keep API queries bounded
    print("\n--- Fetching CURRENT WEEK ---")
    wl_week  = fetch_jira_worklogs(week_start.isoformat(),  week_end.isoformat())
    print("\n--- Fetching CURRENT MONTH ---")
    wl_month = fetch_jira_worklogs(month_start.isoformat(), month_end.isoformat())
    print("\n--- Fetching YTD ---")
    wl_ytd   = fetch_jira_worklogs(ytd_start.isoformat(),   ytd_end.isoformat())

    # Build metrics for each period
    week_data  = build_period_metrics(wl_week,  capacity, week_start,  week_end,  "week")
    month_data = build_period_metrics(wl_month, capacity, month_start, month_end, "month")
    ytd_data   = build_period_metrics(wl_ytd,   capacity, ytd_start,   ytd_end,   "ytd")

    # Weekly target breakdown per member (prorated from Airtable monthly available hours)
    # Plus the fixed team-wide weekly target
    week_data["team_weekly_target"] = WEEKLY_BILLABLE_TARGET

    output = {
        "lastUpdated":         datetime.utcnow().isoformat() + "Z",
        "generatedDate":       today.isoformat(),
        "weeklyBillableTarget": WEEKLY_BILLABLE_TARGET,
        "billabilityTarget":   BILLABILITY_TARGET,
        "week":                week_data,
        "month":               month_data,
        "ytd":                 ytd_data,
    }

    out_path = os.path.join(os.path.dirname(__file__), "..", "data.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅ data.json written to {out_path}")
    print(f"   Week billable:  {week_data['billable_h']}h / {WEEKLY_BILLABLE_TARGET}h target")
    print(f"   Month billable: {month_data['billable_h']}h")
    print(f"   YTD billable:   {ytd_data['billable_h']}h")

if __name__ == "__main__":
    main()
