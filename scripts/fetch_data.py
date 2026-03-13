#!/usr/bin/env python3
"""
SGA Billing Scorecard — Data Fetcher
Fetches worklog data from Jira. BT Plan from bt_plan.json (sourced from Robin's Google Sheet).
Run locally or via GitHub Actions every 30 minutes.

Requirements: pip install requests
"""

import os
import json
import time
import calendar
import requests
from datetime import datetime, date, timedelta
from collections import defaultdict

# ─── CONFIG ────────────────────────────────────────────────────────────────────

JIRA_BASE_URL  = "https://shopsys.atlassian.net"
JIRA_EMAIL     = os.environ["JIRA_EMAIL"]
JIRA_API_TOKEN = os.environ["JIRA_API_TOKEN"]

BILLABILITY_TARGET = 0.80
CURRENT_YEAR       = 2026

# ─── EMAIL → ACCOUNT ID ────────────────────────────────────────────────────────

EMAIL_TO_ACCOUNT_ID = {
    "kalla@soundsgood.agency":         "712020:75e42ba2-3da7-4ef9-8518-b2e43eb3f673",
    "ronalds@soundsgood.agency":       "712020:04ef5dbe-3670-4f4e-b631-535b48c9874c",
    "dan@soundsgood.agency":           "712020:1d8f40be-0dfb-4ccd-b939-6a46d6ce25a2",
    "michal.srnicek@shopsys.com":      "5b700762e72afd064c8a45aa",
    "marek.casnocha@shopsys.com":      "712020:a24c7e3c-0020-4dd0-ac06-1e6692faa6ed",
    "joao@soundsgood.agency":          "5cfa31b59943230e77ad44e0",
    "gomola@soundsgood.agency":        "712020:8279b16f-cb5c-4c1b-8811-ee3afafc9814",
    "adam@soundsgood.agency":          "712020:4488b5a7-b088-4a5a-9a78-7a94b2e82ffd",
    "miroslav.straka@shopsys.com":     "712020:d3e66acf-45af-44db-8ca4-c3f7bb6fcbd0",
    "ahsan@soundsgood.agency":         "712020:ebc61b18-0416-4683-b091-51ea2863e827",
    "patrik.bajer@shopsys.com":        "5e624b356e8fdd0cd8178257",
    "roman@soundsgood.agency":         "712020:ba534d73-c28c-4548-abef-bbfadc02b076",
    "frantisek@soundsgood.agency":     "712020:8501e5fc-65fc-4811-8963-1384b53baad9",
    "eva.martincova@shopsys.com":      "712020:35972894-7915-4f2c-beb7-5fb6d30e428b",
}

ACCOUNT_IDS = set(EMAIL_TO_ACCOUNT_ID.values())

# Display metadata per accountId (name, team, individual billability target)
ACCOUNT_META = {
    "712020:75e42ba2-3da7-4ef9-8518-b2e43eb3f673": {"name": "David Kalla",        "team": "Miro Team",   "target": 0.80},
    "712020:04ef5dbe-3670-4f4e-b631-535b48c9874c": {"name": "Ronalds Nordmanis",  "team": "Independent", "target": 0.80},
    "712020:1d8f40be-0dfb-4ccd-b939-6a46d6ce25a2": {"name": "Dan Dulgerian",      "team": "Miro Team",   "target": 0.85},
    "5b700762e72afd064c8a45aa":                     {"name": "Michal Srníček",     "team": "Patrik Team", "target": 0.80},
    "712020:a24c7e3c-0020-4dd0-ac06-1e6692faa6ed": {"name": "Marek Časnocha",     "team": "Independent", "target": 0.80},
    "5cfa31b59943230e77ad44e0":                     {"name": "João Antunes",       "team": "Independent", "target": 0.75},
    "712020:8279b16f-cb5c-4c1b-8811-ee3afafc9814": {"name": "David Gomola",       "team": "Miro Team",   "target": 0.85},
    "712020:4488b5a7-b088-4a5a-9a78-7a94b2e82ffd": {"name": "Adam Ďuriš",        "team": "Patrik Team", "target": 0.80},
    "712020:d3e66acf-45af-44db-8ca4-c3f7bb6fcbd0": {"name": "Miroslav Straka",    "team": "Miro Team",   "target": 0.80},
    "712020:ebc61b18-0416-4683-b091-51ea2863e827": {"name": "Ahsan Javed",        "team": "Apps Team",   "target": 0.60},
    "5e624b356e8fdd0cd8178257":                     {"name": "Patrik Bajer",       "team": "Patrik Team", "target": 0.80},
    "712020:ba534d73-c28c-4548-abef-bbfadc02b076": {"name": "Roman Filenko",      "team": "Patrik Team", "target": 0.80},
    "712020:8501e5fc-65fc-4811-8963-1384b53baad9": {"name": "František Seifried", "team": "Maintenance", "target": 0.80},
    "712020:35972894-7915-4f2c-beb7-5fb6d30e428b": {"name": "Eva Martincová",     "team": "Management",  "target": 0.0},
}

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
    first    = d.replace(day=1)
    last_day = calendar.monthrange(d.year, d.month)[1]
    return first, d.replace(day=last_day)

def ytd_bounds(d: date):
    return date(d.year, 1, 1), d

def working_days(start: date, end: date) -> int:
    count = 0
    cur   = start
    while cur <= end:
        if cur.weekday() < 5:
            count += 1
        cur += timedelta(days=1)
    return count

# ─── BT PLAN ───────────────────────────────────────────────────────────────────

def load_bt_plan() -> dict:
    """
    Load bt_plan.json. Returns {accountId: [jan_h, ..., dec_h]}.
    João's 0.70 multiplier is already baked into bt_plan.json.
    """
    path = os.path.join(os.path.dirname(__file__), "bt_plan.json")
    with open(path) as f:
        email_plan = json.load(f)
    return {
        EMAIL_TO_ACCOUNT_ID[email]: monthly_h
        for email, monthly_h in email_plan.items()
        if email in EMAIL_TO_ACCOUNT_ID
    }

def plan_for_period(account_plan: list, start: date, end: date) -> float:
    """
    Working-days-prorated plan hours for [start, end].
    account_plan: 12-element list (Jan-Dec) for CURRENT_YEAR.
    """
    total = 0.0
    d = start.replace(day=1)
    while d.year < end.year or (d.year == end.year and d.month <= end.month):
        if d.year == CURRENT_YEAR:
            last_day    = calendar.monthrange(d.year, d.month)[1]
            m_start     = d
            m_end       = d.replace(day=last_day)
            ov_start    = max(start, m_start)
            ov_end      = min(end,   m_end)
            if ov_start <= ov_end:
                wd_overlap = working_days(ov_start, ov_end)
                wd_total   = working_days(m_start,  m_end)
                if wd_total > 0:
                    total += account_plan[d.month - 1] * wd_overlap / wd_total
        # Advance to next month
        if d.month == 12:
            d = d.replace(year=d.year + 1, month=1)
        else:
            d = d.replace(month=d.month + 1)
    return round(total, 2)

def monthly_plan_total(account_plan: list, month: int) -> float:
    """Full (non-prorated) plan hours for a given month (1–12)."""
    return account_plan[month - 1]

# ─── JIRA HELPERS ──────────────────────────────────────────────────────────────

JIRA_AUTH = (JIRA_EMAIL, JIRA_API_TOKEN)

def jira_get(path: str, params: dict = None):
    url  = f"{JIRA_BASE_URL}{path}"
    resp = requests.get(url, auth=JIRA_AUTH, params=params)
    resp.raise_for_status()
    return resp.json()

def search_issues_with_worklogs(start_date: str, end_date: str) -> list:
    user_list = ",".join(f'"{uid}"' for uid in ACCOUNT_IDS)
    jql = (
        f'worklogAuthor in ({user_list}) '
        f'AND updated >= "{start_date}"'
    )
    issues  = []
    start_at = 0
    while True:
        data = jira_get("/rest/api/3/search", {
            "jql":        jql,
            "fields":     "id,key,project",
            "maxResults": 100,
            "startAt":    start_at,
        })
        issues.extend(data["issues"])
        print(f"  Fetched {len(issues)}/{data['total']} issues...")
        if start_at + 100 >= data["total"]:
            break
        start_at += 100
        time.sleep(0.2)
    return issues

def get_issue_worklogs(issue_key: str) -> list:
    result   = []
    start_at = 0
    while True:
        data = jira_get(f"/rest/api/3/issue/{issue_key}/worklog", {
            "startAt":    start_at,
            "maxResults": 100,
        })
        result.extend(data["worklogs"])
        if start_at + 100 >= data["total"]:
            break
        start_at += 100
    return result

# ─── MAIN FETCH ────────────────────────────────────────────────────────────────

def fetch_all_worklogs(start_date: str, end_date: str) -> list:
    """
    Fetch all team worklogs for [start_date, end_date].
    Returns flat list of {accountId, date, seconds, billable} dicts.
    This single fetch is then sliced in-memory for any sub-period.
    """
    print(f"\nFetching Jira worklogs {start_date} → {end_date}")
    issues = search_issues_with_worklogs(start_date, end_date)
    print(f"  Found {len(issues)} issues. Fetching worklog details...")

    entries = []
    for i, issue in enumerate(issues):
        project_id  = int(issue["fields"]["project"]["id"])
        is_billable = project_id in BILLABLE_PROJECT_IDS

        for wl in get_issue_worklogs(issue["key"]):
            wl_date   = wl["started"][:10]
            author_id = wl["author"]["accountId"]
            if not (start_date <= wl_date <= end_date):
                continue
            if author_id not in ACCOUNT_IDS:
                continue
            entries.append({
                "accountId": author_id,
                "date":      wl_date,
                "seconds":   wl["timeSpentSeconds"],
                "billable":  is_billable,
            })

        if (i + 1) % 20 == 0:
            print(f"  Processed {i+1}/{len(issues)} issues...")
            time.sleep(0.3)

    print(f"  Total worklog entries: {len(entries)}")
    return entries

# ─── AGGREGATION ───────────────────────────────────────────────────────────────

def aggregate(worklogs: list, start: date, end: date) -> dict:
    """
    Filter and sum worklogs for [start, end].
    Returns {accountId: {total_h, billable_h}}.
    """
    s = start.isoformat()
    e = end.isoformat()
    totals = defaultdict(lambda: {"total_s": 0, "billable_s": 0})
    for wl in worklogs:
        if s <= wl["date"] <= e:
            t = totals[wl["accountId"]]
            t["total_s"] += wl["seconds"]
            if wl["billable"]:
                t["billable_s"] += wl["seconds"]
    return {
        aid: {
            "total_h":    round(t["total_s"] / 3600, 2),
            "billable_h": round(t["billable_s"] / 3600, 2),
        }
        for aid, t in totals.items()
    }

# ─── METRICS BUILDER ───────────────────────────────────────────────────────────

def _billability(billable_h, total_h):
    return round(billable_h / total_h, 4) if total_h > 0 else None

def build_period_data(aggregated: dict, bt_plan: dict,
                      start: date, end: date, label: str,
                      planned_billable_h_override: float = None) -> dict:
    """
    Build the period output dict with per-member and per-team breakdowns.
    planned_billable_h_override: full monthly plan total (set for 'month' period).
    """
    members_out = []
    team_totals = defaultdict(lambda: {
        "total_h": 0.0, "billable_h": 0.0, "planned_h": 0.0, "has_plan": False
    })

    for aid, meta in ACCOUNT_META.items():
        wl         = aggregated.get(aid, {"total_h": 0.0, "billable_h": 0.0})
        total_h    = wl["total_h"]
        billable_h = wl["billable_h"]

        account_plan = bt_plan.get(aid)
        planned_h    = plan_for_period(account_plan, start, end) if account_plan else 0.0
        gap_h        = round(billable_h - planned_h, 2) if planned_h > 0 else None
        bill_pct     = _billability(billable_h, total_h) if meta["target"] > 0 else None

        members_out.append({
            "name":        meta["name"],
            "team":        meta["team"],
            "target":      meta["target"],
            "total_h":     total_h,
            "billable_h":  billable_h,
            "planned_h":   planned_h,
            "gap_h":       gap_h,
            "billability": bill_pct,
        })

        t = team_totals[meta["team"]]
        t["total_h"]    += total_h
        t["billable_h"] += billable_h
        t["planned_h"]  += planned_h
        if planned_h > 0:
            t["has_plan"] = True

    teams_out = {
        name: {
            "total_h":    round(t["total_h"],    2),
            "billable_h": round(t["billable_h"], 2),
            "planned_h":  round(t["planned_h"],  2),
            "gap_h":      round(t["billable_h"] - t["planned_h"], 2) if t["has_plan"] else None,
            "billability": _billability(t["billable_h"], t["total_h"]),
        }
        for name, t in team_totals.items()
    }

    total_h_all    = sum(m["total_h"]    for m in members_out)
    billable_h_all = sum(m["billable_h"] for m in members_out)
    planned_h_all  = sum(m["planned_h"]  for m in members_out)

    result = {
        "label":       label,
        "start":       start.isoformat(),
        "end":         end.isoformat(),
        "total_h":     round(total_h_all,    2),
        "billable_h":  round(billable_h_all, 2),
        "planned_h":   round(planned_h_all,  2),
        "gap_h":       round(billable_h_all - planned_h_all, 2) if planned_h_all > 0 else None,
        "billability": _billability(billable_h_all, total_h_all),
        "members":     members_out,
        "teams":       teams_out,
    }
    if planned_billable_h_override is not None:
        result["planned_billable_h"] = round(planned_billable_h_override, 2)
    return result

# ─── HISTORY BUILDERS ──────────────────────────────────────────────────────────

def build_monthly_history(all_worklogs: list, bt_plan: dict, today: date) -> list:
    """Monthly history from Jan CURRENT_YEAR through current (partial) month."""
    months = []
    d = date(CURRENT_YEAR, 1, 1)
    while d.year < today.year or (d.year == today.year and d.month <= today.month):
        last_day  = calendar.monthrange(d.year, d.month)[1]
        m_start   = d
        m_end     = min(d.replace(day=last_day), today)   # cap at today for current month

        aggregated = aggregate(all_worklogs, m_start, m_end)
        billable_h = round(sum(v["billable_h"] for v in aggregated.values()), 2)

        # Prorated plan (matches elapsed working days in month)
        planned_h = round(sum(
            plan_for_period(bp, m_start, m_end) for bp in bt_plan.values()
        ), 2)

        # Full-month plan (for reference / target display)
        planned_billable_h = round(sum(
            monthly_plan_total(bp, d.month) for bp in bt_plan.values()
        ), 2)

        gap_h = round(billable_h - planned_h, 2) if planned_h > 0 else None

        months.append({
            "label":              d.strftime("%b %Y"),
            "start":              m_start.isoformat(),
            "end":                m_end.isoformat(),
            "billable_h":         billable_h,
            "planned_h":          planned_h,
            "planned_billable_h": planned_billable_h,
            "gap_h":              gap_h,
        })

        if d.month == 12:
            d = d.replace(year=d.year + 1, month=1)
        else:
            d = d.replace(month=d.month + 1)
    return months


def build_weekly_history(all_worklogs: list, bt_plan: dict, today: date) -> list:
    """Weekly history from W1 of CURRENT_YEAR through current week."""
    weeks = []

    # First Monday of CURRENT_YEAR (may be in late December of previous year)
    jan1          = date(CURRENT_YEAR, 1, 1)
    first_monday  = jan1 - timedelta(days=jan1.weekday())
    if first_monday.year < CURRENT_YEAR:
        first_monday += timedelta(days=7)

    today_week_start = today - timedelta(days=today.weekday())
    d = first_monday

    while d <= today_week_start:
        w_start = d
        w_end   = min(d + timedelta(days=6), today)

        aggregated = aggregate(all_worklogs, w_start, w_end)
        billable_h = round(sum(v["billable_h"] for v in aggregated.values()), 2)

        planned_h = round(sum(
            plan_for_period(bp, w_start, w_end) for bp in bt_plan.values()
        ), 2)

        gap_h = round(billable_h - planned_h, 2) if planned_h > 0 else None

        iso_year, iso_week, _ = w_start.isocalendar()
        weeks.append({
            "label":      f"W{iso_week:02d} {iso_year}",
            "start":      w_start.isoformat(),
            "end":        w_end.isoformat(),
            "billable_h": billable_h,
            "planned_h":  planned_h,
            "gap_h":      gap_h,
        })
        d += timedelta(days=7)

    return weeks

# ─── ENTRY POINT ───────────────────────────────────────────────────────────────

def main():
    today = date.today()

    week_start,  week_end  = week_bounds(today)
    month_start, month_end = month_bounds(today)
    ytd_start,   ytd_end   = ytd_bounds(today)

    # Load BT plan from file
    print("Loading BT plan from bt_plan.json...")
    bt_plan = load_bt_plan()
    print(f"  Loaded plan for {len(bt_plan)} team members.")

    # Single YTD fetch — covers week, month, and all history sub-periods
    all_worklogs = fetch_all_worklogs(ytd_start.isoformat(), ytd_end.isoformat())

    # In-memory aggregation for each period
    print("\nAggregating periods...")
    wl_week  = aggregate(all_worklogs, week_start,  min(week_end,  today))
    wl_month = aggregate(all_worklogs, month_start, today)
    wl_ytd   = aggregate(all_worklogs, ytd_start,   today)

    # Full monthly plan total (used as planned_billable_h on month card)
    month_full_plan = sum(monthly_plan_total(bp, today.month) for bp in bt_plan.values())

    # Build period metrics
    week_data  = build_period_data(wl_week,  bt_plan, week_start,  min(week_end, today), "week")
    month_data = build_period_data(wl_month, bt_plan, month_start, today,                "month",
                                   planned_billable_h_override=month_full_plan)
    ytd_data   = build_period_data(wl_ytd,   bt_plan, ytd_start,   today,                "ytd")

    # Build history
    print("\nBuilding history...")
    monthly_history = build_monthly_history(all_worklogs, bt_plan, today)
    weekly_history  = build_weekly_history(all_worklogs,  bt_plan, today)

    output = {
        "lastUpdated":       datetime.utcnow().isoformat() + "Z",
        "generatedDate":     today.isoformat(),
        "billabilityTarget": BILLABILITY_TARGET,
        "week":              week_data,
        "month":             month_data,
        "ytd":               ytd_data,
        "history": {
            "months": monthly_history,
            "weeks":  weekly_history,
        },
    }

    out_path = os.path.join(os.path.dirname(__file__), "..", "data.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅  data.json written")
    print(f"    Week:   {week_data['billable_h']}h billed / {week_data['planned_h']}h planned  gap {week_data['gap_h']}h")
    print(f"    Month:  {month_data['billable_h']}h billed / {month_data['planned_h']}h planned  gap {month_data['gap_h']}h")
    print(f"    YTD:    {ytd_data['billable_h']}h billed / {ytd_data['planned_h']}h planned  gap {ytd_data['gap_h']}h")
    print(f"    History: {len(monthly_history)} months, {len(weekly_history)} weeks")


if __name__ == "__main__":
    main()
