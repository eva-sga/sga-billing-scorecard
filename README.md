# SGA Billing Scorecard

Live billing dashboard that auto-refreshes every 30 minutes by pulling data from Jira and Airtable.

---

## One-time Setup (15 minutes)

### Step 1 — Create a private GitHub repo

1. Go to [github.com/new](https://github.com/new)
2. Name it `sga-billing-scorecard`
3. Set to **Private**
4. **Do not** tick "Add README" — leave everything blank
5. Click **Create repository**

### Step 2 — Upload these files

On the next screen, GitHub will show a "Quick setup" page.

1. Click **"uploading an existing file"**
2. Drag the entire `billing-scorecard` folder contents into the upload area
3. Make sure the folder structure looks like this:
   ```
   .github/workflows/update-data.yml
   scripts/fetch_data.py
   index.html
   data.json
   README.md
   ```
4. Click **Commit changes**

### Step 3 — Add your API secrets

Go to your repo → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

Add these three secrets:

| Secret name      | Where to find it |
|------------------|-----------------|
| `JIRA_EMAIL`     | Your Atlassian login email: `eva@soundsgood.agency` |
| `JIRA_API_TOKEN` | Go to [id.atlassian.com/manage-profile/security/api-tokens](https://id.atlassian.com/manage-profile/security/api-tokens) → **Create API token** → copy the value |
| `AIRTABLE_TOKEN` | Go to [airtable.com/create/tokens](https://airtable.com/create/tokens) → **Create new token** → scope: `data.records:read` on the SGA CRM base → copy the value |

### Step 4 — Enable GitHub Pages

Go to your repo → **Settings** → **Pages** → **Source**: Deploy from branch `main`, folder `/ (root)` → **Save**

After a minute, GitHub will give you a live URL like:
```
https://YOUR-USERNAME.github.io/sga-billing-scorecard/
```

### Step 5 — Run the first data fetch

Go to **Actions** → **Refresh Billing Data** → **Run workflow** → **Run workflow**

This populates `data.json` for the first time. After that it runs automatically every 30 minutes.

---

## How it works

```
Every 30 min:
  GitHub Actions → fetch_data.py → Jira API + Airtable API → data.json → committed to repo
  Your browser → loads index.html + data.json from GitHub Pages
```

- **All logged time**: all Jira worklogs by the 16 Shopify People team members
- **Billable time**: same worklogs, filtered to the ~150 billable Jira projects
- **Planned hours**: pulled from the Capacity Planning table in Airtable (Available Hours field)
- **Weekly target**: 356 billable hours (team-wide fixed target)

---

## Updating the team or targets

Edit `scripts/fetch_data.py`:

- **Add/remove team members**: update the `TEAM_MEMBERS` list
- **Change weekly target**: update `WEEKLY_BILLABLE_TARGET`
- **Change billability target**: update `BILLABILITY_TARGET`
- **Add/remove billable projects**: update `BILLABLE_PROJECT_IDS`

After editing, commit the file — the next Action run will use the new config.

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| Dashboard shows "Could not load billing data" | Run the GitHub Action manually first (Step 5) |
| Action fails with 401 error | Check your Jira/Airtable secrets are correct |
| Data looks stale | Check Actions tab — look for any failed runs |
| Numbers look wrong | The Jira worklog API can be slow — give it a full run |
