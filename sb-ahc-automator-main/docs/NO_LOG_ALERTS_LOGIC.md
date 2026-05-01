# No-Log Alerts Check — Logic & Fine-Tuning Guide

The no-log alerts check is **not prompt-based**. It uses direct MCP tool calls and programmatic logic. The implementation follows the specification below.

---

## Specification (Logic)

1. **List all alert definitions** whose name indicates "no log" coverage
   - Use name filters: `["no log", "no logs", "no-log", "no_log"]` (case-insensitive)
   - Consider only ENABLED alerts

2. **Determine active apps** — logs from last 24h, group by application name, collect apps with at least one log

3. **Extract app filters** per enabled no-log alert from `application_name` in the raw config
   - If alert has app filter and/or Lucene query → scoped to those apps
   - If any enabled alert has NO app filter AND NO Lucene query → all apps covered, return empty

4. **Coverage rules**
   - `excluded_apps = {"coralogix-alerts", "cx-metrics"}` plus internal apps
   - Fuzzy matching: normalize (lowercase, remove spaces/hyphens/underscores); covered if exact match or either normalized name is substring of the other
   - Uncovered = active + not excluded + not matching any enabled alert

**Output format:** One app name per line, or `ALL_APPS_COVERED` if every active app is covered.

---

## Overview

**Goal:** Identify active applications that have **no** enabled "no log" alert covering them.

**Output:** `apps_without_coverage` — list of app names that sent logs in the last 24h but are not covered by any enabled no-log alert.

**Plain-text format:** `plain_text_output` — one app name per line, or `ALL_APPS_COVERED` if every active app is covered.

---

## Flow (4 steps)

### Step 1: List no-log alert definitions

**MCP tool:** `list_alert_definitions`

**Arguments:**
```python
{
    "page_size": 50,
    "alert_property_filters": {"nameFilters": ["no log", "no logs", "no-log", "no_log"]},
}
```

**Fine-tuning:**
- **`nameFilters`** — Case-insensitive. Add/remove terms if your alerts use different names.
- **Location:** `mcp_checks_check.py` ~line 258.

---

### Step 2: Fetch active apps (last 24h)

**MCP tool:** `get_logs`

**Arguments:**
```python
{
    "name": "get_logs",
    "arguments": {
        "query": "source logs | groupby $l.applicationname count() as cnt",
        "start_date": "<24h ago>",
        "end_date": "<now>",
        "limit": 500,
    },
}
```

**Fine-tuning:**
- **Time window** — Currently fixed to 24h. Change in `_fetch_active_apps` (lines 186–187).
- **Limit** — 500 apps max. Increase if you have more apps.
- **Query** — Uses `$l.applicationname`; adjust if your schema differs.

---

### Step 3: Get app filter per enabled alert

For each **enabled** no-log alert, the code calls:

**MCP tool:** `get_alert_definition`

**Arguments:**
```python
{"alert_version_id": "<version_id>"}
```

**Extraction:** Regex parses `application_name` filters from the raw response:

```python
r'application_name\s*\{[^}]*value\s*\{\s*value:\s*"([^"]+)"'
```

**Fine-tuning:**
- If the API response format changes, update this regex in `_fetch_alert_app_names` (lines 244–246).
- **Lucene scope:** If `lucene_query {` is present, the alert is treated as having a Lucene scope (not "covers all apps").

---

### Step 4: Coverage logic

**Covered** = app is in at least one enabled alert’s app filter (exact or fuzzy match).

**Excluded apps** (never reported as uncovered):
```python
excluded_apps = {"coralogix-alerts", "cx-metrics"}
```

**Fuzzy matching** (`_normalize_for_match`):
- Normalize: remove spaces, hyphens, underscores; lowercase.
- Example: `aws-network-firewall` matches `AWS Network Firewall`.

**`_app_is_covered` logic:**
- Exact match: `active_app in covered_set`
- Fuzzy: `norm_cov in norm_active` or `norm_active in norm_cov`

**Special case:** If any enabled alert has **no** app filter and **no** Lucene query → `all_apps_covered = True` → no apps reported as uncovered.

---

## Config (ahc_runner.py)

```python
"no_log_alerts": {
    "triggered_lookback_days": 7,  # Used for "triggered in last 7d" classification
}
```

---

## Fine-tuning checklist

| What to change | Where | Notes |
|----------------|-------|-------|
| Alert name filters | `mcp_checks_check.py` ~258 | `nameFilters: ["no log", "no logs", "no-log", "no_log"]` |
| Excluded apps | `mcp_checks_check.py` ~325 | Add system/internal apps |
| Active apps time window | `_fetch_active_apps` ~186 | Default 24h |
| Active apps limit | `_fetch_active_apps` ~197 | Default 500 |
| App filter regex | `_fetch_alert_app_names` ~244 | If API format changes |
| Fuzzy match logic | `_app_is_covered` ~331–340 | Substring / containment rules |

---

## Common issues

1. **Too many apps without coverage** — Alerts may use different app names; fuzzy matching may need to be looser or you may need to add more `nameFilters` to find all no-log alerts.
2. **Missing alerts** — `nameFilters` may not match your alert naming (e.g. "No Logs", "NoLog").
3. **False positives** — Some apps may be internal; add them to `excluded_apps`.
4. **Regex not matching** — If `get_alert_definition` response format changes, the `application_name` regex may fail; inspect raw response and adjust.
5. **Enabled extraction misalignment** — The MCP raw response can have `names` and `enabled` in different document order. When `enabled_names` count is less than total alerts, the code falls back to processing all alerts so results match MCP direct tool output.
