# Alerts Status Check — Disabled & Never Triggered Alerts

## Overview

Reports two categories:
- **Disabled Alerts** — Alert definitions with `enabled: false`
- **Never Triggered Alerts (last 30 days)** — Alert definitions that had no incident in the last 30 days

## APIs Used

| API | Purpose |
|-----|---------|
| **Alert Definitions** | GET `/alerts/alerts-general/v3` — All alert names, enabled status |
| **Incident Aggregations** | GET `/incidents/incidents/v1/aggregations` — Try first (matches UI) |
| **List Incidents** | POST `/incidents/incidents/v1` — Fallback when aggregations returns ungrouped data |

## Never Triggered Logic

1. **Try ListIncidentAggregations** with `$.filter.startTime`, `$.filter.endTime`, `pagination.pageSize=1000`, `$.groupBys[0].contextualLabel=alert_name`
2. **Fallback:** List Incidents → filter "open during window" + "created in last 210 days" → extract `contextualLabels.alert_name`
3. **Never triggered** = Alert definitions − incident alert names

See [INCIDENTS_API_FILTER_OPTIONS.md](INCIDENTS_API_FILTER_OPTIONS.md) for details.

## Output

- `disabled_alerts` — List of disabled alert names
- `never_triggered_alerts` — List of alert names with no incident in last 30 days
- PDF section: "Disabled Alerts & Never Triggered Alerts" with tables (max 10 shown, "+X more")
