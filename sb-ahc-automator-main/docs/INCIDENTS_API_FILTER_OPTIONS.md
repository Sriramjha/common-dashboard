# Incidents API — Filter Options & Never-Triggered Logic

Reference: [Coralogix Incidents Service Overview](https://docs.coralogix.com/api-reference/latest/incidents-service/overview)

## Available APIs

| API | Method | Purpose |
|-----|--------|---------|
| **List Incidents** | POST `/incidents/incidents/v1` | Fetch incidents (supports filter in body) |
| **List Incident Aggregations** | GET `/incidents/incidents/v1/aggregations` | Group by alert/field (e.g. "Group by Alert Definition") |

## IncidentQueryFilter Options (List Incidents)

From the [API reference](https://docs.coralogix.com/api-reference/latest/incidents-service/overview):

| Filter Field | Type | Description |
|--------------|------|-------------|
| `createdAtRange` | TimeRange | Filter incidents **created** in time range |
| `incidentDurationRange` | TimeRange | Filter incidents **open (alive)** in time range |
| `startTime` / `endTime` | string (date-time) | Deprecated, use `incident_open_range` |
| `applicationName` | array of string | Filter by application names |
| `subsystemName` | array of string | Filter by subsystem names |
| `assignee` | array of string | Filter by assignee user IDs |
| `state` | IncidentState[] | TRIGGERED, RESOLVED |
| `status` | IncidentStatus[] | TRIGGERED, ACKNOWLEDGED, RESOLVED |
| `severity` | IncidentSeverity[] | INFO, WARNING, ERROR, CRITICAL, LOW |
| `contextualLabels` | object | Filter by contextual label key/value |
| `displayLabels` | object | Filter by display labels |
| `isMuted` | boolean | Muted incidents only |
| `metaLabels` | array | Filter by meta labels |
| `searchQuery` | IncidentSearchQuery | Search by alert name, etc. |

## List Incident Aggregations (Group By)

- **group_bys**: `GroupByIncidentField` (e.g. `incidentField: INCIDENTS_FIELDS_NAME`) or `GroupByContextualLabel` (e.g. `contextualLabel: "alert_name"`)
- **filter**: Same IncidentQueryFilter as above
- **pagination**: pageSize, pageToken

**Note:** The aggregations endpoint accepts flat query params (`$.filter.startTime`, `$.filter.endTime`) but the REST gateway returns ungrouped data (empty `groupBysValue`). The UI uses gRPC directly. We try aggregations first; if no grouped data, we fall back to List Incidents.

## Current Implementation (Never Triggered)

1. **Try ListIncidentAggregations** (GET `/incidents/incidents/v1/aggregations`)
   - Params: `$.filter.startTime`, `$.filter.endTime`, `pagination.pageSize=1000`, `$.groupBys[0].contextualLabel=alert_name`
   - If response has grouped data (`groupBysValue` populated), extract unique alert names
   - Currently returns ungrouped data → fallback

2. **Fallback: List Incidents** (POST `/incidents/incidents/v1`)
   - Fetch all incidents (paginate until `total_fetched >= totalSize`)
   - Filter client-side: incident **open during** [start, end] AND **created** in last 210 days
   - Open during: `created_at <= end` AND (`closed_at` is null OR `closed_at >= start`)
   - Extract `contextualLabels.alert_name` → unique alert names
   - Yields ~214–218 unique alerts, matching UI "Group by Alert Definition"

3. **Never triggered** = Alert definitions − incident alert names
