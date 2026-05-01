# Noisy Alerts Check (Metrics API)

## Purpose

Gets Top 10 security alerts by trigger count in the last 24 hours. Uses Coralogix Metrics API (PromQL) with the `cx_alerts` metric.

## Type

Standalone REST check — uses Metrics API. **Requires CX Alerts Metrics to be enabled.**

## Region-Specific Endpoints

Uses central `modules/region_config.py`. Metrics API URL: `https://{get_api_host(region)}/metrics/api/v1/query`

See [REGION_CONFIG.md](REGION_CONFIG.md) for full region mapping.

## PromQL Query

```promql
topk(10, sort_desc(sum(sum_over_time(cx_alerts{
  alert_def_label_alert_type="security",
  alert_def_name!~"building block",
  alert_def_name!~"null"
}[24h])) by (alert_def_name, alert_def_priority)))
```

## Output

```json
{
  "noisy_alerts": {
    "noisy_alerts": [
      {
        "rank": 1,
        "alert_name": "Gemini GCP - Identity User Agent Pivot Detection",
        "incident_count": 705,
        "priority": "P5"
      }
    ],
    "time_range": "Last 24 hours",
    "total_count": 1234
  }
}
```

## Config

- `cx_api_key` — API key with Metrics/Data Querying permission.
- `cx_region` — Region code (eu1, us1, etc.) for endpoint selection.

## Fine-tuning

- **Query** — Edit `NOISY_ALERTS_QUERY` in `noisy_alerts_check.py` to change filters (e.g. `alert_def_label_alert_type`, exclusions).
- **Top N** — Change `topk(10, ...)` to `topk(20, ...)` etc.
- **Time window** — Change `[24h]` to `[48h]` etc.
