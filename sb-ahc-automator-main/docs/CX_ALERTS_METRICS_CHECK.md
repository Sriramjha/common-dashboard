# CX Alerts Metrics Check

## Purpose

Checks if alerts are aggregated into metrics (alerts auto-send to metrics).

## APIs Used

| Method | Endpoint |
|--------|----------|
| REST | `GET /api/v1/company` |

## Logic

- Pass if `settings.alerts_auto_send_metrics_enabled` is true.

## Output

```json
{
  "cx_alerts_metrics": {
    "enabled": true
  }
}
```

## Config

- `REST_API_HOST_BY_REGION` — Base URL per region.

## Fine-tuning

- Adjust path to `alerts_auto_send_metrics_enabled` if API structure changes.
