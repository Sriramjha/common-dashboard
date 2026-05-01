# Data Usage Metrics Check

## Purpose

Checks if Data Usage metrics toggle is enabled (sends data usage to metrics).

## APIs Used

| Method | Endpoint |
|--------|----------|
| REST (primary) | `GET /api/v1/statistics` |
| REST (fallback) | `GET /api/v1/company` |

## Logic

- Checks `statisticsEnabled`, `metricsEnabled`, or `data_usage_to_metrics_enabled` in response.

## Output

```json
{
  "data_usage_metrics": "enabled"
}
```

Or `"disabled"`.

## Config

- `REST_API_HOST_BY_REGION` — Base URL per region.

## Fine-tuning

- Key names for primary vs fallback responses.
