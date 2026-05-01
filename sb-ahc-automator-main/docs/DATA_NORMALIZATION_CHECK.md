# Data Normalization Check

## Purpose

Checks if logs have `cx_security` field by application/subsystem (data normalisation status).

## APIs Used

| Method | Endpoint |
|--------|----------|
| REST | `POST /api/v1/dataprime/query` |

## Logic

- Runs query to group by application, subsystem and count missing vs total.
- Excludes `cx-metrics`, `coralogix-alerts` from concern list.

## Output

```json
{
  "data_normalization": {
    "concern_count": 5,
    "concern_rows": [
      { "application": "app1", "subsystem": "sub1" }
    ],
    "all_normalized": false,
    "summary": "..."
  }
}
```

Query returns only application and subsystem for app/subsystems where 100% of logs are missing cx_security.

## Config

- `cx_api_key` — DataPrime API key.
- `NORMALIZATION_QUERY` — Custom query override.
- `EXCLUDED_APPS` — Apps to exclude from concerns.

## Fine-tuning

- `NORMALIZATION_QUERY` — Adjust groupby, filters.
- `EXCLUDED_APPS` — Add internal/system apps.
- Lookback window.
