# Limits Check

## Purpose

Reports mapping stats and resource quotas (alerts, enrichments, parsing rules, events2metrics).

## APIs Used

| Method | Endpoint / Service |
|--------|--------------------|
| REST | `POST /api/v1/statistics/mapping` |
| grpcurl | `QuotaService/GetQuotas` |

## Logic

- **Mapping:** `mappingCount`, `mappingLimit`, `mappingErrorCount` from REST.
- **Quotas:** `alert`, `enrichment`, `parsingRule`, `events2Metrics` from gRPC.

## Output

```json
{
  "limits": {
    "ingested_fields_today": 150,
    "mapping_exceptions": 0,
    "alerts": { "used": 1187, "limit": 1200 },
    "enrichments": { "used": 42, "limit": 50 },
    "parsing_rules": { "used": 208, "limit": 250 },
    "events2metrics_labels_limit": 100
  }
}
```

## Config

- `REST_API_HOST_BY_REGION` — Base URL for mapping stats.

## Fine-tuning

- Quota keys if gRPC response structure changes.
