# Suppression Rules Check

## Purpose

Checks if suppression (alert scheduler) rules are used.

## APIs Used

| Method | Service / Endpoint |
|--------|--------------------|
| grpcurl | `AlertSchedulerRuleService/GetBulkAlertSchedulerRule` |
| REST (fallback) | `GET /api/v1/alert-scheduler-rules` |

## Logic

- `used` if count > 0; `not_used` otherwise.

## Output

```json
{
  "suppression_rules": "used"
}
```

Or `"not_used"`.

## Config

- `REST_API_HOST_BY_REGION` — Base URL for fallback.

## Fine-tuning

- `alertSchedulerRules` extraction from REST response.
