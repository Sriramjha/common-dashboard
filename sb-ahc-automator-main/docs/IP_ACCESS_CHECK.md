# IP Access Control Check

## Purpose

Checks if IP allow list is enabled for the company.

## APIs Used

| Method | Endpoint |
|--------|----------|
| REST | `GET /api/v1/company` |

## Logic

- Pass if `settings.ip_allow_list_enabled` is true.

## Output

```json
{
  "ip_access": {
    "enabled": true
  }
}
```

## Config

- `REST_API_HOST_BY_REGION` — Base URL per region.

## Fine-tuning

- Path to `ip_allow_list_enabled` if API structure changes.
