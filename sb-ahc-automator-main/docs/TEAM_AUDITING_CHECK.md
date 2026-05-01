# Team Auditing Check

## Purpose

Checks if team auditing is configured.

## APIs Used

| Method | Endpoint |
|--------|----------|
| REST | `GET /api/v1/auditing/configured` |

## Logic

- Configured if response has `id`, `name`, or `team_url`.

## Output

```json
{
  "team_auditing": {
    "configured": true,
    "audit_team_name": "...",
    "audit_team_id": "..."
  }
}
```

## Config

- `REST_API_HOST_BY_REGION` — Base URL per region.
- `Cgx-Team-Id` header — Team ID for the request.

## Fine-tuning

- Region mapping in `REST_API_HOST_BY_REGION`.
- Handle empty/204 responses.
