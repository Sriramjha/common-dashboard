# Team URL Check

## Purpose

Resolves team URL from team list for report links.

## APIs Used

| Method | Endpoint |
|--------|----------|
| REST | `GET /api/v1/user/team` |

## Logic

- Matches team by `company_id`.
- Builds URL from `team_url` / `teamUrl` / `name` + domain.

## Output

```json
{
  "team_url": "https://ng-api-grpc.eu1.coralogix.com/..."
}
```

## Config

- `REST_API_HOST_BY_REGION` — Base URL per region.
- `TEAM_DOMAIN_BY_REGION` — Domain suffixes per region.

## Fine-tuning

- Region mapping.
- Domain suffixes for different environments.
