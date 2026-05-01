# MFA Check

## Purpose

Checks if MFA (multi-factor authentication) is enforced for the team.

## APIs Used

| Method | Service / Endpoint |
|--------|--------------------|
| grpcurl | `TeamService/GetTeamInfo` |
| REST (fallback) | `GET /api/v1/company` |

## Logic

- Looks for `mfaEnforced`, `mfa_enforced`, etc. in gRPC or REST response.
- Uses REST fallback if gRPC fails.

## Output

```json
{
  "mfa": {
    "enforced": true
  }
}
```

## Config

- `REST_API_HOST_BY_REGION` — Base URL for fallback.

## Fine-tuning

- `mfaEnforced`, `mfa_enabled` key names if API changes.
