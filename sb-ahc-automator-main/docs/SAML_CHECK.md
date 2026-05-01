# SAML Check

## Purpose

Checks SAML configuration and activation status.

## APIs Used

| Method | Endpoint |
|--------|----------|
| REST | `GET /api/v1/company/saml` |

## Logic

- **Configured:** If response has `configured`, `isConfigured`, `idpMetadata`, or `metadata`.
- **Activated:** If `isActivated` or `activated` is true.

## Output

```json
{
  "saml": {
    "configured": true,
    "activated": true
  }
}
```

## Config

- `REST_API_HOST_BY_REGION` — Base URL per region.

## Fine-tuning

- Field names for 404/empty response handling.
