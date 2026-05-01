# CSPM Integration Check

## Purpose

Detects CSPM (Cloud Security Posture Management) integration via DataPrime query.

## APIs Used

| Method | Endpoint |
|--------|----------|
| REST | `POST /api/v1/dataprime/query` |

## Logic

- Runs query: `source logs | filter $d.snowbitData=='v2' | countby snowbit.provider, snowbit.additionalData.account`.
- Integrated if results > 0.
- Extracts providers and account IDs from results.

## Output

```json
{
  "cspm": {
    "integrated": true,
    "total_accounts": 5,
    "providers": [
      { "provider": "AWS", "count": 3, "accounts": ["123", "456"] }
    ],
    "accounts": ["123", "456", "789"]
  }
}
```

## Config

- `cx_api_key` — DataPrime API key.
- `CSPM_QUERY` — Custom query override.
- `API_HOST_BY_REGION` — REST host per region.
- `lookback_hours` — Default 24h.

## Fine-tuning

- `CSPM_QUERY` — Adjust filter/groupby for different CSPM data formats.
- `_extract_provider_account` — Parsing logic for provider/account from results.
