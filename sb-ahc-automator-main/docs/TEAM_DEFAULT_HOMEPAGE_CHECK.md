# Team Default Homepage Check

## Purpose

Gets the team default landing page and whether it is a custom dashboard.

## APIs Used

| Method | Service |
|--------|---------|
| grpcurl | `LandingPageService/GetLandingPage` |

## Logic

- `is_custom_dashboard` is true if value contains `CUSTOM_DASHBOARD`.

## Output

```json
{
  "team_default_homepage": {
    "value": "CUSTOM_DASHBOARD",
    "is_custom_dashboard": true
  }
}
```

## Fine-tuning

- Adjust parsing for `predefinedLandingPage` / `customDashboardId` if API format changes.
