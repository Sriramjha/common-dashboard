# Data Usage Check

## Purpose

Gets daily quota and average daily usage for the team.

## APIs Used

| Method | Service |
|--------|---------|
| grpcurl | `DataUsageService/GetTeamsQuota` |
| grpcurl | `DataUsageService/GetTeamsDailyUsage` |

## Logic

- Quota from `teamsQuota[].quota.value`.
- Usage from `metrics` for yesterday's date.

## Output

```json
{
  "data_usage": {
    "daily_quota": 1000000,
    "avg_daily_units": 450000.5
  }
}
```

## Fine-tuning

- Date range for usage calculation.
- Team matching if multiple teams.
