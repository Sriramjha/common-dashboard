# TCO Distribution Check

## Purpose

Gets TCO (Total Cost of Ownership) priority distribution percentages for today (high, medium, low, blocked).

## APIs Used

| Method | Service |
|--------|---------|
| grpcurl | `DataUsageService/GetTeamsDailyUsage` |

## Logic

- Sums `logsQuota`, `metricsQuota`, `tracesQuota`, `sessionRecordingQuota` by priority.
- Computes percentages for high, medium, low, blocked.

## Output

```json
{
  "tco_distribution": {
    "high_pct": 45.2,
    "medium_pct": 30.1,
    "low_pct": 24.5,
    "blocked_pct": 0.2
  }
}
```

## Fine-tuning

- Quota keys if API structure changes.
- Date selection (default: today).
