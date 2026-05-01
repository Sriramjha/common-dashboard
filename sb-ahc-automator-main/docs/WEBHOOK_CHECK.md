# Webhook Check

## Purpose

Lists outbound webhook types and their connection counts.

## APIs Used

| Method | Service |
|--------|---------|
| gRPC | `OutgoingWebhooksServiceStub.list_outgoing_webhook_types` |

## Logic

- Sums `count` per webhook type.
- Renames `count` to `connections_count` in output.

## Output

```json
{
  "outbound_webhooks": {
    "amount": 3,
    "details": [
      { "label": "Send log", "connections_count": 2 },
      { "label": "Slack", "connections_count": 1 }
    ]
  }
}
```

## Fine-tuning

- Future: alert–webhook mapping via `ListAlertDefs`.
