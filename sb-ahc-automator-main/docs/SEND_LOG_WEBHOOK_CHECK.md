# Send Log Webhook Check

## Purpose

Checks if at least one Send Log webhook exists.

## APIs Used

| Method | Service |
|--------|---------|
| gRPC | `OutgoingWebhooksServiceStub.list_outgoing_webhook_types` |

## Logic

- Pass if `type == 4` or `SEND_LOG` or `label == "Send log"` and `count > 0`.

## Output

```json
{
  "send_log_webhook_created": true
}
```

## Fine-tuning

- Type enum mapping if webhook types change.
