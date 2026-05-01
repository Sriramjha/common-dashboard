# Cora AI Check

## Purpose

Reads AI toggle settings (DataPrime query assistance, Explain Log, Knowledge assistance).

## APIs Used

| Method | Service |
|--------|---------|
| grpcurl | `GenAiService/Settings` |

## Logic

- Reads boolean flags from the response.

## Output

```json
{
  "cora_ai": {
    "dataprime_query_assistance_enabled": true,
    "explain_log_enabled": true,
    "knowledge_assistance_enabled": true
  }
}
```

## Fine-tuning

- Map API keys: `queryAssistantEnabled`, `explainLogEnabled`, `platformAwarenessEnabled` to output fields.
