# Ingestion Block Alert Check (MCP)

## Purpose

Checks if a "Data Ingestion Block" alert is created and active.

## Type

MCP check — uses `list_alert_definitions` tool. Not prompt-based.

## Logic

1. Calls `list_alert_definitions` with `nameFilters: ["ingestion"]`.
2. Filters results for alerts whose name contains both "ingestion" and ("block" or "blocked").
3. Status:
   - **alert_exists + alert_active** — Alert is created and enabled.
   - **alert_exists + !alert_active** — Alert exists but is disabled.
   - **!alert_exists** — No matching alert found (ACTION REQUIRED).

## MCP Tool

**Tool:** `list_alert_definitions`

**Arguments:**
```python
{
  "alert_property_filters": {"nameFilters": ["ingestion"]},
  "page_size": 100
}
```

## Output

```json
{
  "ingestion_block_alert": {
    "alert_exists": true,
    "alert_active": true,
    "alerts": [
      {
        "name": "Data Ingestion Block",
        "enabled": true,
        "priority": "high",
        "last_triggered": "2026-03-15 10:00 UTC"
      }
    ],
    "summary": "Data ingestion block alert is created and active"
  }
}
```

## Fine-tuning

- **`nameFilters`** — Add terms if your alert uses different naming (e.g. "Ingestion Blocked", "Block Ingestion").
- **Name filter logic** — In `mcp_checks_check.py` ~406: `"ingestion" in name_lower and ("block" in name_lower or "blocked" in name_lower)`.
