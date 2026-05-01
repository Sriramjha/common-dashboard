# Key Fields Normalized Check (MCP)

## Purpose

Checks if data sources have normalized fields (`cx_security`) in Coralogix. Runs via MCP `get_logs` with DataPrime queries.

## Type

MCP check — uses `get_logs` tool. Not in default `DEFAULT_MCP_CHECKS`; add to `mcp_checks` config to enable.

## Logic

1. Query 1: `source logs | lucene '_exists_:cx_security' | groupby $l.applicationname:string.toLowerCase() as app_name aggregate count() as normalized_count`
2. Query 2: `source logs | groupby $l.applicationname:string.toLowerCase() as app_name aggregate count() as total_count`
3. App is **normalized** if `normalized_count > 0`; **not normalized** if 0.

## Queries (hardcoded)

```python
q_normalized = "source logs | lucene '_exists_:cx_security' | groupby $l.applicationname:string.toLowerCase() as app_name aggregate count() as normalized_count | orderby normalized_count desc"
q_total      = "source logs | groupby $l.applicationname:string.toLowerCase() as app_name aggregate count() as total_count | orderby total_count desc"
```

## Output

```json
{
  "all_normalized": false,
  "total_apps": 65,
  "fully_normalized_apps": 57,
  "not_normalized_apps": 8,
  "normalized": [
    { "application": "app1", "normalized_count": 1000, "total_count": 1000, "pct": "100%" }
  ],
  "not_normalized": [
    { "application": "app2", "total_count": 500 }
  ],
  "summary": "8 data source(s) are NOT normalized and require attention"
}
```

## Config

Add to `mcp_checks`:

```python
{
  "name": "key_fields_normalized",
  "output_key": "data_normalization",  # or custom key
  "type": "key_fields_normalized",
  "lookback_hours": 24
}
```

## Fine-tuning

- **`lookback_hours`** — Time window (default 24).
- **Queries** — Edit in `mcp_checks_check.py` ~641–642.
- **Limit** — 1000 for both queries.

## Note

The standalone `data_normalization` check (REST-based) uses a different implementation. This MCP version provides an alternative using MCP/DataPrime.
