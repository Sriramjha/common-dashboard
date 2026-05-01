# Unparsed Logs Check (MCP)

## Purpose

Counts parsed vs unparsed logs in the last 24 hours and breaks down unparsed by application.

## Type

MCP check — uses `get_logs` tool with DataPrime queries. Queries are extracted from the `prompt` field in config.

## Logic

- **UNPARSED** logs have a `text` field (raw string, not parsed as JSON).
- **PARSED** logs do NOT have a `text` field.

## Queries (from config prompt)

| Step | Query | Purpose |
|------|-------|---------|
| 1 | `source logs \| lucene '_exists_:text' \| count` | Count unparsed |
| 2 | `source logs \| lucene 'NOT _exists_:text' \| count` | Count parsed |
| 4 | `source logs \| lucene '_exists_:text' \| groupby $l.applicationname count() as unparsed_count \| orderby unparsed_count desc` | Unparsed by app |
| 5 | `source logs \| groupby $l.applicationname count() as total_count \| orderby total_count desc` | Total by app |

Queries are extracted from the prompt via regex: `Run query: <query>`.

## Output

```json
{
  "unparsed_logs": {
    "total_unparsed": 4234,
    "total_parsed": 1500000,
    "grand_total": 1504234,
    "unparsed_pct": 0.28,
    "all_parsed": false,
    "apps": [
      { "application": "app1", "count": 1000, "total": 5000, "pct": 20.0 }
    ]
  }
}
```

## Config (ahc_runner.py)

```python
{
  "name": "unparsed_logs",
  "output_key": "unparsed_logs",
  "lookback_hours": 24,
  "type": "unparsed_logs",
  "prompt": "..."  # Contains STEP 1-5 queries
}
```

## Fine-tuning

- **`lookback_hours`** — Time window (default 24).
- **`prompt`** — Edit queries in the prompt; they are extracted via regex.
- **Fallback queries** — In `mcp_checks_check.py`: `q_unparsed_count`, `q_parsed_count`, `q_unparsed_by_app`, `q_total_by_app` if extraction fails.
- **Limit** — 500 for count queries, 1000 for total-by-app.
