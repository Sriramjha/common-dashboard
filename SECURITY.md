# Security Notes — Coralogix Dashboard Project

## What data is fetched

- **Read-only** queries only — alert definitions, incidents, logs
- No writes, no mutations, no deletions are ever made to the Coralogix account
- All requests go to `https://api.eu1.coralogix.com/api/v2/external` (HTTPS only)
- The dashboard HTML is a **static file** — it makes zero API calls itself

---

## API Key security

| File | Contains key? | Notes |
|---|---|---|
| `.env` | ✅ YES | Local only — never commit |
| `.env.example` | ❌ NO | Safe template — can be committed |
| `test_dashboard.py` | ❌ NO | Reads from `.env` / env var only |
| `coralogix-dashboard.html` | ❌ NO | Pure static HTML, no credentials |
| `dashboard_snapshot.json` | ❌ NO | Metadata only (counts, dates) |
| `~/.cursor/mcp.json` | ✅ YES | Cursor config — local only |

### Key type in use
`cxup_*` — **Personal User API Key** (typical scopes: `alerts:read`, Prometheus/metrics access for `cx_alerts`, `data-usage:Read` as needed — see `.env.example`)

This key type:
- Cannot write, create, or delete anything in Coralogix
- Cannot access raw log content (only aggregates/counts)
- Cannot access other team members' data
- Can be revoked instantly at: **Coralogix Console → Account → API Keys**

---

## What's protected

### `.gitignore` covers:
```
.env              ← API key lives here
dashboard_snapshot.json  ← contains account metadata
agent-tools/      ← MCP tool output cache (may contain log samples)
```

### Runtime protections in `test_dashboard.py`:
- **HTTPS enforced** — script exits if `API_BASE` is not `https://`
- **Key never in URLs** — sent only in `Authorization` header
- **Key never in error logs** — HTTP errors redact the key (`***REDACTED***`)
- **30s timeout** — prevents hanging on slow/unresponsive endpoints
- **Missing key check** — exits with clear instructions if key not set

---

## How to rotate the API key

1. Go to **Coralogix Console → Data Flow → API Keys**
2. Delete the old key (`cxup_RMiqr...`)
3. Create a new key with permissions matching your needs (e.g. `alerts:read`, metrics for cx_alerts, `data-usage:Read` — see `.env.example`).
4. Update `.env`: `CORALOGIX_API_KEY=new_key_here`
5. Update `~/.cursor/mcp.json` env block with the new key
6. Run `python3 test_dashboard.py` to confirm it works

---

## What is NOT stored

- No raw log content is stored anywhere on disk
- No PII or user activity data is written to files
- The `dashboard_snapshot.json` only stores aggregated counts (numbers)
- Agent tool output files (`.cursor/projects/.../agent-tools/`) contain API
  responses temporarily during the AI session — these are in `.gitignore`
