# Region Configuration

All checks use the central region mapping in `modules/region_config.py`.

## API Host (REST, DataPrime, Metrics)

| Region | API Host |
|--------|----------|
| EU1 | api.eu1.coralogix.com |
| EU2 | api.eu2.coralogix.com |
| US1 | api.us1.coralogix.com |
| US2 | api.us2.coralogix.com |
| AP1 | api.ap1.coralogix.com |
| AP2 | api.ap2.coralogix.com |
| AP3 | api.ap3.coralogix.com |

**Usage:** `get_api_host(region)` → returns host for REST, DataPrime, Metrics API.

**Example URLs:**
- REST: `https://{host}/api/v1/company`
- DataPrime: `https://{host}/api/v1/dataprime/query`
- Metrics: `https://{host}/metrics/api/v1/query`

## Team Domain (for team URLs)

| Region | Team Domain |
|--------|-------------|
| EU1 | coralogix.com |
| EU2 | eu2.coralogix.com |
| US1 | coralogix.us |
| US2 | cx498.coralogix.com |
| AP1 | app.coralogix.in |
| AP2 | coralogixsg.com |
| AP3 | ap3.coralogix.com |

**Usage:** `get_team_domain(region)` → for building team URLs (e.g. `https://{team_slug}.{domain}/`).

## MCP URL

| Region | MCP URL |
|--------|---------|
| EU1 | https://api.eu1.coralogix.com/mgmt/api/v1/mcp |
| US1 | https://api.us1.coralogix.com/mgmt/api/v1/mcp |
| ... | (same pattern for all regions) |

**Usage:** `get_mcp_url(region)` → returns full MCP endpoint URL.

## Adding a New Region

Edit `modules/region_config.py` and add the region to:
- `API_HOST_BY_REGION`
- `TEAM_DOMAIN_BY_REGION` (if applicable)
- `MCP_URL_BY_REGION`
