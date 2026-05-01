"""
Central region configuration for all Coralogix API endpoints.

All checks use these mappings for region-specific API hosts.
Pattern: api.{region}.coralogix.com (consistent across REST, Metrics, MCP, DataPrime).
"""
import datetime


def get_report_time_ist() -> str:
    """Return current time in IST (UTC+5:30) for Slack/PDF display. Lambda uses UTC; this ensures consistent local (IST) display."""
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    ist_offset = datetime.timedelta(hours=5, minutes=30)
    ist_now = utc_now + ist_offset
    return ist_now.strftime("%Y-%m-%d %H:%M:%S")

# API host per region — used by REST, DataPrime, Metrics API
# Full URL examples: https://{host}/api/v1/..., https://{host}/metrics/api/v1/query
API_HOST_BY_REGION = {
    "eu1": "api.eu1.coralogix.com",
    "eu2": "api.eu2.coralogix.com",
    "us1": "api.us1.coralogix.com",
    "us2": "api.us2.coralogix.com",
    "ap1": "api.ap1.coralogix.com",
    "ap2": "api.ap2.coralogix.com",
    "ap3": "api.ap3.coralogix.com",
}

# Metrics archive config API — AP1 uses api.app.coralogix.in
METRICS_ARCHIVE_API_HOST_BY_REGION = {
    "eu1": "api.eu1.coralogix.com",
    "eu2": "api.eu2.coralogix.com",
    "us1": "api.us1.coralogix.com",
    "us2": "api.us2.coralogix.com",
    "ap1": "api.app.coralogix.in",
    "ap2": "api.ap2.coralogix.com",
    "ap3": "api.ap3.coralogix.com",
}

# Team/app domain per region — for team URLs (e.g. team.coralogix.com)
TEAM_DOMAIN_BY_REGION = {
    "eu1": "coralogix.com",
    "eu2": "eu2.coralogix.com",
    "us1": "coralogix.us",
    "us2": "cx498.coralogix.com",
    "ap1": "app.coralogix.in",
    "ap2": "coralogixsg.com",
    "ap3": "ap3.coralogix.com",
}

# MCP URL per region
MCP_URL_BY_REGION = {
    "eu1": "https://api.eu1.coralogix.com/mgmt/api/v1/mcp",
    "eu2": "https://api.eu2.coralogix.com/mgmt/api/v1/mcp",
    "us1": "https://api.us1.coralogix.com/mgmt/api/v1/mcp",
    "us2": "https://api.us2.coralogix.com/mgmt/api/v1/mcp",
    "ap1": "https://api.ap1.coralogix.com/mgmt/api/v1/mcp",
    "ap2": "https://api.ap2.coralogix.com/mgmt/api/v1/mcp",
    "ap3": "https://api.ap3.coralogix.com/mgmt/api/v1/mcp",
}


def get_api_host(region: str) -> str:
    """Get API host for the given region. Defaults to eu1 if unknown."""
    r = (region or "").strip().lower() or "eu1"
    return API_HOST_BY_REGION.get(r, API_HOST_BY_REGION["eu1"])


def get_metrics_archive_api_host(region: str) -> str:
    """Get API host for metrics archive config. AP1 uses api.app.coralogix.in."""
    r = (region or "").strip().lower() or "eu1"
    return METRICS_ARCHIVE_API_HOST_BY_REGION.get(r, API_HOST_BY_REGION["eu1"])


def get_team_domain(region: str) -> str:
    """Get team domain for the given region. Defaults to eu1 if unknown."""
    r = (region or "").strip().lower() or "eu1"
    return TEAM_DOMAIN_BY_REGION.get(r, TEAM_DOMAIN_BY_REGION["eu1"])


def get_mcp_url(region: str) -> str:
    """Get MCP URL for the given region. Defaults to eu1 if unknown."""
    r = (region or "").strip().upper() or "EU1"
    return MCP_URL_BY_REGION.get(r.lower(), MCP_URL_BY_REGION["eu1"])
