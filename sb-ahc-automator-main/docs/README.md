# AHC Check Documentation

Documentation for all Account Health Check (AHC) checks in the Snowbit AHC Automator.

## Region Configuration

All checks use the central region mapping. See [REGION_CONFIG.md](REGION_CONFIG.md).

## Check Index

### REST / gRPC Checks

| Check | Doc | Purpose |
|-------|-----|---------|
| `team_url` | [TEAM_URL_CHECK.md](TEAM_URL_CHECK.md) | Resolve team URL for report links |
| `webhook` | [WEBHOOK_CHECK.md](WEBHOOK_CHECK.md) | List outbound webhook types and counts |
| `send_log_webhook` | [SEND_LOG_WEBHOOK_CHECK.md](SEND_LOG_WEBHOOK_CHECK.md) | Check if Send Log webhook exists |
| `archive_bucket` | [ARCHIVE_BUCKET_CHECK.md](ARCHIVE_BUCKET_CHECK.md) | Verify logs/metrics archive buckets |
| `extensions` | [EXTENSIONS_CHECK.md](EXTENSIONS_CHECK.md) | List extensions, version status, security extensions |
| `enrichments` | [ENRICHMENTS_CHECK.md](ENRICHMENTS_CHECK.md) | List enrichments by type (geo, security) |
| `team_default_homepage` | [TEAM_DEFAULT_HOMEPAGE_CHECK.md](TEAM_DEFAULT_HOMEPAGE_CHECK.md) | Team default landing page |
| `default_dashboard` | [DEFAULT_DASHBOARD_CHECK.md](DEFAULT_DASHBOARD_CHECK.md) | Dashboard marked as default |
| `team_auditing` | [TEAM_AUDITING_CHECK.md](TEAM_AUDITING_CHECK.md) | Team auditing configured |
| `cora_ai` | [CORA_AI_CHECK.md](CORA_AI_CHECK.md) | Cora AI toggle settings |
| `cx_alerts_metrics` | [CX_ALERTS_METRICS_CHECK.md](CX_ALERTS_METRICS_CHECK.md) | Alerts auto-send to metrics |
| `saml` | [SAML_CHECK.md](SAML_CHECK.md) | SAML configuration and activation |
| `mfa` | [MFA_CHECK.md](MFA_CHECK.md) | MFA enforcement |
| `ip_access` | [IP_ACCESS_CHECK.md](IP_ACCESS_CHECK.md) | IP allow list enabled |
| `suppression_rules` | [SUPPRESSION_RULES_CHECK.md](SUPPRESSION_RULES_CHECK.md) | Suppression rules used |
| `data_usage_metrics` | [DATA_USAGE_METRICS_CHECK.md](DATA_USAGE_METRICS_CHECK.md) | Data usage metrics toggle |
| `data_usage` | [DATA_USAGE_CHECK.md](DATA_USAGE_CHECK.md) | Daily quota and usage |
| `limits` | [LIMITS_CHECK.md](LIMITS_CHECK.md) | Mapping stats and resource quotas |
| `tco_distribution` | [TCO_DISTRIBUTION_CHECK.md](TCO_DISTRIBUTION_CHECK.md) | TCO priority distribution |
| `dashboard_folders` | [DASHBOARD_FOLDERS_CHECK.md](DASHBOARD_FOLDERS_CHECK.md) | Dashboards in folders |
| `cspm` | [CSPM_CHECK.md](CSPM_CHECK.md) | CSPM integration (DataPrime) |
| `data_normalization` | [DATA_NORMALIZATION_CHECK.md](DATA_NORMALIZATION_CHECK.md) | cx_security by app (REST) |
| `noisy_alerts` | [NOISY_ALERTS_CHECK.md](NOISY_ALERTS_CHECK.md) | Top 10 noisy alerts (Metrics API) |

### MCP Checks

| Check | Doc | Purpose |
|-------|-----|---------|
| `unparsed_logs` | [UNPARSED_LOGS_CHECK.md](UNPARSED_LOGS_CHECK.md) | Parsed vs unparsed logs |
| `no_log_alerts` | [NO_LOG_ALERTS_LOGIC.md](NO_LOG_ALERTS_LOGIC.md) | Apps without no-log alert coverage |
| `ingestion_block_alert` | [INGESTION_BLOCK_ALERT_CHECK.md](INGESTION_BLOCK_ALERT_CHECK.md) | Data ingestion block alert |
| `key_fields_normalized` | [KEY_FIELDS_NORMALIZED_CHECK.md](KEY_FIELDS_NORMALIZED_CHECK.md) | cx_security by app (MCP) |

## Run Order (DEFAULT_CHECKS)

1. team_url, webhook, send_log_webhook, archive_bucket  
2. extensions, enrichments, team_default_homepage, default_dashboard  
3. team_auditing, cora_ai, cx_alerts_metrics  
4. saml, mfa, ip_access, suppression_rules  
5. data_usage_metrics, data_usage, limits, tco_distribution  
6. dashboard_folders, cspm, data_normalization  
7. noisy_alerts (Metrics API), mcp_checks (unparsed_logs, no_log_alerts, ingestion_block_alert)

## Output

All checks append JSON to `output/output.json`. The PDF and Slack reports merge and interpret this data.
