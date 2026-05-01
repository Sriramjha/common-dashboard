# Snowbit Account Health Check (AHC) Automator

Automated health check tool for Coralogix accounts. Runs comprehensive checks across your Coralogix configuration and sends a detailed PDF report to Slack.

## Features

- **20+ automated checks** covering security, configuration, data usage, and best practices
- **Multi-region support** (EU1, EU2, US1, US2, AP1, AP2, AP3)
- **Slack bot integration** with PDF report attachment
- **MCP-based checks** using DataPrime queries for advanced log analysis
- **AWS Lambda deployment** for serverless execution

Optional **output formatting**: install Prettier on your `PATH` (`npm i -g prettier`). The large vendored bundle under `modules/prettier` is not kept in git.

## Checks Included

| Category | Checks |
|----------|--------|
| **Security** | SAML, MFA, IP Access Control |
| **Configuration** | Team URL, Webhooks, Archive Buckets, Extensions, Enrichments |
| **Dashboards** | Default Dashboard, Dashboard Folders, Team Homepage |
| **Monitoring** | Alerts Metrics, Suppression Rules, TCO Distribution |
| **Alerts** | Disabled Alerts, Never Triggered Alerts (last 30 days) |
| **Data** | Data Usage, Limits, Unparsed Logs, Key Fields Normalization |
| **Advanced** | CSPM, No-Log Alerts, Ingestion Block Alert (MCP), Noisy Alerts (Metrics API) |

---

## Quick Start (Slack Bot)

Run the health check from Slack:

**Recommended — use the modal (popup):**
```
/snowbit_ahc_bot
```
This opens a modal where you can enter your credentials securely. Your API key and session token are never shown in the chat.

**Testing only — pass credentials in the command:**
```
/snowbit_ahc_bot region=EU1 company_id=12345 cx_api_key=cxup_xxx session_token=eyJ...
```
> ⚠️ **Warning:** This method prints your credentials in the Slack chat. Use only for testing, not in shared channels.

### Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `region` | Coralogix region | EU1, EU2, US1, US2, AP1, AP2, AP3 |
| `company_id` | Your Coralogix company ID | 47594 |
| `cx_api_key` | API key with full permissions | cxup_xxx |
| `session_token` | Session token from browser cookies | eyJ... |

### Getting Your Credentials

#### Session Token
1. Log into Coralogix in your browser
2. Open Developer Tools (F12) → Application → Cookies
3. Find the cookie for your region:
   - EU1: `production.coralogix_token`
   - EU2: `euprod2.coralogix_token`
   - AP1: `mumbaisaas.coralogix.token`
   - AP2: `approd2.coralogix_token`
   - AP3: `cx440.coralogix_token`
   - US1: `usprod1.coralogix_token`
   - US2: `cx498.coralogix_token`
4. Copy the cookie value

#### API Key
1. Go to Coralogix → Settings → API Keys
2. Create a new key with full permissions
3. Copy the key

---

## Local Development

### Prerequisites

- Python 3.10+
- `grpcurl` installed (for gRPC-based checks)

### Installation

```bash
cd sb-ahc-automator
pip install -r requirements.txt
```

### Run Locally

**Single-line (recommended — avoids shell line-continuation issues):**
```bash
python3 ahc_runner.py --region AP1 --company-id 1010786 --cx-api-key cxup_xxx --session-token "eyJ..."
```

**Or use the helper script:**
```bash
./run_ahc.sh AP1 1010786 cxup_xxx "eyJ..."
```

**Multi-line (ensure no trailing spaces after `\`):**
```bash
python3 ahc_runner.py \
  --region EU1 \
  --company-id 12345 \
  --cx-api-key your_key \
  --session-token your_token
```

Or use the main.py for backward compatibility (requires config.yaml):

```bash
python main.py
```

---

## AWS Lambda Deployment

See [DEPLOYMENT.md](../DEPLOYMENT.md) for detailed deployment instructions.

### Quick Deploy

1. Create deployment package:
   ```bash
   pip install -r requirements.txt -t package/
   cp -r checks package/ && cp -r modules package/ && cp -r assets package/ 2>/dev/null; cp lambda_handler.py ahc_runner.py package/
   cd package && zip -r ../deployment.zip .
   ```

2. Deploy to Lambda:
   ```bash
   aws lambda create-function \
     --function-name snowbit-ahc-automation \
     --runtime python3.11 \
     --handler lambda_handler.lambda_handler \
     --role arn:aws:iam::ACCOUNT_ID:role/ahc-lambda-role \
     --timeout 300 \
     --memory-size 1024 \
     --zip-file fileb://deployment.zip \
     --environment "Variables={SLACK_BOT_TOKEN=xoxb-xxx,SLACK_SIGNING_SECRET=xxx}"
   ```

3. Set up API Gateway and Slack slash command (see DEPLOYMENT.md)

### CI/CD

Push to `main` branch triggers automatic deployment via GitHub Actions.

---

## Project Structure

```
sb-ahc-automator/
├── lambda_handler.py       # AWS Lambda entry point
├── ahc_runner.py           # Core AHC logic (config-free)
├── main.py                 # Local entry point (legacy)
├── requirements.txt        # Python dependencies
├── checks/                 # Individual check modules
│   ├── saml_check.py
│   ├── mfa_check.py
│   ├── mcp_checks_check.py
│   └── ...
├── modules/                # Shared modules
│   ├── builder.py          # Configuration builder
│   ├── region_config.py    # Region-specific API endpoints
│   ├── slack_report.py     # Slack report generator
│   ├── pdf_report.py       # PDF report generator
│   └── ...
├── assets/                 # Logo and images
├── docs/                   # Check documentation
└── output/                 # Generated reports (gitignored)
```

---

## Output

- **Slack**: Formatted report with concerns highlighted + PDF attachment
- **PDF**: Detailed visual report with charts and tables
- **JSON**: Machine-readable results (`AHC_<team>_<date>_output.json`)

---

## Troubleshooting

### Authentication Errors
- Session tokens expire after ~30 days - get a fresh one
- Verify your API key has full permissions

### Slack Report Not Sending
- Verify the bot has `chat:write` and `files:write` scopes
- Check the channel ID is correct

### Lambda Timeout
- Increase timeout to 600 seconds for large accounts
- Increase memory to 2048 MB if PDF generation fails

---

## License

Internal use only - Snowbit
