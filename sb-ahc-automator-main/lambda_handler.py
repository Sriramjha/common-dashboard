"""
AWS Lambda Handler for Snowbit AHC (Account Health Check) Automation
Triggered by Slack slash command: /snowbit_ahc_bot region=<region> company_id=<company_id> cx_api_key=<key> session_token=<token>
"""
import json
import os
import sys
import traceback
import urllib.parse
import hmac
import hashlib
import time
import boto3

# Ensure Lambda deployment root is in path (fixes "No module named 'modules'" in async worker)
_lambda_root = os.path.dirname(os.path.abspath(__file__))
if _lambda_root not in sys.path:
    sys.path.insert(0, _lambda_root)

# Lambda invokes itself asynchronously for the actual work
lambda_client = boto3.client('lambda')


def _log(msg: str, **kwargs):
    """Log to CloudWatch (Lambda captures print to stdout)."""
    extra = f" | {kwargs}" if kwargs else ""
    print(f"[AHC] {msg}{extra}", flush=True)


def verify_slack_signature(event: dict, signing_secret: str, body: str = None) -> bool:
    """Verify the request is from Slack using signing secret.
    body: Use this if API Gateway base64-encodes the body (pass decoded body).
    """
    headers = event.get('headers', {})
    # Handle case-insensitive headers (API Gateway may lowercase)
    headers_lower = {k.lower(): v for k, v in headers.items()}
    
    timestamp = headers_lower.get('x-slack-request-timestamp', '')
    signature = headers_lower.get('x-slack-signature', '')
    if body is None:
        body = event.get('body', '')
    
    if not timestamp or not signature:
        _log("Signature verification failed: missing timestamp or signature")
        return False
    
    # Check timestamp is within 5 minutes
    if abs(time.time() - int(timestamp)) > 60 * 5:
        _log("Signature verification failed: timestamp too old")
        return False
    
    # Compute expected signature
    sig_basestring = f"v0:{timestamp}:{body}"
    expected_sig = 'v0=' + hmac.new(
        signing_secret.encode(),
        sig_basestring.encode(),
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(expected_sig, signature)


def parse_slack_command(body: str) -> dict:
    """Parse Slack slash command body into parameters."""
    params = urllib.parse.parse_qs(body)
    
    # Extract standard Slack fields
    result = {
        'channel_id': params.get('channel_id', [''])[0],
        'channel_name': params.get('channel_name', [''])[0],
        'user_id': params.get('user_id', [''])[0],
        'user_name': params.get('user_name', [''])[0],
        'response_url': params.get('response_url', [''])[0],
        'command': params.get('command', [''])[0],
        'text': params.get('text', [''])[0],
        'trigger_id': params.get('trigger_id', [''])[0],
    }
    
    # Parse command text: region=EU1 company_id=12345 cx_api_key=xxx session_token=yyy
    text = result['text']
    for part in text.split():
        if '=' in part:
            key, value = part.split('=', 1)
            result[key.lower()] = value
    
    return result


def build_ahc_modal(channel_id: str, user_id: str) -> dict:
    """Build modal view for AHC credentials (credentials never appear in chat)."""
    return {
        "type": "modal",
        "callback_id": "ahc_credentials_modal",
        "title": {"type": "plain_text", "text": "AHC Report"},
        "submit": {"type": "plain_text", "text": "Run Health Check"},
        "close": {"type": "plain_text", "text": "Cancel"},
        "private_metadata": json.dumps({"channel_id": channel_id, "user_id": user_id}),
        "blocks": [
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": "💡 _Ensure the bot is invited to this channel — results will be posted here. Use_ `/invite` _to add the app if needed._"}]
            },
            {
                "type": "input",
                "block_id": "region_block",
                "element": {
                    "type": "static_select",
                    "action_id": "region_input",
                    "placeholder": {"type": "plain_text", "text": "Select region"},
                    "options": [
                        {"text": {"type": "plain_text", "text": "EU1"}, "value": "EU1"},
                        {"text": {"type": "plain_text", "text": "EU2"}, "value": "EU2"},
                        {"text": {"type": "plain_text", "text": "US1"}, "value": "US1"},
                        {"text": {"type": "plain_text", "text": "US2"}, "value": "US2"},
                        {"text": {"type": "plain_text", "text": "AP1"}, "value": "AP1"},
                        {"text": {"type": "plain_text", "text": "AP2"}, "value": "AP2"},
                        {"text": {"type": "plain_text", "text": "AP3"}, "value": "AP3"},
                    ],
                },
                "label": {"type": "plain_text", "text": "Region"},
            },
            {
                "type": "input",
                "block_id": "company_id_block",
                "element": {
                    "type": "plain_text_input",
                    "action_id": "company_id_input",
                    "placeholder": {"type": "plain_text", "text": "e.g. 47594"},
                },
                "label": {"type": "plain_text", "text": "Company ID"},
            },
            {
                "type": "input",
                "block_id": "cx_api_key_block",
                "element": {
                    "type": "plain_text_input",
                    "action_id": "cx_api_key_input",
                    "placeholder": {"type": "plain_text", "text": "cxup_xxx..."},
                },
                "label": {"type": "plain_text", "text": "CX API Key"},
            },
            {
                "type": "input",
                "block_id": "session_token_block",
                "element": {
                    "type": "plain_text_input",
                    "action_id": "session_token_input",
                    "placeholder": {"type": "plain_text", "text": "eyJ..."},
                    "multiline": True,
                },
                "label": {"type": "plain_text", "text": "Session Token"},
            },
        ],
    }


def open_modal(trigger_id: str, view: dict, bot_token: str) -> bool:
    """Open modal via Slack API. Returns True on success."""
    import requests
    resp = requests.post(
        "https://slack.com/api/views.open",
        headers={
            "Authorization": f"Bearer {bot_token}",
            "Content-Type": "application/json",
        },
        json={"trigger_id": trigger_id, "view": view},
        timeout=5,
    )
    data = resp.json()
    if not data.get("ok"):
        _log("views.open failed", error=data.get("error", "unknown"))
        return False
    return True


def parse_modal_submission(payload: dict) -> dict:
    """Extract AHC params from view_submission payload."""
    view = payload.get("view", {})
    state = view.get("state", {}).get("values", {})
    metadata = json.loads(view.get("private_metadata", "{}"))

    def get_value(block_id: str, action_id: str) -> str:
        block = state.get(block_id, {})
        elem = block.get(action_id, {})
        # plain_text_input has "value"; static_select has "selected_option" -> "value"
        val = elem.get("value") or (elem.get("selected_option") or {}).get("value", "")
        return (val or "").strip()

    region = get_value("region_block", "region_input")
    company_id = get_value("company_id_block", "company_id_input")
    cx_api_key = get_value("cx_api_key_block", "cx_api_key_input")
    session_token = get_value("session_token_block", "session_token_input")

    return {
        "channel_id": metadata.get("channel_id", ""),
        "user_id": metadata.get("user_id", ""),
        "region": region,
        "company_id": company_id,
        "cx_api_key": cx_api_key,
        "session_token": session_token,
        "user_name": payload.get("user", {}).get("name", ""),
        "response_url": "",
    }


def handle_modal_submission(payload: dict):
    """Process modal submission and start AHC worker."""
    params = parse_modal_submission(payload)
    required = ['region', 'company_id', 'cx_api_key', 'session_token']
    missing = [p for p in required if not params.get(p)]

    block_map = {'region': 'region_block', 'company_id': 'company_id_block', 'cx_api_key': 'cx_api_key_block', 'session_token': 'session_token_block'}
    if missing:
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'response_action': 'errors',
                'errors': {block_map[r]: f'{r.replace("_", " ").title()} is required' for r in missing}
            })
        }

    bot_token = os.environ.get('SLACK_BOT_TOKEN')
    if not bot_token:
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'response_action': 'errors', 'errors': {'region_block': 'Server error: SLACK_BOT_TOKEN not set'}})
        }

    async_payload = {
        '_ahc_async_worker': True,
        'region': params['region'],
        'company_id': params['company_id'],
        'cx_api_key': params['cx_api_key'],
        'session_token': params['session_token'],
        'channel_id': params['channel_id'],
        'user_id': params['user_id'],
        'user_name': params['user_name'],
        'response_url': params['response_url'],
        'bot_token': bot_token,
    }

    try:
        lambda_client.invoke(
            FunctionName=os.environ.get('AWS_LAMBDA_FUNCTION_NAME', 'snowbit-ahc-automation'),
            InvocationType='Event',
            Payload=json.dumps(async_payload)
        )
    except Exception as e:
        _log("Modal: async invoke failed", error=str(e))
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'response_action': 'errors', 'errors': {'region_block': f'Failed to start: {str(e)[:100]}'}})
        }

    # Post ACK message to channel
    import requests
    try:
        requests.post(
            'https://slack.com/api/chat.postMessage',
            headers={'Authorization': f'Bearer {bot_token}', 'Content-Type': 'application/json'},
            json={
                'channel': params['channel_id'],
                'text': f":snowbit: *AHC Report requested by <@{params['user_id']}>*\n"
                        f"Region: `{params['region'].upper()}` | Company ID: `{params['company_id']}`\n"
                        f"Running health checks... Results will be posted here shortly.",
            },
            timeout=5
        )
    except Exception as e:
        _log("Modal: failed to post ACK", error=str(e))

    # Close modal
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({'response_action': 'clear'})
    }


def lambda_handler(event, context):
    """
    Main Lambda handler.
    - For Slack command: Immediately ACK, then invoke async worker
    - For async invocation: Run the actual AHC checks
    """
    _log("Handler invoked", has_async_flag=bool(event.get('_ahc_async_worker')))
    
    # Check if this is an async invocation (worker mode)
    if event.get('_ahc_async_worker'):
        _log("Running as async worker")
        return run_ahc_worker(event)
    
    # Decode body first (API Gateway may base64-encode; Slack signs the raw body)
    body = event.get('body', '')
    if event.get('isBase64Encoded'):
        import base64
        body = base64.b64decode(body).decode('utf-8')
    
    # Get signing secret from environment
    signing_secret = os.environ.get('SLACK_SIGNING_SECRET', '')
    if not signing_secret:
        _log("WARNING: SLACK_SIGNING_SECRET not set")
    
    # Verify Slack signature (skip in dev mode)
    if signing_secret and not os.environ.get('SKIP_SIGNATURE_VERIFICATION'):
        if not verify_slack_signature(event, signing_secret, body=body):
            _log("Signature verification failed")
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'response_type': 'ephemeral',
                    'text': "❌ *Signature verification failed.* Check that SLACK_SIGNING_SECRET matches your Slack app's Signing Secret."
                })
            }
        _log("Signature verified OK")
    
    # Handle view_submission (modal submit) - from Interactivity URL
    params = urllib.parse.parse_qs(body)
    if 'payload' in params:
        _log("Processing modal submission")
        payload = json.loads(params['payload'][0])
        if payload.get('type') == 'view_submission' and payload.get('view', {}).get('callback_id') == 'ahc_credentials_modal':
            return handle_modal_submission(payload)
        # Other interactivity types - acknowledge
        return {'statusCode': 200, 'body': ''}
    
    # This is a Slack slash command
    _log("Processing Slack command")
    params = parse_slack_command(body)
    _log("Parsed params", region=params.get('region'), company_id=params.get('company_id'), channel_id=params.get('channel_id'))
    
    # Get bot token (needed for modal or processing)
    bot_token = os.environ.get('SLACK_BOT_TOKEN')
    if not bot_token:
        _log("ERROR: SLACK_BOT_TOKEN not set")
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'response_type': 'ephemeral',
                'text': "❌ Server configuration error: SLACK_BOT_TOKEN not set"
            })
        }

    # If params missing, open modal (credentials never appear in chat)
    required = ['region', 'company_id', 'cx_api_key', 'session_token']
    missing = [p for p in required if not params.get(p)]
    if missing and params.get('trigger_id'):
        _log("Opening credential modal (params not in chat)")
        view = build_ahc_modal(params['channel_id'], params['user_id'])
        if open_modal(params['trigger_id'], view, bot_token):
            return {'statusCode': 200, 'body': ''}  # Modal opened, no message needed
        # Fall through to show error if modal failed
    elif missing:
        _log("Missing required params", missing=missing)
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'response_type': 'ephemeral',
                'text': f"❌ Missing parameters. Run `/snowbit_ahc_bot` with no args to open the credential form, or provide: `region=EU1 company_id=12345 cx_api_key=xxx session_token=yyy`"
            })
        }
    
    # Prepare async payload
    async_payload = {
        '_ahc_async_worker': True,
        'region': params['region'],
        'company_id': params['company_id'],
        'cx_api_key': params['cx_api_key'],
        'session_token': params['session_token'],
        'channel_id': params['channel_id'],
        'user_id': params['user_id'],
        'user_name': params['user_name'],
        'response_url': params['response_url'],
        'bot_token': bot_token,
    }
    
    # Invoke Lambda asynchronously
    function_name = context.function_name
    _log("Invoking async worker", function=function_name)
    try:
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',  # Async
            Payload=json.dumps(async_payload)
        )
        _log("Async invoke sent successfully")
    except Exception as e:
        _log("ERROR: Async invoke failed", error=str(e))
        traceback.print_exc()
        # Return 200 with ephemeral error so user sees it in Slack (not "app did not respond")
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'response_type': 'ephemeral',
                'text': f"❌ *Failed to start AHC worker*\nError: `{str(e)[:300]}`"
            })
        }
    
    # Immediately respond to Slack
    _log("Returning ACK to Slack")
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({
            'response_type': 'in_channel',
            'text': f":snowbit: *AHC Report requested by <@{params['user_id']}>*\n"
                    f"Region: `{params['region'].upper()}` | Company ID: `{params['company_id']}`\n"
                    f"Running health checks... Results will be posted here shortly."
        })
    }


def _upload_reports_to_s3(output_json_path: str, pdf_path: str = None):
    """Upload output.json and PDF to S3 at ahc_reports/{team_name}/."""
    bucket = os.environ.get('AHC_REPORTS_BUCKET')
    if not bucket:
        _log("AHC_REPORTS_BUCKET not set — skipping S3 upload")
        return
    try:
        import re
        with open(output_json_path, 'r') as f:
            data = json.load(f)
        team_url = data.get('team_url', '')
        team_name = 'unknown'
        if team_url:
            m = re.match(r'https?://([^.]+)\.', team_url)
            if m:
                team_name = m.group(1)
        s3_prefix = f"ahc_reports/{team_name}/"
        s3_client = boto3.client('s3')
        json_basename = os.path.basename(output_json_path)
        s3_client.upload_file(output_json_path, bucket, f"{s3_prefix}{json_basename}")
        _log("S3 upload", path=f"{s3_prefix}{json_basename}")
        if pdf_path and os.path.exists(pdf_path):
            pdf_basename = os.path.basename(pdf_path)
            s3_client.upload_file(pdf_path, bucket, f"{s3_prefix}{pdf_basename}")
            _log("S3 upload", path=f"{s3_prefix}{pdf_basename}")
    except Exception as e:
        _log("S3 upload failed", error=str(e))


def run_ahc_worker(event: dict):
    """
    Async worker that runs the actual AHC checks.
    Called by Lambda invoking itself asynchronously.
    """
    import requests
    
    # Matplotlib needs writable config dir in Lambda (read-only /home)
    if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
        os.environ.setdefault('MPLCONFIGDIR', '/tmp/matplotlib')
    
    region = event['region']
    company_id = event['company_id']
    cx_api_key = event['cx_api_key']
    session_token = event['session_token']
    channel_id = event['channel_id']
    bot_token = event['bot_token']
    response_url = event.get('response_url')
    
    _log("Worker started", region=region, company_id=company_id, channel_id=channel_id)
    
    try:
        # Import and run AHC
        _log("Importing AHCRunner...")
        from ahc_runner import AHCRunner
        
        _log("Creating AHCRunner instance...")
        runner = AHCRunner(
            region=region,
            company_id=company_id,
            cx_api_key=cx_api_key,
            session_token=session_token,
        )
        
        # Run all checks
        _log("Running AHC checks (this takes 2-3 minutes)...")
        output_json_path, pdf_path = runner.run()
        _log("AHC checks complete", output=output_json_path, pdf=pdf_path)
        
        # Check for authentication failure — do not create PDF or send full report
        with open(output_json_path, 'r') as f:
            output_data = json.load(f)
        if output_data.get('auth_error', {}).get('status') == 'FAILED':
            auth_msg = output_data.get('auth_error', {}).get(
                'error', 'Authentication failed. Incorrect API key or incorrect session token.'
            )
            text = f"❌ *Authentication Failed*\n\n{auth_msg}\n\n_No report was generated. Please verify your credentials and try again._"
            _log("Authentication failed — sending auth failure message only")
            posted = False
            if bot_token and channel_id:
                try:
                    r = requests.post(
                        'https://slack.com/api/chat.postMessage',
                        headers={
                            'Authorization': f'Bearer {bot_token}',
                            'Content-Type': 'application/json',
                        },
                        json={'channel': channel_id, 'text': text},
                        timeout=10
                    )
                    data = r.json() if r.text else {}
                    if data.get('ok'):
                        posted = True
                        _log("Auth failure message posted to Slack")
                    else:
                        _log("Slack chat.postMessage failed", error=data.get('error', r.text or 'unknown'))
                except Exception as e:
                    _log("Failed to post auth error to Slack", error=str(e))
            else:
                _log("Cannot use chat.postMessage: bot_token or channel_id missing")
            # Fallback: use response_url (works for slash commands; no bot scope needed)
            if not posted and response_url:
                try:
                    r = requests.post(response_url, json={'response_type': 'ephemeral', 'text': text}, timeout=10)
                    if r.status_code == 200:
                        _log("Auth failure message sent via response_url")
                        posted = True
                    else:
                        _log("response_url post failed", status=r.status_code, body=r.text[:200])
                except Exception as e:
                    _log("response_url fallback failed", error=str(e))
            if not posted:
                _log("WARNING: Could not deliver auth failure message to Slack")
            return {'statusCode': 200, 'body': 'Auth failed'}
        
        # Upload reports to S3 (ahc_reports/{team_name}/)
        _upload_reports_to_s3(output_json_path, pdf_path)
        
        # Send results to Slack
        _log("Sending report to Slack...")
        from modules.slack_report import generate_and_send
        
        slack_cfg = {
            'enabled': True,
            'bot_token': bot_token,
            'channel_id': channel_id,
        }
        
        generate_and_send(
            output_json_path=output_json_path,
            slack_cfg=slack_cfg,
            pdf_path=pdf_path,
        )
        
        _log("Worker completed successfully")
        return {'statusCode': 200, 'body': 'Success'}
        
    except Exception as e:
        error_msg = str(e)
        _log("AHC Error", error=error_msg)
        traceback.print_exc()
        
        # Post formal error to Slack
        try:
            text = (
                "❌ *AHC Report Failed*\n"
                f"Status: Execution error\n"
                f"Error: `{error_msg[:400]}`"
            )
            requests.post(
                'https://slack.com/api/chat.postMessage',
                headers={
                    'Authorization': f'Bearer {bot_token}',
                    'Content-Type': 'application/json',
                },
                json={'channel': channel_id, 'text': text},
                timeout=10
            )
        except Exception:
            pass
        
        return {'statusCode': 500, 'body': f'Error: {error_msg}'}
