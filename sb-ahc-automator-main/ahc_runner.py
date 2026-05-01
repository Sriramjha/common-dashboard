"""
AHC Runner - Runs Account Health Checks without config.yaml
All parameters are passed directly (for Lambda/CLI usage)
"""
import os
import sys
import re
import json
import time
import datetime
from modules.region_config import get_report_time_ist
import importlib
import subprocess
import tempfile

# Ensure deployment root is in path (fixes "No module named 'modules'" in Lambda)
_runner_root = os.path.dirname(os.path.abspath(__file__))
if _runner_root not in sys.path:
    sys.path.insert(0, _runner_root)

from modules.SBLogger import SBLogger
from modules.builder import Builder
from checks.archive_bucket_check import Main as ArchiveBucketConfigured


# Default checks to run (order matters)
DEFAULT_CHECKS = [
    'team_url',
    'webhook',
    'send_log_webhook',
    'archive_bucket',
    'extensions',
    'enrichments',
    'team_default_homepage',
    'default_dashboard',
    'team_auditing',
    'cora_ai',
    'cx_alerts_metrics',
    'saml',
    'mfa',
    'ip_access',
    'suppression_rules',
    'data_usage_metrics',
    'data_usage',
    'limits',
    'tco_distribution',
    'dashboard_folders',
    'cspm',
    'data_normalization',
    'alert_history',
    'alerts_status',
    'noisy_alerts',
    'mcp_checks',
]

# Default MCP checks configuration (CSPM is now a standalone check)
DEFAULT_MCP_CHECKS = [
    {
        'name': 'unparsed_logs',
        'output_key': 'unparsed_logs',
        'lookback_hours': 24,
        'type': 'unparsed_logs',
        'prompt': '''Check if logs are properly parsed as JSON in Coralogix.

LOGIC:
  - UNPARSED logs have a 'text' field (raw string, not parsed as JSON)
  - PARSED logs do NOT have a 'text' field (successfully parsed as JSON)

STEP 1: Count total UNPARSED logs (last 24 hours)
  Run query: source logs | lucene '_exists_:text' | count

STEP 2: Count total PARSED logs (last 24 hours)
  Run query: source logs | lucene 'NOT _exists_:text' | count

STEP 4: Get UNPARSED logs breakdown by application
  Run query: source logs | lucene '_exists_:text' | groupby $l.applicationname:string.toLowerCase() as app_name aggregate count() as unparsed_count | orderby unparsed_count desc

STEP 5: Get TOTAL logs per application
  Run query: source logs | groupby $l.applicationname:string.toLowerCase() as app_name aggregate count() as total_count | orderby total_count desc'''
    },
    {
        'name': 'no_log_alerts',
        'output_key': 'no_log_alerts',
        'type': 'no_log_alerts',
        'triggered_lookback_days': 7,
    },
    {
        'name': 'ingestion_block_alert',
        'output_key': 'ingestion_block_alert',
        'type': 'ingestion_block_alert',
    },
]


class AHCRunner:
    """
    Runs AHC checks with parameters passed directly (no config.yaml needed).
    """
    
    def __init__(
        self,
        region: str,
        company_id: str,
        cx_api_key: str,
        session_token: str,
        checks: list = None,
        mcp_checks: list = None,
        extend_output: bool = True,
        output_dir: str = None,
    ):
        self.sb_logger = SBLogger(False)
        self.region = region.upper()
        self.company_id = str(company_id)
        self.cx_api_key = cx_api_key
        self.session_token = session_token
        self.extend_output = extend_output
        
        # Use /tmp for Lambda (writable) or specified directory
        if output_dir:
            self.output_dir = output_dir
        elif os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            self.output_dir = '/tmp/output'
        else:
            self.output_dir = os.path.join(os.path.dirname(__file__), 'output')
        
        # Checks use code_dir + "/output" for output.json — must point to writable dir in Lambda
        self.code_dir = os.path.dirname(self.output_dir)
        self.endpoint = f"ng-api-grpc.{self._region_resolver(self.region)}"
        self.metadata = [('authorization', f"Bearer {self.session_token}/{self.company_id}")]
        self.archive_bucket_configured = False
        
        # Checks to run
        self.checks = checks or DEFAULT_CHECKS
        self.mcp_checks_config = mcp_checks or DEFAULT_MCP_CHECKS
        
        # Load check modules
        self.checks_modules = self._load_check_modules()
    
    def _region_resolver(self, region_sign: str) -> str:
        """Convert region code to Coralogix domain."""
        region = region_sign.lower()
        domains = {
            'eu1': 'coralogix.com',
            'eu2': 'eu2.coralogix.com',
            'us1': 'coralogix.us',
            'us2': 'cx498.coralogix.com',
            'ap1': 'app.coralogix.in',
            'ap2': 'coralogixsg.com',
            'ap3': 'ap3.coralogix.com',
        }
        if region not in domains:
            raise ValueError(f"Unsupported region: {region_sign}")
        return domains[region]
    
    def _load_check_modules(self) -> dict:
        """Load check modules dynamically."""
        checks_modules = {}
        
        for check_name in self.checks:
            module_name = f"checks.{check_name}_check"
            try:
                module = importlib.import_module(module_name)
                if hasattr(module, 'Main'):
                    checks_modules[module_name] = module.Main
                else:
                    self.sb_logger.error(f"No 'Main' class in {module_name}")
            except Exception as e:
                self.sb_logger.error(f"Failed to load {module_name}: {e}")
        
        return checks_modules
    
    def _get_builder(self) -> Builder:
        """Create Builder instance for checks."""
        deployment_root = os.path.dirname(os.path.abspath(__file__))
        return Builder(
            session_token=self.session_token,
            company_id=self.company_id,
            endpoint=self.endpoint,
            metadata=self.metadata,
            sb_logger=self.sb_logger,
            extend_output=self.extend_output,
            archive_bucket_configured=self.archive_bucket_configured,
            code_dir=self.code_dir,
            cx_api_key=self.cx_api_key,
            cx_region=self.region,
            mcp_checks=self.mcp_checks_config,
            deployment_root=deployment_root,
        )
    
    def _log(self, msg: str, **kwargs):
        """Log to CloudWatch (Lambda captures print)."""
        extra = f" | {kwargs}" if kwargs else ""
        print(f"[AHC Runner] {msg}{extra}", flush=True)

    def _verify_auth(self) -> tuple:
        """
        Early auth check via GenAiService/Settings.
        Returns: (success: bool, company_id_from_api: int|None, error_msg: str|None)
        On success, company_id_from_api is from the API response.
        """
        import subprocess
        from modules.builder import get_grpcurl_path
        deployment_root = os.path.dirname(os.path.abspath(__file__))
        grpcurl_path = get_grpcurl_path(deployment_root)
        params = [
            grpcurl_path,
            "-H", f"Authorization: Bearer {self.session_token}/{self.company_id}",
            "-d", "{}",
            f"{self.endpoint}:443",
            "com.coralogix.genai.v1.GenAiService/Settings",
        ]
        try:
            resp = subprocess.run(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
            stdout = resp.stdout.decode("utf-8", errors="replace").strip()
            stderr = resp.stderr.decode("utf-8", errors="replace").strip()
            if resp.returncode != 0 or not stdout:
                err_str = stderr or stdout or "Unknown error"
                # Check if auth-related
                if any(x in err_str for x in ("UNAUTHENTICATED", "401", "Unauthorized", "Status.UNAUTHENTICATED: 16")):
                    return False, None, "Authentication failed. Incorrect API key or incorrect session token."
                return False, None, err_str
            data = json.loads(stdout)
            company_id = data.get("companyId")
            return True, company_id, None
        except subprocess.TimeoutExpired:
            return False, None, "Authentication check timed out."
        except json.JSONDecodeError as e:
            return False, None, f"Invalid response: {e}"
        except Exception as e:
            err_str = str(e)
            if any(x in err_str for x in ("UNAUTHENTICATED", "401", "Unauthorized", "Status.UNAUTHENTICATED")):
                return False, None, "Authentication failed. Incorrect API key or incorrect session token."
            return False, None, err_str

    def run(self) -> tuple:
        """
        Run all checks and generate reports.
        Returns: (output_json_path, pdf_path)
        """
        self._log("AHC Started", output_dir=self.output_dir)
        self.sb_logger.info("\033[97mAHC Started\033[00m")
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize output file
        output_file = os.path.join(self.output_dir, 'output.json')
        current_datetime = datetime.datetime.now(datetime.timezone.utc)
        formatted_datetime = get_report_time_ist()  # IST for Slack/PDF display
        with open(output_file, 'w') as f:
            f.write(json.dumps({
                'check_time': formatted_datetime,
                'company_id': self.company_id,
            }, indent=2) + '\n')
        
        # Early auth check — stop if authentication fails
        self._log("Verifying authentication...")
        auth_ok, company_id_from_api, auth_error_msg = self._verify_auth()
        if not auth_ok:
            self._log("Authentication failed", error=auth_error_msg)
            self.sb_logger.error(f"Authentication failed: {auth_error_msg}")
            auth_output = {
                'auth_error': {
                    'status': 'FAILED',
                    'error': auth_error_msg or 'Authentication failed. Incorrect API key or incorrect session token.',
                },
                'company_id': self.company_id,
            }
            with open(output_file, 'a') as f:
                f.write(json.dumps(auth_output, indent=2) + '\n')
            merged = self._merge_json_output(output_file)
            date_str = (current_datetime + datetime.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d")  # IST date
            final_output_path = os.path.join(self.output_dir, f'AHC_auth_failed_{date_str}_output.json')
            with open(final_output_path, 'w') as f:
                json.dump(merged, f, indent=2, default=str)
            if os.path.exists(output_file) and final_output_path != output_file:
                os.remove(output_file)
            return final_output_path, None  # No PDF

        # Merge company_id from API into output if available
        if company_id_from_api is not None:
            with open(output_file, 'a') as f:
                f.write(json.dumps({'company_id': company_id_from_api}, indent=2) + '\n')
            self._log("company_id from API", company_id=company_id_from_api)

        # Check archive bucket first
        try:
            self._log("Checking archive bucket...")
            archive_check = ArchiveBucketConfigured(self._get_builder())
            if archive_check.is_archive_bucket_configured():
                self.archive_bucket_configured = True
        except Exception as e:
            self.sb_logger.warning(f"Archive bucket check failed: {e}")
            self._log("Archive bucket check failed", error=str(e))
        
        # Run all checks
        failed_checks = {}
        check_elapsed_seconds = {}
        total = len(self.checks_modules)
        for idx, (module_name, check_class) in enumerate(self.checks_modules.items(), 1):
            check_name = module_name.split('.')[1].replace('_check', '')
            t0 = time.perf_counter()
            try:
                self._log(f"Check {idx}/{total}: {check_name}", status="running")
                self.sb_logger.check_start(check_name)
                check_instance = check_class(self._get_builder())
                check_instance.run_check()
                self.sb_logger.check_done(check_name)
                self._log(f"Check {idx}/{total}: {check_name}", status="done")
            except Exception as e:
                self._log(f"Check {idx}/{total}: {check_name}", status="failed", error=str(e)[:100])
                error_msg = str(e)
                status_code = self._extract_status_code(error_msg)
                failed_checks[check_name] = {'error': error_msg, 'status_code': status_code}
                self.sb_logger.check_failed(check_name, error_msg)
                
                # Write failure to output
                with open(output_file, 'a') as f:
                    f.write(json.dumps({
                        f'{check_name}_error': {
                            'status': 'FAILED',
                            'error': error_msg,
                            'status_code': status_code
                        }
                    }, indent=2) + '\n')
            finally:
                check_elapsed_seconds[check_name] = round(time.perf_counter() - t0, 1)
        
        if failed_checks:
            self.sb_logger.warning(f"{len(failed_checks)} check(s) failed: {', '.join(failed_checks.keys())}")
        
        self.sb_logger.info("\033[92mChecks completed\033[00m")
        self._log("All checks completed", failed_count=len(failed_checks))
        
        # Merge JSON fragments
        self._log("Merging JSON output...")
        merged = self._merge_json_output(output_file)
        merged['check_elapsed_seconds'] = check_elapsed_seconds
        
        # Get team name from URL
        team_url = merged.get('team_url', '')
        team_name = 'unknown'
        if team_url:
            m = re.match(r'https?://([^.]+)\.', team_url)
            if m:
                team_name = m.group(1)
        
        # Save final output
        date_str = (current_datetime + datetime.timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d')  # IST date
        final_output_filename = f'AHC_{team_name}_{date_str}_output.json'
        final_output_path = os.path.join(self.output_dir, final_output_filename)
        
        with open(final_output_path, 'w') as f:
            json.dump(merged, f, indent=2, default=str)
        
        # Remove temp file
        if final_output_path != output_file and os.path.exists(output_file):
            os.remove(output_file)
        
        self.sb_logger.info(f"Output saved: {final_output_filename}")
        
        # Generate PDF report
        pdf_path = None
        try:
            self._log("Generating PDF report...")
            from modules.pdf_report import generate_report
            self.sb_logger.info("Generating PDF report...")
            pdf_path = generate_report(
                output_json_path=final_output_path,
                output_dir=self.output_dir,
                logger=self.sb_logger,
            )
            self.sb_logger.info(f"PDF generated: {pdf_path}")
            self._log("PDF generated", path=pdf_path)
        except Exception as e:
            self.sb_logger.warning(f"PDF generation failed: {e}")
            self._log("PDF generation failed", error=str(e))
        
        self._log("Run complete", output=final_output_path, pdf=pdf_path)
        return final_output_path, pdf_path
    
    def _extract_status_code(self, error_msg: str) -> int:
        """Extract HTTP status code from error message."""
        patterns = [
            ('401', 401), ('Unauthorized', 401),
            ('403', 403), ('Forbidden', 403),
            ('404', 404), ('Not Found', 404),
            ('429', 429), ('Too Many', 429),
            ('500', 500), ('503', 503),
        ]
        for pattern, code in patterns:
            if pattern.lower() in error_msg.lower():
                return code
        return None
    
    def _merge_json_output(self, output_file: str) -> dict:
        """Merge all JSON fragments into single dict."""
        merged = {}
        with open(output_file, 'r') as f:
            raw = f.read()
        
        decoder = json.JSONDecoder()
        pos = 0
        while pos < len(raw):
            while pos < len(raw) and raw[pos] in ' \t\n\r':
                pos += 1
            if pos >= len(raw):
                break
            try:
                obj, end_pos = decoder.raw_decode(raw, pos)
                if isinstance(obj, dict):
                    merged.update(obj)
                pos = end_pos
            except json.JSONDecodeError:
                pos += 1
        
        return merged


# CLI support for local testing
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Run AHC checks')
    parser.add_argument('--region', required=True, help='Coralogix region (EU1, US1, etc.)')
    parser.add_argument('--company-id', required=True, help='Company ID')
    parser.add_argument('--cx-api-key', required=True, help='Coralogix API key')
    parser.add_argument('--session-token', required=True, help='Session token')
    parser.add_argument('--output-dir', help='Output directory')
    
    args = parser.parse_args()
    
    runner = AHCRunner(
        region=args.region,
        company_id=args.company_id,
        cx_api_key=args.cx_api_key,
        session_token=args.session_token,
        output_dir=args.output_dir,
    )
    
    output_json, pdf_path = runner.run()
    print(f"\nOutput: {output_json}")
    if pdf_path:
        print(f"PDF: {pdf_path}")
