from modules.SBLogger import SBLogger
import subprocess
import yaml
import os
import importlib
import sys
from checks.archive_bucket_check import Main as ArchiveBucketConfigured
from modules.builder import Builder
import re
import datetime


class Main:
    def __init__(self):
        self.sb_logger = SBLogger(False)
        self.archive_configured = False
        self.code_dir = os.path.dirname(__file__)
        self.checks = self.parameters_validator("checks")
        self.checks_modules = self.check_modules_init()
        self.cx_region = self.parameters_validator("region")
        self.company_id = self.parameters_validator("company_id")
        self.cx_api_key = self.parameters_validator("cx_api_key")
        
        # Session token: use region-specific token from config
        self.session_token = self._get_session_token()
        
        self.endpoint = f"ng-api-grpc.{self.region_resolver(self.cx_region)}"
        self.extend_output = self.parameters_validator("extend_output")
        self.metadata = [('authorization', f"Bearer {self.session_token}/{self.company_id}")]
        self.archive_bucket_configured = False
    
    def _get_session_token(self):
        """Get region-specific session token from config or env var."""
        region = (self.cx_region or "eu1").lower().strip()
        
        # 1. Check for region-specific token in config (e.g., session_token_eu1)
        region_token_key = f"session_token_{region}"
        config_token = self.parameters_validator(region_token_key)
        if config_token:
            return config_token
        
        # 2. Check for generic session_token in config (backward compatibility)
        generic_token = self.parameters_validator("session_token")
        if generic_token:
            return generic_token
        
        # 3. Check environment variable
        env_token = os.environ.get("CORALOGIX_SESSION_TOKEN")
        if env_token:
            self.sb_logger.info("Using session token from CORALOGIX_SESSION_TOKEN env var")
            return env_token
        
        # No token found
        self.sb_logger.error(
            f"Session token required but not found for region '{region.upper()}'.\n"
            f"Please add 'session_token_{region}: <your_token>' to config.yaml"
        )
        raise RuntimeError(f"Session token not configured for region {region.upper()}")

    def region_resolver(self, region_sign):
        if region_sign:
            region = region_sign.lower()
            if region == "eu1":
                return "coralogix.com"
            if region == "eu2":
                return "eu2.coralogix.com"
            if region == "us1":
                return "coralogix.us"
            if region == "us2":
                return "cx498.coralogix.com"
            if region == "ap1":
                return "app.coralogix.in"
            if region == "ap2":
                return "coralogixsg.com"
            if region == "ap3":
                return "ap3.coralogix.com"
            else:
                self.sb_logger.error(
                    f"Region {region_sign} is not supported - check the 'general.coralogix_region' parameter")
                exit(9)
        else:
            return "coralogix.com"

    def parameters_validator(self, param):
        config_file = os.path.join(self.code_dir, 'config.yaml')
        with open(config_file, "r") as config_file:
            config_file = yaml.safe_load(config_file)
        try:
            if param in config_file and config_file[param]:
                return config_file[param]
        except:
            return None

    @staticmethod
    def is_valid_uuid(api_key):
        pattern = r'^(?:[a-f0-9]{8}\-(?:(?:[a-f0-9]{4})\-){3}[a-f0-9]{12}|^(?:cx[tu][ph]_[a-zA-Z0-9]{30}))$'
        match = re.fullmatch(pattern, api_key, re.IGNORECASE)
        return bool(match)

    def check_modules_init(self):
        checks_module_names = []

        if self.checks:
            checks = [f"{check}_check.py" for check in self.checks]
        else:
            checks = os.listdir(os.path.join(self.code_dir, "checks"))

        for check in checks:
            check_name = check.split(".")[0]
            if check_name.endswith("_check"):
                check_module_name = f"checks.{check_name}"
                checks_module_names.append(check_module_name)
                try:
                    importlib.import_module(check_module_name)
                except Exception as e:
                    self.sb_logger.error(f"Unable to load module check '{check_module_name}' - {e}")

        tests = {}
        for tester_module in checks_module_names:
            if "Main" in sys.modules[tester_module].__dict__:
                tests[tester_module] = sys.modules[tester_module].__dict__["Main"]
            else:
                self.sb_logger.error(f"There is no 'Main' Class in the {tester_module} file!")
                exit(9)

        return tests

    def get_builder(self):
        # Get mcp_checks from config for backward compatibility
        mcp_checks = self.parameters_validator("mcp_checks")
        return Builder(
            self.session_token,
            self.company_id,
            self.endpoint,
            self.metadata,
            self.sb_logger,
            self.extend_output,
            self.archive_bucket_configured,
            self.code_dir,
            self.cx_api_key,
            self.cx_region,
            mcp_checks=mcp_checks
        )

    def main(self):
        if self.is_valid_uuid(self.cx_api_key) and self.company_id and self.session_token:
            self.sb_logger.info("\033[97mClient Auto Checker Started\033[00m")

            # Initialise output.json — checks append JSON fragments; merged at end
            output_dir = os.path.join(self.code_dir, "output")
            output_file = os.path.join(output_dir, "output.json")
            from modules.region_config import get_report_time_ist
            formatted_datetime = get_report_time_ist()  # IST for Slack/PDF display
            if not os.path.isdir(output_dir):
                os.mkdir(output_dir)
            # Seed the file with the header object (checks will append more JSON objects)
            import json as _json
            with open(output_file, 'w') as file:
                file.write(_json.dumps(
                    {"check_time": str(formatted_datetime), "company_id": self.company_id},
                    indent=2) + "\n")

            # Determine if Archive bucket exists - will affect future checks
            archive_bucket_configured = ArchiveBucketConfigured(self.get_builder()).is_archive_bucket_configured()
            if archive_bucket_configured:
                self.archive_bucket_configured = True

            failed_checks = {}
            for key, value in self.checks_modules.items():
                check_name = key.split(".")[1].replace("_check", "")
                check_class = value
                try:
                    self.sb_logger.check_start(check_name)
                    current_check = check_class(self.get_builder())
                    current_check.run_check()
                    self.sb_logger.check_done(check_name)

                    prettier = os.path.join(self.code_dir, "modules", "prettier", "bin", "prettier")
                    if prettier:
                        subprocess.call([prettier, "-w", os.path.join(self.code_dir, "output")],
                                        stdout=subprocess.DEVNULL,
                                        stderr=subprocess.DEVNULL)
                    else:
                        self.sb_logger.warning("'prettier' couldn't run on the system")
                except Exception as e:
                    # Log the error but continue with other checks
                    error_msg = str(e)
                    # Extract HTTP status code if present
                    status_code = None
                    if "401" in error_msg or "Unauthorized" in error_msg.lower():
                        status_code = 401
                    elif "403" in error_msg or "Forbidden" in error_msg.lower():
                        status_code = 403
                    elif "404" in error_msg or "Not Found" in error_msg.lower():
                        status_code = 404
                    elif "429" in error_msg or "Too Many" in error_msg.lower():
                        status_code = 429
                    elif "500" in error_msg:
                        status_code = 500
                    elif "503" in error_msg:
                        status_code = 503
                    
                    failed_checks[check_name] = {
                        "error": error_msg,
                        "status_code": status_code
                    }
                    self.sb_logger.error(f"Check '{check_name}' failed: {error_msg}")
                    self.sb_logger.warning(f"Continuing with remaining checks...")
                    
                    # Write failure to output file
                    import json as _json
                    failure_output = {
                        f"{check_name}_error": {
                            "status": "FAILED",
                            "error": error_msg,
                            "status_code": status_code
                        }
                    }
                    with open(output_file, 'a') as f:
                        f.write(_json.dumps(failure_output, indent=2) + "\n")
            
            # Log summary of failed checks
            if failed_checks:
                self.sb_logger.warning(f"{len(failed_checks)} check(s) failed: {', '.join(failed_checks.keys())}")
            self.sb_logger.info("\033[92mDone\033[00m")

            # ── Merge all JSON fragments into one valid output.json ───────────
            # Each check appends a complete JSON object (possibly multi-line).
            # Use a streaming decoder to extract all top-level objects.
            import json as _json
            import re as _re
            merged = {}
            with open(output_file, "r") as f:
                raw = f.read()
            decoder = _json.JSONDecoder()
            pos = 0
            while pos < len(raw):
                # Skip whitespace between objects
                while pos < len(raw) and raw[pos] in " \t\n\r":
                    pos += 1
                if pos >= len(raw):
                    break
                try:
                    obj, end_pos = decoder.raw_decode(raw, pos)
                    if isinstance(obj, dict):
                        merged.update(obj)
                    pos = end_pos
                except _json.JSONDecodeError:
                    pos += 1  # skip unrecognised character and keep scanning
            
            # Extract team_name from team_url for filename
            team_url = merged.get('team_url', '')
            team_name = 'unknown'
            if team_url:
                m = _re.match(r'https?://([^.]+)\.', team_url)
                if m:
                    team_name = m.group(1)
            
            # Rename output file to AHC_<team_name>_<date>_output.json (IST date)
            date_str = formatted_datetime[:10]
            new_output_filename = f'AHC_{team_name}_{date_str}_output.json'
            new_output_file = os.path.join(output_dir, new_output_filename)
            
            with open(new_output_file, "w") as f:
                f.write(_json.dumps(merged, indent=2, default=str))
            
            # Remove the old temporary output.json if different
            if new_output_file != output_file and os.path.exists(output_file):
                os.remove(output_file)
            
            self.sb_logger.info(f"Output saved: {new_output_filename}")

            # ── PDF report ──────────────────────────────────────────────────────
            pdf_path = None
            try:
                from modules.pdf_report import generate_report
                self.sb_logger.info("Generating PDF report …")
                pdf_path = generate_report(
                    output_json_path=new_output_file,
                    output_dir=output_dir,
                    logger=self.sb_logger,
                )
                self.sb_logger.info(f"\033[92mPDF report generated: {pdf_path}\033[00m")
            except Exception as e:
                self.sb_logger.warning(f"PDF report generation failed: {e}")

            # ── Slack report ──────────────────────────────────────────────────
            slack_cfg = self.parameters_validator("slack_report") or {}
            if slack_cfg.get("enabled") and (slack_cfg.get("webhook_url") or slack_cfg.get("bot_token")):
                try:
                    from modules.slack_report import generate_and_send
                    self.sb_logger.info("Sending Slack report …")
                    generate_and_send(
                        output_json_path=new_output_file,
                        slack_cfg=slack_cfg,
                        logger=self.sb_logger,
                        pdf_path=pdf_path,
                    )
                    self.sb_logger.info("\033[92mSlack report sent\033[00m")
                except Exception as e:
                    self.sb_logger.warning(f"Slack report failed: {e}")
        else:
            self.sb_logger.error("""Missing one or more values for

    - Company ID
    - Session Token (session_token_<region>)
    - CX API Key (cx_api_key)""")
            exit(9)


if __name__ == "__main__":
    Main().main()
