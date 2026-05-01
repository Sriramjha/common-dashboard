"""
Team-level default homepage check.
Uses LandingPageService/GetLandingPage (gRPC via grpcurl).
Reports the raw value and whether it is set to a Custom Dashboard.
On failure: log reason to console and continue; do not fail the run.
"""
import os
import json
import subprocess
import json
from modules.builder import Builder


class Main:
    def __init__(self, init_obj: Builder):
        self.session_token = init_obj.session_token
        self.company_id = init_obj.company_id
        self.endpoint = init_obj.endpoint
        self.code_dir = init_obj.code_dir
        self.sb_logger = init_obj.sb_logger
        self.grpcurl_path = getattr(init_obj, 'grpcurl_path', 'grpcurl') or 'grpcurl'

    def get_landing_page(self):
        params = [
            self.grpcurl_path,
            "-H", f"Authorization: Bearer {self.session_token}/{self.company_id}",
            "-d", "{}",
            f"{self.endpoint}:443",
            "com.coralogix.landingpage.v1.LandingPageService/GetLandingPage",
        ]
        response = subprocess.run(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = response.stdout.decode("utf-8").strip()
        stderr = response.stderr.decode("utf-8").strip()
        if not stdout:
            raise RuntimeError(f"grpcurl error: {stderr}")
        return json.loads(stdout)

    def run_check(self):
        try:
            raw = self.get_landing_page()
            team_page = raw.get("teamLandingPage", {})
            raw_value = team_page.get("predefinedLandingPage") or team_page.get("customDashboardId") or None
            is_custom_dashboard = "CUSTOM_DASHBOARD" in (raw_value or "")
            result = {
                "team_default_homepage": {
                    "value": raw_value,
                    "is_custom_dashboard": is_custom_dashboard,
                }
            }
        except Exception as e:
            self.sb_logger.warning(f"Team default homepage check failed: {e}")
            result = {
                "team_default_homepage": {
                    "value": None,
                    "is_custom_dashboard": False,
                    "error": str(e),
                }
            }

        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("Team default homepage check completed")
