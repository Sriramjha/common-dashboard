"""
cx_alerts metrics check — reads the "Aggregate alerts into a gauge-type metric: cx_alerts" toggle.

Source: REST GET /api/v1/company → settings.alerts_auto_send_metrics_enabled

On failure: log reason to console and continue; do not fail the run.
"""
import os
import json
import requests
from modules.builder import Builder
from modules.region_config import get_api_host


class Main:
    def __init__(self, init_obj: Builder):
        self.session_token = init_obj.session_token
        self.company_id = init_obj.company_id
        self.code_dir = init_obj.code_dir
        self.sb_logger = init_obj.sb_logger
        self.cx_region = (init_obj.cx_region or "").strip().lower() or "eu1"

    def get_company_settings(self):
        host = get_api_host(self.cx_region)
        url = f"https://{host}/api/v1/company"
        headers = {"Authorization": f"Bearer {self.session_token}/{self.company_id}"}
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def run_check(self):
        try:
            company = self.get_company_settings()
            enabled = company.get("settings", {}).get("alerts_auto_send_metrics_enabled", False)
            result = {"cx_alerts_metrics": {"enabled": enabled}}
        except Exception as e:
            self.sb_logger.warning(f"cx_alerts metrics check failed: {e}")
            result = {"cx_alerts_metrics": {"enabled": None, "error": str(e)}}

        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("cx_alerts metrics check completed")
