"""
IP Access check.
Uses REST GET /api/v1/company → settings.ip_allow_list_enabled.
Prints: enabled (true/false).
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

    def get_ip_access(self):
        host = get_api_host(self.cx_region)
        url = f"https://{host}/api/v1/company"
        headers = {"Authorization": f"Bearer {self.session_token}/{self.company_id}"}
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        settings = data.get("settings", {})
        return bool(settings.get("ip_allow_list_enabled", False))

    def run_check(self):
        try:
            enabled = self.get_ip_access()
            result = {"ip_access": {"enabled": enabled}}
        except Exception as e:
            self.sb_logger.warning(f"IP access check failed: {e}")
            result = {"ip_access": {"enabled": None, "error": str(e)}}

        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("IP access check completed")
