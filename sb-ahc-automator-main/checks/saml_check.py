"""
SAML check.
Calls REST GET /api/v1/company/saml.
Prints: configured (true/false) and activated (true/false).
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

    def get_saml_settings(self):
        host = get_api_host(self.cx_region)
        url = f"https://{host}/api/v1/company/saml"
        headers = {"Authorization": f"Bearer {self.session_token}/{self.company_id}"}
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 404:
            return {}
        resp.raise_for_status()
        return resp.json()

    def run_check(self):
        try:
            data = self.get_saml_settings()
            configured = bool(
                data.get("configured") or
                data.get("isConfigured") or
                data.get("idpMetadata") or
                data.get("metadata")
            )
            activated = bool(data.get("isActivated") or data.get("activated"))
            result = {
                "saml": {
                    "configured": configured,
                    "activated": activated,
                }
            }
        except Exception as e:
            self.sb_logger.warning(f"SAML check failed: {e}")
            result = {"saml": {"configured": None, "activated": None, "error": str(e)}}

        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("SAML check completed")
