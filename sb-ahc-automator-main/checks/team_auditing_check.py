"""
Team Auditing check.
Uses REST API GET /api/v1/auditing/configured (same as Coralogix UI).

Configured   → response is object with id, name, team_url (e.g. payu-security-poc_audit)
Not configured → response is null

Auth: Bearer {session_token}/{company_id}, Cgx-Team-Id: {company_id}
Host: api.app.coralogix.in (AP1), api.coralogix.com (EU1), etc. — same as team_url.
"""
import os
import json
import requests
from modules.builder import Builder
from modules.region_config import get_api_host

AUDIT_CONFIGURED_PATH = "/api/v1/auditing/configured"


class Main:
    def __init__(self, init_obj: Builder):
        self.session_token = init_obj.session_token
        self.company_id = init_obj.company_id
        self.code_dir = init_obj.code_dir
        self.sb_logger = init_obj.sb_logger
        self.cx_region = (init_obj.cx_region or "").strip().lower() or "eu1"

    def run_check(self):
        host = get_api_host(self.cx_region)
        url = f"https://{host}{AUDIT_CONFIGURED_PATH}"

        headers = {
            "Authorization": f"Bearer {self.session_token}/{self.company_id}",
            "Content-Type": "application/json",
            "Cgx-Team-Id": str(self.company_id),
        }

        try:
            r = requests.get(url, headers=headers, timeout=15)
            r.raise_for_status()

            # Not configured: API may return empty body, 204, or null
            text = (r.text or "").strip()
            if not text:
                data = None
            else:
                try:
                    data = r.json()
                except (ValueError, json.JSONDecodeError):
                    data = None

            # Configured: response is object with id, name, team_url
            # Not configured: response is null or empty
            if data is None or (isinstance(data, dict) and not data):
                configured = False
                audit_team_name = None
                audit_team_id = None
            elif isinstance(data, dict):
                configured = bool(data.get("id") or data.get("name") or data.get("team_url"))
                audit_team_name = (data.get("name") or data.get("team_url") or "").strip()
                audit_team_id = data.get("id")
            else:
                configured = False
                audit_team_name = None
                audit_team_id = None

            result = {
                "team_auditing": {
                    "configured": configured,
                    "audit_team_name": audit_team_name if configured else None,
                    "audit_team_id": audit_team_id if configured else None,
                }
            }

            if self.sb_logger:
                if configured:
                    self.sb_logger.element_info(f"Team auditing configured — audit team: {audit_team_name}")
                else:
                    self.sb_logger.element_info("Team auditing not configured")

        except requests.exceptions.HTTPError as e:
            if self.sb_logger:
                self.sb_logger.warning(f"Team auditing check failed: {e}")
            raise
        except Exception as e:
            if self.sb_logger:
                self.sb_logger.warning(f"Team auditing check failed: {e}")
            raise

        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("Team auditing check completed")
