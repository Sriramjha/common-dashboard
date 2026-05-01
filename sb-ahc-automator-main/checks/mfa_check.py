"""
MFA check.
Uses gRPC TeamService to describe MFA enforcement state.
Since there is no read-only GetMfaEnforcement endpoint, falls back to
REST GET /api/v1/company and checks for any mfa-related field.
On failure: log reason to console and continue; do not fail the run.
"""
import os
import json
import subprocess
import json
import requests
from modules.builder import Builder

from modules.region_config import get_api_host


class Main:
    def __init__(self, init_obj: Builder):
        self.session_token = init_obj.session_token
        self.company_id = init_obj.company_id
        self.endpoint = init_obj.endpoint
        self.code_dir = init_obj.code_dir
        self.sb_logger = init_obj.sb_logger
        self.cx_region = (init_obj.cx_region or "").strip().lower() or "eu1"
        self.grpcurl_path = getattr(init_obj, 'grpcurl_path', 'grpcurl') or 'grpcurl'

    def get_mfa_via_grpc(self):
        """
        Try GetTeamInfo via gRPC — may contain mfaEnforced field.
        Returns None if unavailable.
        """
        params = [
            self.grpcurl_path,
            "-H", f"Authorization: Bearer {self.session_token}/{self.company_id}",
            "-d", "{}",
            f"{self.endpoint}:443",
            "com.coralogix.identity.teams.v1.TeamService/GetTeamInfo",
        ]
        resp = subprocess.run(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = resp.stdout.decode("utf-8").strip()
        if not stdout:
            return None
        data = json.loads(stdout)
        # Look for any mfa-related field
        for key in ("mfaEnforced", "mfa_enforced", "mfaEnabled", "mfa_enabled"):
            if key in data:
                return bool(data[key])
        return None

    def get_mfa_via_rest(self):
        """Fall back to REST /api/v1/company settings."""
        host = get_api_host(self.cx_region)
        url = f"https://{host}/api/v1/company"
        headers = {"Authorization": f"Bearer {self.session_token}/{self.company_id}"}
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        settings = data.get("settings", {})
        for key in ("mfa_enabled", "mfa_enforced", "mfaEnabled", "mfaEnforced"):
            if key in settings:
                return bool(settings[key])
            if key in data:
                return bool(data[key])
        return None

    def run_check(self):
        try:
            enforced = self.get_mfa_via_grpc()
            if enforced is None:
                enforced = self.get_mfa_via_rest()
            result = {"mfa": {"enforced": enforced}}
        except Exception as e:
            self.sb_logger.warning(f"MFA check failed: {e}")
            result = {"mfa": {"enforced": None, "error": str(e)}}

        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("MFA check completed")
