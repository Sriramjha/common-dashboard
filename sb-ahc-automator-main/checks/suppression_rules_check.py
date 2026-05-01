"""
Suppression rules check.
Calls gRPC AlertSchedulerRuleService/GetBulkAlertSchedulerRule.
Falls back to REST GET /api/v1/alert-scheduler-rules on gRPC failure.

Output:
  suppression_rules: used      → if total rules > 0
  suppression_rules: not_used  → if total rules == 0
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

    def get_rules_via_grpc(self):
        params = [
            self.grpcurl_path,
            "-H", f"Authorization: Bearer {self.session_token}/{self.company_id}",
            "-d", "{}",
            f"{self.endpoint}:443",
            "com.coralogixapis.alerting.alert_scheduler_rule_protobuf.v1.AlertSchedulerRuleService/GetBulkAlertSchedulerRule",
        ]
        resp = subprocess.run(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        stderr = resp.stderr.decode("utf-8").strip()

        if resp.returncode != 0:
            if "Unauthenticated" in stderr or "PermissionDenied" in stderr:
                raise PermissionError(f"Auth error: {stderr}")
            raise RuntimeError(f"gRPC error: {stderr}")

        stdout = resp.stdout.decode("utf-8").strip()
        if not stdout:
            return []
        data = json.loads(stdout)
        return data.get("alertSchedulerRules", [])

    def get_rules_via_rest(self):
        host = get_api_host(self.cx_region)
        url = f"https://{host}/api/v1/alert-scheduler-rules"
        headers = {"Authorization": f"Bearer {self.session_token}/{self.company_id}"}
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 404:
            return []
        if resp.status_code in (401, 403):
            raise PermissionError(f"Auth error: HTTP {resp.status_code}")
        resp.raise_for_status()
        data = resp.json()
        return data.get("alertSchedulerRules", [])

    def run_check(self):
        try:
            try:
                rules = self.get_rules_via_grpc()
            except (RuntimeError, Exception) as grpc_err:
                self.sb_logger.warning(f"Suppression rules gRPC failed ({grpc_err}), trying REST fallback")
                rules = self.get_rules_via_rest()

            status = "used" if len(rules) > 0 else "not_used"
            result = {"suppression_rules": status}

        except PermissionError as e:
            self.sb_logger.warning(f"Suppression rules check: AUTH ERROR — {e}")
            result = {"suppression_rules": None, "error": "AUTH ERROR"}
        except Exception as e:
            self.sb_logger.warning(f"Suppression rules check failed: {e}")
            result = {"suppression_rules": None, "error": str(e)}

        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("Suppression rules check completed")
