"""
Default dashboard check.
Uses DashboardCatalogService/GetDashboardCatalog (gRPC via grpcurl) to find the
dashboard marked as isDefault=true and prints its name.
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

    def get_default_dashboard(self):
        params = [
            self.grpcurl_path,
            "-H", f"Authorization: Bearer {self.session_token}/{self.company_id}",
            "-d", "{}",
            f"{self.endpoint}:443",
            "com.coralogixapis.dashboards.v1.services.DashboardCatalogService/GetDashboardCatalog",
        ]
        resp = subprocess.run(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = resp.stdout.decode("utf-8").strip()
        stderr = resp.stderr.decode("utf-8").strip()
        if not stdout:
            raise RuntimeError(f"grpcurl error: {stderr}")
        data = json.loads(stdout)
        for item in data.get("items", []):
            if item.get("isDefault"):
                return item.get("name")
        return None

    def run_check(self):
        try:
            name = self.get_default_dashboard()
            result = {"default_dashboard": name}
        except Exception as e:
            self.sb_logger.warning(f"Default dashboard check failed: {e}")
            result = {"default_dashboard": None, "error": str(e)}

        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("Default dashboard check completed")
