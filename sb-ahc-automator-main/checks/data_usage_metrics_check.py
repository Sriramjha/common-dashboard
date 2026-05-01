"""
Data Usage Metrics check.
Reads the "Enable metrics" toggle from the Data Usage page.

Primary  : GET /api/v1/statistics  → statisticsEnabled / metricsEnabled / enabled / isEnabled
Fallback : GET /api/v1/company     → settings.data_usage_to_metrics_enabled

Output:
  data_usage_metrics: enabled   → toggle is ON
  data_usage_metrics: disabled  → toggle is OFF
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

    def _get(self, path):
        host = get_api_host(self.cx_region)
        url = f"https://{host}{path}"
        headers = {"Authorization": f"Bearer {self.session_token}/{self.company_id}"}
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code in (401, 403):
            raise PermissionError(f"Auth error: HTTP {resp.status_code}")
        return resp

    def get_metrics_enabled(self):
        # Primary: /api/v1/statistics
        resp = self._get("/api/v1/statistics")
        if resp.status_code == 200:
            data = resp.json()
            for key in ("statisticsEnabled", "metricsEnabled", "enabled", "isEnabled"):
                if key in data:
                    return bool(data[key])

        # Fallback: /api/v1/company → settings.data_usage_to_metrics_enabled
        resp = self._get("/api/v1/company")
        resp.raise_for_status()
        data = resp.json()
        settings = data.get("settings", {})
        for key in ("data_usage_to_metrics_enabled", "statisticsEnabled", "metricsEnabled"):
            if key in settings:
                return bool(settings[key])
        for key in ("statisticsEnabled", "metricsEnabled"):
            if key in data:
                return bool(data[key])

        return False

    def run_check(self):
        try:
            enabled = self.get_metrics_enabled()
            status = "enabled" if enabled else "disabled"
            result = {"data_usage_metrics": status}
        except PermissionError as e:
            self.sb_logger.warning(f"Data usage metrics check: AUTH ERROR — {e}")
            result = {"data_usage_metrics": None, "error": "AUTH ERROR"}
        except Exception as e:
            self.sb_logger.warning(f"Data usage metrics check failed: {e}")
            result = {"data_usage_metrics": None, "error": str(e)}

        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("Data usage metrics check completed")
