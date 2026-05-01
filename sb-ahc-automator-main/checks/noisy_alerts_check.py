"""
Noisy Alerts check — Top 10 security alerts by trigger count (last 24h).

Uses Coralogix Metrics API (PromQL) with cx_alerts metric.
Requires CX Alerts Metrics to be enabled.

Region-specific endpoints: https://api.{region}.coralogix.com/metrics/api/v1/query
"""
import os
import json
import requests
from modules.builder import Builder
from modules.region_config import get_api_host

# PromQL: top 10 security alerts by sum over last 24h, exclude building block and null
NOISY_ALERTS_QUERY = (
    'topk(10, sort_desc(sum(sum_over_time(cx_alerts{'
    'alert_def_label_alert_type="security",'
    'alert_def_name!~"building block",'
    'alert_def_name!~"null"'
    '}[24h])) by (alert_def_name, alert_def_priority)))'
)


class Main:
    def __init__(self, init_obj: Builder):
        self.cx_api_key = init_obj.cx_api_key
        self.code_dir = init_obj.code_dir
        self.sb_logger = init_obj.sb_logger
        self.cx_region = (init_obj.cx_region or "").strip().lower() or "eu1"

    def run_check(self):
        if not self.cx_api_key:
            if self.sb_logger:
                self.sb_logger.warning("Noisy alerts check skipped: cx_api_key not set")
            self._write_output({
                "noisy_alerts": {
                    "noisy_alerts": [],
                    "time_range": "Last 24 hours",
                    "total_count": 0,
                    "error": "cx_api_key not configured",
                }
            })
            return

        host = get_api_host(self.cx_region)
        url = f"https://{host}/metrics/api/v1/query"
        headers = {"Authorization": f"Bearer {self.cx_api_key}"}

        try:
            resp = requests.get(
                url,
                params={"query": NOISY_ALERTS_QUERY},
                headers=headers,
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            if self.sb_logger:
                self.sb_logger.warning(f"Noisy alerts check failed: {e}")
            result = {
                "noisy_alerts": {
                    "noisy_alerts": [],
                    "time_range": "Last 24 hours",
                    "total_count": 0,
                    "error": str(e),
                }
            }
            self._write_output(result)
            return

        # Parse Prometheus vector response
        result_data = data.get("data", {}).get("result", [])
        noisy_list = []
        total_count = 0

        for i, item in enumerate(result_data[:10], 1):
            metric = item.get("metric", {})
            value = item.get("value", [None, "0"])
            count = int(float(value[1])) if value and len(value) > 1 else 0
            total_count += count

            noisy_list.append({
                "rank": i,
                "alert_name": metric.get("alert_def_name", "Unknown"),
                "incident_count": count,
                "priority": metric.get("alert_def_priority", "N/A"),
            })

        result = {
            "noisy_alerts": {
                "noisy_alerts": noisy_list,
                "time_range": "Last 24 hours",
                "total_count": total_count,
            }
        }

        if self.sb_logger:
            self.sb_logger.element_info(
                f"Noisy alerts: {len(noisy_list)} alerts, {total_count} total triggers"
            )

        self._write_output(result)

    def _write_output(self, result: dict):
        output_dir = os.path.join(self.code_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
