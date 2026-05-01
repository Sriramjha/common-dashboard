"""
Alert History check — Security alerts summary (last 24h).

Fetches 5 panels from Security Alerts Summary dashboard via Coralogix Metrics API:
1. Total Alerts Count Over Time (line chart - range query)
2. Total Alert Count (stat)
3. Total P1 Alert Count (stat)
4. Total P2 Alert Count (stat)
5. Alert Count By Priority (bar chart - by alert_def_priority)

Uses PromQL queries with placeholders removed. Time range: last 24 hours.
"""
import os
import json
import time
import requests
from modules.builder import Builder
from modules.region_config import get_api_host

# Base selector (placeholders removed)
BASE_SELECTOR = (
    'alert_def_label_alert_type="security",'
    'alert_def_name!~"building block",'
    'alert_def_name!~"null"'
)

# 1. Line chart: total alerts over time (range query)
QUERY_LINE_CHART = f"sum(cx_alerts{{{BASE_SELECTOR}}})"

# 2. Total alert count (instant, sum over 24h)
QUERY_TOTAL_COUNT = f"sum(sum_over_time(cx_alerts{{{BASE_SELECTOR}}}[24h]))"

# 3. P1 count (use concatenation to avoid f-string single '}' error)
QUERY_P1_COUNT = "sum(sum_over_time((cx_alerts{" + BASE_SELECTOR + ',alert_def_priority="P1"}[24h])))'

# 4. P2 count
QUERY_P2_COUNT = "sum(sum_over_time((cx_alerts{" + BASE_SELECTOR + ',alert_def_priority="P2"}[24h])))'

# 5. By priority (bar chart)
QUERY_BY_PRIORITY = f"sum(sum_over_time((cx_alerts{{{BASE_SELECTOR}}}[24h]))) by (alert_def_priority)"


class Main:
    def __init__(self, init_obj: Builder):
        self.cx_api_key = init_obj.cx_api_key
        self.code_dir = init_obj.code_dir
        self.sb_logger = init_obj.sb_logger
        self.cx_region = (init_obj.cx_region or "").strip().lower() or "eu1"

    def run_check(self):
        if not self.cx_api_key:
            if self.sb_logger:
                self.sb_logger.warning("Alert history check skipped: cx_api_key not set")
            self._write_output(self._empty_result("cx_api_key not configured"))
            return

        host = get_api_host(self.cx_region)
        headers = {"Authorization": f"Bearer {self.cx_api_key}"}

        result = {
            "total_count": 0,
            "p1_count": 0,
            "p2_count": 0,
            "by_priority": {},
            "line_chart_data": [],
            "error": None,
        }

        # 1. Range query for line chart (last 24h)
        end_ts = int(time.time())
        start_ts = end_ts - 86400  # 24h in seconds
        step = "1h"  # 24 points for 24h

        try:
            url_range = f"https://{host}/metrics/api/v1/query_range"
            resp = requests.get(
                url_range,
                params={
                    "query": QUERY_LINE_CHART,
                    "start": start_ts,
                    "end": end_ts,
                    "step": step,
                },
                headers=headers,
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            results = data.get("data", {}).get("result", [])
            if results:
                # Prometheus matrix: result[0].values = [[ts, val], ...]
                values = results[0].get("values", [])
                result["line_chart_data"] = [
                    [int(v[0]), float(v[1]) if v[1] != "NaN" else 0.0] for v in values
                ]
        except requests.exceptions.RequestException as e:
            if self.sb_logger:
                self.sb_logger.warning(f"Alert history line chart query failed: {e}")
            result["error"] = str(e)

        # 2–5. Instant queries
        url_instant = f"https://{host}/metrics/api/v1/query"
        queries = [
            ("total_count", QUERY_TOTAL_COUNT, lambda r: int(float(r[0].get("value", [None, 0])[1] or 0))),
            ("p1_count", QUERY_P1_COUNT, lambda r: int(float(r[0].get("value", [None, 0])[1] or 0))),
            ("p2_count", QUERY_P2_COUNT, lambda r: int(float(r[0].get("value", [None, 0])[1] or 0))),
            ("by_priority", QUERY_BY_PRIORITY, self._parse_by_priority),
        ]

        for key, query, parser in queries:
            if result.get("error") and key == "total_count":
                continue
            try:
                resp = requests.get(
                    url_instant,
                    params={"query": query},
                    headers=headers,
                    timeout=60,
                )
                resp.raise_for_status()
                data = resp.json()
                results = data.get("data", {}).get("result", [])
                if results:
                    result[key] = parser(results)
                elif key == "by_priority":
                    result[key] = {}
            except requests.exceptions.RequestException as e:
                if self.sb_logger:
                    self.sb_logger.warning(f"Alert history {key} query failed: {e}")
                if not result.get("error"):
                    result["error"] = str(e)

        if self.sb_logger and not result.get("error"):
            self.sb_logger.element_info(
                f"Alert history: total={result['total_count']}, P1={result['p1_count']}, P2={result['p2_count']}"
            )

        self._write_output({"alert_history": result})

    def _parse_by_priority(self, results: list) -> dict:
        out = {}
        for item in results:
            metric = item.get("metric", {})
            priority = metric.get("alert_def_priority", "Unknown")
            value = item.get("value", [None, "0"])
            count = int(float(value[1])) if value and len(value) > 1 else 0
            out[priority] = count
        return out

    def _empty_result(self, error: str) -> dict:
        return {
            "alert_history": {
                "total_count": 0,
                "p1_count": 0,
                "p2_count": 0,
                "by_priority": {},
                "line_chart_data": [],
                "error": error,
            }
        }

    def _write_output(self, result: dict):
        output_dir = os.path.join(self.code_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
