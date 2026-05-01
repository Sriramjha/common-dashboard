"""
TCO Distribution check.

Fetches today's TCO priority distribution percentages as shown in the
TCO Optimizer Overview bar.

The UI bar is computed from quota units (logsQuota + metricsQuota + tracesQuota)
broken down by priority for the current day, sourced from GetTeamsDailyUsage.

Output:
  tco_distribution:
    high_pct:    95.5
    medium_pct:   4.5
    low_pct:      0.0
    blocked_pct:  0.0
"""
import json
import os
import subprocess
from datetime import datetime, timezone

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

    def _grpc(self, method: str, payload: dict) -> dict:
        params = [
            self.grpcurl_path,
            "-H", f"Authorization: Bearer {self.session_token}/{self.company_id}",
            "-d", json.dumps(payload),
            f"{self.endpoint}:443",
            method,
        ]
        resp = subprocess.run(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        stderr = resp.stderr.decode("utf-8").strip()
        if resp.returncode != 0:
            if "Unauthenticated" in stderr or "PermissionDenied" in stderr:
                raise PermissionError(f"Auth error: {stderr}")
            raise RuntimeError(f"gRPC error: {stderr}")
        return json.loads(resp.stdout.decode("utf-8").strip() or "{}")

    def get_tco_distribution(self) -> dict | None:
        """
        Returns TCO priority percentages for today based on quota units
        (logsQuota + metricsQuota + tracesQuota) — matching the UI bar.
        Keys: high_pct, medium_pct, low_pct, blocked_pct (rounded to 1 decimal).
        """
        today = datetime.now(timezone.utc).date().isoformat()
        data = self._grpc(
            "com.coralogix.datausage.v1.DataUsageService/GetTeamsDailyUsage",
            {"param": {"teams": [{"id": int(self.company_id)}], "range": 1}},
        )

        for team_entry in data.get("teamsUsage", []):
            # Find today's entry (last entry in the list)
            metrics = team_entry.get("metrics", [])
            today_metric = None
            for metric in metrics:
                if metric.get("date", "")[:10] == today:
                    today_metric = metric
                    break
            # Fallback: use the most recent entry
            if today_metric is None and metrics:
                today_metric = sorted(metrics, key=lambda m: m.get("date", ""))[-1]

            if today_metric is None:
                continue

            def _priority_sum(field: str, priority: str) -> float:
                """Sum a priority bucket across logs, metrics, and traces quota fields."""
                total = 0.0
                for source in (field,):
                    v = today_metric.get(source, {}).get(priority, {})
                    total += float(v.get("value", 0)) if isinstance(v, dict) else float(v or 0)
                return total

            def _quota_sum(priority: str) -> float:
                total = 0.0
                for field in ("logsQuota", "metricsQuota", "tracesQuota", "sessionRecordingQuota"):
                    v = today_metric.get(field, {}).get(priority, {})
                    total += float(v.get("value", 0)) if isinstance(v, dict) else float(v or 0)
                return total

            high    = _quota_sum("high")
            medium  = _quota_sum("medium")
            low     = _quota_sum("low")
            blocked = _quota_sum("blocked")
            total   = high + medium + low + blocked

            if total == 0:
                return {"high_pct": 0.0, "medium_pct": 0.0, "low_pct": 0.0, "blocked_pct": 0.0}

            return {
                "high_pct":    round(high    / total * 100, 1),
                "medium_pct":  round(medium  / total * 100, 1),
                "low_pct":     round(low     / total * 100, 1),
                "blocked_pct": round(blocked / total * 100, 1),
            }
        return None

    def run_check(self):
        try:
            dist = self.get_tco_distribution()
            if dist is not None:
                result = {"tco_distribution": dist}
            else:
                result = {
                    "tco_distribution": {
                        "high_pct": "N/A",
                        "medium_pct": "N/A",
                        "low_pct": "N/A",
                        "blocked_pct": "N/A",
                    }
                }
        except PermissionError as e:
            self.sb_logger.warning(f"TCO distribution check: AUTH ERROR — {e}")
            result = {"tco_distribution": None, "error": "AUTH ERROR"}
        except Exception as e:
            self.sb_logger.warning(f"TCO distribution check failed: {e}")
            result = {"tco_distribution": None, "error": str(e)}

        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("TCO distribution check completed")
