"""
Data Usage check.

Fetches two values for the team:
  1. daily_quota      — provisioned daily quota (units) via GetTeamsQuota
  2. avg_daily_units  — yesterday's actual daily usage (units) via GetTeamsDailyUsage

GetTeamsQuota:
  Request:  { "param": { "teams": [{"id": <id>}], "time": "<TODAY>T00:00:00.000Z" } }
  Response: { "teamsQuota": [{ "quota": {"value": 100}, ... }] }

GetTeamsDailyUsage:
  Request:  { "param": { "teams": [{"id": <id>}], "range": 1 } }
  Response: { "teamsUsage": [{ "metrics": [{ "date": "YYYY-MM-DDT00:00:00Z", "dailyUsage": {"value": 6.56} }] }] }
  → Pick the entry whose "date" matches yesterday, return dailyUsage.value

Output:
  data_usage:
    daily_quota: 100
    avg_daily_units: 6.57
"""
import json
import os
import subprocess
from datetime import datetime, timedelta, timezone

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

    def get_daily_quota(self) -> int | None:
        """Provisioned daily quota via GetTeamsQuota."""
        today_midnight = datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00.000Z")
        data = self._grpc(
            "com.coralogix.datausage.v1.DataUsageService/GetTeamsQuota",
            {"param": {"teams": [{"id": int(self.company_id)}], "time": today_midnight}},
        )
        for entry in data.get("teamsQuota", []):
            if str(entry.get("team", {}).get("id", "")) == str(self.company_id):
                val = entry.get("quota", {}).get("value")
                return int(val) if val is not None else None
        # Fallback: first entry
        entries = data.get("teamsQuota", [])
        if entries:
            val = entries[0].get("quota", {}).get("value")
            return int(val) if val is not None else None
        return None

    def get_yesterday_usage(self) -> float | None:
        """
        Yesterday's actual daily usage via GetTeamsDailyUsage (range=1).
        Returns dailyUsage.value for the entry whose date matches yesterday.
        """
        yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
        data = self._grpc(
            "com.coralogix.datausage.v1.DataUsageService/GetTeamsDailyUsage",
            {"param": {"teams": [{"id": int(self.company_id)}], "range": 1}},
        )
        for team_entry in data.get("teamsUsage", []):
            for metric in team_entry.get("metrics", []):
                # date field is "YYYY-MM-DDT00:00:00Z" — compare just the date part
                date_str = metric.get("date", "")[:10]
                if date_str == yesterday:
                    val = metric.get("dailyUsage", {}).get("value")
                    return round(float(val), 4) if val is not None else None
        return None

    def run_check(self):
        daily_quota = None
        avg_daily_units = None

        try:
            daily_quota = self.get_daily_quota()
        except Exception as e:
            self.sb_logger.warning(f"Data usage: quota fetch failed — {e}")

        try:
            avg_daily_units = self.get_yesterday_usage()
        except Exception as e:
            self.sb_logger.warning(f"Data usage: daily usage fetch failed — {e}")

        both_failed = daily_quota is None and avg_daily_units is None
        result = {
            "data_usage": {
                "daily_quota": daily_quota if daily_quota is not None else "N/A",
                "avg_daily_units": avg_daily_units if avg_daily_units is not None else "N/A",
            }
        }
        if both_failed:
            result["data_usage_error"] = {"status": "FAILED", "error": "Check failed — could not fetch data"}

        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("Data usage check completed")
