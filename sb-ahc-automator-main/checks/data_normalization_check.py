"""
Data Normalisation Status check.
Uses DataPrime HTTP API to check cx_security status per app/subsystem (last 24 hours).
Query: source logs | groupby application, subsystem | count missing vs total
Ref: https://coralogix.com/docs/dataprime/API/direct-archive-query-http/
"""
import json
import os
import datetime
import requests
from modules.builder import Builder
from modules.region_config import get_api_host

NORMALIZATION_QUERY = """source logs
| groupby
    $l.applicationname as application,
    $l.subsystemname   as subsystem
  aggregate
    count_if($d.cx_security == null) as missing_cx_security_count,
    count()                          as total_logs
| filter $d.missing_cx_security_count == $d.total_logs
| orderby $d.missing_cx_security_count desc
| choose
    $d.application as application,
    $d.subsystem   as subsystem"""


def _parse_result_row(record: dict) -> dict:
    """Extract application and subsystem from DataPrime result."""
    labels = {kv.get("key", ""): kv.get("value", "") for kv in record.get("labels", []) if isinstance(kv, dict)}
    ud = record.get("userData", record.get("user_data", "{}"))
    if isinstance(ud, str):
        try:
            ud = json.loads(ud)
        except json.JSONDecodeError:
            ud = {}
    if isinstance(ud, dict):
        labels.update(ud)
    app = str(labels.get("application", "") or "").strip()
    sub = str(labels.get("subsystem", "") or "").strip()
    return {"application": app or "unknown", "subsystem": sub or "-"}


class Main:
    def __init__(self, init_obj: Builder):
        self.cx_api_key = init_obj.cx_api_key
        self.sb_logger = init_obj.sb_logger
        self.code_dir = init_obj.code_dir
        self.cx_region = (init_obj.cx_region or "").strip().lower() or "eu1"

    def run_check(self):
        host = get_api_host(self.cx_region)
        url = f"https://{host}/api/v1/dataprime/query"

        now = datetime.datetime.now(datetime.timezone.utc)
        start = (now - datetime.timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        payload = {
            "query": NORMALIZATION_QUERY,
            "metadata": {
                "tier": "TIER_ARCHIVE",
                "syntax": "QUERY_SYNTAX_DATAPRIME",
                "startDate": start,
                "endDate": end,
                "defaultSource": "logs",
            },
        }

        try:
            r = requests.post(
                url,
                headers={
                    "Authorization": f"Bearer {self.cx_api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=180,
            )
            r.raise_for_status()

            rows = []
            for line in r.text.strip().split("\n"):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                result = obj.get("result", {}).get("results", obj.get("result", []))
                if isinstance(result, list):
                    for rec in result:
                        if isinstance(rec, dict):
                            rows.append(_parse_result_row(rec))
                elif isinstance(result, dict):
                    rows.append(_parse_result_row(result))

            # Exclude cx-metrics and coralogix-alerts (query already filters for 100% missing)
            EXCLUDED_APPS = {"cx-metrics", "coralogix-alerts"}

            concern_rows = [
                row for row in rows
                if (row.get("application", "") or "").strip().lower() not in EXCLUDED_APPS
            ]

            result = {
                "data_normalization": {
                    "concern_count": len(concern_rows),
                    "concern_rows": concern_rows,
                    "all_normalized": len(concern_rows) == 0,
                    "summary": f"{len(concern_rows)} app(s)/subsystem(s) with missing cx_security (last 24h)" if concern_rows else "All data sources have cx_security (last 24h)",
                }
            }

            if self.sb_logger:
                if concern_rows:
                    self.sb_logger.element_info(f"Data normalisation status: {len(concern_rows)} app(s) with missing cx_security (last 24h)")
                else:
                    self.sb_logger.element_info("Data normalisation status: all sources have cx_security (last 24h)")

        except requests.exceptions.RequestException as e:
            if self.sb_logger:
                self.sb_logger.warning(f"Data normalization check failed: {e}")
            result = {
                "data_normalization": {
                    "concern_count": 0,
                    "concern_rows": [],
                    "all_normalized": True,
                    "summary": "Check failed (last 24h)",
                    "error": str(e),
                }
            }

        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("Data normalization check completed")
