"""
Limits check — Mapping Stats page (Settings > Mapping Stats).

Sources:
  - ingested_fields_today + mapping_exceptions:
      POST /api/v1/statistics/mapping  →  mappingCount, mappingLimit, mappingErrorCount
  - alerts / enrichments / parsing_rules:
      gRPC QuotaService/GetQuotas
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

    def get_mapping_stats(self):
        """POST /api/v1/statistics/mapping → mappingCount, mappingLimit, mappingErrorCount."""
        host = get_api_host(self.cx_region)
        url = f"https://{host}/api/v1/statistics/mapping"
        headers = {
            "Authorization": f"Bearer {self.session_token}/{self.company_id}",
            "Content-Type": "application/json",
        }
        resp = requests.post(url, headers=headers, data="{}", timeout=30)
        if resp.status_code in (401, 403):
            raise PermissionError(f"Auth error: HTTP {resp.status_code}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def get_quotas(self):
        """gRPC QuotaService/GetQuotas → alerts, enrichments, parsing rules."""
        params = [
            self.grpcurl_path,
            "-H", f"Authorization: Bearer {self.session_token}/{self.company_id}",
            "-d", "{}",
            f"{self.endpoint}:443",
            "com.coralogix.extensions.v1.QuotaService/GetQuotas",
        ]
        resp = subprocess.run(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        stderr = resp.stderr.decode("utf-8").strip()
        if resp.returncode != 0:
            if "Unauthenticated" in stderr or "PermissionDenied" in stderr:
                raise PermissionError(f"Auth error: {stderr}")
            raise RuntimeError(f"gRPC error: {stderr}")
        return json.loads(resp.stdout.decode("utf-8").strip() or "{}")

    def run_check(self):
        try:
            # --- Mapping stats ---
            mapping = None
            try:
                mapping = self.get_mapping_stats()
            except Exception as e:
                self.sb_logger.warning(f"Limits: mapping stats failed — {e}")

            if mapping:
                ingested_used  = mapping.get("mappingCount", "N/A")
                ingested_limit = mapping.get("mappingLimit", "N/A")
                mapping_exc    = mapping.get("mappingErrorCount", "N/A")
            else:
                ingested_used = ingested_limit = mapping_exc = "N/A"

            # --- QuotaService ---
            quotas = {}
            quotas_failed = False
            try:
                quotas = self.get_quotas()
            except Exception as e:
                self.sb_logger.warning(f"Limits: QuotaService failed — {e}")
                quotas_failed = True

            def quota(key):
                q = quotas.get(key, {})
                return {"used": q.get("used", "N/A"), "limit": q.get("limit", "N/A")} if q else {"used": "N/A", "limit": "N/A"}

            e2m = quotas.get("events2Metrics", {})
            result = {"limits": {
                    "ingested_fields_today": {
                        "used": ingested_used,
                        "limit": ingested_limit,
                    },
                    "mapping_exceptions": mapping_exc,
                    "alerts": quota("alert"),
                    "enrichments": quota("enrichment"),
                    "parsing_rules": quota("parsingRule"),
                    "events2metrics_labels_limit": e2m.get("labelsLimit", "N/A"),
                }
            }
            if quotas_failed:
                result["limits_grpc_error"] = {"status": "FAILED", "error": "Check failed — could not fetch quotas"}

        except PermissionError as e:
            self.sb_logger.warning(f"Limits check: AUTH ERROR — {e}")
            result = {"limits": None, "error": "AUTH ERROR"}
        except Exception as e:
            self.sb_logger.warning(f"Limits check failed: {e}")
            result = {"limits": None, "error": str(e)}

        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("Limits check completed")
