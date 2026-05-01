import os.path
import asyncio
import json
import requests
from modules.builder import Builder
from modules.region_config import get_metrics_archive_api_host
from grpclib.client import Channel
from modules.archive.v2 import TargetServiceStub, GetTargetRequest


class Main:
    def __init__(self, init_obj: Builder):
        self.company_id = init_obj.company_id
        self.session_token = init_obj.session_token
        self.cx_api_key = getattr(init_obj, 'cx_api_key', None)
        self.cx_region = (getattr(init_obj, 'cx_region', '') or '').strip().lower() or 'eu1'
        self.endpoint = init_obj.endpoint
        self.metadata = init_obj.metadata
        self.code_dir = init_obj.code_dir
        self.grpc_config = init_obj.grpc_config
        self.sb_logger = init_obj.sb_logger
        self.extend_output = init_obj.extend_output

    async def get_raw_output(self, query):
        """Logs archive via gRPC TargetService/GetTarget."""
        if query != "LogsBucket":
            return None
        channel = Channel(host=self.endpoint, port=443, ssl=True, config=self.grpc_config)
        try:
            stub = TargetServiceStub(channel)
            request = await stub.get_target(get_target_request=GetTargetRequest(), metadata=self.metadata)
            return request.target.to_dict()
        finally:
            channel.close()

    def is_archive_bucket_configured(self):
        try:
            logs = asyncio.run(self.get_raw_output("LogsBucket"))
            return bool(logs and "s3" in logs)
        except Exception:
            return False

    def _get_metrics_via_rest_api(self) -> tuple[dict | None, str | None, str | None]:
        """
        Metrics via REST GET /api/v1/metricsArchiveConfig.
        Returns (metrics_dict, bucket_name, region) or (None, None, None).
        Response: {"Ok": {"bucket_name": "...", "region": "...", ...}} or {"Ok": null}
        """
        host = get_metrics_archive_api_host(self.cx_region)
        url = f"https://{host}/api/v1/metricsArchiveConfig"
        # Try cx_api_key first, then session_token/company_id
        auth_options = []
        if self.cx_api_key:
            auth_options.append(f"Bearer {self.cx_api_key}")
        auth_options.append(f"Bearer {self.session_token}/{self.company_id}")
        for auth_header in auth_options:
            try:
                resp = requests.get(url, headers={"Authorization": auth_header}, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                ok = data.get("Ok")
                if ok and isinstance(ok, dict):
                    bucket = ok.get("bucket_name") or ok.get("bucketName")
                    region = ok.get("region", "")
                    return ok, bucket, region
                return None, None, None  # {"Ok": null} = not configured
            except requests.HTTPError as e:
                if e.response.status_code in (401, 403):
                    continue  # Try next auth
                self.sb_logger.info(f"metricsArchiveConfig REST API: {e}")
            except Exception as e:
                self.sb_logger.info(f"metricsArchiveConfig REST API: {e}")
        return None, None, None

    def archive_buckets_filter_results(self):
        logs = None
        logs_error = None
        try:
            logs = asyncio.run(self.get_raw_output("LogsBucket"))
        except Exception as e:
            logs_error = str(e)
            self.sb_logger.error(f"Failed to get logs archive from Coralogix - {e}")

        # Metrics: REST API /api/v1/metricsArchiveConfig (works local + Lambda)
        metrics = None
        metrics_bucket = None
        metrics_region = None
        metrics_error = None
        cfg, metrics_bucket, metrics_region = self._get_metrics_via_rest_api()
        if cfg:
            metrics = {"bucket": metrics_bucket, "region": metrics_region}
        if not metrics:
            metrics_error = "Could not fetch metrics archive (REST API failed)"
            self.sb_logger.error(metrics_error)

        results = {"archive_buckets": {}}

        # Record API failure so status/PDF reflect it
        api_errors = []
        if logs_error:
            api_errors.append(f"Logs: {logs_error[:100]}")
        if metrics_error:
            api_errors.append(f"Metrics: {metrics_error[:100]}")
        if api_errors:
            results["archive_bucket_error"] = {
                "status": "FAILED",
                "error": "; ".join(api_errors),
            }

        # Logs
        if logs and "s3" in logs:
            if self.extend_output:
                logs_bucket = logs["s3"]["bucket"]
                logs_region = logs["s3"]["region"]
                logs_is_active = logs["archiveSpec"]["isActive"]
                logs_format = logs["archiveSpec"]["archivingFormatId"]

                results["archive_buckets"].update(
                    {"logs": {"bucket": logs_bucket, "region": logs_region, "active": logs_is_active,
                              "format": logs_format}})
            else:
                results["archive_buckets"].update({"logs": {"active": True}})
        else:
            results["archive_buckets"].update({"logs": {"active": False}})

        # Metrics
        if metrics:
            metrics_entry = {"active": True}
            if metrics_bucket:
                metrics_entry["bucket"] = metrics_bucket
            if metrics_region:
                metrics_entry["region"] = metrics_region
            results["archive_buckets"].update({"metrics": metrics_entry})
        else:
            results["archive_buckets"].update({"metrics": {"active": False}})

        return results

    def run_check(self):
        fin_results = self.archive_buckets_filter_results()

        output_dir = os.path.join(self.code_dir, "output")

        with open(os.path.join(output_dir, "output.json"), "a") as file:
            file.write(json.dumps(fin_results, indent=2, default=str) + "\n")
            self.sb_logger.element_info("Archive check completed")
