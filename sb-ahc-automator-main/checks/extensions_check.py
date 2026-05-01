"""
Extensions check.

Two gRPC calls total:
  1. GetDeployedExtensions  → which extensions are deployed + their current version
  2. GetAllExtensions       → latest available version for every extension

Compare deployed version vs latest version to classify each extension.

Output:
  extensions:
    amount: 36
    updated:
      - WIZAudit
      - ...
    update_available:
      - AWSCloudTrail
      - ...
"""
import asyncio
import json
import os
import subprocess

import json

from modules.builder import Builder
from grpclib.client import Channel
from modules.extension_deployement import ExtensionDeploymentServiceStub


MAX_MSG_BYTES = 50 * 1024 * 1024  # GetAllExtensions response is ~21 MB


class Main:
    def __init__(self, init_obj: Builder):
        self.session_token = init_obj.session_token
        self.company_id = init_obj.company_id
        self.endpoint = init_obj.endpoint
        self.session_metadata = [('authorization', f"Bearer {self.session_token}/{self.company_id}")]
        self.sb_logger = init_obj.sb_logger
        self.extend_output = init_obj.extend_output
        self.code_dir = init_obj.code_dir
        self.grpc_config = init_obj.grpc_config
        self.grpcurl_path = getattr(init_obj, 'grpcurl_path', 'grpcurl') or 'grpcurl'

    # IDs/aliases for security extensions (UI may show "Coralogix System Monitoring", API may return "CoralogixSystem")
    # CoralogixSystemMonitoring and CoralogixSystem are the same extension — we consolidate to one display entry
    SECURITY_EXTENSION_IDS = [
        "SnowbitUtilities",
        "CoralogixSystemMonitoring",  # Same as CoralogixSystem — both map to "Coralogix System Monitoring"
        "CoralogixSystem",
        "SecurityAlertsSummaryDashboard",
        "SecurityAlertsSummaryMITREDashboard",
    ]
    # Display name for consolidated entries (first ID wins as display key)
    CORALOGIX_SYSTEM_IDS = frozenset({"CoralogixSystemMonitoring", "CoralogixSystem"})

    @staticmethod
    def _compare_versions(v1: str, v2: str) -> int:
        """Return 1 if v1 > v2, -1 if v1 < v2, 0 if equal."""
        try:
            p1 = list(map(int, v1.split('.')))
            p2 = list(map(int, v2.split('.')))
            for a, b in zip(p1, p2):
                if a > b:
                    return 1
                if a < b:
                    return -1
            return 0
        except Exception:
            return 0

    def _get_all_extensions_latest(self) -> dict[str, str]:
        """
        Call GetAllExtensions via grpcurl (needs large message size limit ~21 MB).
        Returns { ext_id: latest_version_str }.
        """
        params = [
            self.grpcurl_path,
            "-max-msg-sz", str(MAX_MSG_BYTES),
            "-H", f"Authorization: Bearer {self.session_token}/{self.company_id}",
            "-d", "{}",
            f"{self.endpoint}:443",
            "com.coralogix.extensions.v1.ExtensionService/GetAllExtensions",
        ]
        resp = subprocess.run(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
        if resp.returncode != 0:
            err = resp.stderr.decode().strip()
            raise RuntimeError(f"GetAllExtensions failed: {err}")

        data = json.loads(resp.stdout.decode())
        latest = {}
        for ext in data.get("extensions", []):
            ext_id = ext.get("id")
            revisions = ext.get("revisions", [])
            versions = [r["version"] for r in revisions if r.get("version")]
            if ext_id and versions:
                latest[ext_id] = max(versions, key=lambda v: list(map(int, v.split('.'))))
        return latest

    async def _get_deployed(self) -> dict:
        channel = Channel(host=self.endpoint, port=443, ssl=True, config=self.grpc_config)
        try:
            stub = ExtensionDeploymentServiceStub(channel)
            resp = await stub.get_deployed_extensions(metadata=self.session_metadata)
            return resp.to_pydict()
        finally:
            channel.close()

    def run_check(self):
        result = {
            "security_extensions": {},
            "extensions": {"amount": 0, "updated": [], "update_available": []},
        }

        try:
            deployed_data = asyncio.run(self._get_deployed())
        except Exception as e:
            self.sb_logger.error(f"Extensions check: GetDeployedExtensions failed — {e}")
            output_dir = os.path.join(self.code_dir, "output")
            with open(os.path.join(output_dir, "output.json"), "a") as f:
                f.write(json.dumps({
                    "extensions_error": {"status": "FAILED", "error": str(e)},
                    "security_extensions": {},
                }, indent=2, default=str) + "\n")
            return

        # Support both snake_case (betterproto) and camelCase
        deployed_list = deployed_data.get("deployedExtensions") or deployed_data.get("deployed_extensions") or []
        deployed = [e for e in deployed_list if (e.get("id") or e.get("extension_id")) != "Testing"]

        def _normalize(s):
            return (s or "").replace(" ", "").replace("-", "").lower()

        ext_ids = set()
        ext_ids_normalized = {}  # normalized_id -> original id
        for e in deployed:
            eid = e.get("id") or e.get("extension_id") or ""
            if eid:
                ext_ids.add(eid)
                ext_ids_normalized[_normalize(eid)] = eid
            name = e.get("name") or ""
            if name:
                ext_ids_normalized[_normalize(name)] = eid or name

        for sec_id in self.SECURITY_EXTENSION_IDS:
            # Match by exact id, or by normalized id/name (e.g. "CoralogixSystemMonitoring" matches "Coralogix System Monitoring")
            is_deployed = (
                sec_id in ext_ids
                or _normalize(sec_id) in ext_ids_normalized
            )
            result["security_extensions"][sec_id] = is_deployed

        # Consolidate CoralogixSystemMonitoring and CoralogixSystem into one display entry
        sm_deployed = any(
            result["security_extensions"].get(k, False)
            for k in self.CORALOGIX_SYSTEM_IDS
        )
        for k in self.CORALOGIX_SYSTEM_IDS:
            result["security_extensions"].pop(k, None)
        result["security_extensions"]["Coralogix System Monitoring"] = sm_deployed

        try:
            latest_versions = self._get_all_extensions_latest()
        except Exception as e:
            self.sb_logger.warning(f"Extensions check: GetAllExtensions failed — {e}")
            latest_versions = {}

        result["extensions"]["amount"] = len(deployed)
        for ext in deployed:
            eid = ext.get("id") or ext.get("extension_id") or ""
            cur_ver = str(ext.get("version", ""))
            latest = latest_versions.get(eid)
            if latest and cur_ver and self._compare_versions(str(latest), cur_ver) == 1:
                result["extensions"]["update_available"].append(eid)
            else:
                result["extensions"]["updated"].append(eid)

        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
        self.sb_logger.element_info("Extensions check completed")
