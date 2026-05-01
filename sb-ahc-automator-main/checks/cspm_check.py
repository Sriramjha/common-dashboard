"""
CSPM (Cloud Security Posture Management) check.
Uses Coralogix DataPrime Query API to detect if CSPM is integrated.
Query: source logs | filter $d.snowbitData=='v2'| countby snowbit.provider, snowbit.additionalData.account
If results → CSPM integrated. If no results → CSPM not configured.
Ref: https://coralogix.com/docs/dataprime/API/direct-archive-query-http/
"""
import json
import os
import datetime
import requests
from modules.builder import Builder
from modules.region_config import get_api_host

CSPM_QUERY = "source logs | filter $d.snowbitData=='v2'| countby snowbit.provider, snowbit.additionalData.account"


def _extract_provider_account(record: dict) -> tuple[str, str]:
    """Extract provider and account from a DataPrime result record (countby output)."""
    # Check labels (countby may put group keys in labels)
    labels = {kv.get("key", ""): kv.get("value", "") for kv in record.get("labels", []) if isinstance(kv, dict)}
    provider = str(labels.get("snowbit.provider", labels.get("provider", "")) or "").strip().lower()
    account = str(labels.get("snowbit.additionalData.account", labels.get("account", "")) or "").strip()

    ud = record.get("userData", record.get("user_data", "{}"))
    if isinstance(ud, str):
        try:
            ud = json.loads(ud)
        except json.JSONDecodeError:
            ud = {}
    if isinstance(ud, dict):
        # Try nested: snowbit.provider, snowbit.additionalData.account
        snowbit = ud.get("snowbit") or ud.get("snowbitData")
        if isinstance(snowbit, dict):
            if not provider:
                provider = str(snowbit.get("provider", "") or "").strip().lower()
            addl = snowbit.get("additionalData") or {}
            if isinstance(addl, dict) and not account:
                account = str(addl.get("account", "") or "").strip()
        if not provider:
            provider = str(ud.get("provider", "") or ud.get("snowbit.provider", "") or "").strip().lower()
        if not account:
            account = str(
                ud.get("account", "")
                or ud.get("snowbit.additionalData.account", "")
                or (ud.get("additionalData", {}).get("account", "") if isinstance(ud.get("additionalData"), dict) else "")
            ).strip()

    return provider or "unknown", account or ""


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
            "query": CSPM_QUERY,
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
                timeout=60,
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
                    rows.extend(result)
                elif isinstance(result, dict):
                    rows.append(result)

            # Group by provider and collect accounts
            providers_map = {}
            for row in rows:
                prov, acct = _extract_provider_account(row)
                if not prov or prov == "unknown":
                    continue
                prov_upper = prov.upper()
                if prov_upper == "GOOGLE":
                    prov_upper = "GCP"
                if prov_upper not in providers_map:
                    providers_map[prov_upper] = []
                if acct and acct not in providers_map[prov_upper]:
                    providers_map[prov_upper].append(acct)

            providers = [
                {"provider": p, "count": len(accts), "accounts": accts}
                for p, accts in sorted(providers_map.items())
            ]
            total_accounts = sum(len(accts) for accts in providers_map.values())
            integrated = len(providers) > 0

            result = {
                "integrated": integrated,
                "total_accounts": total_accounts,
                "providers": providers,
                "accounts": [acct for accts in providers_map.values() for acct in accts],
            }

            if self.sb_logger:
                if integrated:
                    self.sb_logger.element_info(
                        f"CSPM integrated: {total_accounts} account(s) across {len(providers)} provider(s)"
                    )
                else:
                    self.sb_logger.element_info("CSPM not configured (no snowbitData v2 results)")

        except requests.exceptions.Timeout:
            if self.sb_logger:
                self.sb_logger.warning("CSPM query timed out")
            result = {
                "integrated": False,
                "error": "Request timed out. CSPM may not be configured.",
                "total_accounts": 0,
                "providers": [],
                "accounts": [],
            }
        except Exception as e:
            if self.sb_logger:
                self.sb_logger.warning(f"CSPM check failed: {e}")
            result = {
                "integrated": False,
                "error": str(e),
                "total_accounts": 0,
                "providers": [],
                "accounts": [],
            }

        output_dir = os.path.join(self.code_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps({"cspm": result}, indent=2, default=str) + "\n")

        self.sb_logger.element_info("CSPM check completed")
