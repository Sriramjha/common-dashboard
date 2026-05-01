"""
Alerts Status check — Disabled Alerts and Never Triggered Alerts.

Uses Coralogix REST APIs:
- Disabled Alerts: alertDefProperties.enabled is False (Alert Definitions API)
- Never Triggered Alerts: alert definitions minus alerts that appear in Incidents API
  (last 30 days, contextualLabels.alert_name)

API refs:
- https://docs.coralogix.com/api-reference/latest/alert-definitions-service/get-a-list-of-all-accessible-alert-definitions
- https://docs.coralogix.com/api-reference/latest/incidents-service/list-incidents-with-filters
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import requests

from modules.builder import Builder
from modules.region_config import get_api_host


def _fetch_incident_alert_names_via_aggregations(
    api_key: str, host: str, start_iso: str, end_iso: str, logger=None
) -> set[str] | None:
    """
    Try ListIncidentAggregations API (same as UI "Group by Alert Definition").
    Uses flat query params: $.filter.startTime, $.filter.endTime, pagination.pageSize=1000.
    Returns set of alert names if successful, None if API returns ungrouped data.
    """
    url = f"https://{host}/mgmt/openapi/latest/incidents/incidents/v1/aggregations"
    params = {
        "$.filter.startTime": start_iso,
        "$.filter.endTime": end_iso,
        "pagination.pageSize": 1000,
        "$.groupBys[0].contextualLabel": "alert_name",  # Group by Alert Definition (matches UI)
    }
    try:
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {api_key}"},
            params=params,
            timeout=200,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        if logger:
            logger.warning(f"Incident aggregations API failed: {e}")
        return None

    aggs = data.get("incidentAggs", data.get("incident_aggs", []))
    names: set[str] = set()
    for agg in aggs:
        gv = agg.get("groupBysValue", agg.get("group_bys_value", []))
        for g in gv:
            ctx = g.get("contextualLabel", g.get("contextualLabels", {}))
            if isinstance(ctx, dict):
                val = ctx.get("fieldValue", ctx.get("contextualLabelValues", []))
                if isinstance(val, list):
                    names.update(v for v in val if isinstance(v, str) and v)
                elif isinstance(val, str):
                    names.add(val)
            elif isinstance(ctx, str):
                names.add(ctx)

    if names:
        if logger:
            logger.element_info(f"Incidents: {len(names)} unique alert names (aggregations API)")
        return names
    return None


def _fetch_incident_alert_names_via_rest(
    api_key: str, host: str, start_iso: str, end_iso: str, created_min_iso: str, logger=None
) -> set[str] | None:
    """
    Fetch unique alert names from incidents (last 30 days) via List Incidents API.

    Fallback when aggregations API returns ungrouped data. Uses "open during window"
    logic: incident was open at any point in [start, end], AND created >= created_min_iso.
    """
    url = f"https://{host}/mgmt/openapi/latest/incidents/incidents/v1"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    incident_alert_names: set[str] = set()
    page_token = None
    total_fetched = 0
    total_size = 0
    request_timeout = 200

    for _ in range(50):  # Cap at 50 pages (~1.5M incidents) — ensure nothing missed
        body = {} if page_token is None else {"pagination": {"pageSize": 10000, "pageToken": page_token}}

        try:
            resp = requests.post(
                url, headers=headers, json=body, timeout=request_timeout
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            if logger:
                logger.warning(f"Incidents REST API failed: {e}")
            return None

        incidents = data.get("incidents", [])
        pagination = data.get("pagination", {})
        total_size = pagination.get("totalSize") or (total_fetched + len(incidents))
        total_fetched += len(incidents)

        for inc in incidents:
            created = inc.get("createdAt") or ""
            closed = inc.get("closedAt")
            # Open during window: started before/at end, and (still open or closed after start)
            # Plus: created >= created_min_iso (exclude very old incidents, matches UI ~218)
            if created and created <= end_iso and created >= created_min_iso:
                if closed is None or closed == "" or closed >= start_iso:
                    name = (inc.get("contextualLabels") or {}).get("alert_name")
                    if name:
                        incident_alert_names.add(name)

        if logger:
            logger.element_info(
                f"Incidents: {len(incident_alert_names)} unique alert names (fetched {total_fetched}/{total_size})"
            )

        # Stop when we have all — API may return nextPageToken even when done
        page_token = pagination.get("nextPageToken")
        if not page_token or not incidents or total_fetched >= total_size:
            break

    return incident_alert_names


def _fetch_alerts_via_rest(api_key: str, host: str, logger=None) -> list[dict]:
    """
    Fetch all alert definitions via REST API.
    Returns list of alert def dicts. Handles pagination if needed.
    """
    url = f"https://{host}/mgmt/openapi/latest/alerts/alerts-general/v3"
    headers = {"Authorization": f"Bearer {api_key}"}
    all_alerts = []
    page_token = None

    for _ in range(50):
        params = {}
        if page_token:
            params["pagination"] = json.dumps({"pageToken": page_token})

        try:
            resp = requests.get(
                url,
                headers=headers,
                params=params if params else None,
                timeout=90,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            if logger:
                logger.warning(f"Alerts status REST API failed: {e}")
            return all_alerts

        alerts = data.get("alertDefs", [])
        all_alerts.extend(alerts)

        pagination = data.get("pagination", {})
        page_token = pagination.get("nextPageToken")
        if not page_token or not alerts:
            break

    return all_alerts


class Main:
    def __init__(self, init_obj: Builder):
        self.cx_api_key = init_obj.cx_api_key
        self.code_dir = init_obj.code_dir
        self.sb_logger = init_obj.sb_logger
        self.cx_region = (getattr(init_obj, "cx_region", "") or "").strip().lower() or "eu1"

    def run_check(self):
        if not self.cx_api_key:
            if self.sb_logger:
                self.sb_logger.warning("Alerts status check skipped: cx_api_key not set")
            self._write_output(self._empty_result("cx_api_key not configured"))
            return

        host = get_api_host(self.cx_region)
        alerts = _fetch_alerts_via_rest(self.cx_api_key, host, self.sb_logger)

        # Disabled: enabled is False
        disabled = []
        all_alert_names = set()
        for a in alerts:
            props = a.get("alertDefProperties", {})
            name = props.get("name") or "Unknown"
            enabled = props.get("enabled", True)

            if not enabled:
                disabled.append(name)
            if name and name != "Unknown":
                all_alert_names.add(name)

        # Never triggered: definitions minus alerts that appear in incidents (last 30 days)
        # Try ListIncidentAggregations first (same API as UI "Group by Alert Definition")
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=30)
        created_min_dt = end_dt - timedelta(days=210)
        start_iso = start_dt.isoformat().replace("+00:00", "Z")
        end_iso = end_dt.isoformat().replace("+00:00", "Z")
        created_min_iso = created_min_dt.isoformat().replace("+00:00", "Z")

        incident_alert_names = _fetch_incident_alert_names_via_aggregations(
            self.cx_api_key, host, start_iso, end_iso, self.sb_logger
        )
        if incident_alert_names is None:
            incident_alert_names = _fetch_incident_alert_names_via_rest(
                self.cx_api_key, host, start_iso, end_iso, created_min_iso, self.sb_logger
            )
        incidents_api_failed = incident_alert_names is None
        if incident_alert_names is None:
            incident_alert_names = set()
        never_triggered = sorted(all_alert_names - incident_alert_names)

        result = {
            "alerts_status": {
                "disabled_alerts": disabled,
                "disabled_count": len(disabled),
                "never_triggered_alerts": never_triggered,
                "never_triggered_count": len(never_triggered),
                "total_alert_definitions": len(alerts),
            }
        }
        if incidents_api_failed:
            result["alerts_status"]["error"] = "Incidents API failed (timeout or error) — never-triggered data incomplete"

        if self.sb_logger:
            self.sb_logger.element_info(
                f"Alerts status: {len(disabled)} disabled, {len(never_triggered)} never triggered"
            )

        self._write_output(result)

    def _empty_result(self, error: str) -> dict:
        return {
            "alerts_status": {
                "disabled_alerts": [],
                "disabled_count": 0,
                "never_triggered_alerts": [],
                "never_triggered_count": 0,
                "total_alert_definitions": 0,
                "error": error,
            }
        }

    def _write_output(self, result: dict):
        output_dir = os.path.join(self.code_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")
