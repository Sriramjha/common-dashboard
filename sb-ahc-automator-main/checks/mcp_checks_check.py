"""
Generic MCP-based checks runner.

Reads the `mcp_checks` list from config.yaml.  Each entry can specify:

  query:   A DataPrime expression — executed directly via the MCP `get_logs` tool.
  prompt:  Plain English — an LLM (OpenAI / Anthropic, needs `llm_api_key` in
           config.yaml) is given the prompt plus access to MCP tools and returns
           a structured answer.

Config example (config.yaml):

  mcp_checks:
    - name: cspm_integrated
      output_key: cspm
      lookback_days: 7
      query: >
        source logs
        | filter $d.snowbitData != null
        | groupby $d.snowbit.additionalData.account count() as log_count
        | orderby log_count desc

    - name: my_custom_check
      output_key: my_result
      prompt: >
        Check if there are any ERROR logs from the 'payments' application in the
        last 24 hours and return the count.

  llm_api_key: sk-...   # only needed for prompt-based checks

Each check's result is stored under its `output_key` in output.json.
"""
from __future__ import annotations

import ast
import datetime
import json
import os
import re

import requests
import yaml

from modules.builder import Builder

DEFAULT_LOOKBACK_DAYS = 7
DEFAULT_LIMIT = 100

from modules.region_config import get_mcp_url


# ── MCP helpers ────────────────────────────────────────────────────────────────

def _mcp_post(api_key: str, payload: dict, timeout: int = 60, mcp_url: str = None) -> list[dict]:
    """POST a JSON-RPC request to the MCP SSE endpoint and return content items."""
    url = mcp_url or get_mcp_url("eu1")
    r = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        },
        data=json.dumps(payload),
        timeout=timeout,
    )
    r.raise_for_status()

    items = []
    for line in r.text.split("\n"):
        if line.startswith("data: "):
            try:
                data = json.loads(line[6:])
                items.extend(data.get("result", {}).get("content", []))
            except (json.JSONDecodeError, KeyError):
                pass
    return items


def _parse_mcp_text(raw: str) -> dict:
    """
    The MCP get_logs tool returns a Python-repr string, not JSON.
    Parse it safely with ast.literal_eval and return the dict.
    """
    try:
        return ast.literal_eval(raw)
    except Exception:
        return {}


def _records_to_flat(records: list[dict]) -> list[dict]:
    """
    Flatten each record's user_data (JSON string) into a plain dict.
    Returns a list of dicts, one per record.
    """
    rows = []
    for rec in records:
        ud = rec.get("user_data", "{}")
        if isinstance(ud, str):
            try:
                ud = json.loads(ud)
            except json.JSONDecodeError:
                ud = {}
        rows.append(ud)
    return rows


# ── No-log alerts mode ─────────────────────────────────────────────────────────

def _extract_no_log_alerts(raw: str, triggered_since_ts: int) -> dict:
    """
    Parse the raw MCP list_alert_definitions response (protobuf-like text) using
    regex — the response is not valid JSON or Python literal due to embedded
    multi-line `value: "..."` wrappers in description fields.

    Returns a structured dict with:
      - all_alerts:      list of all "no log" alert definitions
      - triggered:       those whose lastTriggeredTime >= triggered_since_ts
      - not_triggered:   those that have never triggered (lastTriggeredTime == 0)
      - disabled:        those with enabled=False
    """
    # Extract per-alert blocks: each alert starts with 'name': '...'
    # We pull the fields we need individually via regex
    names         = re.findall(r"'name':\s*'([^']+)'", raw)
    enabled_flags = re.findall(r"'enabled':\s*(True|False)", raw)
    last_triggered = re.findall(r"'lastTriggeredTime':\s*(\d+)", raw)

    all_alerts   = []
    triggered    = []
    not_triggered = []
    disabled     = []

    for i, name in enumerate(names):
        enabled = enabled_flags[i].lower() == "true" if i < len(enabled_flags) else True
        lt_ts   = int(last_triggered[i]) if i < len(last_triggered) else 0

        entry = {
            "name":              name,
            "enabled":           enabled,
            "last_triggered_ts": lt_ts,
            "last_triggered":    (
                datetime.datetime.fromtimestamp(lt_ts, tz=datetime.timezone.utc)
                .strftime("%Y-%m-%d %H:%M UTC")
                if lt_ts else "Never"
            ),
        }
        all_alerts.append(entry)

        if not enabled:
            disabled.append(name)
        elif lt_ts == 0:
            not_triggered.append(name)
        elif lt_ts >= triggered_since_ts:
            triggered.append(name)

    return {
        "total":          len(all_alerts),
        "triggered_7d":   triggered,
        "not_triggered":  not_triggered,
        "disabled":       disabled,
        "all_alerts":     all_alerts,
    }


def _fetch_active_apps(api_key: str, logger, mcp_url: str = None) -> set[str]:
    """
    Returns a lowercase set of all applicationname values seen in logs in the last 24 h.
    Uses a single groupby DataPrime query via MCP get_logs.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    start = (now - datetime.timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end   = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {
            "name": "get_logs",
            "arguments": {
                "query": "source logs | groupby $l.applicationname count() as cnt",
                "start_date": start,
                "end_date":   end,
                "limit":      500,
            },
        },
    }
    items = _mcp_post(api_key, payload, timeout=60, mcp_url=mcp_url)
    apps: set[str] = set()
    for item in items:
        if item.get("type") == "text":
            try:
                parsed = ast.literal_eval(item["text"])
            except Exception:
                continue
            for rec in parsed.get("records", []):
                ud = rec.get("user_data", "{}")
                if isinstance(ud, str):
                    try:
                        ud = json.loads(ud)
                    except Exception:
                        continue
                app = ud.get("applicationname")
                if app:
                    apps.add(app.lower().strip())
    if logger:
        logger.element_info(f"Active apps in last 24h: {len(apps)}")
    return apps


def _fetch_alert_app_names(api_key: str, version_id: str, mcp_url: str = None) -> tuple[list[str], bool]:
    """
    Fetches the full alert definition and extracts:
      - app_names: lowercase list of application_name filter values
      - has_lucene: True if the alert has a Lucene query (scopes it beyond app filter)

    Returns (app_names, has_lucene).
    An alert covers ALL apps only when app_names is empty AND has_lucene is False.
    """
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {
            "name": "get_alert_definition",
            "arguments": {"alert_version_id": version_id},
        },
    }
    items = _mcp_post(api_key, payload, timeout=60, mcp_url=mcp_url)
    for item in items:
        if item.get("type") == "text":
            raw = item["text"]
            # Try to get definition from nested structure (Cursor MCP returns dict)
            search_text = raw
            try:
                parsed = json.loads(raw) if raw.strip().startswith("{") else ast.literal_eval(raw)
                if isinstance(parsed, dict):
                    defn = (parsed.get("properties") or {}).get("definition")
                    if isinstance(defn, str):
                        search_text = defn
            except (json.JSONDecodeError, SyntaxError, ValueError):
                pass
            apps = re.findall(
                r'application_name\s*\{[^}]*value\s*\{\s*value:\s*"([^"]+)"',
                search_text,
            )
            if not apps:
                # Fallback: also match single-quoted values (Python repr)
                apps = re.findall(
                    r"application_name\s*\{[^}]*value\s*\{\s*value:\s*'([^']+)'",
                    search_text,
                )
            has_lucene = bool(re.search(r'lucene_query\s*\{', search_text))
            return [a.lower().strip() for a in apps], has_lucene
    return [], False


def _run_no_log_alerts(api_key: str, triggered_lookback_days: int, logger, mcp_url: str = None) -> dict:
    """
    1. Fetches all 'no log' alert definitions and classifies by trigger status.
    2. Fetches active application names from logs (last 24 h).
    3. For each alert, fetches the application filter from its full definition.
    4. Determines which active apps have NO no-log alert covering them.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    triggered_since_ts = int(
        (now - datetime.timedelta(days=triggered_lookback_days)).timestamp()
    )

    # ── Step 1: list all no-log alert definitions ─────────────────────────────
    all_alerts_raw = []
    page_token = None

    for _ in range(20):
        args: dict = {
            "page_size": 50,
            "alert_property_filters": {"nameFilters": ["no log", "no logs", "no-log", "no_log"]},
        }
        if page_token:
            args["page_token"] = page_token

        payload = {
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "list_alert_definitions", "arguments": args},
        }
        items = _mcp_post(api_key, payload, timeout=60, mcp_url=mcp_url)
        raw = ""
        for item in items:
            if item.get("type") == "text":
                raw = item["text"]
                break
        if not raw:
            break
        all_alerts_raw.append(raw)
        next_match    = re.search(r"'nextPageToken':\s*'([^']+)'", raw)
        names_on_page = re.findall(r"'name':\s*'([^']+)'", raw)
        if not next_match or not names_on_page:
            break
        page_token = next_match.group(1)

    combined_raw = "\n".join(all_alerts_raw)
    result = _extract_no_log_alerts(combined_raw, triggered_since_ts)

    # ── Step 2: extract name → versionId mapping ──────────────────────────────
    # Use first page for extraction (MCP may have 'id' etc. between name and versionId)
    extract_raw = all_alerts_raw[0] if all_alerts_raw else combined_raw
    names = re.findall(r"'name':\s*'([^']+)'", extract_raw)
    version_ids = re.findall(r"'versionId':\s*'([^']+)'", extract_raw)
    name_vid_pairs = list(zip(names, version_ids)) if len(names) == len(version_ids) else []

    # ── Step 3: fetch active apps (last 24 h) ─────────────────────────────────
    active_apps = _fetch_active_apps(api_key, logger, mcp_url=mcp_url)

    # ── Step 4: for each enabled alert, get its app filter ────────────────────
    # Build a set of apps that ARE covered by at least one enabled no-log alert.
    # An alert with an empty app filter covers ALL apps.
    covered_apps: set[str] = set()
    all_apps_covered = False  # True if any alert has no app filter (covers everything)

    enabled_names = set(result["triggered_7d"] + result["not_triggered"])  # enabled alerts
    # When enabled count < alert count, raw structure likely misaligns names vs enabled;
    # process all to match MCP direct tool results
    process_names = enabled_names if len(enabled_names) >= len(name_vid_pairs) else {n for n, _ in name_vid_pairs}

    for alert_name, vid in name_vid_pairs:
        if alert_name not in process_names:
            continue  # skip disabled alerts
        app_filters, has_lucene = _fetch_alert_app_names(api_key, vid, mcp_url=mcp_url)
        if not app_filters and not has_lucene:
            # No app filter AND no Lucene scope → truly covers all apps
            all_apps_covered = True
            break
        covered_apps.update(app_filters)

    # Apps to exclude from "no coverage" list (internal/system apps)
    excluded_apps = {"coralogix-alerts", "cx-metrics"}

    def _normalize_for_match(s: str) -> str:
        """Normalize app name for fuzzy matching (e.g. 'aws-network-firewall' matches 'AWS Network Firewall')."""
        return re.sub(r"[\s\-_]+", "", (s or "").lower())

    def _app_is_covered(active_app: str, covered_set: set[str]) -> bool:
        """Check if active app is covered by any alert (exact or fuzzy: alert filter substring of app)."""
        if active_app in covered_set:
            return True
        norm_active = _normalize_for_match(active_app)
        for cov in covered_set:
            norm_cov = _normalize_for_match(cov)
            if norm_cov and (norm_cov in norm_active or norm_active in norm_cov):
                return True
        return False

    if all_apps_covered:
        apps_without_coverage = []
    else:
        apps_without_coverage = sorted(
            app for app in active_apps
            if app not in excluded_apps and not _app_is_covered(app, covered_apps)
        )

    result["apps_without_coverage"] = apps_without_coverage
    result["active_apps_count"]     = len(active_apps)
    # Plain-text output format: one app per line, or "ALL_APPS_COVERED"
    result["plain_text_output"] = "ALL_APPS_COVERED" if not apps_without_coverage else "\n".join(apps_without_coverage)

    if logger:
        logger.element_info(
            f"No-log alerts: {result['total']} found, "
            f"{len(result['triggered_7d'])} triggered in last {triggered_lookback_days}d, "
            f"{len(apps_without_coverage)} apps without coverage"
        )
    return result


# ── Ingestion block alert mode ─────────────────────────────────────────────────

def _run_ingestion_block_alert(api_key: str, logger, mcp_url: str = None) -> dict:
    """
    Checks if a "Data Ingestion Block" alert is created and active.
    
    Uses list_alert_definitions to search for alerts containing "ingestion" and "block".
    """
    # Search for alerts with "ingestion" in name (matches "Ingestion Blocked", "Data Ingestion Block", etc.)
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {
            "name": "list_alert_definitions",
            "arguments": {
                "alert_property_filters": {"nameFilters": ["ingestion"]},
                "page_size": 100,
            },
        },
    }
    
    items = _mcp_post(api_key, payload, timeout=60, mcp_url=mcp_url)
    
    raw = ""
    for item in items:
        if item.get("type") == "text":
            raw = item["text"]
            break
    
    if logger:
        logger.element_info(f"  → Raw response length: {len(raw)} chars")
    
    # Parse alerts using same approach as no_log_alerts
    # MCP response uses Python-style single quotes: 'name': 'value', 'enabled': True/False
    names          = re.findall(r"'name':\s*'([^']+)'", raw)
    enabled_flags  = re.findall(r"'enabled':\s*(True|False)", raw)
    last_triggered = re.findall(r"'lastTriggeredTime':\s*(\d+)", raw)
    priorities     = re.findall(r"'priority':\s*'([^']+)'", raw)
    
    if logger:
        logger.element_info(f"  → Found {len(names)} alert names in response")
    
    # Filter for alerts containing "ingestion" and ("block" or "blocked") in name
    matching_alerts = []
    for i, name in enumerate(names):
        name_lower = name.lower()
        if "ingestion" in name_lower and ("block" in name_lower or "blocked" in name_lower):
            enabled = enabled_flags[i].lower() == "true" if i < len(enabled_flags) else False
            lt_ts   = int(last_triggered[i]) if i < len(last_triggered) else 0
            priority = priorities[i] if i < len(priorities) else "Unknown"
            
            if lt_ts > 0:
                lt_str = datetime.datetime.fromtimestamp(
                    lt_ts, tz=datetime.timezone.utc
                ).strftime("%Y-%m-%d %H:%M UTC")
            else:
                lt_str = "Never"
            
            matching_alerts.append({
                "name": name,
                "enabled": enabled,
                "priority": priority,
                "last_triggered": lt_str,
            })
    
    # Determine status
    alert_exists = len(matching_alerts) > 0
    alert_active = any(a["enabled"] for a in matching_alerts)
    
    if alert_exists and alert_active:
        summary = "Data ingestion block alert is created and active"
    elif alert_exists and not alert_active:
        summary = "Data ingestion block alert exists but is DISABLED"
    else:
        summary = "No data ingestion block alert found - ACTION REQUIRED"
    
    result = {
        "alert_exists": alert_exists,
        "alert_active": alert_active,
        "alerts": matching_alerts,
        "summary": summary,
    }
    
    if logger:
        logger.element_info(
            f"Ingestion block alert: exists={alert_exists}, active={alert_active}, "
            f"found {len(matching_alerts)} matching alert(s)"
        )
    
    return result


# ── Key fields normalized mode ─────────────────────────────────────────────────

def _run_key_fields_normalized(api_key: str, check_config: dict, logger, mcp_url: str = None) -> dict:
    """
    Checks if data sources have normalized fields (cx_security) in Coralogix.
    
    Runs two queries:
      1. Normalized logs per app (logs with cx_security field)
      2. Total logs per app
    
    Compares to find which apps are fully/partially/not normalized.
    """
    lookback_hours = int(check_config.get("lookback_hours", 24))
    now = datetime.datetime.now(datetime.timezone.utc)
    start = (now - datetime.timedelta(hours=lookback_hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end   = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Queries from the prompt
    q_normalized = "source logs | lucene '_exists_:cx_security' | groupby $l.applicationname:string.toLowerCase() as app_name aggregate count() as normalized_count | orderby normalized_count desc"
    q_total      = "source logs | groupby $l.applicationname:string.toLowerCase() as app_name aggregate count() as total_count | orderby total_count desc"

    def run_mcp_query(query: str, limit: int = 1000) -> list[dict]:
        payload = {
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {
                "name": "get_logs",
                "arguments": {
                    "query": query,
                    "start_date": start,
                    "end_date": end,
                    "limit": limit,
                },
            },
        }
        items = _mcp_post(api_key, payload, timeout=90, mcp_url=mcp_url)
        for item in items:
            if item.get("type") == "text":
                parsed = _parse_mcp_text(item["text"])
                return _records_to_flat(parsed.get("records", []))
        return []

    # STEP 1: Get apps with cx_security (normalized) - count > 0 means normalized
    if logger:
        logger.element_info(f"  → STEP 1: {q_normalized}")
    normalized_rows = run_mcp_query(q_normalized)
    normalized_by_app = {}
    for r in normalized_rows:
        app = r.get("app_name") or r.get("applicationname") or "unknown"
        count = int(r.get("normalized_count", 0) or 0)
        if count > 0:
            normalized_by_app[app] = count

    # STEP 2: Get all apps (for list of data sources)
    if logger:
        logger.element_info(f"  → STEP 2: {q_total}")
    total_rows = run_mcp_query(q_total)
    total_by_app = {}
    for r in total_rows:
        app = r.get("app_name") or r.get("applicationname") or "unknown"
        total_by_app[app] = int(r.get("total_count", 0) or 0)

    # Logic: _exists_:cx_security count > 0 = normalized. 0 = not normalized. No comparison with total.
    normalized_list = []
    not_normalized_list = []
    normalized_count = 0
    not_normalized_count = 0

    for app, total in sorted(total_by_app.items(), key=lambda x: -x[1]):
        norm_count = normalized_by_app.get(app, 0)

        if norm_count > 0:
            # Has cx_security logs = normalized
            normalized_count += 1
            normalized_list.append({
                "application": app,
                "normalized_count": norm_count,
                "total_count": total,
                "pct": "100%" if norm_count >= total else f"{(norm_count / total * 100):.1f}%",
            })
        else:
            # 0 results for cx_security = not normalized
            not_normalized_count += 1
            not_normalized_list.append({
                "application": app,
                "total_count": total,
            })

    total_apps = len(total_by_app)
    all_normalized = not_normalized_count == 0

    if not_normalized_count > 0:
        summary = f"{not_normalized_count} data source(s) are NOT normalized and require attention"
    else:
        summary = "All data sources are normalized"

    result = {
        "all_normalized": all_normalized,
        "total_apps": total_apps,
        "fully_normalized_apps": normalized_count,
        "partially_normalized_apps": 0,
        "not_normalized_apps": not_normalized_count,
        "normalized": normalized_list,
        "not_normalized": not_normalized_list,
        "summary": summary,
    }

    if logger:
        logger.element_info(
            f"Key fields normalized: {normalized_count} normalized, "
            f"{not_normalized_count} not normalized out of {total_apps} apps"
        )

    return result


# ── Unparsed logs mode ─────────────────────────────────────────────────────────

def _extract_queries_from_prompt(prompt: str) -> dict:
    """
    Parse the prompt text to extract queries for each STEP.
    Looks for patterns like: Run query: <query>
    """
    import re
    queries = {}
    
    # Extract STEP 1 query (unparsed count)
    m = re.search(r"STEP 1.*?Run query:\s*(.+?)(?:\n|Expected)", prompt, re.DOTALL | re.IGNORECASE)
    if m:
        queries["unparsed_count"] = m.group(1).strip()
    
    # Extract STEP 2 query (parsed count)
    m = re.search(r"STEP 2.*?Run query:\s*(.+?)(?:\n|Expected)", prompt, re.DOTALL | re.IGNORECASE)
    if m:
        queries["parsed_count"] = m.group(1).strip()
    
    # Extract STEP 4 query (unparsed by app)
    m = re.search(r"STEP 4.*?Run query:\s*(.+?)(?:\n|Expected)", prompt, re.DOTALL | re.IGNORECASE)
    if m:
        queries["unparsed_by_app"] = m.group(1).strip()
    
    # Extract STEP 5 query (total by app)
    m = re.search(r"STEP 5.*?Run query:\s*(.+?)(?:\n|Expected)", prompt, re.DOTALL | re.IGNORECASE)
    if m:
        queries["total_by_app"] = m.group(1).strip()
    
    return queries


def _run_unparsed_logs(api_key: str, check_config: dict, logger, mcp_url: str = None) -> dict:
    """
    Uses MCP to run Lucene queries and count parsed vs unparsed logs.
    
    Reads queries from the 'prompt' field in config.yaml:
      STEP 1: Run query: <unparsed count query>
      STEP 2: Run query: <parsed count query>
      STEP 4: Run query: <unparsed by app query>
      STEP 5: Run query: <total by app query>
    
    Returns structured result with per-app percentages.
    """
    lookback_hours = int(check_config.get("lookback_hours", 24))
    now = datetime.datetime.now(datetime.timezone.utc)
    start = (now - datetime.timedelta(hours=lookback_hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end   = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Extract queries from the prompt in config
    prompt = check_config.get("prompt", "")
    queries = _extract_queries_from_prompt(prompt)
    
    # Use extracted queries or fall back to defaults
    q_unparsed_count  = queries.get("unparsed_count", "source logs | lucene '_exists_:text' | count")
    q_parsed_count    = queries.get("parsed_count", "source logs | lucene 'NOT _exists_:text' | count")
    q_unparsed_by_app = queries.get("unparsed_by_app", "source logs | lucene '_exists_:text' | groupby $l.applicationname count() as unparsed_count | orderby unparsed_count desc")
    q_total_by_app    = queries.get("total_by_app", "source logs | groupby $l.applicationname count() as total_count")

    def run_mcp_query(query: str, limit: int = 500) -> list[dict]:
        """Run a query via MCP get_logs."""
        payload = {
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {
                "name": "get_logs",
                "arguments": {
                    "query": query,
                    "start_date": start,
                    "end_date": end,
                    "limit": limit,
                },
            },
        }
        items = _mcp_post(api_key, payload, timeout=90, mcp_url=mcp_url)
        for item in items:
            if item.get("type") == "text":
                parsed = _parse_mcp_text(item["text"])
                return _records_to_flat(parsed.get("records", []))
        return []

    # STEP 1: Count UNPARSED logs using Lucene _exists_:text
    if logger:
        logger.element_info(f"  → STEP 1: {q_unparsed_count}")
    unparsed_count_rows = run_mcp_query(q_unparsed_count)
    total_unparsed = 0
    if unparsed_count_rows:
        total_unparsed = int(unparsed_count_rows[0].get("_count", 0) or 0)
    
    # STEP 2: Count PARSED logs using Lucene NOT _exists_:text
    if logger:
        logger.element_info(f"  → STEP 2: {q_parsed_count}")
    parsed_count_rows = run_mcp_query(q_parsed_count)
    total_parsed = 0
    if parsed_count_rows:
        total_parsed = int(parsed_count_rows[0].get("_count", 0) or 0)
    
    # STEP 3: Calculate totals
    grand_total = total_unparsed + total_parsed

    # STEP 4: Get unparsed logs breakdown by application
    if logger:
        logger.element_info(f"  → STEP 4: {q_unparsed_by_app}")
    unparsed_by_app = run_mcp_query(q_unparsed_by_app, limit=500)
    
    # STEP 5: Get total logs per application for percentage calculation
    if logger:
        logger.element_info(f"  → STEP 5: {q_total_by_app}")
    total_by_app_rows = run_mcp_query(q_total_by_app, limit=1000)
    total_by_app = {}
    for r in total_by_app_rows:
        # Support both "app_name" (from toLowerCase query) and "applicationname" (original)
        name = r.get("app_name") or r.get("applicationname") or "unknown"
        total_by_app[name] = int(r.get("total_count", 0) or 0)

    # STEP 6: Calculate per-app percentage
    apps = []
    for r in unparsed_by_app:
        # Support both "app_name" (from toLowerCase query) and "applicationname" (original)
        app_name = r.get("app_name") or r.get("applicationname") or "unknown"
        app_unparsed = int(r.get("unparsed_count", 0) or 0)
        app_total = total_by_app.get(app_name, app_unparsed)
        
        if app_unparsed > 0:
            apps.append({
                "application":  app_name,
                "count":        app_unparsed,
                "total_count":  app_total,
            })

    result = {
        "all_parsed":      total_unparsed == 0,
        "total_unparsed":  total_unparsed,
        "total_parsed":    total_parsed,
        "total_logs":      grand_total,
        "affected_apps":   len(apps),
        "apps":            apps,
    }

    if logger:
        pct = (total_unparsed / grand_total * 100) if grand_total else 0
        logger.element_info(
            f"Unparsed logs: {total_unparsed}/{grand_total} ({pct:.1f}%) in {len(apps)} apps"
        )
    return result


# ── Query mode ─────────────────────────────────────────────────────────────────

def _run_query(api_key: str, query: str, lookback_days: int, limit: int, logger,
               lookback_hours: int = 0, mcp_url: str = None) -> list[dict]:
    """Execute a DataPrime query via MCP get_logs and return flat row dicts."""
    now = datetime.datetime.now(datetime.timezone.utc)
    if lookback_hours:
        start = (now - datetime.timedelta(hours=lookback_hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        start = (now - datetime.timedelta(days=lookback_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "get_logs",
            "arguments": {
                "query": query.strip(),
                "start_date": start,
                "end_date": end,
                "limit": limit,
            },
        },
    }

    items = _mcp_post(api_key, payload, mcp_url=mcp_url)
    for item in items:
        if item.get("type") == "text":
            parsed = _parse_mcp_text(item["text"])
            records = parsed.get("records", [])
            warnings = parsed.get("dataprime_warnings", [])
            if warnings and logger:
                logger.warning(f"DataPrime warnings: {warnings}")
            return _records_to_flat(records)
    return []


def _run_prompt(api_key: str, llm_api_key: str, prompt: str, lookback_days: int, logger,
                lookback_hours: int = 0, mcp_url: str = None) -> str:
    """
    Send an English prompt to an LLM (OpenAI) that has the MCP get_logs tool
    available.  Returns the LLM's final text answer.

    Requires `llm_api_key` (OpenAI key) in config.yaml.
    """
    try:
        import openai  # type: ignore
    except ImportError:
        raise RuntimeError(
            "openai package is required for prompt-based MCP checks. "
            "Run: pip install openai"
        )

    now = datetime.datetime.now(datetime.timezone.utc)
    if lookback_hours:
        start = (now - datetime.timedelta(hours=lookback_hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        start = (now - datetime.timedelta(days=lookback_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Describe the get_logs tool to the LLM
    tools = [
        {
            "type": "function",
            "function": {
                "name": "get_logs",
                "description": (
                    "Query Coralogix logs using DataPrime syntax. "
                    "Use $d for user data fields, $l for labels, $m for metadata. "
                    "String literals must use single quotes."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query":      {"type": "string", "description": "DataPrime query"},
                        "start_date": {"type": "string", "description": "ISO 8601 start"},
                        "end_date":   {"type": "string", "description": "ISO 8601 end"},
                        "limit":      {"type": "integer", "default": 100},
                    },
                    "required": ["query", "start_date", "end_date"],
                },
            },
        }
    ]

    system = (
        "You are a Coralogix log analysis assistant. "
        "Use the get_logs tool to answer the user's question. "
        f"The default time range is {start} to {end}. "
        "Return a concise, factual answer — no markdown, no preamble."
    )

    client = openai.OpenAI(api_key=llm_api_key)
    messages = [
        {"role": "system", "content": system},
        {"role": "user",   "content": prompt.strip()},
    ]

    # Agentic loop: LLM may call get_logs multiple times
    for _ in range(5):
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            tools=tools,
            tool_choice="auto",
        )
        msg = resp.choices[0].message

        if msg.tool_calls:
            messages.append(msg)
            for tc in msg.tool_calls:
                args = json.loads(tc.function.arguments)
                args.setdefault("start_date", start)
                args.setdefault("end_date", end)
                args.setdefault("limit", 100)

                if logger:
                    logger.element_info(f"LLM calling get_logs: {args.get('query', '')[:80]}")

                rows = _run_query(api_key, args["query"], lookback_days, args["limit"], logger, mcp_url=mcp_url)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": json.dumps(rows),
                })
        else:
            return msg.content or ""

    return "LLM did not produce a final answer within the allowed steps."


# ── Main check class ───────────────────────────────────────────────────────────

class Main:
    def __init__(self, init_obj: Builder):
        self.cx_api_key   = init_obj.cx_api_key
        self.sb_logger    = init_obj.sb_logger
        self.code_dir     = init_obj.code_dir
        self.region       = (init_obj.cx_region or "EU1").upper()
        self.mcp_url      = get_mcp_url(self.region)
        # Use mcp_checks from builder (passed from AHCRunner) or fall back to config file
        self._mcp_checks  = init_obj.mcp_checks if hasattr(init_obj, 'mcp_checks') and init_obj.mcp_checks else None
        self._cfg         = self._load_config() if self._mcp_checks is None else {}

    def _load_config(self) -> dict:
        """Load config from file (fallback for backward compatibility)."""
        config_path = os.path.join(self.code_dir, "config.yaml")
        if os.path.exists(config_path):
            with open(config_path) as f:
                return yaml.safe_load(f) or {}
        return {}

    def run_check(self):
        # Use mcp_checks from builder or config file
        mcp_checks = self._mcp_checks or self._cfg.get("mcp_checks") or []
        if not mcp_checks:
            self.sb_logger.warning("mcp_checks: no checks configured")
            return

        self.sb_logger.element_info(f"Using MCP URL for region {self.region}: {self.mcp_url}")
        
        llm_api_key = self._cfg.get("llm_api_key")
        result = {}

        for check in mcp_checks:
            name          = check.get("name", "unnamed")
            output_key    = check.get("output_key", name)
            check_type    = check.get("type", "")
            lookback      = int(check.get("lookback_days", DEFAULT_LOOKBACK_DAYS))
            lookback_hrs  = int(check.get("lookback_hours", 0))
            limit         = int(check.get("limit", DEFAULT_LIMIT))
            query         = check.get("query")
            prompt        = check.get("prompt")

            self.sb_logger.element_info(f"MCP check: {name}")

            try:
                if check_type == "no_log_alerts":
                    triggered_days = int(check.get("triggered_lookback_days", 7))
                    result[output_key] = _run_no_log_alerts(
                        self.cx_api_key, triggered_days, self.sb_logger, mcp_url=self.mcp_url
                    )

                elif check_type == "unparsed_logs":
                    result[output_key] = _run_unparsed_logs(
                        self.cx_api_key, check, self.sb_logger, mcp_url=self.mcp_url
                    )

                elif check_type == "ingestion_block_alert":
                    result[output_key] = _run_ingestion_block_alert(
                        self.cx_api_key, self.sb_logger, mcp_url=self.mcp_url
                    )

                elif check_type == "key_fields_normalized":
                    result[output_key] = _run_key_fields_normalized(
                        self.cx_api_key, check, self.sb_logger, mcp_url=self.mcp_url
                    )

                elif query:
                    rows = _run_query(
                        self.cx_api_key, query, lookback, limit, self.sb_logger,
                        lookback_hours=lookback_hrs, mcp_url=self.mcp_url,
                    )
                    result[output_key] = self._summarise_query(name, rows)

                elif prompt:
                    if not llm_api_key:
                        self.sb_logger.warning(
                            f"MCP check '{name}' uses a prompt but no llm_api_key is set in config.yaml — skipping"
                        )
                        result[output_key] = {"error": "llm_api_key not configured"}
                        continue

                    answer = _run_prompt(
                        self.cx_api_key, llm_api_key, prompt, lookback, self.sb_logger,
                        lookback_hours=lookback_hrs, mcp_url=self.mcp_url,
                    )
                    result[output_key] = {"answer": answer}

                else:
                    self.sb_logger.warning(f"MCP check '{name}' has neither query nor prompt — skipping")

            except Exception as e:
                self.sb_logger.warning(f"MCP check '{name}' failed: {e}")
                result[output_key] = {"error": str(e)}

        output_dir = os.path.join(self.code_dir, "output")
        with open(os.path.join(output_dir, "output.json"), "a") as f:
            f.write(json.dumps(result, indent=2, default=str) + "\n")

        self.sb_logger.element_info("MCP checks completed")

    @staticmethod
    def _summarise_query(name: str, rows: list[dict]) -> dict:
        """
        Build a sensible summary dict from raw DataPrime rows.

        For the built-in cspm check we detect snowbit accounts.
        For everything else we return the raw rows so nothing is lost.
        """
        if not rows:
            return {"found": False, "total_rows": 0, "rows": []}

        # ── CSPM: detect cloud accounts and providers ─────────────────────────
        if "cspm" in name.lower():
            accounts_by_provider = {"aws": [], "azure": [], "gcp": [], "unknown": []}
            all_accounts = []
            
            for row in rows:
                acct = (
                    row.get("snowbit", {})
                       .get("additionalData", {})
                       .get("account")
                )
                if acct:
                    acct_str = str(acct)
                    all_accounts.append(acct_str)
                    
                    # Detect provider based on account ID format
                    # AWS: numeric (e.g., 243629380105)
                    # Azure: UUID format (e.g., b31bbc50-136c-48b9-...)
                    # GCP: project ID format (alphanumeric with dashes)
                    if acct_str.isdigit() and len(acct_str) == 12:
                        accounts_by_provider["aws"].append(acct_str)
                    elif "-" in acct_str and len(acct_str) == 36:
                        # UUID format - likely Azure
                        accounts_by_provider["azure"].append(acct_str)
                    elif "-" in acct_str or acct_str.replace("-", "").replace("_", "").isalnum():
                        # GCP project IDs can have dashes/underscores
                        accounts_by_provider["gcp"].append(acct_str)
                    else:
                        accounts_by_provider["unknown"].append(acct_str)
            
            # Build providers summary
            providers = []
            if accounts_by_provider["aws"]:
                providers.append({"provider": "AWS", "count": len(accounts_by_provider["aws"]), "accounts": accounts_by_provider["aws"]})
            if accounts_by_provider["azure"]:
                providers.append({"provider": "Azure", "count": len(accounts_by_provider["azure"]), "accounts": accounts_by_provider["azure"]})
            if accounts_by_provider["gcp"]:
                providers.append({"provider": "GCP", "count": len(accounts_by_provider["gcp"]), "accounts": accounts_by_provider["gcp"]})
            if accounts_by_provider["unknown"]:
                providers.append({"provider": "Unknown", "count": len(accounts_by_provider["unknown"]), "accounts": accounts_by_provider["unknown"]})
            
            return {
                "integrated":     len(all_accounts) > 0,
                "total_accounts": len(all_accounts),
                "accounts":       all_accounts,
                "providers":      providers,
            }

        # ── Unparsed logs: group by application ──────────────────────────────
        if "unparsed" in name.lower():
            total = sum(row.get("unparsed_count", 0) for row in rows)
            apps  = [
                {"application": row.get("applicationname", "unknown"),
                 "count": row.get("unparsed_count", 0)}
                for row in rows
            ]
            return {
                "all_parsed":        False,
                "total_unparsed":    total,
                "affected_apps":     len(apps),
                "apps":              apps,
            }

        # ── Generic: return rows as-is ────────────────────────────────────────
        return {
            "found":      True,
            "total_rows": len(rows),
            "rows":       rows,
        }
