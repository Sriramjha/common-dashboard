#!/usr/bin/env python3
"""
refresh.py — Coralogix Dashboard Data Refresher
================================================
Fetches live data from Coralogix REST APIs server-side and writes a single
data.json file. The dashboard HTML reads data.json locally — the API key
NEVER reaches the browser, DevTools, or any external party.

Also merges the latest ``AHC_*_output.json`` from ``sb-ahc-automator-main/output/``
into ``data.json["ahc"]`` when that file exists (same logic as
``merge_ahc_into_data_json.py``). If no AHC output is present, the previous
``ahc`` block in data.json is kept unchanged.

Security guarantees:
  ✅ API key read only from .env (never hardcoded)
  ✅ All HTTP calls are HTTPS-only
  ✅ data.json contains NO credentials — only response data
  ✅ data.json is gitignored (never committed)
  ✅ Script refuses to run if API key is missing

Usage:
    python3 refresh.py                          # refresh all sections → data.json (root .env)
    python3 refresh.py --account client_acme      # use accounts/manifest.json + secrets for that id
    python3 refresh.py --section alerts         # refresh one section only
    python3 refresh.py --section alerts webhooks extensions
    python3 refresh.py --dry-run                # fetch but print, don't write

    Optional environment (see .env.example):
    CORALOGIX_INCIDENTS_METRICS_CORRELATION_BATCH — cx_alerts: query alert names in batches of N (default 40) to avoid Prometheus series limits
    CORALOGIX_CX_ALERTS_TIMELINE_RANGE_BATCH_DAYS — metrics query_range chunk size for 30d hygiene timeline (default 10)
    CORALOGIX_ALERT_QUERY_FIELD_VALIDATE_CONCURRENCY — parallel DataPrime field checks when validation enabled (default 3)
    CORALOGIX_INCIDENTS_METRICS_HTTP_TIMEOUT — seconds per cx_alerts PromQL HTTP call (default 120)
    MONDAY_FILTER_GROUP_NAMES, MONDAY_GROUP_TITLE_CONTAINS, MONDAY_GROUP_TITLE_EXCLUDE, MONDAY_PARENT_ITEM_CONTAINS, MONDAY_STATUS_VALUES — Monday row filters

Sections covered:
    integrations   GET /integrations/integrations/v1
    extensions     GET /integrations/extensions/v1/deployed
    webhooks       GET /integrations/webhooks/v1
    saml           GET /aaa/team-saml/v1/configuration  (needs saml:ReadConfig perm)
    ip_access      GET /aaa/team-sec-ip-access/v1
    enrichments    GET /enrichment-rules/enrichment-rules/v1
    folders        GET /dashboards/dashboards/v1/folders
    tco_policies   GET /dataplans/policies/v1
    log_ingestion  GET /dataplans/data-usage/v2  (by application; **daily averages** = window sum ÷ window_days)
    query_performance  POST /api/v1/dataprime/query  (C4C EU2 default; CORALOGIX_QUERY_PERF_DAYS — frequent tier clamped to ~15d, use TIER_ARCHIVE for longer)
    audit_active_users POST /api/v1/dataprime/query  (per-account **audit** API key; countby actor username — last 30d default; host from CORALOGIX_API_BASE domain)
    alerts         GET /api/v2/external/alerts  (items[]; lastTriggered from API or derived from incidents in same run)
    incidents      GET …/metrics/api/v1/query on **cx_alerts** (Security Alerts Summary parity; no REST Incidents API).

    Derived (not a --section): src_customer — YES if some outbound webhook name contains both whole words SRC and Orchestrator (e.g. SRC | Orchestrator); then P1/P2/P3 enabled defs missing that webhook id in alert JSON.
    data_plan_units_per_day — GET …/metrics/api/v1/query PromQL sum(cx_data_plan_units_per_day) for Customer info quota (same CORALOGIX_METRICS_QUERY_BASE as incidents).

    Optional — Monday.com (merged into Security Data Sources in data.json):
      MONDAY_API_TOKEN, MONDAY_BOARD_ID  →  GraphQL https://api.monday.com/v2

    Optional — alert query field validation (DataPrime ``_exists_`` vs logs, last N days):
      CORALOGIX_ALERT_QUERY_FIELD_VALIDATE=1 — enable; strips ``.numeric`` / ``.keyword`` for checks

Requirements: Python 3.9+  (stdlib only — no pip installs needed)
"""

import os
import re
import sys
import json
import time
import unicodedata
import argparse
import urllib.request
import urllib.error
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode, urlparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from snapshot_atomic_write import atomic_write_text

# ── Load .env ─────────────────────────────────────────────────────────────────

def _load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def _load_env_overwrite(path: Path) -> None:
    """Set every KEY= from file into os.environ (later files override)."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k:
            os.environ[k] = v


def _env_file_assigns_key(path: Path, key: str) -> bool:
    """True if this .env file contains a non-comment KEY= line for ``key``."""
    if not path.exists() or not key.strip():
        return False
    want = key.strip()
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, _, _ = line.partition("=")
        if k.strip() == want:
            return True
    return False


# Region code → External API base URL (same as Coralogix docs / .env.example)
_CORALOGIX_REGION_EXTERNAL: Dict[str, str] = {
    "EU1": "https://api.eu1.coralogix.com/api/v2/external",
    "EU2": "https://api.eu2.coralogix.com/api/v2/external",
    "US1": "https://api.coralogix.us/api/v2/external",
    "US2": "https://api.cx498.coralogix.com/api/v2/external",
    "AP1": "https://api.ap1.coralogix.com/api/v2/external",
    "AP2": "https://api.ap2.coralogix.com/api/v2/external",
}


def _apply_coralogix_region_to_env() -> None:
    """If CORALOGIX_REGION is set (EU1, US1, …), set CORALOGIX_API_BASE automatically."""
    r = (os.environ.get("CORALOGIX_REGION") or "").strip().upper()
    if r in _CORALOGIX_REGION_EXTERNAL:
        os.environ["CORALOGIX_API_BASE"] = _CORALOGIX_REGION_EXTERNAL[r]


def _mgmt_base_from_external_url(alerts_base: str) -> str:
    """Data usage / integrations mgmt OpenAPI host matches the tenant External API host."""
    try:
        h = urlparse((alerts_base or "").strip()).hostname
        if h:
            return f"https://{h}/mgmt/openapi/latest"
    except Exception:
        pass
    return "https://api.eu1.coralogix.com/mgmt/openapi/latest"


def _region_guess_from_api_base(base: str) -> str:
    h = (urlparse((base or "").strip()).hostname or "").lower()
    if not h:
        return ""
    if "cx498" in h:
        return "US2"
    m = re.match(r"^api\.([a-z]{2}\d+)\.coralogix\.com$", h)
    if m:
        return m.group(1).upper()
    if "coralogix.us" in h:
        return "US1"
    if "ap2" in h:
        return "AP2"
    if "ap1" in h:
        return "AP1"
    if "eu2" in h:
        return "EU2"
    if "eu1" in h:
        return "EU1"
    return ""


def _finalize_coralogix_region_meta(results: Dict[str, Any], alerts_base: str) -> str:
    """Prefer region implied by Data Usage host, then External API base, then env."""
    li = results.get("log_ingestion") or {}
    host = (li.get("api_host") or "").strip().lower()
    if host:
        r = _region_guess_from_api_base(f"https://{host}/api/v2/external")
        if r:
            return r
    r = _region_guess_from_api_base(alerts_base)
    if r:
        return r
    r = (os.environ.get("CORALOGIX_REGION") or "").strip().upper()
    return r or "—"


_load_env(Path(__file__).parent / ".env")

API_KEY: str = ""
ALERTS_BASE: str = ""
MGMT_BASE: str = ""
DATA_FILE = Path(__file__).parent / "data.json"


def _sync_api_globals() -> None:
    """Re-read globals after switching account env files."""
    global API_KEY, ALERTS_BASE, MGMT_BASE
    _apply_coralogix_region_to_env()
    API_KEY = os.environ.get("CORALOGIX_API_KEY", "").strip()
    ALERTS_BASE = os.environ.get("CORALOGIX_API_BASE", "https://api.eu1.coralogix.com/api/v2/external").strip()
    MGMT_BASE = _mgmt_base_from_external_url(ALERTS_BASE)


_sync_api_globals()


def _configure_account_environment(account_id: str) -> Path:
    """Load secrets for manifest account; return absolute data JSON path."""
    from accounts_config import (
        account_by_id,
        account_data_path,
        account_secrets_path,
        load_manifest,
        validate_account_id,
    )

    if not validate_account_id(account_id):
        print(f"ERROR: invalid account id {account_id!r}", file=sys.stderr)
        sys.exit(1)
    man = load_manifest()
    acc = account_by_id(man, account_id)
    if not acc:
        print(
            f"ERROR: account {account_id!r} not in accounts/manifest.json — copy accounts/manifest.example.json",
            file=sys.stderr,
        )
        sys.exit(1)
    root = Path(__file__).parent
    base_env = root / ".env"
    if base_env.exists():
        _load_env_overwrite(base_env)
    sec_path = account_secrets_path(acc)
    try:
        data_path = account_data_path(acc)
    except ValueError as e:
        print(f"ERROR: manifest dataFile: {e}", file=sys.stderr)
        sys.exit(1)
    if sec_path.resolve() != base_env.resolve() and sec_path.exists():
        _load_env_overwrite(sec_path)
    elif not sec_path.exists() and str(acc.get("secretsFile") or "").strip() not in ("", ".env"):
        print(f"  ⚠  secrets file missing: {sec_path} — using env from .env only", file=sys.stderr)
    # Per-account region: explicit CORALOGIX_REGION in this account's secrets wins; else manifest
    # coralogixRegion overrides root .env (so EU1 in root .env + US1 in manifest → US1 for DevRev).
    mr = str(acc.get("coralogixRegion") or acc.get("coralogix_region") or "").strip().upper()
    secrets_explicit_region = sec_path.exists() and _env_file_assigns_key(sec_path, "CORALOGIX_REGION")
    if not secrets_explicit_region and mr in _CORALOGIX_REGION_EXTERNAL:
        os.environ["CORALOGIX_REGION"] = mr
    _sync_api_globals()
    if not API_KEY:
        print(
            f"ERROR: CORALOGIX_API_KEY not set after loading account {account_id!r} (.env / secrets file)",
            file=sys.stderr,
        )
        sys.exit(1)
    if not ALERTS_BASE.startswith("https://"):
        print("ERROR: CORALOGIX_API_BASE must be HTTPS", file=sys.stderr)
        sys.exit(1)
    return data_path


# Safety check — mgmt base is fixed in this project (data usage / integrations)
assert MGMT_BASE.startswith("https://"), "MGMT base must be HTTPS"


def _mgmt_api_hostname() -> str:
    """Hostname for Data usage / mgmt calls (e.g. api.eu1.coralogix.com) — for dashboard labels."""
    try:
        return (urlparse(MGMT_BASE).hostname or "").strip().lower()
    except Exception:
        return ""


# ── Secure HTTP helper ────────────────────────────────────────────────────────

def _get(url: str, timeout: int = 30) -> Any:
    """
    Authenticated HTTPS GET. API key is sent as a Bearer token in the
    Authorization header — it stays server-side and never touches the browser.
    Raises RuntimeError on HTTP errors (key is NOT included in the message).
    """
    assert url.startswith("https://"), f"Refusing non-HTTPS request: {url}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {API_KEY}",
        "Accept":        "application/json",
        "User-Agent":    "coralogix-dashboard-refresh/1.0",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        body = ""
        if e.fp:
            try:
                body = e.read().decode()[:200]
            except Exception:
                pass
        # Deliberately omit the URL (may contain query params) from error msg
        raise RuntimeError(f"HTTP {e.code}: {body}")


def _post_json(url: str, payload: Any, timeout: int = 60) -> Any:
    """POST JSON body. API key stays in Authorization header only."""
    assert url.startswith("https://"), f"Refusing non-HTTPS request: {url}"
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type":  "application/json",
            "Accept":        "application/json",
            "User-Agent":    "coralogix-dashboard-refresh/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        err_body = ""
        if e.fp:
            try:
                err_body = e.read().decode()[:300]
            except Exception:
                pass
        raise RuntimeError(f"HTTP {e.code}: {err_body}")


def _iso_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _looks_like_field_path(s: str) -> bool:
    """True if value looks like a schema key (e.g. process.pod.name), not a human app name."""
    s = (s or "").strip()
    if not s:
        return True
    if " " in s or "/" in s:
        return False
    if "." in s and s == s.lower():
        return True
    return False


_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.I,
)


def _is_uuid_like(s: str) -> bool:
    return bool((s or "").strip() and _UUID_RE.match((s or "").strip()))


def fetch_incidents() -> Dict:
    """
    Security alert activity from **cx_alerts** Prometheus metrics (Security Alerts Summary parity).
    The REST Incidents API is not used.
    """
    return _fetch_incidents_from_cx_alerts_metrics()


def _incidents_metrics_topk_limit() -> int:
    try:
        return max(5, min(200, int(os.environ.get("CORALOGIX_INCIDENTS_METRICS_TOPK", "30"))))
    except ValueError:
        return 30


def _incidents_prometheus_http_timeout() -> int:
    """Per-request timeout for cx_alerts instant/range queries (heavy tenants need 60s+)."""
    try:
        return max(30, min(300, int(os.environ.get("CORALOGIX_INCIDENTS_METRICS_HTTP_TIMEOUT", "120"))))
    except ValueError:
        return 120


def _fetch_incidents_from_cx_alerts_metrics() -> Dict[str, Any]:
    """
    Security alert activity from cx_alerts (same matchers / sum_over_time pattern as Grafana
    Security Alerts Summary). Produces KPI totals, by_priority, by extension pack, and
    synthetic ``items`` (top definitions by fire count in the window — not REST incident rows).
    """
    end_utc = datetime.now(timezone.utc)
    start_utc = end_utc - timedelta(hours=24)
    w_start = _iso_z(start_utc)
    w_end = _iso_z(end_utc)
    rng = _incidents_promql_range()
    matchers = _cx_alerts_security_matchers_promql()
    sel = f"cx_alerts{{{matchers}}}"
    sel_p1 = f"cx_alerts{{{matchers}{_incidents_p1_promql_suffix()}}}"
    sel_p2 = f"cx_alerts{{{matchers}{_incidents_p2_promql_suffix()}}}"
    topk_n = _incidents_metrics_topk_limit()

    q_total = f"sum(sum_over_time({sel}[{rng}]))"
    q_p1 = f"sum(sum_over_time({sel_p1}[{rng}]))"
    q_p2 = f"sum(sum_over_time({sel_p2}[{rng}]))"
    q_ext = f"sum(sum_over_time(({sel}[{rng}]))) by (alert_def_label_alert_extension_pack)"
    q_pri_d = f"sum(sum_over_time(({sel}[{rng}]))) by (alert_def_priority)"
    q_pri_l = f"sum(sum_over_time(({sel}[{rng}]))) by (alert_def_label_priority)"
    q_top = f"topk({topk_n}, sum(sum_over_time(({sel}[{rng}]))) by (alert_def_name, alert_def_priority))"

    stub: Dict[str, Any] = {
        "enabled":           True,
        "range":             rng,
        "open_total_source": "prometheus_cx_alerts",
        "error":             None,
        "queries": {
            "total":             q_total,
            "p1":                q_p1,
            "p2":                q_p2,
            "by_extension_pack": q_ext,
            "by_priority_def":   q_pri_d,
            "by_priority_label": q_pri_l,
            "top_definitions":   q_top,
        },
    }

    origin = _metrics_query_origin()
    if not origin.startswith("https://"):
        stub["error"] = "CORALOGIX_METRICS_QUERY_BASE invalid or unset — cannot query cx_alerts"
        return {
            "count":             0,
            "p1_count":          0,
            "p2_count":          0,
            "items":             [],
            "window_start":      w_start,
            "window_end":        w_end,
            "open_total_source": "prometheus_cx_alerts",
            "error":             stub["error"],
            "metrics":           stub,
            "strategy":          "cx_alerts_metrics",
            "note":              stub["error"],
        }

    to = _incidents_prometheus_http_timeout()
    try:
        jobs = [
            ("total", q_total),
            ("p1", q_p1),
            ("p2", q_p2),
            ("ext", q_ext),
            ("pri_d", q_pri_d),
            ("pri_l", q_pri_l),
            ("top", q_top),
        ]
        out: Dict[str, Dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=7) as pool:
            futs = {
                pool.submit(_prometheus_instant_query, q, to): name
                for name, q in jobs
            }
            for fut in as_completed(futs):
                name = futs[fut]
                out[name] = fut.result()
        r_total = out["total"]
        r_p1 = out["p1"]
        r_p2 = out["p2"]
        r_ext = out["ext"]
        r_pri_d = out["pri_d"]
        r_pri_l = out["pri_l"]
        r_top = out["top"]
    except Exception as e:
        stub["error"] = str(e)[:500]
        print(f"  ⚠  incidents (cx_alerts): {stub['error'][:120]}")
        return {
            "count":             0,
            "p1_count":          0,
            "p2_count":          0,
            "items":             [],
            "window_start":      w_start,
            "window_end":        w_end,
            "open_total_source": "prometheus_cx_alerts",
            "error":             stub["error"],
            "metrics":           stub,
            "strategy":          "cx_alerts_metrics",
            "note":              "cx_alerts metrics query failed — check metrics API key and CORALOGIX_METRICS_QUERY_BASE.",
        }

    total = _prometheus_first_scalar_value(r_total)
    p1v = _prometheus_first_scalar_value(r_p1)
    p2v = _prometheus_first_scalar_value(r_p2)
    if total is None:
        stub["error"] = "unexpected metrics response (total)"
        print("  ⚠  incidents (cx_alerts): could not parse total")
        return {
            "count":             0,
            "p1_count":          0,
            "p2_count":          0,
            "items":             [],
            "window_start":      w_start,
            "window_end":        w_end,
            "open_total_source": "prometheus_cx_alerts",
            "error":             stub["error"],
            "metrics":           stub,
            "strategy":          "cx_alerts_metrics",
            "note":              stub["error"],
        }
    if p1v is None:
        p1v = 0.0
    if p2v is None:
        p2v = 0.0

    by_ext = _prometheus_vector_label_floats(r_ext, "alert_def_label_alert_extension_pack")
    m_pri_d = _prometheus_vector_label_floats(r_pri_d, "alert_def_priority")
    m_pri_l = _prometheus_vector_label_floats(r_pri_l, "alert_def_label_priority")
    order = ("P1", "P2", "P3", "P4", "P5")
    by_pri: Dict[str, int] = {p: 0 for p in order}
    for k, v in m_pri_d.items():
        pk = str(k).strip().upper()
        if pk in order:
            by_pri[pk] += int(round(v))
    if sum(by_pri.values()) == 0:
        for p in order:
            by_pri[p] = 0
        for k, v in m_pri_l.items():
            pk = str(k).strip().upper()
            if pk in order:
                by_pri[pk] += int(round(v))

    labeled = _prometheus_vector_labeled_values(r_top)
    items: List[Dict[str, Any]] = []
    sev_for_pri = {"P1": "critical", "P2": "error", "P3": "warning", "P4": "info", "P5": "info"}
    for m, fv in labeled:
        nm = str(m.get("alert_def_name") or "").strip() or "Unknown"
        pr = str(m.get("alert_def_priority") or "P5").strip().upper()
        if pr not in order:
            pr = "P5"
        cnt = int(round(fv))
        items.append({
            "name":                 nm[:500],
            "alertRuleName":        nm[:500],
            "alertDefinitionName":  nm[:500],
            "type":                 "Standard",
            "severity":             sev_for_pri.get(pr, "info"),
            "priority":             pr,
            "status":               "metrics_window",
            "source":               "cx-alerts",
            "logSource":            "",
            "user":                 "—",
            "created":              w_end,
            "updated":              w_end,
            "mitre":                "",
            "id":                   "",
            "sourceAlertId":        "",
            "metricFireCount":      cnt,
        })
    items.sort(key=lambda x: -int(x.get("metricFireCount") or 0))

    total_i = int(round(total))
    stub["total"] = float(total)
    stub["p1_total"] = float(p1v)
    stub["p2_total"] = float(p2v)
    stub["by_extension_pack"] = by_ext
    stub["by_priority"] = dict(by_pri)

    print(
        f"  ℹ  incidents (cx_alerts): total={total_i} P1={int(round(p1v))} P2={int(round(p2v))} "
        f"({rng}, top {len(items)} defs)"
    )

    return {
        "count":                     total_i,
        "p1_count":                  int(round(p1v)),
        "p2_count":                  int(round(p2v)),
        "items":                     items[:400],
        "window_start":              w_start,
        "window_end":                w_end,
        "open_total_source":         "prometheus_cx_alerts",
        "error":                     None,
        "by_priority":               dict(by_pri),
        "by_extension_pack_metrics": by_ext,
        "metrics":                   stub,
        "strategy":                  "cx_alerts_metrics",
        "note": (
            "Totals and by_priority from cx_alerts Prometheus metrics (Security Alerts Summary parity). "
            "items[] are the top firing definitions in the window (not individual REST incidents)."
        ),
    }


def _prometheus_matrix_series_sums(resp: Dict[str, Any]) -> List[Tuple[int, float]]:
    """Matrix response → sorted [(unix_ts, value)] with values summed across all series at each timestamp."""
    out_ts: Dict[int, float] = {}
    if not isinstance(resp, dict) or resp.get("status") != "success":
        return []
    data = resp.get("data") or {}
    if data.get("resultType") != "matrix":
        return []
    for row in data.get("result") or []:
        if not isinstance(row, dict):
            continue
        for pt in row.get("values") or []:
            if not isinstance(pt, list) or len(pt) < 2:
                continue
            try:
                ts = int(float(pt[0]))
                v = float(pt[1])
            except (TypeError, ValueError):
                continue
            out_ts[ts] = out_ts.get(ts, 0.0) + v
    return sorted(out_ts.items(), key=lambda x: x[0])


def _correlation_rows_from_cx_alerts_by_definition(
    days: int,
    definition_names: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Correlation rows with alertDefinitionName / alertRuleName for never-triggered and hygiene
    name matching. Fire activity is derived from cx_alerts Prometheus series, not REST incidents.

    When ``definition_names`` is set (from alerts.items), PromQL runs in batches of
    CORALOGIX_INCIDENTS_METRICS_CORRELATION_BATCH so Prometheus does not truncate
    high-cardinality ``by (alert_def_name,…)`` results.
    """
    end_utc = datetime.now(timezone.utc)
    start_utc = end_utc - timedelta(days=days)
    w_start = _iso_z(start_utc)
    w_end = _iso_z(end_utc)
    rng = f"{max(1, min(366, days))}d"
    matchers = _cx_alerts_security_matchers_promql()
    base_sel = f"cx_alerts{{{matchers}}}"

    if not _metrics_query_origin().startswith("https://"):
        return {
            "count":        0,
            "rows":         [],
            "window_start": w_start,
            "window_end":   w_end,
            "window_days":  days,
            "strategy":     "cx_alerts_metrics_by_definition",
            "error":        "metrics origin not configured",
            "truncated":    False,
            "note":         "",
        }

    to = _incidents_prometheus_http_timeout()
    order = ("P1", "P2", "P3", "P4", "P5")
    rows_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    err_last: Optional[str] = None
    batch_queries = 0

    names = [str(n).strip() for n in (definition_names or []) if str(n).strip()]
    if names:
        bs = _metrics_correlation_batch_size()
        for i in range(0, len(names), bs):
            chunk = names[i : i + bs]
            name_m = _promql_matcher_alert_def_names(chunk)
            if not name_m:
                continue
            sel = f"cx_alerts{{{matchers},{name_m}}}"
            q = f"sum(sum_over_time(({sel}[{rng}]))) by (alert_def_name, alert_def_priority)"
            batch_queries += 1
            try:
                resp = _prometheus_instant_query(q, timeout=to)
            except RuntimeError as e:
                err_last = str(e)[:500]
                continue
            for m, fv in _prometheus_vector_labeled_values(resp):
                nm = str(m.get("alert_def_name") or "").strip()
                if not nm or fv <= 0:
                    continue
                pr = str(m.get("alert_def_priority") or "P5").strip().upper()
                if pr not in order:
                    pr = "P5"
                key = (nm.casefold(), pr)
                rows_by_key[key] = {
                    "name":                nm[:500],
                    "alertRuleName":       nm[:500],
                    "alertDefinitionName": nm[:500],
                    "priority":            pr,
                    "sourceAlertId":       "",
                    "created":             w_end,
                }
        rows: List[Dict[str, Any]] = list(rows_by_key.values())
    else:
        q = f"sum(sum_over_time(({base_sel}[{rng}]))) by (alert_def_name, alert_def_priority)"
        batch_queries = 1
        try:
            resp = _prometheus_instant_query(q, timeout=to)
        except RuntimeError as e:
            return {
                "count":        0,
                "rows":         [],
                "window_start": w_start,
                "window_end":   w_end,
                "window_days":  days,
                "strategy":     "cx_alerts_metrics_by_definition",
                "error":        str(e)[:500],
                "truncated":    False,
                "note":         "",
            }
        rows = []
        for m, fv in _prometheus_vector_labeled_values(resp):
            nm = str(m.get("alert_def_name") or "").strip()
            if not nm or fv <= 0:
                continue
            pr = str(m.get("alert_def_priority") or "P5").strip().upper()
            if pr not in order:
                pr = "P5"
            rows.append({
                "name":                nm[:500],
                "alertRuleName":       nm[:500],
                "alertDefinitionName": nm[:500],
                "priority":            pr,
                "sourceAlertId":       "",
                "created":             w_end,
            })

    note_parts = [
        f"Rolling {days}d UTC. Fired definitions from cx_alerts sum_over_time (not REST incidents).",
        "Compared to alerts.items by definition name (case-insensitive).",
    ]
    if names and batch_queries > 0:
        note_parts.append(
            f"Batched metrics correlation: {batch_queries} PromQL request(s) "
            f"(batch size ≤{_metrics_correlation_batch_size()} names)."
        )
    if err_last and not rows:
        return {
            "count":        0,
            "rows":         [],
            "window_start": w_start,
            "window_end":   w_end,
            "window_days":  days,
            "strategy":     "cx_alerts_metrics_by_definition",
            "error":        err_last,
            "truncated":    False,
            "note":         " ".join(note_parts),
        }
    if err_last:
        note_parts.append(f"Some batches failed: {err_last[:220]}")

    return {
        "count":        len(rows),
        "rows":         rows,
        "window_start": w_start,
        "window_end":   w_end,
        "window_days":  days,
        "strategy":     "cx_alerts_metrics_by_definition",
        "error":        None,
        "truncated":    False,
        "note":         " ".join(note_parts),
    }


def _cx_alerts_daily_activity_timeline(days: int) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Daily total security cx_alerts activity (sum_over_time … [1d]) for hygiene timeline."""
    end_utc = datetime.now(timezone.utc)
    start_utc = end_utc - timedelta(days=max(1, min(366, days)))
    start_unix = int(start_utc.timestamp())
    end_unix = int(end_utc.timestamp())
    matchers = _cx_alerts_security_matchers_promql()
    sel = f"cx_alerts{{{matchers}}}"
    q = f"sum(sum_over_time(({sel}[1d])))"
    if not _metrics_query_origin().startswith("https://"):
        return [], "metrics origin not configured"
    to = max(120, _incidents_prometheus_http_timeout())
    span_sec = max(1, end_unix - start_unix)
    batch_sec = _cx_alerts_timeline_range_batch_days() * 86400
    merged_ts: Dict[int, float] = {}
    err_last: Optional[str] = None
    cursor = start_unix
    while cursor < end_unix:
        chunk_end = min(end_unix, cursor + batch_sec)
        try:
            resp = _prometheus_query_range(q, cursor, chunk_end, 86400, timeout=to)
        except RuntimeError as e:
            err_last = str(e)[:500]
            break
        for ts, v in _prometheus_matrix_series_sums(resp):
            if ts not in merged_ts:
                merged_ts[ts] = v
            else:
                merged_ts[ts] = max(merged_ts[ts], v)
        cursor = chunk_end
    if err_last and not merged_ts:
        return [], err_last
    series = sorted(merged_ts.items(), key=lambda x: x[0])
    timeline: List[Dict[str, Any]] = []
    for ts, v in series:
        dkey = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
        timeline.append({"date": dkey, "count": int(round(v))})
    return timeline, None




def _metrics_correlation_batch_size() -> int:
    """cx_alerts PromQL: number of alert_def_name values per batched instant query."""
    try:
        return max(5, min(120, int(os.environ.get("CORALOGIX_INCIDENTS_METRICS_CORRELATION_BATCH", "40"))))
    except ValueError:
        return 40


def _cx_alerts_timeline_range_batch_days() -> int:
    try:
        return max(5, min(31, int(os.environ.get("CORALOGIX_CX_ALERTS_TIMELINE_RANGE_BATCH_DAYS", "10"))))
    except ValueError:
        return 10


def _alert_definition_names_from_items(items: Any) -> List[str]:
    """Unique alert definition names from alerts.items (order preserved, case-insensitive dedupe)."""
    out: List[str] = []
    seen_cf: Set[str] = set()
    if not isinstance(items, list):
        return out
    for a in items:
        if not isinstance(a, dict):
            continue
        nm = str(a.get("name") or "").strip()
        if not nm:
            continue
        cf = nm.casefold()
        if cf in seen_cf:
            continue
        seen_cf.add(cf)
        out.append(nm)
    return out


def _promql_matcher_alert_def_names(batch: List[str]) -> str:
    """Label matcher fragment: alert_def_name=… or alert_def_name=~… for batched cx_alerts queries."""
    cleaned = [str(n).strip() for n in batch if str(n).strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        esc = cleaned[0].replace("\\", "\\\\").replace('"', '\\"')
        return f'alert_def_name="{esc}"'
    alts = "|".join(re.escape(s) for s in cleaned)
    return f'alert_def_name=~"{alts}"'


def _api_origin_from_alerts_base() -> str:
    """https://api.xx.coralogix.com from CORALOGIX_API_BASE (…/api/v2/external)."""
    base = ALERTS_BASE.rstrip("/")
    for marker in ("/api/v2/external", "/api/v2"):
        if marker in base:
            return base.split(marker)[0]
    return "https://api.eu1.coralogix.com"


def _metrics_api_key() -> str:
    return (
        (os.environ.get("CORALOGIX_METRICS_API_KEY") or "").strip()
        or (os.environ.get("CORALOGIX_PROMETHEUS_API_KEY") or "").strip()
        or API_KEY
    ).strip()


def _metrics_query_origin() -> str:
    """HTTPS origin for GET …/metrics/api/v1/query (no path, no trailing slash)."""
    raw = (os.environ.get("CORALOGIX_METRICS_QUERY_BASE") or "").strip().rstrip("/")
    if raw:
        if not raw.startswith("https://"):
            return ""
        return raw
    return _api_origin_from_alerts_base().rstrip("/")


def _promql_escape_string_literal(s: str) -> str:
    """Escape for embedding inside a PromQL `"…"` string (regex label values)."""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _incidents_promql_range() -> str:
    r = (os.environ.get("CORALOGIX_INCIDENTS_PROMETHEUS_RANGE") or "24h").strip()
    if not r or not re.match(r"^[0-9]+[smhdwy]+$", r, re.I):
        return "24h"
    return r


def _cx_alerts_security_matchers_promql() -> str:
    """
    Label matchers aligned with Grafana cx_alerts panels (security incidents / alerts).
    Regex filters default to .* — override with CORALOGIX_INCIDENTS_FILTER_SOURCE_IP_REGEX /
    CORALOGIX_INCIDENTS_FILTER_EMAIL_REGEX.
    """
    ip_re = _promql_escape_string_literal(
        (os.environ.get("CORALOGIX_INCIDENTS_FILTER_SOURCE_IP_REGEX") or ".*").strip()
    )
    em_re = _promql_escape_string_literal(
        (os.environ.get("CORALOGIX_INCIDENTS_FILTER_EMAIL_REGEX") or ".*").strip()
    )
    return (
        'alert_def_label_alert_type="security",'
        'alert_def_name!~"building block",'
        'alert_def_name!~"null",'
        f'alert_def_group_by_cx_security_source_ip=~"{ip_re}",'
        f'alert_def_group_by_cx_security_email=~"{em_re}"'
    )


def _incidents_p1_promql_suffix() -> str:
    """
    Extra matchers for the P1-only series (Security Alerts Summary dashboard uses alert_def_priority).
    Override with CORALOGIX_INCIDENTS_P1_PROMQL_SUFFIX.
    """
    suf = (os.environ.get("CORALOGIX_INCIDENTS_P1_PROMQL_SUFFIX") or "").strip()
    if suf:
        return "," + suf.lstrip(",")
    return ',alert_def_priority="P1"'


def _incidents_p2_promql_suffix() -> str:
    suf = (os.environ.get("CORALOGIX_INCIDENTS_P2_PROMQL_SUFFIX") or "").strip()
    if suf:
        return "," + suf.lstrip(",")
    return ',alert_def_priority="P2"'


def _prometheus_instant_query(query: str, timeout: int = 45) -> Dict[str, Any]:
    """GET {origin}/metrics/api/v1/query — same Bearer pattern as other Coralogix APIs."""
    origin = _metrics_query_origin()
    if not origin.startswith("https://"):
        raise RuntimeError("CORALOGIX_METRICS_QUERY_BASE must be https://…")
    key = _metrics_api_key()
    if not key:
        raise RuntimeError("No API key (CORALOGIX_METRICS_API_KEY or CORALOGIX_API_KEY)")
    q = urlencode({"query": query})
    url = f"{origin}/metrics/api/v1/query?{q}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {key}",
            "Accept":        "application/json",
            "User-Agent":    "coralogix-dashboard-refresh/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        body = ""
        if e.fp:
            try:
                body = e.read().decode()[:400]
            except Exception:
                pass
        raise RuntimeError(f"HTTP {e.code}: {body}") from e
    except TimeoutError as e:
        raise RuntimeError(f"metrics query timed out after {timeout}s") from e
    except urllib.error.URLError as e:
        reason = getattr(e, "reason", e)
        raise RuntimeError(f"metrics query failed: {reason}") from e


def _prometheus_query_range(
    query: str,
    start_unix: int,
    end_unix: int,
    step_sec: int,
    timeout: int = 90,
) -> Dict[str, Any]:
    """GET …/metrics/api/v1/query_range — used for 30d daily buckets when supported."""
    origin = _metrics_query_origin()
    if not origin.startswith("https://"):
        raise RuntimeError("CORALOGIX_METRICS_QUERY_BASE must be https://…")
    key = _metrics_api_key()
    if not key:
        raise RuntimeError("No API key (CORALOGIX_METRICS_API_KEY or CORALOGIX_API_KEY)")
    q = urlencode({
        "query": query,
        "start": str(start_unix),
        "end":   str(end_unix),
        "step":  str(step_sec),
    })
    url = f"{origin}/metrics/api/v1/query_range?{q}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {key}",
            "Accept":        "application/json",
            "User-Agent":    "coralogix-dashboard-refresh/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        body = ""
        if e.fp:
            try:
                body = e.read().decode()[:400]
            except Exception:
                pass
        raise RuntimeError(f"HTTP {e.code}: {body}") from e
    except TimeoutError as e:
        raise RuntimeError(f"metrics query_range timed out after {timeout}s") from e
    except urllib.error.URLError as e:
        reason = getattr(e, "reason", e)
        raise RuntimeError(f"metrics query_range failed: {reason}") from e


def _prometheus_vector_labeled_values(resp: Dict[str, Any]) -> List[Tuple[Dict[str, Any], float]]:
    """Instant query vector → [(metric_labels, value), …]."""
    out: List[Tuple[Dict[str, Any], float]] = []
    if not isinstance(resp, dict) or resp.get("status") != "success":
        return out
    data = resp.get("data") or {}
    if data.get("resultType") != "vector":
        return out
    for row in data.get("result") or []:
        if not isinstance(row, dict):
            continue
        m = row.get("metric")
        if not isinstance(m, dict):
            m = {}
        val = row.get("value")
        if not isinstance(val, list) or len(val) < 2:
            continue
        try:
            fv = float(val[1])
        except (TypeError, ValueError):
            continue
        out.append((m, fv))
    return out


def _prometheus_first_scalar_value(resp: Dict[str, Any]) -> Optional[float]:
    if not isinstance(resp, dict) or resp.get("status") != "success":
        return None
    data = resp.get("data") or {}
    rtype = data.get("resultType")
    res = data.get("result")
    if rtype == "scalar" and isinstance(res, list) and len(res) >= 2:
        try:
            return float(res[1])
        except (TypeError, ValueError):
            return None
    if rtype == "vector" and isinstance(res, list):
        if len(res) == 0:
            return 0.0
        for row in res:
            if not isinstance(row, dict):
                continue
            val = row.get("value")
            if isinstance(val, list) and len(val) >= 2:
                try:
                    return float(val[1])
                except (TypeError, ValueError):
                    continue
    return None


def _prometheus_vector_label_floats(resp: Dict[str, Any], label_key: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not isinstance(resp, dict) or resp.get("status") != "success":
        return out
    data = resp.get("data") or {}
    if data.get("resultType") != "vector":
        return out
    for row in data.get("result") or []:
        if not isinstance(row, dict):
            continue
        m = row.get("metric")
        if not isinstance(m, dict):
            continue
        raw = m.get(label_key)
        name = str(raw).strip() if raw is not None else ""
        if not name:
            name = "(empty)"
        val = row.get("value")
        if isinstance(val, list) and len(val) >= 2:
            try:
                out[name] = float(val[1])
            except (TypeError, ValueError):
                pass
    return out


CANONICAL_INGESTION_BLOCK_ALERT = "Coralogix Data Usage Alert - Ingestion Blocked"


def _is_canonical_ingestion_block_name(name: str) -> bool:
    """True for the account-level Data Usage ingestion-blocked alert (name match)."""
    s = " ".join((name or "").split())
    if not s:
        return False
    if s.casefold() == CANONICAL_INGESTION_BLOCK_ALERT.casefold():
        return True
    low = s.casefold()
    return "coralogix data usage" in low and "ingestion" in low and "block" in low


def fetch_suppression_scheduler_rules() -> Dict[str, Any]:
    """
    GET {origin}/api/v1/alert-scheduler-rules — Coralogix suppression / scheduler rules.
    Same path as sb-ahc-automator suppression_rules_check REST fallback.
    """
    url = f"{_api_origin_from_alerts_base()}/api/v1/alert-scheduler-rules"
    try:
        data = _get(url, timeout=35)
    except RuntimeError as e:
        return {
            "count": 0,
            "items": [],
            "error": str(e)[:400],
            "note":    "Grant API access to alert scheduler rules if HTTP 403.",
        }
    rules = (
        data.get("alertSchedulerRules")
        or data.get("alert_scheduler_rules")
        or []
    )
    items: List[Dict[str, str]] = []
    for r in rules:
        if not isinstance(r, dict):
            continue
        inner = r.get("alertSchedulerRule")
        inner = inner if isinstance(inner, dict) else r
        nm = str(
            inner.get("name")
            or inner.get("ruleName")
            or r.get("name")
            or ""
        ).strip()
        rid = str(inner.get("id") or r.get("id") or "").strip()
        if nm or rid:
            items.append({
                "name": nm or "(unnamed rule)",
                "id":   rid,
            })
    items.sort(key=lambda x: x["name"].casefold())
    return {"count": len(items), "items": items, "error": None, "note": None}


# ── Section fetchers ──────────────────────────────────────────────────────────

def fetch_integrations() -> Dict:
    """Deployed integrations (amountIntegrations > 0)."""
    data = _get(f"{MGMT_BASE}/integrations/integrations/v1")
    deployed = []
    for item in data.get("integrations", []):
        if (item.get("amountIntegrations") or 0) > 0:
            deployed.append({
                "name":        item["integration"].get("name", item["integration"].get("id")),
                "id":          item["integration"].get("id"),
                "tags":        item["integration"].get("tags", []),
                "connections": item.get("amountIntegrations", 0),
                "upgrade":     item.get("upgradeAvailable", False),
            })
    deployed.sort(key=lambda x: x["connections"], reverse=True)
    return {"count": len(deployed), "items": deployed}


def fetch_extensions() -> Dict:
    """Deployed extension packs."""
    data = _get(f"{MGMT_BASE}/integrations/extensions/v1/deployed")
    items = []
    for ext in data.get("deployedExtensions", []):
        items.append({
            "id":      ext.get("extensionId", ext.get("id", "")),
            "name":    ext.get("name", ext.get("extensionId", "")),
            "version": ext.get("version", ""),
            "upgrade": ext.get("upgradeAvailable", False),
        })
    return {"count": len(items), "items": items}


def fetch_webhooks() -> Dict:
    """Outbound webhooks (key = 'deployed' in response)."""
    data = _get(f"{MGMT_BASE}/integrations/webhooks/v1")
    hooks = data.get("deployed", data.get("webhooks", []))
    items = []
    for h in hooks:
        if not isinstance(h, dict):
            continue
        ext = h.get("externalId")
        if ext is None:
            ext = h.get("external_id")
        items.append({
            "id":          h.get("id", ""),
            "name":        h.get("name", ""),
            "type":        h.get("type", h.get("webhookType", "")),
            "externalId":  ext,
        })
    return {"count": len(items), "items": items}


def fetch_saml() -> Dict:
    """SAML SSO configuration. Requires saml:ReadConfig permission on the API key."""
    try:
        data = _get(f"{MGMT_BASE}/aaa/team-saml/v1/configuration")
        cfg = data.get("samlConfiguration", data.get("configuration", data))
        return {
            "active":  cfg.get("active", cfg.get("isActive", False)),
            "idp_url": cfg.get("idpMetadataUrl", cfg.get("entityId", "")),
        }
    except RuntimeError as e:
        return {
            "active": None,
            "error":  str(e),
            "note":   "Add 'saml:ReadConfig' permission to the API key to enable this check.",
        }


def fetch_ip_access() -> Dict:
    """IP allowlist / access control settings."""
    data = _get(f"{MGMT_BASE}/aaa/team-sec-ip-access/v1")
    settings = data.get("settings", data)
    enabled  = settings.get("enabled", settings.get("isEnabled", False))
    allowed  = settings.get("allowedIpRanges",
               settings.get("cidrBlocks",
               settings.get("allowList", [])))
    return {
        "enabled":       enabled,
        "allowed_count": len(allowed),
        "allowed":       allowed[:10],
    }


def fetch_enrichments() -> Dict:
    """Active enrichment rules."""
    data = _get(f"{MGMT_BASE}/enrichment-rules/enrichment-rules/v1")
    enrichments = data.get("enrichments", [])
    by_type: Dict[str, int] = {}
    for e in enrichments:
        t = e.get("enrichmentType", {})
        type_name = (list(t.keys())[0] if isinstance(t, dict) and t else str(t))
        by_type[type_name] = by_type.get(type_name, 0) + 1
    return {"count": len(enrichments), "by_type": by_type}


def fetch_folders() -> Dict:
    """Dashboard folder structure (key = 'folder' singular in response)."""
    data = _get(f"{MGMT_BASE}/dashboards/dashboards/v1/folders")
    folders = data.get("folder", data.get("folders", []))
    items = [{"id": f.get("id", ""), "name": f.get("name", "")} for f in folders]
    return {"count": len(items), "items": items}


def fetch_tco_policies() -> Dict:
    """TCO log policies."""
    data = _get(f"{MGMT_BASE}/dataplans/policies/v1")
    policies = data.get("policies", [])
    items = []
    for p in policies:
        items.append({
            "id":       p.get("id", ""),
            "name":     p.get("name", ""),
            "priority": p.get("priority", ""),
            "enabled":  p.get("enabled", True),
        })
    return {"count": len(items), "items": items}


def _data_usage_row_label(dimensions: Any) -> Tuple[str, str]:
    """
    Parse Data Usage entry dimensions into (kind, display_label).
    Prefer application_name (Data Usage dashboard template: by application_name) over subsystem_name.
    """
    if not isinstance(dimensions, list):
        return "", ""
    gdmap: Dict[str, str] = {}
    for d in dimensions:
        if not isinstance(d, dict):
            continue
        for key in ("application", "applicationName", "subsystem", "subsystemName"):
            if d.get(key) is not None and str(d.get(key)).strip():
                gdmap[str(key).casefold()] = str(d.get(key)).strip()
        gd = d.get("genericDimension") or d.get("generic_dimension")
        if isinstance(gd, dict):
            k = str(gd.get("key") or "").strip()
            v = str(gd.get("value") or "").strip()
            if k and v:
                gdmap[k.casefold()] = v
    if "application_name" in gdmap:
        return "application", gdmap["application_name"]
    for k, v in gdmap.items():
        if k in ("application", "applicationname") or (
            "application" in k and "subsystem" not in k
        ):
            return "application", v
    if "subsystem_name" in gdmap:
        return "subsystem", gdmap["subsystem_name"]
    for k, v in gdmap.items():
        if "subsystem" in k:
            return "subsystem", v
    return "", ""


def _extract_usage_entries_from_root(obj: Any) -> List[Dict[str, Any]]:
    """Pull entries[] from either {entries} or {result:{entries}} (stream / JSON shapes)."""
    if not isinstance(obj, dict):
        return []
    res = obj.get("result")
    if isinstance(res, dict) and isinstance(res.get("entries"), list):
        return [x for x in res["entries"] if isinstance(x, dict)]
    ent = obj.get("entries")
    if isinstance(ent, list):
        return [x for x in ent if isinstance(x, dict)]
    return []


def _parse_data_usage_v2_body(raw: str) -> List[Dict[str, Any]]:
    """
    Data Usage GET may return a single JSON object or newline-delimited JSON / SSE
    (see Coralogix Data Usage Service overview).
    """
    raw = (raw or "").strip()
    if raw.startswith("data:"):
        raw = raw[5:].strip()
    if not raw:
        return []
    if raw.startswith("{") or raw.startswith("["):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            data = None
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            got = _extract_usage_entries_from_root(data)
            if got:
                return got
    out: List[Dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith(":"):
            continue
        if line.startswith("data:"):
            line = line[5:].strip()
        if not line or line == "[DONE]":
            continue
        try:
            obj: Any = json.loads(line)
        except json.JSONDecodeError:
            continue
        got = _extract_usage_entries_from_root(obj)
        if got:
            out.extend(got)
        elif isinstance(obj, dict) and ("sizeGb" in obj or "dimensions" in obj or "units" in obj):
            out.append(obj)
    return out


def _fetch_data_usage_v2_entries(url: str) -> List[Dict[str, Any]]:
    """GET Data Usage v2 — try JSON first; if empty, same URL with text/event-stream (per Coralogix docs)."""
    assert url.startswith("https://"), f"Refusing non-HTTPS request: {url}"
    headers_base = {
        "Authorization": f"Bearer {API_KEY}",
        "User-Agent":    "coralogix-dashboard-refresh/1.0",
    }
    last_empty_stream: List[Dict[str, Any]] = []
    for accept in ("application/json", "text/event-stream"):
        req = urllib.request.Request(
            url,
            headers={**headers_base, "Accept": accept},
        )
        try:
            with urllib.request.urlopen(req, timeout=90) as r:
                body = r.read().decode()
        except urllib.error.HTTPError as e:
            err_body = ""
            if e.fp:
                try:
                    err_body = e.read().decode()[:300]
                except Exception:
                    pass
            raise RuntimeError(f"HTTP {e.code}: {err_body}") from e
        entries = _parse_data_usage_v2_body(body)
        if os.environ.get("CORALOGIX_DEBUG_DATA_USAGE") == "1":
            dbg = Path(__file__).parent / "_debug_data_usage_body.txt"
            dbg.write_text(
                f"Accept: {accept}\nlen={len(body)}\n\n{body[:12000]}",
                encoding="utf-8",
            )
        if accept == "application/json" and entries:
            return entries
        if accept == "text/event-stream":
            last_empty_stream = entries
    return last_empty_stream


def _log_ingestion_aggregate_param() -> str:
    """
    CORALOGIX_LOG_INGESTION_AGGREGATE=APPLICATION (default) → AGGREGATE_BY_APPLICATION
    (matches Data Usage template: sum(cx_data_usage_units) / cx_data_usage_bytes_total by application_name).
    SUBSYSTEM → AGGREGATE_BY_SUBSYSTEM
    """
    raw = os.environ.get("CORALOGIX_LOG_INGESTION_AGGREGATE", "APPLICATION").strip().upper()
    if raw in ("SUBSYSTEM", "SUB"):
        return "AGGREGATE_BY_SUBSYSTEM"
    return "AGGREGATE_BY_APPLICATION"


def fetch_log_ingestion_from_data_usage(
    days: int = 7,
    aggregate: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Same metrics family as the Coralogix **Data Usage** dashboard export:
      sum(cx_data_usage_units{application_name=~".+"}) by (application_name)
      sum(cx_data_usage_bytes_total{...}) by (application_name)  → sizeGb in API

    REST: GET {MGMT_BASE}/dataplans/data-usage/v2 with aggregate=AGGREGATE_BY_APPLICATION (default),
    or pass ``aggregate="AGGREGATE_BY_SUBSYSTEM"`` to override (used by alert hygiene).

    API key needs **data-usage:Read** (HTTP 403 if missing).

    Returns **daily averages** over the window: for each application, (sum of units or GB across
    the window) / window_days — not the 7-day cumulative total.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    n_days = float(max(days, 1))
    agg = aggregate if aggregate else _log_ingestion_aggregate_param()
    qs = urlencode(
        {
            "date_range.fromDate": start.strftime("%Y-%m-%dT00:00:00.000Z"),
            "date_range.toDate": end.strftime("%Y-%m-%dT23:59:59.999Z"),
            "aggregate": agg,
            "resolution": "24h",
        },
        safe=":Z",
    )
    url = f"{MGMT_BASE}/dataplans/data-usage/v2?{qs}"
    try:
        flat_entries = _fetch_data_usage_v2_entries(url)
    except Exception as e:
        return {
            "error":            str(e)[:500],
            "window_days":      days,
            "window_start":     start.isoformat(),
            "window_end":       end.isoformat(),
            "aggregate":        agg,
            "api_host":         _mgmt_api_hostname(),
            "entries_received": 0,
            "items":            [],
            "note":             "Grant data-usage:Read on the Coralogix API key if this returns HTTP 403.",
        }

    buckets: Dict[str, Dict[str, Any]] = {}
    for ent in flat_entries:
        if not isinstance(ent, dict):
            continue
        kind, label = _data_usage_row_label(ent.get("dimensions"))
        if not label:
            label = "Other"
        key_cf = label.casefold()
        sz = float(ent.get("sizeGb") or ent.get("size_gb") or 0)
        un = float(ent.get("units") or 0)
        b = buckets.setdefault(
            key_cf,
            {"name": label, "kind": kind or "", "size_gb": 0.0, "units": 0.0},
        )
        b["size_gb"] += sz
        b["units"] += un

    dim_word = "application" if agg == "AGGREGATE_BY_APPLICATION" else "subsystem"
    items: List[Dict[str, Any]] = []
    for _k, b in sorted(buckets.items(), key=lambda kv: -kv[1]["size_gb"]):
        sg_sum = float(b["size_gb"])
        un_sum = float(b["units"])
        sg_avg = sg_sum / n_days
        un_avg = un_sum / n_days
        nm = str(b["name"])
        row: Dict[str, Any] = {
            "name":                nm,
            "application":         nm if dim_word == "application" else "",
            "subsystem":           nm if dim_word == "subsystem" else "",
            "dimension_kind":      str(b.get("kind") or dim_word),
            "category":            "Data usage metrics",
            "units_avg_daily":     round(un_avg, 8),
            "size_gb_avg_daily":   round(sg_avg, 8),
            "raw_kb":              round(sg_avg * 1024 * 1024, 4),
            # Legacy keys (7d sum) for older snapshots / charts — prefer *_avg_daily
            "units_7d":            round(un_sum, 8),
            "size_gb_7d":          round(sg_sum, 8),
            "logs_7d_est":         int(round(un_avg)),
        }
        items.append(row)

    note = (
        f"Coralogix Data Usage API — {days}d window, {dim_word} aggregate; values are **daily averages** "
        f"(window sum ÷ {days}). Same source family as cx_data_usage_units / cx_data_usage_bytes_total."
    )
    if agg == "AGGREGATE_BY_SUBSYSTEM":
        note = (
            f"Coralogix Data Usage API — {days}d window, by subsystem_name; **daily averages** (sum ÷ {days}). "
            f"Set CORALOGIX_LOG_INGESTION_AGGREGATE=APPLICATION for application rows."
        )

    return {
        "error":              None,
        "window_days":        days,
        "window_start":       start.isoformat(),
        "window_end":         end.isoformat(),
        "aggregate":          agg,
        "api_host":           _mgmt_api_hostname(),
        "resolution":         "24h",
        "value_interpretation": "average_daily_over_window",
        "entries_received":   len(flat_entries),
        "items":              items[:500],
        "note":               note,
    }


def attach_log_ingestion_data_usage(results: Dict[str, Any]) -> None:
    """Populate results['log_ingestion'] for the Security Log Ingestion dashboard panel."""
    li = fetch_log_ingestion_from_data_usage(7)
    results["log_ingestion"] = li
    if li.get("error"):
        print(f"  ⚠  data usage / log ingestion: {str(li.get('error'))[:90]}")
    else:
        n = len(li.get("items") or [])
        er = li.get("entries_received")
        extra = f", {er} raw API entr{'y' if er == 1 else 'ies'}" if isinstance(er, int) else ""
        label = "application" if (li.get("aggregate") == "AGGREGATE_BY_APPLICATION") else "subsystem"
        print(
            f"  ℹ  data usage / log ingestion: {n} {label} row(s) "
            f"({li.get('window_days')}d window, daily averages{extra})"
        )


def fetch_data_plan_units_per_day_metric() -> Dict[str, Any]:
    """
    Tenant daily data-plan unit quota from Prometheus metric ``cx_data_plan_units_per_day``.
    Default PromQL: ``sum(cx_data_plan_units_per_day)`` (override with CORALOGIX_DATA_PLAN_UNITS_PROMQL).
    Same metrics origin/key as cx_alerts (CORALOGIX_METRICS_QUERY_BASE, CORALOGIX_METRICS_API_KEY).
    """
    flag = os.environ.get("CORALOGIX_DATA_PLAN_UNITS_METRIC", "").strip().lower()
    if flag in ("0", "false", "no", "off"):
        return {"skipped": True, "reason": "CORALOGIX_DATA_PLAN_UNITS_METRIC disabled", "units_per_day": None}

    promql = (os.environ.get("CORALOGIX_DATA_PLAN_UNITS_PROMQL") or "").strip()
    if not promql:
        promql = "sum(cx_data_plan_units_per_day)"

    origin = _metrics_query_origin()
    if not origin.startswith("https://"):
        return {
            "skipped": True,
            "reason": "CORALOGIX_METRICS_QUERY_BASE invalid or unset",
            "units_per_day": None,
            "promql":        promql,
        }

    try:
        resp = _prometheus_instant_query(promql, timeout=_incidents_prometheus_http_timeout())
    except RuntimeError as e:
        return {
            "error":         str(e)[:500],
            "units_per_day": None,
            "promql":        promql,
        }

    if not isinstance(resp, dict) or resp.get("status") != "success":
        return {
            "error":         "metrics response not successful",
            "units_per_day": None,
            "promql":        promql,
        }

    pdata = resp.get("data") or {}
    res = pdata.get("result")
    if pdata.get("resultType") == "vector" and isinstance(res, list) and len(res) == 0:
        return {
            "units_per_day": None,
            "series_count":  0,
            "promql":        promql,
            "error":         None,
            "note":          "empty vector — no cx_data_plan_units_per_day samples",
        }

    labeled = _prometheus_vector_labeled_values(resp)
    if labeled:
        total = sum(fv for _, fv in labeled)
        rounded = float(int(total)) if abs(total - round(total)) < 1e-6 else round(total, 6)
        return {
            "units_per_day": rounded,
            "series_count":  len(labeled),
            "promql":        promql,
            "error":         None,
            "note":          "Instant query · summed over metric series",
        }

    val = _prometheus_first_scalar_value(resp)
    if val is None:
        return {
            "units_per_day": None,
            "series_count":  0,
            "promql":        promql,
            "error":         None,
            "note":          "could not parse metrics value",
        }

    rounded = float(int(val)) if abs(val - round(val)) < 1e-6 else round(val, 6)
    return {
        "units_per_day": rounded,
        "series_count":  1,
        "promql":        promql,
        "error":         None,
        "note":          "Instant query",
    }


def attach_data_plan_units_per_day(results: Dict[str, Any]) -> None:
    """Populate results['data_plan_units_per_day'] for Customer info quota card."""
    block = fetch_data_plan_units_per_day_metric()
    results["data_plan_units_per_day"] = block
    if block.get("skipped"):
        print(f"  ℹ  data plan units: skipped — {block.get('reason', '')}")
    elif block.get("error"):
        print(f"  ⚠  data plan units: {str(block.get('error'))[:100]}")
    elif isinstance(block.get("units_per_day"), (int, float)):
        u = float(block["units_per_day"])
        ut = f"{int(u):,}" if abs(u - round(u)) < 1e-6 else str(block["units_per_day"])
        nser = block.get("series_count")
        extra = f", {nser} series" if isinstance(nser, int) and nser > 1 else ""
        print(f"  ℹ  data plan units: {ut}/day · cx_data_plan_units_per_day{extra}")
    else:
        print("  ℹ  data plan units: no samples (cx_data_plan_units_per_day)")


# ── Query performance (DataPrime @ central C4C / EU2) ─────────────────────────

QUERY_PERF_TEMPLATE = """source logs
| filter event_obj._message.in('dataprime-query-metrics','ocp-query-metrics')
| filter event_obj.queryInfo.queryEngine == 'dataprime' || event_obj.queryInfo.queryEngine == 'ocp'
| filter event_obj.clientInfo.originatingTeamId.textSearch('{company_id}')
| create source0 from event_obj.queryInfo.sources[0]
| create duration_sec from (source0.timeFrame.durationMs / 1000)
| create time_window from case_lessthan {
source0.timeFrame.durationMs.toInterval('ms').roundInterval('m'),
15m1s -> '15m',
24h1s -> '24h',
7d1s -> '7d'
}
| groupby time_window, event_obj.queryInfo.backendType as backend, event_obj.queryInfo.queryEngine as query_engine agg
round(percentile(0.94,event_obj.queryOutcome.e2eQueryDurationMs)/1000,2) as p94,
count() as count
| filter time_window != null
| orderby backend, time_window"""

# C4C DataPrime — ARR / AM / TAM from originatingTeamId_enriched (CORALOGIX_COMPANY_ID in textSearch)
TEAM_ENRICHMENT_TEMPLATE = """source logs
| filter event_obj._message.in('dataprime-query-metrics','ocp-query-metrics')
| filter $d.event_obj.clientInfo.clientId != 'dp-api-probe'
| filter event_obj.queryInfo.queryEngine == 'dataprime'
| filter event_obj.clientInfo.originatingTeamId.textSearch('{company_id}')
| groupby $d.event_obj.clientInfo.originatingTeamId_enriched.AccountARR as ARR, $d.event_obj.clientInfo.originatingTeamId_enriched.AccountManager as AM, $d.event_obj.clientInfo.originatingTeamId_enriched.TAM as TAM
"""


def _normalize_query_perf_api_host(raw: str) -> str:
    """
    Map team-style host (e.g. eu2.coralogix.com) to REST API host api.eu2.coralogix.com.
    """
    h = (raw or "").strip().lower().replace("https://", "").split("/")[0]
    if not h:
        return "api.eu2.coralogix.com"
    if h.startswith("api."):
        return h
    parts = h.split(".")
    if len(parts) >= 3 and parts[-2] == "coralogix" and parts[-1] == "com":
        if parts[0] in ("eu1", "eu2", "us1", "us2", "ap1", "ap2"):
            return f"api.{parts[0]}.coralogix.com"
    return h


def _query_perf_auth_header(company_id: str) -> str:
    """API key (optional C4C-specific) or session token style for DataPrime."""
    if os.environ.get("CORALOGIX_QUERY_PERF_USE_SESSION", "").strip().lower() in ("1", "true", "yes"):
        tok = os.environ.get("CORALOGIX_SESSION_TOKEN", "").strip()
        if tok and company_id:
            return f"Bearer {tok}/{company_id}"
    key = os.environ.get("CORALOGIX_C4C_DATAPRIME_API_KEY", "").strip() or API_KEY
    return f"Bearer {key}"


def _post_dataprime_query_text(
    host: str,
    query: str,
    start_iso: str,
    end_iso: str,
    tier: str,
    auth_header: str,
    timeout: int = 120,
) -> str:
    """POST DataPrime query; return raw response body (NDJSON / SSE lines)."""
    url = f"https://{host}/api/v1/dataprime/query"
    assert url.startswith("https://"), f"Refusing non-HTTPS request: {url}"
    payload = {
        "query": query,
        "metadata": {
            "tier": tier,
            "syntax": "QUERY_SYNTAX_DATAPRIME",
            "startDate": start_iso,
            "endDate": end_iso,
            "defaultSource": "logs",
        },
    }
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": auth_header,
            "Content-Type": "application/json",
            "Accept": "*/*",
            "User-Agent": "coralogix-dashboard-refresh/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode()
    except urllib.error.HTTPError as e:
        err_body = ""
        if e.fp:
            try:
                err_body = e.read().decode()[:500]
            except Exception:
                pass
        raise RuntimeError(f"HTTP {e.code}: {err_body}")


def _dataprime_merge_labels_userdata(record: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not isinstance(record, dict):
        return out
    for kv in record.get("labels") or []:
        if isinstance(kv, dict):
            k, v = kv.get("key"), kv.get("value")
            if k:
                out[str(k)] = v
    ud = record.get("userData") or record.get("user_data")
    if isinstance(ud, str):
        try:
            ud = json.loads(ud)
        except json.JSONDecodeError:
            ud = {}
    if isinstance(ud, dict):
        for k, v in ud.items():
            if k not in out:
                out[str(k)] = v
    for alt in (
        "time_window",
        "backend",
        "query_engine",
        "p94",
        "count",
        "Time_window",
        "Backend",
        "ARR",
        "AM",
        "TAM",
        "query_count",
    ):
        if alt in record and alt not in out:
            out[alt] = record[alt]
    return out


def _coerce_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _coerce_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _parse_dataprime_ndjson_body(raw: str) -> Tuple[List[Dict[str, Any]], Optional[str], List[str]]:
    """
    Parse DataPrime HTTP response: newline-delimited JSON, optional 'data:' SSE prefix.
    Returns (result_rows, first_error_message, server_warnings).
    """
    rows: List[Dict[str, Any]] = []
    warnings: List[str] = []
    first_err: Optional[str] = None
    text = (raw or "").strip()
    if not text:
        return rows, first_err, warnings

    def _walk_warn(obj: Any, depth: int = 0) -> None:
        if depth > 8 or obj is None:
            return
        if isinstance(obj, str):
            s = obj.strip()
            if len(s) > 5:
                warnings.append(s[:500])
            return
        if isinstance(obj, dict):
            for k, v in obj.items():
                lk = str(k).lower()
                if lk in ("warningmessage", "message", "errormessage") and isinstance(v, str):
                    warnings.append(v.strip()[:500])
                else:
                    _walk_warn(v, depth + 1)
        elif isinstance(obj, list):
            for x in obj[:20]:
                _walk_warn(x, depth + 1)

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith(":"):
            continue
        if line.startswith("data:"):
            line = line[5:].strip()
        if not line or line == "[DONE]":
            continue
        try:
            obj: Any = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        err = obj.get("error") or obj.get("Error")
        if err and first_err is None:
            first_err = str(err)[:400]
        if "warning" in obj:
            _walk_warn(obj["warning"])
        res = obj.get("result")
        chunk: List[Any] = []
        if isinstance(res, dict):
            inner = res.get("results")
            if isinstance(inner, list):
                chunk = inner
            elif inner is None and isinstance(res.get("rows"), list):
                chunk = res["rows"]
        elif isinstance(res, list):
            chunk = res
        for rec in chunk:
            if isinstance(rec, dict):
                rows.append(rec)

    # Single pretty-printed JSON fallback (rare)
    if not rows and text.startswith("{"):
        try:
            one = json.loads(text)
            if isinstance(one, dict):
                if "warning" in one:
                    _walk_warn(one["warning"])
                res = one.get("result")
                if isinstance(res, dict) and isinstance(res.get("results"), list):
                    for rec in res["results"]:
                        if isinstance(rec, dict):
                            rows.append(rec)
        except json.JSONDecodeError:
            pass

    return rows, first_err, warnings


def _query_perf_row_from_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    m = _dataprime_merge_labels_userdata(rec)
    tw = m.get("time_window") or m.get("Time_window") or m.get("timeWindow")
    be = m.get("backend") or m.get("Backend")
    qe = m.get("query_engine") or m.get("queryEngine") or m.get("Query_engine")
    p94 = m.get("p94")
    cnt = m.get("count")
    return {
        "time_window": str(tw).strip() if tw is not None else "",
        "backend": str(be).strip() if be is not None else "",
        "query_engine": str(qe).strip() if qe is not None else "",
        "p94_seconds": _coerce_float(p94),
        "count": _coerce_int(cnt),
    }


def fetch_query_performance() -> Dict[str, Any]:
    """
    DataPrime query against the central C4C account; scope to this customer via
    originatingTeamId == CORALOGIX_COMPANY_ID (same ID as the Variables page in the UI).
    """
    flag = os.environ.get("CORALOGIX_QUERY_PERFORMANCE", "").strip().lower()
    if flag in ("0", "false", "no", "off"):
        return {"skipped": True, "reason": "CORALOGIX_QUERY_PERFORMANCE disabled"}

    company_id = os.environ.get("CORALOGIX_COMPANY_ID", "").strip()
    if not company_id:
        return {"skipped": True, "reason": "CORALOGIX_COMPANY_ID not set (needed for originatingTeamId filter)"}

    if not re.fullmatch(r"[0-9]+", company_id):
        return {
            "error": "CORALOGIX_COMPANY_ID must be numeric for query performance (originating team id)",
            "company_id": company_id,
            "rows": [],
        }

    host = _normalize_query_perf_api_host(os.environ.get("CORALOGIX_QUERY_PERF_API_HOST", "api.eu2.coralogix.com"))
    # Docs: default hot tier is TIER_FREQUENT_SEARCH (not *_ACCESS).
    tier = os.environ.get("CORALOGIX_QUERY_PERF_TIER", "TIER_FREQUENT_SEARCH").strip() or "TIER_FREQUENT_SEARCH"
    try:
        days = int(os.environ.get("CORALOGIX_QUERY_PERF_DAYS", "30"))
    except ValueError:
        days = 30
    days = max(1, min(days, 90))
    requested_days = days

    # OpenSearch frequent tier: API truncates beyond PT360H (~15d) — clamp so the query window matches reality.
    effective_days = days
    window_note: Optional[str] = None
    if tier == "TIER_FREQUENT_SEARCH" and days > 15:
        effective_days = 15
        window_note = (
            f"TIER_FREQUENT_SEARCH allows at most ~15 days (PT360H); "
            f"requested {requested_days}d was clamped to {effective_days}d. "
            f"For a full 30d window use CORALOGIX_QUERY_PERF_TIER=TIER_ARCHIVE."
        )

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=effective_days)
    start_iso = _iso_z(start)
    end_iso = _iso_z(now)
    # Use .replace — .format() would treat DataPrime `{` `}` blocks as placeholders.
    query = QUERY_PERF_TEMPLATE.replace("{company_id}", company_id)
    auth = _query_perf_auth_header(company_id)

    try:
        raw = _post_dataprime_query_text(host, query, start_iso, end_iso, tier, auth, timeout=120)
    except Exception as e:
        return {
            "error": str(e)[:500],
            "company_id": company_id,
            "api_host": host,
            "window_days_requested": requested_days,
            "window_days": effective_days,
            "window_start": start_iso,
            "window_end": end_iso,
            "rows": [],
        }

    records, stream_err, api_warnings = _parse_dataprime_ndjson_body(raw)
    parsed: List[Dict[str, Any]] = []
    for rec in records:
        row = _query_perf_row_from_record(rec)
        if row.get("time_window") or row.get("backend"):
            parsed.append(row)

    tw_order = {"15m": 0, "24h": 1, "7d": 2}

    def sort_key(r: Dict[str, Any]) -> Tuple[str, int]:
        tw = str(r.get("time_window") or "")
        return (str(r.get("backend") or ""), tw_order.get(tw, 99))

    parsed.sort(key=sort_key)

    out: Dict[str, Any] = {
        "company_id": company_id,
        "api_host": host,
        "tier": tier,
        "window_days_requested": requested_days,
        "window_days": effective_days,
        "window_start": start_iso,
        "window_end": end_iso,
        "row_count": len(parsed),
        "rows": parsed,
    }
    if window_note:
        out["note"] = window_note
    if api_warnings:
        # Dedupe while preserving order
        seen: Set[str] = set()
        uniq: List[str] = []
        for w in api_warnings:
            if w and w not in seen:
                seen.add(w)
                uniq.append(w)
        out["api_warnings"] = uniq[:12]
    if stream_err and not parsed:
        out["error"] = stream_err
    elif stream_err:
        out["warning"] = (out.get("warning") + " · " if out.get("warning") else "") + stream_err
    return out


def _team_enrichment_row_from_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    m = _dataprime_merge_labels_userdata(rec)

    def pick(*keys: str) -> str:
        for k in keys:
            if k in m and m[k] is not None:
                s = str(m[k]).strip()
                if s and s not in ("{}", "[]", "null"):
                    return s
        return ""

    arr = pick("ARR", "AccountARR", "account_arr")
    am = pick("AM", "AccountManager", "account_manager")
    tam = pick("TAM", "tam")
    if not arr or not am or not tam:
        for k, v in m.items():
            if v is None:
                continue
            s = str(v).strip()
            if not s or s in ("{}", "[]", "null"):
                continue
            compact = str(k).lower().replace("_", "").replace(".", "")
            kl = str(k).lower()
            if not arr and ("accountarr" in compact or "annualrecurringrevenue" in compact):
                arr = s[:500]
            if not am and "accountmanager" in compact and "technical" not in compact:
                am = s[:500]
            if not tam and (
                compact == "tam"
                or "technicalaccountmanager" in compact
                or kl.endswith("_tam")
                or kl.endswith(".tam")
            ):
                tam = s[:500]
    qc = _coerce_int(m.get("query_count") or m.get("count") or m.get("cnt"))
    return {
        "account_arr":       arr[:500],
        "account_manager":   am[:500],
        "tam":               tam[:500],
        "query_count":       qc if qc is not None else 0,
    }


def fetch_c4c_team_enrichment() -> Dict[str, Any]:
    """
    DataPrime on central C4C: groupby ARR / AM / TAM from originatingTeamId_enriched.
    Uses CORALOGIX_COMPANY_ID in originatingTeamId.textSearch (dashboard account id).
    """
    flag = os.environ.get("CORALOGIX_TEAM_ENRICHMENT", "").strip().lower()
    if flag in ("0", "false", "no", "off"):
        return {"skipped": True, "reason": "CORALOGIX_TEAM_ENRICHMENT disabled"}

    company_id = os.environ.get("CORALOGIX_COMPANY_ID", "").strip()
    if not company_id:
        return {
            "skipped": True,
            "reason": "CORALOGIX_COMPANY_ID not set (required for ARR/AM/TAM enrichment textSearch filter)",
        }

    if not re.fullmatch(r"[0-9]+", company_id):
        return {
            "error": "CORALOGIX_COMPANY_ID must be numeric for team enrichment filter",
            "company_id": company_id,
            "rows": [],
        }

    host = _normalize_query_perf_api_host(os.environ.get("CORALOGIX_QUERY_PERF_API_HOST", "api.eu2.coralogix.com"))
    tier = os.environ.get("CORALOGIX_TEAM_ENRICHMENT_TIER", "TIER_ARCHIVE").strip() or "TIER_ARCHIVE"
    try:
        days = int(os.environ.get("CORALOGIX_TEAM_ENRICHMENT_DAYS", "7"))
    except ValueError:
        days = 7
    days = max(1, min(days, 90))

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)
    start_iso = _iso_z(start)
    end_iso = _iso_z(now)
    query = TEAM_ENRICHMENT_TEMPLATE.replace("{company_id}", company_id)
    auth = _query_perf_auth_header(company_id)

    try:
        raw = _post_dataprime_query_text(host, query, start_iso, end_iso, tier, auth, timeout=120)
    except Exception as e:
        return {
            "error": str(e)[:500],
            "company_id": company_id,
            "api_host": host,
            "tier": tier,
            "window_days": days,
            "window_start": start_iso,
            "window_end": end_iso,
            "rows": [],
        }

    records, stream_err, api_warnings = _parse_dataprime_ndjson_body(raw)
    parsed: List[Dict[str, Any]] = []
    for rec in records:
        row = _team_enrichment_row_from_record(rec)
        if row.get("account_arr") or row.get("account_manager") or row.get("tam") or (row.get("query_count") or 0) > 0:
            parsed.append(row)

    out: Dict[str, Any] = {
        "company_id": company_id,
        "team_id": company_id,
        "api_host": host,
        "tier": tier,
        "window_days": days,
        "window_start": start_iso,
        "window_end": end_iso,
        "row_count": len(parsed),
        "rows": parsed,
    }
    if api_warnings:
        seen: Set[str] = set()
        uniq: List[str] = []
        for w in api_warnings:
            if w and w not in seen:
                seen.add(w)
                uniq.append(w)
        out["api_warnings"] = uniq[:12]
    if stream_err and not parsed:
        out["error"] = stream_err
    elif stream_err:
        out["warning"] = stream_err
    return out


def attach_query_performance(results: Dict[str, Any]) -> None:
    """Populate results['query_performance'] for the dashboard panel."""
    qp = fetch_query_performance()
    results["query_performance"] = qp
    if qp.get("skipped"):
        print(f"  ℹ  query performance: skipped — {qp.get('reason', '')}")
    elif qp.get("error"):
        print(f"  ⚠  query performance: {str(qp.get('error'))[:100]}")
    else:
        print(f"  ℹ  query performance: {qp.get('row_count', 0)} row(s) · company {qp.get('company_id')} · {qp.get('api_host')}")


def attach_c4c_team_enrichment(results: Dict[str, Any]) -> None:
    """Populate results['c4c_team_enrichment'] for Customer info (ARR / AM / TAM)."""
    te = fetch_c4c_team_enrichment()
    results["c4c_team_enrichment"] = te
    if te.get("skipped"):
        print(f"  ℹ  C4C team enrichment: skipped — {te.get('reason', '')}")
    elif te.get("error"):
        print(f"  ⚠  C4C team enrichment: {str(te.get('error'))[:100]}")
    else:
        print(
            f"  ℹ  C4C team enrichment: {te.get('row_count', 0)} row(s) · company {te.get('company_id')} · "
            f"{te.get('window_days')}d · {te.get('api_host')}"
        )


def _dataprime_host_from_coralogix_api_base(base: str) -> str:
    """api.eu1.coralogix.com from https://api.eu1.coralogix.com/api/v2/external"""
    try:
        u = urlparse((base or "").strip())
        if u.netloc:
            return u.netloc.lower()
    except Exception:
        pass
    return "api.eu1.coralogix.com"


# Audit team account — active users (DataPrime countby on audit logs)
AUDIT_ACTIVE_USERS_QUERY = "source logs | countby $d.actorDetails.username"


def _audit_countby_row(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Map one DataPrime countby row → {username, count}."""
    m = _dataprime_merge_labels_userdata(rec)
    username: Optional[str] = None
    ad = m.get("actorDetails")
    if isinstance(ad, dict):
        u = ad.get("username")
        if u is not None and str(u).strip():
            username = str(u).strip()
    if username is None:
        for k, v in m.items():
            ks = str(k)
            if ks.lower() in ("count", "_count", "actordetails"):
                continue
            if "username" in ks.lower() or ks.endswith("actorDetails.username"):
                if v is not None and str(v).strip():
                    username = str(v).strip()
                    break
    if username is None:
        for k in ("actorDetails.username", "ActorDetails.username", "$d.actorDetails.username"):
            v = m.get(k)
            if v is not None and str(v).strip():
                username = str(v).strip()
                break
    cnt = _coerce_int(m.get("_count"))
    if cnt is None:
        cnt = _coerce_int(m.get("count"))
    if username is None or not str(username).strip():
        # Preserve rows with null username as a bucket (audit visibility)
        username = "(no username)"
    return {"username": username, "count": int(cnt) if cnt is not None else 0}


def fetch_audit_active_users() -> Dict[str, Any]:
    """
    DataPrime on the **audit** Coralogix account (separate API key per customer).
    API host follows the same domain as CORALOGIX_API_BASE (e.g. EU1).
    """
    flag = os.environ.get("CORALOGIX_AUDIT_ACTIVE_USERS", "").strip().lower()
    if flag in ("0", "false", "no", "off"):
        return {"skipped": True, "reason": "CORALOGIX_AUDIT_ACTIVE_USERS disabled"}

    audit_key = os.environ.get("CORALOGIX_AUDIT_DATAPRIME_API_KEY", "").strip()
    if not audit_key:
        return {"skipped": True, "reason": "CORALOGIX_AUDIT_DATAPRIME_API_KEY not set"}

    host = (os.environ.get("CORALOGIX_AUDIT_DATAPRIME_HOST", "").strip().lower().replace("https://", "").split("/")[0])
    if not host:
        host = _dataprime_host_from_coralogix_api_base(ALERTS_BASE)

    tier = os.environ.get("CORALOGIX_AUDIT_DATAPRIME_TIER", "TIER_ARCHIVE").strip() or "TIER_ARCHIVE"
    try:
        days = int(os.environ.get("CORALOGIX_AUDIT_ACTIVE_USERS_DAYS", "30"))
    except ValueError:
        days = 30
    days = max(1, min(days, 90))

    requested_days = days
    effective_days = days
    window_note: Optional[str] = None
    if tier == "TIER_FREQUENT_SEARCH" and days > 15:
        effective_days = 15
        window_note = (
            f"TIER_FREQUENT_SEARCH allows at most ~15 days (PT360H); "
            f"requested {requested_days}d clamped to {effective_days}d. "
            f"Use CORALOGIX_AUDIT_DATAPRIME_TIER=TIER_ARCHIVE for a full 30d window."
        )

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=effective_days)
    start_iso = _iso_z(start)
    end_iso = _iso_z(now)
    auth = f"Bearer {audit_key}"

    try:
        raw = _post_dataprime_query_text(
            host, AUDIT_ACTIVE_USERS_QUERY, start_iso, end_iso, tier, auth, timeout=180
        )
    except Exception as e:
        return {
            "error": str(e)[:500],
            "api_host": host,
            "tier": tier,
            "window_days_requested": requested_days,
            "window_days": effective_days,
            "window_start": start_iso,
            "window_end": end_iso,
            "items": [],
        }

    records, stream_err, api_warnings = _parse_dataprime_ndjson_body(raw)
    items: List[Dict[str, Any]] = []
    for rec in records:
        row = _audit_countby_row(rec)
        if row:
            items.append(row)

    items.sort(key=lambda x: (-(x.get("count") or 0), str(x.get("username") or "").lower()))
    items = items[:800]

    total_actions = sum(int(x.get("count") or 0) for x in items)

    out: Dict[str, Any] = {
        "api_host": host,
        "tier": tier,
        "window_days_requested": requested_days,
        "window_days": effective_days,
        "window_start": start_iso,
        "window_end": end_iso,
        "distinct_users": len(items),
        "total_actions": total_actions,
        "row_count": len(items),
        "items": items,
    }
    if window_note:
        out["note"] = window_note
    if api_warnings:
        seen: Set[str] = set()
        uniq: List[str] = []
        for w in api_warnings:
            if w and w not in seen:
                seen.add(w)
                uniq.append(w)
        out["api_warnings"] = uniq[:12]
    if stream_err and not items:
        out["error"] = stream_err
    elif stream_err:
        out["warning"] = stream_err
    return out


def attach_audit_active_users(results: Dict[str, Any]) -> None:
    """Populate results['audit_active_users'] for the dashboard panel."""
    au = fetch_audit_active_users()
    results["audit_active_users"] = au
    if au.get("skipped"):
        print(f"  ℹ  audit active users: skipped — {au.get('reason', '')}")
    elif au.get("error"):
        print(f"  ⚠  audit active users: {str(au.get('error'))[:100]}")
    else:
        print(
            f"  ℹ  audit active users: {au.get('distinct_users', 0)} user(s), "
            f"{au.get('total_actions', 0)} action(s) · {au.get('api_host')}"
        )


def _monday_graphql_post(query: str, variables: Dict[str, Any], timeout: int = 90) -> Dict[str, Any]:
    """POST to Monday.com GraphQL API (token is NOT the Coralogix key)."""
    token = os.environ.get("MONDAY_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError("MONDAY_API_TOKEN not set")
    url = "https://api.monday.com/v2"
    body = json.dumps({"query": query, "variables": variables}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": token,
            "Content-Type": "application/json",
            "API-Version": "2023-10",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            out = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        err_body = ""
        if e.fp:
            try:
                err_body = e.read().decode()[:400]
            except Exception:
                pass
        raise RuntimeError(f"HTTP {e.code}: {err_body}")
    errs = out.get("errors")
    if errs:
        msg = errs[0].get("message", str(errs[0])) if isinstance(errs, list) and errs else str(errs)
        raise RuntimeError(str(msg)[:500])
    return out.get("data") or {}


def _monday_fetch_board_items(board_id: str) -> Tuple[str, List[Dict[str, Any]]]:
    """Return (board_name, raw_items[]) with cursor pagination (max ~12.5k items/board)."""
    board_name = ""
    all_items: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    bid = str(board_id).strip()

    for _ in range(25):
        if cursor is None:
            q = """
            query ($ids: [ID!]) {
              boards(ids: $ids) {
                id
                name
                items_page(limit: 500) {
                  cursor
                  items {
                    id
                    name
                    group { title }
                    column_values { id type text value }
                  }
                }
              }
            }
            """
            variables: Dict[str, Any] = {"ids": [bid]}
        else:
            q = """
            query ($ids: [ID!], $cursor: String!) {
              boards(ids: $ids) {
                id
                name
                items_page(limit: 500, cursor: $cursor) {
                  cursor
                  items {
                    id
                    name
                    group { title }
                    column_values { id type text value }
                  }
                }
              }
            }
            """
            variables = {"ids": [bid], "cursor": cursor}

        data = _monday_graphql_post(q, variables)
        boards = data.get("boards") or []
        if not boards:
            break
        b0 = boards[0]
        if not board_name:
            board_name = str(b0.get("name") or "").strip()
        ip = b0.get("items_page") or {}
        chunk = ip.get("items") or []
        all_items.extend(chunk)
        next_c = ip.get("cursor")
        if not chunk or not next_c:
            break
        if next_c == cursor:
            break
        cursor = next_c

    return board_name, all_items


def _monday_fetch_board_columns(board_id: str) -> List[Dict[str, Any]]:
    """Board column metadata (id, title, type) for mapping people / text columns by title."""
    bid = str(board_id).strip()
    if not bid:
        return []
    q = """
    query ($ids: [ID!]) {
      boards(ids: $ids) {
        columns {
          id
          title
          type
        }
      }
    }
    """
    data = _monday_graphql_post(q, {"ids": [bid]})
    boards = data.get("boards") or []
    if not boards:
        return []
    cols = boards[0].get("columns")
    if not isinstance(cols, list):
        return []
    return [c for c in cols if isinstance(c, dict)]


def _monday_resolve_column_id_by_title_substring(columns: List[Dict[str, Any]], title_sub: str) -> Optional[str]:
    if not (title_sub or "").strip():
        return None
    sub_cf = str(title_sub).casefold().strip()
    for col in columns:
        t = str(col.get("title") or "").casefold()
        if sub_cf in t:
            cid = str(col.get("id") or "").strip()
            if cid:
                return cid
    return None


def _monday_resolve_column_id_by_title_word(columns: List[Dict[str, Any]], word: str) -> Optional[str]:
    """Match column title containing ``word`` as a whole token (avoids \"Source\" matching \"SRC\")."""
    w = str(word or "").strip()
    if not w:
        return None
    try:
        pat = re.compile(r"(?i)\b" + re.escape(w) + r"\b")
    except re.error:
        return None
    for col in columns:
        title = str(col.get("title") or "")
        if pat.search(title):
            cid = str(col.get("id") or "").strip()
            if cid:
                return cid
    return None


def _monday_people_column_ids_for_board(columns: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    """
    Resolve Monday column ids for DevOps assignee and SRC Consultant.
    Optional explicit ids: MONDAY_DEVOPS_COLUMN_ID, MONDAY_SRC_COLUMN_ID.
    Else match column title containing MONDAY_DEVOPS_COLUMN_TITLE (default: DevOps — matches
    \"DevOps\", \"DevOps assignee\", etc.). SRC tries MONDAY_SRC_COLUMN_TITLE if set, then
    \"SRC Consultant\", then \"SRC\" (short column title \"SRC\" does not contain \"src consultant\").
    """
    dev_id = os.environ.get("MONDAY_DEVOPS_COLUMN_ID", "").strip()
    src_id = os.environ.get("MONDAY_SRC_COLUMN_ID", "").strip()
    if dev_id and src_id:
        return dev_id, src_id
    # Short default: column titled exactly "DevOps" does not contain substring "devops assignee".
    dev_t = os.environ.get("MONDAY_DEVOPS_COLUMN_TITLE", "DevOps").strip() or "DevOps"
    src_env = os.environ.get("MONDAY_SRC_COLUMN_TITLE", "").strip()
    if not dev_id:
        dev_id = _monday_resolve_column_id_by_title_substring(columns, dev_t)
    if not src_id:
        if src_env:
            src_id = _monday_resolve_column_id_by_title_substring(columns, src_env)
        if not src_id:
            src_id = _monday_resolve_column_id_by_title_substring(columns, "SRC Consultant")
        if not src_id:
            src_id = _monday_resolve_column_id_by_title_word(columns, "SRC")
    return dev_id or None, src_id or None


def _monday_cv_display_text(cv: Dict[str, Any]) -> str:
    """Monday `text` is often empty for People columns; fall back to `value` JSON (names when present)."""
    if not isinstance(cv, dict):
        return ""
    tx = str(cv.get("text") or "").strip()
    if tx:
        return tx
    raw = cv.get("value")
    if raw is None:
        return ""
    if isinstance(raw, dict):
        obj: Any = raw
    else:
        s = str(raw).strip()
        if not s:
            return ""
        try:
            obj = json.loads(s)
        except json.JSONDecodeError:
            return ""
    if not isinstance(obj, dict):
        return ""
    pts = obj.get("personsAndTeams")
    if isinstance(pts, list):
        names: List[str] = []
        for p in pts:
            if not isinstance(p, dict):
                continue
            nm = str(p.get("name") or p.get("title") or "").strip()
            if nm:
                names.append(nm)
        if names:
            return ", ".join(names)
    return ""


def _monday_text_for_column_id(item: Dict[str, Any], col_id: Optional[str]) -> str:
    if not col_id:
        return ""
    cid = str(col_id).strip()
    for cv in item.get("column_values") or []:
        if not isinstance(cv, dict):
            continue
        if str(cv.get("id") or "").strip() == cid:
            return _monday_cv_display_text(cv)
    return ""


def _monday_merge_parent_people_into_subitem_column_values(
    parent: Dict[str, Any],
    sub: Dict[str, Any],
    devops_column_id: Optional[str],
    src_column_id: Optional[str],
) -> List[Dict[str, Any]]:
    """
    Subitems under e.g. \"Integrations\" usually have Status per integration but DevOps/SRC live on the **parent** row only.
    Copy parent text/value for those column ids into the synthetic row so refresh + dashboard see assignees.
    """
    inherit: Set[str] = set()
    for x in (devops_column_id, src_column_id):
        xs = str(x or "").strip()
        if xs:
            inherit.add(xs)
    if not inherit:
        return [dict(cv) for cv in (sub.get("column_values") or []) if isinstance(cv, dict)]

    parent_by_id = {
        str(cv.get("id") or "").strip(): cv
        for cv in (parent.get("column_values") or [])
        if isinstance(cv, dict) and str(cv.get("id") or "").strip()
    }
    out: List[Dict[str, Any]] = []
    seen_inherit: Set[str] = set()
    for cv in sub.get("column_values") or []:
        if not isinstance(cv, dict):
            continue
        nc = dict(cv)
        kid = str(nc.get("id") or "").strip()
        if kid in inherit and not _monday_cv_display_text(nc).strip():
            p_cv = parent_by_id.get(kid)
            if p_cv:
                p_txt = _monday_cv_display_text(p_cv)
                if p_txt:
                    nc["text"] = p_txt
                    if not nc.get("type"):
                        nc["type"] = str(p_cv.get("type") or "")
        out.append(nc)
        if kid in inherit:
            seen_inherit.add(kid)
    for kid in inherit:
        if kid in seen_inherit:
            continue
        p_cv = parent_by_id.get(kid)
        if not p_cv:
            continue
        p_txt = _monday_cv_display_text(p_cv)
        if not p_txt:
            continue
        out.append(
            {
                "id":   kid,
                "type": str(p_cv.get("type") or ""),
                "text": p_txt,
            }
        )
    return out


def _monday_fetch_subitems_by_parent_ids(parent_rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch subitems only for given parent rows (small batched queries).
    Avoids requesting subitems on every items_page row (timeouts on large boards).
    """
    out: Dict[str, List[Dict[str, Any]]] = {}
    if not parent_rows:
        return out
    chunk_size = 40
    for i in range(0, len(parent_rows), chunk_size):
        chunk = parent_rows[i : i + chunk_size]
        ids = [str(p.get("id") or "").strip() for p in chunk if str(p.get("id") or "").strip()]
        if not ids:
            continue
        q = """
        query ($ids: [ID!]!) {
          items(ids: $ids) {
            id
              subitems {
              id
              name
              column_values { id type text value }
            }
          }
        }
        """
        data = _monday_graphql_post(q, {"ids": ids})
        for node in data.get("items") or []:
            if not isinstance(node, dict):
                continue
            pid = str(node.get("id") or "")
            raw = node.get("subitems")
            subs = raw if isinstance(raw, list) else []
            out[pid] = [s for s in subs if isinstance(s, dict)]
    return out


def _monday_group_title(item: Dict[str, Any]) -> str:
    g = item.get("group")
    if isinstance(g, dict):
        return str(g.get("title") or "").strip()
    return ""


def _monday_status_text(item: Dict[str, Any]) -> str:
    """First non-empty Status column (Monday `type` is usually `status`)."""
    for cv in item.get("column_values") or []:
        if not isinstance(cv, dict):
            continue
        if str(cv.get("type") or "").lower() != "status":
            continue
        tx = str(cv.get("text") or "").strip()
        if tx:
            return tx
    return ""


def _monday_parse_comma_list(raw: str) -> List[str]:
    return [x.strip() for x in raw.replace(";", ",").split(",") if x.strip()]


def _monday_group_title_has_excluded_substring(gt_cf: str, exclude_tokens: List[str]) -> bool:
    """True if group title (already lowercased) contains any exclude token as a substring."""
    for tok in exclude_tokens:
        if str(tok).casefold() in gt_cf:
            return True
    return False


def _monday_item_filters_from_env() -> Dict[str, Any]:
    """
    Optional filters (AND logic):
      MONDAY_FILTER_GROUP_NAMES    — comma-separated; group title must contain at least one token (case-insensitive).
      MONDAY_GROUP_TITLE_CONTAINS  — group title must also contain this substring (e.g. Integration), all rows including subitems.
      MONDAY_GROUP_TITLE_EXCLUDE   — comma-separated substrings; group title must not contain any (excludes e.g. Discovery column).
      MONDAY_PARENT_ITEM_CONTAINS  — if set, parents whose **item name** matches this substring may expand **subitems**
                                     into output rows (subitem status/group filters apply to each subitem; parent row is
                                     not emitted from expansion). Use when “Integrations” is a parent item, not a group.
      MONDAY_STATUS_VALUES         — comma-separated allowed Status labels; if set, row must match one (e.g. Done).
    """
    gn = os.environ.get("MONDAY_FILTER_GROUP_NAMES", "").strip()
    group_tokens = _monday_parse_comma_list(gn)
    contains = os.environ.get("MONDAY_GROUP_TITLE_CONTAINS", "").strip()
    exclude_raw = os.environ.get("MONDAY_GROUP_TITLE_EXCLUDE", "").strip()
    exclude_tokens = _monday_parse_comma_list(exclude_raw)
    pic = os.environ.get("MONDAY_PARENT_ITEM_CONTAINS", "").strip()
    sv = os.environ.get("MONDAY_STATUS_VALUES", "").strip()
    status_cf: Optional[Set[str]] = None
    if sv:
        status_cf = {x.strip().casefold() for x in sv.replace(";", ",").split(",") if x.strip()}
    return {
        "group_names_any":         group_tokens,
        "group_title_contains":    contains or None,
        "group_title_exclude_any": exclude_tokens,
        "parent_item_contains":    pic or None,
        "status_allowed_cf":       status_cf,
    }


def _monday_parent_group_matches_for_expansion(parent: Dict[str, Any], filt: Dict[str, Any]) -> bool:
    """Group / group-title rules on the parent row only (ignore status — section headers often have no status)."""
    gt = _monday_group_title(parent).casefold()
    excl: List[str] = filt.get("group_title_exclude_any") or []
    if excl and _monday_group_title_has_excluded_substring(gt, excl):
        return False
    tokens: List[str] = filt.get("group_names_any") or []
    if tokens:
        if not any(tok.casefold() in gt for tok in tokens):
            return False
    sub = filt.get("group_title_contains")
    if sub:
        if str(sub).casefold() not in gt:
            return False
    return True


def _monday_subitem_as_top_level_item(
    parent: Dict[str, Any],
    sub: Dict[str, Any],
    *,
    devops_column_id: Optional[str] = None,
    src_column_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Subitem inherits parent's board group for display; carries parent item name for UI."""
    g = parent.get("group")
    if not isinstance(g, dict):
        g = {}
    merged_cvs = _monday_merge_parent_people_into_subitem_column_values(
        parent, sub, devops_column_id, src_column_id
    )
    return {
        "id":                   sub.get("id"),
        "name":                 sub.get("name"),
        "group":                dict(g),
        "column_values":        merged_cvs,
        "__monday_parent_name": str(parent.get("name") or "").strip(),
    }


def _monday_item_passes_filters(item: Dict[str, Any], filt: Dict[str, Any]) -> bool:
    gt = _monday_group_title(item).casefold()
    is_sub = bool(str(item.get("__monday_parent_name") or "").strip())
    excl: List[str] = filt.get("group_title_exclude_any") or []
    if excl and _monday_group_title_has_excluded_substring(gt, excl):
        return False
    tokens: List[str] = filt.get("group_names_any") or []
    if tokens:
        if not any(tok.casefold() in gt for tok in tokens):
            return False
    sub = filt.get("group_title_contains")
    if sub:
        if str(sub).casefold() not in gt:
            return False
    pic = filt.get("parent_item_contains")
    if pic and is_sub:
        if str(pic).casefold() not in str(item.get("__monday_parent_name") or "").casefold():
            return False
    allowed: Optional[Set[str]] = filt.get("status_allowed_cf")
    if allowed is not None:
        st = _monday_status_text(item).strip().casefold()
        if not st or st not in allowed:
            return False
    return True


def _monday_row_from_item(
    item: Dict[str, Any],
    *,
    board_name: str,
    board_id: str,
    devops_column_id: Optional[str] = None,
    src_column_id: Optional[str] = None,
) -> Dict[str, Any]:
    name = str(item.get("name") or "Item").strip()
    grp = _monday_group_title(item)
    tags: List[str] = ["Monday"]
    if board_name:
        tags.append(board_name[:80])
    if grp:
        tags.append(grp[:80])
    status_txt = _monday_status_text(item)
    parent_name = str(item.get("__monday_parent_name") or "").strip()
    row: Dict[str, Any] = {
        "catalog":           "monday",
        "name":              name[:500],
        "tags":              tags[:16],
        "connections":       None,
        "upgrade":           False,
        "monday_item_id":    str(item.get("id") or ""),
        "monday_board_id":   board_id,
        "monday_board_name": board_name[:200],
        "monday_group":      grp[:300],
        "status":            status_txt[:200],
    }
    if parent_name:
        row["monday_parent_item"] = parent_name[:200]
    dev_txt = _monday_text_for_column_id(item, devops_column_id)
    src_txt = _monday_text_for_column_id(item, src_column_id)
    if dev_txt:
        row["monday_devops_assignee"] = dev_txt[:300]
    if src_txt:
        row["monday_src_consultant"] = src_txt[:300]
    return row


def fetch_monday_security_sources() -> Dict[str, Any]:
    """
    Optional inventory from Monday.com board(s) — merged in the dashboard with
    Coralogix integrations (direct connections).

    Env:
      MONDAY_API_TOKEN   — API v2 token (Developers → API)
      MONDAY_BOARD_ID    — numeric board id, or comma-separated ids

    Optional filters (AND):
      MONDAY_FILTER_GROUP_NAMES    — customer / group: comma-separated tokens; item's Monday **group title**
                                     must contain at least one (case-insensitive substring).
      MONDAY_GROUP_TITLE_CONTAINS  — e.g. Integration; group title must also contain this substring (all rows, including subitems).
      MONDAY_GROUP_TITLE_EXCLUDE — e.g. Discovery; drop rows whose group title contains any of these substrings (other columns on the same board).
      MONDAY_PARENT_ITEM_CONTAINS  — e.g. Integrations; expand **subitems** under parents whose **item name** contains
                                     this substring (parent must pass group / group-title rules above). When set, **only**
                                     those subitem rows are emitted (other top-level rows like Discovery are skipped).
                                     Subitems inherit the parent's group for display; Status filter applies to each subitem.
                                     DevOps / SRC columns are copied from the **parent** row when the subitem has no value.
      MONDAY_STATUS_VALUES         — e.g. Done or Done,Complete; Status column must match one (case-insensitive).
                                     If unset, no status filter (all statuses included).

    People columns (optional; matched per board via column title substring or explicit id):
      MONDAY_DEVOPS_COLUMN_TITLE   — default DevOps (column title must contain this; use DevOps assignee if your title differs)
      MONDAY_SRC_COLUMN_TITLE      — optional; else try title containing SRC Consultant, then whole-word SRC
      MONDAY_DEVOPS_COLUMN_ID      — optional Monday column id (overrides title match for DevOps)
      MONDAY_SRC_COLUMN_ID         — optional Monday column id for SRC Consultant
    """
    token = os.environ.get("MONDAY_API_TOKEN", "").strip()
    raw_ids = os.environ.get("MONDAY_BOARD_ID", "").strip()
    if not token or not raw_ids:
        return {
            "skipped": True,
            "count":   0,
            "items":   [],
            "note":    "Set MONDAY_API_TOKEN and MONDAY_BOARD_ID in .env to list Monday items "
                       "alongside Coralogix integrations.",
        }

    filt = _monday_item_filters_from_env()
    sv_raw = os.environ.get("MONDAY_STATUS_VALUES", "").strip()
    pic_raw = os.environ.get("MONDAY_PARENT_ITEM_CONTAINS", "").strip()
    status_display = [
        x.strip() for x in sv_raw.replace(";", ",").split(",") if x.strip()
    ] if sv_raw else None
    exclude_display = _monday_parse_comma_list(os.environ.get("MONDAY_GROUP_TITLE_EXCLUDE", "").strip())
    filter_meta = {
        "group_names_any":          filt["group_names_any"],
        "group_title_contains":     filt["group_title_contains"],
        "group_title_exclude_any":  exclude_display or None,
        "parent_item_contains":     pic_raw or None,
        "only_matching_parent_subitems": bool(pic_raw),
        "status_values":            status_display,
    }

    board_ids = [x.strip() for x in raw_ids.replace(";", ",").split(",") if x.strip()]
    out_items: List[Dict[str, Any]] = []
    board_labels: List[str] = []
    raw_total = 0
    subitems_scanned = 0
    skipped_filters = 0

    try:
        for bid in board_ids:
            bname, raw_items = _monday_fetch_board_items(bid)
            board_columns = _monday_fetch_board_columns(bid)
            dev_col_id, src_col_id = _monday_people_column_ids_for_board(board_columns)
            board_labels.append(f"{bname or bid} ({bid})")
            pic = filt.get("parent_item_contains")
            subitems_by_parent: Dict[str, List[Dict[str, Any]]] = {}
            if pic:
                expand_parents: List[Dict[str, Any]] = []
                for it in raw_items:
                    if not isinstance(it, dict):
                        continue
                    if not _monday_parent_group_matches_for_expansion(it, filt):
                        continue
                    if str(pic).casefold() not in str(it.get("name") or "").casefold():
                        continue
                    expand_parents.append(it)
                subitems_by_parent = _monday_fetch_subitems_by_parent_ids(expand_parents)

            for it in raw_items:
                if not isinstance(it, dict):
                    continue
                raw_total += 1
                pid = str(it.get("id") or "")
                subs = subitems_by_parent.get(pid, []) if pic else []
                expanded = False
                if pic and subs and _monday_parent_group_matches_for_expansion(it, filt):
                    pname = str(it.get("name") or "").casefold()
                    if str(pic).casefold() in pname:
                        expanded = True
                        for si in subs:
                            if not isinstance(si, dict):
                                continue
                            subitems_scanned += 1
                            merged = _monday_subitem_as_top_level_item(
                                it,
                                si,
                                devops_column_id=dev_col_id,
                                src_column_id=src_col_id,
                            )
                            if not _monday_item_passes_filters(merged, filt):
                                skipped_filters += 1
                                continue
                            out_items.append(
                                _monday_row_from_item(
                                    merged,
                                    board_name=bname,
                                    board_id=bid,
                                    devops_column_id=dev_col_id,
                                    src_column_id=src_col_id,
                                )
                            )
                if expanded:
                    continue
                if pic:
                    # Parent filter mode: output only expanded subitems (e.g. Integrations), not Discovery/Platform rows.
                    continue
                if not _monday_item_passes_filters(it, filt):
                    skipped_filters += 1
                    continue
                out_items.append(
                    _monday_row_from_item(
                        it,
                        board_name=bname,
                        board_id=bid,
                        devops_column_id=dev_col_id,
                        src_column_id=src_col_id,
                    )
                )
    except Exception as e:
        return {
            "skipped":   False,
            "error":     str(e)[:500],
            "count":     0,
            "items":     [],
            "board_ids": board_ids,
            "filters":   filter_meta,
        }

    note_parts = [
        "Rows from Monday.com boards; not Coralogix deployment state.",
    ]
    if skipped_filters:
        note_parts.append(
            f"Filtered out {skipped_filters} of {raw_total + subitems_scanned} candidate row(s) "
            f"({raw_total} top-level + {subitems_scanned} subitem(s); rules in .env)."
        )
    if (
        filter_meta["group_names_any"]
        or filter_meta["group_title_contains"]
        or filter_meta["group_title_exclude_any"]
        or filter_meta["parent_item_contains"]
        or filter_meta["status_values"]
    ):
        note_parts.append("Filters active — see data.json monday_security_sources.filters.")

    return {
        "skipped":          False,
        "error":            None,
        "count":            len(out_items),
        "items":            out_items[:2000],
        "board_ids":        board_ids,
        "board_names":      board_labels,
        "fetched_at":       datetime.now(timezone.utc).isoformat(),
        "filters":          filter_meta,
        "items_raw_total":       raw_total,
        "subitems_scanned_total": subitems_scanned,
        "items_after_filter":    len(out_items),
        "note":             " ".join(note_parts),
    }


def attach_monday_security_sources(results: Dict[str, Any]) -> None:
    """Always refresh monday_security_sources when credentials exist (independent of --section)."""
    try:
        ms = fetch_monday_security_sources()
    except Exception as e:
        results["monday_security_sources"] = {
            "skipped": False,
            "error":   str(e)[:500],
            "count":   0,
            "items":   [],
        }
        print(f"  ⚠  monday.com: {str(e)[:90]}")
        return

    results["monday_security_sources"] = ms
    if ms.get("skipped"):
        return
    if ms.get("error"):
        print(f"  ⚠  monday.com: {str(ms.get('error'))[:90]}")
        return
    print(
        f"  ℹ  monday.com: {ms.get('count', 0)} item(s) from "
        f"{len(ms.get('board_ids') or [])} board(s)"
    )


def _parsable_last_trigger_value(val: Any) -> bool:
    """
    True if alerts.items lastTriggered should be treated as a real fire time
    (matches dashboard: empty / 'never triggered' / unparseable → no trigger).
    """
    if val is None:
        return False
    if isinstance(val, (int, float)):
        return bool(val)
    s = str(val).strip()
    if not s:
        return False
    if re.match(r"^never\s*triggered$", s, re.I):
        return False
    s_iso = s.replace("Z", "+00:00")
    try:
        datetime.fromisoformat(s_iso)
        return True
    except ValueError:
        pass
    if s.isdigit() and len(s) >= 10:
        return True
    return False


def _coerce_trigger_timestamp_iso(val: Any) -> Optional[str]:
    """Normalize API last-trigger fields to UTC ISO8601 ending in Z (for dashboard JS)."""
    if val is None or val == "":
        return None
    if isinstance(val, str):
        s = val.strip()
        return s if s else None
    if isinstance(val, (int, float)):
        ts = float(val)
        if ts > 1e12:  # milliseconds
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except (OSError, ValueError, OverflowError):
            return None
    return None


def _extract_last_trigger_iso(alert_obj: Dict[str, Any]) -> Optional[str]:
    """Best-effort: Coralogix alert objects vary; scan common keys and nested stats."""
    keys = (
        "last_triggered",
        "lastTriggerTime",
        "last_trigger_time",
        "last_fired",
        "lastFired",
        "lastFiredTime",
        "last_fired_time",
        "last_evaluation_time",
        "lastEvaluationTime",
        "lastNotifiedTime",
        "last_notified_time",
        "lastTriggeredTime",
        "last_triggered_time",
    )
    for k in keys:
        iso = _coerce_trigger_timestamp_iso(alert_obj.get(k))
        if iso:
            return iso
    for nested_key in ("statistics", "stats", "alert_statistics", "execution_stats", "notification"):
        nested = alert_obj.get(nested_key)
        if isinstance(nested, dict):
            found = _extract_last_trigger_iso(nested)
            if found:
                return found
    return _harvest_fired_timestamp_from_json(alert_obj)


def _harvest_fired_timestamp_from_json(obj: Any, max_depth: int = 6) -> Optional[str]:
    """
    Walk alert JSON and pick the strongest ISO-like timestamp on keys that suggest
    'last fired / triggered / evaluated' (external /alerts schema varies by version).
    """
    best_prio = -1
    best_ts = 0.0
    best_iso = ""

    def consider(key: str, val: Any) -> None:
        nonlocal best_prio, best_ts, best_iso
        iso = _coerce_trigger_timestamp_iso(val)
        if not iso:
            return
        lk = re.sub(r"[^a-z0-9]", "", str(key).lower())
        prio = 0
        if any(x in lk for x in ("lasttrigger", "lastfired", "lastfire", "lastevaluation", "lastnotified")):
            prio = 100
        elif "trigger" in lk or "fired" in lk:
            prio = 60
        elif "evaluation" in lk or "evaluated" in lk:
            prio = 40
        elif "notif" in lk:
            prio = 30
        elif any(x in lk for x in ("updated", "modified", "laststate")):
            prio = 10
        ts = _iso_ts_for_sort(iso)
        if prio > best_prio or (prio == best_prio and ts > best_ts):
            best_prio, best_ts, best_iso = prio, ts, iso

    def walk(o: Any, depth: int) -> None:
        if depth > max_depth or o is None:
            return
        if isinstance(o, dict):
            for k, v in o.items():
                consider(k, v)
                walk(v, depth + 1)
        elif isinstance(o, list):
            for it in o[:80]:
                walk(it, depth + 1)

    walk(obj, 0)
    # Do not treat generic "created" timestamps as last-triggered unless no better signal exists
    if best_prio < 10:
        return None
    return best_iso or None


_MITRE_TACTIC_ID_RE = re.compile(r"\b(TA\d{4})\b", re.I)


def _mitre_tactic_id_from_text(raw: str) -> str:
    """Normalize to lowercase ``ta0007`` from meta value or embedded ``TA0007/…`` string."""
    s = str(raw or "").strip()
    if not s:
        return ""
    m = _MITRE_TACTIC_ID_RE.search(s)
    if m:
        return m.group(1).lower()
    sl = s.lower().replace(" ", "")
    if re.match(r"^ta\d{4}$", sl):
        return sl
    return ""


def _mitre_tactic_from_alert_meta(labels: Dict[str, Any]) -> str:
    """Tactic id from definition ``meta_labels`` (``mitre_tactic``, etc.)."""
    if not isinstance(labels, dict):
        return ""
    for mk in (
        "mitre_tactic",
        "mitreTactic",
        "MitreTactic",
        "mitre_tactics",
        "MITRE_Tactic",
    ):
        v = labels.get(mk)
        if v is None:
            continue
        t = _mitre_tactic_id_from_text(str(v))
        if t:
            return t
    return ""


def _application_name_from_alert_meta(labels: Dict[str, Any]) -> str:
    """Application / log source from alert definition meta_labels (Coralogix)."""
    if not isinstance(labels, dict):
        return ""
    for mk in (
        "application_name",
        "applicationName",
        "ApplicationName",
        "cx_application",
        "application",
        "subsystem",
        "integration_name",
        "IntegrationName",
    ):
        v = labels.get(mk)
        if isinstance(v, str) and v.strip():
            t = v.strip()
            if not _looks_like_field_path(t):
                return t[:200]
    return ""


_ALERT_QUERY_SKIP_KEY_SUBSTR = (
    "promql",
    "webhook",
    "emailaddress",
    "slack",
    "pagerduty",
    "opsgenie",
)

# Branches to skip when deep-scanning raw alert JSON for Lucene-like strings.
_ALERT_DEEP_SKIP_KEYS = frozenset(
    {
        "notifications",
        "notification",
        "meta_labels",
        "metaLabels",
        "emails",
        "integrations",
        "description",
        "name",
        "id",
        "alert_id",
        "alertId",
        "unique_identifier",
        "uniqueIdentifier",
        "user_id",
        "userId",
    }
)

# Lucene field atom: include hyphen (e.g. cs-uri-stem, sc-status). Dots separate nested paths.
_LC_ATOM = r"(?:\$?[a-zA-Z_][\w\-]*)"

_RE_LUCENE_FIELD_COLON_QUOTE = re.compile(r'[a-zA-Z_$][\w$.\\-]{0,160}\s*:\s*"')

_LUCENE_FIELD_STOPWORDS = frozenset(
    {
        "and",
        "or",
        "not",
        "to",
        "as",
        "in",
        "filter",
        "true",
        "false",
        "null",
        "exists",
        "missing",
        "is",
        "keywords",
        "http",
        "https",
    }
)

_RE_LUCENE_FIELD_BEFORE_QUOTE = re.compile(
    rf'(?<![\w.$])({_LC_ATOM}(?:\.{_LC_ATOM})*(?:\.(?:numeric|keyword))?)\s*:\s*"'
)
_RE_LUCENE_FIELD_BEFORE_PAREN = re.compile(
    rf'(?<![\w.$])({_LC_ATOM}(?:\.{_LC_ATOM})*(?:\.(?:numeric|keyword))?)\s*:\s*\('
)
# Numeric or string range: sc-status.numeric:[200 TO 399]
_RE_LUCENE_FIELD_BEFORE_BRACKET = re.compile(
    rf'(?<![\w.$])({_LC_ATOM}(?:\.{_LC_ATOM})*(?:\.(?:numeric|keyword))?)\s*:\s*\['
)
_RE_LUCENE_FIELD_WORD_VALUE = re.compile(
    rf'(?<![\w.$])({_LC_ATOM}(?:\.{_LC_ATOM})*(?:\.(?:numeric|keyword))?)\s*:\s*'
    r'([a-zA-Z0-9_.\-]+)(?=\s|$|\)|\])'
)


def _strip_lucene_type_suffixes(field: str) -> str:
    f = (field or "").strip()
    if not f:
        return ""
    changed = True
    while changed:
        changed = False
        for suf in (".numeric", ".keyword"):
            if f.endswith(suf):
                f = f[: -len(suf)]
                changed = True
    return f


def _normalize_alert_query_field_key(raw: str) -> Optional[str]:
    k = _strip_lucene_type_suffixes(raw)
    if not k:
        return None
    if k.startswith("_"):
        return None
    if k.lower() in _LUCENE_FIELD_STOPWORDS:
        return None
    if k.replace(".", "").replace("$", "").isdigit():
        return None
    return k


def _field_path_for_exists_check(k: str) -> str:
    """Map Lucene / DataPrime-style path to ``_exists_`` target (strip ``$d.`` / ``$l.`` prefixes)."""
    k = (k or "").strip()
    for pref in ("$d.", "$l.", "$m."):
        if k.startswith(pref):
            return k[len(pref) :]
    return k


def _walk_alert_query_string_fragments(obj: Any, parts: List[str], depth: int = 0) -> None:
    if depth > 14:
        return
    if isinstance(obj, dict):
        for key, val in obj.items():
            ks = str(key).lower()
            if any(s in ks for s in _ALERT_QUERY_SKIP_KEY_SUBSTR):
                continue
            if isinstance(val, str) and len(val.strip()) > 6:
                if any(
                    h in ks
                    for h in (
                        "lucene",
                        "query",
                        "filter",
                        "text",
                        "dataprime",
                        "ocp",
                        "log",
                    )
                ):
                    if "notification" not in ks:
                        parts.append(val.strip())
            _walk_alert_query_string_fragments(val, parts, depth + 1)
    elif isinstance(obj, list):
        for it in obj[:400]:
            _walk_alert_query_string_fragments(it, parts, depth + 1)


def _string_looks_like_lucene_snippet(s: str) -> bool:
    """True if ``s`` likely contains Lucene ``field.path:\"value\"`` (REST nests queries under varying keys)."""
    if not s or len(s.strip()) < 10:
        return False
    if _RE_LUCENE_FIELD_COLON_QUOTE.search(s):
        return True
    if re.search(r"[a-zA-Z_$][\w$.\\-]{1,120}\s*:\s*\(", s):
        return True
    if re.search(r"[a-zA-Z_$][\w$.\\-]{1,120}\s*:\s*\[", s):
        return True
    return False


def _walk_alert_deep_lucene_strings(obj: Any, parts: List[str], depth: int = 0) -> None:
    if depth > 22:
        return
    if isinstance(obj, dict):
        for key, val in obj.items():
            ks = str(key)
            kl = ks.lower()
            if ks in _ALERT_DEEP_SKIP_KEYS or kl in _ALERT_DEEP_SKIP_KEYS:
                continue
            if any(s in kl for s in _ALERT_QUERY_SKIP_KEY_SUBSTR):
                continue
            if isinstance(val, str) and _string_looks_like_lucene_snippet(val):
                parts.append(val.strip())
            _walk_alert_deep_lucene_strings(val, parts, depth + 1)
    elif isinstance(obj, list):
        for it in obj[:500]:
            _walk_alert_deep_lucene_strings(it, parts, depth + 1)


def _alert_api_query_text_blob(alert_row: Dict[str, Any]) -> str:
    parts: List[str] = []
    _walk_alert_query_string_fragments(alert_row.get("condition"), parts)
    _walk_alert_query_string_fragments(alert_row.get("filters"), parts)
    _walk_alert_query_string_fragments(alert_row.get("tracingAlert"), parts)
    _walk_alert_query_string_fragments(alert_row.get("log_filter"), parts)
    _walk_alert_query_string_fragments(alert_row.get("logFilter"), parts)
    for top_key in (
        "luceneQuery",
        "lucene_query",
        "queryText",
        "query_text",
        "logQuery",
        "filterQuery",
        "searchQuery",
    ):
        tv = alert_row.get(top_key) if isinstance(alert_row, dict) else None
        if isinstance(tv, str) and tv.strip():
            parts.append(tv.strip())
    if isinstance(alert_row, dict):
        _walk_alert_deep_lucene_strings(alert_row, parts, 0)
    seen: Set[str] = set()
    out: List[str] = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return "\n".join(out)


def _extract_field_keys_from_lucene_like_text(text: str) -> List[str]:
    if not text or not isinstance(text, str):
        return []
    found: List[str] = []
    seen: Set[str] = set()

    def _add_raw(raw: str) -> None:
        norm = _normalize_alert_query_field_key(raw)
        if not norm or norm in seen:
            return
        seen.add(norm)
        found.append(norm)

    for m in _RE_LUCENE_FIELD_BEFORE_QUOTE.finditer(text):
        _add_raw(m.group(1))
    for m in _RE_LUCENE_FIELD_BEFORE_PAREN.finditer(text):
        _add_raw(m.group(1))
    for m in _RE_LUCENE_FIELD_BEFORE_BRACKET.finditer(text):
        _add_raw(m.group(1))
    for m in _RE_LUCENE_FIELD_WORD_VALUE.finditer(text):
        _add_raw(m.group(1))
    return found


def _alert_query_field_keys_for_api_row(alert_row: Dict[str, Any]) -> List[str]:
    blob = _alert_api_query_text_blob(alert_row)
    return _extract_field_keys_from_lucene_like_text(blob)


def _lucene_clause_escape_for_dataprime(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "\\'")


def _dataprime_first_count(raw: str) -> Tuple[Optional[int], Optional[str]]:
    """Parse DataPrime NDJSON body → (count, error)."""
    recs, err, _ = _parse_dataprime_ndjson_body(raw)
    if err:
        return None, err
    total: Optional[int] = None
    for rec in recs:
        m = _dataprime_merge_labels_userdata(rec)
        for key in ("_count", "count", "Count"):
            c = _coerce_int(m.get(key))
            if c is not None:
                total = c if total is None else total + c
    if total is None:
        if not recs:
            return 0, None
        return None, "no count field in DataPrime result"
    return total, None


def attach_alert_query_field_validation(results: Dict[str, Any]) -> None:
    """
    For each alert definition, extract Lucene-style field paths from the API payload,
    then run DataPrime ``source logs | lucene '_exists_:field' | count`` over a window.
    Keys ending in ``.numeric`` / ``.keyword`` are normalized away before ``_exists_``.

    Controlled by CORALOGIX_ALERT_QUERY_FIELD_VALIDATE=1 (off by default — many DataPrime calls).
    """
    flag = os.environ.get("CORALOGIX_ALERT_QUERY_FIELD_VALIDATE", "").strip().lower()
    if flag not in ("1", "true", "yes", "on"):
        results["alert_query_field_validation"] = {
            "skipped": True,
            "reason": "set CORALOGIX_ALERT_QUERY_FIELD_VALIDATE=1 to run (issues many DataPrime queries)",
        }
        return

    al = results.get("alerts")
    if not isinstance(al, dict) or al.get("error"):
        results["alert_query_field_validation"] = {
            "skipped": True,
            "reason": "alerts section missing or error",
        }
        return
    items = al.get("items")
    if not isinstance(items, list) or not items:
        results["alert_query_field_validation"] = {
            "skipped": True,
            "reason": "no alerts.items — run full refresh or --section alerts",
        }
        return

    only_enabled = os.environ.get(
        "CORALOGIX_ALERT_QUERY_FIELD_VALIDATE_ENABLED_ONLY", "1"
    ).strip().lower() not in ("0", "false", "no")

    try:
        days = int(os.environ.get("CORALOGIX_ALERT_QUERY_FIELD_VALIDATE_DAYS", "7"))
    except ValueError:
        days = 7
    days = max(1, min(days, 90))

    tier = (
        os.environ.get("CORALOGIX_ALERT_QUERY_FIELD_VALIDATE_TIER", "TIER_ARCHIVE")
        .strip()
        or "TIER_ARCHIVE"
    )
    requested_days = days
    effective_days = days
    window_note: Optional[str] = None
    if tier == "TIER_FREQUENT_SEARCH" and days > 15:
        effective_days = 15
        window_note = (
            f"TIER_FREQUENT_SEARCH allows at most ~15d; requested {requested_days}d "
            f"clamped to {effective_days}d. Use TIER_ARCHIVE for a full {requested_days}d window."
        )

    try:
        max_fields = int(
            os.environ.get("CORALOGIX_ALERT_QUERY_FIELD_VALIDATE_MAX_FIELDS", "500")
        )
    except ValueError:
        max_fields = 500
    max_fields = max(0, max_fields)

    try:
        timeout_sec = int(
            os.environ.get("CORALOGIX_ALERT_QUERY_FIELD_VALIDATE_TIMEOUT_SEC", "90")
        )
    except ValueError:
        timeout_sec = 90
    timeout_sec = max(15, min(timeout_sec, 600))

    sleep_sec = float(
        os.environ.get("CORALOGIX_ALERT_QUERY_FIELD_VALIDATE_SLEEP_SEC", "0.12")
    )

    now = datetime.now(timezone.utc)
    start_iso = _iso_z(now - timedelta(days=effective_days))
    end_iso = _iso_z(now)
    host = _dataprime_host_from_coralogix_api_base(ALERTS_BASE)
    auth = f"Bearer {API_KEY}"

    alert_entries: List[Tuple[str, str, str, List[str]]] = []
    unique_order: List[str] = []
    uniq_seen: Set[str] = set()

    for it in items:
        if not isinstance(it, dict):
            continue
        if only_enabled and not it.get("enabled", True):
            continue
        keys = it.get("queryFieldKeys")
        if not isinstance(keys, list) or not keys:
            continue
        clean_keys = [str(k).strip() for k in keys if str(k).strip()]
        if not clean_keys:
            continue
        aid = str(it.get("id") or "")
        aname = str(it.get("name") or "")
        pri = str(it.get("priority") or "")
        alert_entries.append((aid, aname, pri, clean_keys))
        for k in clean_keys:
            if k not in uniq_seen:
                uniq_seen.add(k)
                unique_order.append(k)

    truncated_fields = False
    if max_fields > 0 and len(unique_order) > max_fields:
        unique_order = unique_order[:max_fields]
        truncated_fields = True
    allowed_fields: Set[str] = set(unique_order)

    field_presence: Dict[str, Optional[int]] = {}
    field_errors: Dict[str, str] = {}

    try:
        conc = int(os.environ.get("CORALOGIX_ALERT_QUERY_FIELD_VALIDATE_CONCURRENCY", "3"))
    except ValueError:
        conc = 3
    conc = max(1, min(12, conc))

    def _dataprime_check_field(field: str) -> Tuple[str, Optional[int], Optional[str]]:
        exists_target = _field_path_for_exists_check(field)
        lq = f"_exists_:{exists_target}"
        dpq = f"source logs | lucene '{_lucene_clause_escape_for_dataprime(lq)}' | count"
        try:
            raw = _post_dataprime_query_text(
                host, dpq, start_iso, end_iso, tier, auth, timeout=timeout_sec
            )
            cnt, perr = _dataprime_first_count(raw)
            if perr:
                return field, None, perr
            return field, int(cnt) if cnt is not None else None, None
        except Exception as e:
            return field, None, str(e)[:240]

    if unique_order:
        with ThreadPoolExecutor(max_workers=conc) as pool:
            futs = [pool.submit(_dataprime_check_field, f) for f in unique_order]
            for fut in as_completed(futs):
                field, cnt, err = fut.result()
                if err:
                    field_errors[field] = err
                    field_presence[field] = None
                else:
                    field_presence[field] = cnt
        if sleep_sec > 0:
            time.sleep(min(sleep_sec, 0.2))

    missing_rows: List[Dict[str, Any]] = []
    for aid, aname, pri, keys in alert_entries:
        missing: List[str] = []
        err_keys: List[str] = []
        for k in keys:
            if k not in allowed_fields:
                continue
            cnt = field_presence.get(k)
            if cnt is None:
                if k in field_errors:
                    err_keys.append(f"{k} ({field_errors[k][:120]})")
                continue
            if cnt == 0:
                missing.append(k)
        if missing or err_keys:
            row: Dict[str, Any] = {
                "alert_id": aid,
                "alert_name": aname,
                "priority": pri,
                "missing_keys": missing,
            }
            if err_keys:
                row["field_check_errors"] = err_keys
            missing_rows.append(row)

    missing_rows.sort(key=lambda x: (str(x.get("alert_name") or "").lower()))

    out: Dict[str, Any] = {
        "enabled": True,
        "window_days_requested": requested_days,
        "window_days": effective_days,
        "window_start": start_iso,
        "window_end": end_iso,
        "tier": tier,
        "api_host": host,
        "alerts_with_extracted_keys": len(alert_entries),
        "unique_field_paths_checked": len(unique_order),
        "missing_field_row_count": len(missing_rows),
        "items": missing_rows[:2000],
        "truncated_unique_fields": truncated_fields,
        "dataprime_concurrency": conc,
    }
    note_parts: List[str] = [
        "Fields parsed heuristically from alert condition/filters text; metric-only / flow alerts may yield none.",
        "Absence = zero hits for Lucene _exists_:path in logs over the window (not a full query replay).",
        f"DataPrime field checks used concurrency={conc} (CORALOGIX_ALERT_QUERY_FIELD_VALIDATE_CONCURRENCY).",
    ]
    if window_note:
        note_parts.append(window_note)
    if truncated_fields:
        note_parts.append(
            f"Unique field checks capped at {max_fields} (CORALOGIX_ALERT_QUERY_FIELD_VALIDATE_MAX_FIELDS)."
        )
    out["note"] = " ".join(note_parts)
    if field_errors and len(field_errors) <= 80:
        out["field_query_errors"] = dict(
            list(sorted(field_errors.items(), key=lambda x: x[0].lower()))[:80]
        )
    elif field_errors:
        out["field_query_errors_count"] = len(field_errors)

    results["alert_query_field_validation"] = out
    print(
        f"  \u2139  alert query field validation: {len(unique_order)} field path(s), "
        f"{len(missing_rows)} alert(s) with missing/err · {effective_days}d · {host}"
    )


def fetch_alerts() -> Dict:
    """Alert definitions — full list with priority/status breakdown."""
    from collections import Counter
    data   = _get(f"{ALERTS_BASE}/alerts")
    alerts = data.get("alerts", [])
    total  = data.get("total", len(alerts))

    def get_meta(a: Dict) -> Dict:
        ml = a.get("meta_labels") or []
        if isinstance(ml, list):
            return {item["key"]: item["value"] for item in ml if "key" in item}
        return ml if isinstance(ml, dict) else {}

    enabled  = [a for a in alerts if     a.get("is_active", True)]
    disabled = [a for a in alerts if not a.get("is_active", True)]

    sev_map = {"critical": "P1", "error": "P2", "warning": "P3",
               "info": "P4", "debug": "P5", "verbose": "P5"}
    by_priority = dict(Counter(
        sev_map.get(a.get("severity", "").lower(), "P5") for a in alerts
    ))

    def names_matching(kw: str, pool: Optional[List] = None) -> List[str]:
        src = pool if pool is not None else alerts
        return [a.get("name", "") for a in src
                if kw.lower() in a.get("name", "").lower()]

    def ingestion_block_enabled_names() -> List[str]:
        out: List[str] = []
        for a in enabled:
            nm = str(a.get("name", "") or "")
            if _is_canonical_ingestion_block_name(nm) or "ingestion blocked" in nm.lower():
                out.append(nm)
        return out

    ext_packs: Dict[str, int] = {}
    for a in alerts:
        ep = get_meta(a).get("alert_extension_pack", "")
        if ep:
            ext_packs[ep] = ext_packs.get(ep, 0) + 1

    # Security-deployed total (extension-pack rules + custom security-named rules)
    security_name_kws = (
        "security", "threat", "attack", "malicious", "suspicious", "unauthorized",
        "compromise", "exploit", "brute", "privilege", "lateral", "cspm", "wiz",
        "wallarm", "tetragon", "siem", "okta", "iam", "cloudtrail", "guardduty",
        "inspector", "macie", "vpc flow", "waf", "intrusion", "anomaly", "hunt",
        "correlation", "building block", "outgoing connection", "unified threat",
        "no logs from", "detected", "disabled", "deleted", "attempted",
    )
    ext_pack_alert_count = sum(ext_packs.values())
    custom_security_count = 0
    for a in alerts:
        labels = get_meta(a)
        if labels.get("alert_extension_pack", ""):
            continue
        alert_t = labels.get("alert_type", "")
        name_lc = (a.get("name", "") or "").lower()
        if alert_t == "security" or any(kw in name_lc for kw in security_name_kws):
            custom_security_count += 1
    security_deployed_total = ext_pack_alert_count + custom_security_count

    # Raw API severities → dashboard donut order: Error, Critical, Warning, Low, Info
    by_sev = Counter((a.get("severity") or "").lower() for a in alerts)
    by_severity_chart = {
        "error":    by_sev.get("error", 0),
        "critical": by_sev.get("critical", 0),
        "warning":  by_sev.get("warning", 0),
        "info":     by_sev.get("info", 0),
        "low":      by_sev.get("debug", 0) + by_sev.get("verbose", 0),
    }

    # Per-definition rows for the HTML table (replaces static March-19 demo data).
    alert_items: List[Dict[str, Any]] = []
    for a in alerts:
        labels = get_meta(a)
        ep = (labels.get("alert_extension_pack") or "").strip()
        prov = ep if ep else (str(labels.get("provider") or "").strip() or "Custom")
        raw_type = labels.get("alert_type") or a.get("type") or "Standard"
        if isinstance(raw_type, dict):
            type_name = str(list(raw_type.keys())[0]) if raw_type else "Standard"
        else:
            type_name = str(raw_type) if raw_type else "Standard"
        qfk = _alert_query_field_keys_for_api_row(a if isinstance(a, dict) else {})
        row_d: Dict[str, Any] = {
            "id":            str(
                a.get("id")
                or a.get("alertId")
                or a.get("alert_id")
                or a.get("unique_id")
                or ""
            ),
            "name":             a.get("name", ""),
            "applicationName":  _application_name_from_alert_meta(labels),
            "priority":         sev_map.get((a.get("severity") or "").lower(), "P5"),
            "type":             type_name,
            "provider":         prov,
            "enabled":          bool(a.get("is_active", True)),
            "lastTriggered":    _extract_last_trigger_iso(a),
            "mitre_tactic":     _mitre_tactic_from_alert_meta(labels),
        }
        if qfk:
            row_d["queryFieldKeys"] = qfk
        alert_items.append(row_d)

    ingestion_block_canonical: Optional[Dict[str, Any]] = None
    for a in alerts:
        nm = str(a.get("name", "") or "")
        if not _is_canonical_ingestion_block_name(nm):
            continue
        ingestion_block_canonical = {
            "name":    nm,
            "id":      str(a.get("id") or a.get("alertId") or a.get("alert_id") or ""),
            "enabled": bool(a.get("is_active", True)),
        }
        break

    return {
        "total":                      total,
        "enabled_count":              len(enabled),
        "disabled_count":             len(disabled),
        "by_priority":                by_priority,
        "by_severity_chart":          by_severity_chart,
        "security_deployed_total":    security_deployed_total,
        "extension_pack_alert_count": ext_pack_alert_count,
        "custom_security_count":      custom_security_count,
        "saml_rules":                 names_matching("saml",    enabled),
        "mfa_rules":                  names_matching("mfa",     enabled),
        "cspm_rules":                 names_matching("cspm",    enabled),
        "wallarm_rules":              names_matching("wallarm", enabled),
        "ingestion_block":            ingestion_block_enabled_names(),
        "ingestion_block_canonical":  ingestion_block_canonical,
        "extension_packs":            ext_packs,
        "disabled_names":             [a.get("name", "") for a in disabled],
        "items":                      alert_items,
        "items_count":                len(alert_items),
    }


# ── Section registry ──────────────────────────────────────────────────────────

SECTIONS: Dict[str, Any] = {
    "integrations": fetch_integrations,
    "extensions":   fetch_extensions,
    "webhooks":     fetch_webhooks,
    "saml":         fetch_saml,
    "ip_access":    fetch_ip_access,
    "enrichments":  fetch_enrichments,
    "folders":      fetch_folders,
    "tco_policies": fetch_tco_policies,
    "alerts":       fetch_alerts,
    "incidents":    fetch_incidents,
}

def _iso_ts_for_sort(s: str) -> float:
    if not s or not isinstance(s, str):
        return 0.0
    t = s.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(t).timestamp()
    except ValueError:
        return 0.0


def enrich_alerts_last_triggered_from_incidents(results: Dict[str, Any]) -> int:
    """
    Coralogix GET /alerts often omits last-fired timestamps. Fill alerts.items[].lastTriggered
    from the latest matching incident row (same refresh; incidents are usually last 24h UTC).
    Sets lastTriggeredSource to 'incidents_window' when derived.
    """
    al = results.get("alerts")
    inc = results.get("incidents")
    if not isinstance(al, dict) or al.get("error"):
        return 0
    if not isinstance(inc, dict) or inc.get("error"):
        return 0
    items = al.get("items")
    inc_items = inc.get("items")
    if not isinstance(items, list) or not isinstance(inc_items, list) or not inc_items:
        return 0

    def row_best_ts(row: Dict) -> str:
        u = str(row.get("updated") or "").strip()
        c = str(row.get("created") or "").strip()
        if not u:
            return c
        if not c:
            return u
        return u if _iso_ts_for_sort(u) >= _iso_ts_for_sort(c) else c

    def names_match(def_cf: str, cand: str) -> bool:
        if not cand:
            return False
        cf = cand.casefold()
        if def_cf == cf:
            return True
        if len(def_cf) < 8 or len(cf) < 8:
            return False
        return def_cf in cf or cf in def_cf

    # Prefer stable correlation: incident sourceAlertId ↔ alert definition id.
    by_alert_id: Dict[str, str] = {}
    for row in inc_items:
        if not isinstance(row, dict):
            continue
        aid = str(row.get("sourceAlertId") or row.get("source_alert_id") or "").strip()
        if not aid:
            continue
        ts = row_best_ts(row)
        if not ts:
            continue
        prev = by_alert_id.get(aid)
        if not prev or _iso_ts_for_sort(ts) > _iso_ts_for_sort(prev):
            by_alert_id[aid] = ts

    filled = 0
    for a in items:
        if not isinstance(a, dict):
            continue
        if str(a.get("lastTriggered") or "").strip():
            continue
        def_aid = str(a.get("id") or "").strip()
        if def_aid and def_aid in by_alert_id:
            a["lastTriggered"] = by_alert_id[def_aid]
            a["lastTriggeredSource"] = "incident_alert_id"
            filled += 1
            continue
        def_name = (a.get("name") or "").strip()
        if not def_name:
            continue
        df = def_name.casefold()
        best = ""
        for row in inc_items:
            if not isinstance(row, dict):
                continue
            candidates = [
                str(row.get("alertRuleName") or "").strip(),
                str(row.get("name") or "").strip(),
            ]
            matched = any(names_match(df, c) for c in candidates if c)
            if not matched:
                continue
            ts = row_best_ts(row)
            if ts and (not best or _iso_ts_for_sort(ts) > _iso_ts_for_sort(best)):
                best = ts
        if best:
            a["lastTriggered"] = best
            a["lastTriggeredSource"] = "incidents_window"
            filled += 1
    return filled


def _alert_join_lookup_from_items(items: Any) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
    by_id: Dict[str, Dict[str, str]] = {}
    by_name_cf: Dict[str, Dict[str, str]] = {}
    if not isinstance(items, list):
        return by_id, by_name_cf
    for a in items:
        if not isinstance(a, dict):
            continue
        aid = str(a.get("id") or "").strip()
        nm = str(a.get("name") or "").strip()
        appn = str(a.get("applicationName") or "").strip()
        praw = str(a.get("priority") or "").strip().upper()
        if praw not in ("P1", "P2", "P3", "P4", "P5"):
            praw = "P5"
        rec = {"name": nm, "applicationName": appn, "id": aid, "priority": praw}
        if aid:
            by_id[aid] = rec
        if nm:
            cf = nm.casefold()
            if cf not in by_name_cf:
                by_name_cf[cf] = rec
    return by_id, by_name_cf


def enrich_incident_items(inc_items: Any, items: Any) -> int:
    """
    Join incident rows to alert definitions (mutates rows in inc_items):
    alertDefinitionName, logSource, priority when sourceAlertId or alertRuleName matches.
    """
    if not isinstance(items, list) or not isinstance(inc_items, list):
        return 0
    by_id, by_name_cf = _alert_join_lookup_from_items(items)
    updated = 0
    for row in inc_items:
        if not isinstance(row, dict):
            continue
        changed = False
        aid = str(row.get("sourceAlertId") or "").strip()
        info = by_id.get(aid) if aid else None
        if not info:
            arn = (row.get("alertRuleName") or "").strip()
            if arn:
                info = by_name_cf.get(arn.casefold())
        if not info:
            continue
        dn = info.get("name") or ""
        if dn and not (row.get("alertDefinitionName") or "").strip():
            row["alertDefinitionName"] = dn[:500]
            changed = True
        app_a = (info.get("applicationName") or "").strip()
        if app_a and not _is_uuid_like(app_a) and not (row.get("logSource") or "").strip():
            row["logSource"] = app_a[:500]
            changed = True
        pr_a = (info.get("priority") or "").strip().upper()
        if pr_a in ("P1", "P2", "P3", "P4", "P5") and row.get("priority") != pr_a:
            row["priority"] = pr_a
            changed = True
        if changed:
            updated += 1
    return updated


def enrich_incidents_from_alerts(results: Dict[str, Any]) -> int:
    """
    Join incidents to alert definitions:
    - alertDefinitionName  ← alerts API name when sourceAlertId matches
    - logSource            ← filled from alert applicationName if incident had none
    """
    al = results.get("alerts")
    inc = results.get("incidents")
    if not isinstance(al, dict) or al.get("error"):
        return 0
    if not isinstance(inc, dict) or inc.get("error"):
        return 0
    items = al.get("items")
    inc_items = inc.get("items")
    return enrich_incident_items(inc_items, items)


def build_never_triggered_definitions(
    alerts_items: List[Dict[str, Any]],
    correlation_rows: List[Dict[str, Any]],
    *,
    window_days: int,
    window_start: str,
    window_end: str,
    correlation_note: str = "",
    truncated: bool = False,
    correlation_error: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Definitions in alerts_items with no matching incident in correlation_rows
    (match by alert id or by definition name / alertRuleName / alertDefinitionName),
    and no parseable lastTriggered on the definition row (alerts API / management UI).
    """
    base: Dict[str, Any] = {
        "window_days":   window_days,
        "window_start":  window_start,
        "window_end":    window_end,
        "truncated":     truncated,
    }
    if correlation_error:
        return {
            **base,
            "error":         correlation_error,
            "count":         0,
            "items":         [],
            "incidents_in_window": 0,
            "definitions_total": len(alerts_items) if isinstance(alerts_items, list) else 0,
            "definitions_with_incident":           0,
            "definitions_excluded_last_triggered": 0,
            "extensions_summary":                [],
            "note":          correlation_error,
        }

    alert_id_set: Set[str] = set()
    for a in alerts_items or []:
        if not isinstance(a, dict):
            continue
        x = str(a.get("id") or "").strip()
        if x:
            alert_id_set.add(x)

    fired_ids: Set[str] = set()
    fired_names_cf: Set[str] = set()
    for row in correlation_rows or []:
        if not isinstance(row, dict):
            continue
        aid = str(row.get("sourceAlertId") or "").strip()
        if aid and aid in alert_id_set:
            fired_ids.add(aid)
        for key in (
            row.get("alertDefinitionName"),
            row.get("alertRuleName"),
        ):
            if isinstance(key, str) and key.strip():
                fired_names_cf.add(key.strip().casefold())

    matched_defs = 0
    excluded_last_triggered = 0
    never: List[Dict[str, Any]] = []
    for a in alerts_items or []:
        if not isinstance(a, dict):
            continue
        aid = str(a.get("id") or "").strip()
        nm_cf = str(a.get("name") or "").strip().casefold()
        hit = (aid and aid in fired_ids) or (nm_cf and nm_cf in fired_names_cf)
        if hit:
            matched_defs += 1
            continue
        if _parsable_last_trigger_value(a.get("lastTriggered")):
            excluded_last_triggered += 1
            continue
        never.append(
            {
                "id":            aid,
                "name":          a.get("name") or "",
                "priority":      a.get("priority") or "P5",
                "type":          a.get("type") or "Standard",
                "provider":      a.get("provider") or "—",
                "enabled":       a.get("enabled", True),
                "lastTriggered": a.get("lastTriggered"),
            }
        )

    def _pri_order(p: str) -> int:
        return {"P1": 1, "P2": 2, "P3": 3, "P4": 4, "P5": 5}.get(
            str(p or "").upper(), 9
        )

    never.sort(key=lambda x: (_pri_order(str(x.get("priority"))), str(x.get("name") or "").lower()))

    def _ext_bucket(row: Dict[str, Any]) -> str:
        return str(row.get("provider") or "").strip() or "—"

    totals_by_ext: Counter = Counter()
    for a in alerts_items or []:
        if isinstance(a, dict):
            totals_by_ext[_ext_bucket(a)] += 1
    never_by_ext: Counter = Counter(_ext_bucket(r) for r in never)
    extensions_summary: List[Dict[str, Any]] = []
    for ext in sorted(totals_by_ext.keys(), key=lambda k: str(k).casefold()):
        ntot = int(totals_by_ext[ext])
        nn = int(never_by_ext.get(ext, 0))
        extensions_summary.append({
            "extension_name":          ext,
            "total_alerts":            ntot,
            "never_triggered_count":   nn,
        })
    extensions_summary.sort(
        key=lambda x: (-x["never_triggered_count"], str(x["extension_name"]).casefold())
    )

    note = (correlation_note or "").strip()
    if truncated:
        note = (note + " Correlation may be incomplete (pagination cap).").strip()
    if excluded_last_triggered:
        note = (
            note + f" {excluded_last_triggered} definition(s) had no {window_days}d correlation match but "
            "were omitted because lastTriggered is set on the alert (fired outside window or per API)."
        ).strip()

    return {
        **base,
        "error":                             None,
        "count":                             len(never),
        "items":                             never[:800],
        "incidents_in_window":               len(correlation_rows or []),
        "definitions_total":                 len(alerts_items) if isinstance(alerts_items, list) else 0,
        "definitions_with_incident":         matched_defs,
        "definitions_excluded_last_triggered": excluded_last_triggered,
        "extensions_summary":                extensions_summary,
        "note":                              note
        or (
            f"No incident in last {window_days}d (UTC) among fetched rows; "
            "listed definitions also have no parseable lastTriggered on the alert."
        ),
    }


def _should_run_never_triggered_correlation(target: List[str]) -> bool:
    t = set(target)
    if t == set(SECTIONS.keys()):
        return True
    return "alerts" in t or "incidents" in t


# 30d cx_alerts correlation: computed once in attach_never_triggered_30d, reused in
# attach_alert_hygiene. Removed from results before writing data.json (see run()).
_REFRESH_CX_ALERTS_CORR_30D_KEY = "_refresh_cx_alerts_corr_30d"


def attach_never_triggered_30d(results: Dict[str, Any], target: List[str]) -> None:
    """Populate results['alerts']['never_triggered_30d'] when alerts + correlation are available."""
    if not _should_run_never_triggered_correlation(target):
        return
    al = results.get("alerts")
    if not isinstance(al, dict) or al.get("error"):
        return
    items = al.get("items")
    if not isinstance(items, list) or not items:
        return

    days = 30
    def_names = _alert_definition_names_from_items(items)
    corr = _correlation_rows_from_cx_alerts_by_definition(days, definition_names=def_names)
    results[_REFRESH_CX_ALERTS_CORR_30D_KEY] = corr
    if corr.get("error") and not corr.get("rows"):
        al["never_triggered_30d"] = build_never_triggered_definitions(
            items,
            [],
            window_days=days,
            window_start=corr.get("window_start") or "",
            window_end=corr.get("window_end") or "",
            correlation_error=str(corr.get("error")),
        )
        print(f"  ⚠  never_triggered_30d: correlation fetch failed — {str(corr.get('error'))[:80]}")
        return

    rows = list(corr.get("rows") or [])
    n_enr = enrich_incident_items(rows, items)
    if n_enr:
        print(f"  ℹ  never_triggered_30d: enriched {n_enr} correlation row(s) from alert definitions")

    payload = build_never_triggered_definitions(
        items,
        rows,
        window_days=days,
        window_start=str(corr.get("window_start") or ""),
        window_end=str(corr.get("window_end") or ""),
        correlation_note=str(corr.get("note") or ""),
        truncated=bool(corr.get("truncated")),
        correlation_error=None,
    )
    al["never_triggered_30d"] = payload
    ex_lt = int(payload.get("definitions_excluded_last_triggered") or 0)
    ex_s = f", {ex_lt} omitted (lastTriggered set on alert)" if ex_lt else ""
    print(
        f"  ℹ  never_triggered_30d: {payload['count']} definition(s) with no incident in {days}d "
        f"and no API last trigger ({payload['incidents_in_window']} correlation rows scanned, "
        f"{payload['definitions_with_incident']} defs matched{ex_s})"
    )


def _no_log_phrase_in_alert_name(name: str) -> bool:
    """True if alert name indicates a no-logs monitor (substring match, case-insensitive)."""
    n = (name or "").casefold()
    return "no logs" in n or "no log" in n or "no-log" in n or "no_log" in n


_NO_LOG_UNIVERSE_EXCLUDED_CF: frozenset = frozenset({"coralogix-alerts", "cx-metrics"})


def _no_log_canonical_casefold(s: str) -> str:
    """Normalize for comparison: NFKC + strip + casefold (application vs subsystem names are case-insensitive)."""
    t = unicodedata.normalize("NFKC", str(s or "").strip())
    return t.casefold()

# Skip meta values that are not real application/subsystem names (case-insensitive).
_NO_LOG_META_VALUE_SKIP_CF: frozenset = frozenset({
    "",
    "custom",
    "standard",
    "coralogix",
    "none",
    "n/a",
    "na",
    "true",
    "false",
    "yes",
    "no",
    "all",
    "any",
    "default",
    "unknown",
})


def _no_log_scalar_from_filter_value(val: Any) -> str:
    """String value from alert filter leaf (plain string or { value: \"...\" })."""
    if isinstance(val, str):
        return val.strip()
    if isinstance(val, dict):
        v = val.get("value")
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _no_log_meta_value_usable_for_association(val: str) -> bool:
    """True if a meta label value can name an application/subsystem (not a path, UUID, or generic tag)."""
    t = str(val or "").strip()
    if len(t) < 1:
        return False
    if _no_log_canonical_casefold(t) in _NO_LOG_META_VALUE_SKIP_CF:
        return False
    if _looks_like_field_path(t):
        return False
    if _is_uuid_like(t):
        return False
    return True


def _no_log_associated_apps_subs_from_meta_labels(raw: Dict[str, Any]) -> Tuple[Set[str], Set[str]]:
    """
    Map alert **association** to data-usage application / subsystem names from ``meta_labels`` only
    (how Coralogix ties a definition to a log source — not alert title text, not only Lucene filters).

    Keys are matched case-insensitively; values are stored with ``_no_log_canonical_casefold`` for comparison.
    """
    apps: Set[str] = set()
    subs: Set[str] = set()
    ml = raw.get("meta_labels")
    if not isinstance(ml, list):
        return apps, subs
    for m in ml:
        if not isinstance(m, dict):
            continue
        k = str(m.get("key") or "").strip()
        v_raw = m.get("value")
        if isinstance(v_raw, str):
            v = v_raw.strip()
        elif v_raw is not None:
            v = str(v_raw).strip()
        else:
            v = ""
        if not k or not _no_log_meta_value_usable_for_association(v):
            continue
        kl = k.lower().replace(" ", "_").replace(".", "_").replace("-", "_")
        vcf = _no_log_canonical_casefold(v)
        # Combined hints → count toward both dimensions
        if "application_subsystem" in kl or "app_subsystem" in kl:
            apps.add(vcf)
            subs.add(vcf)
            continue
        # Subsystem association (key indicates subsystem / pipeline slice)
        if "subsystem" in kl:
            subs.add(vcf)
            continue
        # Application / integration / pack association (feeds data usage application axis)
        if any(
            tok in kl
            for tok in (
                "application_name",
                "applicationname",
                "cx_application",
                "integration_name",
                "integrationname",
                "integration_id",
                "integrationid",
                "application",
                "integration",
                "alert_extension_pack",
                "extension_pack",
                "extensionpack",
                "provider",
                "log_source",
                "logsource",
                "datasource",
                "data_source",
                "sourceapplication",
                "source_application",
                "vendor",
                "technology",
            )
        ):
            apps.add(vcf)
            continue
        if "application" in kl:
            apps.add(vcf)
            continue
        if "integration" in kl:
            apps.add(vcf)
            continue
    return apps, subs


def _no_log_deep_collect_dimension_values(obj: Any, dim: str, out: Set[str], depth: int = 0) -> None:
    """Walk alert definition JSON and collect applicationName / subsystemName filter values."""
    if depth > 28:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            lk = str(k).lower()
            s = _no_log_scalar_from_filter_value(v)
            if s and dim == "application":
                if (
                    lk in (
                        "applicationname",
                        "application_name",
                        "integrationname",
                        "integration_name",
                        "metadataapplicationname",
                    )
                    or lk.endswith(".applicationname")
                    or lk.endswith(".integrationname")
                ):
                    out.add(_no_log_canonical_casefold(s))
            elif s and dim == "subsystem":
                if (
                    lk in (
                        "subsystemname",
                        "subsystem_name",
                        "metadatasubsystemname",
                        "integrationsubsystem",
                    )
                    or lk.endswith(".subsystemname")
                ):
                    out.add(_no_log_canonical_casefold(s))
            _no_log_deep_collect_dimension_values(v, dim, out, depth + 1)
    elif isinstance(obj, list):
        for it in obj[:400]:
            _no_log_deep_collect_dimension_values(it, dim, out, depth + 1)


def _no_log_explicit_apps_and_subsystems_from_raw(raw: Dict[str, Any]) -> Tuple[Set[str], Set[str]]:
    """
    Collect application / subsystem names **associated** with this definition: meta_labels (integration,
    extension pack, application/subsystem fields), plus filter JSON (applicationName / subsystemName)
    and regex on serialized alert. Values use ``_no_log_canonical_casefold`` for storage and matching.
    No \"covers everything\" inference.
    """
    apps: Set[str] = set()
    subs: Set[str] = set()
    ma, ms = _no_log_associated_apps_subs_from_meta_labels(raw)
    apps |= ma
    subs |= ms
    _no_log_deep_collect_dimension_values(raw, "application", apps)
    _no_log_deep_collect_dimension_values(raw, "subsystem", subs)

    try:
        blob = json.dumps(raw, default=str)
    except (TypeError, ValueError):
        blob = ""
    if blob:
        app_pats = (
            re.compile(r'"(?:applicationName|application_name|integrationName|integration_name)"\s*:\s*"((?:\\.|[^"\\])*)"', re.I),
        )
        sub_pats = (
            re.compile(r'"(?:subsystemName|subsystem_name|integrationSubsystem|integration_subsystem)"\s*:\s*"((?:\\.|[^"\\])*)"', re.I),
        )
        for pat in app_pats:
            for m in pat.finditer(blob):
                inner = m.group(1)
                try:
                    decoded = json.loads(f'"{inner}"')
                except json.JSONDecodeError:
                    decoded = inner.replace("\\\"", "\"").replace("\\\\", "\\")
                if isinstance(decoded, str) and decoded.strip() and _no_log_meta_value_usable_for_association(decoded):
                    apps.add(_no_log_canonical_casefold(decoded))
        for pat in sub_pats:
            for m in pat.finditer(blob):
                inner = m.group(1)
                try:
                    decoded = json.loads(f'"{inner}"')
                except json.JSONDecodeError:
                    decoded = inner.replace("\\\"", "\"").replace("\\\\", "\\")
                if isinstance(decoded, str) and decoded.strip() and _no_log_meta_value_usable_for_association(decoded):
                    subs.add(_no_log_canonical_casefold(decoded))
    return apps, subs


def _no_log_normalize_for_fuzzy_match(s: str) -> str:
    """Hyphen/underscore/space-insensitive comparison token (after canonical casefold)."""
    return re.sub(r"[\s\-_]+", "", _no_log_canonical_casefold(s))


def _no_log_universe_name_covered(name: str, covered_cf: Set[str]) -> bool:
    """True if this data-usage name matches any associated name (exact, case-insensitive, or fuzzy)."""
    cf = _no_log_canonical_casefold(name)
    if not cf:
        return True
    if cf in covered_cf:
        return True
    norm_u = _no_log_normalize_for_fuzzy_match(name)
    for cov in covered_cf:
        norm_c = _no_log_normalize_for_fuzzy_match(cov)
        if norm_c and (norm_c in norm_u or norm_u in norm_c):
            return True
    return False


def _log_ingestion_row_has_positive_usage(row: Dict[str, Any]) -> bool:
    """True if the row has any non-zero usage in the data-usage window (avg daily or 7d sum)."""
    for keys in (
        ("units_avg_daily", "units_7d", "units"),
        ("size_gb_avg_daily", "size_gb_7d", "size_gb"),
    ):
        for k in keys:
            v = row.get(k)
            try:
                if v is not None and float(v) > 0:
                    return True
            except (TypeError, ValueError):
                pass
    return False


def _no_log_universe_names_from_usage_items(rows: List[Dict[str, Any]]) -> List[str]:
    """Distinct `name` values with positive volume, sorted case-insensitively."""
    out: List[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        u = str(row.get("name") or "").strip()
        if not u or not _log_ingestion_row_has_positive_usage(row):
            continue
        out.append(u)
    return sorted(set(out), key=str.casefold)


def _hygiene_data_usage_application_and_subsystem_items(
    li_primary: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Optional[str]]:
    """
    Build application- and subsystem-level data-usage item lists for no-log hygiene.
    Reuses ``log_ingestion`` from the same refresh when aggregate matches; otherwise performs
    one or two GETs for the missing aggregate(s).
    """
    li_primary = li_primary if isinstance(li_primary, dict) else {}
    err_extra: Optional[str] = None
    app_items: List[Dict[str, Any]] = []
    sub_items: List[Dict[str, Any]] = []
    agg = li_primary.get("aggregate")
    prim_items = li_primary.get("items") if isinstance(li_primary.get("items"), list) else []
    has_err = bool(li_primary.get("error"))

    if not has_err and agg == "AGGREGATE_BY_APPLICATION":
        app_items = prim_items
        sub_li = fetch_log_ingestion_from_data_usage(7, aggregate="AGGREGATE_BY_SUBSYSTEM")
        if sub_li.get("error"):
            err_extra = str(sub_li.get("error") or "subsystem aggregate failed")[:220]
        else:
            sub_items = sub_li.get("items") if isinstance(sub_li.get("items"), list) else []
    elif not has_err and agg == "AGGREGATE_BY_SUBSYSTEM":
        sub_items = prim_items
        app_li = fetch_log_ingestion_from_data_usage(7, aggregate="AGGREGATE_BY_APPLICATION")
        if app_li.get("error"):
            err_extra = str(app_li.get("error") or "application aggregate failed")[:220]
        else:
            app_items = app_li.get("items") if isinstance(app_li.get("items"), list) else []
    else:
        app_li = fetch_log_ingestion_from_data_usage(7, aggregate="AGGREGATE_BY_APPLICATION")
        sub_li = fetch_log_ingestion_from_data_usage(7, aggregate="AGGREGATE_BY_SUBSYSTEM")
        errs: List[str] = []
        if app_li.get("error"):
            errs.append(f"app: {str(app_li.get('error'))[:120]}")
        else:
            app_items = app_li.get("items") if isinstance(app_li.get("items"), list) else []
        if sub_li.get("error"):
            errs.append(f"sub: {str(sub_li.get('error'))[:120]}")
        else:
            sub_items = sub_li.get("items") if isinstance(sub_li.get("items"), list) else []
        if errs:
            err_extra = "; ".join(errs)

    return app_items, sub_items, err_extra


def _normalize_alert_definition_bucket_key(name: str) -> str:
    """Merge rows that differ only by whitespace or letter case (same logical definition)."""
    t = " ".join(str(name or "").split()).strip()
    return t.casefold() if t else "unknown"


def _duplicate_alerts_hygiene_from_items(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Find alert definitions whose **name** string is identical (after leading/trailing strip).
    Case and internal spacing must match; two definitions with the same name but different ids are listed.
    """
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        nm = str(it.get("name") or "").strip()
        key = nm if nm else ""
        row = {
            "id":       str(it.get("id") or ""),
            "name":     nm or "(unnamed)",
            "enabled":  _alert_definition_row_enabled(it),
            "priority": str(it.get("priority") or "").strip(),
            "type":     str(it.get("type") or "").strip(),
            "provider": str(it.get("provider") or "").strip(),
        }
        buckets.setdefault(key, []).append(row)

    groups_out: List[Dict[str, Any]] = []
    extra_definitions = 0
    for key, defs in buckets.items():
        if len(defs) < 2:
            continue
        defs_sorted = sorted(
            defs,
            key=lambda d: ((d.get("id") or ""), (d.get("name") or "")),
        )
        display_name = key if key else "(unnamed)"
        n = len(defs_sorted)
        extra_definitions += n - 1
        cap = 30
        groups_out.append({
            "alert_name":   display_name,
            "display_name": display_name,
            "count":        n,
            "definitions":  defs_sorted[:cap],
            "truncated":    n > cap,
        })

    groups_out.sort(key=lambda g: (-int(g.get("count") or 0), str(g.get("display_name") or "").lower()))

    max_groups = 200
    total_groups = len(groups_out)
    truncated_groups = total_groups > max_groups
    groups_out = groups_out[:max_groups]

    return {
        "group_count":                total_groups,
        "duplicate_definition_count": extra_definitions,
        "groups":                     groups_out,
        "groups_truncated":           truncated_groups,
        "note": (
            "Grouped by exact alert name (string match after trim). "
            "Definitions that differ only by letter case or spacing are not treated as duplicates."
        ),
    }


def _top_alert_definitions_from_incident_items(
    inc_items: Any, limit: int = 15
) -> List[Dict[str, Any]]:
    """Same bucketing as dashboard JS buildTopAlertDefinitionsFromIncidents (24h snapshot)."""
    buckets: Dict[str, Dict[str, Any]] = {}
    if not isinstance(inc_items, list):
        return []
    pri_rank = {"P1": 1, "P2": 2, "P3": 3, "P4": 4, "P5": 5}
    for row in inc_items:
        if not isinstance(row, dict):
            continue
        raw = str(
            row.get("alertDefinitionName")
            or row.get("alertRuleName")
            or row.get("name")
            or "Unknown"
        ).strip() or "Unknown"
        key = _normalize_alert_definition_bucket_key(raw)
        if key not in buckets:
            buckets[key] = {"count": 0, "bestPri": "P5", "label": raw}
        mfc = row.get("metricFireCount")
        if mfc is not None:
            try:
                add = int(round(float(mfc)))
            except (TypeError, ValueError):
                add = 1
            if add < 0:
                add = 0
        else:
            add = 1
        buckets[key]["count"] += add
        pr = str(row.get("priority") or "P5").strip().upper()
        if pr not in pri_rank:
            pr = "P5"
        if pri_rank[pr] < pri_rank[buckets[key]["bestPri"]]:
            buckets[key]["bestPri"] = pr
    out = [
        {"name": v["label"], "count": v["count"], "priority": v["bestPri"]}
        for v in buckets.values()
    ]
    out.sort(key=lambda x: (-x["count"], x["name"].lower()))
    return out[:limit]


def _should_fetch_hygiene_incidents(target: List[str]) -> bool:
    if not target:
        return False
    return set(target) == set(SECTIONS.keys()) or (
        "alerts" in target or "incidents" in target
    )


# SRC customer: webhook name must include both tokens as whole words (not substring of "resource", etc.)
_RE_WEBHOOK_NAME_HAS_SRC = re.compile(r"(?i)\bSRC\b")
_RE_WEBHOOK_NAME_HAS_ORCHESTRATOR = re.compile(r"(?i)\bOrchestrator\b")


def _webhook_name_matches_src_orchestrator_pattern(name: str) -> bool:
    """True if name contains whole-word SRC and whole-word Orchestrator (case-insensitive), e.g. 'SRC | Orchestrator'."""
    s = str(name or "").strip()
    if not s:
        return False
    return bool(_RE_WEBHOOK_NAME_HAS_SRC.search(s) and _RE_WEBHOOK_NAME_HAS_ORCHESTRATOR.search(s))


def _alert_definition_row_enabled(it: Dict[str, Any]) -> bool:
    """True only for definitions that should be evaluated (explicitly disabled → excluded)."""
    if "enabled" not in it:
        return True
    v = it.get("enabled")
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() not in ("false", "0", "no", "off", "")
    if isinstance(v, (int, float)):
        return v != 0
    return bool(v)


def _collect_scalar_values_for_webhook_match(obj: Any, max_depth: int = 24, max_list: int = 250) -> Set[str]:
    """Nested alert JSON may reference webhook UUIDs as strings or numbers in arbitrary keys."""
    out: Set[str] = set()

    def walk(o: Any, d: int) -> None:
        if d > max_depth or o is None:
            return
        if isinstance(o, str):
            t = o.strip()
            if t:
                out.add(t)
            return
        if isinstance(o, bool):
            return
        if isinstance(o, int):
            out.add(str(o))
            return
        if isinstance(o, float):
            if o == int(o):
                out.add(str(int(o)))
            return
        if isinstance(o, dict):
            for v in o.values():
                walk(v, d + 1)
        elif isinstance(o, list):
            for it in o[:max_list]:
                walk(it, d + 1)

    walk(obj, 0)
    return out


def _raw_alert_references_webhook_id(raw: Dict[str, Any], webhook_id: str) -> bool:
    if not webhook_id or not isinstance(raw, dict):
        return False
    wid = webhook_id.strip()
    if not wid:
        return False
    vals = _collect_scalar_values_for_webhook_match(raw)
    if wid in vals:
        return True
    if wid.isdigit():
        for v in vals:
            if v.isdigit() and int(v) == int(wid):
                return True
    wcf = wid.casefold()
    for v in vals:
        if v.casefold() == wcf:
            return True
    return False


def _iter_alert_integration_dicts(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Per-alert integration rows (UI/CSV export ``integrations`` column)."""
    out: List[Dict[str, Any]] = []
    if not isinstance(raw, dict):
        return out
    for key in (
        "integrations",
        "integration",
        "notificationIntegrations",
        "NotificationIntegrations",
        "webhookIntegrations",
    ):
        v = raw.get(key)
        if isinstance(v, list):
            for x in v:
                if isinstance(x, dict):
                    out.append(x)
        elif isinstance(v, dict):
            out.append(v)
    return out


def _collect_integration_ids_from_notification_groups(raw: Dict[str, Any]) -> Set[str]:
    """
    External GET /alerts uses ``notification_groups[].notifications[].integrationId`` (numeric),
    aligned with webhook ``externalId`` — not always the UUID ``id`` used elsewhere.
    """
    out: Set[str] = set()
    if not isinstance(raw, dict):
        return out
    groups = raw.get("notification_groups")
    if not isinstance(groups, list):
        return out
    for g in groups:
        if not isinstance(g, dict):
            continue
        notifs = g.get("notifications")
        if not isinstance(notifs, list):
            continue
        for n in notifs:
            if not isinstance(n, dict):
                continue
            for k in (
                "integrationId",
                "integration_id",
                "integrationUUID",
                "integration_uuid",
            ):
                v = n.get(k)
                if v is None:
                    continue
                t = str(v).strip()
                if t:
                    out.add(t)
                    out.add(t.casefold())
    return out


def _src_webhook_id_match_strings(src_id: str, src_hook_row: Optional[Dict[str, Any]]) -> Set[str]:
    """Match against integration ``id`` / ``externalId`` on each alert (webhook API vs alert export can differ)."""
    out: Set[str] = set()
    for s in (str(src_id or "").strip(),):
        if s:
            out.add(s)
            out.add(s.casefold())
    if isinstance(src_hook_row, dict):
        for k in ("id", "externalId", "external_id", "webhookId", "webhook_id"):
            v = src_hook_row.get(k)
            if v is None:
                continue
            t = str(v).strip()
            if t:
                out.add(t)
                out.add(t.casefold())
    return out


def _integration_matches_src_webhook(integration: Dict[str, Any], id_strings: Set[str]) -> bool:
    nm = str(integration.get("name") or "").strip()
    if _webhook_name_matches_src_orchestrator_pattern(nm):
        return True
    iid = str(integration.get("id") or "").strip()
    if iid and (iid in id_strings or iid.casefold() in id_strings):
        return True
    for ext_key in ("externalId", "external_id"):
        ext = integration.get(ext_key)
        if ext is None:
            continue
        et = str(ext).strip()
        if et and (et in id_strings or et.casefold() in id_strings):
            return True
    return False


def _alert_has_src_webhook_attachment(
    raw: Optional[Dict[str, Any]],
    src_id: str,
    src_hook_row: Optional[Dict[str, Any]],
) -> bool:
    """
    True if the alert routes to the SRC webhook: ``notification_groups`` integrationIds vs webhook
    ``id``/``externalId``, exported-style ``integrations`` (name or ids), then deep JSON UUID scan.
    """
    if not isinstance(raw, dict):
        return False
    id_strings = _src_webhook_id_match_strings(src_id, src_hook_row)
    notif_ids = _collect_integration_ids_from_notification_groups(raw)
    if id_strings and notif_ids & id_strings:
        return True
    for inte in _iter_alert_integration_dicts(raw):
        if _integration_matches_src_webhook(inte, id_strings):
            return True
    if src_id and _raw_alert_references_webhook_id(raw, src_id):
        return True
    return False


def attach_src_customer_profile(results: Dict[str, Any], target: List[str]) -> None:
    """
    SRC customer: outbound webhook whose name contains both whole-word SRC and Orchestrator (any order, case-insensitive).
    When true, count enabled P1/P2/P3 definitions that do **not** route to that webhook: match webhook
    ``id``/``externalId`` to ``notification_groups`` integrationIds (external API) and to exported-style
    ``integrations``; disabled definitions are excluded from numerators and denominators.
    """
    del target  # reserved for future partial-refresh optimizations
    wh = results.get("webhooks")
    al = results.get("alerts")

    empty: Dict[str, Any] = {
        "is_src_customer": False,
        "src_webhook_id": "",
        "src_webhook_name": "",
        "src_webhook_type": "",
        "p1_enabled_without_src_webhook": None,
        "p2_enabled_without_src_webhook": None,
        "p3_enabled_without_src_webhook": None,
        "p1_enabled_checked": None,
        "p2_enabled_checked": None,
        "p3_enabled_checked": None,
        "note": "",
    }

    if not isinstance(wh, dict) or wh.get("error"):
        results["src_customer"] = {
            **empty,
            "note": "webhooks unavailable — cannot determine SRC customer status",
        }
        return

    src_id = ""
    src_nm = ""
    src_type = ""
    src_hook: Optional[Dict[str, Any]] = None
    for h in wh.get("items") or []:
        if not isinstance(h, dict):
            continue
        nm = str(h.get("name") or "").strip()
        if not _webhook_name_matches_src_orchestrator_pattern(nm):
            continue
        xid = str(h.get("id") or "").strip()
        if xid:
            src_id = xid
            src_nm = nm
            src_type = str(h.get("type") or "").strip()
            src_hook = h
            break

    is_src = bool(src_id)
    block: Dict[str, Any] = {
        **empty,
        "is_src_customer": is_src,
        "src_webhook_id": src_id,
        "src_webhook_name": src_nm,
        "src_webhook_type": src_type,
    }

    if not is_src:
        block["note"] = (
            "No outbound webhook whose name contains both SRC and Orchestrator as whole words "
            "(e.g. SRC | Orchestrator)"
        )
        results["src_customer"] = block
        print("  ℹ  src_customer: NO (no webhook name with SRC + Orchestrator)")
        return

    if not isinstance(al, dict) or al.get("error"):
        block["note"] = "alerts unavailable — cannot count P1/P2/P3 definitions missing SRC webhook"
        results["src_customer"] = block
        return

    items = al.get("items")
    if not isinstance(items, list):
        block["note"] = "alerts.items missing"
        results["src_customer"] = block
        return

    try:
        data = _get(f"{ALERTS_BASE.rstrip('/')}/alerts")
    except RuntimeError as e:
        block["note"] = f"alerts re-fetch for SRC scan failed: {e}"
        results["src_customer"] = block
        return

    raw_list = data.get("alerts", [])
    by_id: Dict[str, Dict[str, Any]] = {}
    for ra in raw_list:
        if not isinstance(ra, dict):
            continue
        aid = str(
            ra.get("id")
            or ra.get("alertId")
            or ra.get("alert_id")
            or ra.get("unique_id")
            or ""
        ).strip()
        if aid:
            by_id[aid] = ra

    p1_miss = p2_miss = p3_miss = 0
    p1_chk = p2_chk = p3_chk = 0
    for it in items:
        if not isinstance(it, dict):
            continue
        if not _alert_definition_row_enabled(it):
            continue
        pr = str(it.get("priority") or "").strip().upper()
        if pr not in ("P1", "P2", "P3"):
            continue
        aid = str(it.get("id") or "").strip()
        raw = by_id.get(aid) if aid else None
        if pr == "P1":
            p1_chk += 1
        elif pr == "P2":
            p2_chk += 1
        else:
            p3_chk += 1
        attached = _alert_has_src_webhook_attachment(
            raw if isinstance(raw, dict) else None, src_id, src_hook
        )
        if not attached:
            if pr == "P1":
                p1_miss += 1
            elif pr == "P2":
                p2_miss += 1
            else:
                p3_miss += 1

    block["p1_enabled_without_src_webhook"] = p1_miss
    block["p2_enabled_without_src_webhook"] = p2_miss
    block["p3_enabled_without_src_webhook"] = p3_miss
    block["p1_enabled_checked"] = p1_chk
    block["p2_enabled_checked"] = p2_chk
    block["p3_enabled_checked"] = p3_chk
    block["note"] = (
        f"Enabled P1/P2/P3 definitions with no notification route to SRC webhook {src_nm!r} "
        f"(notification_groups integrationId / integrations vs webhook id & externalId)"
    )
    results["src_customer"] = block
    print(
        f"  ℹ  src_customer: {'YES' if is_src else 'NO'} — "
        f"P1 without SRC webhook: {p1_miss}/{p1_chk}, P2: {p2_miss}/{p2_chk}, P3: {p3_miss}/{p3_chk}"
    )


def attach_alert_hygiene(results: Dict[str, Any], target: List[str]) -> None:
    """
    Populate results['alerts']['hygiene']: suppression names, no-log coverage vs data usage,
    ingestion-block incidents (30d), merged top-definition counts (24h), 30d incident timeline.
    """
    al = results.get("alerts")
    if not isinstance(al, dict) or al.get("error"):
        if isinstance(al, dict):
            al["hygiene"] = {"error": "alerts section unavailable"}
        return

    sup = fetch_suppression_scheduler_rules()

    items = al.get("items")
    if not isinstance(items, list):
        items = []

    raw_by_id: Dict[str, Dict[str, Any]] = {}
    no_log_raw_fetch_error: Optional[str] = None
    try:
        _al_data = _get(f"{ALERTS_BASE.rstrip('/')}/alerts")
        for ra in _al_data.get("alerts") or []:
            if not isinstance(ra, dict):
                continue
            aid = str(
                ra.get("id")
                or ra.get("alertId")
                or ra.get("alert_id")
                or ra.get("unique_id")
                or ""
            ).strip()
            if aid:
                raw_by_id[aid] = ra
    except Exception as ex:
        no_log_raw_fetch_error = str(ex)

    li = results.get("log_ingestion") or {}
    app_usage_items, sub_usage_items, no_log_usage_extra_err = (
        _hygiene_data_usage_application_and_subsystem_items(li)
    )
    if no_log_usage_extra_err:
        print(f"  ⚠  alert_hygiene: no-log extra data-usage: {no_log_usage_extra_err[:100]}")
    elif isinstance(li, dict) and not li.get("error") and li.get("aggregate") == "AGGREGATE_BY_APPLICATION" and sub_usage_items:
        print("  ℹ  alert_hygiene: fetched subsystem data usage for no-log coverage (2nd GET)")
    elif isinstance(li, dict) and not li.get("error") and li.get("aggregate") == "AGGREGATE_BY_SUBSYSTEM" and app_usage_items:
        print("  ℹ  alert_hygiene: fetched application data usage for no-log coverage (2nd GET)")

    universe_apps = _no_log_universe_names_from_usage_items(app_usage_items)
    universe_subs = _no_log_universe_names_from_usage_items(sub_usage_items)

    covered_apps: Set[str] = set()
    covered_subs: Set[str] = set()
    no_log_rows: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        nm = str(it.get("name") or "")
        if not _no_log_phrase_in_alert_name(nm):
            continue
        app = str(it.get("applicationName") or "").strip()
        en = _alert_definition_row_enabled(it)
        aid = str(it.get("id") or "").strip()
        raw_alert = raw_by_id.get(aid) if aid else None
        apps_scope: Set[str] = set()
        subs_scope: Set[str] = set()
        if isinstance(raw_alert, dict) and raw_alert:
            apps_scope, subs_scope = _no_log_explicit_apps_and_subsystems_from_raw(raw_alert)
        elif app:
            apps_scope.add(_no_log_canonical_casefold(app))
        cap_scope = 80
        no_log_rows.append({
            "id":                   str(it.get("id") or ""),
            "name":                 nm,
            "applicationName":      app,
            "enabled":              en,
            "applications_scoped":  sorted(apps_scope)[:cap_scope],
            "subsystems_scoped":    sorted(subs_scope)[:cap_scope],
        })
        if not en:
            continue
        covered_apps.update(apps_scope)
        covered_subs.update(subs_scope)

    # Same logical source often appears as both an application row and a subsystem row in data usage;
    # meta may only populate one axis. Match both universes against the union (case-insensitive / fuzzy).
    covered_for_no_log = covered_apps | covered_subs

    missing_no_log_by_application: List[str] = []
    for u in universe_apps:
        ucf = _no_log_canonical_casefold(u)
        if ucf in _NO_LOG_UNIVERSE_EXCLUDED_CF:
            continue
        if not _no_log_universe_name_covered(u, covered_for_no_log):
            missing_no_log_by_application.append(u)

    missing_no_log_by_subsystem: List[str] = []
    for u in universe_subs:
        ucf = _no_log_canonical_casefold(u)
        if ucf in _NO_LOG_UNIVERSE_EXCLUDED_CF:
            continue
        if not _no_log_universe_name_covered(u, covered_for_no_log):
            missing_no_log_by_subsystem.append(u)

    missing_no_log_count = len(missing_no_log_by_application) + len(missing_no_log_by_subsystem)
    # Backward compat: single list with prefixes (older UI / exports)
    missing_no_log_legacy: List[str] = (
        [f"[application] {x}" for x in missing_no_log_by_application[:400]]
        + [f"[subsystem] {x}" for x in missing_no_log_by_subsystem[:400]]
    )

    ibc = al.get("ingestion_block_canonical")
    if not isinstance(ibc, dict):
        ibc = {}

    inc_24 = results.get("incidents") or {}
    inc_items_24 = inc_24.get("items") if isinstance(inc_24.get("items"), list) else []
    top_24h = _top_alert_definitions_from_incident_items(inc_items_24, 15)

    ib30: Dict[str, Any] = {
        "canonical_alert_found":     bool(ibc.get("name")),
        "canonical_name":            CANONICAL_INGESTION_BLOCK_ALERT,
        "matched_definition_name":   str(ibc.get("name") or ""),
        "matched_definition_id":     str(ibc.get("id") or ""),
        "enabled":                   bool(ibc.get("enabled")),
        "incidents_30d_count":       0,
        "distinct_blocked_days":     0,
        "blocked_dates_sample":      [],
        "window_days":               30,
        "window_start":              "",
        "window_end":                "",
        "error":                     None,
        "note":                      None,
        "truncated":                 False,
    }

    timeline: List[Dict[str, Any]] = []
    if _should_fetch_hygiene_incidents(target):
        corr = results.pop(_REFRESH_CX_ALERTS_CORR_30D_KEY, None)
        if corr is None:
            corr = _correlation_rows_from_cx_alerts_by_definition(
                30, definition_names=_alert_definition_names_from_items(items)
            )
        ib30["window_start"] = str(corr.get("window_start") or "")
        ib30["window_end"] = str(corr.get("window_end") or "")
        ib30["truncated"] = False
        if corr.get("error"):
            ib30["error"] = str(corr.get("error"))
            ib30["note"] = "30d cx_alerts correlation failed; ingestion-block counts stay at 0."
        else:
            rows30 = list(corr.get("rows") or [])
            n_enr = enrich_incident_items(rows30, items)
            if n_enr:
                print(
                    f"  ℹ  alert_hygiene: enriched {n_enr} 30d cx_alerts correlation row(s)"
                )
            ib30["incidents_30d_count"] = 0
            ib30["distinct_blocked_days"] = 0
            ib30["blocked_dates_sample"] = []
            ib30["note"] = (
                "Ingestion-block day counts use REST incident timestamps; with cx_alerts-only mode "
                "those metrics are not computed. Daily timeline is cx_alerts security activity."
            )
        tl, tl_err = _cx_alerts_daily_activity_timeline(30)
        if tl:
            timeline = tl
        elif tl_err and not ib30.get("error"):
            ib30["error"] = tl_err
            ib30["note"] = (str(ib30.get("note") or "") + " Metrics timeline query failed.").strip()
    else:
        ib30["note"] = (
            "30d stats not refreshed — include alerts or incidents (or run full refresh)."
        )

    nt = al.get("never_triggered_30d") or al.get("never_triggered_90d") or {}
    nt_cnt = int(nt["count"]) if isinstance(nt.get("count"), (int, float)) else 0

    duplicate_alerts = _duplicate_alerts_hygiene_from_items(items)

    al["hygiene"] = {
        "suppression_rules": sup,
        "no_log_coverage": {
            "dimension": "application_and_subsystem",
            "no_log_alert_definitions": no_log_rows,
            "application_universe_names":   universe_apps,
            "applications_universe_count":  len(universe_apps),
            "subsystem_universe_names":     universe_subs,
            "subsystems_universe_count":    len(universe_subs),
            "universe_names":               universe_apps,
            "universe_count":               len(universe_apps),
            "missing_no_log_by_application":    missing_no_log_by_application[:500],
            "missing_no_log_by_subsystem":      missing_no_log_by_subsystem[:500],
            "missing_no_log_by_application_count": len(missing_no_log_by_application),
            "missing_no_log_by_subsystem_count":   len(missing_no_log_by_subsystem),
            "missing_no_log_count":         missing_no_log_count,
            "missing_no_log_alert":         missing_no_log_legacy[:500],
            "global_no_log_coverage":       False,
            "raw_definitions_error":        no_log_raw_fetch_error,
            "data_usage_note":            no_log_usage_extra_err,
            "note": (
                "Alerts whose name contains \"no log(s)\" (case-insensitive): each enabled definition "
                "adds coverage for names **associated** with that rule — primarily ``meta_labels`` "
                "(integration, extension pack, application/subsystem, provider, vendor, etc.), plus "
                "applicationName/subsystemName-style fields in the JSON. Alert **titles** are not parsed. "
                "Application and subsystem **missing lists** both compare against the **same** union of "
                "associated names (a source often appears under both aggregates; meta may only tag one). "
                "Matching is Unicode-normalized, case-insensitive, with fuzzy hyphen/underscore/space "
                "collapse. Excluded from gaps: coralogix-alerts, cx-metrics."
            ),
        },
        "duplicate_alerts": duplicate_alerts,
        "ingestion_blocked_30d":      ib30,
        "top_alert_definitions_24h":   top_24h,
        "incidents_by_day_30d":       timeline,
        "alert_status_summary": {
            "total":                     al.get("total"),
            "enabled_count":             al.get("enabled_count"),
            "disabled_count":            al.get("disabled_count"),
            "by_priority":               al.get("by_priority") or {},
            "never_triggered_30d_count": nt_cnt,
        },
    }

    print(
        f"  ℹ  alert_hygiene: suppression={sup.get('count')}, "
        f"no_log_defs={len(no_log_rows)}, "
        f"duplicate_alert_groups={duplicate_alerts.get('group_count', 0)}, "
        f"ingestion_block_incidents_30d={ib30.get('incidents_30d_count')}"
    )


def _incident_priority_counts(items: Any) -> Dict[str, int]:
    """P1–P5 counts for open incident rows (after alert enrichment)."""
    from collections import Counter

    order = ("P1", "P2", "P3", "P4", "P5")
    c: Counter = Counter()
    if not isinstance(items, list):
        return {p: 0 for p in order}
    for row in items:
        if not isinstance(row, dict):
            continue
        p = str(row.get("priority") or "").strip().upper()
        if p not in order:
            p = "P5"
        c[p] += 1
    return {p: int(c.get(p, 0)) for p in order}


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run(
    sections: Optional[List[str]] = None,
    dry_run: bool = False,
    data_file: Optional[Path] = None,
    account_id: Optional[str] = None,
) -> bool:
    target = sections or list(SECTIONS.keys())
    df = data_file if data_file is not None else DATA_FILE

    _sync_api_globals()
    if not API_KEY:
        print("ERROR: CORALOGIX_API_KEY not set. Add it to .env or use --account with a secrets file.", file=sys.stderr)
        return False
    if not ALERTS_BASE.startswith("https://"):
        print("ERROR: CORALOGIX_API_BASE must be HTTPS", file=sys.stderr)
        return False

    # Merge into existing snapshot so untouched sections stay intact
    existing: Dict = {}
    if df.exists():
        try:
            existing = json.loads(df.read_text())
        except Exception:
            pass

    results: Dict = dict(existing)
    dash_label = "Snowbit-boutique"
    if account_id:
        try:
            from accounts_config import account_by_id, load_manifest

            _acc = account_by_id(load_manifest(), account_id)
            if _acc and str(_acc.get("label") or "").strip():
                dash_label = str(_acc.get("label")).strip()
            else:
                dash_label = str(account_id)
        except Exception:
            dash_label = str(account_id)
    reg_meta = (os.environ.get("CORALOGIX_REGION") or "").strip().upper()
    if not reg_meta:
        reg_meta = _region_guess_from_api_base(ALERTS_BASE) or "—"
    results["_meta"] = {
        "refreshed_at":            datetime.now(timezone.utc).isoformat(),
        "refreshed_sections":      target,
        "dashboard_account_id":    account_id or "default",
        "dashboard_account_label": dash_label,
        "coralogix_region":        reg_meta,
        "account":                 f"dashboard:{account_id or 'default'}",
    }

    print(f"\n{'═'*58}")
    print(f"  Coralogix Dashboard Refresher")
    if account_id:
        print(f"  Account  : {account_id}  →  {df.name}")
    print(f"  Sections : {', '.join(target)}")
    print(f"{'═'*58}\n")

    errors = []
    for key in target:
        fn = SECTIONS.get(key)
        if not fn:
            print(f"  ⚠  Unknown section '{key}' — skipped")
            continue
        print(f"  ⟳  {key} …", end="", flush=True)
        try:
            results[key] = fn()
            err = results[key].get("error") if isinstance(results[key], dict) else None
            if err:
                print(f"  ⚠  partial ({err[:70]})")
                errors.append(key)
            else:
                hint = ""
                if isinstance(results[key], dict):
                    c = results[key].get("count") or results[key].get("total")
                    if c is not None:
                        hint = f" → {c} item(s)"
                print(f"  ✓{hint}")
        except Exception as e:
            msg = str(e)[:100]
            print(f"  ✗  FAILED: {msg}")
            results[key] = {"error": msg}
            errors.append(key)

    n_join = enrich_incidents_from_alerts(results)
    if n_join:
        print(f"  ℹ  incidents: linked {n_join} row(s) to alert definition name (sourceAlertId)")

    inc_blk = results.get("incidents")
    if isinstance(inc_blk, dict) and not inc_blk.get("error"):
        inc_items = inc_blk.get("items")
        if isinstance(inc_items, list):
            if (inc_blk.get("open_total_source") or "") != "prometheus_cx_alerts":
                inc_blk["by_priority"] = _incident_priority_counts(inc_items)

    attach_never_triggered_30d(results, target)

    derived_lt = enrich_alerts_last_triggered_from_incidents(results)
    if derived_lt:
        print(f"  ℹ  lastTriggered: derived from incidents for {derived_lt} alert(s) (definitions API had no fire time)")

    # Alert HTML table needs alerts.items; partial refresh leaves old JSON without it.
    al = results.get("alerts")
    if isinstance(al, dict) and not al.get("error"):
        n_items = len(al.get("items") or [])
        if "alerts" in target and n_items:
            print(f"  ℹ  alerts.items: {n_items} rows (dashboard alert table)")
        elif "alerts" not in target and n_items == 0:
            print(
                "  ⚠  data.json still has no alerts.items — the alert definitions table will show demo data.\n"
                "     Fix: run  python3 refresh.py --section alerts  (or full refresh) with this refresh.py."
            )

    attach_log_ingestion_data_usage(results)
    attach_data_plan_units_per_day(results)
    attach_alert_hygiene(results, target)
    attach_src_customer_profile(results, target)
    attach_query_performance(results)
    attach_c4c_team_enrichment(results)
    attach_audit_active_users(results)
    attach_alert_query_field_validation(results)
    attach_monday_security_sources(results)

    try:
        from merge_ahc_into_data_json import apply_ahc_to_results

        apply_ahc_to_results(results, verbose=True)
    except ImportError as exc:
        print(f"  ⚠  ahc: merge module unavailable ({exc}) — skipping AHC merge")

    results["_meta"]["coralogix_region"] = _finalize_coralogix_region_meta(results, ALERTS_BASE)

    print()

    if dry_run:
        print("── DRY RUN — data file NOT written ───────────────────────")
        results.pop(_REFRESH_CX_ALERTS_CORR_30D_KEY, None)
        print(json.dumps(results, indent=2, default=str))
    else:
        results.pop(_REFRESH_CX_ALERTS_CORR_30D_KEY, None)
        atomic_write_text(df, json.dumps(results, indent=2, default=str))
        size_kb = df.stat().st_size / 1024
        abs_written = str(df.resolve())
        print(f"  ✓  Written → {df.name}  ({size_kb:.1f} KB)")
        print(f"     Absolute path: {abs_written}")
        print(f"     serve.py MUST run from this same folder so the browser loads this file.")
        print(f"     (gitignored — API key never written to this file)")

    if errors:
        print(f"\n  ⚠  {len(errors)} section(s) had errors: {errors}")
        print(f"     Dashboard will show last-known data for these sections.")
        print(
            "     Common causes: (1) Wrong region — root .env defaults to EU1; set CORALOGIX_REGION=US1 "
            "(or EU2, …) in this account’s secrets, or add coralogixRegion to accounts/manifest.json. "
            "(2) API key missing scopes (integrations:Read, alerts:read, metrics, data-usage:Read).",
            file=sys.stderr,
        )
    else:
        print(f"\n  ✓  All {len(target)} section(s) refreshed successfully.")

    print(f"\n{'═'*58}\n")
    if dry_run:
        return len(errors) == 0
    # Snapshot was written — partial section errors are warnings; exit 0 so UI / CI treat run as OK
    return True


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(
        description="Refresh Coralogix dashboard data (server-side, API key stays local)"
    )
    p.add_argument(
        "--section", nargs="+", choices=list(SECTIONS.keys()),
        help="Refresh specific section(s) only (default: all)"
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Fetch data and print JSON without writing data.json"
    )
    p.add_argument(
        "--account",
        metavar="ID",
        default=None,
        help="Account id from accounts/manifest.json (uses that account's dataFile + secretsFile)",
    )
    args = p.parse_args()
    data_path = DATA_FILE
    acc_id: Optional[str] = None
    if args.account:
        data_path = _configure_account_environment(args.account)
        acc_id = args.account
    else:
        _sync_api_globals()
        if not API_KEY:
            print("ERROR: CORALOGIX_API_KEY not set. Add it to .env or use --account <id>", file=sys.stderr)
            sys.exit(1)
        if not ALERTS_BASE.startswith("https://"):
            print("ERROR: CORALOGIX_API_BASE must be HTTPS", file=sys.stderr)
            sys.exit(1)
    ok = run(sections=args.section, dry_run=args.dry_run, data_file=data_path, account_id=acc_id)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
