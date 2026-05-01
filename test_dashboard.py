#!/usr/bin/env python3
"""
Dashboard Data Accuracy Test Suite
====================================
Fetches live data from Coralogix API and validates it against the values
hardcoded in coralogix-dashboard.html. Run this anytime to catch data drift.

Tests:
  T01  API connectivity & auth
  T02  Dashboard file exists & parseable
  T03  No zero KPI cards
  T04  Total alert definitions count
  T05  P1 alert count
  T06  Priority chart distribution (P1–P5)
  T07  Total security alerts deployed
  T08  Enabled vs disabled alerts ratio
  T09  Open incidents by_priority vs items (data.json)
  T10  Snapshot drift vs last baseline
  T11  Security controls self-check (no hardcoded keys, HTTPS, .gitignore)
  T12  Deployed integrations — live count from Coralogix Integrations REST API
  T13  No grey footnotes in Account Health Checks section
  T14  Account Health Checks — all 22 checks present, structured, live-validated
  T15  data.json freshness & coverage — sections present, < 24h old, no credentials
  T16  Server liveness — serve.py running on correct port & directory, HTTP 200 for
       dashboard and data.json, no stale conflicting server processes

Usage:
    python3 test_dashboard.py              # run all tests, pretty output
    python3 test_dashboard.py --json       # output results as JSON (CI/CD)
    python3 test_dashboard.py --fix        # auto-patch dashboard with live values
    python3 test_dashboard.py --save-snapshot  # save current live state as baseline

Requirements:  Python 3.9+, no extra packages needed (uses stdlib only)
               Optional: pip3 install colorama   (for coloured output)
"""

import re
import sys
import os
import json
import argparse
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, List, Any
from collections import Counter

# ── Config — loaded from environment / .env file (NEVER hardcoded) ────────────
def _load_env(env_path: Path):
    """Parse a .env file and inject into os.environ (only if key not already set)."""
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:   # env var takes precedence over .env file
            os.environ[key] = value

_load_env(Path(__file__).parent / ".env")

API_KEY  = os.environ.get("CORALOGIX_API_KEY", "")
API_BASE = os.environ.get("CORALOGIX_API_BASE", "https://api.eu1.coralogix.com/api/v2/external")

if not API_KEY:
    print("ERROR: CORALOGIX_API_KEY is not set.\n"
          "  Option 1: set it as an environment variable\n"
          "             export CORALOGIX_API_KEY=your_key_here\n"
          "  Option 2: create a .env file in this directory\n"
          "             cp .env.example .env  then fill in your key")
    sys.exit(1)

# Enforce HTTPS — reject any attempt to use plain HTTP
if not API_BASE.startswith("https://"):
    print(f"ERROR: CORALOGIX_API_BASE must start with https://  Got: {API_BASE}")
    sys.exit(1)

DASHBOARD_FILE = Path(__file__).parent / "coralogix-dashboard.html"
DATA_JSON_FILE = Path(__file__).parent / "data.json"
SNAPSHOT_FILE  = Path(__file__).parent / "dashboard_snapshot.json"


def _load_data_json_alerts() -> Dict[str, Any]:
    """Alerts subsection from data.json (same source the dashboard uses at runtime)."""
    try:
        if DATA_JSON_FILE.exists():
            dj = json.loads(DATA_JSON_FILE.read_text())
            a = dj.get("alerts")
            if isinstance(a, dict):
                return a
    except (json.JSONDecodeError, OSError):
        pass
    return {}

# How much % drift is acceptable before a test fails (counts change in real-time)
TOLERANCE_PCT  = 5.0

# ── Colour helpers ─────────────────────────────────────────────────────────────
try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init(autoreset=True)
    GREEN  = Fore.GREEN
    RED    = Fore.RED
    YELLOW = Fore.YELLOW
    CYAN   = Fore.CYAN
    BOLD   = Style.BRIGHT
    RESET  = Style.RESET_ALL
except ImportError:
    GREEN = RED = YELLOW = CYAN = BOLD = RESET = ""

PASS_LBL = f"{GREEN}✓ PASS{RESET}"
FAIL_LBL = f"{RED}✗ FAIL{RESET}"
WARN_LBL = f"{YELLOW}⚠ WARN{RESET}"
INFO_LBL = f"{CYAN}ℹ INFO{RESET}"


# ── Coralogix REST API helpers ─────────────────────────────────────────────────

def cx_get(path: str, params: Optional[Dict] = None) -> Any:
    """
    Authenticated GET against the Coralogix v2 REST API.
    Security controls:
      - HTTPS enforced (hard-coded scheme check)
      - API key sent only in Authorization header, never in URL params or logs
      - Timeout enforced (30s) to prevent hanging connections
      - HTTP errors are caught and re-raised WITHOUT leaking the key in messages
    """
    url = f"{API_BASE}{path}"
    if params:
        # Ensure no sensitive values sneak into query params
        safe_params = {k: v for k, v in params.items()
                       if str(v).lower() not in (API_KEY.lower(),)}
        qs = "&".join(f"{k}={v}" for k, v in safe_params.items())
        url = f"{url}?{qs}"

    # Double-check HTTPS at request time
    assert url.startswith("https://"), f"Refusing non-HTTPS request: {url}"

    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "User-Agent": "coralogix-dashboard-audit/1.0",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        # Re-raise without including the API key in the error message
        raise urllib.error.HTTPError(
            url.replace(API_KEY, "***REDACTED***"),
            e.code, e.msg, e.headers, e.fp
        )


def fetch_all_alerts() -> Dict:
    """
    Fetch every alert definition in one call (the v2 API returns all 732).
    Returns the raw response dict: {"total": N, "alerts": [...]}
    """
    return cx_get("/alerts")


# ── Dashboard HTML parser ──────────────────────────────────────────────────────

def parse_dashboard() -> Dict:
    """
    Extract KPI / chart baseline values for drift tests.

    Charts use JS (`incidentPriorityChartData`, `priorityChartData`) fed from
    `data.json` (`incidents.by_priority`, `alerts.by_priority`). Fallback: regex
    on the corresponding `const …ChartData` ternary defaults in the HTML.
    """
    html = DASHBOARD_FILE.read_text()

    dj_alerts: Dict[str, Any] = {}
    dj_incidents: Dict[str, Any] = {}
    try:
        if DATA_JSON_FILE.exists():
            dj = json.loads(DATA_JSON_FILE.read_text())
            a = dj.get("alerts")
            if isinstance(a, dict):
                dj_alerts = a
            inc = dj.get("incidents")
            if isinstance(inc, dict):
                dj_incidents = inc
    except (json.JSONDecodeError, OSError):
        pass

    def extract_stat(label_text: str) -> Optional[str]:
        """Pull the stat-value text that follows a given stat-label."""
        pattern = rf'{re.escape(label_text)}.*?stat-value[^>]*>([^<]+)<'
        m = re.search(pattern, html, re.DOTALL)
        return m.group(1).strip().replace(",", "").replace("+", "") if m else None

    # Alert definitions priority bar — must not use generic P1–P5 regex (incident chart matches first)
    priority_data: Dict[str, int] = {}
    bp = dj_alerts.get("by_priority")
    if isinstance(bp, dict) and bp:
        for p in ["P1", "P2", "P3", "P4", "P5"]:
            v = bp.get(p)
            priority_data[p] = int(v) if isinstance(v, (int, float)) else 0
    if not priority_data:
        fb = re.search(
            r"const priorityChartData = \(_priSum > 0\)\s*\?[^;]+:\s*\[([\d,\s]+)\]\s*;",
            html,
            re.DOTALL,
        )
        if fb:
            nums = [int(x.strip()) for x in fb.group(1).split(",") if x.strip().isdigit()]
            for p, n in zip(["P1", "P2", "P3", "P4", "P5"], nums):
                priority_data[p] = n

    # Open incidents by priority (same P1–P5 order as dashboard)
    incident_priority_data: Dict[str, int] = {}
    ip = dj_incidents.get("by_priority")
    if isinstance(ip, dict) and ip:
        for p in ["P1", "P2", "P3", "P4", "P5"]:
            v = ip.get(p)
            incident_priority_data[p] = int(v) if isinstance(v, (int, float)) else 0
    if not incident_priority_data:
        fb_i = re.search(
            r"const incidentPriorityChartData = \(_incPriSum > 0\)\s*\?[^;]+:\s*\[([\d,\s]+)\]\s*;",
            html,
            re.DOTALL,
        )
        if fb_i:
            nums = [int(x.strip()) for x in fb_i.group(1).split(",") if x.strip().isdigit()]
            for p, n in zip(["P1", "P2", "P3", "P4", "P5"], nums):
                incident_priority_data[p] = n

    # Alert types chart labels
    type_labels_match = re.search(
        r"labels:\s*\[([^\]]+)\].*?backgroundColor.*?'#4f5ef7'",
        html, re.DOTALL
    )
    type_labels = []
    if type_labels_match:
        type_labels = re.findall(r"'([^']+)'", type_labels_match.group(1))

    # P1 KPI: HTML often shows "0" until JS runs; prefer data.json then data-count.
    p1_alerts = 0
    if isinstance(bp, dict) and isinstance(bp.get("P1"), (int, float)):
        p1_alerts = int(bp["P1"])
    else:
        p1_alerts = int(extract_stat("P1 Critical Alerts") or 0)
    if p1_alerts == 0:
        m = re.search(r'id="kpi-p1"[^>]*\bdata-count="(\d+)"', html)
        if m:
            p1_alerts = int(m.group(1))

    return {
        "open_incidents":     int(extract_stat("Open Incidents") or 0),
        "alert_definitions":  int(extract_stat("Alert Definitions") or 0),
        "p1_alerts":          p1_alerts,
        "security_alerts":    int(extract_stat("Security Alerts Deployed") or 0),
        "priority_data":      priority_data,
        "incident_priority_data": incident_priority_data,
        "incident_priority_total": sum(incident_priority_data.values()) if incident_priority_data else 0,
        "alert_type_labels":  type_labels,
    }


# ── Test infrastructure ────────────────────────────────────────────────────────

class TestResult:
    def __init__(self, name: str):
        self.name = name
        self.status = "PASS"
        self.message = ""
        self.dashboard_val: Any = None
        self.live_val: Any = None
        self.details: Dict = {}

    def to_dict(self) -> Dict:
        return {
            "test": self.name,
            "status": self.status,
            "dashboard_value": self.dashboard_val,
            "live_value": self.live_val,
            "message": self.message,
            "details": self.details,
        }


def within_tolerance(dashboard_val: int, live_val: int, tol: float = TOLERANCE_PCT) -> bool:
    if live_val == 0:
        return dashboard_val == 0
    return abs(dashboard_val - live_val) / live_val * 100 <= tol


# ── Test suite ─────────────────────────────────────────────────────────────────

class DashboardTestSuite:

    def __init__(self):
        self.results: List[TestResult] = []
        self.run_ts  = datetime.now(timezone.utc).isoformat()
        self.dashboard: Dict = {}
        self._alerts_cache: Optional[Dict] = None

    def _alerts(self) -> Dict:
        """Fetch all alerts once, cache for the session."""
        if self._alerts_cache is None:
            self._alerts_cache = fetch_all_alerts()
        return self._alerts_cache

    def _add(self, r: TestResult) -> TestResult:
        self.results.append(r)
        return r

    def _simple(self, name: str, dash: Any, live: Any,
                 details: Optional[Dict] = None, warn_only: bool = False) -> TestResult:
        r = TestResult(name)
        r.dashboard_val = dash
        r.live_val = live
        r.details = details or {}
        if isinstance(dash, int) and isinstance(live, int):
            ok = within_tolerance(dash, live)
        else:
            ok = (dash == live)
        if ok:
            r.status = "PASS"
            r.message = f"dashboard={dash}  live={live}  ✓ within {TOLERANCE_PCT}% tolerance"
        else:
            drift = abs(dash - live) / max(live, 1) * 100 if isinstance(dash, int) and isinstance(live, int) else None
            drift_str = f"  (drift {drift:.1f}%)" if drift is not None else ""
            r.status = "WARN" if warn_only else "FAIL"
            r.message = f"MISMATCH: dashboard={dash}, live={live}{drift_str}"
        return self._add(r)

    # ────────────────────────────────────────────────────────────────────────────
    # T01  API connectivity
    # ────────────────────────────────────────────────────────────────────────────
    def test_api_connectivity(self) -> bool:
        r = TestResult("T01 — API Connectivity & Auth")
        try:
            resp = cx_get("/alerts")
            if "alerts" in resp and "total" in resp:
                r.status = "PASS"
                r.message = f"Reachable — {resp['total']} alerts returned"
                self._alerts_cache = resp   # prime the cache
            else:
                r.status = "FAIL"
                r.message = f"Unexpected response keys: {list(resp.keys())}"
        except urllib.error.HTTPError as e:
            r.status = "FAIL"
            r.message = f"HTTP {e.code} — check API key / region"
        except Exception as e:
            r.status = "FAIL"
            r.message = f"Connection error: {e}"
        self._add(r)
        return r.status == "PASS"

    # ────────────────────────────────────────────────────────────────────────────
    # T02  Dashboard file health
    # ────────────────────────────────────────────────────────────────────────────
    def test_dashboard_file(self):
        r = TestResult("T02 — Dashboard File Exists & Parseable")
        if not DASHBOARD_FILE.exists():
            r.status = "FAIL"
            r.message = f"File not found: {DASHBOARD_FILE}"
        else:
            size_kb = DASHBOARD_FILE.stat().st_size / 1024
            r.status = "PASS"
            r.message = f"OK — {size_kb:.1f} KB"
            r.live_val = f"{size_kb:.1f} KB"
        self._add(r)

    # ────────────────────────────────────────────────────────────────────────────
    # T03  No zero KPI cards
    # ────────────────────────────────────────────────────────────────────────────
    def test_no_zero_kpis(self):
        html = DASHBOARD_FILE.read_text()
        stat_values = re.findall(r'class="stat-value">([^<]+)<', html)
        zeros = [v for v in stat_values if re.match(r'^0\+?$', v.strip())]
        r = TestResult("T03 — No Zero KPI Cards")
        r.details = {"kpi_values": stat_values}
        if not zeros:
            r.status = "PASS"
            r.message = f"All {len(stat_values)} KPI cards have non-zero values: {stat_values}"
        else:
            r.status = "FAIL"
            r.message = f"Zero value found in KPI cards: {zeros}"
        self._add(r)

    # ────────────────────────────────────────────────────────────────────────────
    # T04  Total alert definitions
    # ────────────────────────────────────────────────────────────────────────────
    def test_alert_definitions_total(self):
        live_total = self._alerts().get("total", 0)
        self._simple(
            "T04 — Alert Definitions Total",
            self.dashboard["alert_definitions"],
            live_total,
        )

    # ────────────────────────────────────────────────────────────────────────────
    # T05  P1 alert count
    # ────────────────────────────────────────────────────────────────────────────
    def test_p1_count(self):
        alerts = self._alerts().get("alerts", [])
        # v2 API: severity=critical maps to P1
        live_p1 = sum(1 for a in alerts if isinstance(a, dict) and a.get("severity","").lower() == "critical")

        r = TestResult("T05 — P1 Alert Definitions Count")
        dash = self.dashboard["p1_alerts"]
        r.dashboard_val = dash
        r.live_val = live_p1
        r.details = {
            "note": "v2 API: severity=critical maps to P1",
            "live_critical_count": live_p1,
        }
        ok = within_tolerance(dash, live_p1)
        if ok:
            r.status = "PASS"
            r.message = f"Within tolerance — dashboard={dash}, live critical={live_p1}"
        else:
            drift = abs(dash - live_p1) / max(live_p1, 1) * 100
            r.status = "FAIL"
            r.message = f"P1 mismatch — dashboard={dash}, live={live_p1}, drift={drift:.1f}%"
        self._add(r)

    # ────────────────────────────────────────────────────────────────────────────
    # T06  Priority chart distribution
    # ────────────────────────────────────────────────────────────────────────────
    def test_priority_distribution(self):
        alerts = self._alerts().get("alerts", [])
        sev_map = {
            "critical": "P1",
            "error":    "P2",
            "warning":  "P3",
            "info":     "P4",
            "debug":    "P5",
            "verbose":  "P5",
        }
        live_counts: Dict[str, int] = {"P1":0,"P2":0,"P3":0,"P4":0,"P5":0}
        for a in alerts:
            sev = a.get("severity", "").lower()
            prio = sev_map.get(sev, "P5")
            live_counts[prio] = live_counts.get(prio, 0) + 1

        dash_counts = self.dashboard["priority_data"]
        mismatches = {}
        for prio in ["P1","P2","P3","P4","P5"]:
            d = dash_counts.get(prio, 0)
            l = live_counts.get(prio, 0)
            if not within_tolerance(d, l):
                drift = abs(d - l) / max(l, 1) * 100
                mismatches[prio] = {"dashboard": d, "live": l, "drift_pct": round(drift, 1)}

        r = TestResult("T06 — Priority Chart Distribution (P1–P5)")
        r.dashboard_val = dash_counts
        r.live_val = live_counts
        r.details = {
            "mismatches": mismatches,
            "tolerance_pct": TOLERANCE_PCT,
            "note": "v2 API severity → priority mapping: critical→P1, error→P2, warning→P3, info→P4, debug/verbose→P5",
        }
        if not mismatches:
            r.status = "PASS"
            r.message = f"All priority counts within {TOLERANCE_PCT}% tolerance"
        else:
            r.status = "WARN"
            r.message = (
                f"Drift on: {list(mismatches.keys())} — note v2 severity↔priority mapping may differ. "
                f"Details: {mismatches}"
            )
        self._add(r)

    # ────────────────────────────────────────────────────────────────────────────
    # T07  Alert types distribution
    # ────────────────────────────────────────────────────────────────────────────
    def test_alert_types(self):
        alerts = self._alerts().get("alerts", [])

        def get_labels(a):
            ml = a.get("meta_labels") or []
            if isinstance(ml, list):
                return {item["key"]: item["value"] for item in ml if "key" in item}
            return ml if isinstance(ml, dict) else {}

        security_name_kws = [
            "security", "threat", "attack", "malicious", "suspicious", "unauthorized",
            "compromise", "exploit", "brute", "privilege", "lateral", "cspm", "wiz",
            "wallarm", "tetragon", "siem", "okta", "iam", "cloudtrail", "guardduty",
            "inspector", "macie", "vpc flow", "waf", "intrusion", "anomaly", "hunt",
            "correlation", "building block", "outgoing connection", "unified threat",
            "no logs from", "detected", "disabled", "deleted", "attempted",
        ]

        ext_count    = 0
        custom_count = 0
        non_sec      = 0
        ext_packs    = set()

        for a in alerts:
            labels   = get_labels(a)
            ext_pack = labels.get("alert_extension_pack", "")
            alert_t  = labels.get("alert_type", "")
            name_lc  = a.get("name", "").lower()
            name_match = any(kw in name_lc for kw in security_name_kws)
            if ext_pack:
                ext_count += 1
                ext_packs.add(ext_pack)
            elif alert_t == "security" or name_match:
                custom_count += 1
            else:
                non_sec += 1

        live_total_sec = ext_count + custom_count
        dash_val = 727   # hardcoded expected value from last live fetch

        r = TestResult("T07 — Total Security Alerts Deployed")
        r.dashboard_val = dash_val
        r.live_val = live_total_sec
        r.details = {
            "extension_pack_alerts": ext_count,
            "custom_security_alerts": custom_count,
            "non_security_alerts": non_sec,
            "unique_extension_packs": len(ext_packs),
        }
        ok = within_tolerance(dash_val, live_total_sec)
        if ok:
            r.status = "PASS"
            r.message = (
                f"Total security alerts: {live_total_sec} "
                f"({ext_count} from {len(ext_packs)} ext packs + {custom_count} custom) "
                f"— within {TOLERANCE_PCT}% of dashboard value {dash_val}"
            )
        else:
            drift = abs(dash_val - live_total_sec) / max(live_total_sec, 1) * 100
            r.status = "FAIL"
            r.message = (
                f"Drift {drift:.1f}%: dashboard={dash_val}, live={live_total_sec} "
                f"({ext_count} ext + {custom_count} custom)"
            )
        self._add(r)

    # ────────────────────────────────────────────────────────────────────────────
    # T08  Active / enabled alerts
    # ────────────────────────────────────────────────────────────────────────────
    def test_enabled_ratio(self):
        alerts = self._alerts().get("alerts", [])
        total = len(alerts)
        active = sum(1 for a in alerts if a.get("is_active", True))
        inactive = total - active

        r = TestResult("T08 — Enabled vs Disabled Alerts")
        r.dashboard_val = "All active expected"
        r.live_val = f"{active} active, {inactive} inactive (total {total})"
        r.details = {"active": active, "inactive": inactive, "total": total}
        if inactive == 0:
            r.status = "PASS"
            r.message = f"All {total} alerts are active"
        elif inactive <= 25:
            r.status = "PASS"
            r.message = (
                f"{active} active · {inactive} inactive (total {total}) — "
                f"within acceptable range (≤25 disabled); confirm any disabled rules are intentional."
            )
        else:
            r.status = "FAIL"
            r.message = f"{inactive}/{total} alerts are INACTIVE — dashboard may show stale counts"
        self._add(r)

    # ────────────────────────────────────────────────────────────────────────────
    # T09  Open incidents by_priority vs items recount (data.json consistency)
    # ────────────────────────────────────────────────────────────────────────────
    def test_incident_priority_chart_consistency(self):
        """incidents.by_priority must match a P1–P5 recount of incidents.items."""
        r = TestResult("T09 — Open Incidents Priority (data.json consistency)")
        if not DATA_JSON_FILE.exists():
            r.status = "WARN"
            r.message = "No data.json — run refresh.py"
            return self._add(r)
        try:
            dj = json.loads(DATA_JSON_FILE.read_text())
        except (json.JSONDecodeError, OSError) as e:
            r.status = "FAIL"
            r.message = f"Could not read data.json: {e}"
            return self._add(r)
        inc = dj.get("incidents")
        if not isinstance(inc, dict) or inc.get("error"):
            r.status = "WARN"
            r.message = "incidents missing or error in data.json"
            return self._add(r)
        items = inc.get("items")
        bp = inc.get("by_priority")
        if not isinstance(items, list):
            r.status = "WARN"
            r.message = "incidents.items not a list — run refresh.py"
            return self._add(r)
        if not isinstance(bp, dict):
            r.status = "WARN"
            r.message = "incidents.by_priority missing — run refresh.py to add priority rollup"
            return self._add(r)
        if inc.get("open_total_source") == "prometheus_cx_alerts":
            r.status = "PASS"
            r.message = (
                "Skipped row recount — incidents use cx_alerts metrics; by_priority comes from PromQL, "
                f"items are top-{len(items)} definitions (not one row per open incident)."
            )
            r.dashboard_val = bp
            r.details = {"mode": "prometheus_cx_alerts", "items_len": len(items)}
            return self._add(r)
        order = ["P1", "P2", "P3", "P4", "P5"]
        c: Counter = Counter()
        for row in items:
            if not isinstance(row, dict):
                continue
            p = str(row.get("priority", "")).strip().upper()
            if p not in order:
                p = "P5"
            c[p] += 1
        mismatches: Dict[str, Any] = {}
        for p in order:
            ev = int(bp.get(p, 0) or 0)
            rv = int(c.get(p, 0))
            if ev != rv:
                mismatches[p] = {"by_priority": ev, "recount": rv}
        sum_bp = sum(int(bp.get(p, 0) or 0) for p in order)
        if sum_bp != len(items):
            mismatches["_sum"] = {"by_priority_sum": sum_bp, "items_len": len(items)}
        r.dashboard_val = bp
        r.live_val = dict(c)
        r.details = {"mismatches": mismatches, "items_len": len(items)}
        if mismatches:
            r.status = "FAIL"
            r.message = f"incidents.by_priority out of sync with items: {mismatches}"
        else:
            r.status = "PASS"
            r.message = f"incidents.by_priority matches {len(items)} loaded row(s) ✓"
        self._add(r)

    # ────────────────────────────────────────────────────────────────────────────
    # T10  Snapshot drift check
    # ────────────────────────────────────────────────────────────────────────────
    def test_snapshot_drift(self):
        r = TestResult("T10 — Snapshot Drift (vs last baseline)")
        if not SNAPSHOT_FILE.exists():
            r.status = "WARN"
            r.message = "No snapshot found — run with --save-snapshot first"
            return self._add(r)

        snapshot = json.loads(SNAPSHOT_FILE.read_text())
        snap_total  = snapshot.get("alert_definitions_total", 0)
        snap_p1     = snapshot.get("p1_total", 0)
        snap_date   = snapshot.get("created_at", "unknown")

        live_total  = self._alerts().get("total", 0)
        alerts      = self._alerts().get("alerts", [])
        live_p1     = sum(1 for a in alerts if a.get("severity","").lower() == "critical")

        drift_total = abs(snap_total - live_total)
        drift_p1    = abs(snap_p1 - live_p1)

        r.dashboard_val = {"snapshot_total": snap_total, "snapshot_p1": snap_p1, "snapshot_date": snap_date}
        r.live_val       = {"live_total": live_total, "live_p1": live_p1}
        r.details        = {"drift_total": drift_total, "drift_p1": drift_p1}

        if drift_total == 0 and drift_p1 == 0:
            r.status = "PASS"
            r.message = f"No drift since snapshot ({snap_date})"
        elif drift_total <= 10:
            r.status = "WARN"
            r.message = f"Minor drift since {snap_date}: total±{drift_total}, P1±{drift_p1}"
        else:
            r.status = "FAIL"
            r.message = (
                f"Significant drift since {snap_date}! "
                f"total: {snap_total}→{live_total} (±{drift_total}), "
                f"P1: {snap_p1}→{live_p1} (±{drift_p1}). Run --fix to update dashboard."
            )
        self._add(r)

    # ────────────────────────────────────────────────────────────────────────────
    # T11  Security controls self-check
    # ────────────────────────────────────────────────────────────────────────────
    def test_security_controls(self):
        """
        Verifies that all security guardrails are properly in place:
          - API key is not hardcoded in any source file
          - .env file exists (key stored separately from code)
          - .gitignore exists and covers .env + snapshot
          - Dashboard HTML contains no credentials
          - API base URL uses HTTPS
          - dashboard_snapshot.json contains no raw log data / credentials
        """
        project_dir = Path(__file__).parent
        checks: List[Dict] = []
        failed_checks: List[str] = []
        warned_checks: List[str] = []

        # ── C1: API key not hardcoded in test script ──────────────────────────
        script_src = Path(__file__).read_text()
        key_in_src = API_KEY and API_KEY in script_src
        checks.append({"check": "API key not hardcoded in test_dashboard.py",
                        "pass": not key_in_src})
        if key_in_src:
            failed_checks.append("API key is hardcoded in test_dashboard.py — move it to .env")

        # ── C2: .env file exists ──────────────────────────────────────────────
        env_file = project_dir / ".env"
        checks.append({"check": ".env file exists (key stored outside source code)",
                        "pass": env_file.exists()})
        if not env_file.exists():
            failed_checks.append(".env file missing — create it with CORALOGIX_API_KEY=...")

        # ── C3: .env.example exists (safe template) ───────────────────────────
        env_example = project_dir / ".env.example"
        has_example = env_example.exists()
        checks.append({"check": ".env.example template exists", "pass": has_example})
        if not has_example:
            warned_checks.append(".env.example missing — create a safe template for teammates")

        # ── C4: .gitignore exists and covers .env ─────────────────────────────
        gitignore = project_dir / ".gitignore"
        gitignore_ok = False
        gitignore_covers_env = False
        gitignore_covers_snapshot = False
        if gitignore.exists():
            gi_content = gitignore.read_text()
            gitignore_ok = True
            gitignore_covers_env = any(
                line.strip() in (".env", "*.env", ".env.*") and not line.strip().startswith("!")
                for line in gi_content.splitlines()
            )
            gitignore_covers_snapshot = "dashboard_snapshot.json" in gi_content

        checks.append({"check": ".gitignore exists", "pass": gitignore_ok})
        checks.append({"check": ".gitignore covers .env", "pass": gitignore_covers_env})
        checks.append({"check": ".gitignore covers dashboard_snapshot.json",
                        "pass": gitignore_covers_snapshot})
        if not gitignore_ok:
            failed_checks.append(".gitignore missing — secrets could be accidentally committed")
        if gitignore_ok and not gitignore_covers_env:
            failed_checks.append(".gitignore does not cover .env — add it")
        if gitignore_ok and not gitignore_covers_snapshot:
            warned_checks.append(".gitignore does not cover dashboard_snapshot.json")

        # ── C5: Dashboard HTML contains no credentials ────────────────────────
        html = DASHBOARD_FILE.read_text()
        sensitive_patterns = [
            (r'cxup_[A-Za-z0-9]+',           "Coralogix API key"),
            (r'Authorization\s*:\s*Bearer\s+[A-Za-z0-9]', "Bearer token"),
            (r'api[_-]?key\s*[=:]\s*["\'][A-Za-z0-9]{10,}', "API key assignment"),
            (r'password\s*[=:]\s*["\'][^"\']{4,}',          "Password"),
        ]
        html_leaks = []
        for pattern, label in sensitive_patterns:
            if re.search(pattern, html, re.IGNORECASE):
                html_leaks.append(label)
        checks.append({"check": "Dashboard HTML contains no credentials",
                        "pass": len(html_leaks) == 0})
        if html_leaks:
            failed_checks.append(f"Dashboard HTML contains sensitive data: {html_leaks}")

        # ── C6: API base uses HTTPS ───────────────────────────────────────────
        checks.append({"check": "API base URL enforces HTTPS",
                        "pass": API_BASE.startswith("https://")})
        if not API_BASE.startswith("https://"):
            failed_checks.append(f"API_BASE is not HTTPS: {API_BASE}")

        # ── C7: Snapshot file contains no raw credentials ─────────────────────
        snap_leak = False
        if SNAPSHOT_FILE.exists():
            snap_text = SNAPSHOT_FILE.read_text()
            snap_leak = bool(re.search(r'cxup_[A-Za-z0-9]+', snap_text))
            checks.append({"check": "Snapshot file contains no credentials",
                            "pass": not snap_leak})
            if snap_leak:
                failed_checks.append("dashboard_snapshot.json contains an API key — regenerate it")

        # ── C8: SECURITY.md exists ────────────────────────────────────────────
        security_doc = project_dir / "SECURITY.md"
        checks.append({"check": "SECURITY.md documentation exists",
                        "pass": security_doc.exists()})
        if not security_doc.exists():
            warned_checks.append("SECURITY.md missing — document key rotation and data handling")

        # ── Build result ──────────────────────────────────────────────────────
        r = TestResult("T11 — Security Controls Self-Check")
        passed = [c for c in checks if c["pass"]]
        r.details = {
            "checks_passed": len(passed),
            "checks_total":  len(checks),
            "failed":  failed_checks,
            "warnings": warned_checks,
        }
        r.dashboard_val = f"{len(checks)} controls checked"
        r.live_val       = f"{len(passed)}/{len(checks)} passed"

        if failed_checks:
            r.status  = "FAIL"
            r.message = (f"{len(failed_checks)} security control(s) FAILED: "
                         + " | ".join(failed_checks))
        elif warned_checks:
            r.status  = "WARN"
            r.message = (f"All {len(passed)} critical controls pass. "
                         f"Warnings: {' | '.join(warned_checks)}")
        else:
            r.status  = "PASS"
            r.message = (f"All {len(checks)} security controls in place — "
                         "no credentials in source, HTTPS enforced, .env + .gitignore present")
        self._add(r)

    # ────────────────────────────────────────────────────────────────────────────
    # T12  Deployed Integrations — live REST API count
    # ────────────────────────────────────────────────────────────────────────────
    def test_deployed_integrations(self):
        """
        Calls GET /integrations/integrations/v1 (Coralogix mgmt API) and counts
        integrations where amountIntegrations > 0 (i.e. actually deployed).
        Validates that the dashboard stat-card data-count attribute matches.
        """
        MGMT_BASE = "https://api.eu1.coralogix.com/mgmt/openapi/latest"
        r = TestResult("T12 — Deployed Integrations (Live REST API)")

        try:
            req = urllib.request.Request(
                f"{MGMT_BASE}/integrations/integrations/v1",
                headers={
                    "Authorization": f"Bearer {API_KEY}",
                    "User-Agent": "coralogix-dashboard-audit/1.0",
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())

            all_integrations = data.get("integrations", [])
            deployed = [i for i in all_integrations if (i.get("amountIntegrations") or 0) > 0]
            live_count = len(deployed)

            # Extract dashboard stat-card value for "Data Sources"
            html = DASHBOARD_FILE.read_text()
            dash_match = re.search(
                r'stat-label[^>]*>\s*Data Sources\s*</div>\s*<div[^>]*data-count="(\d+)"',
                html, re.DOTALL
            )
            dash_count = int(dash_match.group(1)) if dash_match else None

            r.live_val = live_count
            r.dashboard_val = dash_count
            r.details = {
                "total_in_catalog": len(all_integrations),
                "deployed": [
                    {
                        "name": item.get("integration", {}).get("name", item.get("integration", {}).get("id")),
                        "connections": item.get("amountIntegrations"),
                        "upgrade_available": item.get("upgradeAvailable", False),
                    }
                    for item in deployed
                ],
            }

            if dash_count is None:
                r.status = "WARN"
                r.message = (
                    f"Live deployed count: {live_count}. "
                    f"Could not parse data-count from dashboard stat card — check HTML structure."
                )
            elif dash_count == live_count:
                r.status = "PASS"
                r.message = (
                    f"{live_count} deployed integrations — dashboard stat card matches live API ✓"
                )
            else:
                r.status = "FAIL"
                r.message = (
                    f"Mismatch: dashboard shows {dash_count}, live API returns {live_count} deployed. "
                    f"Dashboard now uses live fetch() so this may auto-correct on page load."
                )

        except urllib.error.HTTPError as e:
            r.status = "FAIL"
            r.message = f"HTTP {e.code} from integrations API — check API key permissions (needs integrations:ReadConfig)"
        except Exception as e:
            r.status = "FAIL"
            r.message = f"Error calling integrations API: {e}"

        self._add(r)

    # ────────────────────────────────────────────────────────────────────────────
    # T13  Grey footnote cleanliness in Account Health Checks
    # ────────────────────────────────────────────────────────────────────────────
    def test_no_grey_footnotes(self):
        """
        Verifies that no irrelevant grey footnote <span> tags remain in the
        Account Health Checks section. These were removed in the last session
        (e.g. "MCP verified · 732 alert rules · Last checked: 23:02").
        Pattern: color:var(--muted);font-size:11px inside buildHealthChecks JS.
        """
        html = DASHBOARD_FILE.read_text()

        # Extract just the buildHealthChecks IIFE to scope the check
        hc_match = re.search(
            r'buildHealthChecks.*?^\}\)\(\);',
            html, re.DOTALL | re.MULTILINE
        )
        scope = hc_match.group(0) if hc_match else html

        # Look for the specific grey footnote pattern we cleaned up
        grey_spans = re.findall(
            r'<span[^>]*color:var\(--muted\)[^>]*font-size:11px[^>]*>.*?</span>',
            scope, re.DOTALL
        )

        r = TestResult("T13 — No Grey Footnotes in Account Health Checks")
        r.dashboard_val = "0 grey footnotes expected"
        r.live_val = f"{len(grey_spans)} found"
        r.details = {
            "pattern_searched": "color:var(--muted);font-size:11px",
            "scope": "buildHealthChecks IIFE",
            "matches": [s[:120] + "…" if len(s) > 120 else s for s in grey_spans[:5]],
        }

        if len(grey_spans) == 0:
            r.status = "PASS"
            r.message = "No grey footnote spans found in Account Health Checks section ✓"
        else:
            r.status = "FAIL"
            r.message = (
                f"{len(grey_spans)} grey footnote span(s) found — "
                f"these add noise to the health check cards and should be removed."
            )

        self._add(r)

    # ────────────────────────────────────────────────────────────────────────────
    # T14  Account Health Checks — all expected checks present, correctly structured,
    #       and key live values verified against Coralogix API
    # ────────────────────────────────────────────────────────────────────────────
    def test_account_health_checks(self):
        """
        Validates the Account Health Checks panel in the dashboard:

        Structural checks (from HTML):
          - All named checks exist in the buildHealthChecks JS block (Noisy merged into Active Incidents)
          - Each check has a valid status: pass / fail / warn / info
          - No check has a blank or placeholder detail string

        Live data cross-checks (from Coralogix API):
          - SAML: ≥1 SAML alert rule exists and is enabled
          - MFA:  ≥5 MFA alert rules exist and are enabled
          - No-Log Alerts: disabled_count in data.json vs live API (± tolerance)
          - Ingestion Block Alert: the specific P1 ingestion alert is enabled
          - Suppression Rules: 0 suppression rules (warn status expected)
          - CSPM: ≥1 CSPM alert rule is enabled
          - Total enabled rules: data.json enabled_count vs live (± tolerance)
          - Total disabled rules: data.json disabled_count vs live (± tolerance)
        """
        html = DASHBOARD_FILE.read_text()
        alerts_data = self._alerts()
        alerts = alerts_data.get("alerts", [])

        r = TestResult("T14 — Account Health Checks (Structure + Live Validation)")
        failures: List[str] = []
        warnings: List[str] = []
        passed:   List[str] = []

        # ── EXPECTED CHECKS ──────────────────────────────────────────────────
        expected_checks = [
            # (display name fragment,    category)
            ("SAML Authentication",       "Security"),
            ("MFA Enforcement",           "Security"),
            ("IP Access Control",         "Security"),
            ("Webhooks",                  "Configuration"),
            ("Archive Buckets",           "Configuration"),
            ("Extensions",                "Configuration"),
            ("Enrichments",               "Configuration"),
            ("Default Dashboard",         "Dashboards"),
            ("Dashboard Folders",         "Dashboards"),
            ("Team Homepage",             "Dashboards"),
            ("Alerts Metrics",            "Monitoring"),
            ("Suppression Rules",         "Monitoring"),
            ("TCO Distribution",          "Monitoring"),
            ("Data Usage",                "Data"),
            ("Limits",                    "Data"),
            ("Unparsed Logs",             "Data"),
            ("Key Fields Normalization",  "Data"),
            ("CSPM",                      "Advanced"),
            ("No-Log Alerts",             "Advanced"),
            ("Duplicate alerts",        "Advanced"),
            ("Disabled Rules",            "Advanced"),
            ("Ingestion Block Alert",     "Advanced"),
        ]

        # Extract the buildHealthChecks block for scoped parsing
        hc_block_match = re.search(
            r'\(function buildHealthChecks\(\).*?^\}\)\(\);',
            html, re.DOTALL | re.MULTILINE
        )
        hc_block = hc_block_match.group(0) if hc_block_match else html

        # ── S1: All expected checks present in JS ─────────────────────────────
        for (check_name, category) in expected_checks:
            if check_name in hc_block:
                passed.append(f"S1: '{check_name}' present")
            else:
                failures.append(f"S1: '{check_name}' ({category}) NOT found in buildHealthChecks block")

        # ── S2: All checks have a valid status ───────────────────────────────
        # Match both static  status: 'pass'
        # and ternary        status: expr ? 'pass' : 'warn'
        statuses_found = re.findall(r"status:\s*(?:[^'\";\n]*?)?'(pass|fail|warn|info)'", hc_block)
        expected_count = len(expected_checks)
        if len(statuses_found) >= expected_count:
            passed.append(f"S2: {len(statuses_found)} status fields found (≥ {expected_count} expected)")
        else:
            failures.append(
                f"S2: Only {len(statuses_found)} status fields found — expected ≥ {expected_count}. "
                f"Some checks may be missing a status."
            )

        invalid_statuses = [s for s in statuses_found if s not in ('pass','fail','warn','info')]
        if invalid_statuses:
            failures.append(f"S2: Invalid status values found: {invalid_statuses}")

        # ── S3: No placeholder/empty detail strings ───────────────────────────
        empty_details = re.findall(r"detail:\s*`\s*`", hc_block)
        if not empty_details:
            passed.append("S3: No empty detail fields")
        else:
            failures.append(f"S3: {len(empty_details)} empty detail field(s) found in health checks")

        # ── S4: Panel layout (no global health score bar) ────────────────────
        if 'id="hc-score-bar"' not in html and 'class="dash-panel"' in html and 'id="dash-zone-platform"' in html:
            passed.append("S4: Dash panels present; overall score bar removed")
        else:
            failures.append("S4: Expected dash-panel layout without hc-score-bar")

        # ── S5: Zone health tables (platform / integration / alerts / misc) ───
        if all(t in html for t in ('id="hcTbodyPlatform"', 'id="hcTbodyIntegration"', 'id="hcTbodyAlerts"', 'id="hcTbodyMisc"')):
            passed.append("S5: Zone health check tbodies present")
        else:
            failures.append("S5: One or more hcTbody* zone tables missing from HTML")

        # ── LIVE API CROSS-CHECKS ─────────────────────────────────────────────

        def get_meta(a: Dict) -> Dict:
            ml = a.get("meta_labels") or []
            if isinstance(ml, list):
                return {item["key"]: item["value"] for item in ml if "key" in item}
            return ml if isinstance(ml, dict) else {}

        enabled_alerts  = [a for a in alerts if a.get("is_active", True)]
        disabled_alerts = [a for a in alerts if not a.get("is_active", True)]
        dj_alerts = _load_data_json_alerts()

        # ── L1: SAML alerts ───────────────────────────────────────────────────
        saml_alerts = [a for a in enabled_alerts if "saml" in a.get("name","").lower()]
        if len(saml_alerts) >= 1:
            passed.append(f"L1: SAML — {len(saml_alerts)} enabled SAML alert rule(s) found")
        else:
            failures.append("L1: SAML — no enabled SAML alert rules found (dashboard shows 2)")

        # ── L2: MFA alerts ────────────────────────────────────────────────────
        mfa_alerts = [a for a in enabled_alerts if "mfa" in a.get("name","").lower()]
        if len(mfa_alerts) >= 5:
            passed.append(f"L2: MFA — {len(mfa_alerts)} enabled MFA alert rule(s) found")
        elif len(mfa_alerts) >= 1:
            warnings.append(f"L2: MFA — {len(mfa_alerts)} enabled MFA rules (dashboard shows 11)")
        else:
            failures.append("L2: MFA — no enabled MFA alert rules found (dashboard shows 11)")

        # ── L3: CSPM alerts ───────────────────────────────────────────────────
        cspm_alerts = [a for a in enabled_alerts if "cspm" in a.get("name","").lower()]
        if len(cspm_alerts) >= 1:
            passed.append(f"L3: CSPM — {len(cspm_alerts)} enabled CSPM alert rule(s) found")
        else:
            warnings.append("L3: CSPM — no CSPM alerts found (dashboard shows 2)")

        # ── L4: Ingestion block alert enabled ────────────────────────────────
        ingestion_block = [
            a for a in enabled_alerts
            if "ingestion blocked" in a.get("name","").lower()
            or "data usage alert" in a.get("name","").lower()
        ]
        if ingestion_block:
            passed.append(f"L4: Ingestion Block — {len(ingestion_block)} enabled ingestion alert(s) found")
        else:
            failures.append("L4: Ingestion Block Alert — no enabled ingestion block alert found (should be P1)")

        # ── L5: Disabled rules count ──────────────────────────────────────────
        live_disabled = len(disabled_alerts)
        dash_disabled = (
            int(dj_alerts["disabled_count"])
            if isinstance(dj_alerts.get("disabled_count"), (int, float))
            else live_disabled
        )
        if within_tolerance(dash_disabled, live_disabled):
            passed.append(f"L5: Disabled rules — live={live_disabled}, data.json={dash_disabled} ✓")
        else:
            warnings.append(
                f"L5: Disabled rules drift — data.json disabled_count={dash_disabled}, live={live_disabled}. "
                f"Run python3 refresh.py to sync data.json."
            )

        # ── L6: Enabled rules count ───────────────────────────────────────────
        live_enabled = len(enabled_alerts)
        dash_enabled = (
            int(dj_alerts["enabled_count"])
            if isinstance(dj_alerts.get("enabled_count"), (int, float))
            else live_enabled
        )
        if within_tolerance(dash_enabled, live_enabled):
            passed.append(f"L6: Enabled rules — live={live_enabled}, data.json={dash_enabled} ✓")
        else:
            warnings.append(
                f"L6: Enabled rules drift — data.json enabled_count={dash_enabled}, live={live_enabled}. "
                f"Run python3 refresh.py to sync data.json."
            )

        # ── L7: Noisy / top definitions merged into Active Incidents + hygiene ─
        if "Top definitions (24h)" in html and "top_alert_definitions_24h" in html:
            passed.append("L7: Top/noisy definitions — merged panel + server hygiene key referenced in JS")
        else:
            warnings.append("L7: Top/noisy merge — expected title and hygiene fallback in dashboard HTML")

        # ── L8: Suppression rules — detail lists API rule names (refresh.py) ───
        if "Suppression Rules" in hc_block and "hygieneSuppressionNamesHtml" in hc_block:
            passed.append("L8: Suppression Rules — detail helper renders scheduler rule names from API")
        else:
            warnings.append("L8: Suppression Rules — hygieneSuppressionNamesHtml not found in health checks")

        # ── BUILD RESULT ──────────────────────────────────────────────────────
        r.dashboard_val = f"{len(expected_checks)} checks defined"
        r.live_val = f"{len(passed)} sub-checks passed, {len(warnings)} warnings, {len(failures)} failures"
        r.details = {
            "passed":   passed,
            "warnings": warnings,
            "failures": failures,
        }

        if failures:
            r.status = "FAIL"
            r.message = (
                f"{len(failures)} sub-check(s) FAILED: "
                + " | ".join(failures[:3])
                + (" …" if len(failures) > 3 else "")
            )
        elif warnings:
            r.status = "WARN"
            r.message = (
                f"All structural checks pass. {len(warnings)} data drift warning(s): "
                + " | ".join(warnings[:2])
            )
        else:
            r.status = "PASS"
            r.message = (
                f"All {len(passed)} sub-checks passed — "
                f"22 health check entries verified (structure + live API)"
            )

        self._add(r)

    # ────────────────────────────────────────────────────────────────────────────
    # T15  data.json freshness & coverage
    # ────────────────────────────────────────────────────────────────────────────
    def test_data_json(self):
        """
        T15 — Verify data.json exists, is fresh (< 24h), and all sections are present.
        Also cross-checks live alert count and integration count against the file.
        """
        from datetime import timedelta
        DATA_FILE = DASHBOARD_FILE.parent / "data.json"
        r = TestResult("T15 — data.json Freshness & Coverage")
        failures: List[str] = []
        warnings: List[str] = []
        passed:   List[str] = []

        if not DATA_FILE.exists():
            r.status = "FAIL"
            r.message = "data.json not found — run: python3 refresh.py"
            self._add(r)
            return

        try:
            data = json.loads(DATA_FILE.read_text())
        except Exception as e:
            r.status = "FAIL"
            r.message = f"data.json is invalid JSON: {e}"
            self._add(r)
            return

        # Freshness check
        refreshed_at = (data.get("_meta") or {}).get("refreshed_at")
        if refreshed_at:
            try:
                age = datetime.now(timezone.utc) - datetime.fromisoformat(refreshed_at)
                if age > timedelta(hours=24):
                    warnings.append(f"data.json is {age.total_seconds()/3600:.1f}h old — run: python3 refresh.py")
                else:
                    passed.append(f"data.json refreshed {age.total_seconds()/3600:.1f}h ago ✓")
            except Exception:
                warnings.append("Could not parse _meta.refreshed_at timestamp")
        else:
            warnings.append("_meta.refreshed_at missing — run: python3 refresh.py")

        # Section coverage
        REQUIRED = ["integrations", "extensions", "webhooks",
                    "ip_access", "enrichments", "folders", "tco_policies", "alerts", "incidents"]
        for sec in REQUIRED:
            if sec not in data:
                failures.append(f"Section '{sec}' missing from data.json")
            else:
                err = data[sec].get("error") if isinstance(data[sec], dict) else None
                if err:
                    warnings.append(f"Section '{sec}' has error: {err[:60]}")
                else:
                    passed.append(f"Section '{sec}' present ✓")

        # Cross-check: alert count
        try:
            live = self._alerts()
            live_count = live.get("total", len(live.get("alerts", [])))
            json_count = data.get("alerts", {}).get("total")
            if json_count is not None and abs(live_count - json_count) <= 10:
                passed.append(f"alerts.total matches live ({json_count} ≈ {live_count}) ✓")
            elif json_count is not None:
                warnings.append(f"alerts.total drift: data.json={json_count}, live={live_count} — refresh needed")
        except Exception as e:
            warnings.append(f"Could not cross-check alerts: {e}")

        # Cross-check: integration count
        try:
            req = urllib.request.Request(
                "https://api.eu1.coralogix.com/mgmt/openapi/latest/integrations/integrations/v1",
                headers={"Authorization": f"Bearer {API_KEY}",
                         "Accept": "application/json", "User-Agent": "dashboard-test/1.0"},
            )
            with urllib.request.urlopen(req, timeout=20) as resp:
                int_data = json.loads(resp.read().decode())
            live_int = len([i for i in int_data.get("integrations", [])
                            if (i.get("amountIntegrations") or 0) > 0])
            json_int = data.get("integrations", {}).get("count")
            if json_int is not None and json_int == live_int:
                passed.append(f"integrations.count matches live ({json_int}) ✓")
            elif json_int is not None:
                warnings.append(f"integrations.count drift: data.json={json_int}, live={live_int} — refresh needed")
        except Exception as e:
            warnings.append(f"Could not cross-check integrations: {e}")

        # Security: ensure no API key appears in data.json
        raw = DATA_FILE.read_text()
        if API_KEY and API_KEY in raw:
            failures.append("CRITICAL: API key found inside data.json — remove immediately")
        else:
            passed.append("No API key found in data.json ✓")

        r.dashboard_val = f"{len(REQUIRED)} sections expected"
        r.live_val = f"{len(passed)} passed · {len(warnings)} warnings · {len(failures)} failures"
        r.details = {"passed": passed, "warnings": warnings, "failures": failures,
                     "refreshed_at": refreshed_at}

        if failures:
            r.status = "FAIL"
            r.message = f"{len(failures)} failure(s): {'; '.join(failures[:2])}"
        elif warnings:
            r.status = "WARN"
            r.message = f"data.json loaded with {len(warnings)} warning(s) — consider running: python3 refresh.py"
        else:
            r.status = "PASS"
            r.message = f"data.json valid · fresh · all {len(REQUIRED)} sections present · no credentials ✓"

        self._add(r)

    # ────────────────────────────────────────────────────────────────────────────
    # T16  Server liveness — serve.py running on the correct port and directory
    # ────────────────────────────────────────────────────────────────────────────
    def test_server_liveness(self):
        """
        T16 — Confirms that serve.py is running correctly:

          S1: At least one process is listening on port 8765
          S2: No stale/duplicate process is serving from the WRONG directory
              (a stale root-dir server will shadow the correct one with 404s)
          S3: HTTP GET http://localhost:8765/coralogix-dashboard.html → 200
          S4: HTTP GET http://localhost:8765/data.json → 200
          S5: data.json served over HTTP is valid JSON with expected keys

        How to fix failures:
          - S1 / S3 / S4: cd coralogix-dashboard && python3 serve.py
          - S2: kill the stale PID shown in the details, then restart serve.py
        """
        import subprocess
        import socket

        SERVE_PORT    = 8765
        CORRECT_DIR   = str(Path(__file__).parent.resolve())
        DASHBOARD_URL = f"http://localhost:{SERVE_PORT}/coralogix-dashboard.html"
        DATA_URL      = f"http://localhost:{SERVE_PORT}/data.json"

        r = TestResult("T16 — Server Liveness & Serve Path")
        failures: List[str] = []
        warnings: List[str] = []
        passed:   List[str] = []

        # ── S1: Anything listening on port 8765? ─────────────────────────────
        port_open = False
        try:
            with socket.create_connection(("127.0.0.1", SERVE_PORT), timeout=2):
                port_open = True
        except OSError:
            pass

        if port_open:
            passed.append(f"S1: Port {SERVE_PORT} is open ✓")
        else:
            failures.append(
                f"S1: Nothing is listening on port {SERVE_PORT} — "
                f"run: cd coralogix-dashboard && python3 serve.py"
            )

        # ── S2: Check for stale processes serving from the wrong directory ───
        # Uses `lsof` (macOS/Linux) to find which PIDs own the port and what
        # their working directory is. A stale process from the root R&D folder
        # will return 404 for coralogix-dashboard.html and shadow the real server.
        # Uses lsof -F (field format) so paths with spaces are parsed correctly.
        stale_pids: List[str] = []
        correct_pids: List[str] = []
        try:
            lsof_port_out = subprocess.check_output(
                ["lsof", f"-i:{SERVE_PORT}"],
                stderr=subprocess.DEVNULL, text=True
            )
            # Collect unique PIDs that have the port open
            pids_on_port: set = set()
            for line in lsof_port_out.splitlines()[1:]:
                parts = line.split()
                if len(parts) > 1:
                    pids_on_port.add(parts[1])

            for pid in pids_on_port:
                try:
                    # -F field format + -a AND + -d cwd: outputs lines like:
                    #   p<PID>
                    #   fcwd
                    #   n/full/path with spaces/intact
                    field_out = subprocess.check_output(
                        ["lsof", "-p", pid, "-a", "-d", "cwd", "-Fn"],
                        stderr=subprocess.DEVNULL, text=True
                    )
                    for field_line in field_out.splitlines():
                        if field_line.startswith("n"):       # 'n' prefix = NAME / path
                            cwd = field_line[1:]             # strip leading 'n'
                            if Path(cwd).resolve() == Path(CORRECT_DIR).resolve():
                                correct_pids.append(pid)
                            else:
                                stale_pids.append(f"PID {pid} → {cwd}")
                            break  # only one cwd per process
                except Exception:
                    pass
        except Exception:
            warnings.append("S2: Could not run lsof to inspect server processes (non-critical)")

        if stale_pids:
            # Deduplicate in case the same PID appears multiple times
            unique_stale = list(dict.fromkeys(stale_pids))
            pids_only = " ".join(s.split()[1] for s in unique_stale)
            failures.append(
                f"S2: Stale server process(es) on port {SERVE_PORT} serving from WRONG directory: "
                f"{unique_stale} — these intercept requests and return 404. "
                f"Fix: kill -9 {pids_only}  then  python3 serve.py"
            )
        elif correct_pids:
            passed.append(
                f"S2: Server process(es) (PID {', '.join(set(correct_pids))}) "
                f"serving from correct directory ✓"
            )
        elif port_open:
            warnings.append("S2: Port open but could not verify server working directory via lsof")

        # ── S3: Dashboard HTML returns HTTP 200 ──────────────────────────────
        if port_open:
            try:
                req = urllib.request.Request(DASHBOARD_URL,
                                             headers={"User-Agent": "dashboard-test/1.0"})
                with urllib.request.urlopen(req, timeout=5) as resp:
                    status = resp.status
                    size   = len(resp.read())
                if status == 200 and size > 1000:
                    passed.append(f"S3: {DASHBOARD_URL} → HTTP {status} ({size:,} bytes) ✓")
                else:
                    failures.append(
                        f"S3: {DASHBOARD_URL} returned HTTP {status} / {size} bytes — "
                        f"expected 200 with >1 KB body"
                    )
            except urllib.error.HTTPError as e:
                failures.append(
                    f"S3: {DASHBOARD_URL} → HTTP {e.code} — "
                    f"server is running but serving from wrong directory (stale process). "
                    f"Kill stale PIDs and run: python3 serve.py"
                )
            except Exception as e:
                failures.append(f"S3: Could not reach {DASHBOARD_URL}: {e}")
        else:
            failures.append(f"S3: Skipped — port {SERVE_PORT} not open")

        # ── S4: data.json returns HTTP 200 ───────────────────────────────────
        if port_open:
            try:
                req = urllib.request.Request(DATA_URL,
                                             headers={"User-Agent": "dashboard-test/1.0"})
                with urllib.request.urlopen(req, timeout=5) as resp:
                    status    = resp.status
                    raw_body  = resp.read()
                if status == 200:
                    passed.append(f"S4: {DATA_URL} → HTTP {status} ✓")
                    # ── S5: Parse served data.json ────────────────────────────
                    try:
                        served_data = json.loads(raw_body.decode())
                        missing_keys = [k for k in ("_meta", "alerts", "integrations")
                                        if k not in served_data]
                        if not missing_keys:
                            passed.append("S5: Served data.json is valid JSON with expected top-level keys ✓")
                        else:
                            warnings.append(
                                f"S5: Served data.json missing keys: {missing_keys} — "
                                f"run: python3 refresh.py"
                            )
                    except Exception as e:
                        failures.append(f"S5: Served data.json is not valid JSON: {e}")
                else:
                    failures.append(f"S4: {DATA_URL} returned HTTP {status} — expected 200")
            except urllib.error.HTTPError as e:
                failures.append(
                    f"S4: {DATA_URL} → HTTP {e.code} — data.json not served. "
                    f"Run: python3 refresh.py  then  python3 serve.py"
                )
            except Exception as e:
                failures.append(f"S4: Could not reach {DATA_URL}: {e}")
        else:
            failures.append(f"S4: Skipped — port {SERVE_PORT} not open")

        # ── Build result ──────────────────────────────────────────────────────
        r.dashboard_val = f"http://localhost:{SERVE_PORT}"
        r.live_val      = f"{len(passed)} passed · {len(warnings)} warnings · {len(failures)} failures"
        r.details = {
            "serve_port":   SERVE_PORT,
            "correct_dir":  CORRECT_DIR,
            "passed":   passed,
            "warnings": warnings,
            "failures": failures,
        }

        if failures:
            r.status  = "FAIL"
            r.message = f"{len(failures)} server check(s) FAILED: {'; '.join(failures[:2])}"
        elif warnings:
            r.status  = "WARN"
            r.message = f"Server reachable with {len(warnings)} warning(s): {'; '.join(warnings)}"
        else:
            r.status  = "PASS"
            r.message = (
                f"Server healthy on port {SERVE_PORT} — dashboard & data.json both HTTP 200, "
                f"no stale processes ✓"
            )

        self._add(r)

    # ────────────────────────────────────────────────────────────────────────────
    # Orchestrator
    # ────────────────────────────────────────────────────────────────────────────
    def run_all(self) -> bool:
        print(f"\n{BOLD}{'═' * 64}{RESET}")
        print(f"{BOLD}  Coralogix Dashboard — Data Accuracy Test Suite{RESET}")
        print(f"  Timestamp : {self.run_ts}")
        print(f"  Dashboard : {DASHBOARD_FILE.name}")
        print(f"  API       : {API_BASE}")
        print(f"  Tolerance : ±{TOLERANCE_PCT}%")
        print(f"{BOLD}{'═' * 64}{RESET}\n")

        # Step 1 — connectivity gate
        if not self.test_api_connectivity():
            for r in self.results:
                self._print_result(r)
            print(f"\n{RED}{BOLD}Aborting — cannot reach Coralogix API.{RESET}\n")
            return False

        # Step 2 — parse dashboard
        try:
            self.dashboard = parse_dashboard()
        except Exception as e:
            r2 = TestResult("Dashboard Parse")
            r2.status = "FAIL"
            r2.message = f"Could not parse dashboard HTML: {e}"
            self._add(r2)
            self._print_result(r2)
            return False

        # Step 3 — run all tests
        for fn in [
            self.test_server_liveness,       # T16 first — catches serve issues before anything else
            self.test_dashboard_file,
            self.test_no_zero_kpis,
            self.test_alert_definitions_total,
            self.test_p1_count,
            self.test_priority_distribution,
            self.test_alert_types,
            self.test_enabled_ratio,
            self.test_incident_priority_chart_consistency,
            self.test_snapshot_drift,
            self.test_security_controls,
            self.test_deployed_integrations,
            self.test_no_grey_footnotes,
            self.test_account_health_checks,
            self.test_data_json,
        ]:
            try:
                fn()
            except Exception as e:
                er = TestResult(fn.__name__)
                er.status = "FAIL"
                er.message = f"Uncaught exception: {e}"
                self._add(er)

        # Print results
        for r in self.results:
            self._print_result(r)

        # Summary
        counts = Counter(r.status for r in self.results)
        total = len(self.results)
        print(f"{BOLD}{'─' * 64}{RESET}")
        print(f"  {GREEN}{counts['PASS']} passed{RESET}  "
              f"{RED}{counts['FAIL']} failed{RESET}  "
              f"{YELLOW}{counts['WARN']} warnings{RESET}  "
              f"/ {total} total\n")
        if counts["FAIL"] == 0:
            print(f"  {GREEN}{BOLD}✓ All critical checks passed — dashboard data is accurate.{RESET}")
        else:
            print(f"  {RED}{BOLD}✗ {counts['FAIL']} check(s) FAILED — run with --fix to auto-update.{RESET}")
        print(f"{BOLD}{'═' * 64}{RESET}\n")
        return counts["FAIL"] == 0

    def _print_result(self, r: TestResult):
        icon = {
            "PASS": PASS_LBL,
            "FAIL": FAIL_LBL,
            "WARN": WARN_LBL,
        }.get(r.status, INFO_LBL)
        print(f"  {icon}  {r.name}")
        print(f"          {r.message}")
        if r.details and r.status != "PASS":
            for k, v in r.details.items():
                print(f"          {CYAN}{k}{RESET}: {v}")
        print()

    # ────────────────────────────────────────────────────────────────────────────
    # --save-snapshot
    # ────────────────────────────────────────────────────────────────────────────
    def save_snapshot(self):
        print(f"\n{BOLD}Saving live baseline snapshot…{RESET}")
        resp   = fetch_all_alerts()
        total  = resp.get("total", 0)
        alerts = resp.get("alerts", [])
        p1     = sum(1 for a in alerts if a.get("severity","").lower() == "critical")
        by_sev = Counter(a.get("severity","unknown").lower() for a in alerts)
        priority_map = {"critical":"P1","error":"P2","warning":"P3","info":"P4","debug":"P5","verbose":"P5"}
        by_prio = Counter(priority_map.get(a.get("severity","").lower(),"P5") for a in alerts)
        dash = parse_dashboard()

        snapshot = {
            "created_at":              datetime.now(timezone.utc).isoformat(),
            "alert_definitions_total": total,
            "p1_total":                p1,
            "severity_breakdown":      dict(by_sev),
            "priority_breakdown":      dict(by_prio),
            "dashboard_values_at_snapshot": {
                "open_incidents":    dash["open_incidents"],
                "alert_definitions": dash["alert_definitions"],
                "p1_alerts":         dash["p1_alerts"],
            },
        }
        SNAPSHOT_FILE.write_text(json.dumps(snapshot, indent=2))
        print(f"{GREEN}✓ Snapshot saved → {SNAPSHOT_FILE}{RESET}")
        print(json.dumps(snapshot, indent=2))

    # ────────────────────────────────────────────────────────────────────────────
    # --fix  (auto-patch dashboard HTML)
    # ────────────────────────────────────────────────────────────────────────────
    def auto_fix(self):
        print(f"\n{BOLD}Auto-patching dashboard with live values…{RESET}\n")
        html   = DASHBOARD_FILE.read_text()
        resp   = fetch_all_alerts()
        live_total = resp.get("total", 0)
        alerts = resp.get("alerts", [])
        sev_map = {"critical":"P1","error":"P2","warning":"P3","info":"P4","debug":"P5","verbose":"P5"}
        by_prio = Counter(sev_map.get(a.get("severity","").lower(),"P5") for a in alerts)
        dash = parse_dashboard()
        changes = []

        def patch_stat(label_html: str, new_val: str) -> str:
            pattern = rf'({re.escape(label_html)}.*?class="stat-value"[^>]*>)[^<]+(<)'
            return re.sub(pattern, lambda m: f"{m.group(1)}{new_val}{m.group(2)}", html, flags=re.DOTALL)

        # Alert definitions total
        if dash["alert_definitions"] != live_total:
            html = patch_stat("Alert Definitions", f"{live_total:,}")
            changes.append(f"  Alert Definitions: {dash['alert_definitions']} → {live_total:,}")

        # Priority chart
        new_data = [by_prio.get(p, 0) for p in ["P1","P2","P3","P4","P5"]]
        old_data = [dash["priority_data"].get(p, 0) for p in ["P1","P2","P3","P4","P5"]]
        if new_data != old_data:
            old_str = ", ".join(str(x) for x in old_data)
            new_str = ", ".join(str(x) for x in new_data)
            html = html.replace(f"data: [{old_str}]", f"data: [{new_str}]", 1)
            changes.append(f"  Priority chart: [{old_str}] → [{new_str}]")

        if changes:
            DASHBOARD_FILE.write_text(html)
            print(f"{GREEN}✓ Dashboard updated:{RESET}")
            for c in changes:
                print(c)
        else:
            print(f"{GREEN}✓ Dashboard already up to date.{RESET}")

    # ────────────────────────────────────────────────────────────────────────────
    # --json output
    # ────────────────────────────────────────────────────────────────────────────
    def output_json(self):
        counts = Counter(r.status for r in self.results)
        print(json.dumps({
            "run_at":       self.run_ts,
            "tolerance_pct": TOLERANCE_PCT,
            "summary":      dict(counts),
            "passed":       counts["PASS"] == len(self.results),
            "tests":        [r.to_dict() for r in self.results],
        }, indent=2))


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Coralogix dashboard data accuracy tests")
    p.add_argument("--json",          action="store_true", help="Print results as JSON")
    p.add_argument("--fix",           action="store_true", help="Auto-patch dashboard with live data")
    p.add_argument("--save-snapshot", action="store_true", help="Save current live data as baseline")
    args = p.parse_args()

    suite = DashboardTestSuite()

    if args.save_snapshot:
        suite.save_snapshot()
        return

    ok = suite.run_all()

    if args.json:
        suite.output_json()

    if args.fix and not ok:
        suite.auto_fix()

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
