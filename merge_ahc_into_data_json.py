#!/usr/bin/env python3
"""
Merge Snowbit AHC automator output into the dashboard data.json.

Looks for the newest file matching:
  sb-ahc-automator-main/output/AHC_*_output.json

Writes/updates data.json key:
  "ahc": { checks[], summary, chart, meta, ... }

Run after:
  python3 refresh.py
  python3 sb-ahc-automator-main/ahc_runner.py --region EU1 --company-id ... --cx-api-key ... --session-token ...

Or only merge when you already have a fresh AHC JSON:
  python3 merge_ahc_into_data_json.py

``python3 refresh.py`` also calls ``apply_ahc_to_results`` before writing ``data.json``,
so one refresh updates API sections and merges the latest AHC file when present.
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from snapshot_atomic_write import atomic_write_text

ROOT = Path(__file__).resolve().parent
AHC_OUTPUT_DIR = ROOT / "sb-ahc-automator-main" / "output"
DATA_JSON = ROOT / "data.json"

# Map AHC merged JSON top-level keys → (display title, dashboard category)
CHECK_REGISTRY: Dict[str, Tuple[str, str]] = {
    "webhook": ("Webhooks", "Configuration"),
    "send_log_webhook": ("Send-log webhook", "Configuration"),
    "archive_bucket": ("Archive buckets", "Configuration"),
    "extensions": ("Extensions", "Configuration"),
    "enrichments": ("Enrichments", "Configuration"),
    "team_default_homepage": ("Team homepage", "Dashboards"),
    "default_dashboard": ("Default dashboard", "Dashboards"),
    "dashboard_folders": ("Dashboard folders", "Dashboards"),
    "team_auditing": ("Team auditing", "Monitoring"),
    "cora_ai": ("Cora AI", "Monitoring"),
    "cx_alerts_metrics": ("CX alerts metrics", "Monitoring"),
    "suppression_rules": ("Suppression rules", "Monitoring"),
    "tco_distribution": ("TCO distribution", "Monitoring"),
    "saml": ("SAML", "Security"),
    "mfa": ("MFA enforcement", "Security"),
    "ip_access": ("IP access control", "Security"),
    "data_usage_metrics": ("Data usage metrics", "Data"),
    "data_usage": ("Data usage", "Data"),
    "limits": ("Limits / quotas", "Data"),
    "data_normalization": ("Data normalization", "Data"),
    "cspm": ("CSPM", "Advanced"),
    "alert_history": ("Alert history", "Advanced"),
    "alerts_status": ("Alerts status", "Advanced"),
    "noisy_alerts": ("Noisy alerts", "Advanced"),
    "mcp_checks": ("MCP / DataPrime checks", "Advanced"),
    "unparsed_logs": ("Unparsed logs", "Data"),
    "no_log_alerts": ("No-log alerts", "Advanced"),
    "disabled_alert_rules": ("Disabled alert rules", "Advanced"),
    "ingestion_block_alert": ("Ingestion block alert", "Advanced"),
}

SKIP_KEYS = frozenset(
    {
        "check_time",
        "company_id",
        "check_elapsed_seconds",
    }
)

# Not surfaced in dashboard ahc.checks (noisy / redundant with account context)
OMIT_FROM_CHECKS = frozenset({"team_url"})

CAT_IDS = {
    "Security": "ahc-security",
    "Configuration": "ahc-config",
    "Dashboards": "ahc-dashboards",
    "Monitoring": "ahc-monitoring",
    "Data": "ahc-data",
    "Advanced": "ahc-advanced",
}


def _truncate(s: str, n: int = 1200) -> str:
    s = s.strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def _json_snippet(obj: Any, n: int = 900) -> str:
    try:
        raw = json.dumps(obj, indent=2, default=str)
    except TypeError:
        raw = str(obj)
    return _truncate(raw, n)


def _find_latest_ahc_json() -> Optional[Path]:
    if not AHC_OUTPUT_DIR.is_dir():
        return None
    candidates = list(AHC_OUTPUT_DIR.glob("AHC_*_output.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _title_for_key(key: str) -> Tuple[str, str]:
    if key in CHECK_REGISTRY:
        return CHECK_REGISTRY[key]
    if key.endswith("_error"):
        base = key[: -len("_error")].replace("_", " ").title()
        return (f"{base} (error)", "Other")
    return (key.replace("_", " ").title(), "Other")


def _scalar_kv_rows(val: dict, *, max_rows: int = 16, max_v: int = 220) -> List[List[str]]:
    """Flatten dict to label/value pairs for dashboard tables (no nested JSON)."""
    rows: List[List[str]] = []
    skip = frozenset({"error", "errors", "raw", "payload", "debug"})
    for k, v in val.items():
        if k in skip or v is None:
            continue
        if isinstance(v, (dict, list)):
            continue
        label = k.replace("_", " ").strip().title() or k
        s = str(v).strip()
        if not s:
            continue
        rows.append([label, s[:max_v] + ("…" if len(s) > max_v else "")])
        if len(rows) >= max_rows:
            break
    return rows


def build_display(key: str, val: Any) -> Dict[str, Any]:
    """
    Human-oriented fields for the dashboard (no raw JSON blobs).
    """
    rows: List[List[str]] = []
    bullets: List[str] = []
    summary = ""

    if key.endswith("_error") or key == "auth_error":
        if isinstance(val, dict):
            summary = str(val.get("error", val.get("message", "Check failed")))
        else:
            summary = str(val)
        return {"summary": _truncate(summary, 400), "rows": [], "bullets": []}

    if isinstance(val, list):
        rows = [[str(i + 1), str(x)[:200] + ("…" if len(str(x)) > 200 else "")] for i, x in enumerate(val[:12])]
        return {
            "summary": _truncate(f"{len(val)} item(s) returned", 400),
            "rows": rows,
            "bullets": [],
        }

    if not isinstance(val, dict):
        return {"summary": _truncate(str(val), 400), "rows": [], "bullets": []}

    if val.get("status") == "FAILED":
        return {
            "summary": _truncate(str(val.get("error", "FAILED")), 400),
            "rows": [],
            "bullets": [],
        }

    if key == "mfa":
        en = val.get("enforced")
        summary = (
            "Company MFA enforcement is ON"
            if en is True
            else ("MFA not enforced at company level" if en is False else "Could not read MFA flag")
        )
        rows = _scalar_kv_rows(val)

    elif key == "ip_access":
        en = val.get("enabled")
        summary = (
            "IP allowlist is enabled"
            if en is True
            else ("IP allowlist disabled — any IP can reach UI" if en is False else "IP access state unknown")
        )
        rows = _scalar_kv_rows(val)

    elif key == "saml":
        rows = _scalar_kv_rows(val)
        summary = next(
            (r[1] for r in rows if "active" in r[0].lower() or "configured" in r[0].lower()),
            "SAML settings captured from platform",
        )

    elif key == "team_url":
        rows = _scalar_kv_rows(val)
        summary = val.get("team_url") or val.get("url") or (rows[0][1] if rows else "Team URL recorded")

    elif key in ("webhook", "send_log_webhook", "archive_bucket", "extensions", "enrichments"):
        rows = _scalar_kv_rows(val)
        summary = val.get("summary") or val.get("message") or (
            f"{len(rows)} data point(s) from {key.replace('_', ' ')} check" if rows else f"{key.replace('_', ' ').title()} check complete"
        )

    elif key in ("default_dashboard", "dashboard_folders", "team_default_homepage"):
        rows = _scalar_kv_rows(val)
        summary = val.get("summary") or (rows[0][1] if rows else f"{key.replace('_', ' ')} OK")

    elif key in ("team_auditing", "cora_ai", "cx_alerts_metrics", "suppression_rules", "tco_distribution"):
        rows = _scalar_kv_rows(val)
        summary = val.get("summary") or val.get("message") or (
            rows[0][1] if rows else f"{key.replace('_', ' ')} metrics captured"
        )

    elif key in ("data_usage", "data_usage_metrics", "limits"):
        rows = _scalar_kv_rows(val)
        summary = val.get("summary") or (rows[0][1] if rows else "Usage / limits snapshot")

    elif key == "data_normalization":
        rows = _scalar_kv_rows(val)
        summary = val.get("summary") or val.get("message") or "Normalization / field coverage"

    elif key == "cspm":
        rows = _scalar_kv_rows(val)
        summary = val.get("summary") or val.get("message") or "CSPM posture"

    elif key == "alert_history":
        rows = _scalar_kv_rows(val)
        parts = []
        if val.get("total") is not None:
            parts.append(f"History window: {val.get('total')} events")
        if val.get("p1") is not None:
            parts.append(f"P1: {val.get('p1')}")
        summary = " · ".join(parts) if parts else (rows[0][1] if rows else "Alert history sampled")

    elif key == "alerts_status":
        rows = _scalar_kv_rows(val)
        dc = val.get("disabled_count", val.get("disabled"))
        nt = val.get("never_triggered", val.get("never_triggered_count"))
        summary = f"Disabled definitions: {dc if dc is not None else '—'} · Never fired (30d): {nt if nt is not None else '—'}"

    elif key == "noisy_alerts":
        na = val.get("noisy_alerts") or val.get("alerts") or []
        tot = val.get("total_triggers") or val.get("totalTriggers")
        if isinstance(na, list) and na:
            summary = f"Top noisy rules: {len(na)} listed"
            if tot is not None:
                summary += f" · ~{tot} triggers"
            for item in na[:10]:
                if isinstance(item, dict):
                    nm = item.get("name") or item.get("alert_name") or "Rule"
                    tc = item.get("trigger_count") or item.get("triggers") or item.get("count")
                    bullets.append(f"{nm}" + (f" — {tc} triggers" if tc is not None else ""))
                else:
                    bullets.append(str(item)[:200])
        else:
            rows = _scalar_kv_rows(val)
            summary = val.get("summary") or "Noisy alert scan complete"

    elif key in ("unparsed_logs", "no_log_alerts", "ingestion_block_alert"):
        rows = _scalar_kv_rows(val)
        summary = (
            val.get("summary")
            or val.get("message")
            or (rows[0][1] if rows else key.replace("_", " ").title())
        )

    elif key in ("mcp_checks", "mcps"):
        rows = _scalar_kv_rows(val)
        summary = "MCP / DataPrime checks (unparsed, no-log, ingestion)"

    else:
        rows = _scalar_kv_rows(val)
        for sk in ("summary", "message", "status", "result", "team_url", "url", "name"):
            if val.get(sk) is not None and not isinstance(val.get(sk), (dict, list)):
                summary = str(val.get(sk))
                break
        if not summary:
            summary = f"{len(rows)} structured field(s)" if rows else "Check completed"

    return {
        "summary": _truncate(summary.strip() or "—", 500),
        "rows": rows[:16],
        "bullets": [_truncate(b, 300) for b in bullets[:12]],
    }


def _infer_status_detail(key: str, val: Any) -> Tuple[str, str]:
    """Return (pass|warn|fail|info, plain-text detail)."""
    if key.endswith("_error") or key == "auth_error":
        if isinstance(val, dict):
            msg = val.get("error") or val.get("message") or _json_snippet(val, 400)
            return "fail", _truncate(str(msg), 1500)
        return "fail", _truncate(str(val), 1500)

    if val is None:
        return "info", "No data"

    if isinstance(val, list):
        return "info", _json_snippet(val, 800)

    if not isinstance(val, dict):
        return "info", _truncate(str(val), 1200)

    if val.get("status") == "FAILED":
        return "fail", _truncate(str(val.get("error", "FAILED")), 1500)

    err_only = val.get("error")
    if err_only and len(val) <= 3 and not any(
        k in val for k in ("enforced", "enabled", "configured", "pass", "ok", "result")
    ):
        return "fail", _truncate(str(err_only), 1500)

    if key == "mfa" or key.endswith("_mfa"):
        en = val.get("enforced")
        if en is True:
            return "pass", "MFA enforced"
        if en is False:
            return "warn", "MFA not enforced"
        if err_only:
            return "fail", _truncate(str(err_only), 800)
        return "info", _json_snippet(val)

    if key == "ip_access" or "ip_allow" in key:
        en = val.get("enabled")
        if en is True:
            return "pass", "IP allowlist enabled"
        if en is False:
            return "warn", "IP allowlist disabled"
        if err_only:
            return "fail", _truncate(str(err_only), 800)
        return "info", _json_snippet(val)

    # Heuristic: explicit pass/fail flags
    for pk in ("pass", "passed", "ok", "success"):
        if val.get(pk) is True:
            return "pass", _json_snippet(val, 700)
    for fk in ("fail", "failed", "violation"):
        if val.get(fk) is True:
            return "fail", _json_snippet(val, 700)

    if "enabled" in val and isinstance(val["enabled"], bool):
        return ("pass" if val["enabled"] else "warn", _json_snippet(val, 700))

    if "configured" in val and isinstance(val["configured"], bool):
        return ("pass" if val["configured"] else "warn", _json_snippet(val, 700))

    # Count-style positives
    if isinstance(val.get("count"), (int, float)) and val["count"] > 0:
        return "pass", _json_snippet(val, 700)

    if err_only:
        return "warn", _truncate(str(err_only), 800)

    return "pass", _json_snippet(val, 900)


def _normalize(ahc_raw: Dict[str, Any], source_file: str) -> Dict[str, Any]:
    elapsed = ahc_raw.get("check_elapsed_seconds") or {}
    if not isinstance(elapsed, dict):
        elapsed = {}

    checks: List[Dict[str, Any]] = []
    summary = {"pass": 0, "warn": 0, "fail": 0, "info": 0}

    for key, val in sorted(ahc_raw.items(), key=lambda x: x[0]):
        if key in SKIP_KEYS:
            continue
        if key in OMIT_FROM_CHECKS:
            continue
        title, category = _title_for_key(key)
        st, _legacy_detail = _infer_status_detail(key, val)
        disp = build_display(key, val)
        if category == "Other":
            cat = "Advanced"
        else:
            cat = category

        sec = None
        if isinstance(elapsed, dict):
            base = key.replace("_error", "")
            sec = elapsed.get(base)
            if sec is None and base.endswith("_check"):
                sec = elapsed.get(base.replace("_check", ""))

        checks.append(
            {
                "id": key,
                "title": title,
                "category": cat,
                "status": st,
                "detail": disp["summary"],
                "display": disp,
                "elapsed_sec": round(float(sec), 2) if isinstance(sec, (int, float)) else None,
            }
        )
        if st in summary:
            summary[st] += 1

    total = sum(summary.values())
    score = 0
    if total > 0:
        score = round(((summary["pass"] + summary["info"] * 0.5) / total) * 100)

    by_cat: Dict[str, Dict[str, int]] = {}
    for c in CAT_IDS:
        by_cat[c] = {"pass": 0, "warn": 0, "fail": 0, "info": 0}
    for ch in checks:
        cat = ch["category"]
        if cat not in by_cat:
            cat = "Advanced"
            ch["category"] = "Advanced"
        st = ch["status"]
        if st in by_cat[cat]:
            by_cat[cat][st] += 1

    cat_labels = list(CAT_IDS.keys())
    cat_scores: List[float] = []
    for c in cat_labels:
        b = by_cat.get(c, {})
        t = b["pass"] + b["warn"] + b["fail"] + b["info"]
        if t == 0:
            cat_scores.append(0.0)
        else:
            cat_scores.append(round(((b["pass"] + b["info"] * 0.5) / t) * 100, 1))

    return {
        "present": True,
        "source_file": source_file,
        "merged_at": datetime.now(timezone.utc).isoformat(),
        "ahc_run": {
            "check_time": ahc_raw.get("check_time"),
            "company_id": ahc_raw.get("company_id"),
        },
        "summary": {**summary, "total": total, "score": score},
        "checks": checks,
        "chart": {
            "status_labels": ["Pass", "Warn", "Fail", "Info"],
            "status_counts": [
                summary["pass"],
                summary["warn"],
                summary["fail"],
                summary["info"],
            ],
            "category_labels": cat_labels,
            "category_scores": cat_scores,
        },
    }


def apply_ahc_to_results(
    results: Dict[str, Any],
    *,
    ahc_path: Optional[Path] = None,
    verbose: bool = True,
) -> bool:
    """
    Merge Snowbit AHC automator JSON into a dashboard data dict (mutates results).

    - If ``ahc_path`` is set and is a file: merge that file.
    - Else: use the newest ``AHC_*_output.json`` under ``sb-ahc-automator-main/output/``.
    - If no file exists: do **not** change ``results["ahc"]`` (keeps prior merge from an
      earlier refresh). This differs from :func:`run` when invoked standalone with no file.

    Returns True if a file was merged, False otherwise.
    """
    if ahc_path is not None:
        path = ahc_path if ahc_path.is_file() else None
        if path is None and verbose:
            print(f"  ⚠  ahc: file not found: {ahc_path}")
            return False
    else:
        path = _find_latest_ahc_json()

    if not path:
        if verbose:
            print(
                "  ℹ  ahc: no AHC_*_output.json under sb-ahc-automator-main/output/ — "
                "keeping existing ahc block (run ahc_runner.py, then refresh again)"
            )
        return False

    ahc_raw = json.loads(path.read_text())
    block = _normalize(ahc_raw, path.name)
    results["ahc"] = block
    if verbose:
        s = block["summary"]
        print(
            f"  ✓  ahc: merged {path.name} ({len(block['checks'])} checks · "
            f"pass {s['pass']}/warn {s['warn']}/fail {s['fail']}/info {s['info']})"
        )
    return True


def run(ahc_path: Optional[Path] = None) -> int:
    path = ahc_path or _find_latest_ahc_json()
    if not path or not path.is_file():
        print(f"No AHC output found under {AHC_OUTPUT_DIR}")
        print("Run: python3 sb-ahc-automator-main/ahc_runner.py --region EU1 --company-id ID \\")
        print("      --cx-api-key KEY --session-token TOKEN")
        if DATA_JSON.exists():
            try:
                data = json.loads(DATA_JSON.read_text())
            except json.JSONDecodeError:
                data = {}
            data["ahc"] = {
                "present": False,
                "merged_at": datetime.now(timezone.utc).isoformat(),
                "message": "No AHC_*_output.json in sb-ahc-automator-main/output/",
            }
            atomic_write_text(DATA_JSON, json.dumps(data, indent=2, default=str))
            print(f"Updated {DATA_JSON.name} with ahc.present=false")
        return 1

    data: Dict[str, Any] = {}
    if DATA_JSON.exists():
        try:
            data = json.loads(DATA_JSON.read_text())
        except json.JSONDecodeError:
            print("WARNING: existing data.json invalid JSON — overwriting non-ahc keys may be lost", file=sys.stderr)

    apply_ahc_to_results(data, ahc_path=path, verbose=True)
    atomic_write_text(DATA_JSON, json.dumps(data, indent=2, default=str))
    kb = DATA_JSON.stat().st_size / 1024
    block = data["ahc"]
    print(f"Merged {path.name} → {DATA_JSON.name} ({kb:.1f} KB)")
    print(f"  AHC checks: {len(block['checks'])}  score: {block['summary']['score']}%  pass/warn/fail/info: "
          f"{block['summary']['pass']}/{block['summary']['warn']}/{block['summary']['fail']}/{block['summary']['info']}")
    return 0


if __name__ == "__main__":
    sys.exit(run())
