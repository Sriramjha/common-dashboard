"""
Slack Report — Coralogix Account Health Check
Sends a rich Block Kit message directly to Slack (no PDF, no third-party hosts).
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime

import requests

from modules.region_config import get_report_time_ist


def _display_time(check_time: str) -> str:
    """Format check_time for display with IST timezone."""
    t = (check_time or "").strip()
    if not t:
        return f"{get_report_time_ist()} IST"
    return f"{t} IST" if not t.upper().endswith(" IST") else t


# ── Emoji helpers ──────────────────────────────────────────────────────────────

def _bool(val) -> tuple[str, str]:
    """Returns (emoji, label) for a boolean-ish value. Only Yes / No — no N/A."""
    if val is True or str(val).lower() in ("true", "yes", "enabled",
                                            "configured", "used", "1"):
        return ":green-check-mark:", "Yes"
    # Everything else (False, None, unknown) → No
    return ":x:", "No"


def _pct_bar(pct: float, width: int = 10) -> str:
    """Renders a text progress bar: ████░░░░░░ 67%"""
    filled = round(pct / 100 * width)
    return "█" * filled + "░" * (width - filled) + f"  {pct:.1f}%"


def _usage_emoji(used, limit) -> str:
    try:
        used  = float(used or 0)
        limit = float(limit or 0)
    except (TypeError, ValueError):
        return "➖"
    if not limit:
        return "➖"
    pct = used / limit * 100
    if pct >= 90:
        return "🔴"
    if pct >= 70:
        return "🟡"
    return "🟢"


# ── Block builders ─────────────────────────────────────────────────────────────

def _divider() -> dict:
    return {"type": "divider"}


def _header(text: str) -> dict:
    return {"type": "header",
            "text": {"type": "plain_text", "text": text, "emoji": True}}


def _section(text: str) -> dict:
    return {"type": "section",
            "text": {"type": "mrkdwn", "text": text}}


def _fields(*items: str) -> dict:
    return {"type": "section",
            "fields": [{"type": "mrkdwn", "text": t} for t in items]}


def _context(text: str) -> dict:
    return {"type": "context",
            "elements": [{"type": "mrkdwn", "text": text}]}


# ── Section formatters ─────────────────────────────────────────────────────────

def _cover(data: dict, team_name: str) -> list[dict]:
    check_time = _display_time(data.get("check_time") or get_report_time_ist())
    company_id = data.get("company_id", data.get("client_id", "N/A"))
    team_url   = data.get("team_url", "N/A")

    return [
        _header(f"📋  Account Health Check — {team_name}"),
        _fields(
            f"*Team:*\n{team_name}",
            f"*Generated:*\n{check_time}",
            f"*Company ID:*\n{company_id}",
            f"*Team URL:*\n{team_url}",
        ),
        _divider(),
    ]


# MCP sub-check keys that can have an "error" field when they fail (CSPM is standalone, not MCP)
_MCP_CHECK_KEYS = ("unparsed_logs", "no_log_alerts", "ingestion_block_alert")


def _count_mcp_sub_check_errors(data: dict) -> int:
    """Count MCP sub-checks that returned an error (e.g. CSPM timeout)."""
    return sum(1 for k in _MCP_CHECK_KEYS if (data.get(k) or {}).get("error"))


def _build_message_text(data: dict, team_name: str, check_time: str, pdf_attached: bool = True) -> str:
    """Build plain text message for Slack file upload initial_comment or standalone post."""
    lines = []
    
    # Count failed checks (top-level _error + CSPM error + MCP sub-check errors)
    failed_count = sum(1 for k, v in data.items()
                      if k.endswith("_error") and isinstance(v, dict) and v.get("status") == "FAILED")
    if (data.get("cspm") or {}).get("error"):
        failed_count += 1  # CSPM standalone check failed
    if (data.get("alerts_status") or {}).get("error"):
        failed_count += 1  # Alerts Status (incidents API failed)
    mcp_errors = _count_mcp_sub_check_errors(data)
    if mcp_errors > 0:
        failed_count += 1  # Treat mcp_checks as failed when any sub-check has error
    
    # Status summary (checks + PDF)
    check_status = f"{failed_count} check(s) failed" if failed_count else "All checks passed"
    pdf_status = "Attached" if pdf_attached else "Failed"
    lines.append(f":gear: *Status:* {check_status} | PDF: {pdf_status}")
    lines.append("")
    
    # Header
    company_id = data.get("company_id", data.get("client_id", "N/A"))
    team_url = data.get("team_url", "N/A")
    lines.append(f"📋 *Account Health Check — {team_name.upper()}*")
    lines.append(f"Team: {team_name} | Company ID: {company_id}")
    lines.append(f"Generated: {_display_time(check_time)}")
    lines.append(f"Team URL: {team_url}")
    lines.append("─" * 40)
    
    # Concerns section
    concerns = _get_concerns_list(data)
    if concerns:
        lines.append("")
        lines.append("🚨 *Concerns — Action Required*")
        lines.append("")
        for c in concerns:
            lines.append(c)
    else:
        lines.append("")
        lines.append("✅ *No concerns — all checks passed!*")
    
    lines.append("─" * 40)
    lines.append("")
    if pdf_attached:
        lines.append("📎 *The detailed Account Health Check report is attached.*")
    else:
        lines.append("⚠️ *PDF generation failed* — summary above. Full JSON available in S3 / Lambda logs.")
    lines.append("")
    # Total checks summary
    total_checks = 26  # Default checks count (includes standalone CSPM)
    passed_count = total_checks - failed_count
    if failed_count == 0:
        lines.append(f"✅ *{total_checks}/{total_checks} checks passed*")
    else:
        lines.append(f"✅ {passed_count} passed  |  ❌ {failed_count} failed")
    lines.append("")
    lines.append(f"_Generated by Snowbit AHC Automation | {team_name}_")
    
    return "\n".join(lines)


def _get_concerns_list(data: dict) -> list[str]:
    """Get sorted list of concerns (same logic as _concerns_section but returns list)."""
    concerns: list[str] = []

    # CSPM check error (standalone, not MCP)
    cspm = data.get("cspm", {})
    if isinstance(cspm, dict) and cspm.get("error"):
        label = "CSPM"
        err = str(cspm.get("error", ""))
        err_short = err[:80] + "..." if len(err) > 80 else err
        concerns.append(f"🚫 *{label}* — FAILED: `{err_short}`")

    # MCP sub-check errors
    _mcp_labels = {
        "unparsed_logs": "Unparsed Logs",
        "no_log_alerts": "No Log Alerts",
        "ingestion_block_alert": "Ingestion Block Alert",
    }
    if (data.get("data_normalization") or {}).get("error"):
        concerns.append(f"🚫 *Data Normalisation Status* — FAILED: `{str(data['data_normalization'].get('error', ''))[:80]}`")
    if (data.get("alerts_status") or {}).get("error"):
        concerns.append(f"🚫 *Alerts Status* — FAILED: `{str(data['alerts_status'].get('error', ''))[:80]}`")
    for key in _MCP_CHECK_KEYS:
        val = data.get(key)
        if isinstance(val, dict) and val.get("error"):
            label = _mcp_labels.get(key, key.replace("_", " ").title())
            err = str(val.get("error", ""))
            err_short = err[:80] + "..." if len(err) > 80 else err
            concerns.append(f"🚫 *{label}* — FAILED: `{err_short}`")

    # Failed checks
    for key, value in data.items():
        if key.endswith("_error") and isinstance(value, dict) and value.get("status") == "FAILED":
            check_name = key.replace("_error", "").replace("_", " ").title()
            error_msg = value.get("error", "Unknown error")
            status_code = value.get("status_code")
            if status_code:
                concerns.append(f"🚫 *{check_name}* — FAILED (HTTP {status_code}): `{error_msg[:80]}...`" if len(error_msg) > 80 else f"🚫 *{check_name}* — FAILED (HTTP {status_code}): `{error_msg}`")
            else:
                concerns.append(f"🚫 *{check_name}* — FAILED: `{error_msg[:80]}...`" if len(error_msg) > 80 else f"🚫 *{check_name}* — FAILED: `{error_msg}`")

    # Boolean checks (skip archive bucket "not configured" when archive_bucket_error — already shown as FAILED)
    ab_error = data.get("archive_bucket_error", {})
    for label, val in _summary_checks(data):
        if _bool(val)[0] == ":x:":
            if ab_error.get("status") == "FAILED" and label in ("S3 Log archive bucket", "S3 Metrics archive bucket"):
                continue
            concerns.append(f"⚠️ *{label}* — not configured / not active")

    # Data usage >= 90%
    du = data.get("data_usage", {})
    if du:
        try:
            quota = float(du.get("daily_quota", 0) or 0)
            avg = float(du.get("avg_daily_units", 0) or 0)
            if quota > 0 and avg > 0:
                pct = avg / quota * 100
                if pct >= 90:
                    concerns.append(f"🔴 *Data Usage* — {pct:.1f}% of daily quota")
        except (TypeError, ValueError):
            pass

    # Limits >= 80%
    lim = data.get("limits", {})
    _limit_labels = {
        "ingested_fields_today": "Ingested Fields limit",
        "alerts": "Alert limit",
        "enrichments": "Enrichments limit",
        "parsing_rules": "Parsing Rules limit",
    }
    for key, label in _limit_labels.items():
        entry = lim.get(key, {})
        try:
            used = float(entry.get("used", 0) or 0)
            limit = float(entry.get("limit", 0) or 0)
        except (TypeError, ValueError):
            continue
        if used and limit and limit > 0:
            pct = used / limit * 100
            if pct >= 90:
                concerns.append(f"🔴 *{label}* — `{int(used)}` / `{int(limit)}` ({pct:.1f}% used)")
            elif pct >= 80:
                concerns.append(f"🟡 *{label}* — `{int(used)}` / `{int(limit)}` ({pct:.1f}% used)")

    # TCO
    tco = data.get("tco_distribution", {})
    if tco:
        low_pct = tco.get("low_pct", 0) or 0
        block_pct = tco.get("block_pct", 0) or 0
        if low_pct > 0:
            concerns.append(f"🟡 *TCO Low Priority* — {low_pct:.1f}% of logs")
        if block_pct > 0:
            concerns.append(f"🔴 *TCO Blocked* — {block_pct:.1f}% of logs")

    # Security extensions
    sec = data.get("security_extensions", {})
    for ext_id, deployed in sec.items():
        if not deployed:
            concerns.append(f"❌ *Security Extension not deployed:* `{ext_id}`")

    # No-log alerts
    nla = data.get("no_log_alerts", {})
    if nla:
        triggered = nla.get("triggered_7d", [])
        if triggered:
            preview = ", ".join(f"`{n}`" for n in triggered[:3])
            extra = len(triggered) - 3
            if extra > 0:
                preview += f" +{extra} more"
            concerns.append(f"🔴 *{len(triggered)} 'No Log' alert(s) triggered in last 7 days* — {preview}")
        uncovered = nla.get("apps_without_coverage", [])
        if uncovered:
            preview = ", ".join(f"`{a}`" for a in uncovered[:5])
            extra = len(uncovered) - 5
            if extra > 0:
                preview += f" +{extra} more"
            concerns.append(f"⚠️ *{len(uncovered)} active app(s) have no 'No Log' alert* — {preview}")

    # Unparsed logs
    ul = data.get("unparsed_logs", {})
    if ul and not ul.get("all_parsed", True) and ul.get("total_unparsed", 0) > 0:
        total_unparsed = ul.get("total_unparsed", 0)
        total_logs = ul.get("total_logs", 0)
        pct = (total_unparsed / total_logs * 100) if total_logs else 0
        apps_preview = ", ".join(f"`{a['application']}`" for a in ul.get("apps", [])[:3])
        extra = len(ul.get("apps", [])) - 3
        if extra > 0:
            apps_preview += f" +{extra} more"
        concerns.append(f"❌ *{total_unparsed} unparsed log entries ({pct:.1f}%)* — {apps_preview}")

    # Dashboards not in folder
    db = data.get("dashboards", {})
    root_count = db.get("not_in_folder", 0)
    if root_count > 0:
        concerns.append(f"❌ *{root_count} dashboard(s) not in any folder*")

    # Geo enrichment
    enr = data.get("enrichments", {})
    if enr:
        if not enr.get("geo_cx_security_source_ip"):
            concerns.append("❌ *Geo Enrichment missing:* `cx_security.source_ip`")
        if not enr.get("geo_cx_security_destination_ip"):
            concerns.append("❌ *Geo Enrichment missing:* `cx_security.destination_ip`")

    # Ingestion block alert
    iba = data.get("ingestion_block_alert", {})
    if iba:
        if not iba.get("alert_exists"):
            concerns.append("❌ *Ingestion Block Alert* — NOT FOUND")
        elif not iba.get("alert_active"):
            concerns.append("🟡 *Ingestion Block Alert* — exists but DISABLED")

    # Data normalisation status (cx_security, last 24h)
    dn = data.get("data_normalization", {})
    if dn and dn.get("concern_count", 0) > 0:
        concerns.append("⚠️ *Few app(s) with missing cx_security.* Check Data normalisation status section in attached report.")

    # Sort: critical first, warnings last
    def concern_priority(c: str) -> int:
        if any(c.startswith(e) for e in ["🔴", "❌", "🚫"]):
            return 0
        elif any(c.startswith(e) for e in ["⚠️", "🟡"]):
            return 2
        return 1

    return sorted(concerns, key=concern_priority)


def _summary_checks(data: dict) -> list[tuple[str, object]]:
    """
    Single source of truth for all boolean checks shown in summary & concerns.
    Only returns checks that are actually present in the data (enabled checks).
    """
    all_checks = [
        ("S3 Log archive bucket",       "archive_buckets" in data, data.get("archive_buckets", {}).get("logs", {}).get("active")),
        ("S3 Metrics archive bucket",    "archive_buckets" in data, data.get("archive_buckets", {}).get("metrics", {}).get("active")),
        ("SAML",               "saml" in data, data.get("saml", {}).get("configured")),
        ("MFA Configured",     "mfa" in data, data.get("mfa", {}).get("enforced")),
        ("IP Access Control",  "ip_access" in data, data.get("ip_access", {}).get("enabled")),
        ("Team Auditing",      "team_auditing" in data, data.get("team_auditing", {}).get("configured")),
        ("Cora AI",            "cora_ai" in data, data.get("cora_ai", {}).get("dataprime_query_assistance_enabled")),
        ("CX Alerts Metrics",  "cx_alerts_metrics" in data, data.get("cx_alerts_metrics", {}).get("enabled")),
        ("Data Usage Metrics", "data_usage_metrics" in data, data.get("data_usage_metrics") == "enabled"),
        ("Suppression Rules",  "suppression_rules" in data, data.get("suppression_rules") == "used"),
        ("Send Log Webhook",   "send_log_webhook_created" in data, data.get("send_log_webhook_created")),
        # MCP checks
        ("CSPM Integrated",    "cspm" in data, data.get("cspm", {}).get("integrated")),
    ]
    # Only return checks that are enabled (present in data)
    return [(label, val) for label, enabled, val in all_checks if enabled]


def _concerns_section(data: dict) -> list[dict]:
    """
    Collects three categories of concerns and surfaces them right after the cover:
      1. Failed checks (errors during execution)
      2. Boolean checks that are :x: (not configured / not active)
      3. Limits that are >= 80% utilised
      4. Security extensions that are not deployed
    """
    concerns: list[str] = []

    # ── 0. Failed checks (execution errors) ────────────────────────────────────
    for key, value in data.items():
        if key.endswith("_error") and isinstance(value, dict) and value.get("status") == "FAILED":
            check_name = key.replace("_error", "").replace("_", " ").title()
            error_msg = value.get("error", "Unknown error")
            status_code = value.get("status_code")
            if status_code:
                concerns.append(f"🚫  *{check_name}* — FAILED (HTTP {status_code}): `{error_msg[:80]}...`" if len(error_msg) > 80 else f"🚫  *{check_name}* — FAILED (HTTP {status_code}): `{error_msg}`")
            else:
                concerns.append(f"🚫  *{check_name}* — FAILED: `{error_msg[:80]}...`" if len(error_msg) > 80 else f"🚫  *{check_name}* — FAILED: `{error_msg}`")

    # ── 1. Boolean checks ────────────────────────────────────────────────────
    ab_error = data.get("archive_bucket_error", {})
    for label, val in _summary_checks(data):
        if label == "CSPM Integrated":
            continue  # CSPM not configured is not a concern
        if _bool(val)[0] == ":x:":
            if ab_error.get("status") == "FAILED" and label in ("S3 Log archive bucket", "S3 Metrics archive bucket"):
                continue  # Already shown as Archive Bucket — FAILED
            concerns.append(f":warning:  *{label}* — not configured / not active")

    # ── 2. Data usage >= 90% ──────────────────────────────────────────────────
    du = data.get("data_usage", {})
    if du:
        try:
            quota = float(du.get("daily_quota", 0) or 0)
            avg   = float(du.get("avg_daily_units", 0) or 0)
            if quota > 0 and avg > 0:
                pct = avg / quota * 100
                if pct >= 90:
                    concerns.append(
                        f"🔴  *Data Usage* — `{int(avg)}` / `{int(quota)}` units ({pct:.1f}% of daily quota)"
                    )
        except (TypeError, ValueError):
            pass

    # ── 3. Limits nearing capacity (>= 80%) ──────────────────────────────────
    lim = data.get("limits", {})
    _limit_labels = {
        "ingested_fields_today": "Ingested Fields limit",
        "alerts":                "Alert limit",
        "enrichments":           "Enrichments limit",
        "parsing_rules":         "Parsing Rules limit",
    }
    for key, label in _limit_labels.items():
        entry = lim.get(key, {})
        try:
            used  = float(entry.get("used", 0) or 0)
            limit = float(entry.get("limit", 0) or 0)
        except (TypeError, ValueError):
            continue
        if used and limit and limit > 0:
            pct = used / limit * 100
            if pct >= 90:
                concerns.append(
                    f"🔴  *{label}* — `{int(used)}` / `{int(limit)}`  ({pct:.1f}% used)"
                )
            elif pct >= 80:
                concerns.append(
                    f"🟡  *{label}* — `{int(used)}` / `{int(limit)}`  ({pct:.1f}% used)"
                )

    # ── 4. TCO Low/Block > 0% ───────────────────────────────────────────────
    tco = data.get("tco_distribution", {})
    if tco:
        low_pct   = tco.get("low_pct", 0) or 0
        block_pct = tco.get("block_pct", 0) or 0
        if low_pct > 0:
            concerns.append(f"🟡  *TCO Low Priority* — {low_pct:.1f}% of logs (consider moving to Medium/High)")
        if block_pct > 0:
            concerns.append(f"🔴  *TCO Blocked* — {block_pct:.1f}% of logs are being blocked")

    # ── 5. Missing security extensions ───────────────────────────────────────
    sec = data.get("security_extensions", {})
    for ext_id, deployed in sec.items():
        if not deployed:
            concerns.append(f":x:  *Security Extension not deployed:* `{ext_id}`")

    # ── 6. No-log alerts triggered ───────────────────────────────────────────
    nla = data.get("no_log_alerts", {})
    if nla:
        triggered = nla.get("triggered_7d", [])
        if triggered:
            preview = ", ".join(f"`{n}`" for n in triggered[:3])
            extra = len(triggered) - 3
            if extra > 0:
                preview += f" _+{extra} more_"
            concerns.append(
                f"🔴  *{len(triggered)} 'No Log' alert(s) triggered in last 7 days* — {preview}"
            )
        uncovered = nla.get("apps_without_coverage", [])
        if uncovered:
            preview = ", ".join(f"`{a}`" for a in uncovered[:5])
            extra = len(uncovered) - 5
            if extra > 0:
                preview += f" _+{extra} more_"
            concerns.append(
                f"⚠️  *{len(uncovered)} active app(s) have no 'No Log' alert* — {preview}"
            )

    # ── 7. Unparsed logs ─────────────────────────────────────────────────────
    ul = data.get("unparsed_logs", {})
    if ul and not ul.get("all_parsed", True) and ul.get("total_unparsed", 0) > 0:
        total_unparsed = ul.get("total_unparsed", 0)
        total_logs     = ul.get("total_logs", 0)
        pct = (total_unparsed / total_logs * 100) if total_logs else 0
        apps_preview = ", ".join(
            f"`{a['application']}`" for a in ul.get("apps", [])[:3]
        )
        extra = len(ul.get("apps", [])) - 3
        if extra > 0:
            apps_preview += f" _+{extra} more_"
        concerns.append(
            f":x:  *{total_unparsed} unparsed log entries ({pct:.1f}%)* in last 24 h "
            f"({ul.get('affected_apps', 0)} apps) — {apps_preview}"
        )

    # ── 8. Dashboards sitting at root (no folder) ────────────────────────────
    db = data.get("dashboards", {})
    root_count = db.get("not_in_folder", 0)
    if root_count > 0:
        concerns.append(
            f":x:  *{root_count} dashboard(s) not in any folder*"
        )

    # ── 9. Missing geo enrichment fields ─────────────────────────────────────
    enr = data.get("enrichments", {})
    if enr:
        if not enr.get("geo_cx_security_source_ip"):
            concerns.append(":x:  *Geo Enrichment missing:* `cx_security.source_ip` not enriched")
        if not enr.get("geo_cx_security_destination_ip"):
            concerns.append(":x:  *Geo Enrichment missing:* `cx_security.destination_ip` not enriched")

    # ── 10. Ingestion block alert missing or disabled ──────────────────────────
    iba = data.get("ingestion_block_alert", {})
    if iba:
        if not iba.get("alert_exists"):
            concerns.append(":x:  *Ingestion Block Alert* — NOT FOUND (ACTION REQUIRED)")
        elif not iba.get("alert_active"):
            concerns.append("🟡  *Ingestion Block Alert* — exists but is DISABLED")

    # ── 11. Data normalisation status (cx_security, last 24h) ────────────────
    dn = data.get("data_normalization", {})
    if dn and dn.get("concern_count", 0) > 0:
        concerns.append(
            ":warning:  *Few app(s) with missing cx_security.* Check Data normalisation status section in attached report."
        )

    # ── Render ────────────────────────────────────────────────────────────────
    if not concerns:
        return [
            _section(":green-check-mark:  *No concerns — all checks passed!*"),
            _divider(),
        ]

    # Sort concerns: critical/errors first, warnings last
    # Critical: 🔴, :red_circle:, :x:, 🚫
    # Warning: ⚠️, :warning:, 🟡
    def concern_priority(c: str) -> int:
        # Critical/errors - should appear first
        if any(c.startswith(e) for e in ["🔴", ":red_circle:", ":x:", "🚫"]):
            return 0
        # Warnings - should appear last
        elif any(c.startswith(e) for e in ["⚠️", ":warning:", "🟡"]):
            return 2
        return 1  # Others in middle
    
    concerns_sorted = sorted(concerns, key=concern_priority)

    return [
        _header("🚨  Concerns — Action Required"),
        _section("\n" + "\n".join(concerns_sorted)),
        _divider(),
    ]


def _quick_summary(data: dict) -> list[dict]:
    lines = []
    for label, val in _summary_checks(data):
        emoji, text = _bool(val)
        lines.append(f"{emoji}  *{label}:*  {text}")

    return [
        _header("🗂  Quick Summary"),
        _section("\n".join(lines)),
        _divider(),
    ]


def _tco_section(data: dict) -> list[dict]:
    tco = data.get("tco_distribution", {})
    if not tco:
        return []

    high    = tco.get("high_pct", 0)
    medium  = tco.get("medium_pct", 0)
    low     = tco.get("low_pct", 0)
    blocked = tco.get("blocked_pct", 0)

    lines = [
        f"🔵  *High*      {_pct_bar(high)}",
        f"🟣  *Medium*  {_pct_bar(medium)}",
        f"🟠  *Low*        {_pct_bar(low)}",
        f"🔴  *Blocked* {_pct_bar(blocked)}",
    ]
    return [
        _header("📊  TCO Priority Distribution"),
        _section("\n".join(lines)),
        _divider(),
    ]


def _data_usage_section(data: dict) -> list[dict]:
    du = data.get("data_usage", {})
    if not du:
        return []

    quota = du.get("daily_quota") or 0
    avg   = du.get("avg_daily_units") or 0
    try:
        pct = min(float(avg) / float(quota) * 100, 100) if quota else 0
    except Exception:
        pct = 0

    emoji = _usage_emoji(avg, quota)
    lines = [
        f"{emoji}  Daily quota usage:  {_pct_bar(pct)}",
        f"• *Daily Quota:*  `{quota}` units",
        f"• *Avg Daily Usage (yesterday):*  `{avg}` units",
    ]
    return [
        _header("📈  Data Usage"),
        _section("\n".join(lines)),
        _divider(),
    ]


def _limits_section(data: dict) -> list[dict]:
    lim = data.get("limits", {})
    if not lim:
        return []

    def _row(label: str, used, limit) -> str:
        try:
            u = float(used or 0)
            l = float(limit or 0)
        except (TypeError, ValueError):
            u, l = 0, 0
        emoji = _usage_emoji(u, l or 1)
        return f"{emoji}  *{label}:*  `{used}` / `{limit}`"

    fields_d = lim.get("ingested_fields_today", {})
    alerts_d = lim.get("alerts", {})
    enrich_d = lim.get("enrichments", {})
    parse_d  = lim.get("parsing_rules", {})

    lines = [
        _row("Ingested Fields limit",
             fields_d.get("used"), fields_d.get("limit")),
        _row("Alert limit",
             alerts_d.get("used"), alerts_d.get("limit")),
        _row("Enrichments limit",
             enrich_d.get("used"), enrich_d.get("limit")),
        _row("Parsing Rules limit",
             parse_d.get("used"), parse_d.get("limit")),
        f"➖  *Mapping Exceptions:*  `{lim.get('mapping_exceptions', 'N/A')}`",
        f"➖  *Events2Metrics Labels Limit:*  `{lim.get('events2metrics_labels_limit', 'N/A')}`",
    ]
    return [
        _header("⚠️  Account Limits"),
        _section("\n".join(lines)),
        _divider(),
    ]


def _extensions_section(data: dict) -> list[dict]:
    ext = data.get("extensions", {})
    if not ext:
        return []

    total   = ext.get("amount", 0)
    updated = ext.get("updated", [])
    needs   = ext.get("update_available", [])

    lines = [
        f"📦  *Total deployed:*  `{total}`",
        f":green-check-mark:  *Up-to-date:*  `{len(updated)}`",
        f"⬆️   *Update available:*  `{len(needs)}`",
    ]
    if needs:
        names = "  •  ".join(needs)
        lines.append(f"\n_Needs update:_  {names}")

    blocks = [
        _header("🧩  Extensions"),
        _section("\n".join(lines)),
    ]

    sec = data.get("security_extensions", {})
    if sec:
        field_items = []
        for k, v in sec.items():
            emoji, label = _bool(v)
            field_items.append(f"{emoji}  *{k}:*  {label}")
        blocks.append(_section("*🔐  Security Extensions*\n" + "\n".join(field_items)))

    blocks.append(_divider())
    return blocks


def _webhooks_section(data: dict) -> list[dict]:
    wb      = data.get("outbound_webhooks", {})
    details = wb.get("details", [])
    if not wb:
        return []

    lines = [f"`{wb.get('amount', 0)}` webhooks configured\n"]
    for d in details:
        lines.append(f"• *{d.get('label', '?')}:*  `{d.get('connections_count', 0)}` connection(s)")

    emoji, label = _bool(data.get("send_log_webhook_created"))
    lines.append(f"\n{emoji}  *Send Log Webhook created:*  {label}")

    return [
        _header("🔗  Outbound Webhooks"),
        _section("\n".join(lines)),
        _divider(),
    ]


def _archive_section(data: dict) -> list[dict]:
    ab = data.get("archive_buckets", {})
    if not ab:
        return []

    logs    = ab.get("logs", {})
    metrics = ab.get("metrics", {})

    le, _ = _bool(logs.get("active"))
    me, _ = _bool(metrics.get("active") if metrics else None)

    lines = [
        f"{le}  *Logs:*  `{logs.get('bucket', 'N/A')}`  |  region: `{logs.get('region', 'N/A')}`  |  format: `{logs.get('format', 'N/A')}`",
        f"{me}  *Metrics:*  `{metrics.get('bucket', 'N/A')}`  |  region: `{metrics.get('region', 'N/A')}`",
    ]
    return [
        _header("🗄️  Archive Buckets"),
        _section("\n".join(lines)),
        _divider(),
    ]


def _security_section(data: dict) -> list[dict]:
    saml = data.get("saml", {})
    mfa  = data.get("mfa", {})
    ip   = data.get("ip_access", {})

    # Skip section if none of the security checks are enabled
    if not saml and not mfa and not ip:
        return []

    def _row(label, val):
        e, l = _bool(val)
        return f"{e}  *{label}:*  {l}"

    lines = []
    if saml:
        lines.append(_row("SAML Configured",   saml.get("configured")))
        lines.append(_row("SAML Activated",    saml.get("activated")))
    if mfa:
        lines.append(_row("MFA Configured",    mfa.get("enforced")))
    if ip:
        lines.append(_row("IP Access Control", ip.get("enabled")))

    if not lines:
        return []

    return [
        _header("🔒  Security & Access"),
        _section("\n".join(lines)),
        _divider(),
    ]


def _config_section(data: dict) -> list[dict]:
    cora_ai  = data.get("cora_ai", {})
    auditing = data.get("team_auditing", {})
    enr      = data.get("enrichments", {})

    # Skip section if none of the config checks are enabled
    has_any = (
        "default_dashboard" in data or
        "team_default_homepage" in data or
        cora_ai or auditing or
        "cx_alerts_metrics" in data or
        "data_usage_metrics" in data or
        "suppression_rules" in data or
        enr
    )
    if not has_any:
        return []

    lines = []

    if "default_dashboard" in data:
        lines.append(f"• *Default Dashboard:*  `{data.get('default_dashboard', 'N/A')}`")
    if "team_default_homepage" in data:
        lines.append(f"• *Default Homepage:*  `{data.get('team_default_homepage', {}).get('value', 'N/A')}`")

    if auditing:
        te, tl = _bool(auditing.get("configured"))
        audit_name = auditing.get("audit_team_name")
        if auditing.get("configured") and audit_name:
            lines.append(f"• *Team Auditing:*  {te}  {tl}   _(Audit team: {audit_name})_")
        else:
            lines.append(f"• *Team Auditing:*  {te}  {tl}")

    if cora_ai:
        ae, al = _bool(cora_ai.get("dataprime_query_assistance_enabled"))
        ee, el = _bool(cora_ai.get("explain_log_enabled"))
        ke, kl = _bool(cora_ai.get("knowledge_assistance_enabled"))
        lines.append("")
        lines.append(f"• *Cora AI:*")
        lines.append(f"    ↳  {ae}  *Dataprime Query Assistance:*  {al}")
        lines.append(f"    ↳  {ee}  *Explain Log:*  {el}")
        lines.append(f"    ↳  {ke}  *Knowledge Assistance:*  {kl}")

    if "cx_alerts_metrics" in data:
        e, l = _bool(data.get("cx_alerts_metrics", {}).get("enabled"))
        lines.append(f"• *CX Alerts Metrics:*  {e}  {l}")
    if "data_usage_metrics" in data:
        e, l = _bool(data.get("data_usage_metrics") == "enabled")
        lines.append(f"• *Data Usage Metrics:*  {e}  {l}")
    if "suppression_rules" in data:
        e, l = _bool(data.get("suppression_rules") == "used")
        lines.append(f"• *Suppression Rules:*  {e}  {l}")

    if enr:
        # List counts for list-type enrichment categories (geo, security, …)
        enr_parts = [f"`{k}` ({len(v)})" for k, v in enr.items() if isinstance(v, list) and v]
        if enr_parts:
            lines.append("• *Enrichments:*  " + "  •  ".join(enr_parts))
        # Geo IP field checks
        src_e, src_l = _bool(enr.get("geo_cx_security_source_ip"))
        dst_e, dst_l = _bool(enr.get("geo_cx_security_destination_ip"))
        lines.append(
            f"• *Geo Enrichment — cx_security.source_ip:*  {src_e}  {src_l}"
        )
        lines.append(
            f"• *Geo Enrichment — cx_security.destination_ip:*  {dst_e}  {dst_l}"
        )

    if not lines:
        return []

    return [
        _header("⚙️  Configuration & Features"),
        _section("\n".join(lines)),
    ]


def _dashboards_section(data: dict) -> list[dict]:
    db = data.get("dashboards", {})
    if not db:
        return []

    total         = db.get("total", 0)
    in_folder     = db.get("in_folder", 0)
    not_in_folder = db.get("not_in_folder", 0)
    root_names    = db.get("not_in_folder_names", [])

    in_mark  = ":green-check-mark:" if in_folder == total else "📁"
    out_mark = ":x:" if not_in_folder > 0 else ":green-check-mark:"

    lines = [
        f"📊  *Total dashboards:*  `{total}`",
        f"{in_mark}  *In a folder:*  `{in_folder}`",
        f"{out_mark}  *At root (no folder):*  `{not_in_folder}`",
    ]
    if root_names:
        names_str = "  •  ".join(f"`{n}`" for n in root_names[:15])
        if len(root_names) > 15:
            names_str += f"  _… and {len(root_names) - 15} more_"
        lines.append(f"\n_Root dashboards:_  {names_str}")

    return [
        _header("📂  Dashboard Folders"),
        _section("\n".join(lines)),
        _divider(),
    ]


def _no_log_alerts_section(data: dict) -> list[dict]:
    nla = data.get("no_log_alerts", {})
    if not nla:
        return []

    total         = nla.get("total", 0)
    triggered_7d  = nla.get("triggered_7d", [])
    not_triggered = nla.get("not_triggered", [])
    disabled      = nla.get("disabled", [])

    lines = [
        f"🔔  *Total 'No Log' alerts configured:*  `{total}`",
        f"🔴  *Triggered in last 7 days:*  `{len(triggered_7d)}`",
        f"🟡  *Never triggered:*  `{len(not_triggered)}`",
        f"⚫  *Disabled:*  `{len(disabled)}`",
    ]

    if triggered_7d:
        lines.append("")
        lines.append("*Triggered recently (last 7 days):*")
        for name in triggered_7d:
            lines.append(f"  🔴  `{name}`")

    # Apps with no no-log alert coverage
    uncovered = nla.get("apps_without_coverage", [])
    if uncovered:
        lines.append("")
        lines.append("*Apps with no 'No Log' alert coverage:*")
        for app in uncovered:
            lines.append(f"  ⚠️  `{app}`")

    return [
        _header("🚨  No-Log Alerts  [via MCP]"),
        _section("\n".join(lines)),
        _divider(),
    ]


def _unparsed_logs_section(data: dict) -> list[dict]:
    ul = data.get("unparsed_logs", {})
    if not ul:
        return []

    all_parsed     = ul.get("all_parsed", True)
    total_unparsed = ul.get("total_unparsed", 0)
    total_logs     = ul.get("total_logs", 0)
    affected       = ul.get("affected_apps", 0)
    apps           = ul.get("apps", [])

    if all_parsed or total_unparsed == 0:
        lines = [":green-check-mark:  *All logs parsed as valid JSON* — no unparsed logs found in last 24 h"]
    else:
        overall_pct = (total_unparsed / total_logs * 100) if total_logs else 0
        lines = [
            f":x:  *Unparsed logs found* (last 24 h) — `{overall_pct:.1f}%` unparsed",
            f"📋  *Total unparsed log entries:*  `{total_unparsed}` / `{total_logs}`",
            f"📦  *Affected applications:*  `{affected}`",
            "",
        ]
        for app in apps:
            app_total = app.get("total_count", 0)
            app_unparsed = app.get("count", 0)
            app_pct = (app_unparsed / app_total * 100) if app_total else 0
            lines.append(f"• `{app['application']}` — {app_unparsed} unparsed logs out of {app_total} ({app_pct:.1f}%)")

    return [
        _header("🔍  Log Parsing  [via MCP]"),
        _section("\n".join(lines)),
        _divider(),
    ]


def _cspm_section(data: dict) -> list[dict]:
    cspm = data.get("cspm", {})
    if not cspm:
        return []

    if cspm.get("error"):
        return [
            _header("🛡️  CSPM  [via DataPrime API]"),
            _section(f"🚫  *Failed:*  `{cspm.get('error', 'Unknown error')[:100]}`"),
            _divider(),
        ]

    integrated     = cspm.get("integrated", False)
    total_accounts = cspm.get("total_accounts", 0)
    providers      = cspm.get("providers", [])

    if integrated:
        lines = [f":green-check-mark:  *Configured:*  Yes"]
        lines.append(f"☁️   *Cloud Accounts detected:*  `{total_accounts}`")
        
        # Show providers breakdown with account IDs
        if providers:
            lines.append("")
            for p in providers:
                provider_name = p.get("provider", "Unknown")
                provider_count = p.get("count", 0)
                provider_accounts = p.get("accounts", [])
                
                # Provider emoji
                if provider_name == "AWS":
                    emoji = "🟠"
                elif provider_name == "Azure":
                    emoji = "🔵"
                elif provider_name == "GCP":
                    emoji = "🔴"
                else:
                    emoji = "⚪"
                
                acct_preview = ", ".join(f"`{a}`" for a in provider_accounts[:5])
                if len(provider_accounts) > 5:
                    acct_preview += f" _+{len(provider_accounts) - 5} more_"
                
                lines.append(f"{emoji}  *{provider_name}:*  `{provider_count}` account(s) — {acct_preview}")
    else:
        lines = [f":white_circle:  *Configured:*  No (CSPM not configured — no concern)"]

    return [
        _header("🛡️  CSPM  [via DataPrime API]"),
        _section("\n".join(lines)),
        _divider(),
    ]


def _ingestion_block_alert_section(data: dict) -> list[dict]:
    iba = data.get("ingestion_block_alert", {})
    if not iba:
        return []

    alert_exists = iba.get("alert_exists", False)
    alert_active = iba.get("alert_active", False)
    alerts       = iba.get("alerts", [])

    # If at least one is active, just show that one with green tick
    if alert_exists and alert_active:
        # Find the first active alert
        active_alert = next((a for a in alerts if a.get("enabled")), None)
        if active_alert:
            lines = [
                f":green-check-mark:  *Configured & Active*",
                f"• `{active_alert.get('name', 'Unknown')}`  _(Priority: {active_alert.get('priority', 'N/A')}, Last triggered: {active_alert.get('last_triggered', 'N/A')})_",
            ]
        else:
            lines = [":green-check-mark:  *Configured & Active*"]
    elif alert_exists and not alert_active:
        # All alerts are disabled - show warning
        lines = [
            "🟡  *Alert exists but is DISABLED*",
            "",
        ]
        for alert in alerts:
            lines.append(f"• `{alert.get('name', 'Unknown')}` — :x: Disabled")
    else:
        # No alert found
        lines = [":x:  *No ingestion block alert found — ACTION REQUIRED*"]

    return [
        _header("🚨  Ingestion Block Alert  [via MCP]"),
        _section("\n".join(lines)),
        _divider(),
    ]


def _data_normalization_section(data: dict) -> list[dict]:
    """Render Data Normalisation Status section (via DataPrime API, last 24h)."""
    dn = data.get("data_normalization", {})
    if not dn:
        return []

    all_normalized = dn.get("all_normalized", False)
    concern_count = dn.get("concern_count", 0)
    concern_rows = dn.get("concern_rows", [])

    if all_normalized or concern_count == 0:
        status_emoji = ":green-check-mark:"
        status_text = "All data sources have cx_security (last 24h)"
    else:
        status_emoji = ":x:"
        status_text = "Few app(s) with missing cx_security — check Data normalisation status section below"

    lines = [
        f"{status_emoji}  *{status_text}*",
    ]

    if concern_rows:
        lines.append("")
        lines.append("*Data normalisation status (last 24h) — missing cx_security:*")
        for row in concern_rows:
            app = row.get("application", "?")
            sub = row.get("subsystem", "?")
            lines.append(f"  ❌  `{app}` / `{sub}`")

    return [
        _header("🔑  Data Normalisation Status  [via DataPrime API, last 24h]"),
        _section("\n".join(lines)),
        _divider(),
    ]


def _mcp_prompt_section(data: dict) -> list[dict]:
    """
    Render any prompt-based MCP check results that contain an 'answer' key.
    These are free-text LLM answers and are shown as-is in a dedicated section.
    """
    blocks = []
    for key, val in data.items():
        if not isinstance(val, dict):
            continue
        answer = val.get("answer")
        error  = val.get("error")
        if answer is None and error is None:
            continue
        # Only render keys that look like MCP prompt results (have answer/error but
        # not the structured cspm/rows shape)
        if "rows" in val or "integrated" in val or "found" in val:
            continue

        label = key.replace("_", " ").title()
        if answer:
            blocks += [
                _header(f"🤖  {label}"),
                _section(answer[:2900]),  # Slack mrkdwn block limit
                _divider(),
            ]
        elif error:
            blocks += [
                _header(f"🤖  {label}"),
                _section(f":x:  _Error: {error}_"),
                _divider(),
            ]
    return blocks


def _failed_checks_section(data: dict) -> list[dict]:
    """
    Render a dedicated section for checks that failed during execution.
    Shows check name, error message, and HTTP status code if available.
    """
    failed = []
    for key, value in data.items():
        if key.endswith("_error") and isinstance(value, dict) and value.get("status") == "FAILED":
            check_name = key.replace("_error", "").replace("_", " ").title()
            error_msg = value.get("error", "Unknown error")
            status_code = value.get("status_code")
            failed.append({
                "name": check_name,
                "error": error_msg,
                "status_code": status_code
            })
    
    if not failed:
        return []
    
    lines = []
    for f in failed:
        status_str = f" (HTTP {f['status_code']})" if f['status_code'] else ""
        error_preview = f['error'][:100] + "..." if len(f['error']) > 100 else f['error']
        lines.append(f"🚫  *{f['name']}*{status_str}")
        lines.append(f"    `{error_preview}`")
    
    return [
        _header("⚠️  Failed Checks"),
        _section("\n".join(lines)),
        _context("_These checks encountered errors during execution. Review permissions and API access._"),
        _divider(),
    ]


def _footer(team_name: str, check_time: str) -> list[dict]:
    return [
        _divider(),
        _context(f"🤖  Coralogix AHC Automation  |  {team_name}  |  {_display_time(check_time)}"),
    ]


def _pdf_reference(pdf_uploaded: bool = False) -> list[dict]:
    """Add a message referencing the attached PDF report."""
    if pdf_uploaded:
        return [
            _divider(),
            _section("📎  *The detailed Account Health Check report is attached in this thread.*"),
        ]
    return [
        _divider(),
        _section("📎  *Please refer to the PDF report in the output folder for detailed information.*"),
    ]


# ── Slack delivery ─────────────────────────────────────────────────────────────

def _post_text_message(bot_token: str, channel_id: str, text: str, logger=None) -> bool:
    """Post a plain text message to Slack via chat.postMessage (fallback when PDF fails)."""
    if not bot_token or not channel_id or not text:
        if logger:
            logger.warning("Missing bot_token, channel_id, or text for Slack post")
        return False
    try:
        r = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={
                "Authorization": f"Bearer {bot_token}",
                "Content-Type": "application/json",
            },
            json={"channel": channel_id, "text": text},
            timeout=15,
        )
        data = r.json()
        if data.get("ok"):
            return True
        if logger:
            logger.warning(f"Slack post failed: {data.get('error', 'Unknown error')}")
        return False
    except Exception as e:
        if logger:
            logger.warning(f"Slack post error: {e}")
        return False


def _upload_pdf_with_message(bot_token: str, channel_id: str, pdf_path: str, team_name: str, message_text: str, logger=None) -> bool:
    """Upload PDF file to Slack with message as initial_comment (single message with attachment)."""
    if not bot_token or not channel_id or not pdf_path:
        if logger:
            logger.warning("Missing bot_token, channel_id, or pdf_path for PDF upload")
        return False
    
    if not os.path.exists(pdf_path):
        if logger:
            logger.warning(f"PDF file not found: {pdf_path}")
        return False
    
    try:
        filename = os.path.basename(pdf_path)
        file_size = os.path.getsize(pdf_path)
        
        # Step 1: Get upload URL (use form data, not JSON)
        get_url_response = requests.post(
            "https://slack.com/api/files.getUploadURLExternal",
            headers={"Authorization": f"Bearer {bot_token}"},
            data={
                "filename": filename,
                "length": file_size,
            },
            timeout=30,
        )
        
        url_data = get_url_response.json()
        if not url_data.get("ok"):
            error = url_data.get("error", "Unknown error")
            if logger:
                logger.warning(f"Slack getUploadURL failed: {error}")
            return False
        
        upload_url = url_data.get("upload_url")
        file_id = url_data.get("file_id")
        
        # Step 2: Upload file to the URL (raw binary upload)
        with open(pdf_path, "rb") as f:
            file_content = f.read()
        
        upload_response = requests.post(
            upload_url,
            data=file_content,
            headers={"Content-Type": "application/octet-stream"},
            timeout=120,
        )
        
        if upload_response.status_code != 200:
            if logger:
                logger.warning(f"Slack file upload failed: {upload_response.status_code}")
            return False
        
        # Step 3: Complete upload and share to channel with message
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
        date_str = date_match.group(1) if date_match else ""
        title_with_date = f"AHC Report - {team_name} - {date_str}" if date_str else f"AHC Report - {team_name}"
        
        complete_response = requests.post(
            "https://slack.com/api/files.completeUploadExternal",
            headers={
                "Authorization": f"Bearer {bot_token}",
                "Content-Type": "application/json",
            },
            json={
                "files": [{"id": file_id, "title": title_with_date}],
                "channel_id": channel_id,
                "initial_comment": message_text,
            },
            timeout=30,
        )
        
        complete_data = complete_response.json()
        if complete_data.get("ok"):
            if logger:
                logger.element_info("PDF uploaded with message to Slack")
            return True
        else:
            error = complete_data.get("error", "Unknown error")
            if logger:
                logger.warning(f"Slack completeUpload failed: {error}")
            return False
            
    except Exception as e:
        if logger:
            logger.warning(f"Slack PDF upload error: {e}")
        return False

def _resolve_channel_id(bot_token: str, channel: str) -> str:
    if re.match(r"^[CGDZ][A-Z0-9]{8,}$", channel):
        return channel
    headers = {"Authorization": f"Bearer {bot_token}",
               "Content-Type": "application/json"}
    r = requests.post("https://slack.com/api/chat.postMessage",
                      headers=headers,
                      json={"channel": channel, "text": " "},
                      timeout=10)
    d = r.json()
    if d.get("ok"):
        ch_id = d["channel"]
        ts    = d.get("ts")
        if ts:
            requests.post("https://slack.com/api/chat.delete",
                          headers=headers,
                          json={"channel": ch_id, "ts": ts},
                          timeout=10)
        return ch_id
    raise RuntimeError(f"Cannot resolve channel '{channel}': {d.get('error')}")


def _post_blocks_via_bot(bot_token: str, channel_id: str, blocks: list[dict], logger=None, file_info: dict = None):
    """Post blocks via Slack Bot API (chat.postMessage). Slack limits 50 blocks per message.
    If file_info is provided, shares the file as a reply in the same thread."""
    LIMIT = 50
    chunks = [blocks[i:i+LIMIT] for i in range(0, len(blocks), LIMIT)]
    
    headers = {
        "Authorization": f"Bearer {bot_token}",
        "Content-Type": "application/json",
    }
    
    thread_ts = None
    for i, chunk in enumerate(chunks):
        payload = {
            "channel": channel_id,
            "blocks": chunk,
        }
        # If we have a thread_ts from previous chunk, use it
        if thread_ts:
            payload["thread_ts"] = thread_ts
        
        r = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers=headers,
            json=payload,
            timeout=15,
        )
        response = r.json()
        if not response.get("ok"):
            if logger:
                logger.warning(f"Slack bot post failed: {response.get('error', 'Unknown error')}")
        else:
            # Get thread_ts from first message for threading
            if i == 0:
                thread_ts = response.get("ts")
    
    # Share the file to the channel as a reply in the thread
    if file_info:
        _complete_file_share(bot_token, channel_id, file_info, thread_ts, logger)


def _complete_file_share(bot_token: str, channel_id: str, file_info: dict, thread_ts: str = None, logger=None):
    """Complete file upload by sharing to channel, optionally in a thread."""
    if not file_info:
        return
    
    file_id = file_info.get("file_id")
    title = file_info.get("title", "AHC Report")
    
    headers = {
        "Authorization": f"Bearer {bot_token}",
        "Content-Type": "application/json",
    }
    
    # Use files.completeUploadExternal to share to channel
    payload = {
        "files": [{"id": file_id, "title": title}],
        "channel_id": channel_id,
    }
    if thread_ts:
        payload["thread_ts"] = thread_ts
    
    r = requests.post(
        "https://slack.com/api/files.completeUploadExternal",
        headers=headers,
        json=payload,
        timeout=15,
    )
    response = r.json()
    if not response.get("ok"):
        if logger:
            logger.warning(f"Slack file share failed: {response.get('error', 'Unknown error')}")


# ── Entry point ────────────────────────────────────────────────────────────────

def generate_and_send(output_json_path: str, slack_cfg: dict, logger=None, pdf_path: str = None):
    with open(output_json_path, "r") as f:
        data = json.load(f)

    # Extract team name from team_url subdomain
    team_url  = data.get("team_url", "")
    team_name = "unknown"
    if team_url:
        m = re.match(r"https?://([^.]+)\.", team_url)
        if m:
            team_name = m.group(1)

    check_time = data.get("check_time") or get_report_time_ist()

    # Check if we have bot_token and channel_id
    bot_token = slack_cfg.get("bot_token")
    channel_id = slack_cfg.get("channel_id")
    
    if not bot_token or not channel_id:
        if logger:
            logger.warning("Slack bot_token or channel_id not configured — skipping Slack report")
        return

    # Build the plain text message
    message_text = _build_message_text(data, team_name, check_time, pdf_attached=bool(pdf_path))
    
    # Upload PDF with message, or post text-only fallback when PDF generation failed
    if pdf_path:
        if logger:
            logger.element_info("Sending Slack message with PDF attachment …")
        success = _upload_pdf_with_message(bot_token, channel_id, pdf_path, team_name, message_text, logger)
        if success:
            if logger:
                logger.element_info("Slack report sent ✔")
        else:
            if logger:
                logger.warning("Failed to send Slack report with PDF")
    else:
        # PDF generation failed (e.g. NumPy/platform issue) — post text summary so user gets feedback
        if logger:
            logger.element_info("Posting text-only report (PDF generation failed) …")
        success = _post_text_message(bot_token, channel_id, message_text, logger)
        if success:
            if logger:
                logger.element_info("Slack report sent ✔ (text only)")
        else:
            if logger:
                logger.warning("Failed to post text-only Slack report")
