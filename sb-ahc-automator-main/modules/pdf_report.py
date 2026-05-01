"""
PDF Report Generator — Coralogix Account Health Check
Creates a professional, visually stunning PDF report with Snowbit/Coralogix branding.
"""
from __future__ import annotations

import html
import io
import json
import os
import re
from datetime import datetime
from typing import Any

from modules.region_config import get_report_time_ist

# Set matplotlib config dir before import (Lambda has read-only /home)
if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
    os.environ.setdefault('MPLCONFIGDIR', '/tmp/matplotlib')

import matplotlib
matplotlib.use('Agg')


def _sanitize_for_pdf(text: str) -> str:
    """Replace problematic Unicode chars that render as black squares in PDF."""
    if not text or not isinstance(text, str):
        return str(text) if text is not None else ""
    replacements = [
        ("\u2014", "-"),   # em dash
        ("\u2013", "-"),   # en dash
        ("\u2018", "'"),   # left single quote
        ("\u2019", "'"),   # right single quote
        ("\u201c", '"'),   # left double quote
        ("\u201d", '"'),   # right double quote
        ("\u2026", "..."), # ellipsis
        ("\u00a0", " "),   # non-breaking space
    ]
    for old, new in replacements:
        text = text.replace(old, new)
    # Remove replacement char and other problematic codepoints
    text = "".join(c for c in text if ord(c) < 0x10000 and c != "\ufffd")
    return text


def _safe_float(val: Any, default: float = 0.0) -> float:
    """Convert to float, handling 'N/A', None, and invalid strings."""
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().lower()
    if s in ('', 'n/a', 'na', 'none', '-'):
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default
import matplotlib.pyplot as plt
import numpy as np

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch, mm
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.platypus import (
    BaseDocTemplate, Paragraph, Spacer, Table, TableStyle,
    Image, PageBreak, KeepTogether, HRFlowable, Flowable,
    Frame, PageTemplate, NextPageTemplate
)
from reportlab.graphics.shapes import Drawing, Rect, String
from reportlab.pdfgen import canvas


# ── Coralogix/Snowbit Brand Colors ──────────────────────────────────────────────
SNOWBIT_TEAL = colors.HexColor("#00C9A7")
SNOWBIT_DARK = colors.HexColor("#1A1A2E")
CORALOGIX_PURPLE = colors.HexColor("#6C5CE7")
CORALOGIX_SUCCESS = colors.HexColor("#00B894")
CORALOGIX_WARNING = colors.HexColor("#FDCB6E")
CORALOGIX_DANGER = colors.HexColor("#E17055")
CORALOGIX_INFO = colors.HexColor("#74B9FF")
CORALOGIX_GRAY = colors.HexColor("#636E72")
CORALOGIX_LIGHT_GRAY = colors.HexColor("#DFE6E9")
CORALOGIX_WHITE = colors.HexColor("#FFFFFF")

# Asset paths
ASSETS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets")
LOGO_PATH = os.path.join(ASSETS_DIR, "snowbit_logo.png")
AWS_LOGO = os.path.join(ASSETS_DIR, "aws.png")
AZURE_LOGO = os.path.join(ASSETS_DIR, "azure.png")
GCP_LOGO = os.path.join(ASSETS_DIR, "google-cloud.png")


# ── Styles ──────────────────────────────────────────────────────────────────────
def get_styles():
    styles = getSampleStyleSheet()
    
    styles.add(ParagraphStyle(
        name='CXTitle',
        fontSize=24,
        leading=30,
        textColor=SNOWBIT_DARK,
        alignment=TA_CENTER,
        spaceAfter=10,
        fontName='Helvetica-Bold'
    ))
    
    styles.add(ParagraphStyle(
        name='CXSubtitle',
        fontSize=12,
        leading=16,
        textColor=CORALOGIX_GRAY,
        alignment=TA_CENTER,
        spaceAfter=20,
        fontName='Helvetica'
    ))
    
    styles.add(ParagraphStyle(
        name='CXHeading1',
        fontSize=16,
        leading=20,
        textColor=SNOWBIT_TEAL,
        spaceBefore=15,
        spaceAfter=10,
        fontName='Helvetica-Bold'
    ))
    
    styles.add(ParagraphStyle(
        name='CXHeading2',
        fontSize=12,
        leading=16,
        textColor=SNOWBIT_DARK,
        spaceBefore=10,
        spaceAfter=6,
        fontName='Helvetica-Bold'
    ))
    
    styles.add(ParagraphStyle(
        name='CXBody',
        fontSize=10,
        leading=14,
        textColor=SNOWBIT_DARK,
        spaceAfter=4,
        fontName='Helvetica'
    ))
    
    styles.add(ParagraphStyle(
        name='CXSmall',
        fontSize=8,
        leading=10,
        textColor=CORALOGIX_GRAY,
        fontName='Helvetica'
    ))
    
    styles.add(ParagraphStyle(
        name='CXSmallBold',
        fontSize=8,
        leading=10,
        textColor=SNOWBIT_DARK,
        fontName='Helvetica-Bold'
    ))
    
    styles.add(ParagraphStyle(
        name='CXTinier',
        fontSize=6,
        leading=8,
        textColor=CORALOGIX_GRAY,
        fontName='Helvetica',
        spaceAfter=4,
    ))
    
    styles.add(ParagraphStyle(
        name='CXBullet',
        fontSize=9,
        leading=12,
        textColor=SNOWBIT_DARK,
        leftIndent=15,
        fontName='Helvetica'
    ))
    
    styles.add(ParagraphStyle(
        name='CXConcern',
        fontSize=10,
        leading=16,
        textColor=CORALOGIX_DANGER,
        leftIndent=10,
        fontName='Helvetica-Bold',
        backColor=colors.HexColor("#FFF5F5"),  # Light red background
        borderPadding=4,
        spaceAfter=4,
    ))
    
    styles.add(ParagraphStyle(
        name='CXSuccess',
        fontSize=9,
        leading=13,
        textColor=CORALOGIX_SUCCESS,
        leftIndent=10,
        fontName='Helvetica'
    ))
    
    return styles


# ── Chart Generators ────────────────────────────────────────────────────────────

def create_health_score_gauge(score: int) -> io.BytesIO:
    """Create a perfectly circular gauge chart for health score."""
    fig = plt.figure(figsize=(4, 4))
    ax = fig.add_subplot(111)
    ax.set_aspect('equal', adjustable='box')
    
    sizes = [score, 100 - score]
    if score >= 80:
        color = '#00B894'
    elif score >= 60:
        color = '#FDCB6E'
    else:
        color = '#E17055'
    
    colors_list = [color, '#E8E8E8']
    
    wedges, _ = ax.pie(sizes, colors=colors_list, startangle=90,
                       wedgeprops=dict(width=0.3, edgecolor='white'))
    
    ax.text(0, 0.05, f'{score}%', ha='center', va='center',
            fontsize=32, fontweight='bold', color='#1A1A2E')
    ax.text(0, -0.25, 'Health Score', ha='center', va='center',
            fontsize=12, color='#636E72')
    
    ax.set_xlim(-1.5, 1.5)
    ax.set_ylim(-1.5, 1.5)
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                facecolor='white', edgecolor='none', pad_inches=0.1)
    plt.close()
    buf.seek(0)
    return buf


def create_tco_bars(data: dict) -> io.BytesIO:
    """Create horizontal bar chart for TCO distribution (no pie chart)."""
    fig, ax = plt.subplots(figsize=(5, 2))
    
    high = _safe_float(data.get('high_pct'), 0)
    medium = _safe_float(data.get('medium_pct'), 0)
    low = _safe_float(data.get('low_pct'), 0)
    blocked = _safe_float(data.get('blocked_pct') or data.get('block_pct'), 0)
    
    categories = ['High', 'Medium', 'Low', 'Blocked']
    values = [high, medium, low, blocked]
    colors_list = ['#00B894', '#74B9FF', '#FDCB6E', '#E17055']
    
    y_pos = np.arange(len(categories))
    bars = ax.barh(y_pos, values, color=colors_list, height=0.6, edgecolor='none')
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(categories, fontsize=10)
    ax.set_xlim(0, 105)
    ax.set_xlabel('Percentage', fontsize=9)
    
    for bar, val in zip(bars, values):
        if val > 0:
            ax.text(val + 1, bar.get_y() + bar.get_height()/2,
                    f'{val:.1f}%', va='center', ha='left', fontsize=9, fontweight='bold')
    
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.invert_yaxis()
    
    plt.tight_layout()
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close()
    buf.seek(0)
    return buf


def create_horizontal_bar(value: float, max_val: float, label: str) -> io.BytesIO:
    """Create a horizontal progress bar."""
    fig, ax = plt.subplots(figsize=(4, 0.8))
    
    pct = (value / max_val * 100) if max_val > 0 else 0
    color = '#00B894' if pct < 70 else ('#FDCB6E' if pct < 90 else '#E17055')
    
    ax.barh(0, 100, height=0.5, color='#E8E8E8', edgecolor='none')
    ax.barh(0, min(pct, 100), height=0.5, color=color, edgecolor='none')
    
    ax.text(105, 0, f'{pct:.1f}%', va='center', ha='left',
            fontsize=11, fontweight='bold', color='#1A1A2E')
    
    ax.set_xlim(0, 130)
    ax.set_ylim(-0.8, 0.8)
    ax.axis('off')
    
    plt.tight_layout()
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close()
    buf.seek(0)
    return buf


def create_alert_history_line_chart(line_data: list) -> io.BytesIO:
    """Create line chart for Total Alerts Count Over Time (last 24h)."""
    fig, ax = plt.subplots(figsize=(5.5, 2.2))
    if not line_data:
        ax.text(0.5, 0.5, 'No data', ha='center', va='center', fontsize=12)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
    else:
        timestamps = [x[0] for x in line_data]
        values = [x[1] for x in line_data]
        x_positions = range(len(values))
        ax.plot(x_positions, values, color='#00C9A7', linewidth=2, label='Alert Count')
        ax.fill_between(x_positions, values, alpha=0.2, color='#00C9A7')
        ax.set_ylabel('Count', fontsize=9)
        ax.legend(loc='upper right', fontsize=8)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        # X-axis: show time labels (e.g. every 4th point for 24h with 1h step)
        step = max(1, len(x_positions) // 6)
        tick_positions = list(x_positions)[::step]
        tick_labels = [
            datetime.utcfromtimestamp(timestamps[i]).strftime('%H:%M')
            for i in range(0, len(timestamps), step)
        ]
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(tick_labels, fontsize=8)
        ax.tick_params(axis='x', labelsize=8)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close()
    buf.seek(0)
    return buf


def create_alert_history_bar_chart(by_priority: dict) -> io.BytesIO:
    """Create bar chart for Alert Count By Priority (P1–P5)."""
    if not by_priority:
        fig, ax = plt.subplots(figsize=(5, 2))
        ax.text(0.5, 0.5, 'No data', ha='center', va='center', fontsize=12)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
    else:
        order = ['P1', 'P2', 'P3', 'P4', 'P5']
        categories = [p for p in order if p in by_priority]
        extra = [p for p in by_priority if p not in order]
        categories = categories + sorted(extra)
        values = [by_priority.get(p, 0) for p in categories]
        colors_map = {
            'P1': '#00B894',
            'P2': '#6C5CE7',
            'P3': '#FDCB6E',
            'P4': '#74B9FF',
            'P5': '#E17055',
        }
        colors_list = [colors_map.get(p, '#636E72') for p in categories]
        fig, ax = plt.subplots(figsize=(5, 2.2))
        bars = ax.bar(categories, values, color=colors_list, edgecolor='none', width=0.6)
        max_val = max(values) if values else 0
        for bar, val in zip(bars, values):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + (max_val * 0.02 if max_val else 1),
                        str(int(val)), ha='center', va='bottom', fontsize=9, fontweight='bold')
        ax.set_ylabel('Count', fontsize=9)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.tick_params(axis='x', labelsize=9)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close()
    buf.seek(0)
    return buf


def create_normalization_bar_chart(data: dict) -> io.BytesIO:
    """Create a bar chart for normalization status (2 categories: Normalized vs Not Normalized)."""
    fig, ax = plt.subplots(figsize=(3.5, 2))
    
    # Combine fully + partially normalized as "Normalized"
    fully = _safe_float(data.get('fully_normalized_apps'), 0)
    partial = _safe_float(data.get('partially_normalized_apps'), 0)
    normalized = fully + partial
    not_norm = _safe_float(data.get('not_normalized_apps'), 0)
    
    categories = ['Normalized', 'Not\nNormalized']
    values = [normalized, not_norm]
    colors_list = ['#00B894', '#E17055']
    
    bars = ax.bar(categories, values, color=colors_list, edgecolor='none', width=0.5)
    
    for bar, val in zip(bars, values):
        if val > 0:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                    str(int(val)), ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    ax.set_ylabel('Apps', fontsize=9)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.tick_params(axis='x', labelsize=9)
    
    plt.tight_layout()
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close()
    buf.seek(0)
    return buf


# ── Helper Functions ────────────────────────────────────────────────────────────

def bool_icon(value: Any) -> str:
    """Return check/cross icon based on boolean value."""
    if value is True or str(value).lower() in ('true', 'yes', 'enabled', 'configured', 'used', '1'):
        return '✓'
    return '✗'


def bool_color(value: Any) -> colors.Color:
    """Return color based on boolean value."""
    if value is True or str(value).lower() in ('true', 'yes', 'enabled', 'configured', 'used', '1'):
        return CORALOGIX_SUCCESS
    return CORALOGIX_DANGER


def calculate_health_score(data: dict) -> int:
    """Calculate overall health score."""
    score = 100
    deductions = 0
    
    ab = data.get('archive_buckets', {})
    if not ab.get('logs', {}).get('active'):
        deductions += 10
    if not ab.get('metrics', {}).get('active'):
        deductions += 5
    
    if not data.get('saml', {}).get('configured'):
        deductions += 10
    if not data.get('mfa', {}).get('enforced'):
        deductions += 10
    if not data.get('ip_access', {}).get('enabled'):
        deductions += 5
    
    du = data.get('data_usage', {})
    if du:
        quota = _safe_float(du.get('daily_quota'), 0)
        avg = _safe_float(du.get('avg_daily_units'), 0)
        if quota > 0 and avg > 0:
            pct = avg / quota * 100
            if pct >= 90:
                deductions += 15
            elif pct >= 80:
                deductions += 10
    
    tco = data.get('tco_distribution', {})
    if tco:
        blocked = _safe_float(tco.get('blocked_pct') or tco.get('block_pct'), 0)
        if blocked > 0:
            deductions += 10
        low = _safe_float(tco.get('low_pct'), 0)
        if low > 20:
            deductions += 5
    
    ul = data.get('unparsed_logs', {})
    if ul and not ul.get('all_parsed', True):
        total = ul.get('total_logs', 0)
        unparsed = ul.get('total_unparsed', 0)
        if total > 0:
            pct = unparsed / total * 100
            if pct > 5:
                deductions += 10
            elif pct > 1:
                deductions += 5
    
    for key in data:
        if key.endswith('_error'):
            deductions += 5
    
    return max(0, score - deductions)


def get_concerns(data: dict) -> list[str]:
    """Extract all concerns from data (same as Slack report)."""
    concerns = []
    
    # Use simple bullet for PDF (Unicode emojis don't render well)
    bullet = "•"
    
    # CSPM check error (standalone check, not MCP)
    cspm = data.get("cspm", {})
    if isinstance(cspm, dict) and cspm.get("error"):
        concerns.append(f"{bullet} CSPM — FAILED: {str(cspm.get('error', ''))[:60]}")

    # Alerts Status check error (incidents API failed)
    if (data.get("alerts_status") or {}).get("error"):
        concerns.append(f"{bullet} Alerts Status — FAILED: {str(data['alerts_status'].get('error', ''))[:60]}")

    # MCP sub-check errors
    _mcp_labels = {"unparsed_logs": "Unparsed Logs", "no_log_alerts": "No Log Alerts",
                   "ingestion_block_alert": "Ingestion Block Alert"}
    for key, label in _mcp_labels.items():
        val = data.get(key)
        if isinstance(val, dict) and val.get("error"):
            concerns.append(f"{bullet} {label} — FAILED: {str(val.get('error', ''))[:60]}")

    # Failed checks
    for key, value in data.items():
        if key.endswith('_error') and isinstance(value, dict) and value.get('status') == 'FAILED':
            check_name = key.replace('_error', '').replace('_', ' ').title()
            error_msg = value.get('error', 'Unknown')[:60]
            status_code = value.get('status_code')
            if status_code:
                concerns.append(f"{bullet} {check_name} — FAILED (HTTP {status_code})")
            else:
                concerns.append(f"{bullet} {check_name} — FAILED: {error_msg}")
    
    # Boolean checks (matching slack_report.py logic)
    # Skip archive bucket "not configured" when archive_bucket_error exists — we already show CHECK FAILED
    ab_error = data.get('archive_bucket_error', {})
    bool_checks = [
        ('S3 Log archive bucket', data.get('archive_buckets', {}).get('logs', {}).get('active')),
        ('S3 Metrics archive bucket', data.get('archive_buckets', {}).get('metrics', {}).get('active')),
        ('SAML', data.get('saml', {}).get('configured')),
        ('MFA Configured', data.get('mfa', {}).get('enforced')),
        ('IP Access Control', data.get('ip_access', {}).get('enabled')),
        ('Team Auditing', data.get('team_auditing', {}).get('configured')),
        ('Cora AI', data.get('cora_ai', {}).get('dataprime_query_assistance_enabled')),
        ('CX Alerts Metrics', data.get('cx_alerts_metrics', {}).get('enabled')),
        ('Data Usage Metrics', data.get('data_usage_metrics') == 'enabled'),
        ('Suppression Rules', data.get('suppression_rules') == 'used'),
        ('Send Log Webhook', data.get('send_log_webhook_created')),
    ]
    for label, val in bool_checks:
        if not val:
            if ab_error.get('status') == 'FAILED' and label in ('S3 Log archive bucket', 'S3 Metrics archive bucket'):
                continue  # Already shown as Archive Bucket — FAILED
            concerns.append(f"{bullet} {label} — not configured / not active")
    
    # Data usage >= 90%
    du = data.get('data_usage', {})
    if du:
        quota = _safe_float(du.get('daily_quota'), 0)
        avg = _safe_float(du.get('avg_daily_units'), 0)
        if quota > 0 and avg > 0:
            pct = avg / quota * 100
            if pct >= 90:
                concerns.append(f"{bullet} Data Usage — {pct:.1f}% of daily quota")
    
    # Limits >= 80%
    lim = data.get('limits', {})
    for key, label in [('ingested_fields_today', 'Ingested Fields limit'), ('alerts', 'Alert limit'),
                       ('enrichments', 'Enrichments limit'), ('parsing_rules', 'Parsing Rules limit')]:
        entry = lim.get(key, {})
        if isinstance(entry, dict):
            used = _safe_float(entry.get('used'), 0)
            limit = _safe_float(entry.get('limit'), 0)
            if limit > 0:
                pct = used / limit * 100
                if pct >= 90:
                    concerns.append(f"{bullet} {label} — {pct:.0f}% used ({int(used)}/{int(limit)})")
                elif pct >= 80:
                    concerns.append(f"{bullet} {label} — {pct:.0f}% used ({int(used)}/{int(limit)})")
    
    # TCO
    tco = data.get('tco_distribution', {})
    if tco:
        low = _safe_float(tco.get('low_pct'), 0)
        blocked = _safe_float(tco.get('blocked_pct') or tco.get('block_pct'), 0)
        if low > 0:
            concerns.append(f"{bullet} TCO Low Priority — {low:.1f}%")
        if blocked > 0:
            concerns.append(f"{bullet} TCO Blocked — {blocked:.1f}%")
    
    # No-log alerts
    nla = data.get('no_log_alerts', {})
    if nla:
        triggered = nla.get('triggered_7d', [])
        if triggered:
            concerns.append(f"{bullet} {len(triggered)} 'No Log' alert(s) triggered in last 7 days")
        uncovered = nla.get('apps_without_coverage', [])
        if uncovered:
            concerns.append(f"{bullet} {len(uncovered)} app(s) have no 'No Log' alert coverage")
    
    # Unparsed logs
    ul = data.get('unparsed_logs', {})
    if ul and not ul.get('all_parsed', True) and ul.get('total_unparsed', 0) > 0:
        total = ul.get('total_unparsed', 0)
        total_logs = ul.get('total_logs', 1)
        pct = (total / total_logs * 100) if total_logs > 0 else 0
        concerns.append(f"{bullet} {total:,} unparsed logs ({pct:.2f}%)")
    
    # Dashboards not in folder
    db = data.get('dashboards', {})
    if db.get('not_in_folder', 0) > 0:
        concerns.append(f"{bullet} {db.get('not_in_folder')} dashboard(s) not in any folder")
    
    # Geo enrichment
    enr = data.get('enrichments', {})
    if enr:
        if not enr.get('geo_cx_security_source_ip'):
            concerns.append(f"{bullet} Geo Enrichment missing: cx_security.source_ip")
        if not enr.get('geo_cx_security_destination_ip'):
            concerns.append(f"{bullet} Geo Enrichment missing: cx_security.destination_ip")
    
    # Ingestion block alert
    iba = data.get('ingestion_block_alert', {})
    if iba:
        if not iba.get('alert_exists'):
            concerns.append(f"{bullet} Ingestion Block Alert — NOT FOUND")
        elif not iba.get('alert_active'):
            concerns.append(f"{bullet} Ingestion Block Alert — DISABLED")
    
    # Disabled alerts
    ast = data.get('alerts_status', {})
    if ast and ast.get('disabled_count', 0) > 0:
        concerns.append(f"{bullet} ⚠️ {ast['disabled_count']} alert(s) are disabled.")

    # Data normalisation status (cx_security, last 24h)
    dn = data.get('data_normalization', {})
    if dn and dn.get('concern_count', 0) > 0:
        concerns.append(f"{bullet} Few app(s) with missing cx_security. Check Data normalisation status section in attached report.")
    
    # Security extensions not deployed
    sec_ext = data.get('security_extensions', {})
    if sec_ext:
        for ext_name, deployed in sec_ext.items():
            if not deployed:
                concerns.append(f"{bullet} Security Extension '{ext_name}' — not deployed")
    
    return concerns


def create_colored_divider() -> HRFlowable:
    """Create a colored horizontal divider."""
    return HRFlowable(width="100%", thickness=2, color=SNOWBIT_TEAL,
                      spaceBefore=5, spaceAfter=10)


def split_into_columns_with_bullets(items: list, num_cols: int = 2) -> list:
    """Split a list into columns for table display with bullet points and bold text."""
    if not items:
        return []
    
    # Calculate rows needed
    rows_needed = (len(items) + num_cols - 1) // num_cols
    
    # Create column data
    columns = []
    for i in range(num_cols):
        start = i * rows_needed
        end = start + rows_needed
        columns.append(items[start:end])
    
    # Pad shorter columns
    max_len = max(len(col) for col in columns) if columns else 0
    for col in columns:
        while len(col) < max_len:
            col.append('')
    
    # Transpose to rows with bullets
    rows = []
    for i in range(max_len):
        row = []
        for j in range(num_cols):
            item = columns[j][i] if i < len(columns[j]) else ''
            if item:
                row.append(f"• <b>{item}</b>")
            else:
                row.append('')
        rows.append(row)
    
    return rows


# ── Page Template with Full-Width Centered Header ───────────────────────────────

def first_page_header(canvas_obj, doc, logo_path, team_name):
    """Draw header with logo on the first page."""
    canvas_obj.saveState()
    
    page_width = A4[0]
    page_height = A4[1]
    margin = 30
    
    # Header with logo - BIGGER size, preserve original aspect ratio, centered
    if os.path.exists(logo_path):
        try:
            logo_width = page_width - 20
            logo_height = 90
            x_pos = 10
            canvas_obj.drawImage(logo_path, x_pos, page_height - 95,
                                 width=logo_width, height=logo_height,
                                 preserveAspectRatio=True, anchor='c', mask='auto')
        except:
            canvas_obj.setFont('Helvetica-Bold', 18)
            canvas_obj.setFillColor(SNOWBIT_TEAL)
            canvas_obj.drawCentredString(page_width / 2, page_height - 50, "Snowbit by Coralogix")
    else:
        canvas_obj.setFont('Helvetica-Bold', 18)
        canvas_obj.setFillColor(SNOWBIT_TEAL)
        canvas_obj.drawCentredString(page_width / 2, page_height - 50, "Snowbit by Coralogix")
    
    # Header line (teal line below the logo)
    canvas_obj.setStrokeColor(SNOWBIT_TEAL)
    canvas_obj.setLineWidth(2)
    canvas_obj.line(10, page_height - 100, page_width - 10, page_height - 100)
    
    # Footer
    canvas_obj.setFont('Helvetica', 8)
    canvas_obj.setFillColor(CORALOGIX_GRAY)
    canvas_obj.drawString(margin, 25, f"Account Health Check Report — {team_name}")
    canvas_obj.drawRightString(page_width - margin, 25, f"Page {doc.page}")
    
    canvas_obj.restoreState()


def later_pages_header(canvas_obj, doc, team_name):
    """Simple header for pages 2+ (no logo, just small line and footer)."""
    canvas_obj.saveState()
    
    page_width = A4[0]
    page_height = A4[1]
    margin = 30
    
    # Small teal line at very top (minimal space used)
    canvas_obj.setStrokeColor(SNOWBIT_TEAL)
    canvas_obj.setLineWidth(2)
    canvas_obj.line(30, page_height - 25, page_width - 30, page_height - 25)
    
    # Footer
    canvas_obj.setFont('Helvetica', 8)
    canvas_obj.setFillColor(CORALOGIX_GRAY)
    canvas_obj.drawString(margin, 25, f"Account Health Check Report — {team_name}")
    canvas_obj.drawRightString(page_width - margin, 25, f"Page {doc.page}")
    
    canvas_obj.restoreState()


# ── Main Report Generator ───────────────────────────────────────────────────────

def generate_pdf_report(output_json_path: str, output_dir: str, logger=None) -> str:
    """Generate a professional PDF report."""
    
    with open(output_json_path, 'r') as f:
        data = json.load(f)
    
    # Extract team info
    team_url = data.get('team_url', '')
    team_name = 'Unknown'
    if team_url:
        m = re.match(r'https?://([^.]+)\.', team_url)
        if m:
            team_name = m.group(1)
    
    check_time = data.get('check_time') or get_report_time_ist()
    company_id = data.get('company_id', data.get('client_id', 'N/A'))
    health_score = calculate_health_score(data)
    
    # PDF path (IST date for filename)
    date_str = check_time[:10] if len(check_time) >= 10 else get_report_time_ist()[:10]
    pdf_filename = f'AHC_{team_name}_{date_str}.pdf'
    pdf_path = os.path.join(output_dir, pdf_filename)
    
    # Create document with different page templates for page 1 vs later pages
    page_width, page_height = A4
    
    # Frame for first page (large top margin for header with logo)
    first_page_frame = Frame(
        30, 50,  # x, y (bottom-left)
        page_width - 60, page_height - 165,  # width, height (115 top + 50 bottom margin)
        id='first_page'
    )
    
    # Frame for later pages (small top margin, no logo)
    later_page_frame = Frame(
        30, 50,  # x, y (bottom-left)
        page_width - 60, page_height - 85,  # width, height (35 top + 50 bottom margin)
        id='later_pages'
    )
    
    # Create page templates
    first_page_template = PageTemplate(
        id='FirstPage',
        frames=[first_page_frame],
        onPage=lambda c, d: first_page_header(c, d, LOGO_PATH, team_name)
    )
    
    later_page_template = PageTemplate(
        id='LaterPages',
        frames=[later_page_frame],
        onPage=lambda c, d: later_pages_header(c, d, team_name)
    )
    
    doc = BaseDocTemplate(pdf_path, pagesize=A4)
    doc.addPageTemplates([first_page_template, later_page_template])
    
    styles = get_styles()
    story = []
    
    def _section_elapsed(*check_keys: str):
        """Return Paragraph for 'Time elapsed: X seconds' if timing available."""
        elapsed = data.get('check_elapsed_seconds') or {}
        total = sum(elapsed.get(k, 0) for k in check_keys)
        if total > 0:
            return Paragraph(f'<i>Time elapsed: {total:.1f} seconds</i>', styles['CXTinier'])
        return None
    
    # ══════════════════════════════════════════════════════════════════════════
    # COVER PAGE (Page 1) - Redesigned
    # ══════════════════════════════════════════════════════════════════════════
    story.append(Spacer(1, 10))
    # Switch to later pages template after first page
    story.append(NextPageTemplate('LaterPages'))
    
    # Title in teal color
    story.append(Paragraph(
        '<font color="#00C9A7">Account Health Check Report</font>',
        styles['CXTitle']
    ))
    
    # Team name in black
    story.append(Paragraph(
        f'<font color="#1A1A2E"><b>{team_name.upper()}</b></font>',
        styles['CXTitle']
    ))
    
    story.append(Spacer(1, 15))
    
    # Format date/time nicely: "12 March 2026 08:15 PM IST"
    try:
        from datetime import datetime as dt
        parsed_time = dt.strptime(check_time[:19], '%Y-%m-%d %H:%M:%S')
        formatted_time = parsed_time.strftime('%d %B %Y %I:%M %p') + ' IST'
    except Exception:
        formatted_time = f"{check_time} IST" if check_time and not str(check_time).upper().endswith(' IST') else (check_time or 'N/A')
    
    # Info as simple text lines (no table)
    story.append(Paragraph(f'<b>Team URL:</b> {team_url or "N/A"}', styles['CXBody']))
    story.append(Paragraph(f'<b>Company ID:</b> {company_id}', styles['CXBody']))
    story.append(Paragraph(f'<b>Report Generated:</b> {formatted_time}', styles['CXBody']))
    story.append(Paragraph(f'<b>Health Score:</b> <font color="#1A1A2E"><b>{health_score}%</b></font>', styles['CXBody']))
    
    # ══════════════════════════════════════════════════════════════════════════
    # FAILED CHECKS BANNER (if any)
    # ══════════════════════════════════════════════════════════════════════════
    failed_checks = [k.replace('_error', '').replace('_', ' ').title()
                     for k, v in data.items()
                     if k.endswith('_error') and isinstance(v, dict) and v.get('status') == 'FAILED']
    if data.get('data_usage_error', {}).get('status') == 'FAILED':
        failed_checks.append('Data Usage')
    if data.get('limits_grpc_error', {}).get('status') == 'FAILED':
        failed_checks.append('Limits (gRPC)')
    # CSPM check error (standalone)
    if (data.get("cspm") or {}).get("error"):
        failed_checks.append("CSPM")
    # MCP sub-check errors
    _mcp_labels = {"unparsed_logs": "Unparsed Logs", "no_log_alerts": "No Log Alerts",
                   "ingestion_block_alert": "Ingestion Block Alert"}
    if (data.get("data_normalization") or {}).get("error"):
        failed_checks.append("Data Normalisation Status")
    if (data.get("noisy_alerts") or {}).get("error"):
        failed_checks.append("Noisy Alerts")
    if (data.get("alerts_status") or {}).get("error"):
        failed_checks.append("Alerts Status")
    for key, label in _mcp_labels.items():
        if (data.get(key) or {}).get("error"):
            failed_checks.append(label)
    # Dedupe and compute status
    failed_checks = list(dict.fromkeys(failed_checks))
    failed_count = len(failed_checks)
    total_checks = 26
    passed_count = total_checks - failed_count
    story.append(Spacer(1, 10))
    if failed_count > 0:
        story.append(Paragraph(
            f'<b>Checks:</b> <font color="#E17055">{passed_count} passed, {failed_count} failed</font>',
            styles['CXBody']
        ))
        story.append(Spacer(1, 10))
        story.append(Paragraph(
            '<font color="#E17055" size="18"><b>✗ CHECK FAILED</b></font><br/>'
            f'<font color="#636E72" size="10">The following checks could not complete: {", ".join(failed_checks[:5])}'
            + (' ...' if len(failed_checks) > 5 else '') + '</font>',
            styles['CXBody']
        ))
        story.append(Spacer(1, 15))
    else:
        story.append(Paragraph(f'<b>Checks:</b> <font color="#00B894">All {total_checks} checks passed</font>', styles['CXBody']))
        story.append(Spacer(1, 15))
    
    # ══════════════════════════════════════════════════════════════════════════
    # CONCERNS / ACTION REQUIRED (on Page 1)
    # ══════════════════════════════════════════════════════════════════════════
    concerns = get_concerns(data)
    
    # Add 3-4 line space before Concerns section
    story.append(Spacer(1, 45))
    story.append(Paragraph('<font color="#E17055">■</font> Concerns — Action Required', styles['CXHeading1']))
    story.append(create_colored_divider())
    
    if concerns:
        for concern in concerns:
            story.append(Paragraph(concern, styles['CXConcern']))
    else:
        story.append(Paragraph('✓ No critical concerns found!', styles['CXSuccess']))
    
    story.append(PageBreak())
    
    # ══════════════════════════════════════════════════════════════════════════
    # DATA USAGE
    # ══════════════════════════════════════════════════════════════════════════
    du = data.get('data_usage', {})
    du_error = data.get('data_usage_error', {})
    if du or du_error:
        section = []
        section.append(Paragraph('📈 Data Usage', styles['CXHeading1']))
        section.append(create_colored_divider())
        el = _section_elapsed('data_usage')
        if el:
            section.append(el)
        
        if du_error.get('status') == 'FAILED':
            section.append(Paragraph(
                '<font color="#E17055" size="16"><b>✗ CHECK FAILED</b></font><br/>'
                '<font color="#636E72">Could not fetch data usage. Daily Quota and Average Usage unavailable.</font>',
                styles['CXBody']
            ))
        elif du:
            quota = _safe_float(du.get('daily_quota'), 0)
            avg = _safe_float(du.get('avg_daily_units'), 0)
            bar_img = create_horizontal_bar(avg, quota, 'Usage')
            section.append(Image(bar_img, width=3.5*inch, height=0.6*inch))
            section.append(Paragraph(f'Daily Quota: <b>{int(quota):,}</b> units  |  Average Usage: <b>{avg:.2f}</b> units', styles['CXBody']))
        section.append(Spacer(1, 10))
        
        story.append(KeepTogether(section))
    
    # ══════════════════════════════════════════════════════════════════════════
    # TCO DISTRIBUTION (Horizontal bars instead of pie chart)
    # ══════════════════════════════════════════════════════════════════════════
    tco = data.get('tco_distribution', {})
    tco_error = data.get('tco_distribution_error', {})
    if tco or tco_error:
        section = []
        section.append(Paragraph('🎯 TCO Priority Distribution', styles['CXHeading1']))
        section.append(create_colored_divider())
        el = _section_elapsed('tco_distribution')
        if el:
            section.append(el)
        
        if tco_error.get('status') == 'FAILED':
            section.append(Paragraph(
                '<font color="#E17055" size="16"><b>✗ CHECK FAILED</b></font><br/>'
                '<font color="#636E72">Could not fetch TCO distribution.</font>',
                styles['CXBody']
            ))
        elif tco:
            tco_chart = create_tco_bars(tco)
            tco_img = Image(tco_chart, width=4.5*inch, height=1.8*inch)
            section.append(tco_img)
            
            high = _safe_float(tco.get('high_pct'), 0)
            medium = _safe_float(tco.get('medium_pct'), 0)
            low = _safe_float(tco.get('low_pct'), 0)
            blocked = _safe_float(tco.get('blocked_pct') or tco.get('block_pct'), 0)
            
            section.append(Paragraph(
                f"High: <b>{high:.1f}%</b>  |  "
                f"Medium: <b>{medium:.1f}%</b>  |  "
                f"Low: <b>{low:.1f}%</b>  |  "
                f"Blocked: <b>{blocked:.1f}%</b>",
                styles['CXBody']
            ))
        section.append(Spacer(1, 10))
        
        story.append(KeepTogether(section))
    
    # ══════════════════════════════════════════════════════════════════════════
    # LIMITS
    # ══════════════════════════════════════════════════════════════════════════
    limits = data.get('limits') or {}
    limits_error = data.get('limits_error', {})
    limits_grpc_error = data.get('limits_grpc_error', {})
    grpc_keys = {'alerts', 'enrichments', 'parsing_rules'}
    if limits or limits_error or limits_grpc_error:
        section = []
        section.append(Paragraph('📊 Resource Limits', styles['CXHeading1']))
        section.append(create_colored_divider())
        el = _section_elapsed('limits')
        if el:
            section.append(el)
        
        if limits_error.get('status') == 'FAILED' and not limits:
            section.append(Paragraph(
                '<font color="#E17055" size="16"><b>✗ CHECK FAILED</b></font><br/>'
                '<font color="#636E72">Could not fetch resource limits.</font>',
                styles['CXBody']
            ))
        else:
            limits_data = []
            for key, label in [('ingested_fields_today', 'Ingested Fields limit'),
                               ('alerts', 'Alert limit'), ('enrichments', 'Enrichments limit'),
                               ('parsing_rules', 'Parsing Rules limit')]:
                entry = limits.get(key, {})
                is_grpc_row = key in grpc_keys
                grpc_failed = limits_grpc_error.get('status') == 'FAILED' and is_grpc_row
                used_val, limit_val = entry.get('used'), entry.get('limit')
                is_na = str(used_val).upper() == 'N/A' or str(limit_val).upper() == 'N/A'
                
                if grpc_failed or (is_grpc_row and is_na):
                    status_text = Paragraph('<font color="#E17055" size="12"><b>✗ CHECK FAILED</b></font>', styles['CXSmall'])
                    limits_data.append([label, str(used_val), str(limit_val), status_text])
                elif isinstance(entry, dict) and limit_val:
                    used = _safe_float(used_val, 0)
                    limit_f = _safe_float(limit_val, 0)
                    pct = (used / limit_f * 100) if limit_f > 0 else 0
                    if pct >= 85:
                        icon = '🔴'
                        status_text = Paragraph(f'<font color="#E17055"><b>{icon} {pct:.0f}%</b></font>', styles['CXSmall'])
                    elif pct >= 70:
                        icon = '🟡'
                        status_text = Paragraph(f'<font color="#FDCB6E">{icon} {pct:.0f}%</font>', styles['CXSmall'])
                    else:
                        icon = '🟢'
                        status_text = Paragraph(f'<font color="#00B894">{icon} {pct:.0f}%</font>', styles['CXSmall'])
                    limits_data.append([label, f'{int(used):,}', f'{int(limit_f):,}', status_text])
            
            # Add mapping exceptions and e2m labels limit
            mapping_exc = limits.get('mapping_exceptions')
            if mapping_exc is not None:
                try:
                    limits_data.append(['Mapping Exceptions', f"{int(mapping_exc):,}", 'N/A', '➖'])
                except (TypeError, ValueError):
                    limits_data.append(['Mapping Exceptions', str(mapping_exc), 'N/A', '➖'])
            e2m = limits.get('events2metrics_labels_limit')
            if e2m is not None:
                limits_data.append(['E2M Labels Limit', str(e2m), 'N/A', '➖'])
            
            if limits_data:
                limits_table = Table([['Resource', 'Used', 'Limit', 'Status']] + limits_data,
                                     colWidths=[1.8*inch, 1*inch, 1*inch, 1*inch])
                limits_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), SNOWBIT_TEAL),
                    ('TEXTCOLOR', (0, 0), (-1, 0), CORALOGIX_WHITE),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                    ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('PADDING', (0, 0), (-1, -1), 6),
                    ('GRID', (0, 0), (-1, -1), 0.5, CORALOGIX_LIGHT_GRAY),
                ]))
                section.append(limits_table)
        section.append(Spacer(1, 10))
        
        story.append(KeepTogether(section))
    
    # ══════════════════════════════════════════════════════════════════════════
    # SECURITY CONFIGURATION
    # ══════════════════════════════════════════════════════════════════════════
    section = []
    section.append(Paragraph('🔐 Security Configuration', styles['CXHeading1']))
    section.append(create_colored_divider())
    el = _section_elapsed('saml', 'mfa', 'ip_access', 'team_auditing')
    if el:
        section.append(el)
    
    security_items = [
        ('SAML SSO', data.get('saml', {}).get('configured')),
        ('MFA Enforcement', data.get('mfa', {}).get('enforced')),
        ('IP Access Control', data.get('ip_access', {}).get('enabled')),
        ('Team Auditing', data.get('team_auditing', {}).get('configured')),
    ]
    
    for label, val in security_items:
        icon = bool_icon(val)
        color = '#00B894' if val else '#E17055'
        line = f'{label}: <font color="{color}">{icon}</font>'
        if label == 'Team Auditing' and val:
            audit_name = data.get('team_auditing', {}).get('audit_team_name')
            if audit_name:
                line += f' <font color="#636E72">(Audit team: {audit_name})</font>'
        section.append(Paragraph(line, styles['CXBody']))
    section.append(Spacer(1, 10))
    
    story.append(KeepTogether(section))
    
    # ══════════════════════════════════════════════════════════════════════════
    # ARCHIVE & WEBHOOKS
    # ══════════════════════════════════════════════════════════════════════════
    section = []
    section.append(Paragraph('💾 Archive & Webhooks', styles['CXHeading1']))
    section.append(create_colored_divider())
    el = _section_elapsed('archive_bucket', 'webhook', 'send_log_webhook', 'suppression_rules')
    if el:
        section.append(el)
    
    ab = data.get('archive_buckets', {})
    ab_error = data.get('archive_bucket_error', {})
    logs_archive = ab.get('logs', {})

    # Show CHECK FAILED when API error occurred
    if ab_error.get('status') == 'FAILED':
        section.append(Paragraph(
            '<font color="#E17055" size="16"><b>✗ CHECK FAILED</b></font><br/>'
            f'<font color="#636E72">{ab_error.get("error", "Could not fetch archive bucket config.")[:120]}</font>',
            styles['CXBody']
        ))
        section.append(Spacer(1, 5))

    # S3 Log archive bucket and S3 Metrics archive bucket first, with bucket details right after
    items = [
        ('S3 Log archive bucket', logs_archive.get('active')),
        ('S3 Metrics archive bucket', ab.get('metrics', {}).get('active')),
    ]
    for label, val in items:
        icon = bool_icon(val)
        color = '#00B894' if val else '#E17055'
        section.append(Paragraph(f'{label}: <font color="{color}">{icon}</font>', styles['CXBody']))
    
    # Bucket details right after archive checks
    if logs_archive.get('active'):
        section.append(Paragraph(f"  Bucket: {logs_archive.get('bucket', 'N/A')} | Region: {logs_archive.get('region', 'N/A')}", styles['CXSmall']))
    metrics_archive = ab.get('metrics', {})
    if metrics_archive.get('active') and metrics_archive.get('bucket'):
        section.append(Paragraph(f"  Metrics Bucket: {metrics_archive.get('bucket', 'N/A')} | Region: {metrics_archive.get('region', 'N/A')}", styles['CXSmall']))
    
    # Outbound webhooks
    owh = data.get('outbound_webhooks', {})
    if owh:
        section.append(Spacer(1, 5))
        section.append(Paragraph(f"Outbound Webhooks: <b>{owh.get('amount', 0)}</b>", styles['CXBody']))
        details = owh.get('details', [])
        for d in details:
            section.append(Paragraph(f"  • {d.get('label', 'Unknown')}: {d.get('connections_count', 0)}", styles['CXSmall']))
    
    # Send Log Webhook and Suppression Rules at bottom
    section.append(Spacer(1, 5))
    section.append(Paragraph(f"Send Log Webhook: <font color=\"{'#00B894' if data.get('send_log_webhook_created') else '#E17055'}\">{bool_icon(data.get('send_log_webhook_created'))}</font>", styles['CXBody']))
    section.append(Paragraph(f"Suppression Rules: <font color=\"{'#00B894' if data.get('suppression_rules') == 'used' else '#E17055'}\">{bool_icon(data.get('suppression_rules') == 'used')}</font>", styles['CXBody']))
    
    section.append(Spacer(1, 10))
    story.append(KeepTogether(section))
    
    # ══════════════════════════════════════════════════════════════════════════
    # EXTENSIONS (with 2-column layout, bullets, bold)
    # ══════════════════════════════════════════════════════════════════════════
    ext = data.get('extensions', {})
    ext_error = data.get('extensions_error', {})
    if ext or ext_error:
        section = []
        section.append(Paragraph('🧩 Extensions', styles['CXHeading1']))
        section.append(create_colored_divider())
        el = _section_elapsed('extensions')
        if el:
            section.append(el)
        
        if ext_error.get('status') == 'FAILED' and not ext:
            section.append(Paragraph(
                '<font color="#E17055" size="16"><b>✗ CHECK FAILED</b></font><br/>'
                '<font color="#636E72">Could not fetch extensions.</font>',
                styles['CXBody']
            ))
        else:
            section.append(Paragraph(f"Total Deployed: <b>{ext.get('amount', 0)}</b>", styles['CXBody']))
            section.append(Paragraph(f"Up to Date: <b>{len(ext.get('updated', []))}</b>", styles['CXBody']))
            
            update_list = ext.get('update_available', [])
            section.append(Paragraph(f"Updates Available: <b>{len(update_list)}</b>", styles['CXBody']))
            
            if update_list:
                section.append(Spacer(1, 5))
                section.append(Paragraph('<b>Extensions needing update:</b>', styles['CXBody']))
                
                # Use 2-column table for ALL extensions with bullets and bold
                rows = split_into_columns_with_bullets(update_list, 2)
                if rows:
                    ext_table = Table([[Paragraph(cell, styles['CXSmallBold']) if cell else '' for cell in row] for row in rows],
                                      colWidths=[2.6*inch, 2.6*inch])
                    ext_table.setStyle(TableStyle([
                        ('PADDING', (0, 0), (-1, -1), 2),
                        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ]))
                    section.append(ext_table)
        section.append(Spacer(1, 10))
        
        story.append(KeepTogether(section))
    
    # ══════════════════════════════════════════════════════════════════════════
    # SECURITY EXTENSIONS (from extensions check; show when ext or sec_ext or ext_error)
    # ══════════════════════════════════════════════════════════════════════════
    sec_ext = data.get('security_extensions', {})
    if sec_ext or ext_error:
        section = []
        section.append(Paragraph('🛡️ Security Extensions', styles['CXHeading1']))
        section.append(create_colored_divider())
        el = _section_elapsed('cspm')
        if el:
            section.append(el)
        
        if ext_error.get('status') == 'FAILED' and not sec_ext:
            section.append(Paragraph(
                '<font color="#E17055" size="16"><b>✗ CHECK FAILED</b></font><br/>'
                '<font color="#636E72">Could not fetch security extensions.</font>',
                styles['CXBody']
            ))
        else:
            for ext_name, deployed in sec_ext.items():
                icon = bool_icon(deployed)
                color = '#00B894' if deployed else '#E17055'
                section.append(Paragraph(f'{ext_name}: <font color="{color}">{icon}</font>', styles['CXBody']))
        section.append(Spacer(1, 10))
        
        story.append(KeepTogether(section))
    
    # ══════════════════════════════════════════════════════════════════════════
    # ENRICHMENTS (with newline before Security Enrichment Fields)
    # ══════════════════════════════════════════════════════════════════════════
    enr = data.get('enrichments', {})
    enr_error = data.get('enrichments_error', {})
    if enr or enr_error:
        section = []
        section.append(Paragraph('🌍 Enrichments', styles['CXHeading1']))
        section.append(create_colored_divider())
        el = _section_elapsed('enrichments')
        if el:
            section.append(el)
        
        if enr_error.get('status') == 'FAILED' and not enr:
            section.append(Paragraph(
                '<font color="#E17055" size="16"><b>✗ CHECK FAILED</b></font><br/>'
                '<font color="#636E72">Could not fetch enrichments.</font>',
                styles['CXBody']
            ))
        else:
            geo_fields = enr.get('geo', [])
            sec_fields = enr.get('security', [])
            
            section.append(Paragraph(f"Geo Enrichment Fields: <b>{len(geo_fields)}</b>", styles['CXBody']))
            if geo_fields:
                section.append(Paragraph(f"  {', '.join(geo_fields)}", styles['CXSmall']))
            
            # Add newline/spacer before Security Enrichment Fields
            section.append(Spacer(1, 8))
            section.append(Paragraph(f"Security Enrichment Fields: <b>{len(sec_fields)}</b>", styles['CXBody']))
            if sec_fields:
                section.append(Paragraph(f"  {', '.join(sec_fields)}", styles['CXSmall']))
            
            section.append(Spacer(1, 5))
            src_ip = enr.get('geo_cx_security_source_ip')
            dst_ip = enr.get('geo_cx_security_destination_ip')
            section.append(Paragraph(
                f"cx_security.source_ip: <font color='{'#00B894' if src_ip else '#E17055'}'>{bool_icon(src_ip)}</font>  |  "
                f"cx_security.destination_ip: <font color='{'#00B894' if dst_ip else '#E17055'}'>{bool_icon(dst_ip)}</font>",
                styles['CXBody']
            ))
        section.append(Spacer(1, 10))
        
        story.append(KeepTogether(section))
    
    # ══════════════════════════════════════════════════════════════════════════
    # DASHBOARDS
    # ══════════════════════════════════════════════════════════════════════════
    db = data.get('dashboards', {})
    db_error = data.get('dashboard_folders_error', {})
    if db or db_error:
        section = []
        section.append(Paragraph('📊 Dashboards', styles['CXHeading1']))
        section.append(create_colored_divider())
        el = _section_elapsed('dashboard_folders', 'default_dashboard')
        if el:
            section.append(el)
        
        if db_error.get('status') == 'FAILED' and not db.get('total', 0) and not db.get('in_folder', 0):
            section.append(Paragraph(
                '<font color="#E17055" size="16"><b>✗ CHECK FAILED</b></font><br/>'
                '<font color="#636E72">Could not fetch dashboards.</font>',
                styles['CXBody']
            ))
        else:
            section.append(Paragraph(f"Total: <b>{db.get('total', 0)}</b>", styles['CXBody']))
            section.append(Paragraph(f"In Folders: <b>{db.get('in_folder', 0)}</b> ✓", styles['CXBody']))
            not_in = db.get('not_in_folder', 0)
            color = '#E17055' if not_in > 0 else '#00B894'
            section.append(Paragraph(f"Not in Folder: <font color='{color}'><b>{not_in}</b></font>", styles['CXBody']))
            
            # Show names of dashboards not in folder
            not_in_names = db.get('not_in_folder_names', [])
            if not_in_names:
                section.append(Spacer(1, 5))
                section.append(Paragraph('<b>Dashboards not in folder:</b>', styles['CXBody']))
                for name in not_in_names:
                    section.append(Paragraph(f"  • {name}", styles['CXSmall']))
            
            # Default dashboard
            if data.get('default_dashboard'):
                section.append(Spacer(1, 5))
                section.append(Paragraph(f"Default Dashboard: <b>{data.get('default_dashboard')}</b>", styles['CXBody']))
        section.append(Spacer(1, 10))
        story.append(KeepTogether(section))
    
    # ══════════════════════════════════════════════════════════════════════════
    # CORA AI
    # ══════════════════════════════════════════════════════════════════════════
    cora = data.get('cora_ai', {})
    cora_error = data.get('cora_ai_error', {})
    if cora or cora_error:
        section = []
        section.append(Paragraph('🤖 Cora AI', styles['CXHeading1']))
        section.append(create_colored_divider())
        el = _section_elapsed('cora_ai')
        if el:
            section.append(el)
        
        if cora.get('error') or cora_error.get('status') == 'FAILED':
            section.append(Paragraph(
                '<font color="#E17055" size="16"><b>✗ CHECK FAILED</b></font><br/>'
                f'<font color="#636E72">{cora.get("error") or cora_error.get("error", "Could not fetch Cora AI settings.")}</font>',
                styles['CXBody']
            ))
        else:
            cora_items = [
                ('DataPrime Query Assistance', cora.get('dataprime_query_assistance_enabled')),
                ('Explain Log', cora.get('explain_log_enabled')),
                ('Knowledge Assistance', cora.get('knowledge_assistance_enabled')),
            ]
            for label, val in cora_items:
                icon = bool_icon(val)
                color = '#00B894' if val else '#E17055'
                section.append(Paragraph(f"{label}: <font color='{color}'>{icon}</font>", styles['CXBody']))
        section.append(Spacer(1, 10))
        
        story.append(KeepTogether(section))
    
    # ══════════════════════════════════════════════════════════════════════════
    # NO-LOG ALERTS (with FULL list of uncovered apps, bullets, bold)
    # MCP checks (no_log_alerts, unparsed_logs, key_fields_normalized, cspm, etc.) use mcp_error when entire mcp_checks fails
    # ══════════════════════════════════════════════════════════════════════════
    mcp_error = data.get('mcp_checks_error', {})
    nla = data.get('no_log_alerts', {})
    if nla or mcp_error:
        section = []
        section.append(Paragraph('🔔 No-Log Alerts', styles['CXHeading1']))
        section.append(create_colored_divider())
        el = _section_elapsed('mcp_checks')
        if el:
            section.append(el)
        
        if nla.get('error') or (mcp_error.get('status') == 'FAILED' and not nla):
            section.append(Paragraph(
                '<font color="#E17055" size="16"><b>✗ CHECK FAILED</b></font><br/>'
                f'<font color="#636E72">{nla.get("error") or mcp_error.get("error", "Could not fetch No-Log Alerts.")}</font>',
                styles['CXBody']
            ))
        else:
            total = nla.get('total', 0)
            triggered = nla.get('triggered_7d', [])
            uncovered = nla.get('apps_without_coverage', [])
            
            section.append(Paragraph(f"Total 'No Log' Alerts: <b>{total}</b>", styles['CXBody']))
            section.append(Paragraph(f"Triggered in Last 7 Days: <b>{len(triggered)}</b>", styles['CXBody']))
            
            if triggered:
                section.append(Paragraph('<font color="#E17055"><b>Triggered alerts:</b></font>', styles['CXSmall']))
                for alert in triggered:
                    section.append(Paragraph(f'<font color="#E17055">  • <b>{alert}</b></font>', styles['CXSmall']))
            
            if uncovered:
                section.append(Spacer(1, 5))
                section.append(Paragraph(f"<b>Apps without 'No Log' alert coverage: {len(uncovered)}</b>", styles['CXBody']))
                
                # Show ALL uncovered apps in 2-column layout with bullets and bold
                rows = split_into_columns_with_bullets(uncovered, 2)
                if rows:
                    uncov_table = Table([[Paragraph(cell, styles['CXSmallBold']) if cell else '' for cell in row] for row in rows],
                                        colWidths=[2.6*inch, 2.6*inch])
                    uncov_table.setStyle(TableStyle([
                        ('PADDING', (0, 0), (-1, -1), 2),
                        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ]))
                    section.append(uncov_table)
        section.append(Spacer(1, 10))
        
        story.append(KeepTogether(section))
    
    story.append(PageBreak())
    
    # ══════════════════════════════════════════════════════════════════════════
    # UNPARSED LOGS (FIXED - using correct field names from output.json)
    # ══════════════════════════════════════════════════════════════════════════
    ul = data.get('unparsed_logs', {})
    if ul or mcp_error:
        section = []
        section.append(Paragraph('📝 Log Parsing Status', styles['CXHeading1']))
        section.append(create_colored_divider())
        el = _section_elapsed('mcp_checks')
        if el:
            section.append(el)
        
        if ul.get('error') or (mcp_error.get('status') == 'FAILED' and not ul):
            section.append(Paragraph(
                '<font color="#E17055" size="16"><b>✗ CHECK FAILED</b></font><br/>'
                f'<font color="#636E72">{ul.get("error") or mcp_error.get("error", "Could not fetch log parsing status.")}</font>',
                styles['CXBody']
            ))
        elif ul.get('all_parsed', False):
            section.append(Paragraph('✓ All logs are parsed as valid JSON', styles['CXSuccess']))
        elif ul:
            total_unparsed = ul.get('total_unparsed', 0)
            total_logs = ul.get('total_logs', 0)
            pct = (total_unparsed / total_logs * 100) if total_logs > 0 else 0
            
            section.append(Paragraph(
                f"Unparsed Logs: <b>{total_unparsed:,}</b> out of <b>{total_logs:,}</b> ({pct:.2f}%)",
                styles['CXBody']
            ))
            
            apps = ul.get('apps', [])
            if apps:
                section.append(Paragraph(f'<br/>Affected Applications: <b>{ul.get("affected_apps", len(apps))}</b>', styles['CXBody']))
                for app in apps:
                    app_name = app.get('application', 'Unknown')
                    # FIXED: use 'count' not 'unparsed_count'
                    count = app.get('count', 0)
                    total = app.get('total_count', 0)
                    app_pct = (count / total * 100) if total > 0 else 0
                    section.append(Paragraph(
                        f"  • <b>{app_name}</b>: {count:,} unparsed out of {total:,} ({app_pct:.1f}%)",
                        styles['CXSmall']
                    ))
        section.append(Spacer(1, 10))
        
        story.append(KeepTogether(section))
    
    # ══════════════════════════════════════════════════════════════════════════
    # DATA NORMALISATION STATUS (standalone check via DataPrime API, last 24h)
    # Apps/subsystems with missing cx_security (excludes cx-metrics, coralogix-alerts)
    # ══════════════════════════════════════════════════════════════════════════
    dn = data.get('data_normalization', {})
    if dn:
        section = []
        section.append(Paragraph('🔄 Data Normalisation Status (cx_security) — Last 24 hours', styles['CXHeading1']))
        section.append(create_colored_divider())
        el = _section_elapsed('data_normalization')
        if el:
            section.append(el)
        
        if dn.get('error'):
            section.append(Paragraph(
                '<font color="#E17055" size="16"><b>✗ CHECK FAILED</b></font><br/>'
                f'<font color="#636E72">{html.escape(str(dn.get("error", "")))}</font>',
                styles['CXBody']
            ))
        elif dn.get('all_normalized'):
            section.append(Paragraph('✓ All data sources have cx_security (last 24h)', styles['CXSuccess']))
        else:
            concern_rows = dn.get('concern_rows', [])
            section.append(Paragraph(
                '<font color="#E17055"><b>Few app(s) with missing cx_security</b></font> — '
                'data normalisation status (last 24h)',
                styles['CXBody']
            ))
            if concern_rows:
                section.append(Spacer(1, 8))
                # Table: Application | Subsystem only
                table_data = [[
                    Paragraph('<b>Application</b>', styles['CXSmallBold']),
                    Paragraph('<b>Subsystem</b>', styles['CXSmallBold']),
                ]]
                for row in concern_rows[:30]:
                    app = html.escape(str(row.get('application', '-') or '-'))
                    sub = html.escape(str(row.get('subsystem', '-') or '-'))
                    table_data.append([
                        Paragraph(app, styles['CXSmall']),
                        Paragraph(sub, styles['CXSmall']),
                    ])
                if len(concern_rows) > 30:
                    table_data.append(['', f'... +{len(concern_rows)-30} more'])
                norm_table = Table(table_data, colWidths=[2.5*inch, 2.5*inch])
                norm_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), SNOWBIT_TEAL),
                    ('TEXTCOLOR', (0, 0), (-1, 0), CORALOGIX_WHITE),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 8),
                    ('PADDING', (0, 0), (-1, -1), 5),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ('GRID', (0, 0), (-1, -1), 0.5, CORALOGIX_LIGHT_GRAY),
                ]))
                section.append(norm_table)
        section.append(Spacer(1, 10))
        
        story.append(KeepTogether(section))
    
    # ══════════════════════════════════════════════════════════════════════════
    # CSPM (with cloud provider logos) — standalone check via DataPrime Query API
    # ══════════════════════════════════════════════════════════════════════════
    cspm = data.get('cspm', {})
    if cspm:
        section = []
        section.append(Paragraph('■ CSPM Integration', styles['CXHeading1']))
        section.append(create_colored_divider())
        el = _section_elapsed('cspm')
        if el:
            section.append(el)
        
        if cspm.get('error'):
            section.append(Paragraph(
                '<font color="#E17055" size="16"><b>✗ CHECK FAILED</b></font><br/>'
                f'<font color="#636E72">{cspm.get("error")}</font>',
                styles['CXBody']
            ))
        elif cspm.get('integrated'):
            accounts = cspm.get('accounts', [])
            providers = cspm.get('providers', [])
            
            section.append(Paragraph(f"Status: <font color='#00B894'>✓ Integrated</font>", styles['CXBody']))
            section.append(Paragraph(f"Total Cloud Accounts: <b>{cspm.get('total_accounts', len(accounts))}</b>", styles['CXBody']))
            
            # Show providers with logos and account IDs
            if providers and isinstance(providers, list):
                section.append(Spacer(1, 8))
                section.append(Paragraph('<b>Cloud Providers:</b>', styles['CXBody']))
                
                provider_rows = []
                for prov in providers:
                    if isinstance(prov, dict):
                        provider_name = prov.get('provider', 'Unknown')
                        count = prov.get('count', 0)
                        prov_accounts = prov.get('accounts', [])
                        
                        # Get logo path from assets
                        logo_path = None
                        logo_size = 20
                        if provider_name.upper() == 'AWS' and os.path.exists(AWS_LOGO):
                            logo_path = AWS_LOGO
                        elif provider_name.upper() == 'AZURE' and os.path.exists(AZURE_LOGO):
                            logo_path = AZURE_LOGO
                        elif provider_name.upper() in ('GCP', 'GOOGLE') and os.path.exists(GCP_LOGO):
                            logo_path = GCP_LOGO
                        
                        # Create row with logo and account IDs
                        if logo_path:
                            logo_img = Image(logo_path, width=logo_size, height=logo_size)
                            provider_rows.append([
                                logo_img,
                                Paragraph(f"<b>{provider_name}</b>", styles['CXBody']),
                                Paragraph(f"{count} account(s)", styles['CXSmall']),
                                Paragraph(f"{', '.join(prov_accounts)}", styles['CXSmall'])
                            ])
                        else:
                            provider_rows.append([
                                '',
                                Paragraph(f"<b>{provider_name}</b>", styles['CXBody']),
                                Paragraph(f"{count} account(s)", styles['CXSmall']),
                                Paragraph(f"{', '.join(prov_accounts)}", styles['CXSmall'])
                            ])
                
                if provider_rows:
                    prov_table = Table(provider_rows, colWidths=[0.4*inch, 0.8*inch, 0.9*inch, 2.8*inch])
                    prov_table.setStyle(TableStyle([
                        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        ('PADDING', (0, 0), (-1, -1), 4),
                    ]))
                    section.append(prov_table)
        else:
            # Not configured
            section.append(Paragraph(
                "Status: <font color='#636E72'>CSPM not configured</font>",
                styles['CXBody']
            ))
        section.append(Spacer(1, 10))
        
        story.append(KeepTogether(section))
    
    # ══════════════════════════════════════════════════════════════════════════
    # INGESTION BLOCK ALERT
    # ══════════════════════════════════════════════════════════════════════════
    iba = data.get('ingestion_block_alert', {})
    if iba or mcp_error:
        section = []
        section.append(Paragraph('🚨 Ingestion Block Alert', styles['CXHeading1']))
        section.append(create_colored_divider())
        el = _section_elapsed('mcp_checks')
        if el:
            section.append(el)
        
        if iba.get('error') or (mcp_error.get('status') == 'FAILED' and not iba):
            section.append(Paragraph(
                '<font color="#E17055" size="16"><b>✗ CHECK FAILED</b></font><br/>'
                f'<font color="#636E72">{iba.get("error") or mcp_error.get("error", "Could not fetch ingestion block alert.")}</font>',
                styles['CXBody']
            ))
        elif iba.get('alert_exists'):
            # Find active alerts only
            alerts = iba.get('alerts', [])
            active_alerts = [a for a in alerts if a.get('enabled', False)]
            
            if active_alerts:
                section.append(Paragraph("Status: <font color='#00B894'>✓ Active</font>", styles['CXBody']))
                # Show only active alert(s)
                for alert in active_alerts:
                    name = alert.get('name', 'Unknown')
                    priority = alert.get('priority', 'N/A')
                    last = alert.get('last_triggered', 'Never')
                    section.append(Paragraph(f"  • <b>{name}</b>", styles['CXSmall']))
                    section.append(Paragraph(f"    Priority: {priority} | Last Triggered: {last}", styles['CXSmall']))
            else:
                # Alert exists but none are active - this is a flag
                section.append(Paragraph("Status: <font color='#E17055'>⚠ EXISTS BUT ALL DISABLED — ACTION REQUIRED</font>", styles['CXBody']))
        else:
            section.append(Paragraph("Status: <font color='#E17055'>✗ NOT FOUND — ACTION REQUIRED</font>", styles['CXBody']))
        section.append(Spacer(1, 10))
        
        story.append(KeepTogether(section))
    
    # ══════════════════════════════════════════════════════════════════════════
    # ALERT HISTORY (Last 24 hrs) — Security Alerts Summary panels
    # ══════════════════════════════════════════════════════════════════════════
    ah = data.get('alert_history', {})
    if ah:
        section = []
        section.append(Paragraph('■ Alert history (Last 24 hrs)', styles['CXHeading1']))
        section.append(create_colored_divider())
        el = _section_elapsed('alert_history')
        if el:
            section.append(el)

        if ah.get('error'):
            section.append(Paragraph(
                '<font color="#E17055" size="16"><b>✗ CHECK FAILED</b></font><br/>'
                f'<font color="#636E72">{ah.get("error")}</font>',
                styles['CXBody']
            ))
        else:
            total = ah.get('total_count', 0)
            p1 = ah.get('p1_count', 0)
            p2 = ah.get('p2_count', 0)
            by_priority = ah.get('by_priority', {})
            line_data = ah.get('line_chart_data', [])

            # Stat boxes: Total, P1, P2 (format like dashboard: 3.78K, 27, 2.35K)
            stat_total = f"{total/1000:.2f}K" if total >= 1000 else str(total)
            stat_p1 = str(p1)
            stat_p2 = f"{p2/1000:.2f}K" if p2 >= 1000 else str(p2)

            stat_table = Table([
                ['Total Alert Count', 'Total P1 Alert Count', 'Total P2 Alert Count'],
                [stat_total, stat_p1, stat_p2],
            ], colWidths=[2*inch, 2*inch, 2*inch], rowHeights=[0.35*inch, 0.5*inch])
            stat_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), SNOWBIT_TEAL),
                ('TEXTCOLOR', (0, 0), (-1, 0), CORALOGIX_WHITE),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('FONTNAME', (0, 1), (-1, 1), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 1), (-1, 1), 14),
                ('TEXTCOLOR', (0, 1), (0, 1), colors.HexColor('#E17055')),
                ('TEXTCOLOR', (1, 1), (1, 1), colors.HexColor('#00B894')),
                ('TEXTCOLOR', (2, 1), (2, 1), colors.HexColor('#E17055')),
                ('PADDING', (0, 0), (-1, 0), 8),
                ('PADDING', (0, 1), (-1, 1), (12, 12, 14, 14)),
                ('GRID', (0, 0), (-1, -1), 0.5, CORALOGIX_LIGHT_GRAY),
            ]))
            section.append(stat_table)
            section.append(Spacer(1, 12))

            # Line chart: Total Alerts Count Over Time
            line_chart = create_alert_history_line_chart(line_data)
            section.append(Paragraph('<b>Total Alerts Count Over Time</b>', styles['CXSmall']))
            section.append(Spacer(1, 4))
            section.append(Image(line_chart, width=5.5*inch, height=2.2*inch))
            section.append(Spacer(1, 12))

            # Bar chart: Alert Count By Priority
            bar_chart = create_alert_history_bar_chart(by_priority)
            section.append(Paragraph('<b>Alert Count By Priority</b>', styles['CXSmall']))
            section.append(Spacer(1, 4))
            section.append(Image(bar_chart, width=5*inch, height=2.2*inch))

        section.append(Spacer(1, 10))
        story.append(KeepTogether(section))

    # ══════════════════════════════════════════════════════════════════════════
    # DISABLED ALERTS & NEVER TRIGGERED ALERTS — via Alert Definitions API
    # ══════════════════════════════════════════════════════════════════════════
    ast = data.get('alerts_status', {})
    if ast:
        section = []
        section.append(Paragraph('■ Disabled Alerts & Never Triggered Alerts', styles['CXHeading1']))
        section.append(create_colored_divider())
        el = _section_elapsed('alerts_status')
        if el:
            section.append(el)

        if ast.get('error'):
            section.append(Paragraph(
                '<font color="#E17055" size="16"><b>✗ CHECK FAILED</b></font><br/>'
                f'<font color="#636E72">{ast.get("error")}</font>',
                styles['CXBody']
            ))
        else:
            disabled = ast.get('disabled_alerts', [])
            never = ast.get('never_triggered_alerts', [])

            def _build_alert_table(items: list, title: str, max_rows: int = 10) -> Table:
                display = items[:max_rows]
                extra = len(items) - max_rows
                rows = [['#', 'Alert Name']]
                for i, name in enumerate(display, 1):
                    rows.append([str(i), _sanitize_for_pdf(name)[:50] + ('...' if len(name) > 50 else '')])
                if extra > 0:
                    rows.append(['', Paragraph(f'<i>+{extra} more</i>', styles['CXSmall'])])
                t = Table(rows, colWidths=[0.5*inch, 4.5*inch])
                t.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), SNOWBIT_TEAL),
                    ('TEXTCOLOR', (0, 0), (-1, 0), CORALOGIX_WHITE),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 8),
                    ('ALIGN', (0, 0), (0, -1), 'CENTER'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('PADDING', (0, 0), (-1, -1), 5),
                    ('GRID', (0, 0), (-1, -1), 0.5, CORALOGIX_LIGHT_GRAY),
                ]))
                return t

            # Disabled Alerts table
            section.append(Paragraph('<b>Disabled Alerts</b>', styles['CXBody']))
            section.append(Paragraph(f"Total: {ast.get('disabled_count', len(disabled))}", styles['CXSmall']))
            section.append(Spacer(1, 4))
            if disabled:
                section.append(_build_alert_table(disabled, 'Disabled'))
            else:
                section.append(Paragraph('<i>None</i>', styles['CXSmall']))
            section.append(Spacer(1, 12))

            # Never Triggered Alerts table
            section.append(Paragraph('<b>Never Triggered Alerts(last 30 days)</b>', styles['CXBody']))
            never_count = ast.get('never_triggered_count', len(never))
            total_defs = ast.get('total_alert_definitions', 0)
            section.append(Paragraph(f"Total: {never_count}  |  Total Alert Definitions: {total_defs}", styles['CXSmall']))
            section.append(Spacer(1, 4))
            if never:
                section.append(_build_alert_table(never, 'Never Triggered'))
            else:
                section.append(Paragraph('<i>None</i>', styles['CXSmall']))

        section.append(Spacer(1, 10))
        story.append(KeepTogether(section))

    # ══════════════════════════════════════════════════════════════════════════
    # TOP 10 NOISY ALERTS (Last 24h) — via Metrics API (cx_alerts)
    # ══════════════════════════════════════════════════════════════════════════
    na = data.get('noisy_alerts', {})
    noisy_list = na.get('noisy_alerts', []) if na else []
    if na:
        section = []
        section.append(Paragraph('■ Top 10 Noisy Alerts (Last 24h)', styles['CXHeading1']))
        section.append(create_colored_divider())
        el = _section_elapsed('noisy_alerts')
        if el:
            section.append(el)
        
        if na.get('error'):
            section.append(Paragraph(
                '<font color="#E17055" size="16"><b>✗ CHECK FAILED</b></font><br/>'
                f'<font color="#636E72">{na.get("error")}</font>',
                styles['CXBody']
            ))
        elif noisy_list:
            time_range = na.get('time_range', 'Last 24 hours')
            total_count = na.get('total_count', sum(a.get('incident_count', 0) for a in noisy_list))
            
            section.append(Paragraph(f"Total Triggers from Top 10: <b>{total_count}</b> ({time_range})", styles['CXBody']))
            section.append(Spacer(1, 8))
            
            # Build table: #, Alert Name, Count, Priority
            alert_data = [['#', 'Alert Name', 'Count', 'Priority']]
            for alert in sorted(noisy_list, key=lambda x: x.get('rank', 99)):
                rank = alert.get('rank', '-')
                alert_name = _sanitize_for_pdf(alert.get('alert_name', 'Unknown'))
                if len(alert_name) > 45:
                    alert_name = alert_name[:42] + '...'
                count = alert.get('incident_count', 0)
                priority = alert.get('priority', 'N/A')
                
                # Color code priority (P1/P2 = higher severity)
                if priority and priority.upper() in ('P1', 'P2'):
                    pri_text = Paragraph(f"<font color='#E17055'><b>{priority}</b></font>", styles['CXSmall'])
                elif priority and priority.upper() in ('P3', 'P4'):
                    pri_text = Paragraph(f"<font color='#FDCB6E'>{priority}</font>", styles['CXSmall'])
                else:
                    pri_text = priority
                
                alert_data.append([str(rank), alert_name, str(count), pri_text])
            
            alert_table = Table(alert_data, colWidths=[0.4*inch, 3.2*inch, 0.6*inch, 0.8*inch])
            alert_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), SNOWBIT_TEAL),
                ('TEXTCOLOR', (0, 0), (-1, 0), CORALOGIX_WHITE),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('ALIGN', (0, 0), (0, -1), 'CENTER'),
                ('ALIGN', (2, 0), (2, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('PADDING', (0, 0), (-1, -1), 5),
                ('GRID', (0, 0), (-1, -1), 0.5, CORALOGIX_LIGHT_GRAY),
            ]))
            section.append(alert_table)
        section.append(Spacer(1, 10))
        story.append(KeepTogether(section))
    
    # ══════════════════════════════════════════════════════════════════════════
    # FAILED CHECKS
    # ══════════════════════════════════════════════════════════════════════════
    failed_checks = [(k, v) for k, v in data.items() if k.endswith('_error') and isinstance(v, dict)]
    if failed_checks:
        section = []
        section.append(Paragraph('❌ Failed Checks', styles['CXHeading1']))
        section.append(create_colored_divider())
        
        for key, value in failed_checks:
            check_name = key.replace('_error', '').replace('_', ' ').title()
            error_msg = value.get('error', 'Unknown error')
            status_code = value.get('status_code')
            
            status_str = f' (HTTP {status_code})' if status_code else ''
            section.append(Paragraph(f"<font color='#E17055'><b>{check_name}</b>{status_str}</font>", styles['CXBody']))
            section.append(Paragraph(f"{error_msg[:150]}", styles['CXSmall']))
            section.append(Spacer(1, 5))
        
        story.append(KeepTogether(section))
    
    # ══════════════════════════════════════════════════════════════════════════
    # FOOTER
    # ══════════════════════════════════════════════════════════════════════════
    story.append(Spacer(1, 20))
    story.append(create_colored_divider())
    footer_time = f"{check_time} IST" if check_time and not str(check_time).upper().endswith(' IST') else (check_time or get_report_time_ist() + ' IST')
    story.append(Paragraph(
        f'<i>Generated by Snowbit AHC Automation | {team_name} | {footer_time}</i>',
        styles['CXSmall']
    ))
    
    # Build PDF
    doc.build(story)
    
    if logger:
        logger.element_info(f'PDF report generated: {pdf_path}')
    
    return pdf_path


# ── Entry Point ─────────────────────────────────────────────────────────────────

def generate_report(output_json_path: str, output_dir: str, logger=None) -> str:
    """Main entry point for PDF report generation."""
    return generate_pdf_report(output_json_path, output_dir, logger)
