#!/usr/bin/env python3
"""Legacy: was used to rebuild <main> with tabbed dash zones + health score deck.

The live coralogix-dashboard.html now uses stacked dash-panel sections (no tabs, no overall
score bar) and a different KPI marker position — running this script will likely fail or
overwrite with the old layout. Edit the HTML directly or rewrite markers/templates first.
"""
from pathlib import Path
import re

path = Path(__file__).resolve().parent / "coralogix-dashboard.html"
text = path.read_text()
m = re.search(r"<main class=\"main\">(.*?)</main>", text, re.DOTALL)
if not m:
    raise SystemExit("main not found")
inner = m.group(1)

def between(s, start_pat, end_pat):
    ms = re.search(start_pat, s)
    if not ms:
        raise SystemExit(f"start not found: {start_pat!r}")
    i = ms.start()
    me = re.search(end_pat, s[ms.end() :])
    if not me:
        raise SystemExit(f"end not found after start: {end_pat!r}")
    j = ms.end() + me.start()
    k = ms.end() + me.end()
    block = s[i:j]
    new_s = s[:i] + s[k:]
    return block, new_s

# Page header = everything before KPI block
kpi_idx = inner.find("<!-- KPI Stats -->")
if kpi_idx < 0:
    raise SystemExit("KPI marker")
page_header = inner[:kpi_idx]
rem = inner[kpi_idx:]

markers = [
    ("kpi", r"\s*<!-- KPI Stats -->[\s\S]*?(?=\s*<!-- ══ ANALYTICS)"),
    ("analytics", r"\s*<!-- ══ ANALYTICS \(collapsible\) ══ -->[\s\S]*?(?=\s*<!-- ══ ACTIVE INCIDENTS)"),
    ("incidents", r"\s*<!-- ══ ACTIVE INCIDENTS \(collapsible\) ══ -->[\s\S]*?(?=\s*<!-- ══ ALERTS SUMMARY)"),
    ("alertdefs", r"\s*<!-- ══ ALERTS SUMMARY \(TRIGGERED\) \(collapsible\) ══ -->[\s\S]*?(?=\s*<!-- ══ NEVER TRIGGERED)"),
    ("stale", r"\s*<!-- ══ NEVER TRIGGERED \(30d correlation\) ══ -->[\s\S]*?(?=\s*<!-- ══ SECURITY DATA SOURCES)"),
    ("sources", r"\s*<!-- ══ SECURITY DATA SOURCES \(collapsible\) ══ -->[\s\S]*?(?=\s*<!-- ══ SECURITY LOG INGESTION)"),
    ("logingest", r"\s*<!-- ══ SECURITY LOG INGESTION \(collapsible\) ══ -->[\s\S]*?(?=\s*<!-- ══ QUERY PERFORMANCE)"),
    ("queryperf", r"\s*<!-- ══ QUERY PERFORMANCE[\s\S]*?(?=\s*<!-- ══ AUDIT)"),
    ("audit", r"\s*<!-- ══ AUDIT — ACTIVE USERS[\s\S]*?(?=\s*<!-- ══ ACCOUNT HEALTH CHECKS)"),
    ("health", r"\s*<!-- ══ ACCOUNT HEALTH CHECKS[\s\S]*?\Z"),
]

chunks = {}
for name, pat in markers:
    mm = re.search(pat, rem)
    if not mm:
        raise SystemExit(f"chunk {name}")
    chunks[name] = mm.group(0)
    rem = rem[: mm.start()] + rem[mm.end() :]

if rem.strip():
    raise SystemExit(f"leftover main content: {rem[:200]!r}")

kpi = chunks["kpi"]
# Split data sources card out
m_ds = re.search(
    r"(<div class=\"stat-card blue\"[\s\S]*?</div>)\s*",
    kpi,
)
if not m_ds:
    raise SystemExit("kpi blue card")
kpi_ds_only = (
    "  <!-- KPI — Data sources (integration) -->\n"
    '  <div class="stats-grid" style="grid-template-columns: minmax(220px, 380px); max-width: 420px;">\n'
    "    "
    + m_ds.group(1).strip()
    + "\n  </div>\n"
)
kpi_alerts_only = kpi[: m_ds.start()] + kpi[m_ds.end() :]
# Ensure 4-card grid
kpi_alerts_only = kpi_alerts_only.replace(
    '<div class="stats-grid">',
    '<div class="stats-grid" style="grid-template-columns: repeat(4, 1fr);">',
    1,
)

THEAD = """            <thead>
              <tr>
                <th scope="col">Category</th>
                <th scope="col">Check</th>
                <th scope="col">Status</th>
                <th scope="col">Summary</th>
                <th scope="col">Details</th>
              </tr>
            </thead>"""


def sub_table(tbody_id: str) -> str:
    return f"""        <div class="hc-table-scroll">
          <table class="health-master-table">
{THEAD}
            <tbody id="{tbody_id}"></tbody>
          </table>
        </div>"""


health_overview = """  <!-- ══ Health score (global) ══ -->
  <div id="healthchecks-section">
    <div class="hc-health-shell" id="hcValidationDeck">
      <div class="hc-score-strip">
        <div style="flex:1;min-width:200px;">
          <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:var(--muted);margin-bottom:6px;">Overall account health</div>
          <div style="background:rgba(255,255,255,0.85);border-radius:99px;height:11px;overflow:hidden;border:1px solid rgba(148,163,184,0.35);">
            <div id="hc-score-bar" style="height:100%;border-radius:99px;background:linear-gradient(90deg,#059669,#34d399);transition:width .6s ease;width:0%"></div>
          </div>
        </div>
        <div style="text-align:center;min-width:88px;">
          <div id="hc-score-pct" style="font-size:34px;font-weight:900;color:#059669;letter-spacing:-0.03em;line-height:1;">—</div>
          <div style="font-size:11px;color:var(--muted);font-weight:600;">Score</div>
        </div>
        <div id="hc-summary-chips" style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;"></div>
      </div>
      <div id="hc-ahc-meta" class="hc-ahc-meta" style="display:none;"></div>
    </div>
  </div>
"""

hc_platform = f"""  <div class="collapsible-section" id="hc-platform-checks-section">
    <button class="section-toggle open" onclick="toggleSection(this)" aria-expanded="true">
      <div class="section-toggle-left">
        <div class="section-toggle-icon">🏥</div>
        <div class="section-toggle-text">
          <div class="section-toggle-title">Platform Overview checks</div>
          <div class="section-toggle-meta">SAML · MFA · IP · Archive · Dashboards · Webhooks · TCO · metrics · limits · usage — Query performance &amp; Other checks below</div>
        </div>
      </div>
      <div class="section-toggle-right">
        <div class="section-toggle-badges" id="hcToggleBadges">
          <span class="hc-toggle-kpi hc-kpi-pass" id="hc-pass-badge">— Passed</span>
          <span class="hc-toggle-kpi hc-kpi-fail" id="hc-fail-badge">— Failed</span>
          <span class="hc-toggle-kpi hc-kpi-warn" id="hc-warn-badge">— Warnings</span>
        </div>
        <div class="chevron">▾</div>
      </div>
    </button>
    <div class="section-body">
      <div class="hc-matrix-panel hc-zone-panel">
        <div class="hc-matrix-panel-head">
          <strong>Search all check tables</strong>
          <span id="hcMatrixBlurb">Rows without deep validation use the API snapshot only.</span>
        </div>
        <div class="hc-table-toolbar">
          <input type="search" id="hcTableSearch" placeholder="Filter checks across all sections…" autocomplete="off" aria-label="Filter health checks" />
        </div>
{sub_table("hcTbodyPlatform")}
        <div id="hcEmptyFilter" class="hc-empty-filter" aria-live="polite">No checks match this filter. Try clearing the search.</div>
      </div>
    </div>
  </div>
"""

hc_integration = f"""  <div class="collapsible-section" id="hc-integration-checks-section">
    <button class="section-toggle" onclick="toggleSection(this)" aria-expanded="false">
      <div class="section-toggle-left">
        <div class="section-toggle-icon">🔗</div>
        <div class="section-toggle-text">
          <div class="section-toggle-title">Integration checks</div>
          <div class="section-toggle-meta">Extensions · enrichments · parsing · normalization · CSPM</div>
        </div>
      </div>
      <div class="section-toggle-right"><div class="chevron">▾</div></div>
    </button>
    <div class="section-body">
      <div class="hc-matrix-panel hc-zone-panel">
{sub_table("hcTbodyIntegration")}
      </div>
    </div>
  </div>
"""

hc_alerts = f"""  <div class="collapsible-section" id="hc-alerts-checks-section">
    <button class="section-toggle" onclick="toggleSection(this)" aria-expanded="false">
      <div class="section-toggle-left">
        <div class="section-toggle-icon">📣</div>
        <div class="section-toggle-text">
          <div class="section-toggle-title">Alert &amp; incident hygiene</div>
          <div class="section-toggle-meta">Suppression · noisy rules · disabled · ingestion block</div>
        </div>
      </div>
      <div class="section-toggle-right"><div class="chevron">▾</div></div>
    </button>
    <div class="section-body">
      <div class="hc-matrix-panel hc-zone-panel">
{sub_table("hcTbodyAlerts")}
      </div>
    </div>
  </div>
"""

hc_misc = f"""  <div class="collapsible-section" id="hc-misc-checks-section">
    <button class="section-toggle" onclick="toggleSection(this)" aria-expanded="false">
      <div class="section-toggle-left">
        <div class="section-toggle-icon">📎</div>
        <div class="section-toggle-text">
          <div class="section-toggle-title">Other checks</div>
          <div class="section-toggle-meta">Only unmapped AHC check IDs — usually empty</div>
        </div>
      </div>
      <div class="section-toggle-right"><div class="chevron">▾</div></div>
    </button>
    <div class="section-body">
      <div class="hc-matrix-panel hc-zone-panel">
{sub_table("hcTbodyMisc")}
      </div>
    </div>
  </div>
"""

nav = """  <nav class="dash-nav-zones" aria-label="Dashboard sections">
    <button type="button" class="is-active" data-dash-zone="platform" onclick="dashSetZone('platform')">1 · Platform Overview</button>
    <button type="button" data-dash-zone="integration" onclick="dashSetZone('integration')">2 · Integration details</button>
    <button type="button" data-dash-zone="alerts" onclick="dashSetZone('alerts')">3 · Alert analytics</button>
  </nav>
"""

intro_p = """    <div class="dash-zone-intro"><strong>Platform Overview.</strong> TCO, SAML, MFA, IP access, archive bucket, default dashboard &amp; folders, outbound webhooks (incl. sendlog paths), alert metrics, limits, data usage metrics, Cora AI when validated via AHC, audit account activity, cross-account query performance (DataPrime), unmapped AHC checks, and related views. <em>Team homepage</em> was removed from this view by request.</div>
"""

intro_i = """    <div class="dash-zone-intro"><strong>Integration details.</strong> Log sources integrated (Monday inventory + extensions), parsing signal, enrichments, key-field normalization, CSPM — plus data usage volume by application.</div>
"""

intro_a = """    <div class="dash-zone-intro"><strong>Alert analytics.</strong> KPIs, charts, open incidents, alert definitions, never-triggered correlation, and alert-hygiene checks.</div>
"""

new_main = f"""<main class="main">
{page_header}
{nav}
  <div id="dash-zone-platform" class="dash-zone is-active">
{intro_p}
{chunks["audit"].strip()}
{health_overview}
{hc_platform}
{chunks["queryperf"].strip()}
{hc_misc}
  </div>

  <div id="dash-zone-integration" class="dash-zone">
{intro_i}
{kpi_ds_only.strip()}
{chunks["sources"].strip()}
{chunks["logingest"].strip()}
{hc_integration}
  </div>

  <div id="dash-zone-alerts" class="dash-zone">
{intro_a}
{kpi_alerts_only.strip()}
{chunks["analytics"].strip()}
{chunks["incidents"].strip()}
{chunks["alertdefs"].strip()}
{chunks["stale"].strip()}
{hc_alerts}
  </div>
</main>
"""

new_text = text[: m.start()] + new_main + text[m.end() :]
path.write_text(new_text)
print("OK — main rebuilt:", path)
