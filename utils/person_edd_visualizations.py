"""HTML report rendering for the person-based EDD module."""

import html
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout

from .edd_visualizations import (
    _fig_to_svg,
    _parse_date,
    format_display_date,
    generate_grants_report_html,
)
from .person_edd import PersonEDDReport, PersonCompanyRecord


# Status colour palette — mirrors the conventions used in edd_visualizations.
_STATUS_COLOURS = {
    "active": "#2e7d32",
    "dissolved": "#c62828",
    "liquidation": "#c62828",
    "in administration": "#ef6c00",
    "administration": "#ef6c00",
    "voluntary-arrangement": "#ef6c00",
    "receivership": "#ef6c00",
    "open": "#1976d2",
}

_SEVERITY_COLOURS = {
    "HIGH": "#c62828",
    "MEDIUM": "#ef6c00",
    "LOW": "#2e7d32",
    "NOT_ASSESSED": "#9e9e9e",
}


def _status_colour(status: Optional[str]) -> str:
    if not status:
        return "#9e9e9e"
    return _STATUS_COLOURS.get(status.lower(), "#1976d2")


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------

def _render_directorship_timeline(report: PersonEDDReport) -> str:
    """Horizontal timeline: one row per company, x-axis is time."""
    rows = []
    for c in report.companies:
        start = _parse_date(c.subject_appointed_on) or _parse_date(c.incorporated_on)
        end = _parse_date(c.subject_resigned_on) or _parse_date(c.dissolved_on) or datetime.now()
        if not start:
            continue
        rows.append((c, start, end))

    if not rows:
        fig, ax = plt.subplots(figsize=(10, 2))
        ax.text(0.5, 0.5, "No appointment dates available for timeline.",
                ha="center", va="center", transform=ax.transAxes, fontsize=12, color="grey")
        ax.axis("off")
        return _fig_to_svg(fig)

    rows.sort(key=lambda r: r[1])
    height = max(3, 0.4 * len(rows) + 1)
    fig, ax = plt.subplots(figsize=(12, height))

    for i, (c, start, end) in enumerate(rows):
        colour = _status_colour(c.company_status)
        ax.barh(i, (end - start).days, left=mdates.date2num(start), height=0.55,
                color=colour, edgecolor="#333", linewidth=0.4, alpha=0.85)
        label = c.company_name or c.company_number
        if c.subject_is_psc:
            label += "  ★"
        ax.text(mdates.date2num(start), i, " " + label, va="center", ha="left",
                fontsize=8, color="#222")

    ax.set_yticks([])
    ax.set_ylim(-0.6, len(rows) - 0.4)
    ax.xaxis_date()
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.set_title("Directorship timeline", fontsize=11, loc="left", pad=10)

    legend_patches = [
        mpatches.Patch(color=_STATUS_COLOURS["active"], label="Active"),
        mpatches.Patch(color=_STATUS_COLOURS["dissolved"], label="Dissolved / liquidation"),
        mpatches.Patch(color=_STATUS_COLOURS["administration"], label="Administration / other"),
    ]
    ax.legend(handles=legend_patches, loc="lower right", fontsize=8, frameon=False)

    fig.tight_layout()
    return _fig_to_svg(fig)


def _render_codirector_graph(report: PersonEDDReport, top_n: int = 25) -> str:
    """Subject-centred radial graph of repeat co-directors."""
    counts = report.co_director_counts
    if not counts:
        fig, ax = plt.subplots(figsize=(8, 2))
        ax.text(0.5, 0.5, "No co-directors identified.",
                ha="center", va="center", transform=ax.transAxes, fontsize=12, color="grey")
        ax.axis("off")
        return _fig_to_svg(fig)

    top = counts.most_common(top_n)
    if not top:
        fig, ax = plt.subplots(figsize=(8, 2))
        ax.text(0.5, 0.5, "No co-directors identified.",
                ha="center", va="center", transform=ax.transAxes, fontsize=12, color="grey")
        ax.axis("off")
        return _fig_to_svg(fig)

    G = nx.Graph()
    subject_label = report.subject.display_name or "Subject"
    G.add_node("__subject__", label=subject_label, node_type="subject")
    for name, count in top:
        node_id = f"co::{name}"
        G.add_node(node_id, label=f"{name}\n({count} shared)", node_type="codirector")
        G.add_edge("__subject__", node_id, weight=count)

    G.graph["graph"] = {"overlap": "false", "splines": "true"}
    try:
        pos = graphviz_layout(G, prog="twopi", root="__subject__")
    except Exception:
        pos = nx.spring_layout(G, seed=42)

    fig, ax = plt.subplots(figsize=(11, 9))
    max_count = max(c for _, c in top)
    node_sizes = []
    node_colours = []
    for n, attrs in G.nodes(data=True):
        if attrs.get("node_type") == "subject":
            node_sizes.append(2400)
            node_colours.append("#1565c0")
        else:
            count = next((c for name, c in top if f"co::{name}" == n), 1)
            node_sizes.append(600 + (count / max_count) * 1500)
            node_colours.append("#ffb74d")

    nx.draw_networkx_edges(G, pos, ax=ax, alpha=0.4, edge_color="#666")
    nx.draw_networkx_nodes(G, pos, ax=ax, node_size=node_sizes, node_color=node_colours,
                           edgecolors="#333", linewidths=0.6)
    labels = {n: attrs["label"] for n, attrs in G.nodes(data=True)}
    nx.draw_networkx_labels(G, pos, labels=labels, ax=ax, font_size=8)
    ax.axis("off")
    fig.tight_layout()
    return _fig_to_svg(fig)


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------

_CSS = """
:root { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; }
body { margin: 0; background: #f4f6f8; color: #222; }
.container { max-width: 1200px; margin: 0 auto; padding: 24px; }
header.report-header { background: #1e3a5f; color: #fff; padding: 24px; border-radius: 8px; }
header.report-header h1 { margin: 0 0 6px 0; font-size: 26px; }
header.report-header .meta { font-size: 13px; opacity: 0.85; }
.section { background: #fff; border-radius: 8px; padding: 20px 24px; margin: 18px 0;
           box-shadow: 0 1px 3px rgba(0,0,0,0.06); }
.section h2 { margin: 0 0 12px 0; font-size: 18px; color: #1e3a5f; border-bottom: 1px solid #eee; padding-bottom: 6px; }
.section h3 { font-size: 14px; margin: 16px 0 6px 0; }
.subject-card { display: flex; gap: 32px; flex-wrap: wrap; }
.subject-card .field { min-width: 140px; }
.subject-card .label { font-size: 11px; text-transform: uppercase; color: #666; letter-spacing: 0.5px; }
.subject-card .value { font-size: 16px; color: #222; margin-top: 2px; }
.findings { display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 14px; }
.finding { border-left: 4px solid #9e9e9e; background: #fafafa; padding: 14px 16px; border-radius: 4px; }
.finding.high { border-left-color: #c62828; }
.finding.medium { border-left-color: #ef6c00; }
.finding.low { border-left-color: #2e7d32; }
.finding .badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px;
                  font-weight: 600; color: #fff; }
.finding.high .badge { background: #c62828; }
.finding.medium .badge { background: #ef6c00; }
.finding.low .badge { background: #2e7d32; }
.finding .badge.not-assessed { background: #9e9e9e; }
.finding h4 { margin: 8px 0 4px 0; font-size: 14px; }
.finding .narrative { white-space: pre-line; font-size: 13px; line-height: 1.45; }
.finding .recommendation { margin-top: 10px; font-size: 12px; color: #555; font-style: italic; }
table.companies { width: 100%; border-collapse: collapse; font-size: 13px; }
table.companies th, table.companies td { padding: 6px 10px; text-align: left; border-bottom: 1px solid #eee; }
table.companies th { background: #f4f6f8; font-weight: 600; }
table.address-clusters { width: 100%; border-collapse: collapse; font-size: 13px; }
table.address-clusters th, table.address-clusters td { padding: 6px 10px; text-align: left; border-bottom: 1px solid #eee; vertical-align: top; }
table.address-clusters th { background: #f4f6f8; font-weight: 600; }
.note { font-size: 12px; color: #666; }
.warnings { background: #fff3e0; border-left: 4px solid #ef6c00; padding: 10px 14px; margin: 14px 0;
            border-radius: 4px; font-size: 13px; }
.errors { background: #ffebee; border-left: 4px solid #c62828; padding: 10px 14px; margin: 14px 0;
          border-radius: 4px; font-size: 12px; }
/* Reuse the grants table styling from the company EDD report. */
.grants-summary { display: flex; gap: 14px; margin: 12px 0; flex-wrap: wrap; }
.grants-stat { background: #f0f4f8; padding: 8px 14px; border-radius: 6px; font-size: 13px; }
.grants-stat strong { display: block; font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 0.5px; }
.grants-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.grants-table th, .grants-table td { padding: 6px 10px; border-bottom: 1px solid #eee; text-align: left; }
.grants-table th { background: #f4f6f8; font-weight: 600; }
.grants-table tr:hover td { background: #f5f7ff; }
.grant-detail { background: #fafafa; padding: 10px 14px; margin: 8px 0; border-radius: 4px; }
.grant-detail h4 { margin: 0 0 5px 0; color: #333; }
.grant-meta { font-size: 12px; color: #666; margin: 0 0 10px 0; }
"""


def _finding_card(result) -> str:
    flag = result.risk_flag or "NOT_ASSESSED"
    cls = flag.lower().replace("_", "-")
    badge_class = "badge not-assessed" if flag == "NOT_ASSESSED" else "badge"
    narrative = html.escape(result.narrative or "")
    rec_html = ""
    if result.recommendation:
        rec_html = f'<div class="recommendation">{html.escape(result.recommendation)}</div>'
    title = html.escape(result.title or result.rule_id)
    return f"""
    <div class="finding {cls}">
      <span class="{badge_class}">{html.escape(flag)}</span>
      <h4>{result.rule_id} — {title}</h4>
      <div class="narrative">{narrative}</div>
      {rec_html}
    </div>
    """


def _subject_card(report: PersonEDDReport) -> str:
    s = report.subject
    active = sum(1 for c in report.companies if (c.company_status or "").lower() == "active")
    dissolved = sum(1 for c in report.companies if (c.company_status or "").lower() in {"dissolved", "liquidation"})
    psc_count = sum(1 for c in report.companies if c.subject_is_psc)
    dob_text = "—"
    if s.dob_year:
        if s.dob_month:
            dob_text = f"{s.dob_month:02d}/{s.dob_year}"
        else:
            dob_text = str(s.dob_year)
    return f"""
    <section class="section">
      <h2>Subject</h2>
      <div class="subject-card">
        <div class="field"><div class="label">Name</div><div class="value">{html.escape(s.display_name)}</div></div>
        <div class="field"><div class="label">DOB (month/year)</div><div class="value">{dob_text}</div></div>
        <div class="field"><div class="label">Total companies</div><div class="value">{len(report.companies)}</div></div>
        <div class="field"><div class="label">Active</div><div class="value">{active}</div></div>
        <div class="field"><div class="label">Dissolved / liquidated</div><div class="value">{dissolved}</div></div>
        <div class="field"><div class="label">Also recorded as PSC</div><div class="value">{psc_count}</div></div>
        <div class="field"><div class="label">Appointments reviewed</div><div class="value">{s.source_appointments}</div></div>
      </div>
      <div class="note" style="margin-top:10px;">
        Canonical keys merged into this subject: <code>{html.escape(', '.join(s.canonical_keys) or 'n/a')}</code>
      </div>
    </section>
    """


def _companies_table(report: PersonEDDReport) -> str:
    rows = []
    for c in sorted(report.companies, key=lambda c: (c.company_status or "", c.company_name or "")):
        appt = format_display_date(c.subject_appointed_on or "")
        resd = format_display_date(c.subject_resigned_on or "") if c.subject_resigned_on else "—"
        psc = "Yes" if c.subject_is_psc else ""
        status_html = html.escape(c.company_status or "")
        rows.append(
            f"<tr><td>{html.escape(c.company_name or '')}</td>"
            f"<td>{html.escape(c.company_number)}</td>"
            f"<td>{status_html}</td>"
            f"<td>{html.escape(c.subject_role or '')}</td>"
            f"<td>{appt}</td><td>{resd}</td>"
            f"<td>{psc}</td></tr>"
        )
    return f"""
    <section class="section">
      <h2>Companies in scope ({len(report.companies)})</h2>
      <table class="companies">
        <thead><tr>
          <th>Company</th><th>Number</th><th>Status</th><th>Role</th>
          <th>Appointed</th><th>Resigned</th><th>PSC?</th>
        </tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
    </section>
    """


def _address_clusters_section(report: PersonEDDReport) -> str:
    if not report.address_clusters:
        return ""
    rows = []
    for addr, cnums in sorted(report.address_clusters.items(), key=lambda kv: -len(kv[1])):
        rows.append(
            f"<tr><td>{html.escape(addr)}</td><td>{len(cnums)}</td>"
            f"<td>{html.escape(', '.join(cnums))}</td></tr>"
        )
    return f"""
    <section class="section">
      <h2>Shared registered addresses</h2>
      <table class="address-clusters">
        <thead><tr><th>Address (cleaned)</th><th>Count</th><th>Companies</th></tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
      <p class="note">Shared addresses are often a registered agent (accountant or formation service) rather than a genuine operating link. Use with judgement.</p>
    </section>
    """


def _aggregate_grants(report: PersonEDDReport) -> List[Dict]:
    grants = []
    for c in report.companies:
        for g in c.grants:
            # Decorate with the recipient company for context.
            g2 = dict(g)
            g2.setdefault("_recipient_company_name", c.company_name)
            g2.setdefault("_recipient_company_number", c.company_number)
            grants.append(g2)
    return grants


# ---------------------------------------------------------------------------
# Top-level
# ---------------------------------------------------------------------------

def generate_person_edd_html(report: PersonEDDReport) -> str:
    """Render the full HTML report for a person-EDD result."""
    s = report.subject
    timeline_svg = _render_directorship_timeline(report)
    codir_svg = _render_codirector_graph(report)
    findings_html = "".join(_finding_card(r) for r in report.results)

    grants_html = generate_grants_report_html(_aggregate_grants(report))

    errors_html = ""
    if report.fetch_errors:
        items = "".join(
            f"<li>{html.escape(cnum)}: {html.escape(err or 'unknown error')}</li>"
            for cnum, err in report.fetch_errors
        )
        errors_html = f'<div class="errors"><strong>{len(report.fetch_errors)} companies could not be fetched:</strong><ul>{items}</ul></div>'

    timestamp = datetime.now().strftime("%d %B %Y, %H:%M")
    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
<title>Person EDD — {html.escape(s.display_name)}</title>
<style>{_CSS}</style>
</head><body>
<div class="container">
  <header class="report-header">
    <h1>Person-Based Enhanced Due Diligence</h1>
    <div class="meta">{html.escape(s.display_name)} &middot; Generated {timestamp}</div>
  </header>

  {_subject_card(report)}

  {errors_html}

  <section class="section">
    <h2>Risk synthesis</h2>
    <div class="findings">{findings_html}</div>
  </section>

  <section class="section">
    <h2>Directorship timeline</h2>
    {timeline_svg}
    <p class="note">★ indicates the subject is also recorded as a PSC on that company.</p>
  </section>

  {_companies_table(report)}

  <section class="section">
    <h2>Co-director network</h2>
    {codir_svg}
    <p class="note">Showing up to 25 co-directors who appear on multiple companies with the subject. Node size scales with shared-company count.</p>
  </section>

  {_address_clusters_section(report)}

  {grants_html}

  <footer class="note" style="text-align:center; padding:24px;">
    Data sources: Companies House &middot; 360Giving GrantNav &middot; v1 person-EDD synthesis
  </footer>
</div>
</body></html>"""
