"""HTML report rendering for the person-based EDD module."""

import html
import re
from collections import Counter
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout

from .edd_visualizations import (
    _choose_year_step,
    _fig_to_svg,
    _parse_date,
    format_display_date,
    generate_grants_report_html,
)
from .helpers import narrative_to_html, prettify_role, prettify_status
from .insolvency_helpers import is_genuine_insolvency
from .person_edd import (
    PersonEDDReport,
    PersonCompanyRecord,
    aggregate_charges,
    aggregate_co_directors,
)


# Map the cross-analysis risk flags onto the unified company taxonomy
# (Critical / Elevated / Moderate / Info). Critical is reserved — no person
# rule currently escalates that far. Each value is (css_class, badge_label).
_SEVERITY_DISPLAY = {
    "HIGH": ("elevated", "ELEVATED"),
    "MEDIUM": ("moderate", "MODERATE"),
    "LOW": ("info", "INFO"),
    "INFO": ("info", "INFO"),
    "NOT_ASSESSED": ("not-assessed", "NOT ASSESSED"),
}

# Severity ranking for the headline dashboard tally.
_SEVERITY_ORDER = ["Critical", "Elevated", "Moderate", "Info"]
_DISPLAY_TO_LABEL = {
    "critical": "Critical",
    "elevated": "Elevated",
    "moderate": "Moderate",
    "info": "Info",
    "not-assessed": "Info",
}


def _severity_display(flag: Optional[str]) -> Tuple[str, str]:
    """Return (css_class, badge_label) for a raw risk flag."""
    return _SEVERITY_DISPLAY.get((flag or "NOT_ASSESSED").upper(), ("not-assessed", "NOT ASSESSED"))


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

    row_labels = []
    for i, (c, start, end) in enumerate(rows):
        colour = _status_colour(c.company_status)
        ax.barh(i, (end - start).days, left=mdates.date2num(start), height=0.55,
                color=colour, edgecolor="#333", linewidth=0.4, alpha=0.85)
        label = c.company_name or c.company_number
        row_labels.append(label)

    # Company names live on the y-axis so they no longer overprint the bars.
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels(row_labels, fontsize=8)
    ax.set_ylim(-0.6, len(rows) - 0.4)
    ax.xaxis_date()
    # Choose a year-tick interval wide enough to stay readable for people
    # with a long directorship history, where a tick every single year
    # would otherwise overwrite itself.
    span_start = min(start for _, start, _ in rows)
    span_end = max(end for _, _, end in rows)
    year_step = _choose_year_step(span_start, span_end)
    ax.xaxis.set_major_locator(mdates.YearLocator(base=year_step))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    # Title omitted — the HTML <h2> already names this chart.

    legend_patches = [
        mpatches.Patch(color=_STATUS_COLOURS["active"], label="Active"),
        mpatches.Patch(color=_STATUS_COLOURS["dissolved"], label="Dissolved / liquidation"),
        mpatches.Patch(color=_STATUS_COLOURS["administration"], label="Administration / other"),
    ]
    ax.legend(handles=legend_patches, loc="upper left", fontsize=8, frameon=False)

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
    # Draw labels with a semi-transparent white bbox so the central hub label
    # no longer collides illegibly with adjacent co-director labels.
    labels = {n: attrs["label"] for n, attrs in G.nodes(data=True)}
    label_bbox = dict(boxstyle="round,pad=0.15", facecolor="white", alpha=0.7, edgecolor="none")
    nx.draw_networkx_labels(G, pos, labels=labels, ax=ax, font_size=8, bbox=label_bbox)
    ax.axis("off")
    fig.tight_layout()
    return _fig_to_svg(fig)


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------

_CSS = """
body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
       max-width: 1200px; margin: 20px auto; padding: 20px;
       background-color: #f5f5f5; color: #222; }
.container { max-width: 1200px; margin: 0 auto; }
header.report-header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                       color: #fff; padding: 30px; border-radius: 10px; margin-bottom: 30px; }
header.report-header h1 { margin: 0 0 10px 0; }
header.report-header .meta { margin: 5px 0; opacity: 0.9; font-size: 13px; }
.section { background: #fff; padding: 25px; margin-bottom: 20px; border-radius: 8px;
           box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
.section h2 { color: #333; border-bottom: 2px solid #667eea; padding-bottom: 10px; margin-top: 0; }
.section h3 { font-size: 15px; margin: 18px 0 8px 0; color: #555; }
.subsection { margin: 20px 0; }
.subsection h3 { margin: 0 0 10px 0; color: #667eea; font-size: 16px; }
.subject-card { display: flex; gap: 32px; flex-wrap: wrap; }
.subject-card .field { min-width: 140px; }
.subject-card .label { font-size: 11px; text-transform: uppercase; color: #667eea;
                       letter-spacing: 0.5px; font-weight: 600; }
.subject-card .value { font-size: 16px; color: #222; margin-top: 4px; }
.findings { display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 14px; }
.finding { margin: 12px 0; padding: 15px; border-left: 4px solid #ccc;
           background: #f9f9f9; border-radius: 4px; }
/* Unified severity taxonomy — matches the company DD report (master spec).
   Critical is reserved; person findings currently render Elevated/Moderate/Info. */
.finding.critical { border-left-color: #dc3545; background: #fff5f5; }
.finding.elevated { border-left-color: #fd7e14; background: #fff9f5; }
.finding.moderate { border-left-color: #ffc107; background: #fffef5; }
.finding.info { border-left-color: #667eea; background: #f0f4ff; }
.finding.not-assessed { border-left-color: #9e9e9e; background: #f9f9f9; }
.finding .badge { display: inline-block; padding: 3px 10px; border-radius: 3px;
                  font-size: 12px; font-weight: bold; color: #fff; margin-bottom: 6px; }
.finding.critical .badge { background: #dc3545; }
.finding.elevated .badge { background: #fd7e14; }
.finding.moderate .badge { background: #ffc107; color: #333; }
.finding.info .badge { background: #667eea; }
.finding.not-assessed .badge { background: #9e9e9e; }
.finding h4 { margin: 8px 0 6px 0; font-size: 14px; color: #333; }
.finding .narrative { font-size: 13px; line-height: 1.5; color: #333; }
.finding .narrative p { margin: 6px 0; }
.finding .narrative ul { margin: 6px 0; padding-left: 20px; }
.finding .narrative li { margin-bottom: 4px; }
table.report-table, table.companies, table.address-clusters,
table.insolvent, table.phoenix { width: 100%; border-collapse: collapse; font-size: 13px;
                                 margin-top: 10px; }
table.report-table th, table.report-table td,
table.companies th, table.companies td,
table.address-clusters th, table.address-clusters td,
table.insolvent th, table.insolvent td,
table.phoenix th, table.phoenix td { padding: 10px 12px; text-align: left;
                                     border-bottom: 1px solid #eee; vertical-align: top; }
table.report-table th, table.companies th, table.address-clusters th,
table.insolvent th, table.phoenix th { background: #667eea; color: #fff;
                                       font-weight: 600; }
table.companies tr:hover td, table.insolvent tr:hover td,
table.phoenix tr:hover td, table.report-table tr:hover td { background: #f9faff; }
table.companies tr.resigned td { opacity: 0.6; }
table a { color: #667eea; text-decoration: none; }
table a:hover { text-decoration: underline; }
details.collapsible-table > summary { cursor: pointer; color: #667eea; font-weight: 600;
                                      font-size: 13px; margin: 6px 0 4px; }
details.collapsible-table > summary:hover { text-decoration: underline; }
.note { font-size: 12px; color: #666; margin-top: 8px; }
.warnings { background: #fff3e0; border-left: 4px solid #fd7e14; padding: 10px 14px;
            margin: 14px 0; border-radius: 4px; font-size: 13px; }
.errors { background: #ffebee; border-left: 4px solid #dc3545; padding: 10px 14px;
          margin: 14px 0; border-radius: 4px; font-size: 12px; }
/* Grants table reuse */
.grants-summary { display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 20px 0; }
.grants-stat { background: #f0f4ff; padding: 10px 14px; border-radius: 6px; font-size: 13px; }
.grants-stat strong { display: block; font-size: 11px; color: #667eea; text-transform: uppercase;
                      letter-spacing: 0.5px; margin-bottom: 4px; }
.grants-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.grants-table th, .grants-table td { padding: 10px 12px; border-bottom: 1px solid #eee; text-align: left; }
.grants-table th { background: #667eea; color: #fff; font-weight: 600; }
.grants-table tr:hover td { background: #f9faff; }
.grant-detail { background: #fafafa; padding: 10px 14px; margin: 8px 0; border-radius: 4px; }
.grant-detail h4 { margin: 0 0 5px 0; color: #333; }
.grant-meta { font-size: 12px; color: #666; margin: 0 0 10px 0; }
/* Executive summary + headline dashboard (mirrors the company report) */
.executive-summary { font-size: 15px; line-height: 1.6; padding: 20px; background: #f0f4ff;
                     border-radius: 5px; border-left: 4px solid #667eea; }
.dashboard-panel { background: #fff; border: 2px solid #667eea; border-radius: 8px;
                   padding: 18px 20px; margin-bottom: 20px; }
.dash-header { margin-bottom: 12px; }
.dash-total { font-size: 15px; font-weight: 600; color: #333; }
.dash-bars { display: flex; flex-direction: column; gap: 8px; }
.dash-row { display: flex; align-items: center; gap: 12px; }
.dash-label { width: 170px; font-size: 13px; color: #555; }
.dash-bar { flex: 1; height: 20px; background: #eef0f5; border-radius: 3px;
            overflow: hidden; display: flex; }
.dash-seg { height: 100%; display: flex; align-items: center; justify-content: center;
            font-size: 11px; color: #fff; font-weight: 600; min-width: 16px; }
.dash-seg-critical { background: #dc3545; }
.dash-seg-elevated { background: #fd7e14; }
.dash-seg-moderate { background: #ffc107; color: #333; }
.dash-seg-info { background: #667eea; }
.dash-empty { font-size: 11px; color: #999; padding-left: 8px; align-self: center; }
.dash-caption { margin-top: 10px; text-align: right; font-size: 11px; color: #888; font-style: italic; }
.dash-detail { width: 80px; font-size: 12px; color: #777; text-align: right; }
.dash-legend { display: flex; gap: 16px; margin-top: 12px; font-size: 11px; color: #666; }
.dash-legend-item { display: flex; align-items: center; gap: 5px; }
.dash-legend-swatch { width: 12px; height: 12px; border-radius: 2px; display: inline-block; }
.dash-meta { display: flex; gap: 18px; margin-top: 12px; font-size: 12px; color: #666; flex-wrap: wrap; }
/* Consolidated recommendations */
.recommendations ul { margin: 8px 0 0 0; padding-left: 20px; }
.recommendations li { margin-bottom: 8px; font-size: 13px; line-height: 1.5; color: #333; }
/* Save-as-PDF button */
.print-btn { position: fixed; top: 16px; right: 16px; z-index: 1000; background: #667eea;
             color: #fff; border: none; padding: 10px 16px; border-radius: 6px;
             font-size: 13px; cursor: pointer; box-shadow: 0 2px 6px rgba(0,0,0,0.2); }
.print-btn:hover { background: #556bd8; }
@media print {
  body { background: #fff; }
  .print-btn { display: none !important; }
  .section { box-shadow: none; border: 1px solid #ddd; }
  .finding, .dashboard-panel, .chart-container, .grant-detail { break-inside: avoid; }
  table { break-inside: auto; }
  tr, thead { break-inside: avoid; }
  h2, h3 { break-after: avoid; }
  * { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
}
"""


_UK_POSTCODE_RE = re.compile(
    r"\b([A-Z]{1,2}\d[A-Z\d]?)\s*(\d[A-Z]{2})\b",
    re.IGNORECASE,
)


def _proper_case_address(address: Optional[str]) -> str:
    """Title-case an address while keeping any UK postcode uppercased."""
    if not address:
        return ""
    titled = address.title()
    return _UK_POSTCODE_RE.sub(
        lambda m: f"{m.group(1).upper()} {m.group(2).upper()}",
        titled,
    )


def _years_between(later: str, earlier: str) -> Optional[float]:
    """Return ``later - earlier`` in years (float), or None if either is unparseable."""
    a = _parse_date(later)
    b = _parse_date(earlier)
    if not a or not b:
        return None
    return (a - b).days / 365.25


def _format_year_gap(years: Optional[float]) -> str:
    if years is None:
        return "—"
    if years < 0:
        return f"−{abs(years):.1f} yrs"
    return f"{years:.1f} yrs"


def _ch_company_link(company_number: Optional[str]) -> str:
    """Render a company number as an anchor pointing at Companies House.

    The URL uses an 8-character zero-padded form (Companies House requires
    leading zeros), while the visible link text preserves the original
    string so the user can still see whether the number was zero-padded.
    """
    cnum = (company_number or "").strip()
    if not cnum:
        return ""
    padded = cnum.zfill(8)
    return (
        f'<a href="https://find-and-update.company-information.service.gov.uk/'
        f'company/{html.escape(padded)}" target="_blank" rel="noopener">'
        f'{html.escape(cnum)}</a>'
    )


def _ch_charges_link(company_number: Optional[str]) -> str:
    """Like :func:`_ch_company_link` but points at the company's charges page."""
    cnum = (company_number or "").strip()
    if not cnum:
        return ""
    padded = cnum.zfill(8)
    return (
        f'<a href="https://find-and-update.company-information.service.gov.uk/'
        f'company/{html.escape(padded)}/charges" target="_blank" rel="noopener">'
        f'{html.escape(cnum)}</a>'
    )


def _ch_insolvency_link(company_number: Optional[str], text: str = "Yes") -> str:
    """Link to a company's Companies House insolvency tab, with custom text."""
    cnum = (company_number or "").strip()
    if not cnum:
        return html.escape(text)
    padded = cnum.zfill(8)
    return (
        f'<a href="https://find-and-update.company-information.service.gov.uk/'
        f'company/{html.escape(padded)}/insolvency" target="_blank" rel="noopener">'
        f'{html.escape(text)}</a>'
    )


def _finding_card(result) -> str:
    cls, badge = _severity_display(result.risk_flag)
    narrative = narrative_to_html(result.narrative or "")
    title = html.escape(result.title or result.rule_id)
    return f"""
    <div class="finding {cls}">
      <span class="badge">{badge}</span>
      <h4>{title}</h4>
      <div class="narrative">{narrative}</div>
    </div>
    """


def _subject_card(report: PersonEDDReport) -> str:
    s = report.subject
    active = sum(1 for c in report.companies if (c.company_status or "").lower() == "active")
    dissolved = sum(1 for c in report.companies if (c.company_status or "").lower() in {"dissolved", "liquidation"})
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
        <div class="field"><div class="label">Appointments reviewed</div><div class="value">{s.source_appointments}</div></div>
      </div>
      <div class="note" style="margin-top:10px;">
        Canonical keys merged into this subject: <code>{html.escape(', '.join(s.canonical_keys) or 'n/a')}</code>
      </div>
    </section>
    """


def _overdue_cell(overdue: bool, status: Optional[str]) -> str:
    """Render a filing-compliance cell.

    "Overdue" (red) when the profile flags it; "OK" for live companies that are
    not overdue; "—" for dissolved/closed companies where the obligation no
    longer applies (and where the profile usually omits the flag).
    """
    if overdue:
        return '<span style="color:#c62828;font-weight:600;">Overdue</span>'
    if (status or "").lower() == "active":
        return "OK"
    return "—"


_COLLAPSE_THRESHOLD = 10


def _maybe_collapsible(table_html: str, n_rows: int, noun: str) -> str:
    """Wrap a table in a collapsed <details> when it has more than 10 rows.

    Native HTML disclosure — no JavaScript — mirroring the grants section.
    Tables at or below the threshold are returned unchanged (always visible).
    """
    if n_rows > _COLLAPSE_THRESHOLD:
        return (
            '<details class="collapsible-table">'
            f"<summary>Show all {n_rows} {noun}</summary>{table_html}</details>"
        )
    return table_html


def _companies_table(report: PersonEDDReport) -> str:
    rows = []
    # Current directorships first (subject not resigned), then A–Z by name.
    ordered = sorted(
        report.companies,
        key=lambda c: (bool(c.subject_resigned_on), (c.company_name or "").lower()),
    )
    for c in ordered:
        appt = format_display_date(c.subject_appointed_on or "") if c.subject_appointed_on else "—"
        resd = format_display_date(c.subject_resigned_on or "") if c.subject_resigned_on else "—"
        # Slightly fade rows where the subject is no longer a director so live
        # directorships stand out.
        row_class = ' class="resigned"' if c.subject_resigned_on else ""
        rows.append(
            f"<tr{row_class}><td>{html.escape(c.company_name or '')}</td>"
            f"<td>{_ch_company_link(c.company_number)}</td>"
            f"<td>{html.escape(prettify_status(c.company_status))}</td>"
            f"<td>{html.escape(prettify_role(c.subject_role) or '—')}</td>"
            f"<td>{appt}</td><td>{resd}</td>"
            f"<td>{_overdue_cell(c.accounts_overdue, c.company_status)}</td>"
            f"<td>{_overdue_cell(c.confirmation_statement_overdue, c.company_status)}</td>"
            "</tr>"
        )

    total = len(report.companies)
    acc_overdue = sum(1 for c in report.companies if c.accounts_overdue)
    cs_overdue = sum(1 for c in report.companies if c.confirmation_statement_overdue)
    summary = (
        f"<p class=\"note\"><strong>{acc_overdue}</strong> of {total} companies have "
        f"overdue accounts; <strong>{cs_overdue}</strong> have an overdue confirmation "
        "statement.</p>"
    )
    table_html = f"""<table class="companies">
        <thead><tr>
          <th>Company</th><th>Number</th><th>Status</th><th>Role</th>
          <th>Appointed</th><th>Resigned</th>
          <th>Accounts</th><th>Confirmation Stmt</th>
        </tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>"""
    return f"""
    <section class="section">
      <h2>Companies in Scope ({len(report.companies)})</h2>
      {summary}
      {_maybe_collapsible(table_html, total, "companies")}
    </section>
    """


def _phoenix_subsection(report: PersonEDDReport) -> str:
    matches = report.phoenix_matches or []
    if not matches:
        return ""
    rows = []
    for m in matches:
        anchor_iso = m.get("anchor_date") or ""
        inc_iso = m.get("new_incorporated_on") or ""
        anchor_date = format_display_date(anchor_iso) or "—"
        inc_date = format_display_date(inc_iso) or "—"
        ins_type = html.escape(m.get("insolvency_type") or "—")
        gap_text = _format_year_gap(m.get("gap_years"))
        rows.append(
            "<tr>"
            f"<td>{html.escape(m.get('old_company') or '')}</td>"
            f"<td>{_ch_company_link(m.get('old_number'))}</td>"
            f"<td>{html.escape(m.get('new_company') or '')}</td>"
            f"<td>{_ch_company_link(m.get('new_number'))}</td>"
            f"<td>{m.get('similarity', '')}%</td>"
            f"<td>{anchor_date}</td>"
            f"<td>{inc_date}</td>"
            f"<td>{gap_text}</td>"
            f"<td>{ins_type}</td>"
            "</tr>"
        )

    table_html = f"""<table class="phoenix">
        <thead><tr>
          <th>Liquidated Company</th><th>Number</th>
          <th>Phoenix Match</th><th>Number</th>
          <th>Similarity</th>
          <th>Date of Liquidation</th>
          <th>Date of Incorporation</th>
          <th>Time Since Liquidation</th>
          <th>Type of Liquidation</th>
        </tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>"""
    return f"""
    <div class="subsection">
      <h3>Phoenix Companies ({len(matches)})</h3>
      {_maybe_collapsible(table_html, len(matches), "phoenix matches")}
      <p class="note">Distinctive-name match (≥80%) between a dissolved/liquidated company and a live company in the subject's footprint that was incorporated within the phoenix window — from one year before to five years after the older company's dissolution/liquidation. A negative "Time Since Liquidation" means the live company was incorporated before that date. Re-use of a failed company's name is restricted by <em>Section 216 Insolvency Act 1986</em>.</p>
    </div>
    """


def _insolvent_companies_subsection(report: PersonEDDReport) -> str:
    items = report.insolvent_companies or []
    if not items:
        return ""
    rows = []
    genuine_count = 0
    for it in items:
        liq_date = format_display_date(it.get("liquidation_date") or "") or "—"
        # Only formal insolvency proceedings (CVL, administration, etc.) count as
        # "genuine" and link to the CH insolvency page. Strike-offs and bare
        # dissolutions have no insolvency record, so they render as "No".
        is_genuine = is_genuine_insolvency(it.get("is_benign"), it.get("insolvency_type"))
        if is_genuine:
            genuine_count += 1
            genuine_cell = _ch_insolvency_link(it.get("company_number"), "Yes")
        else:
            genuine_cell = "No"
        rows.append(
            "<tr>"
            f"<td>{html.escape(it.get('company_name') or '')}</td>"
            f"<td>{_ch_company_link(it.get('company_number'))}</td>"
            f"<td>{html.escape(it.get('company_status') or '')}</td>"
            f"<td>{liq_date}</td>"
            f"<td>{html.escape(it.get('insolvency_type') or '—')}</td>"
            f"<td>{genuine_cell}</td>"
            "</tr>"
        )
    table_html = f"""<table class="insolvent">
        <thead><tr>
          <th>Company</th><th>Number</th><th>Status</th>
          <th>Date of Liquidation</th><th>Type of Liquidation</th>
          <th>Genuine Insolvency?</th>
        </tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>"""
    return f"""
    <div class="subsection">
      <h3>Dissolved &amp; Insolvent Companies ({len(items)})</h3>
      {_maybe_collapsible(table_html, len(items), "companies")}
      <p class="note">All companies in the subject's footprint with a liquidation, dissolution or administration status — {genuine_count} of {len(items)} are genuine (non-benign) insolvencies, matching the Insolvency footprint count above. The remainder are either benign, solvent wind-downs (e.g. Members' Voluntary Liquidation) or strike-offs/dissolutions, which are not insolvency proceedings and have no Companies House insolvency record. "Yes" links to the company's Companies House insolvency record.</p>
    </div>
    """


def _address_clusters_section(report: PersonEDDReport) -> str:
    if not report.address_clusters:
        return ""
    rows = []
    for addr, cnums in sorted(report.address_clusters.items(), key=lambda kv: -len(kv[1])):
        rows.append(
            f"<tr><td>{html.escape(_proper_case_address(addr))}</td>"
            f"<td>{len(cnums)}</td>"
            f"<td>{html.escape(', '.join(cnums))}</td></tr>"
        )
    table_html = f"""<table class="address-clusters">
        <thead><tr><th>Address</th><th>Count</th><th>Companies</th></tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>"""
    return f"""
    <section class="section">
      <h2>Shared registered addresses</h2>
      {_maybe_collapsible(table_html, len(rows), "addresses")}
    </section>
    """


def _company_links_list(companies: List[Tuple[str, str]], link_fn=_ch_company_link) -> str:
    """Render a list of (name, number) tuples as escaped names with CH links.

    ``link_fn`` selects which Companies House page each number links to
    (defaults to the company profile; pass ``_ch_charges_link`` for charges).
    """
    parts = []
    for name, number in companies:
        link = link_fn(number)
        nm = html.escape(name or number or "")
        parts.append(f"{nm} ({link})" if link else nm)
    return ", ".join(parts)


def _charges_section(report: PersonEDDReport) -> str:
    """Lender-centric summary of charges across the subject's companies."""
    agg = aggregate_charges(report.companies)
    if not agg["total_charges"]:
        return ""

    stats = "".join([
        f'<div class="grants-stat"><strong>Total charges</strong>{agg["total_charges"]}</div>',
        f'<div class="grants-stat"><strong>Outstanding</strong>{agg["outstanding"]}</div>',
        f'<div class="grants-stat"><strong>Satisfied</strong>{agg["satisfied"]}</div>',
        f'<div class="grants-stat"><strong>Companies with charges</strong>{agg["companies_with_charges"]}</div>',
    ])

    rows = []
    for lender in agg["lenders"]:
        rows.append(
            "<tr>"
            f"<td>{html.escape(lender['lender'])}</td>"
            f"<td>{lender['charge_count']}</td>"
            f"<td>{lender['outstanding_count']}</td>"
            f"<td>{_company_links_list(lender['companies'], link_fn=_ch_charges_link)}</td>"
            "</tr>"
        )
    table_html = f"""<table class="report-table">
        <thead><tr>
          <th>Lender</th><th># Charges</th><th>Outstanding</th><th>Companies</th>
        </tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>"""
    return f"""
    <section class="section">
      <h2>Charges (Secured Lending)</h2>
      <div class="grants-summary">{stats}</div>
      {_maybe_collapsible(table_html, len(agg["lenders"]), "lenders")}
      <p class="note">Registered charges (e.g. mortgages and debentures) aggregated across the subject's companies, grouped by secured party. Informational — it indicates secured lending relationships, not a risk in itself. Sorted by number of charges.</p>
    </section>
    """


def _co_director_table(report: PersonEDDReport) -> str:
    """Tabular view of co-directors with attributes, counts and companies."""
    rows_data = aggregate_co_directors(report.companies)
    if not rows_data:
        return ""
    rows = []
    for d in rows_data:
        rows.append(
            "<tr>"
            f"<td>{html.escape(d['name'] or '')}</td>"
            f"<td>{html.escape(d['nationality'] or '—')}</td>"
            f"<td>{html.escape(d['occupation'] or '—')}</td>"
            f"<td>{html.escape(d['country'] or '—')}</td>"
            f"<td>{d['count']}</td>"
            f"<td>{_company_links_list(d['companies'])}</td>"
            "</tr>"
        )
    table_html = f"""<table class="report-table">
        <thead><tr>
          <th>Name</th><th>Nationality</th><th>Occupation</th>
          <th>Country of Residence</th><th>Shared Companies</th><th>Related Companies</th>
        </tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>"""
    return f"""
      <h3>Co-director detail ({len(rows_data)})</h3>
      {_maybe_collapsible(table_html, len(rows_data), "co-directors")}
      <p class="note">Every individual recorded as an officer alongside the subject. "Shared Companies" counts the companies they hold in common with the subject. Sorted by shared-company count, then name.</p>
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

# Findings whose mapped severity is one of these go in the Risk Findings
# section; everything else (Info / clean results) goes in Informational.
_RISK_LABELS = {"Critical", "Elevated", "Moderate"}

# Domain grouping for the headline dashboard bars. P7 (offshore controllers)
# is reserved for the parallel offshore rule; it groups here if present.
# Each tuple is (label, rule_ids, max_risks). Every rule fires at most once, so
# the maximum number of risk findings a domain can ever produce equals the
# number of rules in it — that fixed maximum is the bar's full-scale value, so
# bars stay comparable between reports instead of always filling to 100%.
_DOMAINS = [
    ("Insolvency & continuity", {"P1", "P2"}, 2),
    ("Compliance", {"P8", "P9"}, 2),
    ("Network & structure", {"P3", "P4", "P5", "P7"}, 4),
    ("Funding", {"P6"}, 1),
]


def _result_label(result) -> str:
    """Mapped severity label (Critical/Elevated/Moderate/Info) for a finding."""
    cls, _ = _severity_display(result.risk_flag)
    return _DISPLAY_TO_LABEL.get(cls, "Info")


def _executive_summary_body(report: PersonEDDReport, name: str,
                            counts: Counter, total_concerning: int) -> str:
    n_companies = len(report.companies)
    if total_concerning == 0:
        return (
            f"Based on available data, <strong>{name}</strong> shows no elevated or moderate "
            f"risk indicators across the {n_companies} companies reviewed. Informational "
            "findings are listed below."
        )
    parts = [
        f"Based on available data, <strong>{name}</strong> shows "
        f"<strong>{total_concerning} concerning indicator(s)</strong> across the "
        f"{n_companies} companies reviewed."
    ]
    if counts.get("Critical"):
        parts.append(
            f"<strong>Critical findings ({counts['Critical']}):</strong> severe red flags "
            "requiring immediate attention."
        )
    if counts.get("Elevated"):
        parts.append(
            f"<strong>Elevated risk findings ({counts['Elevated']}):</strong> heightened risk "
            "that should be investigated further before proceeding."
        )
    if counts.get("Moderate"):
        parts.append(
            f"<strong>Moderate concerns ({counts['Moderate']}):</strong> factors that should be "
            "considered and may require additional information or monitoring."
        )
    return "<br><br>".join(parts)


def _summary_and_dashboard(report: PersonEDDReport) -> str:
    name = html.escape(report.subject.display_name)
    counts = Counter(_result_label(r) for r in report.results)
    total_concerning = sum(counts.get(lbl, 0) for lbl in _RISK_LABELS)

    summary_body = _executive_summary_body(report, name, counts, total_concerning)
    header = " &middot; ".join(f"{counts.get(lbl, 0)} {lbl}" for lbl in _SEVERITY_ORDER)

    rows_html = ""
    for domain_name, ids, domain_max in _DOMAINS:
        domain_results = [r for r in report.results if r.rule_id in ids]
        if not domain_results:
            continue
        dcounts = Counter(_result_label(r) for r in domain_results)
        # Only Critical/Elevated/Moderate count as risks; Info findings are
        # informational and are not plotted on the bar (mirrors the EDD report).
        risk_total = sum(dcounts.get(sev, 0) for sev in _RISK_LABELS)
        segs = ""
        for sev in ("Critical", "Elevated", "Moderate"):
            c = dcounts.get(sev, 0)
            if not c:
                continue
            # Scale to the fixed per-domain maximum (capped at 100%) so the fill
            # reflects how many risks were found relative to the most possible,
            # rather than always spanning the whole bar.
            width = min(c / domain_max * 100, 100)
            segs += (
                f'<div class="dash-seg dash-seg-{sev.lower()}" '
                f'style="width:{width:.1f}%">{c}</div>'
            )
        if not segs:
            segs = '<span class="dash-empty">No risks</span>'
        rows_html += (
            f'<div class="dash-row"><span class="dash-label">{html.escape(domain_name)}</span>'
            f'<div class="dash-bar">{segs}</div>'
            f'<span class="dash-detail">{risk_total} of {domain_max}</span></div>'
        )

    legend = "".join(
        f'<div class="dash-legend-item"><span class="dash-legend-swatch" '
        f'style="background:{colour};"></span>{lbl}</div>'
        for lbl, colour in (
            ("Critical", "#dc3545"), ("Elevated", "#fd7e14"),
            ("Moderate", "#ffc107"), ("Info", "#667eea"),
        )
    )

    active = sum(1 for c in report.companies if (c.company_status or "").lower() == "active")
    insolvent = len(report.insolvent_companies or [])
    meta = "".join(f"<span>{m}</span>" for m in (
        f"Companies in scope: {len(report.companies)}",
        f"Active: {active}",
        f"Dissolved / insolvent records: {insolvent}",
    ))

    scale_caption = " &middot; ".join(
        f"{html.escape(name)} /{domain_max}" for name, _ids, domain_max in _DOMAINS
    )

    return f"""
  <section class="section">
    <h2>Executive Summary</h2>
    <div class="executive-summary">{summary_body}</div>
  </section>

  <div class="dashboard-panel">
    <div class="dash-header"><span class="dash-total">{header}</span></div>
    <div class="dash-bars">{rows_html}</div>
    <div class="dash-caption">Fixed scale &mdash; {scale_caption}</div>
    <div class="dash-legend">{legend}</div>
    <div class="dash-meta">{meta}</div>
  </div>
"""


# Severity rank for ordering recommendations (most serious first); anything
# unmapped (e.g. Info) sorts last.
_REC_RANK = {"Critical": 0, "Elevated": 1, "Moderate": 2, "Info": 3}


def _recommendations_section(report: PersonEDDReport) -> str:
    """Consolidated recommendations, each anchored to the finding(s) that raised
    it and ordered by severity.

    A bare list of recommendations ("Request an explanation from management…")
    gives the reader no idea which risk an action addresses. Prefixing each one
    with the finding title — and surfacing the most serious first — keeps the
    advice readable and self-explanatory. Identical recommendations triggered by
    more than one finding are merged under their combined titles.
    """
    grouped: Dict[str, Dict] = {}
    for r in report.results:
        rec = (r.recommendation or "").strip()
        if not rec:
            continue
        title = (r.title or r.rule_id or "").strip()
        label = _result_label(r)
        key = rec.lower()
        entry = grouped.get(key)
        if entry is None:
            entry = {"text": rec, "titles": [], "rank": _REC_RANK.get(label, 3)}
            grouped[key] = entry
        if title and title not in entry["titles"]:
            entry["titles"].append(title)
        entry["rank"] = min(entry["rank"], _REC_RANK.get(label, 3))

    if not grouped:
        return ""

    ordered = sorted(grouped.values(), key=lambda e: (e["rank"], e["titles"]))
    items = "".join(
        f'<li><strong>{html.escape(" / ".join(e["titles"]))}:</strong> '
        f'{html.escape(e["text"])}</li>'
        for e in ordered
    )
    return f"""
  <section class="section recommendations">
    <h2>Recommended actions</h2>
    <p class="note">Each action is tied to the finding that raised it; the most serious appear first.</p>
    <ul>{items}</ul>
  </section>
"""


def generate_person_edd_html(report: PersonEDDReport) -> str:
    """Render the full HTML report for a person-EDD result."""
    s = report.subject
    timeline_svg = _render_directorship_timeline(report)
    codir_svg = _render_codirector_graph(report)

    risk_results = [r for r in report.results if _result_label(r) in _RISK_LABELS]
    info_results = [r for r in report.results if _result_label(r) not in _RISK_LABELS]
    risk_findings_html = "".join(_finding_card(r) for r in risk_results) or \
        '<p class="note">No elevated or moderate risk findings.</p>'
    info_findings_html = "".join(_finding_card(r) for r in info_results)

    insolvent_html = _insolvent_companies_subsection(report)
    phoenix_html = _phoenix_subsection(report)

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
<title>Director Diligence Report — {html.escape(s.display_name)}</title>
<style>{_CSS}</style>
</head><body>
<button class="print-btn" onclick="window.print()">&#128424; Save as PDF</button>
<div class="container">
  <header class="report-header">
    <h1>Director Diligence Report</h1>
    <p>{html.escape(s.display_name)}</p>
    <p class="meta">Generated {timestamp}</p>
  </header>

  {_subject_card(report)}

  {errors_html}

  {_summary_and_dashboard(report)}

  <section class="section">
    <h2>Risk Findings</h2>
    <div class="findings">{risk_findings_html}</div>
    {insolvent_html}
    {phoenix_html}
  </section>

  <section class="section">
    <h2>Informational Findings</h2>
    <div class="findings">{info_findings_html}</div>
  </section>

  <section class="section">
    <h2>Directorship timeline</h2>
    {timeline_svg}
  </section>

  {_companies_table(report)}

  {_charges_section(report)}

  <section class="section">
    <h2>Co-director network</h2>
    {codir_svg}
    <p class="note">Showing up to 25 co-directors who appear on multiple companies with the subject. Node size scales with shared-company count.</p>
    {_co_director_table(report)}
  </section>

  {_address_clusters_section(report)}

  {grants_html}

  {_recommendations_section(report)}

  <footer class="note" style="text-align:center; padding:24px;">
    Data sources: Companies House &middot; 360Giving GrantNav &middot; Director Diligence Report
  </footer>
</div>
</body></html>"""
