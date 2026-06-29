"""HTML report rendering for the person-based EDD module."""

import html
import re
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
    _fig_to_svg,
    _parse_date,
    format_display_date,
    generate_grants_report_html,
)
from .person_edd import (
    PersonEDDReport,
    PersonCompanyRecord,
    aggregate_charges,
    aggregate_co_directors,
)


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
    labels = {n: attrs["label"] for n, attrs in G.nodes(data=True)}
    nx.draw_networkx_labels(G, pos, labels=labels, ax=ax, font_size=8)
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
.finding.high { border-left-color: #dc3545; background: #fff5f5; }
.finding.medium { border-left-color: #fd7e14; background: #fff9f5; }
.finding.low { border-left-color: #6c757d; background: #f9f9f9; }
.finding.info { border-left-color: #667eea; background: #f0f4ff; }
.finding.not-assessed { border-left-color: #9e9e9e; background: #f9f9f9; }
.finding .badge { display: inline-block; padding: 3px 10px; border-radius: 3px;
                  font-size: 12px; font-weight: bold; color: #fff; margin-bottom: 6px; }
.finding.high .badge { background: #dc3545; }
.finding.medium .badge { background: #fd7e14; }
.finding.low .badge { background: #6c757d; }
.finding.info .badge { background: #667eea; }
.finding.not-assessed .badge { background: #9e9e9e; }
.finding h4 { margin: 8px 0 6px 0; font-size: 14px; color: #333; }
.finding .narrative { white-space: pre-line; font-size: 13px; line-height: 1.5; color: #333; }
.finding .recommendation { margin-top: 10px; padding: 10px; background: #fff;
                           border-left: 3px solid #667eea; font-size: 12px;
                           color: #555; font-style: italic; }
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
table.insolvent th, table.phoenix th { background: #f0f4ff; color: #667eea;
                                       font-weight: 600; }
table.companies tr:hover td, table.insolvent tr:hover td,
table.phoenix tr:hover td, table.report-table tr:hover td { background: #f9faff; }
table.companies tr.resigned td { opacity: 0.6; }
table a { color: #667eea; text-decoration: none; }
table a:hover { text-decoration: underline; }
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
.grants-table th { background: #f0f4ff; color: #667eea; font-weight: 600; }
.grants-table tr:hover td { background: #f9faff; }
.grant-detail { background: #fafafa; padding: 10px 14px; margin: 8px 0; border-radius: 4px; }
.grant-detail h4 { margin: 0 0 5px 0; color: #333; }
.grant-meta { font-size: 12px; color: #666; margin: 0 0 10px 0; }
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
    flag = result.risk_flag or "NOT_ASSESSED"
    cls = flag.lower().replace("_", "-")
    narrative = html.escape(result.narrative or "")
    rec_html = ""
    if result.recommendation:
        rec_html = f'<div class="recommendation">{html.escape(result.recommendation)}</div>'
    title = html.escape(result.title or result.rule_id)
    return f"""
    <div class="finding {cls}">
      <span class="badge">{html.escape(flag)}</span>
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
        psc = "Yes" if c.subject_is_psc else ""
        status_html = html.escape(c.company_status or "")
        # Slightly fade rows where the subject is no longer a director so live
        # directorships stand out.
        row_class = ' class="resigned"' if c.subject_resigned_on else ""
        rows.append(
            f"<tr{row_class}><td>{html.escape(c.company_name or '')}</td>"
            f"<td>{_ch_company_link(c.company_number)}</td>"
            f"<td>{status_html}</td>"
            f"<td>{html.escape(c.subject_role or '—')}</td>"
            f"<td>{appt}</td><td>{resd}</td>"
            f"<td>{psc}</td>"
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
    return f"""
    <section class="section">
      <h2>Companies in Scope ({len(report.companies)})</h2>
      {summary}
      <table class="companies">
        <thead><tr>
          <th>Company</th><th>Number</th><th>Status</th><th>Role</th>
          <th>Appointed</th><th>Resigned</th><th>PSC?</th>
          <th>Accounts</th><th>Confirmation Stmt</th>
        </tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
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

    return f"""
    <div class="subsection">
      <h3>Phoenix Companies ({len(matches)})</h3>
      <table class="phoenix">
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
      </table>
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
        is_genuine = not it.get("is_benign")
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
    return f"""
    <div class="subsection">
      <h3>Dissolved &amp; Insolvent Companies ({len(items)})</h3>
      <table class="insolvent">
        <thead><tr>
          <th>Company</th><th>Number</th><th>Status</th>
          <th>Date of Liquidation</th><th>Type of Liquidation</th>
          <th>Genuine Insolvency?</th>
        </tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
      <p class="note">All companies in the subject's footprint with a liquidation, dissolution or administration status — {genuine_count} of {len(items)} are genuine (non-benign) insolvencies, matching the Insolvency footprint count above. The remainder are benign, solvent wind-downs (e.g. Members' Voluntary Liquidation or voluntary strike-off). "Yes" links to the company's Companies House insolvency record.</p>
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
    return f"""
    <section class="section">
      <h2>Shared registered addresses</h2>
      <table class="address-clusters">
        <thead><tr><th>Address</th><th>Count</th><th>Companies</th></tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
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
    return f"""
    <section class="section">
      <h2>Charges (Secured Lending)</h2>
      <div class="grants-summary">{stats}</div>
      <table class="report-table">
        <thead><tr>
          <th>Lender</th><th># Charges</th><th>Outstanding</th><th>Companies</th>
        </tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
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
    return f"""
      <h3>Co-director detail ({len(rows_data)})</h3>
      <table class="report-table">
        <thead><tr>
          <th>Name</th><th>Nationality</th><th>Occupation</th>
          <th>Country of Residence</th><th>Shared Companies</th><th>Related Companies</th>
        </tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
      <p class="note">Every individual recorded as an officer alongside the subject. "Shared Companies" counts the companies they hold in common with the subject. Sorted alphabetically by name.</p>
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

_RISK_RULE_IDS = {"P1", "P2"}


def generate_person_edd_html(report: PersonEDDReport) -> str:
    """Render the full HTML report for a person-EDD result."""
    s = report.subject
    timeline_svg = _render_directorship_timeline(report)
    codir_svg = _render_codirector_graph(report)

    risk_results = [r for r in report.results if r.rule_id in _RISK_RULE_IDS]
    info_results = [r for r in report.results if r.rule_id not in _RISK_RULE_IDS]
    risk_findings_html = "".join(_finding_card(r) for r in risk_results)
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
<div class="container">
  <header class="report-header">
    <h1>Director Diligence Report</h1>
    <p>{html.escape(s.display_name)}</p>
    <p class="meta">Generated {timestamp}</p>
  </header>

  {_subject_card(report)}

  {errors_html}

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
    <p class="note">★ indicates the subject is also recorded as a PSC on that company.</p>
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

  <footer class="note" style="text-align:center; padding:24px;">
    Data sources: Companies House &middot; 360Giving GrantNav &middot; Director Diligence Report
  </footer>
</div>
</body></html>"""
