# multitool/utils/edd_visualizations.py
"""Visualization and data processing utilities for the Enhanced Due Diligence module."""

import base64
import html
import threading
import urllib.parse
from collections import deque
from io import BytesIO
from datetime import datetime, timedelta
from typing import Optional
from dateutil.relativedelta import relativedelta

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout

from ..api.companies_house import ch_get_data
from ..api.grantnav import grantnav_get_data
from ..constants import FILING_TYPE_CATEGORIES, GRANT_DATA_FIELDS, GRANTNAV_API_BASE_URL
from ..utils.helpers import log_message, clean_company_number


def _fig_to_base64(fig) -> str:
    """Convert a matplotlib figure to a base64-encoded PNG string."""
    buffer = BytesIO()
    fig.savefig(buffer, format='png', dpi=120, bbox_inches='tight')
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.getvalue()).decode()
    plt.close(fig)
    return image_base64


def _fig_to_svg(fig) -> str:
    """Convert a matplotlib figure to an inline SVG string.

    SVG output scales without quality loss, allowing the user to zoom
    in on detail (e.g. small text on a timeline) without pixelation.
    """
    buffer = BytesIO()
    fig.savefig(buffer, format='svg', bbox_inches='tight')
    buffer.seek(0)
    svg_str = buffer.getvalue().decode('utf-8')
    plt.close(fig)
    return svg_str


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse a date string, handling YYYY-MM-DD and ISO 8601 formats."""
    if not date_str or not date_str.strip():
        return None
    s = date_str.strip()
    # Try plain YYYY-MM-DD first (most common from Companies House)
    try:
        return datetime.strptime(s[:10], '%Y-%m-%d')
    except (ValueError, TypeError):
        pass
    # Try common display formats (e.g. "16 Mar 2021", "16 March 2021")
    for fmt in ('%d %b %Y', '%d %B %Y'):
        try:
            return datetime.strptime(s, fmt)
        except (ValueError, TypeError):
            pass
    return None


def _strftime_day(d: datetime) -> str:
    """Format a datetime as 'd Month YYYY', e.g. '5 March 2021'.

    Avoids platform-specific format codes (%-d on Unix, %#d on Windows)
    by stripping the leading zero manually.
    """
    return f"{d.day} {d.strftime('%B %Y')}"


def format_display_date(date_str: str) -> str:
    """Format a date string to 'd Month YYYY' (e.g. '16 March 2021') for display.

    Returns the original string if parsing fails.
    """
    d = _parse_date(date_str)
    if d:
        return _strftime_day(d)
    return date_str or 'N/A'


def _get_nested_value(data: dict, key_path: str):
    """Get a value from a nested dict using underscore-separated path."""
    keys = key_path.split('_')
    current = data
    # Try progressively joining keys to handle ambiguous splits
    i = 0
    while i < len(keys):
        found = False
        # Try joining from current position forward
        for j in range(len(keys), i, -1):
            candidate_key = '_'.join(keys[i:j])
            if isinstance(current, dict) and candidate_key in current:
                current = current[candidate_key]
                i = j
                found = True
                break
            elif isinstance(current, list) and len(current) > 0:
                current = current[0]
                if isinstance(current, dict) and candidate_key in current:
                    current = current[candidate_key]
                    i = j
                    found = True
                    break
        if not found:
            return None
    return current


# ---------------------------------------------------------------------------
# Feature 1: Company Timeline
# ---------------------------------------------------------------------------

def generate_company_timeline(
    profile: dict,
    officers: dict,
    pscs: dict,
    filing_history: dict,
    grants_data: Optional[list] = None,
    figsize: tuple = (16, 10),
) -> str:
    """
    Generate a combined timeline chart showing company events and periods.
    Returns a base64-encoded PNG string.
    """
    now = datetime.now()
    inc_date = _parse_date(profile.get('date_of_creation', ''))
    timeline_start = inc_date if inc_date else now - timedelta(days=365 * 10)

    # ---- Collect data ----
    # Directors
    director_bars = []
    for officer in (officers or {}).get('items', []):
        name = officer.get('name', 'Unknown')
        role = officer.get('officer_role', '')
        start = _parse_date(officer.get('appointed_on', ''))
        end = _parse_date(officer.get('resigned_on', ''))
        if start:
            director_bars.append({
                'name': f"{name} ({role})" if role else name,
                'start': start,
                'end': end or now,
                'active': end is None,
            })

    # PSCs
    psc_bars = []
    for psc in (pscs or {}).get('items', []):
        name = psc.get('name', 'Unknown')
        start = _parse_date(psc.get('notified_on', ''))
        end = _parse_date(psc.get('ceased_on', ''))
        if start:
            psc_bars.append({
                'name': name,
                'start': start,
                'end': end or now,
                'active': end is None,
            })

    # Filing events categorised
    filing_events = {'Accounts Filed': [], 'Confirmation Statement': [],
                     'Notices & Events': [], 'Other Filings': []}

    # Find earliest AA action_date for first-year detection
    all_items = (filing_history or {}).get('items', [])
    aa_action_dates = []
    for filing in all_items:
        if filing.get('type', '').startswith('AA') and filing.get('action_date'):
            ad = _parse_date(filing['action_date'])
            if ad:
                aa_action_dates.append(ad)
    earliest_aa_action_date = min(aa_action_dates) if aa_action_dates else None

    for filing in all_items:
        f_date = _parse_date(filing.get('date', ''))
        f_type = filing.get('type', '')
        f_action_date = _parse_date(filing.get('action_date', ''))
        if not f_date:
            continue

        category = None
        for prefix, cat in FILING_TYPE_CATEGORIES.items():
            if f_type.startswith(prefix):
                category = cat
                break

        if category in ('Accounts Filed',):
            # Determine if filing was late using statutory deadlines:
            # First-year: 21 months from incorporation; subsequent: 9 months from made-up-to date
            is_late = False
            if f_action_date and f_date:
                is_first_year = (
                    inc_date is not None
                    and earliest_aa_action_date is not None
                    and f_action_date == earliest_aa_action_date
                )
                if is_first_year and inc_date:
                    deadline = inc_date + relativedelta(months=21)
                else:
                    deadline = f_action_date + relativedelta(months=9)
                is_late = f_date > deadline
            filing_events['Accounts Filed'].append({
                'date': f_date, 'type': f_type, 'late': is_late,
                'description': filing.get('description', ''),
            })
        elif category in ('Confirmation Statement',):
            filing_events['Confirmation Statement'].append({
                'date': f_date, 'type': f_type,
            })
        elif category in ('First Gazette (Strike-off)', 'Second Gazette (Strike-off)',
                          'Liquidation', 'Administration', 'Change of Name',
                          'Striking Off Application', 'Restoration to Register',
                          'Voluntary Arrangement', 'Receiver/Manager Appointed'):
            filing_events['Notices & Events'].append({
                'date': f_date, 'type': f_type, 'category': category,
                'description': filing.get('description', ''),
            })
        elif category in ('Charge Registered', 'Charge Satisfied', 'Allotment of Shares',
                          'Special Resolution', 'Incorporation'):
            filing_events['Other Filings'].append({
                'date': f_date, 'type': f_type, 'category': category or f_type,
            })

    # Grants
    grant_events = []
    if grants_data:
        for grant in grants_data:
            award_date_str = grant.get('awardDate', '')
            award_date = _parse_date(award_date_str)
            if award_date:
                amount = grant.get('amountAwarded', 0)
                try:
                    amount = float(amount)
                except (ValueError, TypeError):
                    amount = 0
                grant_events.append({
                    'date': award_date,
                    'amount': amount,
                    'title': grant.get('title', ''),
                    'funder': _get_nested_value(grant, 'fundingOrganization_name') or '',
                })

    # ---- Build chart ----
    # Calculate y-positions for each row
    rows = []
    row_labels = []
    row_colors = []

    # Section: Directors
    for d in director_bars:
        rows.append(('bar', d))
        row_labels.append(d['name'][:40])
        row_colors.append('#4A90D9' if d['active'] else '#A0C4E8')

    # Section: PSCs
    for p in psc_bars:
        rows.append(('bar', p))
        row_labels.append(p['name'][:40])
        row_colors.append('#5CB85C' if p['active'] else '#B5E6B5')

    # Section: Filing event rows (one row per category that has data)
    event_row_map = {}
    for cat_name, events in filing_events.items():
        if events:
            event_row_map[cat_name] = len(rows)
            rows.append(('events', cat_name))
            row_labels.append(cat_name)
            row_colors.append('#888888')

    # Section: Grants
    if grant_events:
        grant_row_idx = len(rows)
        rows.append(('events', 'Grants Received'))
        row_labels.append('Grants Received')
        row_colors.append('#F0AD4E')

    if not rows:
        # Nothing to plot
        fig, ax = plt.subplots(figsize=(8, 2))
        ax.text(0.5, 0.5, 'Insufficient data for timeline', ha='center', va='center',
                transform=ax.transAxes, fontsize=12, color='grey')
        ax.axis('off')
        return _fig_to_svg(fig)

    n_rows = len(rows)
    fig_height = max(4, min(figsize[1], 2 + n_rows * 0.55))
    fig, ax = plt.subplots(figsize=(figsize[0], fig_height))

    y_positions = list(range(n_rows))

    # Draw bars for directors and PSCs
    for i, (row_type, data) in enumerate(rows):
        if row_type == 'bar':
            duration = (data['end'] - data['start']).days
            ax.barh(i, duration, left=mdates.date2num(data['start']),
                    height=0.6, color=row_colors[i], edgecolor='#333333',
                    linewidth=0.5, alpha=0.85)

    # Draw filing events as scatter points
    for cat_name, row_idx in event_row_map.items():
        events = filing_events[cat_name]
        if cat_name == 'Accounts Filed':
            for evt in events:
                color = '#DC3545' if evt.get('late') else '#333333'
                marker = 'o'
                ax.scatter(mdates.date2num(evt['date']), row_idx,
                           c=color, marker=marker, s=30, zorder=5, edgecolors='black',
                           linewidths=0.5)
        elif cat_name == 'Confirmation Statement':
            for evt in events:
                ax.scatter(mdates.date2num(evt['date']), row_idx,
                           c='#667EEA', marker='D', s=25, zorder=5, edgecolors='black',
                           linewidths=0.5)
        elif cat_name == 'Notices & Events':
            color_map = {
                'First Gazette (Strike-off)': '#DC3545',
                'Second Gazette (Strike-off)': '#8B0000',
                'Liquidation': '#DC3545',
                'Administration': '#FD7E14',
                'Change of Name': '#6C757D',
                'Striking Off Application': '#DC3545',
                'Restoration to Register': '#28A745',
                'Voluntary Arrangement': '#FD7E14',
                'Receiver/Manager Appointed': '#DC3545',
            }
            # Only annotate if not too cluttered
            annotate = len(events) <= 15
            for evt in events:
                c = color_map.get(evt.get('category', ''), '#DC3545')
                ax.scatter(mdates.date2num(evt['date']), row_idx,
                           c=c, marker='^', s=50, zorder=5, edgecolors='black',
                           linewidths=0.5)
                if annotate:
                    label_text = evt.get('category', evt.get('type', ''))
                    if label_text:
                        ax.annotate(
                            label_text, (mdates.date2num(evt['date']), row_idx),
                            textcoords="offset points", xytext=(0, 8),
                            fontsize=5, ha='center', color=c, rotation=45,
                        )
        elif cat_name == 'Other Filings':
            for evt in events:
                ax.scatter(mdates.date2num(evt['date']), row_idx,
                           c='#6C757D', marker='s', s=20, zorder=5, edgecolors='black',
                           linewidths=0.3, alpha=0.7)

    # Draw grant events
    if grant_events:
        amounts = [g['amount'] for g in grant_events]
        max_amount = max(amounts) if amounts else 1
        for g in grant_events:
            size = max(40, min(200, (g['amount'] / max(max_amount, 1)) * 200))
            ax.scatter(mdates.date2num(g['date']), grant_row_idx,
                       c='#F0AD4E', marker='*', s=size, zorder=5,
                       edgecolors='#8B6914', linewidths=0.5)

    # Draw incorporation date line
    if inc_date:
        ax.axvline(x=mdates.date2num(inc_date), color='#667EEA', linestyle='--',
                   alpha=0.6, linewidth=1, label='Incorporation')

    # Draw cessation date line
    cessation_date = _parse_date(profile.get('date_of_cessation', ''))
    if cessation_date:
        ax.axvline(x=mdates.date2num(cessation_date), color='#DC3545', linestyle='--',
                   alpha=0.6, linewidth=1, label='Cessation')

    # Format axes
    ax.set_yticks(y_positions)
    ax.set_yticklabels(row_labels, fontsize=8)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=[1, 7]))

    # Add section labels
    director_count = len(director_bars)
    psc_count = len(psc_bars)
    if director_count > 0:
        ax.axhspan(-0.5, director_count - 0.5, alpha=0.05, color='#4A90D9')
    if psc_count > 0:
        ax.axhspan(director_count - 0.5, director_count + psc_count - 0.5,
                   alpha=0.05, color='#5CB85C')

    ax.set_xlabel('Date', fontsize=10)
    ax.set_title('Company Timeline', fontsize=13, fontweight='bold', pad=10)
    ax.grid(True, axis='x', alpha=0.3, linestyle='-')
    ax.invert_yaxis()

    # Legend
    legend_items = []
    if director_bars:
        legend_items.append(mpatches.Patch(color='#4A90D9', label='Directors'))
    if psc_bars:
        legend_items.append(mpatches.Patch(color='#5CB85C', label='PSCs'))
    if filing_events['Accounts Filed']:
        legend_items.append(plt.Line2D([0], [0], marker='o', color='w',
                           markerfacecolor='#333333', markersize=6, label='Accounts Filed'))
        legend_items.append(plt.Line2D([0], [0], marker='o', color='w',
                           markerfacecolor='#DC3545', markersize=6, label='Accounts Filed (Late)'))
    if filing_events['Notices & Events']:
        legend_items.append(plt.Line2D([0], [0], marker='^', color='w',
                           markerfacecolor='#DC3545', markersize=8, label='Notices & Events'))
    if grant_events:
        legend_items.append(plt.Line2D([0], [0], marker='*', color='w',
                           markerfacecolor='#F0AD4E', markersize=10, label='Grants Received'))

    if legend_items:
        ax.legend(handles=legend_items, loc='upper left', fontsize=7,
                 bbox_to_anchor=(0, -0.08), ncol=min(len(legend_items), 4),
                 framealpha=0.9)

    plt.tight_layout()
    return _fig_to_svg(fig)


# ---------------------------------------------------------------------------
# Feature 2: Grants Report HTML
# ---------------------------------------------------------------------------

def fetch_grants_for_company(company_number: str) -> list:
    """Fetch all grants for a company from GrantNav API.

    Uses the /org/{id}/grants_received endpoint (same as Director Search)
    which is the reliable endpoint for fetching grants by organisation.
    """
    cleaned = clean_company_number(company_number)
    if not cleaned:
        return []

    org_id = f"GB-COH-{cleaned}"
    encoded_id = urllib.parse.quote(org_id)
    url = f"{GRANTNAV_API_BASE_URL}/org/{encoded_id}/grants_received?limit=1000"

    all_grants = []
    while url:
        data, error = grantnav_get_data(url)
        if error or not data:
            break
        results = data.get("results", [])
        if not results:
            break
        all_grants.extend(item.get("data", {}) for item in results if isinstance(item, dict))
        url = data.get("next")

    return all_grants


def fetch_grants_for_org(org_id: str) -> list:
    """Fetch all grants for an organisation from GrantNav API.

    Unlike fetch_grants_for_company, this accepts a pre-formatted org identifier
    (e.g. 'GB-CHC-12345' for charities or 'GB-COH-12345678' for companies)
    without applying clean_company_number().
    """
    if not org_id:
        return []

    encoded_id = urllib.parse.quote(org_id)
    url = f"{GRANTNAV_API_BASE_URL}/org/{encoded_id}/grants_received?limit=1000"

    all_grants = []
    while url:
        data, error = grantnav_get_data(url)
        if error or not data:
            break
        results = data.get("results", [])
        if not results:
            break
        all_grants.extend(item.get("data", {}) for item in results if isinstance(item, dict))
        url = data.get("next")

    return all_grants


def generate_grants_report_html(grants_data: list) -> str:
    """Generate HTML section for grants data in the EDD report."""
    if not grants_data:
        return '''
        <div class="section">
            <h2>Grants Received</h2>
            <p>No grants data was found for this company in the 360Giving GrantNav database.</p>
        </div>
        '''

    # Calculate summary statistics
    total_grants = len(grants_data)
    total_value = 0
    currencies = set()
    funders = set()
    dates = []

    for grant in grants_data:
        try:
            amount = float(grant.get('amountAwarded', 0))
            total_value += amount
        except (ValueError, TypeError):
            pass
        currency = grant.get('currency', 'GBP')
        if currency:
            currencies.add(currency)
        funder = _get_nested_value(grant, 'fundingOrganization_name')
        if funder:
            funders.add(funder)
        d = _parse_date(grant.get('awardDate', ''))
        if d:
            dates.append(d)

    date_range = ""
    if dates:
        dates.sort()
        date_range = f"{_strftime_day(dates[0])} to {_strftime_day(dates[-1])}"

    currency_symbol = '£' if 'GBP' in currencies or not currencies else list(currencies)[0] + ' '

    # Build HTML
    html_out = f'''
    <div class="section">
        <h2>Grants Received</h2>
        <p>The following grants data was retrieved from the 360Giving GrantNav database.</p>

        <div class="grants-summary">
            <div class="grants-stat"><strong>Total Grants</strong>{total_grants}</div>
            <div class="grants-stat"><strong>Total Value</strong>{currency_symbol}{total_value:,.2f}</div>
            <div class="grants-stat"><strong>Unique Funders</strong>{len(funders)}</div>
            <div class="grants-stat"><strong>Date Range</strong>{date_range or "N/A"}</div>
        </div>

        {'<details><summary><h3 style="display:inline;cursor:pointer">Grant Details (' + str(total_grants) + ' grants — click to expand)</h3></summary>' if total_grants > 20 else '<h3>Grant Details</h3>'}
        <table class="grants-table">
            <thead>
                <tr>
                    <th>Award Date</th>
                    <th>Title</th>
                    <th>Funder</th>
                    <th>Amount</th>
                    <th>Programme</th>
                </tr>
            </thead>
            <tbody>
    '''

    # Sort by award date descending
    sorted_grants = sorted(grants_data,
                           key=lambda g: _parse_date(g.get('awardDate', '')) or datetime.min,
                           reverse=True)

    for grant in sorted_grants:
        award_date = format_display_date(grant.get('awardDate', ''))

        title = html.escape(str(grant.get('title', 'N/A')))
        funder = html.escape(str(_get_nested_value(grant, 'fundingOrganization_name') or 'N/A'))
        try:
            amount = float(grant.get('amountAwarded', 0))
            amount_str = f"{currency_symbol}{amount:,.2f}"
        except (ValueError, TypeError):
            amount_str = 'N/A'
        programme = html.escape(str(_get_nested_value(grant, 'grantProgramme_title') or 'N/A'))

        html_out += f'''
                <tr>
                    <td>{html.escape(str(award_date))}</td>
                    <td>{title}</td>
                    <td>{funder}</td>
                    <td>{amount_str}</td>
                    <td>{programme}</td>
                </tr>
        '''

    html_out += '''
            </tbody>
        </table>
    '''
    if total_grants > 20:
        html_out += '</details>'

    # Detailed descriptions section
    grants_with_desc = [g for g in sorted_grants if g.get('description')]
    if grants_with_desc:
        if total_grants > 20:
            html_out += '<details><summary><h3 style="display:inline;cursor:pointer">Grant Descriptions (click to expand)</h3></summary>'
        else:
            html_out += '<h3>Grant Descriptions</h3>'
        for grant in grants_with_desc:
            title = html.escape(str(grant.get('title', 'Untitled')))
            desc = html.escape(str(grant.get('description', '')))
            funder = html.escape(str(_get_nested_value(grant, 'fundingOrganization_name') or 'Unknown'))
            date_str = format_display_date(grant.get('awardDate', ''))
            try:
                amount = float(grant.get('amountAwarded', 0))
                amount_str = f"{currency_symbol}{amount:,.2f}"
            except (ValueError, TypeError):
                amount_str = 'N/A'

            html_out += f'''
            <div class="grant-detail">
                <h4>{title}</h4>
                <p class="grant-meta">{funder} &middot; {date_str} &middot; {amount_str}</p>
                <p>{desc}</p>
            </div>
            '''
        if total_grants > 20:
            html_out += '</details>'

    html_out += '</div>'
    return html_out


# ---------------------------------------------------------------------------
# Feature 3: Corporate Ownership Structure Graph
# ---------------------------------------------------------------------------

def trace_ownership_chain(
    api_key: str,
    token_bucket,
    company_number: str,
    cancel_flag: threading.Event,
    max_depth: int = 5,
    status_callback=None,
) -> list:
    """
    Trace the corporate ownership chain through PSC data.
    Returns a flat list of relationship dicts.
    """
    company_number = clean_company_number(company_number)
    results = []
    seen_companies = {company_number}
    companies_this_level = [company_number]
    level = 1

    while companies_this_level and not cancel_flag.is_set() and level <= max_depth:
        next_level = []

        for cnum in companies_this_level:
            if cancel_flag.is_set():
                break

            if status_callback:
                status_callback(f"Tracing ownership level {level} - {cnum}...")

            pscs_data, error = ch_get_data(
                api_key, token_bucket,
                f"/company/{cnum}/persons-with-significant-control?items_per_page=100",
                is_psc=True,
            )
            if error or not pscs_data:
                continue

            for psc_summary in pscs_data.get('items', []):
                if cancel_flag.is_set():
                    break

                psc_link = psc_summary.get('links', {}).get('self')
                if not psc_link:
                    continue

                psc, err = ch_get_data(api_key, token_bucket, psc_link, is_psc=True)
                if err or not psc:
                    continue

                notified_on = psc.get('notified_on', '').strip()
                ceased_on = psc.get('ceased_on', '').strip()

                name = psc.get('name', '').strip()
                kind = psc.get('kind', '')
                is_corporate = 'corporate' in kind
                natures = ' | '.join(psc.get('natures_of_control', []))

                psc_cnum = ''
                if is_corporate:
                    raw_cnum = (psc.get('identification', {}) or {}).get(
                        'registration_number', ''
                    )
                    psc_cnum = (
                        raw_cnum.strip().zfill(8)
                        if raw_cnum and raw_cnum.strip().isdigit() and len(raw_cnum.strip()) < 8
                        else raw_cnum.strip()
                    ).upper()

                    if psc_cnum and psc_cnum not in seen_companies:
                        next_level.append(psc_cnum)
                        seen_companies.add(psc_cnum)

                country = psc.get('country_of_residence', '') or (
                    psc.get('identification', {}) or {}
                ).get('country_of_residence', '')

                results.append({
                    'level': level,
                    'parent_company_number': cnum,
                    'psc_name': name,
                    'psc_company_number': psc_cnum,
                    'psc_kind': kind,
                    'is_corporate': is_corporate,
                    'country': country,
                    'natures_of_control': natures,
                    'notified_on': notified_on,
                    'ceased_on': ceased_on,
                })

        companies_this_level = next_level
        level += 1

    return results


def generate_static_ownership_graph(
    company_name: str,
    company_number: str,
    ownership_data: list,
    figsize: tuple = (14, 10),
) -> str:
    """
    Generate a static ownership tree image using NetworkX + graphviz.
    Returns a base64-encoded PNG string.
    """
    if not ownership_data:
        fig, ax = plt.subplots(figsize=(8, 2))
        ax.text(0.5, 0.5, 'No corporate ownership chain found', ha='center', va='center',
                transform=ax.transAxes, fontsize=12, color='grey')
        ax.axis('off')
        return _fig_to_svg(fig)

    G = nx.DiGraph()

    # Root company node
    root_id = company_number
    root_label = _wrap_label(f"{company_name}\n({company_number})", max_width=25)
    G.add_node(root_id, label=root_label, node_type='company')

    # Build graph from ownership data
    # Track which nodes have an active (non-ceased) entry so that when the
    # same person appears both as active and ceased we keep the active state.
    company_names = {company_number: company_name}
    for rel in ownership_data:
        parent = rel['parent_company_number']
        psc_name = rel['psc_name']
        is_corporate = rel['is_corporate']
        psc_cnum = rel['psc_company_number']
        natures = rel.get('natures_of_control', '')
        ceased = rel.get('ceased_on', '')

        if is_corporate and psc_cnum:
            node_id = psc_cnum
            node_label = _wrap_label(f"{psc_name}\n({psc_cnum})", max_width=25)
            node_type = 'corporate_psc'
            company_names[psc_cnum] = psc_name
        else:
            node_id = f"person_{psc_name}_{parent}"
            node_label = _wrap_label(psc_name, max_width=25)
            node_type = 'individual_psc'

        if ceased:
            node_type = 'ceased'

        # If this node already exists with an active (non-ceased) type, do not
        # overwrite it with 'ceased' — the active relationship takes precedence.
        existing_type = G.nodes[node_id].get('node_type') if node_id in G else None
        if existing_type and existing_type != 'ceased' and node_type == 'ceased':
            # Keep the existing active node; still add the edge below
            pass
        else:
            G.add_node(node_id, label=node_label, node_type=node_type)

        # Edge: PSC controls parent company
        edge_label = _abbreviate_natures(natures)
        G.add_edge(node_id, parent, label=edge_label)

    if len(G.nodes) == 0:
        fig, ax = plt.subplots(figsize=(8, 2))
        ax.text(0.5, 0.5, 'No ownership data to display', ha='center', va='center',
                transform=ax.transAxes, fontsize=12, color='grey')
        ax.axis('off')
        return _fig_to_svg(fig)

    # Determine maximum ownership depth
    max_depth = max((rel.get('level', 1) for rel in ownership_data), default=1)
    is_shallow = max_depth <= 1

    # Layout - compress spacing to reduce long edges
    if is_shallow:
        G.graph['graph'] = {'rankdir': 'TB', 'ranksep': '0.3', 'nodesep': '0.25'}
    else:
        G.graph['graph'] = {'rankdir': 'TB', 'ranksep': '0.5', 'nodesep': '0.3'}
    try:
        pos = graphviz_layout(G, prog='dot')
    except Exception:
        # Fallback to manual hierarchical layout
        pos = _hierarchical_layout(G, root_id)

    # For shallow graphs, compress the vertical spacing after layout
    if is_shallow and pos:
        y_vals = [p[1] for p in pos.values()]
        y_min, y_max = min(y_vals), max(y_vals)
        y_range = y_max - y_min
        if y_range > 0:
            # Compress vertical range to ~40% of original
            y_mid = (y_max + y_min) / 2
            pos = {n: (x, y_mid + (y - y_mid) * 0.4) for n, (x, y) in pos.items()}

    # Node colours and sizes (smaller nodes since labels are offset)
    node_colors = []
    node_sizes = []
    node_size_scale = 0.7 if is_shallow else 1.0
    for node in G.nodes():
        ntype = G.nodes[node].get('node_type', '')
        if ntype == 'company':
            node_colors.append('#B9D9EB')
            node_sizes.append(int(1400 * node_size_scale))
        elif ntype == 'corporate_psc':
            node_colors.append('#C5D9F1')
            node_sizes.append(int(1200 * node_size_scale))
        elif ntype == 'individual_psc':
            node_colors.append('#D9E8B9')
            node_sizes.append(int(1000 * node_size_scale))
        elif ntype == 'ceased':
            node_colors.append('#E0E0E0')
            node_sizes.append(int(800 * node_size_scale))
        else:
            node_colors.append('#FFFFFF')
            node_sizes.append(int(800 * node_size_scale))

    # Draw — use a compact figure for shallow structures
    if is_shallow:
        fig_width = min(figsize[0], max(8, len(G.nodes) * 2.5))
        fig_height = max(4, 2 + len(G.nodes) * 0.4)
    else:
        fig_width = figsize[0]
        fig_height = max(figsize[1], 3 + len(G.nodes) * 0.5)
    fig, ax = plt.subplots(figsize=(fig_width, min(fig_height, 20)))

    labels = {n: G.nodes[n].get('label', n) for n in G.nodes()}

    # Offset label positions above nodes so they do not overlap the node circle.
    y_vals = [p[1] for p in pos.values()]
    y_range = max(y_vals) - min(y_vals) if len(y_vals) > 1 else 100
    label_offset = y_range * 0.06
    label_pos = {node: (x, y + label_offset) for node, (x, y) in pos.items()}

    nx.draw_networkx_nodes(G, pos, ax=ax, node_color=node_colors,
                           node_size=node_sizes, edgecolors='#333333',
                           linewidths=1)
    nx.draw_networkx_labels(G, label_pos, labels=labels, ax=ax, font_size=7,
                            font_family='sans-serif')
    nx.draw_networkx_edges(G, pos, ax=ax, edge_color='#666666',
                           arrows=True, arrowsize=15, arrowstyle='-|>',
                           connectionstyle='arc3,rad=0.1')

    # Edge labels
    edge_labels = nx.get_edge_attributes(G, 'label')
    if edge_labels:
        nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, ax=ax,
                                     font_size=6, font_color='#444444')

    ax.set_title('Corporate Ownership Structure', fontsize=13, fontweight='bold', pad=10)
    ax.axis('off')

    # Add horizontal and vertical margins so node labels are not clipped
    x_vals = [p[0] for p in pos.values()]
    if x_vals:
        x_min, x_max = min(x_vals), max(x_vals)
        x_range = x_max - x_min if x_max != x_min else 100
        x_pad = x_range * 0.15  # 15% padding on each side
        ax.set_xlim(x_min - x_pad, x_max + x_pad)

    if y_vals:
        y_min_pos, y_max_pos = min(y_vals), max(y_vals)
        y_range_pos = y_max_pos - y_min_pos if y_max_pos != y_min_pos else 100
        y_pad_bottom = y_range_pos * 0.15
        # Extra headroom above the topmost label (label sits label_offset above node)
        y_pad_top = label_offset + y_range_pos * 0.20
        ax.set_ylim(y_min_pos - y_pad_bottom, y_max_pos + y_pad_top)

    # Legend
    legend_items = [
        mpatches.Patch(facecolor='#B9D9EB', edgecolor='#333', label='Investigated Company'),
        mpatches.Patch(facecolor='#C5D9F1', edgecolor='#333', label='Corporate PSC'),
        mpatches.Patch(facecolor='#D9E8B9', edgecolor='#333', label='Individual PSC'),
        mpatches.Patch(facecolor='#E0E0E0', edgecolor='#333', label='Ceased'),
    ]
    ax.legend(handles=legend_items, loc='lower right', fontsize=7, framealpha=0.9)

    plt.tight_layout()
    return _fig_to_svg(fig)


def _wrap_label(text: str, max_width: int = 25) -> str:
    """Wrap text for graph node labels."""
    words = text.split()
    lines = []
    current_line = []
    current_len = 0
    for word in words:
        if current_len + len(word) + 1 > max_width and current_line:
            lines.append(' '.join(current_line))
            current_line = [word]
            current_len = len(word)
        else:
            current_line.append(word)
            current_len += len(word) + 1
    if current_line:
        lines.append(' '.join(current_line))
    return '\n'.join(lines)


def _abbreviate_natures(natures: str) -> str:
    """Abbreviate natures of control for edge labels."""
    if not natures:
        return ''
    # Common abbreviations
    result = natures
    result = result.replace('ownership-of-shares-', '')
    result = result.replace('voting-rights-', 'votes ')
    result = result.replace('right-to-appoint-and-remove-directors', 'appoint directors')
    result = result.replace('significant-influence-or-control', 'sig. influence')
    result = result.replace('25-to-50-percent', '25-50%')
    result = result.replace('50-to-75-percent', '50-75%')
    result = result.replace('75-to-100-percent', '75-100%')
    result = result.replace('more-than-25-percent', '>25%')
    # Truncate if still too long
    if len(result) > 40:
        result = result[:37] + '...'
    return result


def _hierarchical_layout(G, root, width=1.0, vert_gap=0.2, vert_loc=0):
    """Simple BFS-based hierarchical layout as fallback when graphviz is unavailable."""
    pos = {}
    visited = set()
    queue = deque([(root, 0)])
    levels = {}

    while queue:
        node, depth = queue.popleft()
        if node in visited:
            continue
        visited.add(node)
        if depth not in levels:
            levels[depth] = []
        levels[depth].append(node)
        for pred in G.predecessors(node):
            if pred not in visited:
                queue.append((pred, depth + 1))

    # Also add unreachable nodes
    for node in G.nodes():
        if node not in visited:
            depth = max(levels.keys()) + 1 if levels else 0
            if depth not in levels:
                levels[depth] = []
            levels[depth].append(node)

    for depth, nodes in levels.items():
        for i, node in enumerate(nodes):
            x = (i + 0.5) / len(nodes) * width
            y = -depth * vert_gap
            pos[node] = (x * 400, y * 200)

    return pos
