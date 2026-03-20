# multitool/utils/edd_charity_visualizations.py
"""Charity-specific chart and timeline generation for EDD reports.

All charts are rendered with matplotlib (Agg backend) and returned as
base64-encoded PNG images for embedding in the HTML report.
"""

import base64
import html
from io import BytesIO
from typing import Dict, List, Optional

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

from .helpers import log_message
from .edd_visualizations import format_display_date


def _fig_to_base64(fig) -> str:
    """Convert a matplotlib figure to a base64-encoded PNG string."""
    buf = BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    b64 = base64.b64encode(buf.getvalue()).decode()
    plt.close(fig)
    return b64


def generate_charity_chart_html(charity_data: dict) -> str:
    """Generate all financial charts for the charity report.

    Returns an HTML string with embedded chart images.
    """
    fin_history = charity_data.get('financial_history') or []
    assets_liabilities = charity_data.get('assets_liabilities') or []

    if not fin_history:
        return ""

    html_output = '<div class="section"><h2>Financial Analysis Charts</h2>'

    sorted_fin = sorted(
        fin_history,
        key=lambda x: x.get('fin_period_end_date', '') or '',
    )

    try:
        # ── Chart 1: Income & Expenditure Trend ──────────────────────
        years = []
        incomes = []
        expenditures = []
        for entry in sorted_fin:
            yr = _extract_year(entry)
            inc = _safe_float(entry.get('inc_total'))
            exp = _safe_float(entry.get('exp_total'))
            if yr is not None:
                years.append(yr)
                incomes.append(inc)
                expenditures.append(exp)

        if years and any(v is not None for v in incomes):
            html_output += '''
            <h3>Income &amp; Expenditure Trend</h3>
            <p>This chart shows the charity's total income and expenditure over available
            financial periods. Green shading indicates surplus (income exceeds expenditure);
            red shading indicates deficit.</p>
            '''
            fig, ax = plt.subplots(figsize=(10, 5))

            valid_inc = [(y, v) for y, v in zip(years, incomes) if v is not None]
            valid_exp = [(y, v) for y, v in zip(years, expenditures) if v is not None]

            if valid_inc:
                yrs_i, vals_i = zip(*valid_inc)
                ax.plot(yrs_i, vals_i, marker='o', label='Total Income',
                        linewidth=2, color='#28a745')

            if valid_exp:
                yrs_e, vals_e = zip(*valid_exp)
                ax.plot(yrs_e, vals_e, marker='s', label='Total Expenditure',
                        linewidth=2, color='#dc3545')

            # Shaded surplus/deficit area
            if valid_inc and valid_exp and len(valid_inc) == len(valid_exp):
                yrs_i, vals_i = zip(*valid_inc)
                _, vals_e = zip(*valid_exp)
                for i in range(len(yrs_i)):
                    if vals_i[i] is not None and vals_e[i] is not None:
                        color = '#28a74530' if vals_i[i] >= vals_e[i] else '#dc354530'
                        if i < len(yrs_i) - 1:
                            ax.fill_between(
                                [yrs_i[i], yrs_i[i + 1]],
                                [vals_i[i], vals_i[i + 1]],
                                [vals_e[i], vals_e[i + 1]],
                                alpha=0.2,
                                color='#28a745' if vals_i[i] >= vals_e[i] else '#dc3545',
                            )

            ax.set_xlabel('Year')
            ax.set_ylabel('£')
            ax.set_title('Income & Expenditure Trend')
            ax.legend()
            ax.grid(True, alpha=0.3)
            ax.xaxis.set_major_locator(MaxNLocator(integer=True))
            ax.yaxis.set_major_formatter(
                plt.FuncFormatter(lambda x, p: f'£{x:,.0f}'))

            html_output += (
                f'<div class="chart-container">'
                f'<img src="data:image/png;base64,{_fig_to_base64(fig)}" '
                f'alt="Income and Expenditure Trend"></div>'
            )

        # ── Chart 2: Income Breakdown (Stacked Bar) ─────────────────
        income_cats = [
            ('inc_donations_and_legacies', 'Donations & Legacies'),
            ('inc_charitable_activities', 'Charitable Activities'),
            ('inc_investment', 'Investment'),
            ('inc_other_trading_activities', 'Other Trading'),
            ('inc_endowments', 'Endowments'),
            ('inc_other', 'Other'),
        ]

        # Check if breakdown data exists
        has_breakdown = False
        for entry in sorted_fin:
            if any(_safe_float(entry.get(k)) for k, _ in income_cats):
                has_breakdown = True
                break

        if has_breakdown and len(years) >= 2:
            html_output += '''
            <h3>Income Breakdown by Source</h3>
            <p>Shows how the charity's income is composed across different sources.
            Concentration in a single source may indicate funding vulnerability.</p>
            '''
            fig, ax = plt.subplots(figsize=(10, 5))
            colors = ['#667eea', '#28a745', '#ffc107', '#fd7e14', '#dc3545', '#6c757d']
            bottoms = [0.0] * len(years)

            for idx, (field, label) in enumerate(income_cats):
                values = []
                for entry in sorted_fin:
                    val = _safe_float(entry.get(field))
                    values.append(val if val and val > 0 else 0)
                if any(v > 0 for v in values):
                    ax.bar(years, values, bottom=bottoms, label=label,
                           color=colors[idx % len(colors)], alpha=0.85)
                    bottoms = [b + v for b, v in zip(bottoms, values)]

            ax.set_xlabel('Year')
            ax.set_ylabel('£')
            ax.set_title('Income Breakdown by Source')
            ax.legend(loc='upper left', fontsize=8)
            ax.grid(True, alpha=0.3, axis='y')
            ax.xaxis.set_major_locator(MaxNLocator(integer=True))
            ax.yaxis.set_major_formatter(
                plt.FuncFormatter(lambda x, p: f'£{x:,.0f}'))

            html_output += (
                f'<div class="chart-container">'
                f'<img src="data:image/png;base64,{_fig_to_base64(fig)}" '
                f'alt="Income Breakdown"></div>'
            )

        # ── Chart 3: Assets & Liabilities ────────────────────────────
        sorted_al = sorted(
            (assets_liabilities if isinstance(assets_liabilities, list) else [assets_liabilities]),
            key=lambda x: x.get('fin_period_end_date', '') or '',
        )

        al_years = []
        al_data = {
            'assets_own_use': [],
            'assets_long_term_investment': [],
            'defined_net_assets_pension': [],
            'assets_other_assets': [],
            'assets_total_liabilities': [],
        }

        for entry in sorted_al:
            yr = _extract_year(entry)
            if yr is not None:
                al_years.append(yr)
                for key in al_data:
                    al_data[key].append(_safe_float(entry.get(key)) or 0)

        if al_years and any(any(v != 0 for v in vals) for vals in al_data.values()):
            html_output += '''
            <h3>Assets &amp; Liabilities Composition</h3>
            <p>Shows the composition of the charity's balance sheet. The stacked bars
            represent asset categories; the red bars show total liabilities.</p>
            '''
            fig, ax = plt.subplots(figsize=(10, 5))

            asset_items = [
                ('assets_own_use', 'Own Use Assets', '#667eea'),
                ('assets_long_term_investment', 'Long-term Investments', '#28a745'),
                ('defined_net_assets_pension', 'Pension Assets', '#ffc107'),
                ('assets_other_assets', 'Other (Current) Assets', '#fd7e14'),
            ]

            x_pos = list(range(len(al_years)))
            bar_width = 0.35
            bottoms = [0.0] * len(al_years)

            for key, label, color in asset_items:
                vals = al_data[key]
                if any(v > 0 for v in vals):
                    ax.bar([x - bar_width / 2 for x in x_pos], vals,
                           bar_width, bottom=bottoms, label=label, color=color, alpha=0.85)
                    bottoms = [b + v for b, v in zip(bottoms, vals)]

            # Liabilities as separate bars
            liab_vals = al_data['assets_total_liabilities']
            if any(v > 0 for v in liab_vals):
                ax.bar([x + bar_width / 2 for x in x_pos], liab_vals,
                       bar_width, label='Total Liabilities', color='#dc3545', alpha=0.85)

            ax.set_xlabel('Year')
            ax.set_ylabel('£')
            ax.set_title('Assets & Liabilities Composition')
            ax.set_xticks(x_pos)
            ax.set_xticklabels(al_years)
            ax.legend(loc='upper left', fontsize=8)
            ax.grid(True, alpha=0.3, axis='y')
            ax.yaxis.set_major_formatter(
                plt.FuncFormatter(lambda x, p: f'£{x:,.0f}'))

            html_output += (
                f'<div class="chart-container">'
                f'<img src="data:image/png;base64,{_fig_to_base64(fig)}" '
                f'alt="Assets and Liabilities"></div>'
            )

    except Exception as e:
        log_message(f"Error generating charity financial charts: {e}")
        html_output += (
            '<p>Unable to generate financial charts. '
            'Please check the charity data.</p>'
        )

    html_output += '</div>'
    return html_output


def generate_charity_profile_html(charity_data: dict) -> str:
    """Generate the charity profile grid for the report."""
    details = charity_data.get('details', {})
    overview = charity_data.get('overview', {})
    governing_doc = charity_data.get('governing_document', {})
    other_names = charity_data.get('other_names') or []
    areas = charity_data.get('area_of_operation') or []

    def esc(val, default='N/A'):
        if val is None or val == '':
            return default
        return html.escape(str(val))

    def fmt_money(val):
        v = _safe_float(val)
        if v is None:
            return 'N/A'
        return f'£{v:,.0f}'

    # Build address
    addr_parts = []
    for i in range(1, 6):
        part = details.get(f'address_line_{i}') or details.get(f'address_line{i}')
        if part:
            addr_parts.append(str(part).strip())
    postcode = details.get('address_post_code', '')
    if postcode:
        addr_parts.append(str(postcode).strip())
    address = '<br>'.join(html.escape(p) for p in addr_parts) if addr_parts else 'N/A'

    # Reporting status badge
    rs = (details.get('reporting_status') or '').lower()
    if 'received' in rs:
        rs_badge = '<span style="background:#28a745;color:white;padding:2px 8px;border-radius:3px;font-size:12px;">Submission Received</span>'
    elif 'double default' in rs:
        rs_badge = '<span style="background:#dc3545;color:white;padding:2px 8px;border-radius:3px;font-size:12px;">Double Default</span>'
    elif 'overdue' in rs:
        rs_badge = '<span style="background:#fd7e14;color:white;padding:2px 8px;border-radius:3px;font-size:12px;">Overdue</span>'
    else:
        rs_badge = esc(details.get('reporting_status'))

    # Status display
    reg_status = details.get('reg_status', '')
    status_text = 'Registered' if reg_status == 'R' else 'Removed' if reg_status == 'RM' else esc(reg_status)
    removal_date = details.get('date_of_removal', '')
    if removal_date and reg_status == 'RM':
        status_text += f' (removed {format_display_date(removal_date)})'

    # Contact info
    contact_parts = []
    if details.get('phone'):
        contact_parts.append(f"Phone: {esc(details['phone'])}")
    if details.get('email'):
        contact_parts.append(f"Email: {esc(details['email'])}")
    if details.get('web'):
        web_url = html.escape(details['web'])
        contact_parts.append(f'Web: <a href="{web_url}" target="_blank">{web_url}</a>')
    contact_html = '<br>'.join(contact_parts) if contact_parts else 'Not published'

    # Companies House link
    co_reg = details.get('charity_co_reg_number', '')
    ch_link = ''
    if co_reg:
        ch_link = f'{esc(co_reg)} (<a href="https://find-and-update.company-information.service.gov.uk/company/{html.escape(str(co_reg))}" target="_blank">View on Companies House</a>)'

    # Previous names
    names_html = ''
    if other_names:
        names_items = []
        for n in other_names:
            name_val = esc(n.get('charity_name', ''))
            name_type = esc(n.get('other_name_type', ''))
            names_items.append(f"{name_val} ({name_type})")
        names_html = '; '.join(names_items)

    # Areas of operation
    areas_html = ''
    if areas:
        area_names = [esc(a.get('geographic_area_description', '')) for a in areas]
        areas_html = '; '.join(a for a in area_names if a and a != 'N/A')

    # Build profile grid
    grid = f'''<div class="company-profile">
    <div class="profile-item"><strong>Charity Name</strong>{esc(details.get('charity_name'))}</div>
    <div class="profile-item"><strong>Registration Number</strong>{esc(details.get('reg_charity_number'))}</div>
    <div class="profile-item"><strong>Status</strong>{status_text}</div>
    <div class="profile-item"><strong>Charity Type</strong>{esc(details.get('charity_type'))}</div>
    <div class="profile-item"><strong>Date Registered</strong>{format_display_date(details.get('date_of_registration', ''))}</div>
    <div class="profile-item"><strong>Reporting Status</strong>{rs_badge}</div>
    <div class="profile-item"><strong>Registered Address</strong>{address}</div>
    <div class="profile-item"><strong>Contact</strong>{contact_html}</div>
    <div class="profile-item"><strong>Active Trustees</strong>{esc(overview.get('trustees'))}</div>
    <div class="profile-item"><strong>Employees (FTE)</strong>{esc(overview.get('employees'), 'Not reported')}</div>
    <div class="profile-item"><strong>Volunteers</strong>{esc(overview.get('volunteers'), 'Not reported')}</div>
    <div class="profile-item"><strong>Latest Income</strong>{fmt_money(overview.get('latest_income') or details.get('latest_income'))}</div>
    <div class="profile-item"><strong>Latest Expenditure</strong>{fmt_money(overview.get('latest_expenditure') or details.get('latest_expenditure'))}</div>
    <div class="profile-item"><strong>Companies House Number</strong>{ch_link if ch_link else 'N/A'}</div>
    '''

    if names_html:
        grid += f'<div class="profile-item"><strong>Previous / Working Names</strong>{names_html}</div>'

    if areas_html:
        grid += f'<div class="profile-item"><strong>Area of Operation</strong>{areas_html}</div>'

    grid += '</div>'

    # Full-width rows for objects and activities
    objects_text = ''
    if governing_doc and governing_doc.get('charitable_objects'):
        objects_text = html.escape(governing_doc['charitable_objects'])
    elif details.get('charitable_objects'):
        objects_text = html.escape(details['charitable_objects'])

    if objects_text:
        grid += f'''
        <div style="margin-top:15px;padding:10px;background:#f9f9f9;border-radius:5px;">
            <strong style="color:#667eea;">Charitable Objects</strong>
            <p style="margin:5px 0 0 0;font-size:14px;">{objects_text}</p>
        </div>'''

    activities = overview.get('activities') or details.get('activities', '')
    if activities:
        grid += f'''
        <div style="margin-top:10px;padding:10px;background:#f9f9f9;border-radius:5px;">
            <strong style="color:#667eea;">Activities</strong>
            <p style="margin:5px 0 0 0;font-size:14px;">{html.escape(str(activities))}</p>
        </div>'''

    return grid


def generate_charity_limitations_html(charity_data: dict, grants_enabled: bool) -> str:
    """Generate the data limitations section for a charity report."""
    limitations = '''
    <ul>
        <li><strong>Charity Commission Data:</strong> All charity data is sourced from the
        Charity Commission's Register of Charities API. Financial data represents the
        charity's own submissions and may not capture all off-balance-sheet arrangements
        or contingent liabilities.</li>
        <li><strong>Financial Data:</strong> The Charity Commission API provides up to 5 years
        of structured financial history. Balance sheet data (assets and liabilities) uses the
        broad categories defined by the Commission, which are less granular than company
        accounts. Notably, current and non-current liabilities are not separated.</li>
        <li><strong>Trustee Information:</strong> Trustee data reflects the current position
        as reported to the Commission. Historical trustee changes (resignations and
        appointments) are not fully tracked by the API, limiting churn analysis.</li>
        <li><strong>Timeliness:</strong> The register is updated when charities submit their
        annual returns and accounts. There may be a lag between real-world events and
        their reflection in the register.</li>
    '''

    if grants_enabled:
        limitations += '''
        <li><strong>GrantNav Data:</strong> Grants data is sourced from 360Giving's GrantNav
        database and reflects grants published under the 360Giving Data Standard. Not all
        funders publish their data, so this may understate total grants received.</li>
        '''

    limitations += '''
        <li><strong>Scope:</strong> This report covers the charity's registration in England
        and Wales only. Charities operating across multiple UK jurisdictions may have
        separate registrations with OSCR (Scotland) or CCNI (Northern Ireland) that are
        not covered here.</li>
    </ul>
    <p><em>This report is based on publicly available information and should not be the
    sole basis for decision-making. Professional advice should be sought where
    appropriate.</em></p>
    '''

    return limitations


def _extract_year(entry: dict) -> Optional[int]:
    """Extract fiscal year from a CC API response entry."""
    for key in ('fin_period_end_date', 'ar_cycle_reference'):
        val = entry.get(key)
        if val:
            try:
                if isinstance(val, str) and len(val) >= 4:
                    return int(val[:4])
                elif isinstance(val, (int, float)):
                    return int(val)
            except (ValueError, TypeError):
                continue
    return None


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
