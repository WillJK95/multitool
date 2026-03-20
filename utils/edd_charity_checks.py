# multitool/utils/edd_charity_checks.py
"""Charity-specific risk checks for the Enhanced Due Diligence module.

All functions accept charity_data (dict with CC API responses) and a
thresholds dict, and return a list of finding dicts with the standard
keys: category, severity, title, narrative, recommendation.
"""

import html
from datetime import datetime
from typing import Dict, List, Optional

from .edd_visualizations import format_display_date
from ..constants import CHARITY_EXPECTED_POLICIES


def check_charity_status(charity_data: dict, thresholds: dict) -> List[dict]:
    """Check charity registration status and regulatory flags."""
    findings = []
    details = charity_data.get('details', {})

    reg_status = (details.get('reg_status') or '').upper()
    charity_name = html.escape(details.get('charity_name') or 'Unknown')

    # Removed charity
    if reg_status == 'RM':
        removal_date = details.get('date_of_removal', '')
        removal_reason = details.get('removal_reason', 'Unknown reason')
        findings.append({
            'category': 'Governance',
            'severity': 'Critical',
            'title': 'Charity Removed from Register',
            'narrative': (
                f"{charity_name} was removed from the Charity Commission register"
                f"{' on ' + format_display_date(removal_date) if removal_date else ''}. "
                f"Removal reason: {html.escape(str(removal_reason))}. "
                "A removed charity cannot operate as a registered charity."
            ),
            'recommendation': (
                'Do not proceed with new funding arrangements. '
                'Verify the charity\'s current legal status and any successor organisation.'
            ),
        })

    # Insolvent
    if details.get('insolvent'):
        findings.append({
            'category': 'Governance',
            'severity': 'Critical',
            'title': 'Charity Is Insolvent',
            'narrative': (
                f"{charity_name} is recorded as insolvent by the Charity Commission. "
                "This means the charity cannot meet its financial obligations as they fall due."
            ),
            'recommendation': (
                'Do not enter new funding commitments. Review any existing commitments '
                'with legal counsel and the appointed insolvency practitioner.'
            ),
        })

    # In administration
    if details.get('in_administration'):
        findings.append({
            'category': 'Governance',
            'severity': 'Critical',
            'title': 'Charity in Administration',
            'narrative': (
                f"{charity_name} is under administration. Control of the charity has passed "
                "to an appointed administrator."
            ),
            'recommendation': (
                'New commitments should not proceed without administrator approval. '
                'Review existing arrangements immediately.'
            ),
        })

    # CIO dissolution
    if details.get('cio_dissolution_ind'):
        findings.append({
            'category': 'Governance',
            'severity': 'Critical',
            'title': 'CIO Dissolution Notice',
            'narrative': (
                f"{charity_name} is a Charitable Incorporated Organisation (CIO) that is "
                "being dissolved. This is equivalent to a company strike-off."
            ),
            'recommendation': (
                'Do not proceed with new commitments. '
                'The CIO will cease to exist once dissolution is complete.'
            ),
        })

    # Interim manager appointed
    if details.get('interim_manager_ind'):
        appt_date = details.get('date_of_interim_manager_appt', '')
        findings.append({
            'category': 'Governance',
            'severity': 'Critical',
            'title': 'Interim Manager Appointed by Charity Commission',
            'narrative': (
                f"The Charity Commission has appointed an interim manager for {charity_name}"
                f"{' on ' + format_display_date(appt_date) if appt_date else ''}. "
                "Interim managers are only appointed in cases of serious concern about "
                "mismanagement or misconduct."
            ),
            'recommendation': (
                'This is a significant regulatory intervention. Investigate the reasons '
                'for appointment before proceeding with any funding decisions.'
            ),
        })

    # Recently registered
    reg_date_str = details.get('date_of_registration', '')
    if reg_date_str:
        try:
            reg_date = datetime.strptime(reg_date_str[:10], '%Y-%m-%d')
            age_months = (datetime.now() - reg_date).days / 30
            if age_months < 6:
                findings.append({
                    'category': 'Governance',
                    'severity': 'Moderate',
                    'title': 'Recently Registered Charity',
                    'narrative': (
                        f"{charity_name} was registered on "
                        f"{format_display_date(reg_date_str)}, approximately "
                        f"{int(age_months)} months ago. Recently registered charities "
                        "have limited operational history."
                    ),
                    'recommendation': (
                        'Request additional due diligence on the trustees and any '
                        'linked organisations. Consider phased or smaller initial grants.'
                    ),
                })
        except (ValueError, TypeError):
            pass


    return findings


def check_reporting_status(charity_data: dict, thresholds: dict) -> List[dict]:
    """Check charity reporting/filing status."""
    findings = []
    details = charity_data.get('details', {})
    reporting_status = (details.get('reporting_status') or '').lower()

    if 'double default' in reporting_status:
        findings.append({
            'category': 'Filing Compliance',
            'severity': 'Critical',
            'title': 'Accounts Submission Double Default',
            'narrative': (
                "The charity has failed to submit accounts for two consecutive years "
                "(double default). This is a serious compliance failure and may indicate "
                "governance breakdown or inability to prepare accounts."
            ),
            'recommendation': (
                'Treat as a significant red flag. Request an explanation from trustees '
                'and consider whether the charity is capable of managing funds.'
            ),
        })
    elif 'overdue' in reporting_status:
        findings.append({
            'category': 'Filing Compliance',
            'severity': 'Elevated',
            'title': 'Accounts Submission Overdue',
            'narrative': (
                "The charity's accounts submission is overdue. While a single late "
                "submission may have an innocent explanation, it is a governance concern."
            ),
            'recommendation': (
                'Request an explanation and a copy of the most recent accounts. '
                'Monitor for follow-up submission.'
            ),
        })
    elif 'received' in reporting_status:
        findings.append({
            'category': 'Filing Compliance',
            'severity': 'Positive',
            'title': 'Accounts Filed on Time',
            'narrative': "The charity's most recent accounts submission has been received.",
            'recommendation': '',
        })

    return findings


def check_regulatory_reports(charity_data: dict, thresholds: dict) -> List[dict]:
    """Check for published regulatory reports and inquiries."""
    findings = []
    reports = charity_data.get('regulatory_reports') or []
    if not reports:
        return findings

    for report in reports:
        report_name = html.escape(str(report.get('report_name', 'Unknown report')))
        findings.append({
            'category': 'Governance',
            'severity': 'Elevated',
            'title': 'Regulatory Report Published',
            'narrative': (
                f"The Charity Commission has published a regulatory report: "
                f"'{report_name}'. Regulatory reports are issued following inquiries "
                "into concerns about charity governance or conduct."
            ),
            'recommendation': (
                'Review the report in full on the Charity Commission website. '
                'Assess whether the issues identified have been resolved.'
            ),
        })

    return findings


def check_accounts_qualified(charity_data: dict, thresholds: dict) -> List[dict]:
    """Check whether the most recent accounts had a qualified audit opinion."""
    findings = []
    account_info = charity_data.get('account_ar_info') or []
    if not account_info:
        return findings

    # Sort by date, check most recent
    sorted_info = sorted(
        account_info,
        key=lambda x: x.get('financial_period_end_date', '') or '',
        reverse=True,
    )

    for info in sorted_info[:1]:
        if info.get('accounts_qualified'):
            findings.append({
                'category': 'Financial Health',
                'severity': 'Elevated',
                'title': 'Accounts Qualified by Auditor',
                'narrative': (
                    "The most recent audited accounts received a qualified opinion. "
                    "This means the auditor identified material issues with the "
                    "charity's financial statements or could not obtain sufficient "
                    "evidence on certain matters."
                ),
                'recommendation': (
                    'Review the auditor\'s report to understand the specific '
                    'qualifications. Assess whether they indicate systemic issues.'
                ),
            })
            break

    return findings


def check_accounts_submission_pattern(charity_data: dict, thresholds: dict) -> List[dict]:
    """Analyze accounts submission dates for late filing patterns."""
    findings = []
    account_info = charity_data.get('account_ar_info') or []
    if not account_info:
        return findings

    late_count = 0
    total_count = 0

    for info in account_info:
        date_received = info.get('date_received', '')
        date_due = info.get('date_due', '')
        if date_received and date_due:
            total_count += 1
            try:
                received = datetime.strptime(date_received[:10], '%Y-%m-%d')
                due = datetime.strptime(date_due[:10], '%Y-%m-%d')
                if received > due:
                    late_count += 1
            except (ValueError, TypeError):
                continue

    threshold_count = thresholds.get('late_filings_count', 2)
    threshold_period = thresholds.get('late_filings_period', 5)

    if late_count >= threshold_count and total_count <= threshold_period:
        findings.append({
            'category': 'Filing Compliance',
            'severity': 'Elevated',
            'title': 'Pattern of Late Accounts Submissions',
            'narrative': (
                f"Of the last {total_count} accounts submissions, {late_count} were "
                f"filed after the due date. A pattern of late filing suggests governance "
                "weakness and may affect the reliability of financial information."
            ),
            'recommendation': (
                'Request an explanation for late filings. '
                'Consider whether the charity has adequate financial management capacity.'
            ),
        })

    return findings


def check_net_assets(charity_data: dict, thresholds: dict) -> List[dict]:
    """Check for negative net assets (solvency indicator)."""
    findings = []
    assets_liabilities = charity_data.get('assets_liabilities') or []
    if not assets_liabilities:
        return findings

    # Use the most recent period
    sorted_al = sorted(
        (assets_liabilities if isinstance(assets_liabilities, list) else [assets_liabilities]),
        key=lambda x: x.get('financial_period_end_date', '') or '',
        reverse=True,
    )

    for al in sorted_al[:1]:
        own_use = _safe_float(al.get('assets_own_use')) or 0
        invest = _safe_float(al.get('assets_long_term_investment')) or 0
        pension = _safe_float(al.get('defined_net_assets_pension')) or 0
        other = _safe_float(al.get('assets_other_assets')) or 0
        liab = _safe_float(al.get('assets_total_liabilities')) or 0

        net_assets = own_use + invest + pension + other - liab

        if net_assets < 0:
            findings.append({
                'category': 'Financial Health',
                'severity': 'Critical',
                'title': 'Negative Net Assets',
                'narrative': (
                    f"The charity has negative net assets of "
                    f"\u00a3{net_assets:,.0f}. This indicates the charity's total "
                    "liabilities exceed its total assets, a sign of potential insolvency."
                ),
                'recommendation': (
                    'Review the charity\'s financial position in detail. '
                    'Assess whether there is a credible plan to restore positive net assets.'
                ),
            })
        elif net_assets > 0:
            findings.append({
                'category': 'Financial Health',
                'severity': 'Positive',
                'title': 'Positive Net Assets',
                'narrative': (
                    f"The charity has positive net assets of \u00a3{net_assets:,.0f}."
                ),
                'recommendation': '',
            })

    return findings


def check_reserves_ratio(charity_data: dict, thresholds: dict) -> List[dict]:
    """Check reserves-to-expenditure ratio (< 3 months = flag)."""
    findings = []
    fin_history = charity_data.get('financial_history') or []
    assets_liabilities = charity_data.get('assets_liabilities') or []

    if not fin_history or not assets_liabilities:
        return findings

    # Get latest expenditure
    sorted_fin = sorted(
        fin_history,
        key=lambda x: x.get('financial_period_end_date', '') or '',
        reverse=True,
    )
    latest_exp = _safe_float(sorted_fin[0].get('exp_total')) if sorted_fin else None
    if not latest_exp or latest_exp <= 0:
        return findings

    # Get latest net assets (reserves proxy)
    sorted_al = sorted(
        (assets_liabilities if isinstance(assets_liabilities, list) else [assets_liabilities]),
        key=lambda x: x.get('financial_period_end_date', '') or '',
        reverse=True,
    )
    al = sorted_al[0] if sorted_al else {}
    own_use = _safe_float(al.get('assets_own_use')) or 0
    invest = _safe_float(al.get('assets_long_term_investment')) or 0
    pension = _safe_float(al.get('defined_net_assets_pension')) or 0
    other = _safe_float(al.get('assets_other_assets')) or 0
    liab = _safe_float(al.get('assets_total_liabilities')) or 0
    net_assets = own_use + invest + pension + other - liab

    ratio = net_assets / latest_exp if latest_exp else None
    min_ratio = thresholds.get('reserves_to_expenditure_min', 0.25)

    if ratio is not None and ratio < min_ratio:
        months = ratio * 12
        findings.append({
            'category': 'Financial Health',
            'severity': 'Elevated',
            'title': 'Low Reserves Relative to Expenditure',
            'narrative': (
                f"The charity's reserves-to-expenditure ratio is {ratio:.2f}, equivalent "
                f"to approximately {months:.1f} months of operating costs. "
                f"Best practice recommends at least 3 months of reserves "
                f"(ratio \u2265 {min_ratio})."
            ),
            'recommendation': (
                'Review the charity\'s reserves policy. Assess whether there is a '
                'credible plan to build reserves or whether expenditure reductions are needed.'
            ),
        })

    return findings


def check_income_expenditure_trends(charity_data: dict, thresholds: dict) -> List[dict]:
    """Check for sustained expenditure exceeding income and income decline."""
    findings = []
    fin_history = charity_data.get('financial_history') or []
    if len(fin_history) < 2:
        return findings

    sorted_fin = sorted(
        fin_history,
        key=lambda x: x.get('financial_period_end_date', '') or '',
    )

    # Check consecutive deficit years
    consecutive_deficits = 0
    max_consecutive = 0
    for entry in sorted_fin:
        inc = _safe_float(entry.get('inc_total'))
        exp = _safe_float(entry.get('exp_total'))
        if inc is not None and exp is not None and exp > inc:
            consecutive_deficits += 1
            max_consecutive = max(max_consecutive, consecutive_deficits)
        else:
            consecutive_deficits = 0

    deficit_threshold = thresholds.get('consecutive_deficit_years', 3)
    if max_consecutive >= deficit_threshold:
        findings.append({
            'category': 'Financial Health',
            'severity': 'Elevated',
            'title': 'Sustained Expenditure Exceeding Income',
            'narrative': (
                f"Expenditure has exceeded income for {max_consecutive} consecutive "
                f"years. While charities may run planned deficits to deploy restricted "
                f"funds, sustained deficits deplete reserves and threaten viability."
            ),
            'recommendation': (
                'Review the charity\'s financial strategy. Determine whether deficits '
                'are planned (e.g. spending restricted funds) or structural.'
            ),
        })

    # Check income decline
    decline_years = thresholds.get('income_decline_years', 2)
    decline_pct = thresholds.get('income_decline_pct', -15)

    if len(sorted_fin) >= decline_years + 1:
        recent = sorted_fin[-1]
        earlier = sorted_fin[-(decline_years + 1)]
        inc_recent = _safe_float(recent.get('inc_total'))
        inc_earlier = _safe_float(earlier.get('inc_total'))

        if inc_recent is not None and inc_earlier is not None and inc_earlier > 0:
            change_pct = ((inc_recent - inc_earlier) / inc_earlier) * 100
            if change_pct <= decline_pct:
                findings.append({
                    'category': 'Financial Health',
                    'severity': 'Moderate',
                    'title': 'Significant Income Decline',
                    'narrative': (
                        f"Total income has declined by {change_pct:.1f}% over the last "
                        f"{decline_years} years (from \u00a3{inc_earlier:,.0f} to "
                        f"\u00a3{inc_recent:,.0f}). This may indicate loss of funding "
                        "sources or reduced charitable activity."
                    ),
                    'recommendation': (
                        'Review the charity\'s income sources and fundraising strategy. '
                        'Assess the sustainability of the current funding model.'
                    ),
                })

    return findings


def check_income_volatility(charity_data: dict, thresholds: dict) -> List[dict]:
    """Flag high year-on-year income volatility."""
    findings = []
    fin_history = charity_data.get('financial_history') or []
    if len(fin_history) < 2:
        return findings

    sorted_fin = sorted(
        fin_history,
        key=lambda x: x.get('financial_period_end_date', '') or '',
    )

    volatility_pct = thresholds.get('income_volatility_pct', 40)
    volatile_years = []

    for i in range(1, len(sorted_fin)):
        prev_inc = _safe_float(sorted_fin[i - 1].get('inc_total'))
        curr_inc = _safe_float(sorted_fin[i].get('inc_total'))
        if prev_inc and curr_inc and prev_inc > 0:
            change = abs((curr_inc - prev_inc) / prev_inc) * 100
            if change > volatility_pct:
                yr = sorted_fin[i].get('financial_period_end_date', '')[:4]
                volatile_years.append((yr, change))

    if volatile_years:
        detail = "; ".join(f"{yr}: {pct:.0f}% change" for yr, pct in volatile_years)
        findings.append({
            'category': 'Financial Health',
            'severity': 'Moderate',
            'title': 'High Income Volatility',
            'narrative': (
                f"Income has varied by more than {volatility_pct}% year-on-year in "
                f"{len(volatile_years)} period(s): {detail}. "
                "High volatility suggests instability in the funding base."
            ),
            'recommendation': (
                'Review income sources for concentration risk. '
                'Assess whether income swings are due to large one-off gifts, '
                'legacy income, or loss of recurring funders.'
            ),
        })

    return findings


def check_fundraising_cost_ratio(charity_data: dict, thresholds: dict) -> List[dict]:
    """Check if fundraising costs are disproportionate to income."""
    findings = []
    fin_history = charity_data.get('financial_history') or []
    if not fin_history:
        return findings

    sorted_fin = sorted(
        fin_history,
        key=lambda x: x.get('financial_period_end_date', '') or '',
        reverse=True,
    )

    latest = sorted_fin[0]
    inc_total = _safe_float(latest.get('inc_total'))
    exp_fundraising = _safe_float(latest.get('exp_raising_funds'))

    if inc_total and exp_fundraising and inc_total > 0:
        ratio = exp_fundraising / inc_total
        threshold = thresholds.get('fundraising_cost_ratio', 0.30)
        if ratio > threshold:
            findings.append({
                'category': 'Financial Health',
                'severity': 'Moderate',
                'title': 'High Fundraising Cost Ratio',
                'narrative': (
                    f"Fundraising costs (\u00a3{exp_fundraising:,.0f}) represent "
                    f"{ratio:.0%} of total income (\u00a3{inc_total:,.0f}). "
                    f"The threshold is {threshold:.0%}. Charities spending a high "
                    "proportion on fundraising attract regulatory scrutiny."
                ),
                'recommendation': (
                    'Review the efficiency of fundraising activities. '
                    'Consider whether the return on fundraising investment is adequate.'
                ),
            })

    return findings


def check_government_funding_concentration(charity_data: dict, thresholds: dict) -> List[dict]:
    """Check if government funding exceeds concentration threshold."""
    findings = []
    fin_history = charity_data.get('financial_history') or []
    if not fin_history:
        return findings

    sorted_fin = sorted(
        fin_history,
        key=lambda x: x.get('financial_period_end_date', '') or '',
        reverse=True,
    )

    latest = sorted_fin[0]
    inc_total = _safe_float(latest.get('inc_total'))
    govt_contracts = _safe_float(latest.get('income_from_govt_contracts')) or 0
    govt_grants = _safe_float(latest.get('income_from_govt_grants')) or 0
    govt_total = govt_contracts + govt_grants

    if inc_total and inc_total > 0 and govt_total > 0:
        ratio = govt_total / inc_total
        threshold = thresholds.get('govt_funding_concentration', 0.70)
        if ratio > threshold:
            findings.append({
                'category': 'Financial Health',
                'severity': 'Moderate',
                'title': 'High Government Funding Concentration',
                'narrative': (
                    f"Government funding (\u00a3{govt_total:,.0f}) represents "
                    f"{ratio:.0%} of total income. High dependency on government "
                    "funding creates concentration risk — changes in policy or "
                    "commissioning could significantly impact the charity."
                ),
                'recommendation': (
                    'Assess the diversity of the charity\'s income streams. '
                    'Consider whether there are plans to diversify funding sources.'
                ),
            })

    return findings


def check_trustee_remuneration(charity_data: dict, thresholds: dict) -> List[dict]:
    """Report on trustee remuneration and benefits."""
    findings = []
    overview = charity_data.get('overview') or {}

    remuneration_flags = []
    if overview.get('any_trustee_benefit'):
        remuneration_flags.append('trustees receive benefits')
    if overview.get('trustee_payments_acting_as_trustee'):
        remuneration_flags.append('trustees are paid for acting as trustee')
    if overview.get('trustee_payments_services'):
        remuneration_flags.append('trustees are paid for providing services')

    if remuneration_flags:
        flags_str = "; ".join(remuneration_flags)
        findings.append({
            'category': 'Governance',
            'severity': 'Moderate',
            'title': 'Trustee Remuneration Reported',
            'narrative': (
                f"The charity reports: {flags_str}. Trustee remuneration is not "
                "inherently problematic but is material for due diligence purposes "
                "and must be authorised by the charity's governing document."
            ),
            'recommendation': (
                'Verify that trustee remuneration is properly authorised and disclosed. '
                'Review the governing document and accounts for details.'
            ),
        })

    return findings


def check_policies(charity_data: dict, thresholds: dict) -> List[dict]:
    """Check which standard policies are held or missing."""
    findings = []
    policies = charity_data.get('policies') or []

    if not policies:
        # No policy data available
        return findings

    # Normalise policy names from the API response
    held_policies = set()
    for p in policies:
        name = (p.get('policy_desc') or '').lower().replace(' ', '_').replace('-', '_')
        held_policies.add(name)

    missing = []
    for expected in CHARITY_EXPECTED_POLICIES:
        # Fuzzy match: check if expected policy keyword appears in any held policy
        found = any(expected.replace('_', '') in hp.replace('_', '') for hp in held_policies)
        if not found:
            missing.append(expected.replace('_', ' ').title())

    if missing:
        findings.append({
            'category': 'Governance',
            'severity': 'Moderate',
            'title': 'Standard Policies Not Reported',
            'narrative': (
                f"The following standard policies were not reported as held: "
                f"{', '.join(missing)}. Charity Commission guidance recommends that "
                "all charities have these policies in place."
            ),
            'recommendation': (
                'Request confirmation that the charity has appropriate policies '
                'in place, even if not reported to the Commission.'
            ),
        })
    else:
        findings.append({
            'category': 'Governance',
            'severity': 'Positive',
            'title': 'Standard Policies Reported',
            'narrative': "The charity reports holding all expected standard policies.",
            'recommendation': '',
        })

    return findings


def check_trustee_count(charity_data: dict, thresholds: dict) -> List[dict]:
    """Check if trustee count is unusually low or high."""
    findings = []
    overview = charity_data.get('overview') or {}
    trustee_count = _safe_float(overview.get('trustees'))

    if trustee_count is None:
        return findings

    trustee_count = int(trustee_count)
    low = thresholds.get('trustee_count_low', 3)
    high = thresholds.get('trustee_count_high', 15)

    if trustee_count < low:
        findings.append({
            'category': 'Governance',
            'severity': 'Moderate',
            'title': 'Low Trustee Count',
            'narrative': (
                f"The charity has {trustee_count} trustee(s). Charity governance "
                f"best practice recommends at least {low} trustees to ensure "
                "adequate oversight and diversity of skills."
            ),
            'recommendation': (
                'Assess whether the charity has sufficient governance capacity. '
                'A low trustee count may indicate difficulty recruiting or retaining trustees.'
            ),
        })
    elif trustee_count > high:
        findings.append({
            'category': 'Governance',
            'severity': 'Moderate',
            'title': 'Unusually High Trustee Count',
            'narrative': (
                f"The charity has {trustee_count} trustees. Very large boards may "
                "indicate inefficient governance or a legacy structure."
            ),
            'recommendation': (
                'This is not inherently concerning but is noted for context. '
                'Large boards can be effective with proper committee structures.'
            ),
        })

    return findings


def check_contact_transparency(charity_data: dict, thresholds: dict) -> List[dict]:
    """Check if the charity has a website and email."""
    findings = []
    details = charity_data.get('details', {})

    web = details.get('web', '')
    email = details.get('email', '')

    if not web and not email:
        findings.append({
            'category': 'Governance',
            'severity': 'Moderate',
            'title': 'No Website or Email Published',
            'narrative': (
                "The charity has not published either a website or email address "
                "on the Charity Commission register. Most genuine operating charities "
                "maintain at least a basic web presence."
            ),
            'recommendation': (
                'Request contact details directly and verify the charity\'s '
                'operational presence through other channels.'
            ),
        })

    return findings


def check_default_address(charity_data: dict, thresholds: dict) -> List[dict]:
    """Check for known PO Box or virtual office addresses."""
    findings = []
    details = charity_data.get('details', {})

    addr_parts = []
    for i in range(1, 6):
        part = details.get(f'address_line_{i}') or details.get(f'address_line{i}')
        if part:
            addr_parts.append(str(part).strip())
    postcode = details.get('address_post_code', '')
    if postcode:
        addr_parts.append(str(postcode).strip())
    full_addr = ' '.join(addr_parts).lower()

    if not full_addr.strip():
        findings.append({
            'category': 'Governance',
            'severity': 'Moderate',
            'title': 'No Registered Address',
            'narrative': "No registered address was found for the charity.",
            'recommendation': 'Request the charity\'s operational address.',
        })
        return findings

    # Check for PO Box
    if 'po box' in full_addr or 'p.o. box' in full_addr:
        findings.append({
            'category': 'Governance',
            'severity': 'Moderate',
            'title': 'PO Box Registered Address',
            'narrative': (
                "The charity's registered address is a PO Box. While not inherently "
                "problematic, this limits the ability to verify physical operations."
            ),
            'recommendation': (
                'Request the charity\'s operational address and verify physical presence.'
            ),
        })

    return findings


def check_area_of_operation(charity_data: dict, thresholds: dict) -> List[dict]:
    """Check for overseas-only operation and broad area claims for small charities."""
    findings = []
    areas = charity_data.get('area_of_operation') or []
    details = charity_data.get('details', {})
    overview = charity_data.get('overview') or {}

    if not areas:
        return findings

    # Check for overseas-only
    area_names = [str(a.get('area_of_operation', '')).lower() for a in areas]
    uk_keywords = ['england', 'wales', 'scotland', 'northern ireland', 'united kingdom', 'uk']
    has_uk = any(kw in name for name in area_names for kw in uk_keywords)

    if not has_uk and areas:
        findings.append({
            'category': 'Governance',
            'severity': 'Moderate',
            'title': 'Operates Exclusively Overseas',
            'narrative': (
                "The charity operates exclusively outside the UK with no domestic "
                "area of operation listed. This is relevant context for domestic "
                "grant-making decisions."
            ),
            'recommendation': (
                'Consider whether overseas-only operation is appropriate for your '
                'funding programme. Review the charity\'s presence in operating countries.'
            ),
        })

    # Broad area claim for small charity
    country_threshold = thresholds.get('broad_area_country_count', 10)
    income_threshold = thresholds.get('broad_area_income_threshold', 100_000)
    latest_income = _safe_float(overview.get('latest_income') or details.get('latest_income'))

    if len(areas) > country_threshold and latest_income and latest_income < income_threshold:
        findings.append({
            'category': 'Governance',
            'severity': 'Moderate',
            'title': 'Broad Geographic Scope for Small Charity',
            'narrative': (
                f"The charity claims to operate in {len(areas)} areas but has "
                f"income of only \u00a3{latest_income:,.0f}. This scope may be "
                "unrealistic for a charity of this size."
            ),
            'recommendation': (
                'Verify the charity\'s actual operational reach and capacity '
                'to deliver in the stated areas.'
            ),
        })

    return findings


def check_professional_fundraiser(charity_data: dict, thresholds: dict) -> List[dict]:
    """Check for professional fundraiser without agreement."""
    findings = []
    overview = charity_data.get('overview') or {}

    if overview.get('professional_fundraiser') and not overview.get('agreement_professional_fundraiser'):
        findings.append({
            'category': 'Governance',
            'severity': 'Elevated',
            'title': 'Professional Fundraiser Without Agreement',
            'narrative': (
                "The charity uses a professional fundraiser but has not confirmed "
                "a written agreement is in place. Under the Charities Act, a written "
                "agreement is a legal requirement."
            ),
            'recommendation': (
                'Request confirmation of the written agreement with the professional '
                'fundraiser. Non-compliance is a regulatory risk.'
            ),
        })

    return findings


def _safe_float(val) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
