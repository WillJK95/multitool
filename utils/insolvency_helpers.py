"""Shared helpers for classifying Companies House insolvency / dissolution.

Used by both the company-centric Enhanced Due Diligence module
(:mod:`multitool.modules.enhanced_dd`) and the person-centric Director
Diligence Report (:mod:`multitool.utils.person_edd`).
"""

from typing import Tuple, Optional, Dict, Any

from ..api.companies_house import ch_get_insolvency, ch_get_filing_history


def format_insolvency_type(raw_type: Optional[str]) -> str:
    """Convert CH API insolvency type string to human-readable form.

    e.g. ``'creditors-voluntary-liquidation'`` -> ``"Creditors' Voluntary Liquidation"``.
    """
    if not raw_type:
        return "Unknown"
    display = raw_type.replace('-', ' ').title()
    display = display.replace('Creditors ', "Creditors' ")
    display = display.replace('Members ', "Members' ")
    return display


def normalise_company_name(name: Optional[str]) -> str:
    """Strip legal suffixes, generic terms, and apply basic stemming for comparison."""
    if not name:
        return ""

    name = name.lower().strip()
    name = name.replace("'", "").replace("’", "")

    legal_suffixes = [
        'public limited company', 'private limited company',
        'community interest company', 'charitable incorporated organisation',
        'limited liability partnership', 'limited partnership',
        'limited', 'ltd', 'plc', 'llp', 'lp', 'cic', 'cio', 'inc', 'corp',
    ]
    generic_terms = {
        'services', 'solutions', 'group', 'holdings', 'uk', 'gb', 'international',
        'consulting', 'consultants', 'management', 'associates', 'partners',
        'enterprises', 'ventures', 'trading', 'company', 'co', '&', 'and', 'the',
    }

    for suffix in legal_suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
            break

    words = name.split()
    distinctive = [w for w in words if w not in generic_terms]
    if not distinctive:
        distinctive = words

    stemmed = []
    for w in distinctive:
        if w.endswith('ies') and len(w) > 3:
            w = w[:-3] + 'y'
        elif w.endswith('s') and len(w) > 2:
            w = w[:-1]
        stemmed.append(w)
    return ' '.join(stemmed).strip()


# Preference order for extracting a representative insolvency date.
_DATE_TYPE_PRIORITY = (
    "wound-up-on",
    "declared-insolvent-on",
    "administration-started-on",
    "administration-discharged-on",
    "voluntary-arrangement-start-on",
    "case-end-on",
)


def _pick_case_date(case: Dict[str, Any]) -> Optional[str]:
    """Return the most informative date string from an insolvency case payload."""
    dates = case.get("dates") or []
    if not dates:
        return None
    by_type = {d.get("type"): d.get("date") for d in dates if d.get("date")}
    for pref in _DATE_TYPE_PRIORITY:
        if by_type.get(pref):
            return by_type[pref]
    # Fall back to whichever date is present.
    for d in dates:
        if d.get("date"):
            return d["date"]
    return None


def classify_insolvency(
    api_key: str,
    token_bucket,
    company_number: str,
    company_status: str,
    profile_dissolved_on: Optional[str] = None,
) -> Tuple[bool, str, Optional[str]]:
    """Determine the insolvency type, benignness and date for a company.

    Returns ``(is_benign, insolvency_type, liquidation_date)``:

    - ``is_benign`` is ``True`` for Members' Voluntary Liquidation (solvent
      wind-down) and voluntary strike-off; ``False`` for everything else
      including unknown.
    - ``insolvency_type`` is a human-readable string (e.g.
      ``"Creditors' Voluntary Liquidation"``).
    - ``liquidation_date`` is the most informative ISO date associated with
      the insolvency event (case date > strike-off filing date > profile
      ``date_of_cessation``).
    """
    status = (company_status or "").lower()

    if 'liquidation' in status or 'dissolved' in status:
        data, _err = ch_get_insolvency(api_key, token_bucket, company_number)
        if data and data.get('cases'):
            chosen_date: Optional[str] = None
            formatted = "Unknown"
            for case in data['cases']:
                case_type = (case.get('type') or '').lower()
                raw_type = case.get('type') or ''
                formatted = format_insolvency_type(raw_type)
                case_date = _pick_case_date(case)
                if case_date and (chosen_date is None or case_date > chosen_date):
                    chosen_date = case_date
                if 'members' in case_type and 'voluntary' in case_type:
                    return True, formatted, chosen_date or profile_dissolved_on
            # Cases exist but none are MVL.
            return False, formatted, chosen_date or profile_dissolved_on

        if 'dissolved' in status:
            data, _err = ch_get_filing_history(
                api_key, token_bucket, company_number, items_per_page=15,
            )
            if data and data.get('items'):
                saw_ds01 = False
                saw_compulsory = False
                ds01_date: Optional[str] = None
                compulsory_date: Optional[str] = None
                for filing in data['items']:
                    ftype = (filing.get('type') or '').upper()
                    fdate = filing.get('action_date') or filing.get('date')
                    if ftype.startswith('DS01'):
                        saw_ds01 = True
                        if fdate and (ds01_date is None or fdate > ds01_date):
                            ds01_date = fdate
                    elif ftype.startswith('GAZ2') or ftype == 'DISS40':
                        saw_compulsory = True
                        if fdate and (compulsory_date is None or fdate > compulsory_date):
                            compulsory_date = fdate
                if saw_ds01:
                    return True, "Voluntary Strike-Off", ds01_date or profile_dissolved_on
                if saw_compulsory:
                    return False, "Compulsory Strike-Off", compulsory_date or profile_dissolved_on

                for filing in data['items']:
                    desc = (filing.get('description') or '').lower()
                    fdate = filing.get('action_date') or filing.get('date')
                    if 'dissolved' in desc:
                        if 'strike-off' in desc or 'strike off' in desc:
                            if 'voluntary' in desc:
                                return True, "Voluntary Strike-Off", fdate or profile_dissolved_on
                            if 'compulsory' in desc:
                                return False, "Compulsory Strike-Off", fdate or profile_dissolved_on

                return False, "Dissolved", profile_dissolved_on

        return False, "Unknown", profile_dissolved_on

    if 'administration' in status:
        # Administration not in 'liquidation'/'dissolved' branch — try insolvency
        data, _err = ch_get_insolvency(api_key, token_bucket, company_number)
        if data and data.get('cases'):
            chosen_date: Optional[str] = None
            formatted = "Administration"
            for case in data['cases']:
                raw_type = case.get('type') or ''
                formatted = format_insolvency_type(raw_type) or formatted
                case_date = _pick_case_date(case)
                if case_date and (chosen_date is None or case_date > chosen_date):
                    chosen_date = case_date
            return False, formatted, chosen_date
        return False, "Administration", None

    return False, "Unknown", None
