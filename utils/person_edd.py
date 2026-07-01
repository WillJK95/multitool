"""Person-centric EDD synthesis: aggregate signals across all of a person's companies.

Mirrors the structure of ``edd_cross_analysis`` but pivoted to a single subject
and their footprint across multiple companies. Each rule returns a
``CrossAnalysisResult`` so the renderer can reuse existing card styling.
"""

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from rapidfuzz.fuzz import WRatio

from .edd_cross_analysis import CrossAnalysisResult
from .helpers import (
    clean_address_string,
    extract_address_string,
    extract_postcode as _extract_postcode,
    get_canonical_name_key,
    log_message,
    strip_postcode as _strip_postcode,
)
from .insolvency_helpers import (
    classify_insolvency,
    is_genuine_insolvency,
    normalise_company_name,
)


PHOENIX_SIMILARITY_PCT = 80
ADDRESS_SIMILARITY_PCT = 80

# Phoenix incorporation window: a live company is only treated as a phoenix
# candidate if it was incorporated between PHOENIX_WINDOW_YEARS_BEFORE years
# before and PHOENIX_WINDOW_YEARS_AFTER years after the older company's
# dissolution/liquidation.
PHOENIX_WINDOW_YEARS_BEFORE = 1
PHOENIX_WINDOW_YEARS_AFTER = 5

# Common low-transparency / tax-haven jurisdictions. Mirrors the list used by
# the Enhanced Due Diligence module (enhanced_dd._check_offshore_pscs) so the
# two modules flag the same places.
OFFSHORE_JURISDICTIONS = [
    "jersey", "guernsey", "isle of man",
    "british virgin islands", "bvi", "cayman islands",
    "bermuda", "bahamas", "panama", "seychelles",
    "gibraltar", "malta", "cyprus", "luxembourg",
    "liechtenstein", "monaco", "andorra",
]


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class PersonCompanyRecord:
    """All evidence we gather for a single company in the subject's footprint."""
    company_number: str
    company_name: str = ""
    company_status: str = ""
    incorporated_on: Optional[str] = None
    dissolved_on: Optional[str] = None
    has_been_liquidated: bool = False
    sic_codes: List[str] = field(default_factory=list)
    registered_address_raw: Optional[str] = None
    registered_address_clean: Optional[str] = None
    # Subject's role on this company
    subject_appointed_on: Optional[str] = None
    subject_resigned_on: Optional[str] = None
    subject_role: Optional[str] = None
    subject_is_psc: bool = False
    subject_psc_natures: List[str] = field(default_factory=list)
    # Filing compliance (from the company profile — no extra API call)
    accounts_overdue: bool = False
    confirmation_statement_overdue: bool = False
    next_accounts_due: Optional[str] = None
    next_cs_due: Optional[str] = None
    # Other people on the company
    co_officers: List[Dict] = field(default_factory=list)   # {name, dob, role, appointed_on, resigned_on, nationality, occupation, country_of_residence}
    co_pscs: List[Dict] = field(default_factory=list)       # {name, dob, natures, country, nationality}
    # Other signals
    insolvency_cases: List[Dict] = field(default_factory=list)
    grants: List[Dict] = field(default_factory=list)
    charges: List[Dict] = field(default_factory=list)       # {lenders: [str], status: str, created_on: str}
    fetch_error: Optional[str] = None


@dataclass
class PersonSubject:
    """Identification of the person under review."""
    canonical_keys: List[str]              # all keys merged into this subject
    display_name: str
    dob_year: Optional[int] = None
    dob_month: Optional[int] = None
    source_appointments: int = 0           # number of appointment rows the user selected


@dataclass
class PersonEDDReport:
    """Aggregated output for one person."""
    subject: PersonSubject
    companies: List[PersonCompanyRecord]
    results: List[CrossAnalysisResult]
    co_director_counts: Counter                       # name -> shared-company count
    address_clusters: Dict[str, List[str]]            # clean address -> [company_number, ...]
    grants_total_value: float = 0.0
    grants_total_count: int = 0
    fetch_errors: List[Tuple[str, str]] = field(default_factory=list)
    # Risk-finding subsections (populated by run_person_edd when an API
    # key is supplied; classification is empty otherwise).
    insolvent_companies: List[Dict[str, Any]] = field(default_factory=list)
    phoenix_matches: List[Dict[str, Any]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_role_combo(roles) -> str:
    """Render a set of raw officer-role strings as a display label.

    - ``{"director"}`` → ``"Director"``
    - ``{"secretary"}`` → ``"Secretary"``
    - ``{"director", "secretary"}`` → ``"Director/Secretary"``
    - anything else (corporate-director, llp-member, …) is title-cased.
    """
    norm = {(r or "").strip().lower() for r in roles if r}
    norm.discard("")
    if not norm:
        return ""
    is_director = any("director" in r for r in norm)
    is_secretary = any("secretary" in r for r in norm)
    if is_director and is_secretary:
        return "Director/Secretary"
    if is_director and len(norm) == 1 and "director" in norm:
        return "Director"
    if is_secretary and len(norm) == 1 and "secretary" in norm:
        return "Secretary"
    # Fallback: pretty-print whatever roles were given.
    pretty = sorted({r.replace("-", " ").title() for r in norm})
    return "/".join(pretty)


def _parse_iso_date(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _months_between(a: datetime, b: datetime) -> float:
    return abs((a - b).days) / 30.4375


def build_subject(rows: List[Dict]) -> Optional[PersonSubject]:
    """Derive the canonical subject(s) from the user-selected appointment rows.

    Returns None if no rows were supplied.
    """
    if not rows:
        return None
    keys = []
    display_counter = Counter()
    dob_counter = Counter()
    for row in rows:
        name = row.get("officer_name") or ""
        dob_str = row.get("date_of_birth") or ""
        # dob_str format from director_search: "MM-YYYY"
        dob_obj = None
        if dob_str and dob_str != "N/A":
            try:
                month_s, year_s = dob_str.split("-", 1)
                dob_obj = {"year": int(year_s), "month": int(month_s)}
                dob_counter[(dob_obj["year"], dob_obj["month"])] += 1
            except (ValueError, AttributeError):
                pass
        key = get_canonical_name_key(name, dob_obj)
        if key and key not in keys:
            keys.append(key)
        display_counter[name] += 1

    display_name = display_counter.most_common(1)[0][0] if display_counter else ""
    dob_year = dob_month = None
    if dob_counter:
        (y, m), _ = dob_counter.most_common(1)[0]
        dob_year, dob_month = y, m

    return PersonSubject(
        canonical_keys=keys,
        display_name=display_name,
        dob_year=dob_year,
        dob_month=dob_month,
        source_appointments=len(rows),
    )


def _subject_matches(subject: PersonSubject, name: str, dob: Optional[Dict]) -> bool:
    """Check whether an officer/PSC entry matches the subject by canonical key."""
    if not name:
        return False
    key = get_canonical_name_key(name, dob)
    if not key:
        return False
    if key in subject.canonical_keys:
        return True
    # Allow DOB-less match when subject has DOB and entry doesn't (CH PSC entries
    # sometimes omit DOB). Use the bare name portion.
    if subject.dob_year and not dob:
        bare_subject_keys = {k.split("-")[0] for k in subject.canonical_keys}
        return key in bare_subject_keys
    return False


# ---------------------------------------------------------------------------
# Rules — each returns a CrossAnalysisResult
# ---------------------------------------------------------------------------

def rule_p1_insolvency_pattern(
    companies: List[PersonCompanyRecord],
    classify_cache: Optional[Dict[str, Tuple[bool, str, Optional[str]]]] = None,
) -> CrossAnalysisResult:
    """Flag genuine (non-benign) insolvency in the subject's footprint.

    Benign/solvent wind-downs (e.g. Members' Voluntary Liquidation) are
    excluded from the headline count here — they are still shown, with their
    type, in the Dissolved & Insolvent Companies table below. ``classify_cache`` is the
    ``{company_number: (is_benign, insolvency_type, liq_date)}`` map produced
    by :func:`_collect_insolvent_companies`; when it is absent every insolvent
    company is treated as non-benign (we cannot prove otherwise without it).
    """
    classify_cache = classify_cache or {}

    def _is_genuine(cnum: str) -> bool:
        cached = classify_cache.get(cnum)
        # No cache entry → treat as genuine (cannot prove otherwise).
        if not cached:
            return True
        return is_genuine_insolvency(cached[0], cached[1])

    candidates = [
        c for c in companies
        if (c.has_been_liquidated or c.insolvency_cases) and c.company_number
    ]
    affected = [c for c in candidates if _is_genuine(c.company_number)]

    if not affected:
        return CrossAnalysisResult(
            rule_id="P1",
            title="Insolvency footprint",
            risk_flag="LOW",
            confidence="AUTO",
            narrative=(
                "No companies in the subject's footprint show a genuine (non-benign) "
                "insolvency or liquidation. Any solvent wind-downs are listed in the "
                "Dissolved & Insolvent Companies table below."
            ),
            recommendation="",
        )

    severity = "MEDIUM" if len(affected) == 1 else "HIGH"
    evidence_lines = []
    for c in affected:
        cached = classify_cache.get(c.company_number)
        ins_type = cached[1] if cached and cached[1] else None
        suffix = f" — {ins_type}" if ins_type and ins_type.lower() != "unknown" else ""
        evidence_lines.append(
            f"{c.company_name or c.company_number} ({c.company_number}) — "
            f"status: {c.company_status or 'unknown'}{suffix}"
        )
    narrative = (
        f"The subject is or was connected to {len(affected)} compan"
        f"{'y' if len(affected) == 1 else 'ies'} with a genuine (non-benign) "
        "insolvency or liquidation. Benign, solvent wind-downs (e.g. Members' "
        "Voluntary Liquidation) are excluded from this count and listed separately "
        "in the Dissolved & Insolvent Companies table below.\n\n"
        + "\n".join(f"• {line}" for line in evidence_lines)
    )
    return CrossAnalysisResult(
        rule_id="P1",
        title="Insolvency footprint",
        risk_flag=severity,
        confidence="AUTO",
        narrative=narrative,
        recommendation=(
            "Review each insolvent company's filings and the subject's role at the time. "
            "Multiple genuine insolvencies warrant deeper scrutiny."
        ),
    )


def _is_insolvent_status(c: PersonCompanyRecord) -> bool:
    status = (c.company_status or "").lower()
    if any(term in status for term in ("liquidation", "dissolved", "administration")):
        return True
    return bool(c.has_been_liquidated or c.insolvency_cases)


def _is_live_status(c: PersonCompanyRecord) -> bool:
    status = (c.company_status or "").lower()
    if not status:
        return False
    if any(term in status for term in ("liquidation", "dissolved", "administration", "receiver")):
        return False
    return True


def rule_p2_phoenix_signal(
    companies: List[PersonCompanyRecord],
    phoenix_matches: Optional[List[Dict[str, Any]]] = None,
) -> CrossAnalysisResult:
    """Flag live in-scope companies whose distinctive name closely matches a
    dissolved/liquidated in-scope company.

    Mirrors the Enhanced Due Diligence module's phoenix-name match. Pairing
    is restricted to ``dissolved → live`` because the user has already
    curated the cohort via Director Research; alias expansion is therefore
    implicit. Match details are written to ``phoenix_matches`` so the
    renderer can present them in a dedicated subsection.
    """
    matches = phoenix_matches if phoenix_matches is not None else []

    insolvent = [c for c in companies if _is_insolvent_status(c)]
    live = [c for c in companies if _is_live_status(c)]
    if not insolvent or not live:
        return CrossAnalysisResult(
            rule_id="P2",
            title="Phoenix pattern",
            risk_flag="LOW",
            confidence="AUTO",
            narrative=(
                "No phoenix candidates in scope — needs at least one "
                "insolvent/dissolved company and one live company."
            ),
            recommendation="",
        )

    for old in insolvent:
        old_norm = normalise_company_name(old.company_name)
        if not old_norm:
            continue
        for new in live:
            if new.company_number == old.company_number:
                continue
            new_norm = normalise_company_name(new.company_name)
            if not new_norm:
                continue
            similarity = WRatio(old_norm, new_norm)
            if similarity < PHOENIX_SIMILARITY_PCT:
                continue
            matches.append({
                "old_company": old.company_name or old.company_number,
                "old_number": old.company_number,
                "old_status": old.company_status,
                "new_company": new.company_name or new.company_number,
                "new_number": new.company_number,
                "new_status": new.company_status,
                "new_incorporated_on": new.incorporated_on,
                "similarity": round(similarity),
                "old_dissolved_on": old.dissolved_on,
                "liquidation_date": None,
                "insolvency_type": None,
                "anchor_date": None,
                "gap_years": None,
            })

    if not matches:
        return CrossAnalysisResult(
            rule_id="P2",
            title="Phoenix pattern",
            risk_flag="LOW",
            confidence="AUTO",
            narrative=(
                "No live company in scope has a distinctive-name similarity "
                f"≥{PHOENIX_SIMILARITY_PCT}% to any dissolved/liquidated company."
            ),
            recommendation="",
        )

    lines = [
        f"{m['old_company']} → {m['new_company']} ({m['similarity']}% name match)"
        for m in matches[:5]
    ]
    extra = f"\n…and {len(matches) - 5} more." if len(matches) > 5 else ""
    return CrossAnalysisResult(
        rule_id="P2",
        title="Phoenix pattern",
        risk_flag="HIGH",
        confidence="LIMITED",
        narrative=(
            f"Detected {len(matches)} potential phoenix link(s) — distinctive "
            f"company name match between a dissolved/liquidated company and a "
            f"live company in the subject's footprint:\n\n"
            + "\n".join(f"• {line}" for line in lines)
            + extra
        ),
        recommendation=(
            "Phoenix patterns can indicate avoidance of creditor obligations. "
            "Cross-check trading names, premises and customer continuity before progressing."
        ),
    )


def rule_p3_mass_resignation(
    companies: List[PersonCompanyRecord], threshold: int = 5, window_months: int = 12
) -> CrossAnalysisResult:
    """Flag resignations from multiple distinct companies within a short window."""
    resignations = []
    for c in companies:
        d = _parse_iso_date(c.subject_resigned_on)
        if d:
            resignations.append((c, d))
    resignations.sort(key=lambda x: x[1])

    if len(resignations) < threshold:
        return CrossAnalysisResult(
            rule_id="P3",
            title="Resignation clusters",
            risk_flag="INFO",
            confidence="AUTO",
            narrative=f"{len(resignations)} resignation(s) recorded across the footprint — no clustering test applied.",
            recommendation="",
        )

    # Sliding window
    worst = []
    for i in range(len(resignations)):
        window = [r for r in resignations[i:] if _months_between(resignations[i][1], r[1]) <= window_months]
        if len(window) > len(worst):
            worst = window

    if len(worst) < threshold:
        return CrossAnalysisResult(
            rule_id="P3",
            title="Resignation clusters",
            risk_flag="INFO",
            confidence="AUTO",
            narrative=f"{len(resignations)} resignations recorded but none clustered above the {threshold} threshold within {window_months} months.",
            recommendation="",
        )

    start = worst[0][1].strftime("%b %Y")
    end = worst[-1][1].strftime("%b %Y")
    lines = [f"{c.company_name or c.company_number} ({c.company_number}) — {d.strftime('%d %b %Y')}" for c, d in worst]
    return CrossAnalysisResult(
        rule_id="P3",
        title="Resignation clusters",
        risk_flag="INFO",
        confidence="AUTO",
        narrative=(
            f"{len(worst)} director resignations occurred between {start} and {end} "
            f"(window ≤ {window_months} months):\n\n"
            + "\n".join(f"• {line}" for line in lines)
        ),
        recommendation=(
            "Clustered resignations can precede insolvency events or signal disengagement from "
            "a corporate group. Check whether companies continued to file accounts after the resignations."
        ),
    )


def _cluster_addresses(
    companies: List[PersonCompanyRecord],
) -> Dict[str, List[str]]:
    """Group company registered addresses by postcode + fuzzy remainder match.

    Addresses with the same postcode are compared by their non-postcode
    portion using RapidFuzz ``WRatio``; pairs scoring at or above
    :data:`ADDRESS_SIMILARITY_PCT` are merged into the same cluster.
    Addresses with no detectable UK postcode fall back to fuzzy matching
    against each other only.

    Returns a mapping ``{representative_raw_address: [company_number, ...]}``
    where the representative is the longest raw address in the cluster.
    """
    # cluster_id -> {
    #   "postcode": str|None,
    #   "remainder": str,
    #   "members": [(raw_address, company_number), ...],
    # }
    clusters: List[Dict[str, Any]] = []

    for c in companies:
        raw = c.registered_address_raw or c.registered_address_clean
        if not raw:
            continue
        postcode = _extract_postcode(raw)
        remainder = _strip_postcode(raw)

        target = None
        for cluster in clusters:
            if cluster["postcode"] != postcode:
                continue
            if not cluster["remainder"] and not remainder:
                target = cluster
                break
            if WRatio(cluster["remainder"], remainder) >= ADDRESS_SIMILARITY_PCT:
                target = cluster
                break
        if target is None:
            clusters.append({
                "postcode": postcode,
                "remainder": remainder,
                "members": [(raw, c.company_number)],
            })
        else:
            target["members"].append((raw, c.company_number))

    grouped: Dict[str, List[str]] = {}
    for cluster in clusters:
        members = cluster["members"]
        # Representative = longest raw address (richest detail)
        rep = max((m[0] for m in members), key=len)
        grouped[rep] = [m[1] for m in members]
    return grouped


def rule_p4_address_clustering(
    companies: List[PersonCompanyRecord], threshold: int = 3
) -> Tuple[CrossAnalysisResult, Dict[str, List[str]]]:
    """Flag shared registered addresses across multiple companies."""
    by_addr = _cluster_addresses(companies)
    clusters = {addr: cnums for addr, cnums in by_addr.items() if len(cnums) >= threshold}

    if not clusters:
        return (
            CrossAnalysisResult(
                rule_id="P4",
                title="Shared address signals",
                risk_flag="INFO",
                confidence="AUTO",
                narrative=f"No registered address is shared by {threshold} or more of the subject's companies.",
                recommendation="",
            ),
            by_addr,
        )

    lines = []
    for addr, cnums in sorted(clusters.items(), key=lambda kv: -len(kv[1])):
        lines.append(f"{addr} — {len(cnums)} companies ({', '.join(cnums)})")
    return (
        CrossAnalysisResult(
            rule_id="P4",
            title="Shared address signals",
            risk_flag="INFO",
            confidence="LIMITED",
            narrative=(
                f"{len(clusters)} registered address(es) host ≥{threshold} of the subject's companies:\n\n"
                + "\n".join(f"• {line}" for line in lines)
            ),
            recommendation="",
        ),
        by_addr,
    )


def rule_p5_codirector_density(
    subject: PersonSubject, companies: List[PersonCompanyRecord], threshold: int = 3
) -> Tuple[CrossAnalysisResult, Counter]:
    """Identify co-directors who appear alongside the subject across multiple companies."""
    co_counts: Counter = Counter()
    for c in companies:
        for off in c.co_officers:
            name = off.get("name")
            if not name:
                continue
            if _subject_matches(subject, name, off.get("dob")):
                continue
            key = get_canonical_name_key(name, off.get("dob"))
            if key:
                co_counts[(key, name)] += 1

    # Aggregate to display name
    display_counts: Counter = Counter()
    for (key, name), count in co_counts.items():
        display_counts[name] += count

    repeats = {name: count for name, count in display_counts.items() if count >= threshold}

    severity = "INFO"
    if not repeats:
        return (
            CrossAnalysisResult(
                rule_id="P5",
                title="Co-director network",
                risk_flag="INFO",
                confidence="AUTO",
                narrative=f"No co-director appears alongside the subject on {threshold} or more companies.",
                recommendation="",
            ),
            display_counts,
        )

    lines = [f"{name} — co-director on {count} companies" for name, count in sorted(repeats.items(), key=lambda kv: -kv[1])]
    return (
        CrossAnalysisResult(
            rule_id="P5",
            title="Co-director network",
            risk_flag=severity,
            confidence="AUTO",
            narrative=(
                f"{len(repeats)} individual(s) sit on {threshold}+ boards with the subject:\n\n"
                + "\n".join(f"• {line}" for line in lines)
            ),
            recommendation=(
                "Repeated co-directorships often indicate a business partnership or corporate group. "
                "Useful for understanding the subject's wider network when conducting due diligence."
            ),
        ),
        display_counts,
    )


def rule_p6_grant_footprint(companies: List[PersonCompanyRecord]) -> Tuple[CrossAnalysisResult, float, int]:
    total_value = 0.0
    total_count = 0
    companies_with_grants = 0
    funders = set()

    for c in companies:
        if not c.grants:
            continue
        companies_with_grants += 1
        for g in c.grants:
            total_count += 1
            try:
                total_value += float(g.get("amountAwarded", 0) or 0)
            except (ValueError, TypeError):
                pass
            funder = g.get("fundingOrganization", [{}])
            if isinstance(funder, list) and funder:
                fname = funder[0].get("name") if isinstance(funder[0], dict) else None
                if fname:
                    funders.add(fname)

    if total_count == 0:
        return (
            CrossAnalysisResult(
                rule_id="P6",
                title="Grants footprint",
                risk_flag="LOW",
                confidence="AUTO",
                narrative="No 360Giving grants found across the subject's companies.",
                recommendation="",
            ),
            0.0,
            0,
        )

    return (
        CrossAnalysisResult(
            rule_id="P6",
            title="Grants footprint",
            risk_flag="INFO",
            confidence="AUTO",
            narrative=(
                f"{total_count} grants totalling £{total_value:,.2f} have been awarded across "
                f"{companies_with_grants} of the subject's companies, from {len(funders)} unique funder(s). "
                "See the full grants table below for detail."
            ),
            recommendation=(
                "Cross-reference funders to identify any existing relationships or repeat funding."
            ),
        ),
        total_value,
        total_count,
    )


def _fmt_due_date(iso: Optional[str]) -> str:
    """Format an ISO due date as 'd Month YYYY', falling back to the raw value."""
    d = _parse_iso_date(iso)
    if d:
        return f"{d.day} {d.strftime('%B %Y')}"
    return iso or "unknown"


def rule_p8_overdue_accounts(companies: List[PersonCompanyRecord]) -> CrossAnalysisResult:
    """Companies where the subject is *currently* appointed and accounts are overdue.

    Overdue accounts on a live appointment are a direct compliance/financial-distress
    signal for the subject, so this is flagged HIGH (renders as Elevated).
    """
    affected = [
        c for c in companies
        if c.accounts_overdue and not c.subject_resigned_on
    ]
    if not affected:
        return CrossAnalysisResult(
            rule_id="P8",
            title="Overdue accounts (current appointments)",
            risk_flag="LOW",
            confidence="AUTO",
            narrative="No company where the subject is currently appointed has overdue accounts.",
            recommendation="",
        )
    lines = [
        f"{c.company_name or c.company_number} ({c.company_number}) — accounts due {_fmt_due_date(c.next_accounts_due)}"
        for c in affected
    ]
    narrative = (
        f"The subject is currently appointed at {len(affected)} compan"
        f"{'y' if len(affected) == 1 else 'ies'} with overdue annual accounts.\n\n"
        + "\n".join(f"• {line}" for line in lines)
    )
    return CrossAnalysisResult(
        rule_id="P8",
        title="Overdue accounts (current appointments)",
        risk_flag="HIGH",
        confidence="AUTO",
        narrative=narrative,
        recommendation=(
            "Overdue accounts on a live appointment can indicate financial distress or "
            "administrative failure. Confirm each company is trading and request up-to-date "
            "management accounts."
        ),
    )


def _is_offshore(*values) -> Optional[str]:
    """Return the matched offshore jurisdiction name if any value names one."""
    for v in values:
        text = (v or "").strip().lower()
        if not text:
            continue
        for j in OFFSHORE_JURISDICTIONS:
            if j in text:
                return j
    return None


def rule_p7_offshore_pscs(companies: List[PersonCompanyRecord]) -> CrossAnalysisResult:
    """Flag co-PSCs registered/resident in low-transparency jurisdictions.

    Informational by default — offshore ownership is not inherently improper,
    but it can complicate beneficial-ownership due diligence. Severity rises to
    LOW when more than one offshore controller is present across the footprint.
    """
    # Dedupe by (name, jurisdiction) but collect the companies each appears on.
    found: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for c in companies:
        for psc in c.co_pscs:
            jurisdiction = _is_offshore(psc.get("country"), psc.get("nationality"))
            if not jurisdiction:
                continue
            name = psc.get("name") or "Unknown"
            key = (name.lower(), jurisdiction)
            entry = found.setdefault(key, {
                "name": name,
                "jurisdiction": (psc.get("country") or psc.get("nationality") or jurisdiction),
                "companies": [],
            })
            label = c.company_name or c.company_number
            if label not in entry["companies"]:
                entry["companies"].append(label)

    if not found:
        return CrossAnalysisResult(
            rule_id="P7",
            title="Offshore controllers",
            risk_flag="LOW",
            confidence="AUTO",
            narrative=(
                "No persons with significant control in the subject's footprint are "
                "registered or resident in a low-transparency jurisdiction."
            ),
            recommendation="",
        )

    entries = sorted(found.values(), key=lambda e: (-len(e["companies"]), e["name"].lower()))
    lines = [
        f"{e['name']} ({e['jurisdiction']}) — {len(e['companies'])} compan"
        f"{'y' if len(e['companies']) == 1 else 'ies'}: {', '.join(e['companies'])}"
        for e in entries[:5]
    ]
    extra = f"\n…and {len(entries) - 5} more." if len(entries) > 5 else ""
    severity = "LOW" if len(entries) > 1 else "INFO"
    return CrossAnalysisResult(
        rule_id="P7",
        title="Offshore controllers",
        risk_flag=severity,
        confidence="AUTO",
        narrative=(
            f"{len(entries)} person(s) with significant control across the subject's "
            "companies are registered or resident in a low-transparency jurisdiction:\n\n"
            + "\n".join(f"• {line}" for line in lines)
            + extra
        ),
        recommendation=(
            "Offshore ownership is not inherently problematic, but it can obscure "
            "ultimate beneficial ownership. Request supporting documentation on the "
            "controllers and corporate structure where relevant."
        ),
    )


def rule_p9_overdue_confirmation(companies: List[PersonCompanyRecord]) -> CrossAnalysisResult:
    """Companies where the subject is *currently* appointed and the confirmation
    statement is overdue. Flagged MEDIUM (renders as Moderate)."""
    affected = [
        c for c in companies
        if c.confirmation_statement_overdue and not c.subject_resigned_on
    ]
    if not affected:
        return CrossAnalysisResult(
            rule_id="P9",
            title="Overdue confirmation statement (current appointments)",
            risk_flag="LOW",
            confidence="AUTO",
            narrative="No company where the subject is currently appointed has an overdue confirmation statement.",
            recommendation="",
        )
    lines = [
        f"{c.company_name or c.company_number} ({c.company_number}) — confirmation statement due {_fmt_due_date(c.next_cs_due)}"
        for c in affected
    ]
    narrative = (
        f"The subject is currently appointed at {len(affected)} compan"
        f"{'y' if len(affected) == 1 else 'ies'} with an overdue confirmation statement.\n\n"
        + "\n".join(f"• {line}" for line in lines)
    )
    return CrossAnalysisResult(
        rule_id="P9",
        title="Overdue confirmation statement (current appointments)",
        risk_flag="MEDIUM",
        confidence="AUTO",
        narrative=narrative,
        recommendation=(
            "An overdue confirmation statement points to administrative neglect. Verify each "
            "company is actively managed and that its registered information is current."
        ),
    )


# ---------------------------------------------------------------------------
# Aggregation helpers for the renderer
# ---------------------------------------------------------------------------

def aggregate_charges(companies: List[PersonCompanyRecord]) -> Dict[str, Any]:
    """Aggregate charges across the footprint into a lender-centric view.

    Returns a dict with::

        {
            "lenders": [ {lender, charge_count, outstanding_count,
                          companies: [(name, number), ...]}, ... ],
            "total_charges": int,
            "outstanding": int,
            "satisfied": int,
            "companies_with_charges": int,
        }

    ``lenders`` is sorted by charge count descending, then lender name. A single
    charge with multiple persons-entitled is attributed to each named lender.
    """
    _OUTSTANDING = {"outstanding", "part-satisfied"}
    by_lender: Dict[str, Dict[str, Any]] = {}
    total_charges = 0
    outstanding = 0
    satisfied = 0
    companies_with_charges = 0

    for c in companies:
        if not c.charges:
            continue
        companies_with_charges += 1
        co_label = (c.company_name or c.company_number, c.company_number)
        for ch in c.charges:
            total_charges += 1
            is_outstanding = (ch.get("status") or "") in _OUTSTANDING
            if is_outstanding:
                outstanding += 1
            else:
                satisfied += 1
            lenders = ch.get("lenders") or ["(unnamed)"]
            for lender in lenders:
                entry = by_lender.setdefault(lender, {
                    "lender": lender,
                    "charge_count": 0,
                    "outstanding_count": 0,
                    "companies": [],
                })
                entry["charge_count"] += 1
                if is_outstanding:
                    entry["outstanding_count"] += 1
                if co_label not in entry["companies"]:
                    entry["companies"].append(co_label)

    lenders_sorted = sorted(
        by_lender.values(),
        key=lambda e: (-e["charge_count"], e["lender"].lower()),
    )
    return {
        "lenders": lenders_sorted,
        "total_charges": total_charges,
        "outstanding": outstanding,
        "satisfied": satisfied,
        "companies_with_charges": companies_with_charges,
    }


def aggregate_co_directors(companies: List[PersonCompanyRecord]) -> List[Dict[str, Any]]:
    """Aggregate co-officers across the footprint, one row per distinct person.

    Returns a list of dicts ``{name, count, nationality, occupation, country,
    companies: [(name, number), ...]}`` sorted by shared-company count
    (descending) then name. ``count`` is the number of distinct companies the
    person shares with the subject; attributes are taken as the most common
    non-empty value seen.
    """
    agg: Dict[str, Dict[str, Any]] = {}
    attr_counters: Dict[str, Dict[str, Counter]] = {}

    for c in companies:
        co_label = (c.company_name or c.company_number, c.company_number)
        for off in c.co_officers:
            name = off.get("name")
            if not name:
                continue
            key = get_canonical_name_key(name, off.get("dob")) or name
            entry = agg.setdefault(key, {
                "name": name,
                "count": 0,
                "companies": [],
                "nationality": None,
                "occupation": None,
                "country": None,
            })
            if co_label not in entry["companies"]:
                entry["companies"].append(co_label)
                entry["count"] += 1
            counters = attr_counters.setdefault(key, {
                "nationality": Counter(),
                "occupation": Counter(),
                "country": Counter(),
            })
            if off.get("nationality"):
                counters["nationality"][off["nationality"]] += 1
            if off.get("occupation"):
                counters["occupation"][off["occupation"]] += 1
            if off.get("country_of_residence"):
                counters["country"][off["country_of_residence"]] += 1

    for key, entry in agg.items():
        counters = attr_counters.get(key, {})
        for attr in ("nationality", "occupation", "country"):
            counter = counters.get(attr)
            if counter:
                entry[attr] = counter.most_common(1)[0][0]

    return sorted(agg.values(), key=lambda e: (-e["count"], e["name"].lower()))


# ---------------------------------------------------------------------------
# Top-level runner
# ---------------------------------------------------------------------------

def _collect_insolvent_companies(
    companies: List[PersonCompanyRecord],
    api_key: Optional[str],
    token_bucket: Any,
) -> Tuple[List[Dict[str, Any]], Dict[str, Tuple[bool, str, Optional[str]]]]:
    """For every dissolved/in-liquidation/in-administration company in scope,
    classify the insolvency via the shared helper.

    Returns ``(rows, cache)`` where ``cache`` maps company_number to the raw
    classify_insolvency triple so callers (e.g. Phoenix enrichment) can reuse
    it without re-fetching.
    """
    rows: List[Dict[str, Any]] = []
    cache: Dict[str, Tuple[bool, str, Optional[str]]] = {}
    for c in companies:
        if not _is_insolvent_status(c):
            continue
        if api_key and c.company_number:
            try:
                triple = classify_insolvency(
                    api_key, token_bucket, c.company_number,
                    c.company_status or "",
                    profile_dissolved_on=c.dissolved_on,
                )
            except Exception as e:
                log_message(f"Person EDD: classify_insolvency failed for {c.company_number}: {e}")
                triple = (False, "Unknown", c.dissolved_on)
        else:
            triple = (False, "Unknown", c.dissolved_on)
        cache[c.company_number] = triple
        is_benign, insolvency_type, liq_date = triple
        rows.append({
            "company_name": c.company_name or c.company_number,
            "company_number": c.company_number,
            "company_status": c.company_status,
            "liquidation_date": liq_date,
            "insolvency_type": insolvency_type,
            "is_benign": is_benign,
        })
    rows.sort(key=lambda r: (r["liquidation_date"] or "", r["company_name"]), reverse=True)
    return rows, cache


def _enrich_phoenix_matches(
    matches: List[Dict[str, Any]],
    classify_cache: Dict[str, Tuple[bool, str, Optional[str]]],
) -> None:
    """Fill in liquidation date and type on each phoenix match in place."""
    for m in matches:
        cached = classify_cache.get(m["old_number"])
        if cached:
            _is_benign, ins_type, liq_date = cached
            m["insolvency_type"] = ins_type
            m["liquidation_date"] = liq_date


def _filter_phoenix_window(matches: List[Dict[str, Any]]) -> None:
    """Keep only matches whose live company was incorporated within the phoenix
    window of the older company's dissolution/liquidation, in place.

    The window runs from ``PHOENIX_WINDOW_YEARS_BEFORE`` years before to
    ``PHOENIX_WINDOW_YEARS_AFTER`` years after the anchor date (the enriched
    liquidation date, falling back to the profile dissolution date). Matches
    without a datable anchor or incorporation date are dropped — they cannot be
    confirmed as phoenixes. Each kept match is annotated with ``anchor_date``
    and ``gap_years`` so the renderer stays consistent with this filter.
    """
    kept = []
    for m in matches:
        anchor = m.get("liquidation_date") or m.get("old_dissolved_on")
        inc = _parse_iso_date(m.get("new_incorporated_on"))
        anchor_dt = _parse_iso_date(anchor)
        if not inc or not anchor_dt:
            continue
        gap = (inc - anchor_dt).days / 365.25
        if -PHOENIX_WINDOW_YEARS_BEFORE <= gap <= PHOENIX_WINDOW_YEARS_AFTER:
            m["anchor_date"] = anchor
            m["gap_years"] = gap
            kept.append(m)
    matches[:] = kept


def _build_phoenix_result(matches: List[Dict[str, Any]]) -> CrossAnalysisResult:
    """Build the P2 finding from the window-filtered phoenix matches."""
    if not matches:
        return CrossAnalysisResult(
            rule_id="P2",
            title="Phoenix pattern",
            risk_flag="LOW",
            confidence="AUTO",
            narrative=(
                "No live company in scope was incorporated within the phoenix window "
                f"({PHOENIX_WINDOW_YEARS_BEFORE} year before to {PHOENIX_WINDOW_YEARS_AFTER} "
                "years after) of a dissolved/liquidated company with a closely matching name."
            ),
            recommendation="",
        )
    lines = [
        f"{m['old_company']} → {m['new_company']} ({m['similarity']}% name match)"
        for m in matches[:5]
    ]
    extra = f"\n…and {len(matches) - 5} more." if len(matches) > 5 else ""
    return CrossAnalysisResult(
        rule_id="P2",
        title="Phoenix pattern",
        risk_flag="HIGH",
        confidence="LIMITED",
        narrative=(
            f"Detected {len(matches)} potential phoenix link(s) — a live company "
            "incorporated within the phoenix window "
            f"({PHOENIX_WINDOW_YEARS_BEFORE} year before to {PHOENIX_WINDOW_YEARS_AFTER} "
            "years after) of a dissolved/liquidated company in the subject's footprint, "
            "with a closely matching distinctive name:\n\n"
            + "\n".join(f"• {line}" for line in lines)
            + extra
        ),
        recommendation=(
            "Phoenix patterns can indicate avoidance of creditor obligations. "
            "Cross-check trading names, premises and customer continuity before progressing."
        ),
    )


def run_person_edd(
    subject: PersonSubject,
    companies: List[PersonCompanyRecord],
    api_key: Optional[str] = None,
    token_bucket: Any = None,
) -> PersonEDDReport:
    """Run every person-EDD rule and assemble the aggregated report.

    ``api_key`` / ``token_bucket`` are required to populate the
    insolvent-company table and the Phoenix subsection's liquidation
    date/type columns (they drive the shared classify_insolvency helper).
    Without them the report still renders but those columns show as ``—``.
    """
    if not subject:
        raise ValueError("subject is required")

    insolvent_companies, classify_cache = _collect_insolvent_companies(
        companies, api_key, token_bucket,
    )

    phoenix_matches: List[Dict[str, Any]] = []

    results: List[CrossAnalysisResult] = []
    results.append(rule_p1_insolvency_pattern(companies, classify_cache))
    # Phoenix: collect name-similar candidates, enrich with liquidation dates,
    # then keep only those inside the incorporation window before building the
    # finding from the filtered set.
    rule_p2_phoenix_signal(companies, phoenix_matches=phoenix_matches)
    _enrich_phoenix_matches(phoenix_matches, classify_cache)
    _filter_phoenix_window(phoenix_matches)
    results.append(_build_phoenix_result(phoenix_matches))
    results.append(rule_p3_mass_resignation(companies))

    p4_result, by_addr = rule_p4_address_clustering(companies)
    results.append(p4_result)

    p5_result, co_counts = rule_p5_codirector_density(subject, companies)
    results.append(p5_result)

    p6_result, grants_value, grants_count = rule_p6_grant_footprint(companies)
    results.append(p6_result)

    results.append(rule_p7_offshore_pscs(companies))
    results.append(rule_p8_overdue_accounts(companies))
    results.append(rule_p9_overdue_confirmation(companies))

    fetch_errors = [(c.company_number, c.fetch_error) for c in companies if c.fetch_error]
    if fetch_errors:
        log_message(f"Person EDD: {len(fetch_errors)} companies had fetch errors.")

    return PersonEDDReport(
        subject=subject,
        companies=companies,
        results=results,
        co_director_counts=co_counts,
        address_clusters={addr: cnums for addr, cnums in by_addr.items() if len(cnums) >= 2},
        grants_total_value=grants_value,
        grants_total_count=grants_count,
        fetch_errors=fetch_errors,
        insolvent_companies=insolvent_companies,
        phoenix_matches=phoenix_matches,
    )


# ---------------------------------------------------------------------------
# Per-company fetch — assembles a PersonCompanyRecord from CH responses
# ---------------------------------------------------------------------------

def build_company_record(
    subject: PersonSubject,
    company_number: str,
    profile: Optional[Dict],
    officers: Optional[Dict],
    pscs: Optional[Dict],
    insolvency: Optional[Dict],
    grants: Optional[List[Dict]],
    charges: Optional[Dict] = None,
    profile_error: Optional[str] = None,
    appt_info: Optional[Dict[str, Any]] = None,
) -> PersonCompanyRecord:
    """Assemble a single PersonCompanyRecord from raw CH responses.

    The director_search module already fetches profile/officers/PSCs in
    ``_fetch_company_network_data``. Insolvency and grants are fetched
    separately by the caller (see modules/director_search.py).
    """
    record = PersonCompanyRecord(company_number=company_number, fetch_error=profile_error)
    if appt_info:
        roles = appt_info.get("roles") or set()
        if roles:
            record.subject_role = _format_role_combo(roles)
        record.subject_appointed_on = appt_info.get("appointed_on")
        record.subject_resigned_on = appt_info.get("resigned_on")
    if not profile:
        return record

    record.company_name = profile.get("company_name", "") or ""
    record.company_status = profile.get("company_status", "") or ""
    record.incorporated_on = profile.get("date_of_creation")
    record.dissolved_on = profile.get("date_of_cessation")
    record.has_been_liquidated = bool(profile.get("has_been_liquidated", False))
    record.sic_codes = list(profile.get("sic_codes") or [])

    # Filing compliance — the profile carries boolean "overdue" flags and the
    # next-due dates, so no extra API call is needed.
    accounts = profile.get("accounts") or {}
    record.accounts_overdue = bool(accounts.get("overdue"))
    record.next_accounts_due = accounts.get("next_due")
    cs = profile.get("confirmation_statement") or {}
    record.confirmation_statement_overdue = bool(cs.get("overdue"))
    record.next_cs_due = cs.get("next_due")

    addr_raw = extract_address_string(profile.get("registered_office_address"))
    record.registered_address_raw = addr_raw
    record.registered_address_clean = clean_address_string(addr_raw)

    # Subject's role on this company.  ``appt_info`` (when supplied by the
    # Director Research caller) is the source of truth — those values came
    # directly from the subject's /officers/{id}/appointments payload.  The
    # /officers list lookup below remains as a fallback for callers that
    # don't pass appt_info; it also still drives co-officer collection.
    have_appt_info = bool(appt_info and appt_info.get("roles"))
    if officers and officers.get("items"):
        for off in officers["items"]:
            name = off.get("name")
            dob = off.get("date_of_birth")
            if _subject_matches(subject, name, dob):
                if have_appt_info:
                    # Subject identified — don't overwrite the appt-info data.
                    continue
                # Take the most recent / longest-serving match
                if not record.subject_appointed_on or (
                    off.get("appointed_on", "") > (record.subject_appointed_on or "")
                ):
                    record.subject_appointed_on = off.get("appointed_on")
                    record.subject_resigned_on = off.get("resigned_on")
                    record.subject_role = _format_role_combo({(off.get("officer_role") or "").lower()})
            else:
                record.co_officers.append({
                    "name": name,
                    "dob": dob,
                    "role": off.get("officer_role"),
                    "appointed_on": off.get("appointed_on"),
                    "resigned_on": off.get("resigned_on"),
                    "nationality": off.get("nationality"),
                    "occupation": off.get("occupation"),
                    "country_of_residence": off.get("country_of_residence"),
                })

    # PSC freebie: did the subject also appear as a PSC on this company?
    if pscs and pscs.get("items"):
        for psc in pscs["items"]:
            name = psc.get("name")
            dob = psc.get("date_of_birth")
            natures = psc.get("natures_of_control") or []
            if _subject_matches(subject, name, dob):
                record.subject_is_psc = True
                record.subject_psc_natures = list(natures)
            else:
                ident = psc.get("identification") or {}
                country = (
                    psc.get("country_of_residence")
                    or ident.get("country_registered")
                )
                record.co_pscs.append({
                    "name": name,
                    "dob": dob,
                    "natures": natures,
                    "country": country,
                    "nationality": psc.get("nationality"),
                })

    # Insolvency cases
    if insolvency and insolvency.get("cases"):
        record.insolvency_cases = list(insolvency["cases"])

    # Grants
    if grants:
        record.grants = list(grants)

    # Charges (secured lending) — informational, not a risk flag.
    if charges and charges.get("items"):
        for ch in charges["items"]:
            lenders = []
            for pe in (ch.get("persons_entitled") or []):
                nm = (pe.get("name") or "").strip()
                if nm:
                    lenders.append(nm)
            record.charges.append({
                "lenders": lenders,
                "status": (ch.get("status") or "").strip().lower(),
                "created_on": ch.get("created_on"),
            })

    return record
