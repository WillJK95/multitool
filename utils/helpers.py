# multitool/utils/helpers.py
"""Shared utility functions used across modules."""

import os
import re
import textwrap
from datetime import datetime
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

from ..constants import CONFIG_DIR


def log_message(message: str) -> None:
    """Log a message to the application log file."""
    os.makedirs(CONFIG_DIR, exist_ok=True)
    log_file = os.path.join(CONFIG_DIR, "app.log")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


def clean_company_number(cnum_raw: Optional[str]) -> Optional[str]:
    """
    Clean and format a company number to standard format.
    
    Handles:
    - Stripping whitespace
    - Uppercase conversion
    - Zero-padding numeric company numbers to 8 digits
    - Preserving prefix for Scottish, NI, etc. companies
    
    Args:
        cnum_raw: Raw company number string
        
    Returns:
        Cleaned company number or None if invalid
    """
    if not cnum_raw or not isinstance(cnum_raw, str):
        return None
    
    cleaned_num = cnum_raw.strip().upper()
    
    # Check for prefixed company numbers (Scotland, NI, etc.)
    if cleaned_num.startswith(("SC", "NI", "OC", "LP", "SL", "SO", "NC", "NL", "R0", "ZC")):
        return cleaned_num
    elif cleaned_num.isdigit():
        # Zero-pad numeric company numbers to 8 digits
        return cleaned_num.zfill(8)
    
    return cleaned_num


def clean_address_string(address: Optional[str]) -> Optional[str]:
    """
    Clean an address string for consistent matching.
    
    Normalises case, removes extra whitespace, and standardises punctuation.
    
    Args:
        address: Raw address string
        
    Returns:
        Cleaned address string or None if empty
    """
    if not address:
        return None
    
    # Lowercase and strip
    cleaned = address.lower().strip()
    
    # Remove extra whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # Standardise common variations
    cleaned = cleaned.replace('.', '')
    cleaned = cleaned.replace(',', ', ')
    cleaned = re.sub(r',\s+', ', ', cleaned)
    
    return cleaned if cleaned else None


def get_canonical_name_key(name: str, dob_obj: dict = None) -> str:
    if not name:
        return ""
    
    # 1. Standardize Case
    cleaned_name = name.lower()

    # 2. Handle "Surname, Firstname" format (Critical for Companies House data)
    if "," in cleaned_name:
        parts = cleaned_name.split(",", 1)
        cleaned_name = f"{parts[1].strip()} {parts[0].strip()}"

    # 3. Remove non-alphanumeric (punctuations/brackets)
    cleaned_name = re.sub(r"[^a-z0-9\s]", "", cleaned_name)
    tokens = cleaned_name.split()

    if not tokens:
        return ""

    # 4. Generate Key
    name_key = tokens[0] + tokens[-1] if len(tokens) > 1 else tokens[0]

    # 5. Append DOB
    if dob_obj and "year" in dob_obj and "month" in dob_obj:
        try:
            return f"{name_key}-{dob_obj['year']}-{int(dob_obj['month']):02d}"
        except (ValueError, TypeError):
            return name_key
    else:
        return name_key


def format_address_label(address_str: str, line_length: int = 25) -> str:
    """
    Format an address string for display in graph labels.
    
    Wraps long addresses to multiple lines for better graph readability.
    
    Args:
        address_str: Address string to format
        line_length: Maximum characters per line
        
    Returns:
        Formatted address with line breaks
    """
    if not address_str:
        return ""
    return "\n".join(textwrap.wrap(address_str, width=line_length))


def get_nested_value(data_dict: Dict, key_path: str, default: Any = "") -> Any:
    """
    Get a value from a nested dictionary using dot notation.
    
    Args:
        data_dict: Dictionary to search
        key_path: Dot-separated path (e.g., "address.postal_code")
        default: Default value if path not found
        
    Returns:
        Value at path or default
    """
    keys = key_path.split(".")
    value = data_dict
    
    try:
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key, default)
            else:
                return default
        return value if value is not None else default
    except (KeyError, TypeError):
        return default

def extract_address_string(addr_data: Optional[Dict]) -> Optional[str]:
    """
    Extract a formatted address string from an address dictionary.
    Works with Companies House address format containing fields like
    address_line_1, address_line_2, locality, region, postal_code, country.
    Args:
        addr_data: Address dictionary from API response
    Returns:
        Comma-separated address string or None if no valid data
    """
    if not addr_data or not isinstance(addr_data, dict):
        return None

    raw_address_str = ", ".join(
        filter(
            None,
            [
                addr_data.get("address_line_1"),
                addr_data.get("address_line_2"),
                addr_data.get("locality"),
                addr_data.get("region"),
                addr_data.get("postal_code"),
            ],
        )
    )

    return raw_address_str if raw_address_str else None


def _friendly_error_label(error_string: str) -> str:
    """Normalise a ch_get_data error string to a short human-friendly label."""
    if not error_string:
        return "Unknown Error"
    e = error_string.lower()
    if "404" in e:
        return "Not Found"
    if "401" in e:
        return "Unauthorized"
    if "403" in e:
        return "Forbidden"
    if "rate limited" in e or "429" in e:
        return "Rate Limited"
    if "excessive use" in e:
        return "Rate Limited"
    if "server error" in e or "500" in e or "502" in e or "503" in e or "504" in e:
        return "Server Error"
    if "connection error" in e:
        return "Connection Error"
    return "Error"


def format_error_summary(
    failures: List[Tuple[str, str]],
    item_type: str = "company",
) -> str:
    """
    Build a categorised error summary from a list of (identifier, error) tuples.

    Logs a per-item breakdown and returns a short grouped string suitable for
    the status bar, e.g. "5 company(ies) failed (3 Not Found, 2 Rate Limited)".

    Args:
        failures: List of (identifier, raw_error_string) tuples.
        item_type: Noun to use in the summary (e.g. "company", "officer", "row").

    Returns:
        A summary string for display in the UI status bar.
    """
    if not failures:
        return ""

    # Log per-item details
    for identifier, error in failures:
        label = _friendly_error_label(error)
        log_message(f"Failed {item_type} {identifier}: {label}")

    # Group by friendly label
    counts = Counter(_friendly_error_label(err) for _, err in failures)
    breakdown = ", ".join(f"{count} {label}" for label, count in counts.items())

    n = len(failures)
    plural = f"{item_type}(ies)" if item_type.endswith("y") else f"{item_type}(s)"
    return f"WARNING: {n} {plural} failed ({breakdown})."



def format_eta(elapsed_sec: float, processed: int, total: int,
               rate_limit_wait: float = 0.0) -> str:
    """Return a human-readable ETA string incorporating rate-limit overhead.

    Args:
        elapsed_sec: Seconds elapsed since processing began.
        processed: Number of items completed so far.
        total: Total number of items to process.
        rate_limit_wait: Estimated additional seconds of rate-limit waiting
            still ahead (from TokenBucket.estimate_wait_seconds). Defaults to
            0.0 (time-based extrapolation only).

    Returns:
        A short ETA string such as "~3-4 minutes" or "< 2 minutes".
    """
    remaining = total - processed
    if processed == 0 or elapsed_sec < 2 or remaining <= 0:
        return "calculating..."
    rate = processed / elapsed_sec  # items per second
    time_based_sec = remaining / rate
    # Take the larger of the two estimates: the time-based extrapolation
    # (which includes past rate-limit pauses) and the forward-looking quota
    # calculation (which captures future window resets not yet experienced).
    total_sec = max(time_based_sec, rate_limit_wait)
    if total_sec < 90:
        return "< 2 minutes"
    minutes = total_sec / 60
    low = max(1, round(minutes))
    high = low + 1
    return f"~{low}-{high} minutes"


def match_officer_name_tokens(search_name: str, officer_title: str) -> bool:
    """
    Token-based name matching for CH officer search results.

    Returns True if either name's tokens are a subset of the other
    (after cleaning titles, punctuation, and hyphens).  This ensures
    that searching "sacha lord" matches "sacha john edward lord" *and*
    vice-versa.
    """
    # Normalise both sides identically: remove honorifics/punctuation,
    # replace hyphens with spaces so "lord-marchionne" matches "lord marchionne"
    def _clean(text):
        text = re.sub(
            r"\b(mr|mrs|ms|miss|dr|prof)\b|[.,]", "", text.lower()
        ).strip()
        return set(text.replace("-", " ").split())

    search_tokens = _clean(search_name)
    officer_tokens = _clean(officer_title)
    return search_tokens.issubset(officer_tokens) or officer_tokens.issubset(search_tokens)


# ---------------------------------------------------------------------------
# Presentation helpers (shared by the company and person DD reports)
# ---------------------------------------------------------------------------

# Known label mappings for raw Companies House API values. Anything not listed
# falls back to a sensible title-case so we never show a raw token verbatim.
_STATUS_LABELS = {
    "active": "Active",
    "dissolved": "Dissolved",
    "liquidation": "In liquidation",
    "receivership": "In receivership",
    "administration": "In administration",
    "voluntary-arrangement": "Voluntary arrangement",
    "converted-closed": "Converted / closed",
    "insolvency-proceedings": "Insolvency proceedings",
    "registered": "Registered",
    "removed": "Removed",
    "closed": "Closed",
    "open": "Open",
}

_TYPE_LABELS = {
    "ltd": "Private limited company",
    "private-limited-guarant-nsc": "Private limited by guarantee (no share capital)",
    "private-limited-guarant-nsc-limited-exemption": "Private limited by guarantee (no share capital, exempt)",
    "private-unlimited": "Private unlimited company",
    "private-unlimited-nsc": "Private unlimited (no share capital)",
    "plc": "Public limited company",
    "llp": "Limited liability partnership",
    "limited-partnership": "Limited partnership",
    "old-public-company": "Old public company",
    "private-limited-shares-section-30-exemption": "Private limited (section 30 exemption)",
    "community-interest-company": "Community interest company",
    "charitable-incorporated-organisation": "Charitable incorporated organisation",
    "registered-society-non-jurisdictional": "Registered society",
    "industrial-and-provident-society": "Industrial and provident society",
    "royal-charter": "Royal charter",
    "uk-establishment": "UK establishment",
    "scottish-partnership": "Scottish partnership",
}

_JURISDICTION_LABELS = {
    "england-wales": "England & Wales",
    "england": "England",
    "wales": "Wales",
    "scotland": "Scotland",
    "northern-ireland": "Northern Ireland",
    "united-kingdom": "United Kingdom",
    "great-britain": "Great Britain",
    "european-union": "European Union",
    "non-eu": "Non-EU",
}


def _fallback_titlecase(value: str) -> str:
    """Turn a hyphen/underscore token into a readable title-cased label."""
    return value.replace("-", " ").replace("_", " ").strip().capitalize()


def prettify_status(value: Optional[str]) -> str:
    """Map a raw company_status token (e.g. 'active') to a display label."""
    if not value:
        return "N/A"
    key = value.strip().lower()
    return _STATUS_LABELS.get(key, _fallback_titlecase(value))


def prettify_company_type(value: Optional[str]) -> str:
    """Map a raw company type token (e.g. 'ltd') to a display label."""
    if not value:
        return "N/A"
    key = value.strip().lower()
    return _TYPE_LABELS.get(key, _fallback_titlecase(value))


def prettify_jurisdiction(value: Optional[str]) -> str:
    """Map a raw jurisdiction token (e.g. 'england-wales') to a display label."""
    if not value:
        return "N/A"
    key = value.strip().lower()
    return _JURISDICTION_LABELS.get(key, _fallback_titlecase(value))


def prettify_role(value: Optional[str]) -> str:
    """Tidy a role label, upper-casing the LLP acronym.

    The role combo is already roughly formatted (e.g. 'Llp Designated Member');
    this just fixes the LLP acronym so it reads 'LLP Designated Member'.
    """
    if not value:
        return value or ""
    return re.sub(r"\bLlp\b", "LLP", value)


def narrative_to_html(text: Optional[str]) -> str:
    """Render a finding narrative as proper HTML.

    Lines beginning with a bullet ('•' or '-') are collapsed into a single
    <ul> so wrapped lines hang-indent against the text rather than the bullet.
    Other blocks of text become <p> elements. Input is treated as untrusted and
    HTML-escaped.
    """
    import html as _html

    if not text:
        return ""
    out: List[str] = []
    bullets: List[str] = []

    def _flush_bullets():
        if bullets:
            items = "".join(f"<li>{_html.escape(b)}</li>" for b in bullets)
            out.append(f"<ul>{items}</ul>")
            bullets.clear()

    for raw_line in text.split("\n"):
        line = raw_line.strip()
        if not line:
            _flush_bullets()
            continue
        if line[0] in "•-" and line[1:2] == " ":
            bullets.append(line[1:].strip())
        elif line.startswith("•"):
            bullets.append(line[1:].strip())
        else:
            _flush_bullets()
            out.append(f"<p>{_html.escape(line)}</p>")
    _flush_bullets()
    return "".join(out)


def account_age_qualifier(account_year: Any, report_date: Optional[datetime] = None) -> str:
    """Return a staleness qualifier for a financial year, e.g.
    'as of FY2023 — now 3 years old'.

    Returns an empty string if the year can't be parsed.
    """
    try:
        yr = int(account_year)
    except (TypeError, ValueError):
        return ""
    report_year = (report_date or datetime.now()).year
    age = report_year - yr
    if age <= 0:
        return f"as of FY{yr}"
    return f"as of FY{yr} — now {age} year{'s' if age != 1 else ''} old"
