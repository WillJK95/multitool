# multitool/utils/helpers.py
"""Shared utility functions used across modules."""

import os
import re
import textwrap
from datetime import datetime
from typing import Any, Dict, Optional

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
