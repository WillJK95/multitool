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
    if cleaned_num.startswith(("SC", "NI", "OC", "LP", "SL", "SO", "NC", "NL")):
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


def get_canonical_name_key(name: str, dob_obj: Optional[Dict] = None) -> str:
    """
    Generate a canonical key for matching people across different records.
    
    Creates a simplified key from first and last name tokens, optionally
    combined with date of birth for disambiguation.
    
    Args:
        name: Full name string
        dob_obj: Optional dict with 'year' and 'month' keys
        
    Returns:
        Canonical key string for matching
    """
    if not name:
        return ""
    
    cleaned_name = name.lower()
    
    # Remove common titles
    titles = ["mr", "mrs", "ms", "miss", "dr", "prof", "sir", "dame", "rev"]
    for title in titles:
        cleaned_name = re.sub(
            r"\b" + re.escape(title) + r"\b\.?", "", cleaned_name
        ).strip()
    
    # Handle "SURNAME, Forename" format
    if "," in cleaned_name:
        parts = cleaned_name.split(",", 1)
        cleaned_name = f"{parts[1].strip()} {parts[0].strip()}"
    
    # Remove non-alphanumeric characters
    cleaned_name = re.sub(r"[^a-z0-9\s]", "", cleaned_name)
    
    tokens = cleaned_name.split()
    if not tokens:
        return ""
    
    # Create key from first and last name tokens
    name_key = tokens[0] + tokens[-1] if len(tokens) > 1 else tokens[0]
    
    # Add DOB if available for disambiguation
    if dob_obj and "year" in dob_obj and "month" in dob_obj:
        return f"{name_key}-{dob_obj['year']}-{dob_obj['month']}"
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
