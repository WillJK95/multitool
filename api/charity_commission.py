# multitool/api/charity_commission.py
"""Charity Commission API client."""

import threading
import time
import urllib.parse
import requests
from typing import Tuple, Optional, Any, Dict, List

from ..constants import CHARITY_API_BASE_URL, DEFAULT_MAX_RETRIES, DEFAULT_BACKOFF_FACTOR
from ..utils.helpers import log_message

# Success-only cache — errors are never stored so transient failures (e.g. a
# 429 that exhausts retries) do not permanently block a charity for the session.
# Using the same pattern as the Companies House client.
_cache: Dict[Tuple, Tuple] = {}
_cache_lock = threading.Lock()
_CACHE_MAX_SIZE = 1024


def cc_get_data(
    api_key: str,
    path: str,
    retries: int = DEFAULT_MAX_RETRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR
) -> Tuple[Optional[Any], Optional[str]]:
    """
    Make a GET request to the Charity Commission API.

    Implements intelligent retries with exponential backoff for transient errors.
    Successful results are cached to avoid repeated API calls. Failed results
    are NOT cached so that transient errors (e.g. 429) can be retried.

    Args:
        api_key: Charity Commission API subscription key
        path: API endpoint path (e.g., "/charitydetails/123456/0")
        retries: Maximum number of retry attempts
        backoff_factor: Base delay multiplier for exponential backoff

    Returns:
        Tuple of (data or None, error message or None)
    """
    if not api_key:
        return None, "Charity Commission API Key is missing."

    cache_key = (api_key, path)
    with _cache_lock:
        if cache_key in _cache:
            return _cache[cache_key]

    headers = {
        "Ocp-Apim-Subscription-Key": api_key,
        "Cache-Control": "no-cache"
    }
    url = f"{CHARITY_API_BASE_URL}{path}"

    for i in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)

            # Client errors - don't retry
            if response.status_code in [404, 401, 403]:
                log_message(
                    f"Charity API Client Error {response.status_code} for {url}."
                )
                return None, f"Client Error: {response.status_code}"

            # Server errors - retry with backoff
            if response.status_code in [429, 500, 502, 503, 504]:
                wait_time = backoff_factor * (2 ** i)
                log_message(
                    f"Charity API returned status {response.status_code}. "
                    f"Retrying in {wait_time:.2f}s..."
                )
                time.sleep(wait_time)
                continue

            response.raise_for_status()

            if not response.text:
                return None, "Not Found (Empty Response)"

            # Success - cache the result and add delay to respect rate limits
            result = (response.json(), None)
            with _cache_lock:
                if len(_cache) >= _CACHE_MAX_SIZE:
                    _cache.pop(next(iter(_cache)))
                _cache[cache_key] = result
            time.sleep(0.5)
            return result

        except requests.exceptions.RequestException as e:
            wait_time = backoff_factor * (2 ** i)
            log_message(
                f"Charity API request failed: {e}. Retrying in {wait_time:.2f}s..."
            )
            time.sleep(wait_time)

    return None, f"Error: Failed to get data for {path} after {retries} retries."


def cc_get_charity_details(
    api_key: str,
    reg_num: str,
    suffix: str = "0"
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Get basic details for a charity.
    
    Args:
        api_key: Charity Commission API subscription key
        reg_num: Charity registration number
        suffix: Charity suffix (usually "0" for main charity)
        
    Returns:
        Tuple of (charity details dict or None, error message or None)
    """
    return cc_get_data(api_key, f"/charitydetails/{reg_num}/{suffix}")


def cc_get_trustees(
    api_key: str,
    reg_num: str,
    suffix: str = "0"
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    """
    Get trustees for a charity.
    
    Args:
        api_key: Charity Commission API subscription key
        reg_num: Charity registration number
        suffix: Charity suffix (usually "0" for main charity)
        
    Returns:
        Tuple of (list of trustees or None, error message or None)
    """
    return cc_get_data(api_key, f"/charitytrusteenamesV2/{reg_num}/{suffix}")


def cc_get_financial_history(
    api_key: str,
    reg_num: str,
    suffix: str = "0"
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    """
    Get financial history for a charity.
    
    Args:
        api_key: Charity Commission API subscription key
        reg_num: Charity registration number
        suffix: Charity suffix (usually "0" for main charity)
        
    Returns:
        Tuple of (list of financial years or None, error message or None)
    """
    return cc_get_data(api_key, f"/charityfinancialhistory/{reg_num}/{suffix}")


def cc_search_charities(
    api_key: str,
    search_term: str
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    """
    Search for charities by name.
    
    Args:
        api_key: Charity Commission API subscription key
        search_term: Name to search for
        
    Returns:
        Tuple of (list of matching charities or None, error message or None)
    """
    encoded_term = urllib.parse.quote(search_term)
    return cc_get_data(api_key, f"/allcharitydetailsbyname/{encoded_term}")


def check_api_status(api_key: str) -> bool:
    """
    Check if the Charity Commission API is accessible with the given key.
    
    Args:
        api_key: Charity Commission API subscription key
        
    Returns:
        True if API is accessible, False otherwise
    """
    # Use a well-known charity for the health check (British Red Cross)
    data, error = cc_get_data(api_key, "/charitydetails/220949/0")
    return data is not None
