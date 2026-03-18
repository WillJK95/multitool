# multitool/api/companies_house.py
"""Companies House API client."""

import threading
import time
import requests
from typing import Tuple, Optional, Any, Dict

from ..constants import API_BASE_URL, DEFAULT_MAX_RETRIES, DEFAULT_BACKOFF_FACTOR
from ..utils.helpers import log_message

# Thread-safe success-only cache (errors are never cached so retries work)
_cache = {}
_cache_lock = threading.Lock()
_CACHE_MAX_SIZE = 1024


def ch_get_data(
    api_key: str,
    token_bucket,
    path: str,
    is_psc: bool = False,
    retries: int = DEFAULT_MAX_RETRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Make a GET request to the Companies House API.

    Implements intelligent retries with exponential backoff for transient errors.
    Successful results are cached to avoid repeated API calls. Failed results
    are NOT cached so that transient errors (e.g. 429) can be retried.

    Args:
        api_key: Companies House API key
        token_bucket: TokenBucket instance for rate limiting
        path: API endpoint path (e.g., "/company/12345678")
        is_psc: Whether this is a PSC-related request (unused, kept for compatibility)
        retries: Maximum number of retry attempts
        backoff_factor: Base delay multiplier for exponential backoff

    Returns:
        Tuple of (data dict or None, error message or None)
    """
    cache_key = (api_key, path, is_psc)

    with _cache_lock:
        if cache_key in _cache:
            return _cache[cache_key]

    token_bucket.consume()
    url = f"{API_BASE_URL}{path}"

    for i in range(retries):
        try:
            response = requests.get(url, auth=(api_key, ""), timeout=30)

            # Client errors - don't retry
            if response.status_code in [404, 401, 403]:
                log_message(
                    f"Client Error {response.status_code} for {path}. "
                    "This is a final error and will not be retried."
                )
                return None, f"Client Error: {response.status_code}"

            # Server errors - retry with backoff
            if response.status_code in [429, 500, 502, 503, 504]:
                wait_time = backoff_factor * (2 ** i)
                log_message(
                    f"API returned status {response.status_code}. "
                    f"Retrying in {wait_time:.2f}s..."
                )
                time.sleep(wait_time)
                continue

            response.raise_for_status()

            # Success - cache the result
            result = (response.json(), None)
            with _cache_lock:
                if len(_cache) >= _CACHE_MAX_SIZE:
                    # Evict oldest entry
                    _cache.pop(next(iter(_cache)))
                _cache[cache_key] = result
            return result

        except requests.exceptions.RequestException as e:
            wait_time = backoff_factor * (2 ** i)
            log_message(f"Request failed: {e}. Retrying in {wait_time:.2f}s...")
            time.sleep(wait_time)

    # All retries exhausted - NOT cached so future attempts can retry
    return None, f"Error: Failed to get data for {path} after {retries} retries."


def ch_search_officers(
    api_key: str,
    token_bucket,
    query: str,
    items_per_page: int = 100
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Search for officers by name.
    
    Args:
        api_key: Companies House API key
        token_bucket: TokenBucket instance for rate limiting
        query: Name to search for
        items_per_page: Number of results per page (max 100)
        
    Returns:
        Tuple of (search results dict or None, error message or None)
    """
    import urllib.parse
    encoded_query = urllib.parse.quote(query)
    path = f"/search/officers?q={encoded_query}&items_per_page={items_per_page}"
    return ch_get_data(api_key, token_bucket, path)


def ch_search_companies(
    api_key: str,
    token_bucket,
    query: str,
    items_per_page: int = 100
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Search for companies by name.
    
    Args:
        api_key: Companies House API key
        token_bucket: TokenBucket instance for rate limiting
        query: Company name to search for
        items_per_page: Number of results per page (max 100)
        
    Returns:
        Tuple of (search results dict or None, error message or None)
    """
    import urllib.parse
    encoded_query = urllib.parse.quote(query)
    path = f"/search/companies?q={encoded_query}&items_per_page={items_per_page}"
    return ch_get_data(api_key, token_bucket, path)


def ch_get_company(
    api_key: str,
    token_bucket,
    company_number: str
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Get company profile by company number.
    
    Args:
        api_key: Companies House API key
        token_bucket: TokenBucket instance for rate limiting
        company_number: Company registration number
        
    Returns:
        Tuple of (company profile dict or None, error message or None)
    """
    return ch_get_data(api_key, token_bucket, f"/company/{company_number}")


def ch_get_officers(
    api_key: str,
    token_bucket,
    company_number: str,
    items_per_page: int = 100
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Get officers for a company.
    
    Args:
        api_key: Companies House API key
        token_bucket: TokenBucket instance for rate limiting
        company_number: Company registration number
        items_per_page: Number of results per page
        
    Returns:
        Tuple of (officers dict or None, error message or None)
    """
    path = f"/company/{company_number}/officers?items_per_page={items_per_page}"
    return ch_get_data(api_key, token_bucket, path)


def ch_get_pscs(
    api_key: str,
    token_bucket,
    company_number: str,
    items_per_page: int = 100
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Get Persons with Significant Control for a company.
    
    Args:
        api_key: Companies House API key
        token_bucket: TokenBucket instance for rate limiting
        company_number: Company registration number
        items_per_page: Number of results per page
        
    Returns:
        Tuple of (PSCs dict or None, error message or None)
    """
    path = f"/company/{company_number}/persons-with-significant-control?items_per_page={items_per_page}"
    return ch_get_data(api_key, token_bucket, path)


def ch_get_filing_history(
    api_key: str,
    token_bucket,
    company_number: str,
    items_per_page: int = 100
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Get filing history for a company.
    
    Args:
        api_key: Companies House API key
        token_bucket: TokenBucket instance for rate limiting
        company_number: Company registration number
        items_per_page: Number of results per page
        
    Returns:
        Tuple of (filing history dict or None, error message or None)
    """
    path = f"/company/{company_number}/filing-history?items_per_page={items_per_page}"
    return ch_get_data(api_key, token_bucket, path)


def check_api_status(api_key: str, token_bucket) -> bool:
    """
    Check if the Companies House API is accessible with the given key.
    
    Args:
        api_key: Companies House API key
        token_bucket: TokenBucket instance for rate limiting
        
    Returns:
        True if API is accessible, False otherwise
    """
    # Use a known company number for the health check
    data, error = ch_get_data(api_key, token_bucket, "/company/00000006")
    return data is not None
