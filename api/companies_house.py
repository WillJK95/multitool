# multitool/api/companies_house.py
"""Companies House API client."""

import threading
import time
import urllib.parse
import requests
from typing import Tuple, Optional, Any, Dict

from ..constants import API_BASE_URL, DEFAULT_MAX_RETRIES, DEFAULT_BACKOFF_FACTOR
from ..utils.helpers import log_message

# Thread-safe success-only cache (errors are never cached so retries work)
_cache = {}
_cache_lock = threading.Lock()
_CACHE_MAX_SIZE = 1024


def _safe_json(response: requests.Response) -> Optional[Dict[str, Any]]:
    """Return parsed JSON body or None if the body isn't valid JSON."""
    try:
        return response.json()
    except (ValueError, AttributeError):
        return None


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
    last_error_reason = "Unknown Error"

    for i in range(retries):
        try:
            response = requests.get(url, auth=(api_key, ""), timeout=30)

            # Sync token bucket with server-reported rate limit state on
            # every response (success or failure) so we stay aligned.
            token_bucket.sync_from_headers(response.headers)

            # Client errors - don't retry
            if response.status_code in [404, 401, 403]:
                log_message(
                    f"Client Error {response.status_code} for {path}. "
                    "This is a final error and will not be retried."
                )
                return None, f"Client Error: {response.status_code}"

            # Excessive-use response - the API has temporarily blocked us.
            # Wait for the full window reset before retrying.
            if response.status_code == 429:
                last_error_reason = "Rate Limited (429)"
                body = _safe_json(response)
                if body and body.get("type") == "ch:service/excessive-use":
                    last_error_reason = "Excessive Use Block (429)"
                    reset_wait = token_bucket.get_wait_from_reset(
                        response.headers
                    )
                    wait_time = reset_wait if reset_wait else 300.0
                    log_message(
                        f"Excessive-use block received for {path}. "
                        f"Waiting {wait_time:.0f}s for window reset..."
                    )
                    time.sleep(wait_time)
                    continue

                # Standard 429 - use the reset header for precise wait time
                reset_wait = token_bucket.get_wait_from_reset(
                    response.headers
                )
                if reset_wait is not None:
                    log_message(
                        f"Rate limited (429) for {path}. "
                        f"Waiting {reset_wait:.1f}s until window reset..."
                    )
                    time.sleep(reset_wait)
                    continue

                # Fallback: exponential backoff if headers are absent
                wait_time = backoff_factor * (2 ** i)
                log_message(
                    f"Rate limited (429) for {path}. "
                    f"Retrying in {wait_time:.2f}s (no reset header)..."
                )
                time.sleep(wait_time)
                continue

            # Server errors - retry with backoff
            if response.status_code in [500, 502, 503, 504]:
                last_error_reason = f"Server Error ({response.status_code})"
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
            last_error_reason = "Connection Error"
            wait_time = backoff_factor * (2 ** i)
            log_message(f"Request failed: {e}. Retrying in {wait_time:.2f}s...")
            time.sleep(wait_time)

    # All retries exhausted - NOT cached so future attempts can retry
    return None, last_error_reason


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
    Get all officers for a company, automatically paginating beyond 100 results.

    Each page is fetched via ch_get_data and benefits from the response cache.
    The merged result has the same structure as a single-page response so all
    existing callers continue to work without changes.

    Args:
        api_key: Companies House API key
        token_bucket: TokenBucket instance for rate limiting
        company_number: Company registration number
        items_per_page: Results per page (max 100 per API limits)

    Returns:
        Tuple of (officers dict with all items, or None, error message or None)
    """
    all_items = []
    start_index = 0
    last_data = None

    while True:
        path = (
            f"/company/{company_number}/officers"
            f"?items_per_page={items_per_page}&start_index={start_index}"
        )
        data, error = ch_get_data(api_key, token_bucket, path)
        if error or not data:
            if not all_items:
                return None, error
            break

        last_data = data
        page_items = data.get("items", [])
        all_items.extend(page_items)

        total_results = data.get("total_results", 0)
        start_index += len(page_items)
        if not page_items or start_index >= total_results:
            break

    if last_data is not None:
        merged = dict(last_data)
        merged["items"] = all_items
        return merged, None
    return {"items": all_items, "total_results": len(all_items)}, None


def ch_get_pscs(
    api_key: str,
    token_bucket,
    company_number: str,
    items_per_page: int = 100
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Get all Persons with Significant Control for a company, automatically
    paginating beyond 100 results.

    Each page is fetched via ch_get_data and benefits from the response cache.
    The merged result has the same structure as a single-page response so all
    existing callers continue to work without changes.

    Args:
        api_key: Companies House API key
        token_bucket: TokenBucket instance for rate limiting
        company_number: Company registration number
        items_per_page: Results per page (max 100 per API limits)

    Returns:
        Tuple of (PSCs dict with all items, or None, error message or None)
    """
    all_items = []
    start_index = 0
    last_data = None

    while True:
        path = (
            f"/company/{company_number}/persons-with-significant-control"
            f"?items_per_page={items_per_page}&start_index={start_index}"
        )
        data, error = ch_get_data(api_key, token_bucket, path)
        if error or not data:
            if not all_items:
                return None, error
            break

        last_data = data
        page_items = data.get("items", [])
        all_items.extend(page_items)

        total_results = data.get("total_results", 0)
        start_index += len(page_items)
        if not page_items or start_index >= total_results:
            break

    if last_data is not None:
        merged = dict(last_data)
        merged["items"] = all_items
        return merged, None
    return {"items": all_items, "total_results": len(all_items)}, None


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


def ch_get_document_metadata(
    api_key: str,
    token_bucket,
    metadata_url: str,
    retries: int = DEFAULT_MAX_RETRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Fetch document metadata from the Companies House Document API.

    The Document API is a separate service from the main CH REST API.
    Each filing's metadata describes which file formats are available
    (e.g. PDF, iXBRL).

    Args:
        api_key: Companies House API key
        token_bucket: TokenBucket instance for rate limiting
        metadata_url: Full URL from filing['links']['document_metadata']
        retries: Maximum retry attempts
        backoff_factor: Base delay multiplier for exponential backoff

    Returns:
        Tuple of (metadata dict or None, error message or None)
    """
    token_bucket.consume()
    url = metadata_url
    last_error = "Unknown Error"

    for i in range(retries):
        try:
            response = requests.get(url, auth=(api_key, ""), timeout=30)
            token_bucket.sync_from_headers(response.headers)

            if response.status_code in [404, 401, 403]:
                return None, f"Client Error: {response.status_code}"

            if response.status_code == 429:
                last_error = "Rate Limited (429)"
                reset_wait = token_bucket.get_wait_from_reset(response.headers)
                wait_time = reset_wait if reset_wait else backoff_factor * (2 ** i)
                log_message(f"Rate limited on document metadata. Waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue

            if response.status_code in [500, 502, 503, 504]:
                last_error = f"Server Error ({response.status_code})"
                wait_time = backoff_factor * (2 ** i)
                log_message(f"Document API returned {response.status_code}. Retrying in {wait_time:.2f}s...")
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            return response.json(), None

        except requests.exceptions.RequestException as e:
            last_error = f"Connection Error: {e}"
            wait_time = backoff_factor * (2 ** i)
            log_message(f"Document metadata request failed: {e}. Retrying in {wait_time:.2f}s...")
            time.sleep(wait_time)

    return None, last_error


def ch_download_document_content(
    api_key: str,
    token_bucket,
    metadata_url: str,
    dest_path: str,
    retries: int = DEFAULT_MAX_RETRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR
) -> Tuple[Optional[str], Optional[str]]:
    """
    Download iXBRL document content from the Companies House Document API.

    Requests the content endpoint with an Accept header for iXBRL format.
    The Document API returns an HTTP 302 redirect to an AWS S3 bucket;
    the requests library automatically strips the Authorization header
    on cross-domain redirects, which is the correct behaviour.

    Args:
        api_key: Companies House API key
        token_bucket: TokenBucket instance for rate limiting
        metadata_url: Full URL from filing['links']['document_metadata']
        dest_path: Local file path to save the downloaded content
        retries: Maximum retry attempts
        backoff_factor: Base delay multiplier for exponential backoff

    Returns:
        Tuple of (saved file path or None, error message or None)
    """
    token_bucket.consume()
    url = f"{metadata_url}/content"
    headers = {"Accept": "application/xhtml+xml"}
    last_error = "Unknown Error"

    for i in range(retries):
        try:
            response = requests.get(
                url,
                auth=(api_key, ""),
                headers=headers,
                allow_redirects=True,
                timeout=60,
            )
            token_bucket.sync_from_headers(response.headers)

            if response.status_code in [404, 401, 403]:
                return None, f"Client Error: {response.status_code}"

            if response.status_code == 429:
                last_error = "Rate Limited (429)"
                reset_wait = token_bucket.get_wait_from_reset(response.headers)
                wait_time = reset_wait if reset_wait else backoff_factor * (2 ** i)
                log_message(f"Rate limited on document download. Waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue

            if response.status_code in [500, 502, 503, 504]:
                last_error = f"Server Error ({response.status_code})"
                wait_time = backoff_factor * (2 ** i)
                log_message(f"Document download returned {response.status_code}. Retrying in {wait_time:.2f}s...")
                time.sleep(wait_time)
                continue

            response.raise_for_status()

            with open(dest_path, "wb") as f:
                f.write(response.content)
            return dest_path, None

        except requests.exceptions.RequestException as e:
            last_error = f"Connection Error: {e}"
            wait_time = backoff_factor * (2 ** i)
            log_message(f"Document download failed: {e}. Retrying in {wait_time:.2f}s...")
            time.sleep(wait_time)

    return None, last_error
