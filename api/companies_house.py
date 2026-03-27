# multitool/api/companies_house.py
"""Companies House API client."""

import re
import threading
import time
import urllib.parse
import requests
from typing import Tuple, Optional, Any, Dict

from ..constants import (
    API_BASE_URL, CH_DOCUMENT_API_BASE_URL,
    DEFAULT_MAX_RETRIES, DEFAULT_BACKOFF_FACTOR,
)
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


def _extract_document_id(metadata_url: str) -> Optional[str]:
    """Extract the document/transaction ID from any CH Document API URL.

    The filing history API may return metadata URLs on different hosts
    (e.g. frontend-doc-api.company-information.service.gov.uk) but the
    actual Document API lives at document-api.companieshouse.gov.uk.
    The path is always /document/<id>.
    """
    match = re.search(r'/document/([A-Za-z0-9_-]+)', metadata_url)
    return match.group(1) if match else None


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

    # Construct canonical Document API URL from whatever host variant
    # the filing history API returned.
    doc_id = _extract_document_id(metadata_url)
    if doc_id:
        url = f"{CH_DOCUMENT_API_BASE_URL}/document/{doc_id}"
    else:
        log_message(f"Could not extract document ID from {metadata_url}, using as-is")
        url = metadata_url

    last_error = "Unknown Error"

    for i in range(retries):
        try:
            response = requests.get(
                url,
                auth=(api_key, ""),
                headers={"Accept": "application/json"},
                timeout=30,
            )

            if response.status_code in [404, 401, 403]:
                return None, f"Client Error: {response.status_code}"

            if response.status_code == 429:
                last_error = "Rate Limited (429)"
                wait_time = backoff_factor * (2 ** i)
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
    accept_mime: str = "application/xhtml+xml",
    retries: int = DEFAULT_MAX_RETRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR
) -> Tuple[Optional[str], Optional[str]]:
    """
    Download document content from the Companies House Document API.

    Uses the official two-step redirect workflow:
      1. Request the /content endpoint with auth + Accept headers and
         allow_redirects=False.  The Document API returns a 302 with an
         empty body and a Location header pointing to a presigned S3 URL.
      2. GET the S3 URL with NO auth headers to download the actual file.

    Args:
        api_key: Companies House API key
        token_bucket: TokenBucket instance for rate limiting
        metadata_url: Full URL from filing['links']['document_metadata'].
                      The document ID is extracted and used to construct
                      the canonical content URL on document-api.companieshouse.gov.uk.
        dest_path: Local file path to save the downloaded content
        accept_mime: MIME type to request (default 'application/xhtml+xml').
                     Pass 'application/xml' for older-format filings.
        retries: Maximum retry attempts
        backoff_factor: Base delay multiplier for exponential backoff

    Returns:
        Tuple of (saved file path or None, error message or None)
    """
    token_bucket.consume()

    doc_id = _extract_document_id(metadata_url)
    if doc_id:
        url = f"{CH_DOCUMENT_API_BASE_URL}/document/{doc_id}/content"
    else:
        log_message(f"Could not extract document ID from {metadata_url}, using as-is")
        url = f"{metadata_url}/content"

    last_error = "Unknown Error"

    for i in range(retries):
        try:
            # Step 1: Request content from Document API (do NOT follow redirects)
            response = requests.get(
                url,
                auth=(api_key, ""),
                headers={"Accept": accept_mime},
                allow_redirects=False,
                timeout=30,
            )

            if response.status_code in [404, 401, 403]:
                return None, f"Client Error: {response.status_code}"

            if response.status_code == 429:
                last_error = "Rate Limited (429)"
                wait_time = backoff_factor * (2 ** i)
                log_message(f"Rate limited on document download. Waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue

            if response.status_code in [500, 502, 503, 504]:
                last_error = f"Server Error ({response.status_code})"
                wait_time = backoff_factor * (2 ** i)
                log_message(f"Document download returned {response.status_code}. Retrying in {wait_time:.2f}s...")
                time.sleep(wait_time)
                continue

            # Step 2: Extract S3 presigned URL from the redirect Location header
            if response.status_code in [301, 302]:
                s3_url = response.headers.get("Location")
                if not s3_url:
                    last_error = f"Redirect {response.status_code} but no Location header"
                    log_message(last_error)
                    wait_time = backoff_factor * (2 ** i)
                    time.sleep(wait_time)
                    continue

                # Step 3: Download from S3 with NO auth headers
                s3_response = requests.get(s3_url, timeout=60)
                s3_response.raise_for_status()

                if not s3_response.content:
                    last_error = "S3 response body is empty"
                    log_message(last_error)
                    wait_time = backoff_factor * (2 ** i)
                    time.sleep(wait_time)
                    continue

                with open(dest_path, "wb") as f:
                    f.write(s3_response.content)
                return dest_path, None

            # Defensive: if CH ever serves content directly (200 with body)
            if response.status_code == 200 and response.content:
                with open(dest_path, "wb") as f:
                    f.write(response.content)
                return dest_path, None

            last_error = f"Unexpected status {response.status_code}"
            log_message(f"Document download: {last_error} for {url}")
            wait_time = backoff_factor * (2 ** i)
            time.sleep(wait_time)

        except requests.exceptions.RequestException as e:
            last_error = f"Connection Error: {e}"
            wait_time = backoff_factor * (2 ** i)
            log_message(f"Document download failed: {e}. Retrying in {wait_time:.2f}s...")
            time.sleep(wait_time)

    return None, last_error
