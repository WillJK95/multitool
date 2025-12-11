# multitool/api/grantnav.py
"""360Giving GrantNav API client."""

import time
import threading
import requests
from typing import Tuple, Optional, Any, Dict, List

from ..constants import GRANTNAV_API_BASE_URL

# Thread-safe rate limiting
_rate_limit_lock = threading.Lock()
_last_request_time = 0.0


def grantnav_get_data(url: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Make a GET request to the GrantNav API.
    
    Args:
        url: Full URL to request
        
    Returns:
        Tuple of (data dict or None, error message or None)
    """
    global _last_request_time
    
    try:
        # Thread-safe rate limiting (2 requests/second max)
        # Calculate sleep time inside lock, but sleep outside it
        sleep_time = 0
        with _rate_limit_lock:
            elapsed = time.time() - _last_request_time
            if elapsed < 0.5:
                sleep_time = 0.5 - elapsed
            _last_request_time = time.time() + sleep_time  # Reserve our slot
        
        if sleep_time > 0:
            time.sleep(sleep_time)
        
        resp = requests.get(
            url,
            headers={"Accept": "application/json"},
            timeout=30
        )
        
        if resp.status_code == 404:
            return None, "not_found"
        
        resp.raise_for_status()
        
        return resp.json(), None
        
    except requests.exceptions.HTTPError as e:
        return None, f"HTTP Error {e.response.status_code}"
    except requests.exceptions.RequestException as e:
        return None, f"Network error: {e}"


def search_grants_by_org_id(
    org_id: str,
    page_size: int = 100
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Search for grants by organisation identifier.
    
    Args:
        org_id: Organisation identifier (e.g., company number, charity number)
        page_size: Number of results per page
        
    Returns:
        Tuple of (grants data dict or None, error message or None)
    """
    import urllib.parse
    encoded_id = urllib.parse.quote(org_id)
    url = f"{GRANTNAV_API_BASE_URL}/grants?recipientOrganization.id={encoded_id}&page_size={page_size}"
    return grantnav_get_data(url)


def search_grants_by_org_name(
    org_name: str,
    page_size: int = 100
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Search for grants by organisation name.
    
    Args:
        org_name: Organisation name to search for
        page_size: Number of results per page
        
    Returns:
        Tuple of (grants data dict or None, error message or None)
    """
    import urllib.parse
    encoded_name = urllib.parse.quote(org_name)
    url = f"{GRANTNAV_API_BASE_URL}/grants?recipientOrganization.name={encoded_name}&page_size={page_size}"
    return grantnav_get_data(url)


def get_all_grants_for_org(
    org_id: str,
    max_pages: int = 10
) -> List[Dict[str, Any]]:
    """
    Get all grants for an organisation, handling pagination.
    
    Args:
        org_id: Organisation identifier
        max_pages: Maximum number of pages to fetch
        
    Returns:
        List of all grant records
    """
    all_grants = []
    page = 1
    
    while page <= max_pages:
        import urllib.parse
        encoded_id = urllib.parse.quote(org_id)
        url = f"{GRANTNAV_API_BASE_URL}/grants?recipientOrganization.id={encoded_id}&page={page}&page_size=100"
        
        data, error = grantnav_get_data(url)
        
        if error or not data:
            break
        
        grants = data.get("results", [])
        if not grants:
            break
        
        all_grants.extend(grants)
        
        # Check if there are more pages
        total = data.get("total", 0)
        if len(all_grants) >= total:
            break
        
        page += 1
    
    return all_grants


def check_api_status() -> bool:
    """
    Check if the GrantNav API is accessible.
    
    Returns:
        True if API is accessible, False otherwise
    """
    url = f"{GRANTNAV_API_BASE_URL}/grants?page_size=1"
    data, error = grantnav_get_data(url)
    return data is not None
