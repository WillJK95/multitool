# multitool/api/grantnav.py
"""360Giving GrantNav API client."""

import time
import threading
import urllib.parse
import requests
from typing import Tuple, Optional, Any, Dict, List

from ..constants import GRANTNAV_API_BASE_URL

# Thread-safe rate limiting
_rate_limit_lock = threading.Lock()
_last_request_time = 0.0


def grantnav_get_data(
    url: str,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Make a GET request to the GrantNav API with retry logic.
    
    Args:
        url: Full URL to request
        max_retries: Maximum number of retry attempts on failure
        retry_delay: Initial delay between retries (doubles each retry)
        
    Returns:
        Tuple of (data dict or None, error message or None)
    """
    global _last_request_time
    
    last_error = None
    current_delay = retry_delay
    
    for attempt in range(max_retries):
        try:
            # Thread-safe rate limiting (2 requests/second max)
            with _rate_limit_lock:
                elapsed = time.time() - _last_request_time
                if elapsed < 0.5:
                    sleep_time = 0.5 - elapsed
                else:
                    sleep_time = 0
                _last_request_time = time.time() + sleep_time
            
            # Sleep outside the lock to avoid blocking other threads
            if sleep_time > 0:
                time.sleep(sleep_time)
            
            resp = requests.get(
                url,
                headers={"Accept": "application/json"},
                timeout=30
            )
            
            if resp.status_code == 200:
                return resp.json(), None
            
            elif resp.status_code == 404:
                return None, "not_found"
            
            elif resp.status_code == 429:
                # Rate limited - wait and retry
                last_error = "Rate limited by GrantNav API"
                time.sleep(current_delay)
                current_delay *= 2
                continue
            
            elif resp.status_code >= 500:
                # Server error - retry
                last_error = f"GrantNav server error: {resp.status_code}"
                time.sleep(current_delay)
                current_delay *= 2
                continue
            
            else:
                # Other client error - don't retry
                return None, f"HTTP Error {resp.status_code}"
            
        except requests.exceptions.Timeout:
            last_error = "Request timed out"
            time.sleep(current_delay)
            current_delay *= 2
            
        except requests.exceptions.ConnectionError as e:
            last_error = f"Connection error: {str(e)}"
            time.sleep(current_delay)
            current_delay *= 2
            
        except requests.exceptions.RequestException as e:
            # Don't retry on other request errors
            return None, f"Network error: {e}"
    
    # All retries exhausted
    return None, f"Request failed after {max_retries} attempts: {last_error}"


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
        List of grant dictionaries
    """
    all_grants = []
    encoded_id = urllib.parse.quote(org_id)
    url = f"{GRANTNAV_API_BASE_URL}/grants?recipientOrganization.id={encoded_id}&page_size=100"
    
    for page in range(max_pages):
        data, error = grantnav_get_data(url)
        
        if error or not data:
            break
            
        grants = data.get("grants", [])
        if not grants:
            break
            
        all_grants.extend(grants)
        
        # Check for next page
        next_url = data.get("next")
        if not next_url:
            break
        url = next_url
    
    return all_grants
