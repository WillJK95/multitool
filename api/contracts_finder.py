# multitool/api/contracts_finder.py
"""UK Government Contracts Finder API client."""

import time
import threading
import requests
from typing import List, Dict, Tuple, Optional, Callable

from ..constants import CONTRACTS_FINDER_BASE_URL
from ..utils.helpers import log_message

# Thread-safe rate limiting
_rate_limit_lock = threading.Lock()
_last_request_time = 0.0


def _rate_limited_request(
    method: str,
    url: str,
    headers: Dict = None,
    json_data: Dict = None,
    params: Dict = None,
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Make a rate-limited request to the Contracts Finder API.
    """
    global _last_request_time
    
    if headers is None:
        headers = {}
    headers.setdefault("Accept", "application/json")
    headers.setdefault("Content-Type", "application/json")
    
    last_error = None
    current_delay = retry_delay
    
    for attempt in range(max_retries):
        try:
            # Thread-safe rate limiting (max 2 requests/second)
            with _rate_limit_lock:
                elapsed = time.time() - _last_request_time
                if elapsed < 0.5:
                    sleep_time = 0.5 - elapsed
                else:
                    sleep_time = 0
                _last_request_time = time.time() + sleep_time
            
            if sleep_time > 0:
                time.sleep(sleep_time)
            
            # Make the request
            if method.upper() == "POST":
                resp = requests.post(url, headers=headers, json=json_data, timeout=30)
            else:
                resp = requests.get(url, headers=headers, params=params, timeout=30)
            
            if resp.status_code == 200:
                return resp.json(), None
            
            elif resp.status_code == 403:
                # Rate limited - wait (cap at 300s)
                last_error = "Rate limited by Contracts Finder API"
                log_message(f"Contracts Finder rate limit hit, waiting {current_delay}s")
                time.sleep(min(current_delay, 300))
                current_delay *= 2
                continue
            
            elif resp.status_code == 404:
                return None, "Not found"
            
            elif resp.status_code >= 500:
                last_error = f"Server error: {resp.status_code}"
                log_message(f"Contracts Finder server error {resp.status_code}, retry {attempt + 1}/{max_retries}")
                time.sleep(current_delay)
                current_delay *= 2
                continue
            
            else:
                return None, f"HTTP Error {resp.status_code}: {resp.text[:200]}"
                
        except requests.exceptions.Timeout:
            last_error = "Request timed out"
            log_message(f"Contracts Finder timeout, retry {attempt + 1}")
            time.sleep(current_delay)
            current_delay *= 2
            
        except requests.exceptions.ConnectionError as e:
            last_error = f"Connection error: {str(e)}"
            log_message(f"Contracts Finder connection error, retry {attempt + 1}")
            time.sleep(current_delay)
            current_delay *= 2
            
        except requests.exceptions.RequestException as e:
            return None, f"Request failed: {str(e)}"
    
    return None, f"Request failed after {max_retries} attempts: {last_error}"


def search_notices(
    keyword: str = None,
    status: str = None,
    published_from: str = None,
    published_to: str = None,
    awarded_from: str = None,
    awarded_to: str = None,
    value_from: float = None,
    value_to: float = None,
    cpv_codes: List[str] = None,
    regions: str = None,
    suitable_for_sme: bool = None,
    suitable_for_vco: bool = None,
    awarded_to_sme: bool = None,
    awarded_to_vcse: bool = None,
    size: int = 1000,
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Search for contract notices using the Contracts Finder V2 search API.
    
    Args:
        keyword: Search term (searches title, description, organisation name)
        status: Filter by status ('Open', 'Closed', 'Awarded', 'Withdrawn')
        published_from: Filter notices published on or after this date (ISO format)
        published_to: Filter notices published on or before this date (ISO format)
        awarded_from: Filter notices awarded on or after this date (ISO format)
        awarded_to: Filter notices awarded on or before this date (ISO format)
        value_from: Minimum contract value
        value_to: Maximum contract value
        cpv_codes: List of CPV codes to filter by
        regions: Comma-separated region names (e.g., "Wales,South East")
        suitable_for_sme: Filter for SME-suitable contracts
        suitable_for_vco: Filter for VCSE-suitable contracts
        awarded_to_sme: Filter for contracts awarded to SMEs
        awarded_to_vcse: Filter for contracts awarded to VCSEs
        size: Maximum number of results (up to 1000)
        
    Returns:
        Tuple of (search results dict or None, error message or None)
    """
    url = f"{CONTRACTS_FINDER_BASE_URL}/api/rest/2/search_notices/json"
    
    # Build the nested searchCriteria object as per API spec
    search_criteria = {}
    
    if keyword:
        search_criteria["keyword"] = keyword
    
    if status:
        # Status must be in a list
        status_map = {
            "open": "Open",
            "closed": "Closed", 
            "awarded": "Awarded",
            "withdrawn": "Withdrawn",
        }
        search_criteria["statuses"] = [status_map.get(status.lower(), status)]
    
    if published_from:
        search_criteria["publishedFrom"] = published_from
    if published_to:
        search_criteria["publishedTo"] = published_to
    
    if awarded_from:
        search_criteria["awardedFrom"] = awarded_from
    if awarded_to:
        search_criteria["awardedTo"] = awarded_to
    
    if value_from is not None:
        search_criteria["valueFrom"] = value_from
    if value_to is not None:
        search_criteria["valueTo"] = value_to
    
    if cpv_codes:
        search_criteria["cpvCodes"] = cpv_codes
    
    if regions:
        search_criteria["regions"] = regions
    
    if suitable_for_sme is not None:
        search_criteria["suitableForSme"] = suitable_for_sme
    
    if suitable_for_vco is not None:
        search_criteria["suitableForVco"] = suitable_for_vco
    
    if awarded_to_sme is not None:
        search_criteria["awardedToSme"] = awarded_to_sme
    
    if awarded_to_vcse is not None:
        search_criteria["awardedToVcse"] = awarded_to_vcse
    
    # Build the full request body
    request_body = {
        "searchCriteria": search_criteria,
        "size": min(size, 1000)
    }
    
    return _rate_limited_request("POST", url, json_data=request_body)


def get_notice(notice_id: str) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Get full details of a specific notice by ID.
    
    Args:
        notice_id: The GUID identifier of the notice
        
    Returns:
        Tuple of (notice data dict or None, error message or None)
    """
    url = f"{CONTRACTS_FINDER_BASE_URL}/api/rest/2/get_published_notice/json/{notice_id}"
    return _rate_limited_request("GET", url)


def get_notice_ocds(notice_id: str) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Get a notice in OCDS (Open Contracting Data Standard) format.
    """
    url = f"{CONTRACTS_FINDER_BASE_URL}/Published/Notice/OCDS/{notice_id}"
    return _rate_limited_request("GET", url)


def search_ocds(
    published_from: str = None,
    published_to: str = None,
    stages: List[str] = None,
    limit: int = 100,
    cursor: str = None,
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Search for notices in OCDS format.
    """
    url = f"{CONTRACTS_FINDER_BASE_URL}/Published/Notices/OCDS/Search"
    params = {"limit": min(limit, 100)}
    if published_from:
        params["publishedFrom"] = published_from
    if published_to:
        params["publishedTo"] = published_to
    if stages:
        params["stages"] = ",".join(stages)
    if cursor:
        params["cursor"] = cursor
    return _rate_limited_request("GET", url, params=params)


def get_awards_for_notice(notice_id: str) -> Tuple[Optional[List[Dict]], Optional[str]]:
    """
    Get award details for a specific notice.
    """
    notice, error = get_notice(notice_id)
    if error:
        return None, error
    if notice and "awards" in notice:
        return notice["awards"], None
    return [], None


def extract_supplier_info(notice: Dict) -> List[Dict]:
    """
    Extract supplier information from a notice.
    
    Args:
        notice: Full notice data dict
        
    Returns:
        List of supplier info dicts
    """
    suppliers = []
    awards = notice.get("awards", [])
    
    for award in awards:
        supplier_info = {
            "name": award.get("supplierName", ""),
            "company_number": None,
            "charity_number": None,
            "address": None,
            "awarded_value": award.get("value") or award.get("supplierAwardedValue"),
            "awarded_date": award.get("awardedDate"),
            "contract_start": award.get("startDate"),
            "contract_end": award.get("endDate"),
        }
        
        # Extract company/charity number from reference fields
        ref_type = award.get("referenceType", "")
        ref_value = award.get("reference", "")
        
        if ref_type == "COMPANIES_HOUSE" and ref_value:
            supplier_info["company_number"] = ref_value.strip().upper()
        elif ref_type == "CHARITY_COMMISSION" and ref_value:
            supplier_info["charity_number"] = ref_value.strip()
        
        # Extract address
        address_parts = []
        for field in ["supplierAddress1", "supplierAddress2", "supplierCity", 
                      "supplierPostcode", "supplierCountry"]:
            if award.get(field):
                address_parts.append(award[field])
        if address_parts:
            supplier_info["address"] = ", ".join(address_parts)
        
        suppliers.append(supplier_info)
    
    return suppliers


def extract_supplier_info_ocds(ocds_release: Dict) -> List[Dict]:
    """
    Extract supplier information from an OCDS release.
    """
    suppliers = []
    parties = ocds_release.get("parties", [])
    supplier_parties = {
        p["id"]: p for p in parties 
        if "supplier" in p.get("roles", [])
    }
    
    awards = ocds_release.get("awards", [])
    for award in awards:
        for supplier_ref in award.get("suppliers", []):
            supplier_id = supplier_ref.get("id")
            party = supplier_parties.get(supplier_id, {})
            
            supplier_info = {
                "name": supplier_ref.get("name") or party.get("name", ""),
                "company_number": None,
                "charity_number": None,
                "address": None,
                "awarded_value": award.get("value", {}).get("amount"),
                "awarded_date": award.get("date"),
            }
            
            identifier = party.get("identifier", {})
            scheme = identifier.get("scheme", "")
            id_value = identifier.get("id", "")
            
            if scheme == "GB-COH" and id_value:
                supplier_info["company_number"] = id_value.strip().upper()
            elif scheme == "GB-CHC" and id_value:
                supplier_info["charity_number"] = id_value.strip()
            
            for add_id in party.get("additionalIdentifiers", []):
                add_scheme = add_id.get("scheme", "")
                add_value = add_id.get("id", "")
                if add_scheme == "GB-COH" and add_value:
                    supplier_info["company_number"] = add_value.strip().upper()
                elif add_scheme == "GB-CHC" and add_value:
                    supplier_info["charity_number"] = add_value.strip()
            
            address = party.get("address", {})
            address_parts = []
            for field in ["streetAddress", "locality", "region", "postalCode", "countryName"]:
                if address.get(field):
                    address_parts.append(address[field])
            if address_parts:
                supplier_info["address"] = ", ".join(address_parts)
            
            suppliers.append(supplier_info)
    
    return suppliers


def search_awarded_by_buyer(
    buyer_name: str,
    from_date: str = None,
    to_date: str = None,
    max_results: int = 1000,
    progress_callback: Optional[Callable] = None # <--- Added Argument
) -> Tuple[List[Dict], Optional[str]]:
    """
    Search for all awarded contracts by a specific buyer organisation.
    
    Note: The API doesn't have a dedicated buyer filter, so we search by
    keyword (which includes organisation name) and filter results.
    
    Args:
        buyer_name: Name of the buying organisation
        from_date: Start date for search (ISO format)
        to_date: End date for search (ISO format)
        max_results: Maximum number of results to fetch
        progress_callback: Optional function to call with current results list
        
    Returns:
        Tuple of (list of contract dicts with supplier info, error message or None)
    """
    if not buyer_name or not buyer_name.strip():
        return [], "Buyer name cannot be empty"

    # Search using the buyer name as keyword
    results, error = search_notices(
        keyword=buyer_name,
        status="Awarded",
        awarded_from=from_date,
        awarded_to=to_date,
        size=min(max_results, 1000),
    )
    
    if error:
        return [], error
    
    if not results:
        return [], "No results returned"
    
    # Response structure: { "hitCount": N, "noticeList": [ { "score": X, "item": {...} }, ... ] }
    hit_count = results.get("hitCount", 0)
    notice_list = results.get("noticeList", [])
    
    log_message(f"Contracts Finder search returned {hit_count} hits, processing {len(notice_list)} notices")
    
    all_contracts = []
    
    for hit in notice_list:
        # Each hit has "score" and "item" keys
        notice_summary = hit.get("item", {})
        notice_id = notice_summary.get("id")
        
        if not notice_id:
            continue
        
        # Check if organisation name matches (case-insensitive partial match)
        org_name = notice_summary.get("organisationName", "")
        if buyer_name.lower() not in org_name.lower():
            # Skip notices from other organisations
            continue
        
        # Get full notice details for awards
        full_notice, err = get_notice(notice_id)
        if err or not full_notice:
            log_message(f"Failed to get notice {notice_id}: {err}")
            continue
        
        suppliers = extract_supplier_info(full_notice)
        
        contract_info = {
            "notice_id": notice_id,
            "title": notice_summary.get("title", ""),
            "description": notice_summary.get("description", ""),
            "buyer_name": org_name,
            "published_date": notice_summary.get("publishedDate"),
            "awarded_date": notice_summary.get("awardedDate"),
            "awarded_value": notice_summary.get("awardedValue"),
            "awarded_supplier": notice_summary.get("awardedSupplier"),
            "value_low": notice_summary.get("valueLow"),
            "value_high": notice_summary.get("valueHigh"),
            "cpv_codes": notice_summary.get("cpvCodes", "").split(",") if notice_summary.get("cpvCodes") else [],
            "region": notice_summary.get("region", ""),
            "suppliers": suppliers,
        }
        all_contracts.append(contract_info)

        # --- NEW: Trigger the callback if provided ---
        if progress_callback:
            progress_callback(all_contracts)
    
    log_message(f"Found {len(all_contracts)} contracts matching buyer '{buyer_name}'")
    
    return all_contracts, None


def check_api_status() -> bool:
    """
    Check if the Contracts Finder API is accessible.
    """
    results, error = search_notices(size=1)
    return error is None and results is not None
