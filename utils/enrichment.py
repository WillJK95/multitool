# multitool/utils/enrichment.py
"""Functions for enriching data rows with company and charity information."""

from typing import Dict, Any

# Import will be done at runtime to avoid circular imports
# from ..api.companies_house import ch_get_data
# from ..api.charity_commission import cc_get_data


def enrich_with_company_data(
    row: Dict[str, Any],
    api_key: str,
    ch_token_bucket,
    profile: Dict[str, Any],
    fields_to_fetch: Dict[str, Any],
    ch_get_data_func=None
) -> None:
    """
    Enrich a data row with company information based on selected fields.
    
    Args:
        row: Dictionary to enrich with company data
        api_key: Companies House API key
        ch_token_bucket: Token bucket for rate limiting
        profile: Company profile data from API
        fields_to_fetch: Dict of field names to BooleanVar indicating which to fetch
        ch_get_data_func: Function to call for additional API requests
    """
    cid = profile.get("company_number")
    if not cid:
        return
    
    # Basic profile fields
    if fields_to_fetch.get("company_number") and fields_to_fetch["company_number"].get():
        row["company_number"] = cid
    
    if fields_to_fetch.get("incorporation_date") and fields_to_fetch["incorporation_date"].get():
        row["incorporation_date"] = profile.get("date_of_creation")
    
    if fields_to_fetch.get("company_status") and fields_to_fetch["company_status"].get():
        row["company_status"] = profile.get("company_status")
    
    if fields_to_fetch.get("company_type") and fields_to_fetch["company_type"].get():
        row["company_type"] = profile.get("type")
    
    if fields_to_fetch.get("jurisdiction") and fields_to_fetch["jurisdiction"].get():
        row["jurisdiction"] = profile.get("jurisdiction")
    
    if fields_to_fetch.get("date_of_cessation") and fields_to_fetch["date_of_cessation"].get():
        row["date_of_cessation"] = profile.get("date_of_cessation")
    
    if fields_to_fetch.get("registered_address") and fields_to_fetch["registered_address"].get():
        addr = profile.get("registered_office_address", {})
        row["registered_address"] = ", ".join(
            filter(
                None,
                [
                    addr.get("address_line_1"),
                    addr.get("address_line_2"),
                    addr.get("locality"),
                    addr.get("postal_code"),
                    addr.get("country"),
                ],
            )
        )
    
    if fields_to_fetch.get("previous_company_names") and fields_to_fetch["previous_company_names"].get():
        previous_names = profile.get("previous_company_names", [])
        row["previous_company_names"] = "; ".join(
            [f"{p.get('name')} (until {p.get('ceased_on')})" for p in previous_names]
        )
    
    # Accounts information
    accounts = profile.get("accounts", {})
    
    if fields_to_fetch.get("accounts_next_due") and fields_to_fetch["accounts_next_due"].get():
        row["accounts_next_due"] = accounts.get("next_due")
    
    if fields_to_fetch.get("accounts_last_made_up_to") and fields_to_fetch["accounts_last_made_up_to"].get():
        row["accounts_last_made_up_to"] = accounts.get("last_accounts", {}).get("made_up_to")
    
    if fields_to_fetch.get("accounts_type") and fields_to_fetch["accounts_type"].get():
        row["accounts_type"] = accounts.get("last_accounts", {}).get("type")
    
    # Confirmation statement
    cs = profile.get("confirmation_statement", {})
    
    if fields_to_fetch.get("confirmation_statement_next_due") and fields_to_fetch["confirmation_statement_next_due"].get():
        row["confirmation_statement_next_due"] = cs.get("next_due")
    
    if fields_to_fetch.get("confirmation_statement_last_made_up_to") and fields_to_fetch["confirmation_statement_last_made_up_to"].get():
        row["confirmation_statement_last_made_up_to"] = cs.get("last_made_up_to")
    
    # SIC codes
    if fields_to_fetch.get("sic_codes") and fields_to_fetch["sic_codes"].get():
        sic_data = profile.get("sic_codes", [])
        row["sic_codes"] = "; ".join(sic_data) if sic_data else ""
    
    # Officers (requires additional API call)
    if fields_to_fetch.get("officers") and fields_to_fetch["officers"].get() and ch_get_data_func:
        officers, _ = ch_get_data_func(api_key, ch_token_bucket, f"/company/{cid}/officers")
        if officers:
            row["officers"] = "; ".join(
                [o.get("name", "") for o in officers.get("items", [])]
            )
    
    # PSCs (requires additional API call)
    if fields_to_fetch.get("persons_with_significant_control") and fields_to_fetch["persons_with_significant_control"].get() and ch_get_data_func:
        pscs, _ = ch_get_data_func(
            api_key, ch_token_bucket, f"/company/{cid}/persons-with-significant-control"
        )
        if pscs:
            row["persons_with_significant_control"] = "; ".join(
                [p.get("name", "") for p in pscs.get("items", [])]
            )


def enrich_with_charity_data(
    row: Dict[str, Any],
    charity_api_key: str,
    reg_num: str,
    fields_to_fetch: Dict[str, Any],
    cc_get_data_func=None
) -> None:
    """
    Enrich a data row with charity information based on selected fields.
    
    Args:
        row: Dictionary to enrich with charity data
        charity_api_key: Charity Commission API key
        reg_num: Charity registration number
        fields_to_fetch: Dict of field names to BooleanVar indicating which to fetch
        cc_get_data_func: Function to call for API requests
    """
    if not cc_get_data_func:
        return
    
    suffix = "0"  # Standard suffix for main charity
    
    if fields_to_fetch.get("reg_charity_number") and fields_to_fetch["reg_charity_number"].get():
        row["charity_number"] = reg_num
    
    if fields_to_fetch.get("main_details") and fields_to_fetch["main_details"].get():
        row["charity_name"], row["address"], row["phone"] = "", "", ""
        data, _ = cc_get_data_func(charity_api_key, f"/charitydetails/{reg_num}/{suffix}")
        if data:
            details_obj = data[0] if isinstance(data, list) and data else data
            if isinstance(details_obj, dict):
                row["charity_name"] = details_obj.get("charity_name")
                row["address"] = ", ".join(
                    filter(
                        None,
                        [details_obj.get(f"address_line_{i}") for i in range(1, 5)]
                        + [details_obj.get("postcode")],
                    )
                )
                row["phone"] = details_obj.get("phone")
    
    if fields_to_fetch.get("date_of_registration") and fields_to_fetch["date_of_registration"].get():
        data, _ = cc_get_data_func(charity_api_key, f"/charitydetails/{reg_num}/{suffix}")
        if data:
            details_obj = data[0] if isinstance(data, list) and data else data
            if isinstance(details_obj, dict):
                row["date_of_registration"] = details_obj.get("date_of_registration")
    
    if fields_to_fetch.get("other_names") and fields_to_fetch["other_names"].get():
        row["other_names"] = ""
        data, _ = cc_get_data_func(charity_api_key, f"/charityothernames/{reg_num}/{suffix}")
        if data and isinstance(data, list):
            row["other_names"] = "; ".join(
                [item.get("charity_name", "") for item in data]
            )
    
    if fields_to_fetch.get("trustee_names") and fields_to_fetch["trustee_names"].get():
        row["trustees"] = ""
        data, _ = cc_get_data_func(
            charity_api_key, f"/charitytrusteenamesV2/{reg_num}/{suffix}"
        )
        if data and isinstance(data, list):
            row["trustees"] = "; ".join([item.get("trustee_name", "") for item in data])
    
    if fields_to_fetch.get("financial_history") and fields_to_fetch["financial_history"].get():
        data, _ = cc_get_data_func(
            charity_api_key, f"/charityfinancialhistory/{reg_num}/{suffix}"
        )
        if data and isinstance(data, list):
            for fin_year in data:
                year_end = fin_year.get("financial_year_end_date", "YYYY-MM-DD").split("-")[0]
                row[f"income_{year_end}"] = fin_year.get("income")
                row[f"spending_{year_end}"] = fin_year.get("spending")
    
    if fields_to_fetch.get("assets_liabilities") and fields_to_fetch["assets_liabilities"].get():
        row["total_assets"], row["total_liabilities"] = "", ""
        data, _ = cc_get_data_func(
            charity_api_key, f"/charityassetsliabilities/{reg_num}/{suffix}"
        )
        if data:
            assets_obj = data[0] if isinstance(data, list) and data else data
            if isinstance(assets_obj, dict):
                row["total_assets"] = assets_obj.get("total_assets")
                row["total_liabilities"] = assets_obj.get("total_liabilities")
    
    if fields_to_fetch.get("annual_return_overview") and fields_to_fetch["annual_return_overview"].get():
        row["ar_employees"], row["ar_volunteers"] = "", ""
        data, _ = cc_get_data_func(charity_api_key, f"/charityoverview/{reg_num}/{suffix}")
        if data:
            overview_obj = data[0] if isinstance(data, list) and data else data
            if isinstance(overview_obj, dict):
                row["ar_employees"] = overview_obj.get("employees")
                row["ar_volunteers"] = overview_obj.get("volunteers")
    
    if fields_to_fetch.get("other_regulators") and fields_to_fetch["other_regulators"].get():
        row["other_regulators"] = ""
        data, _ = cc_get_data_func(
            charity_api_key, f"/Charity%20Other%20Regulators/{reg_num}/{suffix}"
        )
        if data and isinstance(data, list):
            row["other_regulators"] = "; ".join(
                [item.get("regulator_name", "") for item in data]
            )
    
    if fields_to_fetch.get("regulatory_reports") and fields_to_fetch["regulatory_reports"].get():
        row["regulatory_reports"] = ""
        data, _ = cc_get_data_func(
            charity_api_key, f"/charityregulatoryreport/{reg_num}/{suffix}"
        )
        if data and isinstance(data, list):
            reports = [
                f"{item.get('report_name', 'N/A')} ({item.get('date_published', 'N/A')}) - URL: {item.get('report_location', 'N/A')}"
                for item in data
            ]
            row["regulatory_reports"] = " | ".join(reports)
    
    if fields_to_fetch.get("area_of_operation") and fields_to_fetch["area_of_operation"].get():
        row["area_of_operation"] = ""
        data, _ = cc_get_data_func(
            charity_api_key, f"/charityareaofoperation/{reg_num}/{suffix}"
        )
        if data and isinstance(data, list):
            areas = [item.get("area_of_operation", "") for item in data]
            row["area_of_operation"] = "; ".join(filter(None, areas))
    
    if fields_to_fetch.get("filing_information") and fields_to_fetch["filing_information"].get():
        row["qualified_accounts_years"], row["late_submission_years"] = "", ""
        data, _ = cc_get_data_func(
            charity_api_key, f"/charityaccountarinformation/{reg_num}/{suffix}"
        )
        if data and isinstance(data, list):
            qualified_accounts = [
                item["reporting_period_year_end"]
                for item in data
                if item.get("accounts_qualified")
            ]
            late_submissions = [
                item["reporting_period_year_end"]
                for item in data
                if item.get("date_received")
                and item.get("date_due")
                and item["date_received"] > item["date_due"]
            ]
            if qualified_accounts:
                row["qualified_accounts_years"] = "; ".join(qualified_accounts)
            if late_submissions:
                row["late_submission_years"] = "; ".join(late_submissions)
    
    if fields_to_fetch.get("removal_info") and fields_to_fetch["removal_info"].get():
        row.update({
            "registration_status": "",
            "date_of_removal": "",
            "removal_reason": "",
            "registration_event_history": "",
            "parsed_removal_date": "",
        })
        
        details_data, _ = cc_get_data_func(
            charity_api_key, f"/charitydetails/{reg_num}/{suffix}"
        )
        if details_data and isinstance(details_data, list):
            details = details_data[0]
            row["registration_status"] = details.get("reg_status")
            row["date_of_removal"] = details.get("date_of_removal")
            row["removal_reason"] = details.get("removal_reason")
        
        history_data, _ = cc_get_data_func(
            charity_api_key, f"/charityregistrationhistory/{reg_num}/{suffix}"
        )
        if history_data and isinstance(history_data, list):
            history_events = [
                f"{item.get('reg_desc', 'N/A')} on {item.get('reg_date', 'N/A')}"
                for item in history_data
            ]
            row["registration_event_history"] = " | ".join(history_events)
            removal_dates = []
            for event in history_events:
                if "Removed on" in event:
                    parts = event.split(" on ")
                    if len(parts) > 1:
                        removal_dates.append(parts[1])
            if removal_dates:
                row["parsed_removal_date"] = "; ".join(removal_dates)
    
    if fields_to_fetch.get("governance_status") and fields_to_fetch["governance_status"].get():
        row.update({
            "organisation_number": "",
            "insolvent": "",
            "in_administration": "",
            "charity_companies_house_number": "",
        })
        
        details_data, _ = cc_get_data_func(
            charity_api_key, f"/charitydetails/{reg_num}/{suffix}"
        )
        if details_data and isinstance(details_data, list):
            details = details_data[0]
            row["organisation_number"] = details.get("organisation_number")
            row["insolvent"] = details.get("insolvent")
            row["in_administration"] = details.get("in_administration")
            row["charity_companies_house_number"] = details.get("charity_co_reg_number")
