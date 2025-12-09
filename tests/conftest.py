# multitool/tests/conftest.py
"""
Pytest configuration and shared fixtures.

This file is automatically loaded by pytest and provides
fixtures available to all test files.
"""

import pytest
import sys
import os

# Ensure the package root is in the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


@pytest.fixture
def sample_company_profile():
    """Sample company profile data for testing."""
    return {
        "company_number": "12345678",
        "company_name": "TEST COMPANY LTD",
        "company_status": "active",
        "type": "ltd",
        "date_of_creation": "2020-01-15",
        "registered_office_address": {
            "address_line_1": "123 Test Street",
            "address_line_2": "Test Building",
            "locality": "London",
            "postal_code": "SW1A 1AA",
            "country": "United Kingdom"
        },
        "sic_codes": ["62011", "62012"],
        "accounts": {
            "next_due": "2025-10-31",
            "last_accounts": {
                "made_up_to": "2024-01-31",
                "type": "micro-entity"
            }
        },
        "confirmation_statement": {
            "next_due": "2025-01-29",
            "last_made_up_to": "2024-01-15"
        }
    }


@pytest.fixture
def sample_officers_response():
    """Sample officers API response for testing."""
    return {
        "items": [
            {
                "name": "SMITH, John",
                "officer_role": "director",
                "date_of_birth": {"year": 1980, "month": 6},
                "appointed_on": "2020-01-15"
            },
            {
                "name": "DOE, Jane",
                "officer_role": "secretary",
                "appointed_on": "2020-01-15"
            }
        ],
        "total_results": 2
    }


@pytest.fixture
def sample_pscs_response():
    """Sample PSCs API response for testing."""
    return {
        "items": [
            {
                "name": "Mr John Smith",
                "date_of_birth": {"year": 1980, "month": 6},
                "natures_of_control": [
                    "ownership-of-shares-75-to-100-percent",
                    "voting-rights-75-to-100-percent"
                ],
                "notified_on": "2020-01-15"
            }
        ],
        "total_results": 1
    }


@pytest.fixture
def sample_charity_details():
    """Sample charity details for testing."""
    return {
        "charity_name": "Test Charity",
        "reg_status": "R",
        "date_of_registration": "2010-05-20",
        "address_line_1": "Charity House",
        "address_line_2": "10 Charity Road",
        "postcode": "EC1A 1BB",
        "phone": "020 1234 5678"
    }


@pytest.fixture
def sample_grant_data():
    """Sample grant data for testing."""
    return {
        "id": "360G-ExampleFunder-Grant001",
        "title": "Community Project Grant",
        "description": "Funding for local community initiatives",
        "amountAwarded": 50000,
        "currency": "GBP",
        "awardDate": "2024-03-15",
        "fundingOrganization": {
            "name": "Example Foundation",
            "id": "GB-CHC-123456"
        },
        "recipientOrganization": {
            "name": "Test Charity",
            "id": "GB-CHC-654321"
        }
    }


@pytest.fixture
def mock_token_bucket():
    """Mock token bucket that always allows requests."""
    from unittest.mock import Mock
    bucket = Mock()
    bucket.consume = Mock(return_value=True)
    bucket.try_consume = Mock(return_value=True)
    bucket.available_tokens = 50
    return bucket


@pytest.fixture(autouse=True)
def clear_api_caches():
    """Clear API caches before each test to ensure isolation."""
    try:
        from multitool.api.companies_house import ch_get_data
        ch_get_data.cache_clear()
    except (ImportError, AttributeError):
        pass
    
    try:
        from multitool.api.charity_commission import cc_get_data
        cc_get_data.cache_clear()
    except (ImportError, AttributeError):
        pass
    
    try:
        from multitool.api.grantnav import grantnav_get_data
        grantnav_get_data.cache_clear()
    except (ImportError, AttributeError):
        pass
    
    yield  # Run the test
    
    # Optionally clear after test too
