# multitool/tests/test_utils.py
"""
Unit tests for utility functions.

Run with: python -m pytest multitool/tests/test_utils.py -v
Or run all tests: python -m pytest multitool/tests/ -v
"""

import pytest
from multitool.utils.helpers import (
    clean_company_number,
    clean_address_string,
    get_canonical_name_key,
    get_nested_value,
)
from multitool.utils.token_bucket import TokenBucket


class TestCleanCompanyNumber:
    """Tests for the clean_company_number function."""

    def test_pads_numeric_to_8_digits(self):
        """Numeric company numbers should be zero-padded to 8 digits."""
        assert clean_company_number("12345") == "00012345"
        assert clean_company_number("1") == "00000001"
        assert clean_company_number("12345678") == "12345678"

    def test_preserves_scottish_prefix(self):
        """Scottish company numbers (SC prefix) should be preserved."""
        assert clean_company_number("SC123456") == "SC123456"
        assert clean_company_number("sc123456") == "SC123456"

    def test_preserves_ni_prefix(self):
        """Northern Ireland company numbers (NI prefix) should be preserved."""
        assert clean_company_number("NI012345") == "NI012345"

    def test_preserves_other_prefixes(self):
        """Other valid prefixes should be preserved."""
        assert clean_company_number("OC123456") == "OC123456"  # LLP
        assert clean_company_number("LP123456") == "LP123456"  # Limited Partnership
        assert clean_company_number("SL123456") == "SL123456"  # Scottish LP

    def test_strips_whitespace(self):
        """Whitespace should be stripped."""
        assert clean_company_number("  12345678  ") == "12345678"
        assert clean_company_number(" SC123456 ") == "SC123456"

    def test_uppercase_conversion(self):
        """Letters should be converted to uppercase."""
        assert clean_company_number("sc123456") == "SC123456"
        assert clean_company_number("ni012345") == "NI012345"

    def test_none_input(self):
        """None input should return None."""
        assert clean_company_number(None) is None

    def test_empty_string(self):
        """Empty string should return None."""
        assert clean_company_number("") is None

    def test_non_string_input(self):
        """Non-string input should return None."""
        assert clean_company_number(12345) is None


class TestCleanAddressString:
    """Tests for the clean_address_string function."""

    def test_lowercase_conversion(self):
        """Address should be converted to lowercase."""
        result = clean_address_string("123 HIGH STREET")
        assert result == "123 high street"

    def test_removes_periods(self):
        """Periods should be removed."""
        result = clean_address_string("123 High St.")
        assert "." not in result

    def test_normalises_whitespace(self):
        """Multiple spaces should be collapsed to single space."""
        result = clean_address_string("123   High    Street")
        assert "  " not in result

    def test_strips_whitespace(self):
        """Leading/trailing whitespace should be stripped."""
        result = clean_address_string("  123 High Street  ")
        assert result == "123 high street"

    def test_none_input(self):
        """None input should return None."""
        assert clean_address_string(None) is None

    def test_empty_string(self):
        """Empty string should return None."""
        assert clean_address_string("") is None


class TestGetCanonicalNameKey:
    """Tests for the get_canonical_name_key function."""

    def test_basic_name(self):
        """Basic name should return first+last tokens."""
        result = get_canonical_name_key("John Smith", None)
        assert result == "johnsmith"

    def test_removes_titles(self):
        """Common titles should be removed."""
        assert get_canonical_name_key("Mr John Smith", None) == "johnsmith"
        assert get_canonical_name_key("Dr. Jane Doe", None) == "janedoe"
        assert get_canonical_name_key("Prof Sarah Jones", None) == "sarahjones"

    def test_handles_surname_comma_format(self):
        """SURNAME, Forename format should be handled."""
        result = get_canonical_name_key("SMITH, John", None)
        assert result == "johnsmith"

    def test_includes_dob_when_provided(self):
        """DOB should be appended to key when provided."""
        dob = {"year": 1980, "month": 6}
        result = get_canonical_name_key("John Smith", dob)
        assert result == "johnsmith-1980-6"

    def test_single_name(self):
        """Single name should work."""
        result = get_canonical_name_key("Madonna", None)
        assert result == "madonna"

    def test_empty_name(self):
        """Empty name should return empty string."""
        assert get_canonical_name_key("", None) == ""

    def test_removes_special_characters(self):
        """Special characters should be removed."""
        result = get_canonical_name_key("John O'Brien-Smith", None)
        assert result == "johnobriensmith"


class TestGetNestedValue:
    """Tests for the get_nested_value function."""

    def test_simple_key(self):
        """Simple key should return direct value."""
        data = {"name": "Test Company"}
        assert get_nested_value(data, "name") == "Test Company"

    def test_nested_key(self):
        """Dot-separated key should traverse nested dicts."""
        data = {"address": {"postal_code": "SW1A 1AA"}}
        assert get_nested_value(data, "address.postal_code") == "SW1A 1AA"

    def test_missing_key_returns_default(self):
        """Missing key should return default value."""
        data = {"name": "Test"}
        assert get_nested_value(data, "missing", "N/A") == "N/A"

    def test_none_value_returns_default(self):
        """None value should return default."""
        data = {"name": None}
        assert get_nested_value(data, "name", "Unknown") == "Unknown"

    def test_deeply_nested(self):
        """Deeply nested values should be accessible."""
        data = {"level1": {"level2": {"level3": "value"}}}
        assert get_nested_value(data, "level1.level2.level3") == "value"


class TestTokenBucket:
    """Tests for the TokenBucket rate limiter."""

    def test_initial_capacity(self):
        """Bucket should start at full capacity."""
        bucket = TokenBucket(capacity=10, refill_rate=1.0)
        assert bucket.available_tokens == 10

    def test_consume_reduces_tokens(self):
        """Consuming should reduce available tokens."""
        bucket = TokenBucket(capacity=10, refill_rate=1.0)
        bucket.consume(1)
        # Use approximate comparison due to refill timing
        assert 8.9 < bucket.available_tokens < 9.1

    def test_consume_multiple(self):
        """Should be able to consume multiple tokens."""
        bucket = TokenBucket(capacity=10, refill_rate=1.0)
        bucket.consume(5)
        # Use approximate comparison due to refill timing
        assert 4.9 < bucket.available_tokens < 5.1

    def test_try_consume_success(self):
        """try_consume should return True when tokens available."""
        bucket = TokenBucket(capacity=10, refill_rate=1.0)
        assert bucket.try_consume(5) is True
        # Use approximate comparison due to refill timing
        assert 4.9 < bucket.available_tokens < 5.1

    def test_try_consume_failure(self):
        """try_consume should return False when insufficient tokens."""
        bucket = TokenBucket(capacity=5, refill_rate=1.0)
        assert bucket.try_consume(10) is False
        assert bucket.available_tokens == 5  # Unchanged

    def test_capacity_limit(self):
        """Tokens should not exceed capacity after refill."""
        bucket = TokenBucket(capacity=10, refill_rate=100.0)  # Fast refill
        bucket.consume(5)
        import time
        time.sleep(0.1)  # Allow refill
        assert bucket.available_tokens <= 10


class TestConstantsExist:
    """Tests to verify constants are properly defined."""

    def test_api_urls_defined(self):
        """API URLs should be defined."""
        from multitool.constants import (
            API_BASE_URL,
            CHARITY_API_BASE_URL,
            GRANTNAV_API_BASE_URL,
        )
        assert API_BASE_URL.startswith("https://")
        assert CHARITY_API_BASE_URL.startswith("https://")
        assert GRANTNAV_API_BASE_URL.startswith("https://")

    def test_field_definitions_not_empty(self):
        """Field definition dicts should not be empty."""
        from multitool.constants import (
            COMPANY_DATA_FIELDS,
            GRANT_DATA_FIELDS,
            CHARITY_DATA_FIELDS,
        )
        assert len(COMPANY_DATA_FIELDS) > 0
        assert len(GRANT_DATA_FIELDS) > 0
        assert len(CHARITY_DATA_FIELDS) > 0

    def test_config_paths_defined(self):
        """Config paths should be defined."""
        from multitool.constants import CONFIG_DIR, CONFIG_FILE
        assert CONFIG_DIR is not None
        assert CONFIG_FILE is not None


class TestHelpContentExists:
    """Tests to verify help content is defined."""

    def test_help_content_keys_exist(self):
        """Expected help content keys should exist."""
        from multitool.help_content import HELP_CONTENT
        
        expected_keys = ["main", "api_keys", "director", "ubo", "network_creator"]
        for key in expected_keys:
            assert key in HELP_CONTENT, f"Missing help content for '{key}'"

    def test_help_content_not_empty(self):
        """Help content should not be empty strings."""
        from multitool.help_content import HELP_CONTENT
        
        for key, content in HELP_CONTENT.items():
            assert len(content) > 0, f"Help content for '{key}' is empty"


# Run tests if executed directly
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
