# multitool/tests/test_api.py
"""
Unit tests for API client functions.

These tests use mocking to avoid making real API calls.

Run with: python -m pytest multitool/tests/test_api.py -v
"""

import pytest
from unittest.mock import Mock, patch, MagicMock


class TestCompaniesHouseAPI:
    """Tests for the Companies House API client."""

    @patch('multitool.api.companies_house.requests.get')
    def test_ch_get_data_success(self, mock_get):
        """Successful API call should return data and no error."""
        # Clear the LRU cache before test
        from multitool.api.companies_house import ch_get_data
        ch_get_data.cache_clear()
        
        # Setup mock
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"company_name": "Test Ltd"}
        mock_get.return_value = mock_response
        
        # Create mock token bucket
        mock_bucket = Mock()
        mock_bucket.consume = Mock()
        
        # Call function
        data, error = ch_get_data("test_key", mock_bucket, "/company/12345678")
        
        # Assertions
        assert data == {"company_name": "Test Ltd"}
        assert error is None
        mock_bucket.consume.assert_called_once()

    @patch('multitool.api.companies_house.requests.get')
    def test_ch_get_data_404_returns_none(self, mock_get):
        """404 response should return None with error message."""
        from multitool.api.companies_house import ch_get_data
        ch_get_data.cache_clear()
        
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        mock_bucket = Mock()
        mock_bucket.consume = Mock()
        
        data, error = ch_get_data("test_key", mock_bucket, "/company/00000000")
        
        assert data is None
        assert "404" in error

    @patch('multitool.api.companies_house.requests.get')
    def test_ch_get_data_401_returns_none(self, mock_get):
        """401 unauthorized should return None with error."""
        from multitool.api.companies_house import ch_get_data
        ch_get_data.cache_clear()
        
        mock_response = Mock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response
        
        mock_bucket = Mock()
        mock_bucket.consume = Mock()
        
        data, error = ch_get_data("bad_key", mock_bucket, "/company/12345678")
        
        assert data is None
        assert "401" in error


class TestCharityCommissionAPI:
    """Tests for the Charity Commission API client."""

    @patch('multitool.api.charity_commission.requests.get')
    def test_cc_get_data_success(self, mock_get):
        """Successful API call should return data."""
        from multitool.api.charity_commission import cc_get_data
        cc_get_data.cache_clear()
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"charity_name": "Test Charity"}'
        mock_response.json.return_value = {"charity_name": "Test Charity"}
        mock_get.return_value = mock_response
        
        data, error = cc_get_data("test_key", "/charitydetails/123456/0")
        
        assert data == {"charity_name": "Test Charity"}
        assert error is None

    def test_cc_get_data_missing_key(self):
        """Missing API key should return error."""
        from multitool.api.charity_commission import cc_get_data
        cc_get_data.cache_clear()
        
        data, error = cc_get_data("", "/charitydetails/123456/0")
        
        assert data is None
        assert "missing" in error.lower()

    @patch('multitool.api.charity_commission.requests.get')
    def test_cc_get_data_empty_response(self, mock_get):
        """Empty response should return error."""
        from multitool.api.charity_commission import cc_get_data
        cc_get_data.cache_clear()
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = ""
        mock_get.return_value = mock_response
        
        data, error = cc_get_data("test_key", "/charitydetails/999999/0")
        
        assert data is None
        assert error is not None


class TestGrantNavAPI:
    """Tests for the GrantNav API client."""

    @patch('multitool.api.grantnav.requests.get')
    def test_grantnav_get_data_success(self, mock_get):
        """Successful API call should return data."""
        from multitool.api.grantnav import grantnav_get_data
        grantnav_get_data.cache_clear()
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [{"id": "grant1"}]}
        mock_get.return_value = mock_response
        
        data, error = grantnav_get_data("https://api.threesixtygiving.org/api/v1/grants")
        
        assert data == {"results": [{"id": "grant1"}]}
        assert error is None

    @patch('multitool.api.grantnav.requests.get')
    def test_grantnav_get_data_404(self, mock_get):
        """404 should return not_found error."""
        from multitool.api.grantnav import grantnav_get_data
        grantnav_get_data.cache_clear()
        
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        data, error = grantnav_get_data("https://api.threesixtygiving.org/api/v1/org/invalid")
        
        assert data is None
        assert error == "not_found"


class TestAPIHelperFunctions:
    """Tests for API helper/convenience functions."""

    @patch('multitool.api.companies_house.ch_get_data')
    def test_ch_get_company(self, mock_ch_get_data):
        """ch_get_company should call ch_get_data with correct path."""
        from multitool.api.companies_house import ch_get_company
        
        mock_bucket = Mock()
        ch_get_company("test_key", mock_bucket, "12345678")
        
        mock_ch_get_data.assert_called_once()
        call_args = mock_ch_get_data.call_args
        assert "/company/12345678" in call_args[0]

    @patch('multitool.api.companies_house.ch_get_data')
    def test_ch_get_officers(self, mock_ch_get_data):
        """ch_get_officers should call ch_get_data with officers path."""
        from multitool.api.companies_house import ch_get_officers
        
        mock_bucket = Mock()
        ch_get_officers("test_key", mock_bucket, "12345678")
        
        mock_ch_get_data.assert_called_once()
        call_args = mock_ch_get_data.call_args
        assert "/officers" in call_args[0][2]

    @patch('multitool.api.companies_house.ch_get_data')
    def test_ch_get_pscs(self, mock_ch_get_data):
        """ch_get_pscs should call ch_get_data with PSC path."""
        from multitool.api.companies_house import ch_get_pscs
        
        mock_bucket = Mock()
        ch_get_pscs("test_key", mock_bucket, "12345678")
        
        mock_ch_get_data.assert_called_once()
        call_args = mock_ch_get_data.call_args
        assert "persons-with-significant-control" in call_args[0][2]


class TestCaching:
    """Tests to verify API response caching works."""

    @patch('multitool.api.companies_house.requests.get')
    def test_ch_get_data_caches_results(self, mock_get):
        """Repeated calls should use cache, not make new requests."""
        from multitool.api.companies_house import ch_get_data
        ch_get_data.cache_clear()
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"company_name": "Cached Ltd"}
        mock_get.return_value = mock_response
        
        mock_bucket = Mock()
        mock_bucket.consume = Mock()
        
        # First call
        data1, _ = ch_get_data("test_key", mock_bucket, "/company/11111111")
        # Second call (should be cached)
        data2, _ = ch_get_data("test_key", mock_bucket, "/company/11111111")
        
        # Should only have made one actual HTTP request
        assert mock_get.call_count == 1
        assert data1 == data2


# Run tests if executed directly
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
