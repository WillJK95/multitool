# multitool/utils/charity_financial_data.py
"""Wraps Charity Commission API financial data into the same interface
used by the cross-analysis rules engine (UnifiedFinancialData).

The CC API provides structured financial data directly — no iXBRL parsing
needed.  This class maps CC API fields to the metric names expected by
edd_cross_analysis.py so the existing rules (G1, G2/G3, F1, F3, F4) work
unchanged.
"""

from typing import Dict, List, Optional


class CharityFinancialData:
    """Adapts CC API financial responses for the cross-analysis engine.

    Implements the same interface as UnifiedFinancialData:
        get_years(), get_metric(name, year), get_metric_series(name),
        has_auto(name), has_manual(name), provenance.

    Parameters
    ----------
    financial_history : list[dict] or None
        Response from GetCharityFinancialHistory (5-year income/expenditure).
    assets_liabilities : list[dict] or None
        Response from GetCharityAssetsLiabilities (balance sheet).
    overview : dict or None
        Response from GetCharityOverview (annual return data).
    """

    # Map cross-analysis metric names to CC API field names.
    # The rules engine asks for 'Revenue', 'NetAssets', etc.
    _INCOME_FIELD = 'inc_total'
    _EXPENDITURE_FIELD = 'exp_total'

    def __init__(
        self,
        financial_history: Optional[List[dict]] = None,
        assets_liabilities: Optional[List[dict]] = None,
        overview: Optional[dict] = None,
    ):
        self._provenance: Dict[str, str] = {}

        # Index financial history by fiscal year end
        self._fin_by_year: Dict[int, dict] = {}
        if financial_history:
            for entry in financial_history:
                yr = self._extract_year(entry)
                if yr is not None:
                    self._fin_by_year[yr] = entry

        # Index assets/liabilities by fiscal year end
        self._bal_by_year: Dict[int, dict] = {}
        if assets_liabilities:
            for entry in (assets_liabilities if isinstance(assets_liabilities, list) else [assets_liabilities]):
                yr = self._extract_year(entry)
                if yr is not None:
                    self._bal_by_year[yr] = entry

        self._overview = overview or {}

    @staticmethod
    def _extract_year(entry: dict) -> Optional[int]:
        """Extract fiscal year from a CC API response entry."""
        # Try fin_period_end_date first (financial history), then date field
        for key in ('fin_period_end_date', 'ar_cycle_reference', 'fin_period_end'):
            val = entry.get(key)
            if val:
                try:
                    # Dates come as "YYYY-MM-DDT00:00:00" or just year int
                    if isinstance(val, str) and len(val) >= 4:
                        return int(val[:4])
                    elif isinstance(val, (int, float)):
                        return int(val)
                except (ValueError, TypeError):
                    continue
        return None

    def get_years(self) -> List[int]:
        """Return sorted list of all years with financial data."""
        years = set(self._fin_by_year.keys())
        years.update(self._bal_by_year.keys())
        return sorted(years)

    def get_metric(self, name: str, year: Optional[int] = None) -> Optional[float]:
        """Return a metric value for a given year (or most recent if None)."""
        all_years = self.get_years()
        target_year = year if year is not None else (all_years[-1] if all_years else None)
        if target_year is None:
            return None

        val = self._resolve_metric(name, target_year)
        if val is not None:
            self._provenance.setdefault(name, 'auto')
        return val

    def get_metric_series(self, name: str) -> Dict[int, float]:
        """Return {year: value} for all available years of a metric."""
        result: Dict[int, float] = {}
        for yr in self.get_years():
            val = self._resolve_metric(name, yr)
            if val is not None:
                result[yr] = val
        return result

    def has_auto(self, name: str) -> bool:
        """All charity data is 'auto' (from API)."""
        return self.get_metric(name) is not None

    def has_manual(self, name: str) -> bool:
        """Charity mode has no manual input."""
        return False

    @property
    def provenance(self) -> Dict[str, str]:
        return dict(self._provenance)

    def _resolve_metric(self, name: str, year: int) -> Optional[float]:
        """Resolve a metric name to a CC API value for a specific year."""
        fin = self._fin_by_year.get(year, {})
        bal = self._bal_by_year.get(year, {})

        # Map standard metric names to CC API fields
        if name in ('Revenue', 'Turnover'):
            return self._safe_float(fin.get('inc_total'))

        if name == 'ProfitLoss':
            # Charity equivalent: surplus = income - expenditure
            inc = self._safe_float(fin.get('inc_total'))
            exp = self._safe_float(fin.get('exp_total'))
            if inc is not None and exp is not None:
                return inc - exp
            return None

        if name == 'NetAssets':
            # Derive from balance sheet: own_use + investments + pension + other - liabilities
            own = self._safe_float(bal.get('assets_own_use')) or 0
            invest = self._safe_float(bal.get('assets_long_term_investment')) or 0
            pension = self._safe_float(bal.get('defined_net_assets_pension')) or 0
            other = self._safe_float(bal.get('assets_other_assets')) or 0
            liab = self._safe_float(bal.get('assets_total_liabilities')) or 0
            # Only compute if we have at least some data
            if any(bal.get(k) is not None for k in
                   ('assets_own_use', 'assets_long_term_investment',
                    'assets_other_assets', 'assets_total_liabilities')):
                return own + invest + pension + other - liab
            return None

        if name in ('CurrentAssets', 'NetCurrentAssets'):
            # Proxy: assets_other_assets is the closest to current assets
            return self._safe_float(bal.get('assets_other_assets'))

        if name in ('CurrentLiabilities', 'TotalLiabilities'):
            return self._safe_float(bal.get('assets_total_liabilities'))

        if name == 'TotalAssets':
            own = self._safe_float(bal.get('assets_own_use')) or 0
            invest = self._safe_float(bal.get('assets_long_term_investment')) or 0
            pension = self._safe_float(bal.get('defined_net_assets_pension')) or 0
            other = self._safe_float(bal.get('assets_other_assets')) or 0
            if any(bal.get(k) is not None for k in
                   ('assets_own_use', 'assets_long_term_investment',
                    'assets_other_assets')):
                return own + invest + pension + other
            return None

        if name == 'TangibleAssets':
            return self._safe_float(bal.get('assets_own_use'))

        if name == 'Investments':
            return self._safe_float(bal.get('assets_long_term_investment'))

        if name == 'CreditorsAfterOneYear':
            # CC doesn't split current/non-current liabilities
            return None

        if name == 'CashBankInHand':
            # Not available from CC balance sheet
            return None

        if name == 'IntangibleAssets':
            # Not available from CC
            return None

        # Income breakdown fields
        income_fields = {
            'inc_donations_and_legacies', 'inc_charitable_activities',
            'inc_investment', 'inc_other_trading_activities',
            'inc_endowments', 'inc_other',
        }
        if name in income_fields:
            return self._safe_float(fin.get(name))

        # Expenditure breakdown
        exp_fields = {
            'exp_charitable_activities', 'exp_raising_funds',
            'exp_governance', 'exp_other',
        }
        if name in exp_fields:
            return self._safe_float(fin.get(name))

        # Government income fields
        if name == 'income_from_govt_contracts':
            return self._safe_float(fin.get('income_from_govt_contracts'))
        if name == 'income_from_govt_grants':
            return self._safe_float(fin.get('income_from_govt_grants'))

        # Expenditure total
        if name in ('TotalExpenses', 'Expenditure'):
            return self._safe_float(fin.get('exp_total'))

        # Income total
        if name == 'Income':
            return self._safe_float(fin.get('inc_total'))

        return None

    @staticmethod
    def _safe_float(val) -> Optional[float]:
        """Convert a value to float, returning None on failure."""
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
