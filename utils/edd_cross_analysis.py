# utils/edd_cross_analysis.py
"""Cross-analysis rules engine for EDD: grant-specific and financial health rules."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from .helpers import log_message


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class CrossAnalysisResult:
    """Result of a single cross-analysis rule."""
    rule_id: str              # e.g. "G1", "F2"
    title: str
    risk_flag: str            # HIGH, MEDIUM, LOW, NOT_ASSESSED
    confidence: str           # AUTO, ENRICHED, LIMITED, SKIPPED
    narrative: str
    recommendation: str
    trend_data: List[Dict] = field(default_factory=list)


@dataclass
class CrossAnalysisReport:
    """Aggregated output from all cross-analysis rules."""
    results: List[CrossAnalysisResult]
    composite_warning: Optional[str] = None
    pattern_warnings: List[str] = field(default_factory=list)
    filing_quality_caveat: Optional[str] = None
    company_age_note: Optional[str] = None
    accounts_type: Optional[str] = None


# ---------------------------------------------------------------------------
# Unified data layer
# ---------------------------------------------------------------------------

class UnifiedFinancialData:
    """Merges auto-parsed iXBRL data with manual user input.

    Parameters
    ----------
    auto_analyzer : FinancialAnalyzer or None
        The existing FinancialAnalyzer populated from iXBRL files.
    manual_data : dict
        Keys are field names (matching MANUAL_INPUT_FIELDS keys or auto
        column names), values are floats.  May also contain
        ``'period_end'`` (str, YYYY-MM-DD).
    """

    def __init__(self, auto_analyzer=None, manual_data: Optional[Dict] = None):
        self.auto_analyzer = auto_analyzer
        self.manual_data = manual_data or {}
        self._provenance: Dict[str, str] = {}  # metric -> "auto" | "manual"

        # Build auto DataFrame reference
        if auto_analyzer and not auto_analyzer.data.empty:
            self._auto_df = auto_analyzer.data.sort_values('Year').copy()
        else:
            self._auto_df = pd.DataFrame()

        # Compute derived fields in auto data if not already present
        self._compute_derived_fields()

    # -- derived fields -----------------------------------------------------

    def _compute_derived_fields(self):
        """Compute NetCurrentAssets from components if not directly available."""
        if self._auto_df.empty:
            return
        if 'NetCurrentAssets' not in self._auto_df.columns:
            if 'CurrentAssets' in self._auto_df.columns and 'CurrentLiabilities' in self._auto_df.columns:
                self._auto_df['NetCurrentAssets'] = self._auto_df.apply(
                    lambda r: r['CurrentAssets'] - r['CurrentLiabilities']
                    if pd.notna(r.get('CurrentAssets')) and pd.notna(r.get('CurrentLiabilities'))
                    else None,
                    axis=1,
                )

    # -- manual → auto field mapping ----------------------------------------

    _MANUAL_TO_AUTO = {
        'Turnover': 'Revenue',
        'PreTaxProfitLoss': 'ProfitLoss',
        'CashAtBank': 'CashBankInHand',
        'ManualDebtors': 'Debtors',
    }

    # -- accessors ----------------------------------------------------------

    def get_years(self) -> List[int]:
        """Return sorted list of years available in auto data."""
        if self._auto_df.empty:
            return []
        return sorted(self._auto_df['Year'].dropna().astype(int).tolist())

    def get_metric(self, name: str, year: Optional[int] = None) -> Optional[float]:
        """Return a metric value, preferring manual override for the latest year.

        If *year* is None, return the value for the most recent year.
        Manual data is only applied to the latest year (or the year matching
        the user-specified accounting period).
        """
        # Determine the auto column to query
        auto_col = self._MANUAL_TO_AUTO.get(name, name)

        # Try manual data first (applies to latest year only)
        latest_year = self.get_years()[-1] if self.get_years() else None
        if year is None or year == latest_year:
            # Check if the manual key itself is provided
            if name in self.manual_data and self.manual_data[name] is not None:
                self._provenance[name] = 'manual'
                return float(self.manual_data[name])
            # Check mapped auto column name from manual data
            if auto_col != name and auto_col in self.manual_data and self.manual_data[auto_col] is not None:
                self._provenance[name] = 'manual'
                return float(self.manual_data[auto_col])

        # Fall back to auto data
        if not self._auto_df.empty and auto_col in self._auto_df.columns:
            if year is not None:
                rows = self._auto_df[self._auto_df['Year'] == year]
            else:
                rows = self._auto_df.tail(1)
            if not rows.empty:
                val = rows.iloc[0].get(auto_col)
                if pd.notna(val):
                    self._provenance.setdefault(name, 'auto')
                    return float(val)

        return None

    def get_metric_series(self, name: str) -> Dict[int, float]:
        """Return {year: value} dict for all available years of a metric."""
        auto_col = self._MANUAL_TO_AUTO.get(name, name)
        result: Dict[int, float] = {}

        if not self._auto_df.empty and auto_col in self._auto_df.columns:
            for _, row in self._auto_df.iterrows():
                yr = int(row['Year'])
                val = row.get(auto_col)
                if pd.notna(val):
                    result[yr] = float(val)

        # Overlay manual value onto latest year
        latest_year = max(result.keys()) if result else None
        manual_val = self.manual_data.get(name) or self.manual_data.get(auto_col)
        if manual_val is not None and latest_year is not None:
            result[latest_year] = float(manual_val)

        return result

    def has_auto(self, name: str) -> bool:
        """Check if a metric exists in auto-parsed data."""
        auto_col = self._MANUAL_TO_AUTO.get(name, name)
        return not self._auto_df.empty and auto_col in self._auto_df.columns

    def has_manual(self, name: str) -> bool:
        """Check if a metric was provided via manual input."""
        auto_col = self._MANUAL_TO_AUTO.get(name, name)
        return (name in self.manual_data and self.manual_data[name] is not None) or \
               (auto_col in self.manual_data and self.manual_data[auto_col] is not None)

    @property
    def provenance(self) -> Dict[str, str]:
        return dict(self._provenance)


# ---------------------------------------------------------------------------
# Confidence helper
# ---------------------------------------------------------------------------

def _determine_confidence(
    required_auto: List[str],
    optional_manual: List[str],
    unified: UnifiedFinancialData,
) -> str:
    """Determine confidence tag for a rule.

    Returns AUTO, ENRICHED, LIMITED, or SKIPPED.
    """
    has_all_required = all(unified.get_metric(f) is not None for f in required_auto)
    has_any_manual = any(unified.has_manual(f) for f in optional_manual)

    if not has_all_required:
        # Check if manual data compensates
        all_auto_names = [unified._MANUAL_TO_AUTO.get(f, f) for f in required_auto]
        has_via_manual = all(
            unified.get_metric(f) is not None for f in required_auto
        )
        if has_via_manual:
            return 'ENRICHED'
        # Check if at least some data is available
        any_data = any(unified.get_metric(f) is not None for f in required_auto)
        return 'LIMITED' if any_data else 'SKIPPED'

    if has_any_manual:
        return 'ENRICHED'
    return 'AUTO'


# ---------------------------------------------------------------------------
# Grant rules
# ---------------------------------------------------------------------------

def rule_g1_match_funding_capacity(
    unified: UnifiedFinancialData,
    proposed_award: float,
    payment_mechanism: str,
) -> CrossAnalysisResult:
    """G1: Match-Funding Capacity and Liquidity."""
    title = "Match-Funding Capacity & Liquidity"

    if not proposed_award or proposed_award <= 0:
        return CrossAnalysisResult(
            rule_id="G1", title=title,
            risk_flag="NOT_ASSESSED", confidence="SKIPPED",
            narrative="No proposed award amount provided. This rule requires a proposed grant amount to assess match-funding capacity.",
            recommendation="Enter the proposed award amount to enable this analysis.",
        )

    nca = unified.get_metric('NetCurrentAssets')
    cash = unified.get_metric('CashBankInHand')

    if nca is None and cash is None:
        return CrossAnalysisResult(
            rule_id="G1", title=title,
            risk_flag="NOT_ASSESSED", confidence="SKIPPED",
            narrative="Neither net current assets nor cash at bank data is available. Cannot assess match-funding capacity.",
            recommendation="Upload accounts or enter supplementary financial data.",
        )

    # Determine confidence
    confidence = 'AUTO'
    cash_note = ""
    if cash is None:
        confidence = 'LIMITED'
        cash_note = " Cash position unknown — analysis based on net current assets only."
    elif unified.has_manual('CashAtBank') or unified.has_manual('CashBankInHand'):
        confidence = 'ENRICHED'

    # Core logic
    risk_flag = "LOW"
    narratives = []
    mechanism_lower = (payment_mechanism or 'unknown').lower()

    if nca is not None and nca < 0 and mechanism_lower == 'arrears':
        risk_flag = "HIGH"
        narratives.append(
            f"Net current assets are negative (£{nca:,.0f}) and the grant is paid in arrears. "
            f"The company cannot cash-flow a grant paid in arrears."
        )
    elif nca is not None and nca < 0 and mechanism_lower in ('milestone-based', 'unknown'):
        risk_flag = "HIGH"
        narratives.append(
            f"Net current assets are negative (£{nca:,.0f}). With {payment_mechanism.lower()} payments, "
            f"the company may struggle to bridge funding gaps between milestones."
        )

    if cash is not None and cash < 0.25 * proposed_award and mechanism_lower in ('arrears', 'milestone-based'):
        new_flag = "MEDIUM"
        if risk_flag != "HIGH":
            risk_flag = new_flag
        narratives.append(
            f"Cash at bank (£{cash:,.0f}) is less than 25% of the proposed award "
            f"(£{proposed_award:,.0f}). Limited cash buffer relative to award size."
        )

    if nca is not None and nca > 0 and nca > 0.5 * proposed_award and risk_flag not in ("HIGH", "MEDIUM"):
        risk_flag = "LOW"
        narratives.append(
            f"Net current assets (£{nca:,.0f}) exceed 50% of the proposed award "
            f"(£{proposed_award:,.0f}), suggesting adequate liquidity."
        )

    # Advance payment reduces severity by one tier
    if mechanism_lower == 'advance' and risk_flag in ("HIGH", "MEDIUM"):
        risk_flag = "MEDIUM" if risk_flag == "HIGH" else "LOW"
        narratives.append(
            "Payment mechanism is full advance, which reduces liquidity risk."
        )

    if not narratives:
        narratives.append(
            f"Net current assets: £{nca:,.0f}. " if nca is not None else ""
            f"Proposed award: £{proposed_award:,.0f} ({payment_mechanism})."
        )

    narrative = " ".join(narratives) + cash_note

    return CrossAnalysisResult(
        rule_id="G1", title=title,
        risk_flag=risk_flag, confidence=confidence,
        narrative=narrative,
        recommendation=(
            "Review the company's cash flow projections against the proposed grant schedule. "
            "Consider whether advance payments or adjusted milestones could mitigate liquidity risk."
            if risk_flag in ("HIGH", "MEDIUM") else
            "Liquidity position appears adequate for the proposed grant."
        ),
    )


def rule_g2_grant_dependency(
    unified: UnifiedFinancialData,
    grants_data: Optional[List[Dict]],
) -> CrossAnalysisResult:
    """G2: Grant-Dependency Ratio."""
    title = "Grant-Dependency Ratio"

    if not grants_data:
        return CrossAnalysisResult(
            rule_id="G2", title=title,
            risk_flag="NOT_ASSESSED", confidence="SKIPPED",
            narrative="No grants data available from GrantNav. Cannot assess grant dependency.",
            recommendation="Enable grants lookup to assess grant dependency.",
        )

    net_assets = unified.get_metric('NetAssets')
    if net_assets is None:
        return CrossAnalysisResult(
            rule_id="G2", title=title,
            risk_flag="NOT_ASSESSED", confidence="SKIPPED",
            narrative="Net assets data not available. Cannot compute grant dependency ratio.",
            recommendation="Upload accounts to enable this analysis.",
        )

    # Sum grants in last 3 years
    now = datetime.now()
    three_years_ago = now.replace(year=now.year - 3)
    total_grants_3yr = 0.0
    grant_count_3yr = 0

    for grant in grants_data:
        award_date_str = grant.get('awardDate', '')
        if award_date_str:
            try:
                award_date = datetime.strptime(award_date_str[:10], '%Y-%m-%d')
                if award_date >= three_years_ago:
                    try:
                        amount = float(grant.get('amountAwarded', 0))
                        total_grants_3yr += amount
                        grant_count_3yr += 1
                    except (ValueError, TypeError):
                        pass
            except (ValueError, TypeError):
                pass

    # Confidence
    confidence = 'AUTO'
    if unified.has_manual('Turnover') or unified.has_manual('Revenue'):
        confidence = 'ENRICHED'

    narratives = []
    risk_flag = "LOW"

    # Always flag HIGH if net assets negative
    if net_assets < 0:
        risk_flag = "HIGH"
        narratives.append(
            f"The company has negative net assets (£{net_assets:,.0f}). "
            f"It is insolvent before grant dependency is considered."
        )
    elif net_assets > 0:
        grant_dependency_ratio = total_grants_3yr / net_assets
        narratives.append(
            f"Total grants received in the last 3 years: £{total_grants_3yr:,.0f} "
            f"({grant_count_3yr} grants). Net assets: £{net_assets:,.0f}. "
            f"Grant dependency ratio: {grant_dependency_ratio:.2f}."
        )

        if grant_dependency_ratio > 2.0:
            risk_flag = "HIGH"
            narratives.append(
                "Cumulative grant funding significantly exceeds the net asset base."
            )
        elif grant_dependency_ratio > 1.0:
            risk_flag = "MEDIUM"
            narratives.append("Grant funding exceeds net assets.")
        elif grant_dependency_ratio < 0.5:
            risk_flag = "LOW"

    # Turnover enrichment
    turnover = unified.get_metric('Revenue') or unified.get_metric('Turnover')
    if turnover and turnover > 0:
        grant_revenue_ratio = total_grants_3yr / turnover
        narratives.append(
            f"Grant-to-revenue ratio: {grant_revenue_ratio:.2f} "
            f"(grants as proportion of annual turnover of £{turnover:,.0f})."
        )
        if grant_revenue_ratio > 0.5:
            if risk_flag == "LOW":
                risk_flag = "MEDIUM"
            narratives.append(
                "Grant income represents a major proportion of total revenue."
            )

    return CrossAnalysisResult(
        rule_id="G2", title=title,
        risk_flag=risk_flag, confidence=confidence,
        narrative=" ".join(narratives),
        recommendation=(
            "Assess the company's ability to sustain operations without continued grant funding. "
            "Consider the risk that the organisation is economically dependent on grants."
            if risk_flag in ("HIGH", "MEDIUM") else
            "Grant dependency does not appear excessive relative to the company's financial base."
        ),
    )


# ---------------------------------------------------------------------------
# Financial health rules
# ---------------------------------------------------------------------------

def _consecutive_decline_count(series: Dict[int, float]) -> int:
    """Count consecutive years of decline from the most recent year backwards."""
    if len(series) < 2:
        return 0
    years = sorted(series.keys(), reverse=True)
    count = 0
    for i in range(len(years) - 1):
        if series[years[i]] < series[years[i + 1]]:
            count += 1
        else:
            break
    return count


def _consecutive_increase_count(series: Dict[int, float]) -> int:
    """Count consecutive years of increase from the most recent year backwards."""
    if len(series) < 2:
        return 0
    years = sorted(series.keys(), reverse=True)
    count = 0
    for i in range(len(years) - 1):
        if series[years[i]] > series[years[i + 1]]:
            count += 1
        else:
            break
    return count


def _build_trend_data(series: Dict[int, float]) -> List[Dict]:
    """Build trend data list with year-over-year change."""
    years = sorted(series.keys())
    trend = []
    for i, yr in enumerate(years):
        entry = {'year': yr, 'value': series[yr]}
        if i > 0:
            prev = series[years[i - 1]]
            if prev != 0:
                entry['change_pct'] = round(((series[yr] - prev) / abs(prev)) * 100, 1)
            else:
                entry['change_pct'] = None
        else:
            entry['change_pct'] = None
        trend.append(entry)
    return trend


def rule_f1_capital_erosion(unified: UnifiedFinancialData) -> CrossAnalysisResult:
    """F1: Profitability Proxy — Capital Erosion."""
    title = "Capital Erosion (Profitability Proxy)"

    net_assets_series = unified.get_metric_series('NetAssets')
    if len(net_assets_series) < 2:
        return CrossAnalysisResult(
            rule_id="F1", title=title,
            risk_flag="NOT_ASSESSED",
            confidence="SKIPPED" if not net_assets_series else "LIMITED",
            narrative=(
                "Insufficient filing history for trend analysis. "
                "At least 3 years of capital and reserves data are needed for this rule."
                if not net_assets_series else
                f"Only {len(net_assets_series)} year(s) of data available. "
                "At least 3 years are needed for meaningful trend analysis."
            ),
            recommendation="Obtain additional years of accounts to enable trend analysis.",
        )

    trend_data = _build_trend_data(net_assets_series)
    decline_count = _consecutive_decline_count(net_assets_series)
    latest_year = max(net_assets_series.keys())
    latest_value = net_assets_series[latest_year]

    confidence = 'AUTO'
    if unified.has_manual('NetAssets'):
        confidence = 'ENRICHED'

    narratives = []
    risk_flag = "LOW"

    # Negative net assets in latest year
    if latest_value < 0:
        risk_flag = "HIGH"
        narratives.append(
            f"The company has negative net assets of £{latest_value:,.0f} in {latest_year}."
        )

    # Consecutive decline
    if decline_count >= 3:
        risk_flag = "HIGH"
        narratives.append(
            f"Capital and reserves have declined for {decline_count} consecutive years, "
            "indicating sustained erosion from persistent losses or aggressive extraction."
        )
    elif decline_count >= 2:
        if risk_flag != "HIGH":
            risk_flag = "MEDIUM"
        narratives.append(
            f"Capital and reserves have declined for {decline_count} consecutive years."
        )

    # Check for share capital increase masking erosion
    share_cap_series = unified.get_metric_series('ShareCapital')
    if share_cap_series and len(share_cap_series) >= 2:
        sc_increase = _consecutive_increase_count(share_cap_series)
        if sc_increase >= 1 and decline_count >= 2:
            narratives.append(
                "Note: Called-up share capital has increased during this period. "
                "Capital erosion may be partially masked by new equity issuance."
            )

    if not narratives:
        narratives.append(
            f"Capital and reserves are stable or growing. "
            f"Latest value: £{latest_value:,.0f} ({latest_year})."
        )

    return CrossAnalysisResult(
        rule_id="F1", title=title,
        risk_flag=risk_flag, confidence=confidence,
        narrative=" ".join(narratives),
        recommendation=(
            "Investigate the causes of capital erosion. Request management accounts and "
            "projections showing how the company plans to return to profitability."
            if risk_flag in ("HIGH", "MEDIUM") else
            "Capital position appears stable."
        ),
        trend_data=trend_data,
    )


def rule_f2_intangible_asset_bloat(unified: UnifiedFinancialData) -> CrossAnalysisResult:
    """F2: Intangible Asset Bloat."""
    title = "Intangible Asset Bloat"

    net_assets = unified.get_metric('NetAssets')
    intangibles = unified.get_metric('IntangibleAssets')

    if intangibles is None:
        return CrossAnalysisResult(
            rule_id="F2", title=title,
            risk_flag="NOT_ASSESSED", confidence="SKIPPED",
            narrative=(
                "Accounts do not distinguish tangible from intangible assets. "
                "This is typical of micro-entity accounts where only total fixed assets are reported."
            ),
            recommendation="Request detailed accounts that include an asset breakdown to enable this analysis.",
        )

    if net_assets is None:
        return CrossAnalysisResult(
            rule_id="F2", title=title,
            risk_flag="NOT_ASSESSED", confidence="SKIPPED",
            narrative="Net assets data not available.",
            recommendation="Upload accounts to enable this analysis.",
        )

    confidence = 'AUTO'
    tangible_net_worth = net_assets - intangibles
    total_assets = unified.get_metric('TotalAssets')

    narratives = []
    risk_flag = "LOW"

    # Core check: solvent on paper but insolvent tangibly
    if net_assets > 0 and tangible_net_worth < 0:
        risk_flag = "HIGH"
        narratives.append(
            f"Net assets are positive (£{net_assets:,.0f}) but tangible net worth is negative "
            f"(£{tangible_net_worth:,.0f}). The company is technically insolvent on a tangible "
            f"asset basis — its net asset position depends entirely on intangible assets "
            f"(£{intangibles:,.0f})."
        )

    # Intangibles > 50% of total assets
    if total_assets and total_assets > 0 and intangibles > 0.5 * total_assets:
        if risk_flag != "HIGH":
            risk_flag = "MEDIUM"
        narratives.append(
            f"Intangible assets (£{intangibles:,.0f}) exceed 50% of total assets "
            f"(£{total_assets:,.0f}). Realisable value in distress would be significantly lower."
        )

    # Trend check: growing intangibles with flat/declining tangibles
    intangibles_series = unified.get_metric_series('IntangibleAssets')
    tangibles_series = unified.get_metric_series('TangibleAssets')
    if len(intangibles_series) >= 2 and len(tangibles_series) >= 2:
        intangibles_growing = _consecutive_increase_count(intangibles_series) >= 1
        tangibles_flat_or_declining = _consecutive_decline_count(tangibles_series) >= 1 or \
            _consecutive_increase_count(tangibles_series) == 0
        if intangibles_growing and tangibles_flat_or_declining:
            if risk_flag == "LOW":
                risk_flag = "MEDIUM"
            narratives.append(
                "Intangible assets are growing year-on-year while tangible assets are flat or declining."
            )

    if not narratives:
        narratives.append(
            f"Intangible assets (£{intangibles:,.0f}) relative to net assets "
            f"(£{net_assets:,.0f}) do not raise concerns. "
            f"Tangible net worth: £{tangible_net_worth:,.0f}."
        )

    return CrossAnalysisResult(
        rule_id="F2", title=title,
        risk_flag=risk_flag, confidence=confidence,
        narrative=" ".join(narratives),
        recommendation=(
            "Request a breakdown of intangible assets (goodwill, IP, development costs) and "
            "assess their realisable value. Consider whether the balance sheet overstates the "
            "company's true financial position."
            if risk_flag in ("HIGH", "MEDIUM") else
            "Asset composition does not raise concerns."
        ),
    )


def rule_f3_working_capital_deterioration(unified: UnifiedFinancialData) -> CrossAnalysisResult:
    """F3: Working Capital Deterioration."""
    title = "Working Capital Deterioration"

    nca_series = unified.get_metric_series('NetCurrentAssets')
    if len(nca_series) < 2:
        return CrossAnalysisResult(
            rule_id="F3", title=title,
            risk_flag="NOT_ASSESSED",
            confidence="SKIPPED" if not nca_series else "LIMITED",
            narrative=(
                "Insufficient data for working capital trend analysis. "
                "At least 3 years of net current assets data are needed."
                if not nca_series else
                f"Only {len(nca_series)} year(s) of net current assets data available."
            ),
            recommendation="Obtain additional years of accounts to enable trend analysis.",
        )

    trend_data = _build_trend_data(nca_series)
    decline_count = _consecutive_decline_count(nca_series)
    latest_year = max(nca_series.keys())
    latest_nca = nca_series[latest_year]

    # Check for positive-to-negative swing
    years_sorted = sorted(nca_series.keys())
    positive_to_negative = False
    for i in range(len(years_sorted) - 1):
        if nca_series[years_sorted[i]] > 0 and nca_series[years_sorted[i + 1]] < 0:
            positive_to_negative = True
            break

    confidence = 'AUTO'
    narratives = []
    risk_flag = "LOW"

    # Basic version
    if positive_to_negative:
        risk_flag = "HIGH"
        narratives.append(
            "Net current assets have moved from positive to negative, "
            "indicating a critical deterioration in working capital."
        )
    elif decline_count >= 3:
        risk_flag = "MEDIUM"
        narratives.append(
            f"Net current assets have declined for {decline_count} consecutive years, "
            "indicating sustained working capital deterioration."
        )
    elif decline_count >= 2:
        risk_flag = "MEDIUM"
        narratives.append(
            f"Net current assets have declined for {decline_count} consecutive years."
        )

    # Detailed version — cash trap detection
    debtors = unified.get_metric('Debtors')
    stock = unified.get_metric('StockInventory')
    cash = unified.get_metric('CashBankInHand')
    creditors = unified.get_metric('CurrentLiabilities')

    has_detail = any(v is not None for v in [debtors, stock])
    if has_detail:
        confidence = 'ENRICHED' if unified.has_manual('ManualDebtors') or unified.has_manual('StockInventory') else confidence

        debtors_series = unified.get_metric_series('Debtors')
        cash_series = unified.get_metric_series('CashBankInHand')
        creditors_series = unified.get_metric_series('CurrentLiabilities')

        debtors_growing = _consecutive_increase_count(debtors_series) >= 1 if len(debtors_series) >= 2 else False
        cash_flat_declining = _consecutive_decline_count(cash_series) >= 1 if len(cash_series) >= 2 else False
        creditors_rising = _consecutive_increase_count(creditors_series) >= 1 if len(creditors_series) >= 2 else False

        if debtors_growing and cash_flat_declining and creditors_rising:
            risk_flag = "HIGH"
            narratives.append(
                "Classic cash trap pattern detected: debtors are growing while cash is "
                "flat or declining, and creditors due within one year are rising. "
                "Sales are not converting to cash and supplier payments are being delayed."
            )

    if not narratives:
        narratives.append(
            f"Working capital position is stable. "
            f"Net current assets: £{latest_nca:,.0f} ({latest_year})."
        )

    return CrossAnalysisResult(
        rule_id="F3", title=title,
        risk_flag=risk_flag, confidence=confidence,
        narrative=" ".join(narratives),
        recommendation=(
            "Request a detailed working capital breakdown and cash flow forecast. "
            "Assess whether the company can meet its obligations as they fall due."
            if risk_flag in ("HIGH", "MEDIUM") else
            "Working capital position does not raise immediate concerns."
        ),
        trend_data=trend_data,
    )


def rule_f4_leverage_creep(unified: UnifiedFinancialData) -> CrossAnalysisResult:
    """F4: Leverage Creep."""
    title = "Leverage Creep"

    lt_creditors_series = unified.get_metric_series('CreditorsAfterOneYear')
    if len(lt_creditors_series) < 2:
        return CrossAnalysisResult(
            rule_id="F4", title=title,
            risk_flag="NOT_ASSESSED",
            confidence="SKIPPED" if not lt_creditors_series else "LIMITED",
            narrative=(
                "Long-term creditors data not available or insufficient for trend analysis."
            ),
            recommendation="Upload accounts covering 3+ years to enable leverage trend analysis.",
        )

    trend_data = _build_trend_data(lt_creditors_series)
    increase_count = _consecutive_increase_count(lt_creditors_series)
    net_assets_series = unified.get_metric_series('NetAssets')

    confidence = 'AUTO'
    narratives = []
    risk_flag = "LOW"

    # Check if long-term creditors increasing while net assets stagnant/declining
    na_stagnant_declining = False
    if len(net_assets_series) >= 2:
        na_decline = _consecutive_decline_count(net_assets_series)
        na_increase = _consecutive_increase_count(net_assets_series)
        na_stagnant_declining = na_decline >= 1 or na_increase == 0

    if increase_count >= 3 and na_stagnant_declining:
        risk_flag = "MEDIUM"
        narratives.append(
            f"Long-term creditors have increased for {increase_count} consecutive years "
            "while net assets are stagnant or declining. This indicates increasing "
            "long-term indebtedness without corresponding asset growth."
        )
    elif increase_count >= 3:
        risk_flag = "MEDIUM"
        narratives.append(
            f"Long-term creditors have increased for {increase_count} consecutive years."
        )

    # Director loans enrichment
    director_loans = unified.get_metric('DirectorLoans')
    if director_loans is not None:
        confidence = 'ENRICHED'
        latest_year = max(lt_creditors_series.keys())
        latest_lt = lt_creditors_series[latest_year]
        if latest_lt > 0:
            dl_pct = (director_loans / latest_lt) * 100
            if dl_pct > 50:
                narratives.append(
                    f"Director loans (£{director_loans:,.0f}) represent {dl_pct:.0f}% of long-term "
                    f"creditors (£{latest_lt:,.0f}). Growth is primarily director loans — "
                    "may indicate owner-funded bootstrapping rather than third-party leverage."
                )
                # Director loan-driven growth is less risky than third-party
                if risk_flag == "MEDIUM":
                    risk_flag = "LOW"
            else:
                third_party = latest_lt - director_loans
                narratives.append(
                    f"Director loans (£{director_loans:,.0f}) represent {dl_pct:.0f}% of long-term "
                    f"creditors. Third-party debt: £{third_party:,.0f}."
                )
                if risk_flag == "MEDIUM":
                    risk_flag = "HIGH"
                    narratives.append(
                        "Growing reliance on external debt with stagnant fundamentals."
                    )

    if not narratives:
        narratives.append("Long-term creditor position is stable.")

    return CrossAnalysisResult(
        rule_id="F4", title=title,
        risk_flag=risk_flag, confidence=confidence,
        narrative=" ".join(narratives),
        recommendation=(
            "Review the composition of long-term creditors and assess the sustainability "
            "of the company's debt profile."
            if risk_flag in ("HIGH", "MEDIUM") else
            "Leverage position does not raise immediate concerns."
        ),
        trend_data=trend_data,
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_cross_analysis(
    unified: UnifiedFinancialData,
    grants_data: Optional[List[Dict]],
    proposed_award: float,
    payment_mechanism: str,
    late_filing_detected: bool = False,
    company_age_months: Optional[float] = None,
    accounts_type: Optional[str] = None,
) -> CrossAnalysisReport:
    """Run all cross-analysis rules and assemble the report."""

    results = [
        rule_g1_match_funding_capacity(unified, proposed_award, payment_mechanism),
        rule_g2_grant_dependency(unified, grants_data),
        rule_f1_capital_erosion(unified),
        rule_f2_intangible_asset_bloat(unified),
        rule_f3_working_capital_deterioration(unified),
        rule_f4_leverage_creep(unified),
    ]

    # Composite warning: 3+ HIGH flags
    high_count = sum(1 for r in results if r.risk_flag == "HIGH")
    composite_warning = None
    if high_count >= 3:
        composite_warning = (
            f"Multiple high-risk indicators detected across {high_count} checks. "
            "This company warrants detailed manual review before any award decision."
        )

    # Special pattern: G1 + G2 + F1 all HIGH
    pattern_warnings = []
    g1_high = any(r.rule_id == "G1" and r.risk_flag == "HIGH" for r in results)
    g2_high = any(r.rule_id == "G2" and r.risk_flag == "HIGH" for r in results)
    f1_high = any(r.rule_id == "F1" and r.risk_flag == "HIGH" for r in results)
    if g1_high and g2_high and f1_high:
        pattern_warnings.append(
            "Critical pattern detected: The combination of insufficient match-funding capacity (G1), "
            "high grant dependency (G2), and sustained capital erosion (F1) represents a particularly "
            "concerning risk profile. This company may be unable to deliver grant-funded activities "
            "without significant financial distress."
        )

    # Filing quality caveat
    filing_quality_caveat = None
    if late_filing_detected:
        filing_quality_caveat = (
            "Note: This company has a history of late or irregular filing. "
            "Financial analysis should be interpreted with caution."
        )

    # Company age note
    company_age_note = None
    if company_age_months is not None and company_age_months < 36:
        company_age_note = (
            "Recently incorporated company — limited filing history available. "
            "Trend-based analyses may have reduced reliability."
        )

    return CrossAnalysisReport(
        results=results,
        composite_warning=composite_warning,
        pattern_warnings=pattern_warnings,
        filing_quality_caveat=filing_quality_caveat,
        company_age_note=company_age_note,
        accounts_type=accounts_type,
    )
