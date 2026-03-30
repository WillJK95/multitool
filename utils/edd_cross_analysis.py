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
    value_format: str = 'currency'  # 'currency', 'percentage', 'multiplier'

    @property
    def unified_severity(self) -> str:
        """Map cross-analysis risk flags to the unified taxonomy."""
        return {
            'HIGH': 'Elevated',
            'MEDIUM': 'Moderate',
            'LOW': 'Low',
            'NOT_ASSESSED': 'Not Assessed',
        }.get(self.risk_flag, self.risk_flag)

    @property
    def unified_confidence_label(self) -> str:
        """Human-readable confidence label."""
        return {
            'AUTO': 'Based on filed accounts',
            'ENRICHED': 'Supplemented by user-provided data',
            'LIMITED': 'Limited data available',
            'SKIPPED': 'Insufficient data',
        }.get(self.confidence, self.confidence)


@dataclass
class CrossAnalysisReport:
    """Aggregated output from all cross-analysis rules."""
    results: List[CrossAnalysisResult]
    composite_warning: Optional[str] = None
    pattern_warnings: List[str] = field(default_factory=list)
    filing_quality_caveat: Optional[str] = None
    company_age_note: Optional[str] = None
    accounts_type: Optional[str] = None


@dataclass
class CrossAnalysisThresholds:
    """Configurable thresholds for all cross-analysis rules.

    All fields have sensible defaults matching the previously hard-coded values.
    Pass a customised instance to ``run_cross_analysis`` to override.
    """
    # G1: Match-Funding Capacity
    g1_cash_buffer_pct: float = 0.25       # Cash < this * award → MEDIUM flag
    g1_nca_comfortable_pct: float = 0.5   # NCA > this * award → LOW (comfortable)
    # G2: Grant Dependency
    g2_lookback_years: int = 3
    g2_dependency_high: float = 2.0
    g2_dependency_medium: float = 1.0
    g2_revenue_ratio: float = 0.5
    # G3: Grant Management Experience
    g3_scale_high_pct: float = 100.0      # Award > historical max by this % → HIGH
    g3_scale_medium_pct: float = 50.0     # Award > historical max by this % → MEDIUM
    # F1: Capital Erosion
    f1_erosion_high_years: int = 3
    f1_erosion_medium_years: int = 2
    # F2: Intangible Asset Bloat
    f2_intangible_bloat_pct: float = 0.5
    # F3: Working Capital Deterioration
    f3_nca_drop_pct: float = 0.25
    # F4: Leverage Creep
    f4_leverage_years: int = 3
    # Return on Equity
    roe_negative_years_medium: int = 2
    roe_negative_years_high: int = 3
    # Asset Turnover Efficiency
    asset_turnover_decline_years: int = 2
    asset_turnover_min: float = 0.3
    # Profit Margin Compression
    profit_margin_negative_years_medium: int = 2
    profit_margin_negative_years_high: int = 3
    profit_margin_compression_pts: float = 10.0
    # Staff Cost Burden
    staff_cost_ratio_max: float = 0.75
    staff_cost_ratio_critical: float = 0.90
    # Composite warning
    composite_high_count: int = 3


# ---------------------------------------------------------------------------
# Unified data layer
# ---------------------------------------------------------------------------

class UnifiedFinancialData:
    """Merges auto-parsed iXBRL data with manual user input.

    Parameters
    ----------
    auto_analyzer : FinancialAnalyzer or None
        The existing FinancialAnalyzer populated from iXBRL files.
    manual_data : list[dict] or dict
        Either a list of year-dicts (each with ``'_year'`` key and metric
        values) for multi-year manual entry, or a single flat dict for
        backward compatibility.
    """

    def __init__(self, auto_analyzer=None, manual_data=None):
        self.auto_analyzer = auto_analyzer
        self._provenance: Dict[str, str] = {}  # metric -> "auto" | "manual"

        # Normalise manual_data to a list of year-dicts
        if manual_data is None:
            self._manual_years: List[Dict] = []
        elif isinstance(manual_data, list):
            self._manual_years = [d for d in manual_data if isinstance(d, dict)]
        elif isinstance(manual_data, dict):
            # Legacy single-dict format — wrap in a list
            self._manual_years = [manual_data] if manual_data else []
        else:
            self._manual_years = []

        # Index manual data by year for fast lookup
        self._manual_by_year: Dict[int, Dict] = {}
        for entry in self._manual_years:
            yr = entry.get('_year')
            if yr is not None:
                self._manual_by_year[int(yr)] = entry

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

    # Reverse mapping: auto column name → manual field key(s)
    _AUTO_TO_MANUAL = {
        'Revenue': 'Turnover',
        'ProfitLoss': 'PreTaxProfitLoss',
        'CashBankInHand': 'CashAtBank',
        'Debtors': 'ManualDebtors',
    }

    # -- accessors ----------------------------------------------------------

    def get_years(self) -> List[int]:
        """Return sorted list of all years available (auto + manual)."""
        years = set()
        if not self._auto_df.empty:
            years.update(self._auto_df['Year'].dropna().astype(int).tolist())
        years.update(self._manual_by_year.keys())
        return sorted(years)

    def _all_keys_for(self, name: str) -> List[str]:
        """Return all possible dict keys for a metric (manual + auto names)."""
        auto_col = self._MANUAL_TO_AUTO.get(name, name)
        manual_key = self._AUTO_TO_MANUAL.get(name)
        keys = [name]
        if auto_col != name:
            keys.append(auto_col)
        if manual_key and manual_key not in keys:
            keys.append(manual_key)
        return keys

    def _manual_value(self, name: str, year: int) -> Optional[float]:
        """Look up a manual value for a specific year."""
        entry = self._manual_by_year.get(year)
        if entry is None:
            return None
        for key in self._all_keys_for(name):
            val = entry.get(key)
            if val is not None:
                return float(val)
        return None

    def get_metric(self, name: str, year: Optional[int] = None) -> Optional[float]:
        """Return a metric value, preferring manual data where available.

        If *year* is None, return the value for the most recent year.
        """
        auto_col = self._MANUAL_TO_AUTO.get(name, name)
        all_years = self.get_years()
        target_year = year if year is not None else (all_years[-1] if all_years else None)

        if target_year is not None:
            # Try manual data for this year
            manual_val = self._manual_value(name, target_year)
            if manual_val is not None:
                self._provenance[name] = 'manual'
                return manual_val

        # Fall back to auto data
        if not self._auto_df.empty and auto_col in self._auto_df.columns:
            if target_year is not None:
                rows = self._auto_df[self._auto_df['Year'] == target_year]
            else:
                rows = self._auto_df.tail(1)
            if not rows.empty:
                val = rows.iloc[0].get(auto_col)
                if pd.notna(val):
                    self._provenance.setdefault(name, 'auto')
                    return float(val)

        return None

    def get_metric_series(self, name: str) -> Dict[int, float]:
        """Return {year: value} dict for all available years of a metric.

        Manual data overlays/supplements auto-parsed data per year.
        """
        auto_col = self._MANUAL_TO_AUTO.get(name, name)
        result: Dict[int, float] = {}

        # Auto data first
        if not self._auto_df.empty and auto_col in self._auto_df.columns:
            for _, row in self._auto_df.iterrows():
                yr = int(row['Year'])
                val = row.get(auto_col)
                if pd.notna(val):
                    result[yr] = float(val)

        # Overlay / add manual data per year (check all possible key names)
        keys_to_check = self._all_keys_for(name)
        for yr, entry in self._manual_by_year.items():
            for key in keys_to_check:
                val = entry.get(key)
                if val is not None:
                    result[yr] = float(val)
                    break

        return result

    def has_auto(self, name: str) -> bool:
        """Check if a metric exists in auto-parsed data."""
        auto_col = self._MANUAL_TO_AUTO.get(name, name)
        return not self._auto_df.empty and auto_col in self._auto_df.columns

    def has_manual(self, name: str) -> bool:
        """Check if a metric was provided via manual input in any year."""
        keys_to_check = self._all_keys_for(name)
        for entry in self._manual_years:
            for key in keys_to_check:
                if entry.get(key) is not None:
                    return True
        return False

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
    thresholds: CrossAnalysisThresholds = None,
) -> CrossAnalysisResult:
    if thresholds is None:
        thresholds = CrossAnalysisThresholds()
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

    if cash is not None and cash < thresholds.g1_cash_buffer_pct * proposed_award and mechanism_lower in ('arrears', 'milestone-based'):
        new_flag = "MEDIUM"
        if risk_flag != "HIGH":
            risk_flag = new_flag
        narratives.append(
            f"Cash at bank (£{cash:,.0f}) is less than {thresholds.g1_cash_buffer_pct*100:.0f}% of the proposed award "
            f"(£{proposed_award:,.0f}). Limited cash buffer relative to award size."
        )

    if nca is not None and nca > 0 and nca > thresholds.g1_nca_comfortable_pct * proposed_award and risk_flag not in ("HIGH", "MEDIUM"):
        risk_flag = "LOW"
        narratives.append(
            f"Net current assets (£{nca:,.0f}) exceed {thresholds.g1_nca_comfortable_pct*100:.0f}% of the proposed award "
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
    thresholds: CrossAnalysisThresholds = None,
) -> CrossAnalysisResult:
    if thresholds is None:
        thresholds = CrossAnalysisThresholds()
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

    # Sum grants in last N years
    now = datetime.now()
    three_years_ago = now.replace(year=now.year - thresholds.g2_lookback_years)
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

    if net_assets <= 0:
        return CrossAnalysisResult(
            rule_id="G2", title=title,
            risk_flag="NOT_ASSESSED", confidence=confidence,
            narrative=(
                f"Net assets are negative (£{net_assets:,.0f}). Grant dependency ratio "
                "cannot be meaningfully calculated when the entity is insolvent. "
                "See the financial health findings for solvency assessment."
            ),
            recommendation=(
                "Address the underlying solvency position before assessing grant dependency."
            ),
        )

    if net_assets > 0:
        grant_dependency_ratio = total_grants_3yr / net_assets
        narratives.append(
            f"Total grants received in the last {thresholds.g2_lookback_years} years: £{total_grants_3yr:,.0f} "
            f"({grant_count_3yr} grants). Net assets: £{net_assets:,.0f}. "
            f"Grant dependency ratio: {grant_dependency_ratio:.2f}."
        )

        if grant_dependency_ratio > thresholds.g2_dependency_high:
            risk_flag = "HIGH"
            narratives.append(
                "Cumulative grant funding significantly exceeds the net asset base."
            )
        elif grant_dependency_ratio > thresholds.g2_dependency_medium:
            risk_flag = "MEDIUM"
            narratives.append("Grant funding exceeds net assets.")

    # Turnover enrichment
    turnover = unified.get_metric('Revenue') or unified.get_metric('Turnover')
    if turnover and turnover > 0:
        grant_revenue_ratio = total_grants_3yr / turnover
        narratives.append(
            f"Grant-to-revenue ratio: {grant_revenue_ratio:.2f} "
            f"(grants as proportion of annual turnover of £{turnover:,.0f})."
        )
        if grant_revenue_ratio > thresholds.g2_revenue_ratio:
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


def rule_f1_capital_erosion(unified: UnifiedFinancialData, thresholds: CrossAnalysisThresholds = None) -> CrossAnalysisResult:
    if thresholds is None:
        thresholds = CrossAnalysisThresholds()
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
    if decline_count >= thresholds.f1_erosion_high_years:
        risk_flag = "HIGH"
        narratives.append(
            f"Capital and reserves have declined for {decline_count} consecutive years, "
            "indicating sustained erosion from persistent losses or aggressive extraction."
        )
    elif decline_count >= thresholds.f1_erosion_medium_years:
        if risk_flag != "HIGH":
            risk_flag = "MEDIUM"
        narratives.append(
            f"Capital and reserves have declined for {decline_count} consecutive years."
        )

    # Check for share capital increase masking erosion
    share_cap_series = unified.get_metric_series('ShareCapital')
    if share_cap_series and len(share_cap_series) >= 2:
        sc_increase = _consecutive_increase_count(share_cap_series)
        if sc_increase >= 1 and decline_count >= thresholds.f1_erosion_medium_years:
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


def rule_f2_intangible_asset_bloat(unified: UnifiedFinancialData, thresholds: CrossAnalysisThresholds = None) -> CrossAnalysisResult:
    if thresholds is None:
        thresholds = CrossAnalysisThresholds()
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

    # Intangibles > threshold % of total assets
    if total_assets and total_assets > 0 and intangibles > thresholds.f2_intangible_bloat_pct * total_assets:
        if risk_flag != "HIGH":
            risk_flag = "MEDIUM"
        narratives.append(
            f"Intangible assets (£{intangibles:,.0f}) exceed {thresholds.f2_intangible_bloat_pct*100:.0f}% of total assets "
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


def rule_f3_working_capital_deterioration(unified: UnifiedFinancialData, thresholds: CrossAnalysisThresholds = None) -> CrossAnalysisResult:
    if thresholds is None:
        thresholds = CrossAnalysisThresholds()
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

    # Compute peak NCA and single-year drop for additional checks
    peak_nca = max(nca_series.values())
    prior_year = years_sorted[-2] if len(years_sorted) >= 2 else None
    prior_nca = nca_series[prior_year] if prior_year is not None else None

    # Single-year significant drop: latest year dropped > threshold from prior year
    single_year_large_drop = (
        prior_nca is not None
        and prior_nca != 0
        and ((latest_nca - prior_nca) / abs(prior_nca)) < -thresholds.f3_nca_drop_pct
    )

    # Peak-to-latest deterioration: latest NCA is > threshold below the historical peak
    peak_to_latest_deterioration = (
        peak_nca != 0
        and peak_nca > 0
        and latest_nca < peak_nca
        and ((latest_nca - peak_nca) / abs(peak_nca)) < -thresholds.f3_nca_drop_pct
    )

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
    elif single_year_large_drop and prior_nca is not None:
        pct = ((latest_nca - prior_nca) / abs(prior_nca)) * 100
        risk_flag = "MEDIUM"
        narratives.append(
            f"Net current assets fell by {abs(pct):.0f}% in the most recent year "
            f"(from £{prior_nca:,.0f} to £{latest_nca:,.0f}), "
            "a significant single-year deterioration in working capital."
        )
    elif peak_to_latest_deterioration:
        pct = ((latest_nca - peak_nca) / abs(peak_nca)) * 100
        risk_flag = "MEDIUM"
        narratives.append(
            f"Net current assets of £{latest_nca:,.0f} ({latest_year}) are "
            f"{abs(pct):.0f}% below the historical peak of £{peak_nca:,.0f}, "
            "indicating a material erosion of the working capital buffer."
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


def rule_f4_leverage_creep(unified: UnifiedFinancialData, thresholds: CrossAnalysisThresholds = None) -> CrossAnalysisResult:
    if thresholds is None:
        thresholds = CrossAnalysisThresholds()
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

    if increase_count >= thresholds.f4_leverage_years and na_stagnant_declining:
        risk_flag = "MEDIUM"
        narratives.append(
            f"Long-term creditors have increased for {increase_count} consecutive years "
            "while net assets are stagnant or declining. This indicates increasing "
            "long-term indebtedness without corresponding asset growth."
        )
    elif increase_count >= thresholds.f4_leverage_years:
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
# IGM-specific rule
# ---------------------------------------------------------------------------

def rule_g3_grant_management_experience(
    grants_data: Optional[List[Dict]],
    proposed_award: float,
    thresholds: CrossAnalysisThresholds = None,
) -> CrossAnalysisResult:
    if thresholds is None:
        thresholds = CrossAnalysisThresholds()
    """G3: Grant Management Experience (IGM mode only).

    Compares the proposed award to the largest grant the organisation has
    previously received, as a proxy for their experience managing grants at
    this scale.
    """
    title = "Grant Management Experience"

    if not proposed_award or proposed_award <= 0:
        return CrossAnalysisResult(
            rule_id="G3", title=title,
            risk_flag="NOT_ASSESSED", confidence="SKIPPED",
            narrative="No proposed award amount provided. This rule requires a proposed grant amount.",
            recommendation="Enter the proposed award amount to enable this analysis.",
        )

    if not grants_data:
        return CrossAnalysisResult(
            rule_id="G3", title=title,
            risk_flag="NOT_ASSESSED", confidence="SKIPPED",
            narrative=(
                "No grants data available from GrantNav. Cannot assess grant management experience. "
                "This rule requires historical grants data to compare against the proposed award."
            ),
            recommendation="Ensure grants data is enabled and the organisation appears in GrantNav.",
        )

    amounts = []
    for grant in grants_data:
        try:
            amount = float(grant.get('amountAwarded', 0))
            if amount > 0:
                amounts.append(amount)
        except (ValueError, TypeError):
            pass

    if not amounts:
        return CrossAnalysisResult(
            rule_id="G3", title=title,
            risk_flag="NOT_ASSESSED", confidence="SKIPPED",
            narrative="Grants data found but no valid award amounts could be parsed.",
            recommendation="Review grants data manually to assess grant management track record.",
        )

    largest_historical = max(amounts)
    excess_pct = ((proposed_award - largest_historical) / largest_historical) * 100

    if excess_pct >= thresholds.g3_scale_high_pct:
        risk_flag = "HIGH"
        narrative = (
            f"The proposed award (£{proposed_award:,.0f}) is more than double the largest grant "
            f"this organisation has previously received (£{largest_historical:,.0f} — a "
            f"{excess_pct:.0f}% increase). Managing grants at this scale represents a significant "
            f"step-change and may exceed the organisation's current governance, financial controls, "
            f"and operational capacity."
        )
        recommendation = (
            "Assess whether the organisation has the staffing, financial controls, and reporting "
            "infrastructure to manage a grant of this scale. Strongly consider phased or milestone-"
            "based payments, enhanced monitoring conditions, and a capacity review prior to award."
        )
    elif excess_pct >= thresholds.g3_scale_medium_pct:
        risk_flag = "MEDIUM"
        narrative = (
            f"The proposed award (£{proposed_award:,.0f}) is {excess_pct:.0f}% larger than the "
            f"largest grant this organisation has previously received (£{largest_historical:,.0f}). "
            f"While the organisation has relevant grant management experience, this represents a "
            f"material increase in scale and associated responsibility."
        )
        recommendation = (
            "Verify that the organisation has sufficient staffing, financial controls, and reporting "
            "capacity to manage a grant of this size. Consider milestone-based payments to manage "
            "delivery risk proportionate to the increased scale."
        )
    else:
        risk_flag = "LOW"
        narrative = (
            f"The proposed award (£{proposed_award:,.0f}) is within a comparable range to the "
            f"largest grant this organisation has previously received (£{largest_historical:,.0f}). "
            f"The organisation appears to have relevant experience managing grants at this scale."
        )
        recommendation = "Grant scale is consistent with the organisation's prior grant management experience."

    return CrossAnalysisResult(
        rule_id="G3", title=title,
        risk_flag=risk_flag, confidence="AUTO",
        narrative=narrative,
        recommendation=recommendation,
    )


# ---------------------------------------------------------------------------
# New rules: income-statement / efficiency checks
# ---------------------------------------------------------------------------

def rule_roe_trend(unified: UnifiedFinancialData, thresholds: CrossAnalysisThresholds = None) -> CrossAnalysisResult:
    """ROE: Return on Equity trend."""
    title = "Return on Equity (ROE)"
    if thresholds is None:
        thresholds = CrossAnalysisThresholds()

    profit_series = unified.get_metric_series('ProfitLoss')
    na_series = unified.get_metric_series('NetAssets')

    # Need at least 2 matched years
    shared_years = sorted(set(profit_series) & set(na_series))
    if len(shared_years) < 2:
        return CrossAnalysisResult(
            rule_id="ROE", title=title,
            risk_flag="NOT_ASSESSED",
            confidence="SKIPPED" if not shared_years else "LIMITED",
            narrative=(
                "Insufficient data to calculate Return on Equity. "
                "At least 2 years of both profit/loss and net assets figures are required."
            ),
            recommendation="Upload multi-year iXBRL accounts or enter data manually to enable this analysis.",
        )

    roe_series = {}
    skipped = []
    for yr in shared_years:
        na = na_series[yr]
        pl = profit_series[yr]
        if na is None or na <= 0:
            skipped.append(yr)
            continue
        roe_series[yr] = (pl / na) * 100  # as %

    if len(roe_series) < 2:
        return CrossAnalysisResult(
            rule_id="ROE", title=title,
            risk_flag="NOT_ASSESSED", confidence="LIMITED",
            narrative=(
                "ROE cannot be computed for most years because net assets are zero or negative. "
                "This is consistent with findings from the Solvency and Capital Erosion checks."
            ),
            recommendation="Address underlying solvency issues before interpreting ROE.",
        )

    confidence = 'ENRICHED' if unified.has_manual('PreTaxProfitLoss') else 'AUTO'
    trend_data = _build_trend_data(roe_series)
    latest_year = max(roe_series)

    # Count consecutive years of negative ROE (most recent years)
    years_sorted = sorted(roe_series)
    neg_streak = 0
    for yr in reversed(years_sorted):
        if roe_series[yr] < 0:
            neg_streak += 1
        else:
            break

    narratives = []
    risk_flag = "LOW"

    if neg_streak >= thresholds.roe_negative_years_high:
        risk_flag = "HIGH"
        narratives.append(
            f"Return on equity has been negative for {neg_streak} consecutive years, "
            "indicating sustained destruction of shareholder value."
        )
    elif neg_streak >= thresholds.roe_negative_years_medium:
        risk_flag = "MEDIUM"
        narratives.append(
            f"Return on equity has been negative for {neg_streak} consecutive years "
            f"({', '.join(str(y) for y in years_sorted[-neg_streak:])})."
        )
    else:
        latest_roe = roe_series[latest_year]
        # Check declining trend even if positive
        decline_count = _consecutive_decline_count(roe_series)
        if decline_count >= 2 and latest_roe > 0:
            narratives.append(
                f"Return on equity is positive ({latest_roe:.1f}% in {latest_year}) but "
                f"has declined for {decline_count} consecutive years, indicating eroding capital efficiency."
            )
            risk_flag = "MEDIUM"

    if not narratives:
        latest_roe = roe_series[latest_year]
        narratives.append(
            f"Return on equity is {latest_roe:.1f}% in {latest_year}. "
            "Capital is being used productively."
        )

    return CrossAnalysisResult(
        rule_id="ROE", title=title,
        risk_flag=risk_flag, confidence=confidence,
        narrative=" ".join(narratives),
        recommendation=(
            "Investigate the causes of poor capital returns. Assess whether management "
            "has a credible plan to restore profitability."
            if risk_flag in ("HIGH", "MEDIUM") else
            "Capital efficiency does not raise concerns."
        ),
        trend_data=trend_data,
        value_format='percentage',
    )


def rule_asset_turnover(unified: UnifiedFinancialData, thresholds: CrossAnalysisThresholds = None) -> CrossAnalysisResult:
    """ATR: Asset Turnover Efficiency."""
    title = "Asset Turnover Efficiency"
    if thresholds is None:
        thresholds = CrossAnalysisThresholds()

    rev_series = unified.get_metric_series('Revenue')
    ta_series = unified.get_metric_series('TotalAssets')

    shared_years = sorted(set(rev_series) & set(ta_series))
    if len(shared_years) < 2:
        return CrossAnalysisResult(
            rule_id="ATR", title=title,
            risk_flag="NOT_ASSESSED",
            confidence="SKIPPED" if not shared_years else "LIMITED",
            narrative=(
                "Insufficient data to calculate asset turnover. "
                "Revenue and total assets figures are required for at least 2 years."
            ),
            recommendation="Upload multi-year iXBRL accounts to enable this analysis.",
        )

    atr_series = {}
    for yr in shared_years:
        ta = ta_series[yr]
        rev = rev_series[yr]
        if ta and ta > 0 and rev is not None:
            atr_series[yr] = rev / ta

    if len(atr_series) < 2:
        return CrossAnalysisResult(
            rule_id="ATR", title=title,
            risk_flag="NOT_ASSESSED", confidence="LIMITED",
            narrative="Asset turnover could not be computed for sufficient years (total assets may be zero or missing).",
            recommendation="Review the completeness of uploaded accounts.",
        )

    confidence = 'AUTO'
    trend_data = _build_trend_data(atr_series)
    decline_count = _consecutive_decline_count(atr_series)
    latest_year = max(atr_series)
    latest_atr = atr_series[latest_year]

    narratives = []
    risk_flag = "LOW"

    if decline_count >= thresholds.asset_turnover_decline_years:
        risk_flag = "MEDIUM"
        narratives.append(
            f"Asset turnover has declined for {decline_count} consecutive years "
            f"(currently {latest_atr:.2f}× in {latest_year}), suggesting the asset base "
            "is becoming progressively less efficient at generating revenue."
        )

    if latest_atr < thresholds.asset_turnover_min:
        if risk_flag != "HIGH":
            risk_flag = "MEDIUM"
        narratives.append(
            f"Asset turnover of {latest_atr:.2f}× is very low, indicating the company "
            "generates little revenue relative to its total asset base. This may signal "
            "dormant or non-productive assets."
        )

    if not narratives:
        narratives.append(
            f"Asset turnover is {latest_atr:.2f}× in {latest_year}. "
            "Revenue generation relative to assets does not raise concerns."
        )

    return CrossAnalysisResult(
        rule_id="ATR", title=title,
        risk_flag=risk_flag, confidence=confidence,
        narrative=" ".join(narratives),
        recommendation=(
            "Request a breakdown of the asset base and assess whether fixed assets are "
            "being actively used. Low or declining turnover may warrant a review of asset "
            "utilisation and business model viability."
            if risk_flag in ("HIGH", "MEDIUM") else
            "Asset utilisation does not raise concerns."
        ),
        trend_data=trend_data,
        value_format='multiplier',
    )


def rule_profit_margin(unified: UnifiedFinancialData, thresholds: CrossAnalysisThresholds = None) -> CrossAnalysisResult:
    """PMG: Profit Margin Compression."""
    title = "Profit Margin Compression"
    if thresholds is None:
        thresholds = CrossAnalysisThresholds()

    profit_series = unified.get_metric_series('ProfitLoss')
    rev_series = unified.get_metric_series('Revenue')

    shared_years = sorted(set(profit_series) & set(rev_series))
    if len(shared_years) < 2:
        return CrossAnalysisResult(
            rule_id="PMG", title=title,
            risk_flag="NOT_ASSESSED",
            confidence="SKIPPED" if not shared_years else "LIMITED",
            narrative=(
                "Insufficient data to calculate profit margin trend. "
                "Profit/loss and revenue figures are required for at least 2 years."
            ),
            recommendation="Upload multi-year iXBRL accounts or enter data manually to enable this analysis.",
        )

    margin_series = {}
    for yr in shared_years:
        rev = rev_series[yr]
        pl = profit_series[yr]
        if rev and rev > 0 and pl is not None:
            margin_series[yr] = (pl / rev) * 100  # as %

    if len(margin_series) < 2:
        return CrossAnalysisResult(
            rule_id="PMG", title=title,
            risk_flag="NOT_ASSESSED", confidence="LIMITED",
            narrative="Profit margin could not be computed for sufficient years (revenue may be zero or missing).",
            recommendation="Review the completeness of uploaded accounts.",
        )

    confidence = 'ENRICHED' if unified.has_manual('PreTaxProfitLoss') or unified.has_manual('Turnover') else 'AUTO'
    trend_data = _build_trend_data(margin_series)
    years_sorted = sorted(margin_series)
    latest_year = years_sorted[-1]
    latest_margin = margin_series[latest_year]

    # Count consecutive negative-margin years
    neg_streak = 0
    for yr in reversed(years_sorted):
        if margin_series[yr] < 0:
            neg_streak += 1
        else:
            break

    # Measure overall compression (first available vs latest)
    earliest_margin = margin_series[years_sorted[0]]
    compression_pts = earliest_margin - latest_margin  # positive = margin fell

    narratives = []
    risk_flag = "LOW"

    if neg_streak >= thresholds.profit_margin_negative_years_high:
        risk_flag = "HIGH"
        narratives.append(
            f"Profit margin has been negative for {neg_streak} consecutive years, "
            "indicating that the company is loss-making on a sustained basis."
        )
    elif neg_streak >= thresholds.profit_margin_negative_years_medium:
        risk_flag = "MEDIUM"
        narratives.append(
            f"Profit margin has been negative for {neg_streak} consecutive years "
            f"({', '.join(str(y) for y in years_sorted[-neg_streak:])})."
        )
    elif compression_pts >= thresholds.profit_margin_compression_pts:
        risk_flag = "MEDIUM"
        narratives.append(
            f"Profit margin has compressed by {compression_pts:.1f} percentage points "
            f"from {earliest_margin:.1f}% ({years_sorted[0]}) to {latest_margin:.1f}% ({latest_year}). "
            "Sustained margin compression, even with positive margins, suggests rising cost pressure "
            "or pricing weakness."
        )

    if not narratives:
        narratives.append(
            f"Profit margin is {latest_margin:.1f}% in {latest_year}. "
            "Margin does not show signs of significant compression."
        )

    return CrossAnalysisResult(
        rule_id="PMG", title=title,
        risk_flag=risk_flag, confidence=confidence,
        narrative=" ".join(narratives),
        recommendation=(
            "Investigate the drivers of margin deterioration (cost inflation, revenue mix, "
            "pricing pressure). Request a management explanation and forward projections."
            if risk_flag in ("HIGH", "MEDIUM") else
            "Profit margin does not raise concerns."
        ),
        trend_data=trend_data,
        value_format='percentage',
    )


def rule_staff_cost_burden(unified: UnifiedFinancialData, thresholds: CrossAnalysisThresholds = None) -> CrossAnalysisResult:
    """SCB: Staff Cost Burden."""
    title = "Staff Cost Burden"
    if thresholds is None:
        thresholds = CrossAnalysisThresholds()

    staff_costs = unified.get_metric('StaffCosts')
    revenue = unified.get_metric('Revenue')

    if staff_costs is None:
        return CrossAnalysisResult(
            rule_id="SCB", title=title,
            risk_flag="NOT_ASSESSED", confidence="SKIPPED",
            narrative=(
                "Staff costs not available — this field requires manual entry in the "
                "Supplementary Accounts Data section. This rule is most relevant for "
                "service-sector organisations where labour is the primary cost."
            ),
            recommendation="Enter staff costs in the Supplementary Accounts section to enable this analysis.",
        )

    if revenue is None or revenue <= 0:
        return CrossAnalysisResult(
            rule_id="SCB", title=title,
            risk_flag="NOT_ASSESSED", confidence="SKIPPED",
            narrative="Revenue data not available; staff cost ratio cannot be calculated.",
            recommendation="Ensure revenue figures are available in accounts or entered manually.",
        )

    ratio = staff_costs / revenue
    confidence = 'ENRICHED'

    # Also check trend in staff cost ratio if multi-year data available
    staff_series = unified.get_metric_series('StaffCosts')
    rev_series = unified.get_metric_series('Revenue')
    ratio_series = {}
    shared_years = sorted(set(staff_series) & set(rev_series))
    for yr in shared_years:
        s = staff_series[yr]
        r = rev_series[yr]
        if s is not None and r and r > 0:
            ratio_series[yr] = s / r

    trend_data = _build_trend_data({yr: v * 100 for yr, v in ratio_series.items()}) if ratio_series else None

    narratives = []
    risk_flag = "LOW"

    if ratio > thresholds.staff_cost_ratio_critical:
        risk_flag = "HIGH"
        narratives.append(
            f"Staff costs (£{staff_costs:,.0f}) represent {ratio*100:.0f}% of revenue "
            f"(£{revenue:,.0f}). At this level the organisation is highly exposed to any "
            "revenue shortfall — even a modest decline would push it into operating losses."
        )
    elif ratio > thresholds.staff_cost_ratio_max:
        risk_flag = "MEDIUM"
        narratives.append(
            f"Staff costs (£{staff_costs:,.0f}) represent {ratio*100:.0f}% of revenue "
            f"(£{revenue:,.0f}). A high staff cost ratio leaves limited margin for other "
            "operational costs and creates vulnerability to revenue volatility."
        )

    # Rising trend check
    if len(ratio_series) >= 2:
        rise_count = _consecutive_increase_count(ratio_series)
        if rise_count >= 2:
            narratives.append(
                f"The staff cost ratio has been rising for {rise_count} consecutive years, "
                "indicating that wage growth is outpacing revenue growth."
            )
            if risk_flag == "LOW":
                risk_flag = "MEDIUM"

    if not narratives:
        narratives.append(
            f"Staff costs represent {ratio*100:.0f}% of revenue. "
            "This does not raise immediate concerns."
        )

    return CrossAnalysisResult(
        rule_id="SCB", title=title,
        risk_flag=risk_flag, confidence=confidence,
        narrative=" ".join(narratives),
        recommendation=(
            "Assess whether the organisation has flexibility to reduce staff costs if "
            "revenue falls. Consider whether grant conditions could be met if revenues "
            "decline and payroll cannot be adjusted quickly."
            if risk_flag in ("HIGH", "MEDIUM") else
            "Staff cost burden does not raise concerns."
        ),
        trend_data=trend_data,
        value_format='percentage',
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
    igm_mode: bool = False,
    entity_type: str = "company",
    thresholds: CrossAnalysisThresholds = None,
) -> CrossAnalysisReport:
    """Run all cross-analysis rules and assemble the report.

    When ``igm_mode`` is True the grant-dependency rule (G2) is replaced by
    the grant management experience rule (G3), which is more appropriate for
    organisations that are themselves grant-giving bodies.
    Pass a ``CrossAnalysisThresholds`` instance to override default thresholds.
    """
    if thresholds is None:
        thresholds = CrossAnalysisThresholds()

    if igm_mode:
        grant_rule = rule_g3_grant_management_experience(grants_data, proposed_award, thresholds)
    else:
        grant_rule = rule_g2_grant_dependency(unified, grants_data, thresholds)

    results = [
        rule_g1_match_funding_capacity(unified, proposed_award, payment_mechanism, thresholds),
        grant_rule,
        rule_f1_capital_erosion(unified, thresholds),
        rule_f3_working_capital_deterioration(unified, thresholds),
        rule_roe_trend(unified, thresholds),
        rule_asset_turnover(unified, thresholds),
        rule_profit_margin(unified, thresholds),
        rule_staff_cost_burden(unified, thresholds),
    ]
    # F2/F4 depend on account line-items that are generally unavailable in
    # Charity Commission datasets. Excluding these in charity mode avoids
    # showing non-actionable "skipped" checks in the report summary.
    if entity_type != "charity":
        results.insert(3, rule_f2_intangible_asset_bloat(unified, thresholds))
        results.insert(5, rule_f4_leverage_creep(unified, thresholds))

    # Composite warning
    high_count = sum(1 for r in results if r.risk_flag == "HIGH")
    composite_warning = None
    if high_count >= thresholds.composite_high_count:
        composite_warning = (
            f"Multiple high-risk indicators detected across {high_count} checks. "
            "This company warrants detailed manual review before any award decision."
        )

    # Special pattern: G1 + G2 + F1 all HIGH (only relevant in standard mode)
    pattern_warnings = []
    if not igm_mode:
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
