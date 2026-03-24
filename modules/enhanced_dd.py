# modules/enhanced_dd.py
"""Enhanced Due Diligence"""

import os
import base64
import html
import threading
import traceback
import webbrowser
import tkinter as tk
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from tkinter import ttk, filedialog, messagebox

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from rapidfuzz.fuzz import WRatio

from ..utils.financial_analyzer import FinancialAnalyzer, iXBRLParser
from ..ui.tooltip import Tooltip
from ..api.companies_house import ch_get_data
from ..api.charity_commission import (
    cc_get_charity_details_v2,
    cc_get_financial_history,
    cc_get_assets_liabilities,
    cc_get_overview,
    cc_get_account_ar_info,
    cc_get_governing_document,
    cc_get_registration_history,
    cc_get_regulatory_report,
    cc_get_policy_information,
    cc_get_other_regulators,
    cc_get_other_names,
    cc_get_area_of_operation,
)
from ..constants import (
    CONFIG_DIR,
    FILING_TYPE_CATEGORIES,
    MANUAL_INPUT_FIELDS_TIER1,
    MANUAL_INPUT_FIELDS_TIER2,
    BALANCE_SHEET_FIELDS,
    INCOME_STATEMENT_FIELDS,
    CHARITY_BALANCE_SHEET_FIELDS,
    CHARITY_INCOME_STATEMENT_FIELDS,
    PAYMENT_MECHANISMS,
    CHARITY_EDD_THRESHOLDS,
)
from .base import InvestigationModuleBase
from ..utils.helpers import log_message, clean_company_number
from ..utils.edd_visualizations import (
    generate_company_timeline,
    fetch_grants_for_company,
    fetch_grants_for_org,
    generate_grants_report_html,
    trace_ownership_chain,
    generate_static_ownership_graph,
    format_display_date,
)
from ..utils.edd_cross_analysis import (
    UnifiedFinancialData,
    CrossAnalysisThresholds,
    run_cross_analysis,
)
from ..utils.edd_charity_checks import (
    check_charity_status,
    check_reporting_status,
    check_regulatory_reports,
    check_accounts_qualified,
    check_accounts_submission_pattern,
    check_net_assets,
    check_reserves_ratio,
    check_income_expenditure_trends,
    check_income_volatility,
    check_fundraising_cost_ratio,
    check_government_funding_concentration,
    check_trustee_remuneration,
    check_policies,
    check_trustee_count,
    check_contact_transparency,
    check_default_address as check_charity_default_address,
    check_area_of_operation,
    check_professional_fundraiser,
)
from ..utils.edd_charity_visualizations import (
    generate_charity_chart_html,
    generate_charity_profile_html,
    generate_charity_limitations_html,
)
from ..utils.charity_financial_data import CharityFinancialData

class EnhancedDueDiligence(InvestigationModuleBase):
    def __init__(self, parent_app, api_key, back_callback, ch_token_bucket,
                 charity_api_key=None, prefill_entity=None):
        super().__init__(parent_app, back_callback, api_key, help_key=None)
        self.ch_token_bucket = ch_token_bucket
        self.charity_api_key = charity_api_key
        self._prefill_entity = prefill_entity
        self.company_data = {}
        self.charity_data = {}
        self.financial_analyzer = None
        self.accounts_loaded = False
        self._entity_type = 'company'  # 'company' or 'charity'
        
        # Default thresholds (used by all check methods and cross-analysis rules)
        self.thresholds = {
            # Solvency
            'solvency_decline_pct': 30,
            # Liquidity
            'current_ratio_min': 1.0,
            'current_ratio_critical': 0.5,
            'quick_ratio_min': 0.5,
            'cash_pct_min': 10,
            'debt_to_equity_max': 2.0,
            # Revenue trends
            'revenue_decline_pct': -10,
            'revenue_decline_years': 2,
            'consecutive_loss_years': 2,
            # Predictive outlook
            'predictive_profit_decline_pct': 20,
            'predictive_revenue_decline_pct': 15,
            # Governance / filing
            'late_filings_count': 2,
            'late_filings_period': 5,
            'director_churn_count': 3,
            'director_churn_months': 12,
            # Deep investigation
            'insolvency_company_count': 3,
            'insolvency_critical_count': 5,
            'phoenix_similarity_pct': 80,
            'phoenix_officer_count': 5,
            # Cross-analysis: G1
            'g1_cash_buffer_pct': 0.25,
            'g1_nca_comfortable_pct': 0.5,
            # Cross-analysis: G2
            'g2_lookback_years': 3,
            'g2_dependency_high': 2.0,
            'g2_dependency_medium': 1.0,
            'g2_revenue_ratio': 0.5,
            # Cross-analysis: G3
            'g3_scale_high_pct': 100.0,
            'g3_scale_medium_pct': 50.0,
            # Cross-analysis: F1
            'f1_erosion_high_years': 3,
            'f1_erosion_medium_years': 2,
            # Cross-analysis: F2
            'f2_intangible_bloat_pct': 0.5,
            # Cross-analysis: F3
            'f3_nca_drop_pct': 0.25,
            # Cross-analysis: F4
            'f4_leverage_years': 3,
            # Cross-analysis: ROE
            'roe_negative_years_medium': 2,
            'roe_negative_years_high': 3,
            # Cross-analysis: Asset Turnover
            'asset_turnover_decline_years': 2,
            'asset_turnover_min': 0.3,
            # Cross-analysis: Profit Margin
            'profit_margin_negative_years_medium': 2,
            'profit_margin_negative_years_high': 3,
            'profit_margin_compression_pts': 10.0,
            # Cross-analysis: Staff Cost Burden
            'staff_cost_ratio_max': 0.75,
            'staff_cost_ratio_critical': 0.90,
            # Composite warning
            'composite_high_count': 3,
        }
        
        self._build_ui()

        # Apply prefill if provided (from Quick Launch)
        if self._prefill_entity:
            etype = self._prefill_entity.get("type", "company")
            eid = self._prefill_entity.get("id", "")
            self.entity_type_var.set(etype)
            self._on_entity_type_changed()
            self.company_num_var.set(eid)
            if eid:
                self.after(200, self.fetch_entity_profile)

    def _build_ui(self):
        # Step 1: Entity Lookup
        self._lookup_frame = ttk.LabelFrame(
            self.content_frame, text="Step 1: Enter Company Number", padding=10
        )
        self._lookup_frame.pack(fill=tk.X, pady=5, padx=10)

        # Entity type selector
        entity_row = ttk.Frame(self._lookup_frame)
        entity_row.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(entity_row, text="Entity Type:").pack(side=tk.LEFT, padx=(0, 10))
        self.entity_type_var = tk.StringVar(value='company')
        ttk.Radiobutton(
            entity_row, text="Company", variable=self.entity_type_var,
            value='company', command=self._on_entity_type_changed,
        ).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Radiobutton(
            entity_row, text="Charity", variable=self.entity_type_var,
            value='charity', command=self._on_entity_type_changed,
        ).pack(side=tk.LEFT)

        input_frame = ttk.Frame(self._lookup_frame)
        input_frame.pack(fill=tk.X, pady=5)
        self._input_label = ttk.Label(input_frame, text="Company Number:")
        self._input_label.pack(side=tk.LEFT, padx=(0, 5))
        self.company_num_var = tk.StringVar()
        company_entry = ttk.Entry(input_frame, textvariable=self.company_num_var, width=15)
        company_entry.pack(side=tk.LEFT, padx=5)
        self.fetch_btn = ttk.Button(
            input_frame, text="Fetch Company Data", command=self.fetch_entity_profile
        )
        self.fetch_btn.pack(side=tk.LEFT, padx=5)

        self.company_summary_text = tk.Text(self._lookup_frame, height=6, wrap=tk.WORD, state='disabled')
        self.company_summary_text.pack(fill=tk.X, pady=5)

        # Step 2: Upload Accounts (hidden for charity)
        self._upload_frame = ttk.LabelFrame(
            self.content_frame, text="Step 2: Upload iXBRL Accounts (Optional)", padding=10
        )
        upload_frame = self._upload_frame
        upload_frame.pack(fill=tk.X, pady=5, padx=10)
        
        buttons_frame = ttk.Frame(upload_frame)
        buttons_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(
            buttons_frame, text="Upload Accounts Files...", command=self.load_accounts
        ).pack(side=tk.LEFT, padx=(0, 5))
        
        ttk.Button(
            buttons_frame, text="Clear Files", command=self.clear_accounts
        ).pack(side=tk.LEFT, padx=5)
        
        self.accounts_status_label = ttk.Label(upload_frame, text="No accounts loaded.")
        self.accounts_status_label.pack(side=tk.LEFT, padx=10)

        # Charity mode note (replaces upload frame when charity selected)
        self._charity_accounts_note = ttk.LabelFrame(
            self.content_frame, text="Step 2: Financial Data", padding=10
        )
        ttk.Label(
            self._charity_accounts_note,
            text="Financial data for charities is retrieved automatically from the "
                 "Charity Commission API. No accounts upload is required.",
            wraplength=600,
        ).pack(anchor='w')
        # Not packed by default — shown only for charity mode

        # Step 2b: Manual Input Form (collapsible)
        self._build_manual_input_form()

        # Step 3: Configure Analysis
        config_frame = ttk.LabelFrame(
            self.content_frame, text="Step 3: Configure Analysis", padding=10
        )
        config_frame.pack(fill=tk.X, pady=5, padx=10)

        self.check_vars = {}
        self.check_widgets = {}

        # All standard checks always run — hardcode as True BooleanVars (no UI controls)
        for key in (
            'solvency', 'liquidity', 'filing_status', 'company_status',
            'director_churn', 'revenue_trends', 'predictive_outlook',
            'default_address', 'accounting_changes', 'offshore_pscs',
            'filing_patterns',
        ):
            self.check_vars[key] = tk.BooleanVar(value=True)

        # Deep investigation: one var shared by both slow checks (off by default)
        deep_var = tk.BooleanVar(value=False)
        self.check_vars['director_history'] = deep_var
        self.check_vars['phoenix_check'] = deep_var

        # Grants: one var shared by lookup + cross-analysis (off by default)
        grants_var = tk.BooleanVar(value=False)
        self.check_vars['grants_lookup'] = grants_var
        self.check_vars['cross_analysis'] = grants_var

        # IGM mode and ownership graph
        igm_var = tk.BooleanVar(value=False)
        self.check_vars['igm_mode'] = igm_var
        ownership_var = tk.BooleanVar(value=False)
        self.check_vars['ownership_graph'] = ownership_var

        ttk.Label(
            config_frame,
            text="All standard due diligence checks run automatically. Select additional options:"
        ).pack(anchor='w', pady=(0, 6))

        grants_cb = ttk.Checkbutton(
            config_frame,
            text="Include grants data (360Giving GrantNav) and financial cross-analysis",
            variable=grants_var,
        )
        grants_cb.pack(anchor='w')
        self.check_widgets['grants_lookup'] = grants_cb

        igm_cb = ttk.Checkbutton(
            config_frame,
            text="Intermediate grant maker (IGM) — adjusts rules for grant-giving organisations",
            variable=igm_var,
        )
        igm_cb.pack(anchor='w')
        self.check_widgets['igm_mode'] = igm_cb

        ownership_cb = ttk.Checkbutton(
            config_frame,
            text="Corporate ownership structure graph (makes additional API calls per corporate PSC)",
            variable=ownership_var,
        )
        ownership_cb.pack(anchor='w')
        self.check_widgets['ownership_graph'] = ownership_cb

        deep_cb = ttk.Checkbutton(
            config_frame,
            text="Deep investigation — director insolvency history & phoenix check (slow)",
            variable=deep_var,
        )
        deep_cb.pack(anchor='w')
        self.check_widgets['director_history'] = deep_cb

        # Enabling IGM mode auto-enables grants (G3 rule needs grants data)
        def _on_igm_toggle(*_args):
            if igm_var.get():
                grants_var.set(True)
        igm_var.trace_add('write', _on_igm_toggle)

        ttk.Button(
            config_frame,
            text="Rule Details & Thresholds...",
            command=self._open_rules_window,
        ).pack(anchor='w', pady=(8, 0))

        # Industry context
        context_frame = ttk.Frame(config_frame)
        context_frame.pack(fill=tk.X, pady=5)
        ttk.Label(context_frame, text="Industry Context (Optional):").pack(anchor='w')
        self.industry_context_var = tk.StringVar()
        ttk.Entry(context_frame, textvariable=self.industry_context_var, width=50).pack(fill=tk.X)
        Tooltip(context_frame, "e.g., 'Construction sector' - helps readers interpret ratios appropriately")
        
        # Step 4: Generate Report
        generate_frame = ttk.LabelFrame(
            self.content_frame, text="Step 4: Generate Report", padding=10
        )
        generate_frame.pack(fill=tk.BOTH, expand=True, pady=5, padx=10)
        
        self.generate_btn = ttk.Button(
            generate_frame,
            text="Generate Due Diligence Report",
            state='disabled',
            command=self.start_report_generation,
            bootstyle='success'
        )
        self.generate_btn.pack(pady=10, ipady=5)
        
        self.progress_bar = ttk.Progressbar(
            generate_frame, orient="horizontal", length=300, mode="determinate"
        )
        self.progress_bar.pack(pady=10)
        
        self.status_var = tk.StringVar(value="Ready. Enter a company number to begin.")
        ttk.Label(generate_frame, textvariable=self.status_var).pack()

    def clear_accounts(self):
        """Clear loaded accounts."""
        self.financial_analyzer = None
        self.accounts_loaded = False
        self.accounts_status_label.config(text="No accounts loaded.", foreground="black")
        self._update_accounts_checkboxes()
        self.status_var.set("Accounts cleared.")

    def _validate_accounts_match_company(self):
        """Check if uploaded accounts match the selected company."""
        if not self.financial_analyzer or self.financial_analyzer.data.empty:
            return True, "No accounts to validate"
        
        issues = []
        current_company_number = self.company_data['profile'].get('company_number', '')
        
        # Strip leading zeros for comparison (handles both 11566024 and 011566024)
        current_company_number_stripped = current_company_number.lstrip('0')
        
        # Check each source file
        for file_path in self.financial_analyzer.files_processed:
            try:
                parser = iXBRLParser(file_path)
                
                # Extract company number from xbrli:identifier
                # This is reliable and always present in iXBRL files
                identifiers = parser.tree.xpath(
                    ".//xbrli:identifier[@scheme='http://www.companieshouse.gov.uk/']",
                    namespaces=parser.namespaces
                )
                
                if identifiers and identifiers[0].text:
                    file_company_number = identifiers[0].text.strip()
                    file_company_number_stripped = file_company_number.lstrip('0')
                    
                    if file_company_number_stripped != current_company_number_stripped:
                        issues.append(
                            f"File '{Path(file_path).name}' is for company {file_company_number}, "
                            f"but you selected company {current_company_number}"
                        )
                else:
                    # Couldn't find company number in file
                    issues.append(f"Could not find company number in file '{Path(file_path).name}'")
            
            except Exception as e:
                log_message(f"Error validating {file_path}: {e}")
                issues.append(f"Could not validate file '{Path(file_path).name}': {e}")
        
        if issues:
            return False, "\n".join(issues)
        
        return True, "All accounts match the selected company"

    def _open_rules_window(self):
        """Open the Rule Details & Thresholds modal window."""
        from ..ui.scrollable_frame import ScrollableFrame

        if hasattr(self, '_rules_window') and self._rules_window.winfo_exists():
            self._rules_window.lift()
            return

        win = tk.Toplevel(self)
        self._rules_window = win
        win.title("Rule Details & Thresholds")
        win.geometry("820x660")
        win.minsize(640, 500)
        win.transient(self)
        win.grab_set()

        # Keys stored as integers
        _INT_KEYS = {
            'revenue_decline_years', 'consecutive_loss_years', 'late_filings_count',
            'late_filings_period', 'director_churn_count', 'director_churn_months',
            'insolvency_company_count', 'insolvency_critical_count',
            'phoenix_similarity_pct', 'phoenix_officer_count', 'cash_pct_min',
            'solvency_decline_pct', 'predictive_profit_decline_pct',
            'predictive_revenue_decline_pct', 'g2_lookback_years',
            'f1_erosion_high_years', 'f1_erosion_medium_years',
            'f4_leverage_years', 'composite_high_count',
            'roe_negative_years_medium', 'roe_negative_years_high',
            'asset_turnover_decline_years',
            'profit_margin_negative_years_medium', 'profit_margin_negative_years_high',
        }
        local_vars = {}
        for key, val in self.thresholds.items():
            if key in _INT_KEYS:
                local_vars[key] = tk.IntVar(value=int(val))
            else:
                local_vars[key] = tk.DoubleVar(value=float(val))

        notebook = ttk.Notebook(win)
        notebook.pack(fill='both', expand=True, padx=10, pady=(10, 0))

        def make_tab(title):
            sf = ScrollableFrame(notebook)
            notebook.add(sf, text=title)
            return sf.scrollable_frame

        def rule_section(parent, title, description):
            outer = ttk.Frame(parent)
            outer.pack(fill='x', padx=6, pady=(8, 0))
            ttk.Label(outer, text=title, font=('TkDefaultFont', 9, 'bold')).pack(anchor='w')
            frame = ttk.LabelFrame(outer, padding=(8, 4))
            frame.pack(fill='x', pady=(4, 0))
            desc = ttk.Label(frame, text=description, wraplength=720,
                             justify='left', foreground='grey')
            desc.pack(anchor='w', pady=(0, 6))
            return frame

        def trow(parent, label, key, from_, to, increment, note=''):
            row = ttk.Frame(parent)
            row.pack(fill='x', pady=2)
            ttk.Label(row, text=label, width=50, anchor='w').pack(side='left')
            is_int = key in _INT_KEYS
            sp_kwargs = dict(textvariable=local_vars[key], from_=from_, to=to,
                             increment=increment, width=8)
            if not is_int:
                sp_kwargs['format'] = '%.2f'
            ttk.Spinbox(row, **sp_kwargs).pack(side='left', padx=4)
            if note:
                ttk.Label(row, text=note, foreground='grey').pack(side='left', padx=4)

        # ── Tab 1: Financial Analysis ────────────────────────────────────────
        tab1 = make_tab("Financial Analysis")

        s = rule_section(tab1, "Solvency — Net Asset Position",
            "Checks whether net assets are positive and stable. Negative net assets indicate "
            "technical insolvency. A large year-on-year decline also triggers a warning.")
        trow(s, "Net asset year-on-year decline to flag (%)", 'solvency_decline_pct',
             5, 80, 5, note="Flags if single-year decline exceeds this %")

        s = rule_section(tab1, "Capital Erosion",
            "Tracks whether net assets are declining year on year. Sustained erosion of the "
            "equity base is a leading indicator of financial distress, particularly if driven "
            "by operating losses rather than planned distributions.")
        trow(s, "Consecutive net asset decline years — HIGH", 'f1_erosion_high_years', 2, 10, 1)
        trow(s, "Consecutive net asset decline years — MEDIUM", 'f1_erosion_medium_years', 1, 8, 1)

        s = rule_section(tab1, "Liquidity — Current & Quick Ratios",
            "Assesses the company's ability to meet short-term obligations. The current ratio "
            "compares all current assets to current liabilities; the quick ratio excludes "
            "inventory. Low ratios indicate difficulty paying debts as they fall due.")
        trow(s, "Current ratio — warn threshold (Elevated)", 'current_ratio_min', 0.1, 3.0, 0.1)
        trow(s, "Current ratio — critical threshold (below = Critical severity)", 'current_ratio_critical', 0.1, 1.5, 0.1)
        trow(s, "Quick ratio — warn threshold", 'quick_ratio_min', 0.1, 2.0, 0.1)
        trow(s, "Cash as % of current liabilities — warn if below (%)", 'cash_pct_min',
             1, 50, 1, note="Flags very low cash relative to short-term debts")

        s = rule_section(tab1, "Working Capital Deterioration",
            "Monitors the trend in net current assets (current assets minus current "
            "liabilities). A single large drop or sustained multi-year decline signals "
            "worsening short-term financial health.")
        trow(s, "Single-year NCA drop proportion to flag", 'f3_nca_drop_pct', 0.05, 0.90, 0.05,
             note="e.g. 0.25 = a 25% drop in one year triggers a flag")

        s = rule_section(tab1, "Leverage Creep",
            "Checks whether total long-term creditors have been rising consistently while net "
            "assets are stagnant or declining. Increasing leverage in this context creates "
            "refinancing and solvency risk.")
        trow(s, "Consecutive creditor-increase years to flag", 'f4_leverage_years', 2, 8, 1)

        s = rule_section(tab1, "Intangible Asset Bloat",
            "Checks whether intangible assets (goodwill, IP, software) represent an unusually "
            "large share of total assets. High intangible ratios can inflate the balance sheet; "
            "these assets may not be realisable in a wind-down scenario.")
        trow(s, "Intangibles as proportion of total assets — warn if above",
             'f2_intangible_bloat_pct', 0.1, 1.0, 0.05,
             note="e.g. 0.5 = intangibles > 50% of total assets")

        s = rule_section(tab1, "Revenue & Profitability Trends",
            "Detects sustained revenue decline or consecutive loss-making years, measured "
            "against filed accounts. Occasional losses can be acceptable; persistent trends "
            "are a warning sign.")
        trow(s, "Revenue cumulative decline % to flag", 'revenue_decline_pct', -80, -1, 1,
             note="e.g. -10 flags a 10% cumulative decline")
        trow(s, "Revenue decline measured over N years", 'revenue_decline_years', 1, 10, 1)
        trow(s, "Consecutive loss-making years to flag", 'consecutive_loss_years', 1, 10, 1)

        s = rule_section(tab1, "Return on Equity (ROE)",
            "Measures how efficiently the company uses its capital base (net assets / "
            "shareholders' equity) to generate profit. Sustained negative ROE means the "
            "company is destroying value for its owners. Skipped if net assets are ≤ 0.")
        trow(s, "Consecutive years of negative ROE — MEDIUM", 'roe_negative_years_medium', 1, 8, 1)
        trow(s, "Consecutive years of negative ROE — HIGH", 'roe_negative_years_high', 2, 10, 1)

        s = rule_section(tab1, "Asset Turnover Efficiency",
            "Measures how effectively the company uses its total asset base to generate "
            "revenue (Revenue ÷ Total Assets). A declining ratio suggests assets are becoming "
            "progressively less productive. A very low absolute ratio may indicate dormant "
            "or non-operational assets.")
        trow(s, "Consecutive years of declining ratio to flag", 'asset_turnover_decline_years', 1, 8, 1)
        trow(s, "Absolute ratio — warn if below (all years)", 'asset_turnover_min', 0.05, 2.0, 0.05,
             note="e.g. 0.3 = revenue less than 30% of total assets")

        s = rule_section(tab1, "Profit Margin Compression",
            "Tracks the net profit margin (Profit/Loss ÷ Revenue) as a trend, separately from "
            "absolute revenue movements. Margin compression — even with growing revenue — "
            "signals rising cost pressure or pricing weakness.")
        trow(s, "Consecutive years of negative margin — MEDIUM", 'profit_margin_negative_years_medium', 1, 8, 1)
        trow(s, "Consecutive years of negative margin — HIGH", 'profit_margin_negative_years_high', 2, 10, 1)
        trow(s, "Overall margin compression to flag (percentage points)", 'profit_margin_compression_pts',
             2.0, 50.0, 1.0, note="e.g. 10 = margin fell by 10pp over available period")

        s = rule_section(tab1, "Staff Cost Burden",
            "Compares staff costs to revenue to assess operational fragility. A very high "
            "ratio leaves little margin for other costs and makes the organisation vulnerable "
            "to any revenue shortfall. Requires staff costs to be entered manually in "
            "Supplementary Accounts Data.")
        trow(s, "Staff costs as proportion of revenue — MEDIUM", 'staff_cost_ratio_max',
             0.30, 0.99, 0.05, note="e.g. 0.75 = staff costs > 75% of revenue")
        trow(s, "Staff costs as proportion of revenue — HIGH (critical)", 'staff_cost_ratio_critical',
             0.50, 1.0, 0.05)

        s = rule_section(tab1, "Predictive Financial Outlook",
            "Uses linear extrapolation of filed accounts to project key metrics one year "
            "forward. Flags when the trajectory points toward insolvency, worsening losses, "
            "or revenue collapse. Requires at least 2 years of accounts.")
        trow(s, "Projected profit/loss worsening % to flag", 'predictive_profit_decline_pct',
             5, 80, 5, note="Applied as: projected worsening exceeds this %")
        trow(s, "Projected revenue decline % to flag", 'predictive_revenue_decline_pct', 5, 50, 5)

        s = rule_section(tab1, "Director & PSC Turnover",
            "Counts director appointments and resignations in a rolling window. High turnover "
            "can indicate governance instability or internal disputes. Exactly double the "
            "warning threshold triggers Critical severity.")
        trow(s, "Total director changes to flag", 'director_churn_count', 1, 20, 1)
        trow(s, "Rolling window for changes (months)", 'director_churn_months', 3, 60, 3)

        s = rule_section(tab1, "Filing Compliance",
            "Checks for late or missing annual returns and accounts. Persistent late filing "
            "indicates poor governance and may affect the reliability of financial information.")
        trow(s, "Late filings to flag", 'late_filings_count', 1, 10, 1)
        trow(s, "Late filings measured over N years", 'late_filings_period', 1, 10, 1)

        s = rule_section(tab1, "Debt-to-Equity Ratio",
            "Compares total debt to shareholders' equity. Retained for future use in the "
            "report — not currently used to generate a finding.")
        trow(s, "Debt-to-equity ratio — warn if above", 'debt_to_equity_max', 0.5, 10.0, 0.5)

        # ── Tab 2: Deep Investigation ────────────────────────────────────────
        tab2 = make_tab("Deep Investigation")

        s = rule_section(tab2, "Director Insolvency History",
            "Checks whether current directors have previously been associated with companies "
            "that entered liquidation, administration, or dissolution. Multiple associations "
            "may indicate elevated risk or poor business judgment.")
        trow(s, "Insolvent companies per director — warn threshold", 'insolvency_company_count',
             1, 10, 1)
        trow(s, "Insolvent companies per director — Critical threshold", 'insolvency_critical_count',
             2, 15, 1)

        s = rule_section(tab2, "Phoenix Company Detection",
            "Compares the current company name against dissolved or liquidated companies "
            "associated with the same directors. A high name-similarity score suggests the "
            "company may be a phoenix of a previously failed entity.")
        trow(s, "Name similarity % to flag as a phoenix match", 'phoenix_similarity_pct', 50, 99, 5)
        trow(s, "Number of officers to check (top N)", 'phoenix_officer_count', 1, 20, 1)

        # ── Tab 3: Grants Analysis ───────────────────────────────────────────
        tab3 = make_tab("Grants Analysis")

        s = rule_section(tab3, "Match-Funding Capacity & Liquidity",
            "Assesses whether the organisation has sufficient liquidity to manage a grant, "
            "particularly when payments are made in arrears or on a milestone basis. Compares "
            "cash at bank and net current assets to the proposed award amount.")
        trow(s, "Cash buffer — warn if cash < X × proposed award", 'g1_cash_buffer_pct',
             0.05, 0.75, 0.05, note="e.g. 0.25 = cash must be ≥25% of award")
        trow(s, "NCA comfortable — OK if NCA > X × proposed award", 'g1_nca_comfortable_pct',
             0.10, 1.0, 0.10)

        s = rule_section(tab3, "Grant Dependency Ratio  (standard mode only)",
            "Compares total grant income in recent years to the organisation's net assets and "
            "annual revenue. High grant dependency creates fragility if grant income is "
            "disrupted. Not applied when Intermediate Grant Maker mode is selected.")
        trow(s, "Grant lookback period (years)", 'g2_lookback_years', 1, 10, 1)
        trow(s, "Grant-to-net-assets ratio — HIGH threshold", 'g2_dependency_high', 0.5, 10.0, 0.5)
        trow(s, "Grant-to-net-assets ratio — MEDIUM threshold", 'g2_dependency_medium', 0.1, 5.0, 0.1)
        trow(s, "Grant-to-revenue ratio — escalate to MEDIUM if above", 'g2_revenue_ratio', 0.1, 2.0, 0.1)

        s = rule_section(tab3, "Grant Management Experience  (IGM mode only)",
            "Compares the proposed award to the largest grant the organisation has previously "
            "received, as a proxy for their experience managing grants at this scale. Only "
            "applied when the Intermediate Grant Maker option is selected.")
        trow(s, "Award above historical maximum — HIGH threshold (%)", 'g3_scale_high_pct',
             50, 500, 10, note="e.g. 100 = more than double the largest previous grant")
        trow(s, "Award above historical maximum — MEDIUM threshold (%)", 'g3_scale_medium_pct',
             10, 200, 10)

        s = rule_section(tab3, "Composite Warning",
            "When several checks each return a HIGH result, an additional summary warning is "
            "added to the report to highlight the combined risk profile.")
        trow(s, "Number of HIGH results to trigger composite warning", 'composite_high_count', 2, 10, 1)

        # ── Bottom buttons ───────────────────────────────────────────────────
        btn_frame = ttk.Frame(win)
        btn_frame.pack(fill='x', padx=10, pady=8)

        _DEFAULTS = {
            'solvency_decline_pct': 30, 'current_ratio_min': 1.0,
            'current_ratio_critical': 0.5, 'quick_ratio_min': 0.5,
            'cash_pct_min': 10, 'debt_to_equity_max': 2.0,
            'revenue_decline_pct': -10, 'revenue_decline_years': 2,
            'consecutive_loss_years': 2, 'predictive_profit_decline_pct': 20,
            'predictive_revenue_decline_pct': 15, 'late_filings_count': 2,
            'late_filings_period': 5, 'director_churn_count': 3,
            'director_churn_months': 12, 'insolvency_company_count': 3,
            'insolvency_critical_count': 5, 'phoenix_similarity_pct': 80,
            'phoenix_officer_count': 5, 'g1_cash_buffer_pct': 0.25,
            'g1_nca_comfortable_pct': 0.5, 'g2_lookback_years': 3,
            'g2_dependency_high': 2.0, 'g2_dependency_medium': 1.0,
            'g2_revenue_ratio': 0.5, 'g3_scale_high_pct': 100.0,
            'g3_scale_medium_pct': 50.0, 'f1_erosion_high_years': 3,
            'f1_erosion_medium_years': 2, 'f2_intangible_bloat_pct': 0.5,
            'f3_nca_drop_pct': 0.25, 'f4_leverage_years': 3,
            'composite_high_count': 3, 'roe_negative_years_medium': 2,
            'roe_negative_years_high': 3, 'asset_turnover_decline_years': 2,
            'asset_turnover_min': 0.3, 'profit_margin_negative_years_medium': 2,
            'profit_margin_negative_years_high': 3, 'profit_margin_compression_pts': 10.0,
            'staff_cost_ratio_max': 0.75, 'staff_cost_ratio_critical': 0.90,
        }

        def _reset():
            for k, v in _DEFAULTS.items():
                if k in local_vars:
                    local_vars[k].set(v)

        def _apply():
            for key, var in local_vars.items():
                try:
                    self.thresholds[key] = var.get()
                except Exception:
                    pass
            win.destroy()

        ttk.Button(btn_frame, text="Reset to Defaults", command=_reset).pack(side='left')
        ttk.Button(btn_frame, text="Cancel", command=win.destroy).pack(side='right', padx=(4, 0))
        ttk.Button(btn_frame, text="Apply & Close", command=_apply,
                   bootstyle='success').pack(side='right')
    
    def _build_manual_input_form(self):
        """Build the manual input form for grant details and supplementary accounts data."""
        self.show_manual_input = tk.BooleanVar(value=False)
        manual_toggle = ttk.Checkbutton(
            self.content_frame,
            text="\u25B6 Grant Details & Supplementary Accounts Data (Optional)",
            variable=self.show_manual_input,
            command=self._toggle_manual_input,
        )
        manual_toggle.pack(anchor='w', padx=10, pady=(5, 0))
        self._manual_toggle_widget = manual_toggle

        self.manual_input_frame = ttk.Frame(self.content_frame)
        # Will be packed/unpacked by toggle

        # --- Proposed Grant Details ---
        grant_frame = ttk.LabelFrame(
            self.manual_input_frame, text="Proposed Grant Details", padding=5
        )
        grant_frame.pack(fill=tk.X, pady=2, padx=5)

        row = ttk.Frame(grant_frame)
        row.pack(fill=tk.X, pady=2)
        ttk.Label(row, text="Proposed Award Amount (\u00a3):", width=30).pack(side=tk.LEFT)
        self.proposed_award_var = tk.StringVar()
        award_entry = ttk.Entry(row, textvariable=self.proposed_award_var, width=15)
        award_entry.pack(side=tk.LEFT)

        def _validate_award(*_args):
            raw = self.proposed_award_var.get().strip().replace(',', '').replace('\u00a3', '')
            if not raw:
                try:
                    award_entry.configure(bootstyle='default')
                except Exception:
                    pass
                return
            try:
                float(raw)
                award_entry.configure(bootstyle='default')
            except (ValueError, AttributeError):
                try:
                    award_entry.configure(bootstyle='danger')
                except Exception:
                    pass

        self.proposed_award_var.trace_add('write', _validate_award)

        row = ttk.Frame(grant_frame)
        row.pack(fill=tk.X, pady=2)
        ttk.Label(row, text="Payment Mechanism:", width=30).pack(side=tk.LEFT)
        self.payment_mechanism_var = tk.StringVar(value='Unknown')
        ttk.Combobox(
            row, textvariable=self.payment_mechanism_var,
            values=PAYMENT_MECHANISMS, state='readonly', width=18
        ).pack(side=tk.LEFT)

        # --- Supplementary Accounts Data (opens separate window) ---
        supp_frame = ttk.LabelFrame(
            self.manual_input_frame, text="Supplementary Accounts Data", padding=5
        )
        supp_frame.pack(fill=tk.X, pady=2, padx=5)

        ttk.Label(
            supp_frame,
            text="Enter Balance Sheet and Income Statement figures in a layout that mirrors "
                 "standard UK filed accounts.  If iXBRL accounts have been uploaded, fields "
                 "will be auto-populated — you can then review and amend.",
            foreground='grey', wraplength=600, justify=tk.LEFT,
        ).pack(anchor='w', pady=(0, 5))

        btn_row = ttk.Frame(supp_frame)
        btn_row.pack(fill=tk.X, pady=(0, 5))
        ttk.Button(
            btn_row, text="Open Supplementary Accounts\u2026",
            command=self._open_supplementary_accounts_window,
        ).pack(side=tk.LEFT, padx=(0, 10))
        self._supp_status_label = ttk.Label(btn_row, text="No data entered.", foreground='grey')
        self._supp_status_label.pack(side=tk.LEFT)

        # Persistent storage shared with the window and _collect_manual_input
        self._manual_year_panels = []

    # ------------------------------------------------------------------
    # Supplementary Accounts Window
    # ------------------------------------------------------------------

    def _open_supplementary_accounts_window(self):
        """Open a Toplevel window with Balance Sheet and Income Statement tabs."""
        is_charity = self._entity_type == 'charity'

        win = tk.Toplevel(self.app)
        win.title("Supplementary Accounts Data")
        win.geometry("1050x620")
        win.minsize(700, 500)
        win.transient(self.app)
        win.grab_set()

        # Select field definitions based on entity type
        if is_charity:
            bs_fields = CHARITY_BALANCE_SHEET_FIELDS
            is_fields = CHARITY_INCOME_STATEMENT_FIELDS
            is_tab_label = "Statement of Financial Activities"
        else:
            bs_fields = BALANCE_SHEET_FIELDS
            is_fields = INCOME_STATEMENT_FIELDS
            is_tab_label = "Income Statement"

        # --- shared year columns (max 5) ---
        year_columns = []   # list of {'period_end': StringVar, 'vars': {key: StringVar}}

        # --- validation tracking ---
        validation_entries = []  # (entry_widget, StringVar, field_label)

        def _validate_currency(var, entry_widget, _allow_empty=True):
            raw = var.get().strip().replace(',', '').replace('\u00a3', '')
            if not raw and _allow_empty:
                try:
                    entry_widget.configure(bootstyle='default')
                except Exception:
                    pass
                return True
            try:
                float(raw)
                try:
                    entry_widget.configure(bootstyle='default')
                except Exception:
                    pass
                return True
            except (ValueError, AttributeError):
                try:
                    entry_widget.configure(bootstyle='danger')
                except Exception:
                    pass
                return False

        # --- notebook ---
        notebook = ttk.Notebook(win)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=(10, 0))

        bs_outer = ttk.Frame(notebook)
        is_outer = ttk.Frame(notebook)
        notebook.add(bs_outer, text="Balance Sheet")
        notebook.add(is_outer, text=is_tab_label)

        # Canvas + scrollbar per tab (with horizontal scrollbar for wide layouts)
        tab_grids = {}
        canvases = {}
        for tab_name, outer in [('bs', bs_outer), ('is', is_outer)]:
            canvas = tk.Canvas(outer, highlightthickness=0)
            v_scroll = ttk.Scrollbar(outer, orient='vertical', command=canvas.yview)
            h_scroll = ttk.Scrollbar(outer, orient='horizontal', command=canvas.xview)
            inner = ttk.Frame(canvas)
            inner.bind('<Configure>',
                       lambda e, c=canvas: c.configure(scrollregion=c.bbox('all')))
            canvas.create_window((0, 0), window=inner, anchor='nw')
            canvas.configure(yscrollcommand=v_scroll.set, xscrollcommand=h_scroll.set)
            h_scroll.pack(side=tk.BOTTOM, fill=tk.X)
            v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
            canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            canvases[tab_name] = canvas
            tab_grids[tab_name] = inner

        # Per-canvas mousewheel scrolling using enter/leave pattern
        # (avoids TclError from global bind_all hitting destroyed canvases)
        def _bind_mousewheel(target_canvas):
            def _on_mousewheel(event):
                try:
                    target_canvas.yview_scroll(
                        int(-1 * (event.delta / 120)) if event.delta
                        else (-1 if event.num == 4 else 1),
                        'units',
                    )
                except tk.TclError:
                    pass  # canvas already destroyed

            def _on_enter(_event):
                target_canvas.bind_all('<MouseWheel>', _on_mousewheel)
                target_canvas.bind_all('<Button-4>', _on_mousewheel)
                target_canvas.bind_all('<Button-5>', _on_mousewheel)

            def _on_leave(_event):
                target_canvas.unbind_all('<MouseWheel>')
                target_canvas.unbind_all('<Button-4>')
                target_canvas.unbind_all('<Button-5>')

            target_canvas.bind('<Enter>', _on_enter)
            target_canvas.bind('<Leave>', _on_leave)

        for _c in canvases.values():
            _bind_mousewheel(_c)

        # Clean up global bindings when window is closed
        def _on_window_close():
            for evt in ('<MouseWheel>', '<Button-4>', '<Button-5>'):
                try:
                    win.unbind_all(evt)
                except Exception:
                    pass
            win.grab_release()
            win.destroy()

        win.protocol('WM_DELETE_WINDOW', _on_window_close)

        def _rebuild_grids():
            """Rebuild both tab grids to reflect the current year_columns."""
            validation_entries.clear()
            for tab_name, fields in [('bs', bs_fields), ('is', is_fields)]:
                grid = tab_grids[tab_name]
                for w in grid.winfo_children():
                    w.destroy()

                n_years = len(year_columns)

                # Header row
                ttk.Label(grid, text="Period End (YYYY-MM-DD)", foreground='grey',
                          font=('TkDefaultFont', 8)).grid(
                    row=0, column=0, sticky='e', padx=(5, 10), pady=2)
                for ci, ycol in enumerate(year_columns):
                    pe_entry = ttk.Entry(grid, textvariable=ycol['period_end'],
                                         width=14, justify='center')
                    pe_entry.grid(row=0, column=ci + 1, padx=3, pady=2)

                grid_row = 1
                for field_key, auto_col, label in fields:
                    # Section header
                    if field_key is None:
                        if label:
                            lbl = ttk.Label(grid, text=label,
                                            font=('TkDefaultFont', 9, 'bold'))
                            lbl.grid(row=grid_row, column=0, columnspan=n_years + 1,
                                     sticky='w', padx=5, pady=(8, 2))
                        grid_row += 1
                        continue

                    # Field row
                    display = label
                    if any(kw in label for kw in ('Profit', 'Loss', 'Net Current', 'Net Assets')):
                        display += "  (neg. for loss)"
                    is_currency = field_key != 'Employees'
                    suffix = " (\u00a3)" if is_currency else ""

                    ttk.Label(grid, text=f"{display}{suffix}").grid(
                        row=grid_row, column=0, sticky='e', padx=(5, 10), pady=1)

                    for ci, ycol in enumerate(year_columns):
                        var = ycol['vars'].setdefault(field_key, tk.StringVar())
                        entry = ttk.Entry(grid, textvariable=var, width=14, justify='right')
                        entry.grid(row=grid_row, column=ci + 1, padx=3, pady=1)
                        if is_currency:
                            validation_entries.append((entry, var, label))
                            var.trace_add(
                                'write',
                                lambda *_a, v=var, e=entry: _validate_currency(v, e),
                            )
                    grid_row += 1

        def _add_year_column():
            if len(year_columns) >= 5:
                return
            year_columns.append({'period_end': tk.StringVar(), 'vars': {}})
            _rebuild_grids()
            _update_year_label()

        def _remove_last_year():
            if year_columns:
                year_columns.pop()
                _rebuild_grids()
                _update_year_label()

        def _clear_all():
            year_columns.clear()
            _add_year_column()

        def _update_year_label():
            n = len(year_columns)
            year_count_label.config(text=f"{n} year{'s' if n != 1 else ''}")

        def _cc_extract_year(entry):
            """Extract fiscal year from a CC API entry."""
            for key in ('financial_period_end_date', 'fin_period_end_date', 'ar_cycle_reference'):
                val = entry.get(key)
                if val:
                    try:
                        if isinstance(val, str) and len(val) >= 4:
                            return int(val[:4])
                        elif isinstance(val, (int, float)):
                            return int(val)
                    except (ValueError, TypeError):
                        continue
            return None

        def _safe_f(val):
            """Convert to float or None."""
            if val is None:
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        def _set_if(ycol, field_key, val):
            """Set a year column var if the value is not None."""
            fval = _safe_f(val)
            if fval is not None:
                formatted = f"{fval:,.0f}" if fval == int(fval) else f"{fval:,.2f}"
                ycol['vars'][field_key] = tk.StringVar(value=formatted)

        def _auto_populate():
            """Populate fields from iXBRL data (company) or CC API data (charity)."""
            if is_charity:
                _auto_populate_charity()
            else:
                _auto_populate_company()

        def _auto_populate_company():
            """Populate fields from iXBRL data if available."""
            if not self.accounts_loaded or not self.financial_analyzer:
                messagebox.showinfo(
                    "No iXBRL Data",
                    "Upload iXBRL accounts files first, then re-open this window.",
                    parent=win,
                )
                return

            df = self.financial_analyzer.data
            if df.empty:
                return

            years = sorted(df['Year'].unique())
            year_columns.clear()
            for yr in years[:5]:
                year_columns.append({
                    'period_end': tk.StringVar(value=f"{int(yr)}-12-31"),
                    'vars': {},
                })

            all_fields = bs_fields + is_fields
            for field_key, auto_col, _ in all_fields:
                if field_key is None:
                    continue
                col_name = auto_col if auto_col else field_key
                if col_name not in df.columns:
                    continue
                for i, yr in enumerate(years[:5]):
                    rows = df[df['Year'] == yr]
                    if rows.empty:
                        continue
                    val = rows.iloc[0].get(col_name)
                    if pd.notna(val):
                        fval = float(val)
                        formatted = f"{fval:,.0f}" if fval == int(fval) else f"{fval:,.2f}"
                        year_columns[i]['vars'][field_key] = tk.StringVar(value=formatted)

            _rebuild_grids()
            _update_year_label()

        def _auto_populate_charity():
            """Populate fields from Charity Commission API data."""
            fin_history = self.charity_data.get('financial_history') or []
            assets_liab = self.charity_data.get('assets_liabilities') or []
            overview = self.charity_data.get('overview') or {}

            if not fin_history and not assets_liab:
                messagebox.showinfo(
                    "No Charity Data",
                    "Fetch charity data first, then re-open this window.",
                    parent=win,
                )
                return

            # Sort financial history by period end date
            sorted_fin = sorted(
                fin_history,
                key=lambda x: x.get('financial_period_end_date', '') or x.get('fin_period_end_date', '') or '',
            )
            # Index assets/liabilities by year
            al_by_year = {}
            for entry in (assets_liab if isinstance(assets_liab, list) else [assets_liab] if assets_liab else []):
                yr_key = _cc_extract_year(entry)
                if yr_key is not None:
                    al_by_year[yr_key] = entry

            # Determine years
            years = []
            for entry in sorted_fin:
                yr = _cc_extract_year(entry)
                if yr is not None and yr not in years:
                    years.append(yr)
            for yr in sorted(al_by_year.keys()):
                if yr not in years:
                    years.append(yr)
            years = sorted(years)[:5]

            year_columns.clear()
            # Index financial data by year
            fin_by_year = {}
            for entry in sorted_fin:
                yr = _cc_extract_year(entry)
                if yr is not None:
                    fin_by_year[yr] = entry

            for yr in years:
                pe_date = ''
                fin = fin_by_year.get(yr, {})
                pe_raw = fin.get('financial_period_end_date') or fin.get('fin_period_end_date', '')
                if pe_raw and len(str(pe_raw)) >= 10:
                    pe_date = str(pe_raw)[:10]
                elif yr:
                    pe_date = f"{yr}-03-31"  # Default to fiscal year end

                ycol = {'period_end': tk.StringVar(value=pe_date), 'vars': {}}

                al = al_by_year.get(yr, {})

                # Populate balance sheet fields from assets/liabilities
                _set_if(ycol, 'TangibleAssets', al.get('assets_own_use'))
                _set_if(ycol, 'LongTermInvestments', al.get('assets_long_term_investment'))
                _set_if(ycol, 'CurrentAssets', al.get('assets_other_assets'))
                _set_if(ycol, 'TotalLiabilities', al.get('assets_total_liabilities'))
                _set_if(ycol, 'PensionAssets', al.get('defined_net_assets_pension'))

                # Derive net assets
                own = _safe_f(al.get('assets_own_use')) or 0
                invest = _safe_f(al.get('assets_long_term_investment')) or 0
                pension = _safe_f(al.get('defined_net_assets_pension')) or 0
                other = _safe_f(al.get('assets_other_assets')) or 0
                liab = _safe_f(al.get('assets_total_liabilities')) or 0
                if any(al.get(k) is not None for k in
                       ('assets_own_use', 'assets_long_term_investment',
                        'assets_other_assets', 'assets_total_liabilities')):
                    net = own + invest + pension + other - liab
                    _set_if(ycol, 'NetAssets', net)
                    _set_if(ycol, 'TotalCharityFunds', net)

                # Employees from overview (single value, apply to most recent year)
                if yr == years[-1] and overview.get('employees') is not None:
                    _set_if(ycol, 'Employees', overview.get('employees'))

                # Populate income statement fields from financial history
                _set_if(ycol, 'TotalIncome', fin.get('inc_total'))
                _set_if(ycol, 'IncCharitableActivities', fin.get('inc_charitable_activities'))
                _set_if(ycol, 'IncDonationsLegacies', fin.get('inc_donations_and_legacies'))

                # Combined: Other Trading + Investments
                trading = _safe_f(fin.get('inc_other_trading_activities')) or 0
                investment = _safe_f(fin.get('inc_investment')) or 0
                if fin.get('inc_other_trading_activities') is not None or fin.get('inc_investment') is not None:
                    _set_if(ycol, 'IncTradingInvestment', trading + investment)

                _set_if(ycol, 'TotalExpenditure', fin.get('exp_total'))
                _set_if(ycol, 'ExpCharitableActivities', fin.get('exp_charitable_activities'))
                _set_if(ycol, 'ExpFundraising', fin.get('exp_raising_funds'))

                # Combined: Governance + Other
                governance = _safe_f(fin.get('exp_governance')) or 0
                exp_other = _safe_f(fin.get('exp_other')) or 0
                if fin.get('exp_governance') is not None or fin.get('exp_other') is not None:
                    _set_if(ycol, 'ExpGovernanceOther', governance + exp_other)

                # Net income
                inc = _safe_f(fin.get('inc_total'))
                exp = _safe_f(fin.get('exp_total'))
                if inc is not None and exp is not None:
                    _set_if(ycol, 'NetIncome', inc - exp)

                year_columns.append(ycol)

            _rebuild_grids()
            _update_year_label()

        def _save_and_close():
            """Validate, persist data to self._manual_year_panels, close."""
            invalid = []
            for entry_w, var, label in validation_entries:
                if not _validate_currency(var, entry_w):
                    invalid.append(label)
            if invalid:
                messagebox.showerror(
                    "Invalid Input",
                    "The following fields contain non-numeric values:\n\n"
                    + "\n".join(f"  - {f}" for f in invalid[:10]),
                    parent=win,
                )
                return

            # --- Sum validation for charity supplementary accounts ---
            if is_charity:
                discrepancies = []
                for ycol in year_columns:
                    pe = ycol['period_end'].get() or '?'
                    vd = ycol['vars']

                    def _val(key):
                        v = vd.get(key)
                        if v is None:
                            return None
                        txt = v.get().strip().replace(',', '') if isinstance(v, tk.StringVar) else ''
                        if not txt:
                            return None
                        try:
                            return float(txt)
                        except (ValueError, TypeError):
                            return None

                    # Income check
                    inc_parts = [_val('IncCharitableActivities'),
                                 _val('IncDonationsLegacies'),
                                 _val('IncTradingInvestment')]
                    inc_total = _val('TotalIncome')
                    if inc_total is not None and any(p is not None for p in inc_parts):
                        inc_sum = sum(p for p in inc_parts if p is not None)
                        if abs(inc_sum - inc_total) > 1:
                            discrepancies.append(
                                f"  {pe}: Income constituents sum to "
                                f"\u00a3{inc_sum:,.0f} but Total Income is "
                                f"\u00a3{inc_total:,.0f}")

                    # Expenditure check
                    exp_parts = [_val('ExpCharitableActivities'),
                                 _val('ExpFundraising'),
                                 _val('ExpGovernanceOther')]
                    exp_total = _val('TotalExpenditure')
                    if exp_total is not None and any(p is not None for p in exp_parts):
                        exp_sum = sum(p for p in exp_parts if p is not None)
                        if abs(exp_sum - exp_total) > 1:
                            discrepancies.append(
                                f"  {pe}: Expenditure constituents sum to "
                                f"\u00a3{exp_sum:,.0f} but Total Expenditure is "
                                f"\u00a3{exp_total:,.0f}")

                    # Net assets check
                    tangible = _val('TangibleAssets')
                    lt_inv = _val('LongTermInvestments')
                    cur_assets = _val('CurrentAssets')
                    liabilities = _val('TotalLiabilities')
                    pension = _val('PensionAssets')
                    net_assets = _val('NetAssets')
                    asset_parts = [tangible, lt_inv, cur_assets, pension]
                    if net_assets is not None and any(p is not None for p in asset_parts + [liabilities]):
                        asset_sum = sum(p for p in asset_parts if p is not None)
                        liab_val = liabilities if liabilities is not None else 0
                        calc_net = asset_sum - liab_val
                        if abs(calc_net - net_assets) > 1:
                            discrepancies.append(
                                f"  {pe}: Assets minus liabilities = "
                                f"\u00a3{calc_net:,.0f} but Net Assets is "
                                f"\u00a3{net_assets:,.0f}")

                if discrepancies:
                    msg = ("The following totals don't match their constituent "
                           "values:\n\n" + "\n".join(discrepancies) +
                           "\n\nThis may be due to rounding or other income/"
                           "expenditure categories not captured here.\n\n"
                           "Save anyway?")
                    if not messagebox.askokcancel("Sum Mismatch", msg,
                                                 parent=win):
                        return

            self._manual_year_panels = []
            filled_count = 0
            for ycol in year_columns:
                vars_dict = {}
                vars_dict['_period_end'] = tk.StringVar(value=ycol['period_end'].get())
                has_data = False
                for key, var in ycol['vars'].items():
                    vars_dict[key] = tk.StringVar(value=var.get())
                    if var.get().strip():
                        has_data = True
                if has_data:
                    filled_count += 1
                self._manual_year_panels.append({'frame': None, 'vars': vars_dict})

            if filled_count > 0:
                self._supp_status_label.config(
                    text=f"{filled_count} year{'s' if filled_count != 1 else ''} of data entered.",
                    foreground='green',
                )
            else:
                self._supp_status_label.config(text="No data entered.", foreground='grey')

            _on_window_close()

        # --- Bottom button bar ---
        btn_bar = ttk.Frame(win)
        btn_bar.pack(fill=tk.X, padx=10, pady=10)

        ttk.Button(btn_bar, text="Add Year", command=_add_year_column).pack(
            side=tk.LEFT, padx=(0, 5))
        ttk.Button(btn_bar, text="Remove Last Year", command=_remove_last_year).pack(
            side=tk.LEFT, padx=(0, 5))
        ttk.Button(btn_bar, text="Clear All", command=_clear_all).pack(
            side=tk.LEFT, padx=(0, 15))

        if is_charity and self.charity_data:
            ttk.Button(btn_bar, text="Auto-populate from CC API",
                       command=_auto_populate).pack(side=tk.LEFT, padx=(0, 5))
        elif not is_charity and self.accounts_loaded:
            ttk.Button(btn_bar, text="Auto-populate from iXBRL",
                       command=_auto_populate).pack(side=tk.LEFT, padx=(0, 5))

        year_count_label = ttk.Label(btn_bar, text="")
        year_count_label.pack(side=tk.LEFT, padx=10)

        ttk.Button(btn_bar, text="Save & Close", command=_save_and_close,
                   bootstyle='success').pack(side=tk.RIGHT)

        # --- Seed initial data ---
        if self._manual_year_panels:
            for panel in self._manual_year_panels:
                pv = panel['vars']
                pe_val = ''
                pe = pv.get('_period_end')
                if isinstance(pe, tk.StringVar):
                    pe_val = pe.get()
                elif isinstance(pe, str):
                    pe_val = pe
                ycol = {'period_end': tk.StringVar(value=pe_val), 'vars': {}}
                for key, var in pv.items():
                    if key.startswith('_'):
                        continue
                    val = var.get() if isinstance(var, tk.StringVar) else str(var)
                    ycol['vars'][key] = tk.StringVar(value=val)
                year_columns.append(ycol)
        else:
            year_columns.append({'period_end': tk.StringVar(), 'vars': {}})

        _rebuild_grids()
        _update_year_label()

        # Auto-populate if data available and no manual data yet
        has_data = any(
            any(v.get().strip() for v in yc['vars'].values()) for yc in year_columns
        )
        if not has_data:
            if is_charity and self.charity_data:
                _auto_populate()
            elif not is_charity and self.accounts_loaded:
                _auto_populate()

    def _toggle_manual_input(self):
        """Show/hide the manual input form."""
        if self.show_manual_input.get():
            self.manual_input_frame.pack(fill=tk.X, pady=5, padx=10,
                                         before=self._get_config_frame())
            self._manual_toggle_widget.config(
                text="\u25BC Grant Details & Supplementary Accounts Data (Optional)"
            )
        else:
            self.manual_input_frame.pack_forget()
            self._manual_toggle_widget.config(
                text="\u25B6 Grant Details & Supplementary Accounts Data (Optional)"
            )

    def _get_config_frame(self):
        """Return the Step 3 config frame widget for insertion ordering."""
        for widget in self.content_frame.winfo_children():
            if isinstance(widget, ttk.LabelFrame) and "Configure Analysis" in str(widget.cget('text')):
                return widget
        return None

    def _collect_manual_input(self):
        """Collect multi-year manual input into a list of year dicts. Call on main thread.

        Returns a list of dicts, each with 'year' (int extracted from period end)
        and metric keys with float values.  Empty panels are skipped.
        """
        years_data = []
        for panel in self._manual_year_panels:
            vars_dict = panel['vars']
            period_raw = vars_dict['_period_end'].get().strip()

            # Extract year from period end date
            year = None
            if period_raw:
                try:
                    year = datetime.strptime(period_raw[:10], '%Y-%m-%d').year
                except ValueError:
                    # Try plain year
                    if period_raw.isdigit() and len(period_raw) == 4:
                        year = int(period_raw)

            year_data = {}
            for key, var in vars_dict.items():
                if key.startswith('_'):
                    continue
                raw = var.get().strip().replace(',', '').replace('\u00a3', '')
                if raw:
                    try:
                        year_data[key] = float(raw)
                    except ValueError:
                        log_message(f"Invalid manual input for {key}: {raw}")

            if year_data and year is not None:
                year_data['_year'] = year
                years_data.append(year_data)

        return years_data

    def _on_entity_type_changed(self):
        """Update UI when entity type radio button changes."""
        is_charity = self.entity_type_var.get() == 'charity'
        self._entity_type = 'charity' if is_charity else 'company'

        # Update Step 1 labels
        if is_charity:
            self._lookup_frame.config(text="Step 1: Enter Charity Registration Number")
            self._input_label.config(text="Registration Number:")
            self.fetch_btn.config(text="Fetch Charity Data")
        else:
            self._lookup_frame.config(text="Step 1: Enter Company Number")
            self._input_label.config(text="Company Number:")
            self.fetch_btn.config(text="Fetch Company Data")

        # Toggle Step 2: upload vs charity note
        # The manual input toggle (grant details + supplementary accounts) stays
        # visible for both modes — only the iXBRL upload frame is swapped.
        if is_charity:
            self._upload_frame.pack_forget()
            self._charity_accounts_note.pack(fill=tk.X, pady=5, padx=10,
                                             after=self._lookup_frame)
            # Re-place the manual toggle after the charity note
            if hasattr(self, '_manual_toggle_widget'):
                self._manual_toggle_widget.pack_forget()
                self.manual_input_frame.pack_forget()
                self._manual_toggle_widget.pack(anchor='w', padx=10, pady=(5, 0),
                                                after=self._charity_accounts_note)
        else:
            self._charity_accounts_note.pack_forget()
            self._upload_frame.pack(fill=tk.X, pady=5, padx=10,
                                    after=self._lookup_frame)
            if hasattr(self, '_manual_toggle_widget'):
                self._manual_toggle_widget.pack_forget()
                self.manual_input_frame.pack_forget()
                self._manual_toggle_widget.pack(anchor='w', padx=10, pady=(5, 0),
                                                after=self._upload_frame)

        # Update Step 3 checkbox labels
        if 'ownership_graph' in self.check_widgets:
            text = ("Linked charities & subsidiary structure"
                    if is_charity
                    else "Corporate ownership structure graph (makes additional API calls per corporate PSC)")
            self.check_widgets['ownership_graph'].config(text=text)

        if 'director_history' in self.check_widgets:
            text = ("Deep investigation — trustee cross-charity analysis & linked charity check (slow)"
                    if is_charity
                    else "Deep investigation — director insolvency history & phoenix check (slow)")
            self.check_widgets['director_history'].config(text=text)

        # Clear previous data and reset
        self.company_data = {}
        self.charity_data = {}
        self._manual_year_panels = []
        if hasattr(self, '_supp_status_label'):
            self._supp_status_label.config(text="No data entered.", foreground='grey')
        self.company_summary_text.config(state='normal')
        self.company_summary_text.delete('1.0', tk.END)
        self.company_summary_text.config(state='disabled')
        self.generate_btn.config(state='disabled')
        self.status_var.set(f"Ready. Enter a {'charity registration number' if is_charity else 'company number'} to begin.")

    def fetch_entity_profile(self):
        """Dispatch to company or charity fetch based on entity type."""
        if self._entity_type == 'charity':
            self._fetch_charity_profile()
        else:
            self.fetch_company_profile()

    def _fetch_charity_profile(self):
        """Validate and start charity data fetch."""
        reg_num = self.company_num_var.get().strip()
        if not reg_num:
            messagebox.showerror("Input Error", "Please enter a charity registration number.")
            return

        # Validate: numeric only, 1-7 digits
        if not reg_num.isdigit() or len(reg_num) > 7:
            messagebox.showerror(
                "Input Error",
                "Charity registration number must be numeric (1-7 digits)."
            )
            return

        if not self.charity_api_key:
            messagebox.showerror(
                "API Key Missing",
                "A Charity Commission API key is required. "
                "Configure it via File → Manage API Keys."
            )
            return

        self.fetch_btn.config(state='disabled')
        self.status_var.set(f"Fetching charity data for {reg_num}...")
        threading.Thread(
            target=self._fetch_charity_thread, args=(reg_num,), daemon=True
        ).start()

    def _fetch_charity_thread(self, reg_num):
        """Background thread to fetch all charity data from CC API."""
        try:
            api_key = self.charity_api_key

            # Core profile
            details, error = cc_get_charity_details_v2(api_key, reg_num)
            if error or not details:
                raise ValueError(
                    f"Could not fetch charity {reg_num}. "
                    f"Please check the registration number. ({error})"
                )

            # Financial history (5-year income/expenditure)
            self.safe_update(self.status_var.set, "Fetching financial history...")
            fin_history, _ = cc_get_financial_history(api_key, reg_num)

            # Assets & liabilities
            self.safe_update(self.status_var.set, "Fetching assets & liabilities...")
            assets_liab, _ = cc_get_assets_liabilities(api_key, reg_num)

            # Overview (annual return data)
            self.safe_update(self.status_var.set, "Fetching overview data...")
            overview, _ = cc_get_overview(api_key, reg_num)

            # Accounts submission info
            account_ar, _ = cc_get_account_ar_info(api_key, reg_num)

            # Governing document
            gov_doc, _ = cc_get_governing_document(api_key, reg_num)

            # Registration history
            reg_history, _ = cc_get_registration_history(api_key, reg_num)

            self.charity_data = {
                'details': details,
                'financial_history': fin_history,
                'assets_liabilities': assets_liab,
                'overview': overview or {},
                'account_ar_info': account_ar,
                'governing_document': gov_doc or {},
                'registration_history': reg_history,
            }

            self.after(100, self._display_charity_summary)
            self.safe_update(self.status_var.set, "Charity data loaded successfully.")
            self.safe_update(self.generate_btn.config, {'state': 'normal'})

        except Exception as e:
            log_message(f"Error fetching charity data: {e}")
            self.safe_update(messagebox.showerror, "Error", str(e))
            self.safe_update(self.status_var.set, "Error fetching charity data.")
        finally:
            self.safe_update(self.fetch_btn.config, {'state': 'normal'})

    def _display_charity_summary(self):
        """Display basic charity info in the summary box."""
        details = self.charity_data.get('details', {})
        overview = self.charity_data.get('overview', {})

        status_map = {'R': 'Registered', 'RM': 'Removed'}
        status = status_map.get(details.get('reg_status', ''), details.get('reg_status', 'N/A'))

        summary = f"Charity Name: {details.get('charity_name', 'N/A')}\n"
        summary += f"Registration Number: {details.get('reg_charity_number', 'N/A')}\n"
        summary += f"Status: {status}\n"
        summary += f"Registered: {format_display_date(details.get('date_of_registration', ''))}\n"

        addr_parts = []
        for i in range(1, 6):
            part = details.get(f'address_line_{i}') or details.get(f'address_line{i}')
            if part:
                addr_parts.append(str(part).strip())
        postcode = details.get('address_post_code', '')
        if postcode:
            addr_parts.append(str(postcode).strip())
        summary += f"Address: {', '.join(addr_parts) if addr_parts else 'N/A'}\n"

        trustees = overview.get('trustees', 'N/A')
        summary += f"Active Trustees: {trustees}"

        self.company_summary_text.config(state='normal')
        self.company_summary_text.delete('1.0', tk.END)
        self.company_summary_text.insert('1.0', summary)
        self.company_summary_text.config(state='disabled')

    def fetch_company_profile(self):
        """Fetch comprehensive company data from API."""
        cnum_raw = self.company_num_var.get().strip()
        if not cnum_raw:
            messagebox.showerror("Input Error", "Please enter a company number.")
            return

        cnum = clean_company_number(cnum_raw)

        self.fetch_btn.config(state='disabled')
        self.status_var.set(f"Fetching data for {cnum}...")

        threading.Thread(target=self._fetch_company_thread, args=(cnum,), daemon=True).start()
    
    def _fetch_company_thread(self, cnum):
        """Background thread to fetch all company data."""
        try:
            # Fetch profile
            profile, error = ch_get_data(self.api_key, self.ch_token_bucket, f"/company/{cnum}")
            if error or not profile:
                raise ValueError(f"Could not fetch company {cnum}. Please check the number.")
            
            # Fetch officers
            officers, _ = ch_get_data(
                self.api_key, self.ch_token_bucket, f"/company/{cnum}/officers?items_per_page=100"
            )
            
            # Fetch PSCs
            pscs, _ = ch_get_data(
                self.api_key, self.ch_token_bucket,
                f"/company/{cnum}/persons-with-significant-control?items_per_page=100"
            )
            
            # Fetch filing history
            filing_history, _ = ch_get_data(
                self.api_key, self.ch_token_bucket, f"/company/{cnum}/filing-history?items_per_page=100"
            )
            
            self.company_data = {
                'profile': profile,
                'officers': officers,
                'pscs': pscs,
                'filing_history': filing_history,
            }
            
            self.after(100, self._display_company_summary)
            self.safe_update(self.status_var.set, "Company data loaded successfully.")
            self.safe_update(self.generate_btn.config, {'state': 'normal'})
            self.safe_update(self._update_accounts_checkboxes)
            
        except Exception as e:
            log_message(f"Error fetching company data: {e}")
            self.safe_update(messagebox.showerror, "Error", str(e))
            self.safe_update(self.status_var.set, "Error fetching company data.")
        finally:
            self.safe_update(self.fetch_btn.config, {'state': 'normal'})
    
    def _display_company_summary(self):
        """Display basic company info in the summary box."""
        profile = self.company_data['profile']
        
        summary = f"Company Name: {profile.get('company_name', 'N/A')}\n"
        summary += f"Company Number: {profile.get('company_number', 'N/A')}\n"
        summary += f"Status: {profile.get('company_status', 'N/A')}\n"
        summary += f"Incorporated: {format_display_date(profile.get('date_of_creation', ''))}\n"
        
        addr = profile.get('registered_office_address', {})
        addr_str = ", ".join(filter(None, [
            addr.get('address_line_1'),
            addr.get('locality'),
            addr.get('postal_code')
        ]))
        summary += f"Address: {addr_str}\n"
        
        officers = self.company_data.get('officers', {})
        active_officers = len([o for o in officers.get('items', []) if not o.get('resigned_on')])
        summary += f"Active Officers: {active_officers}"
        
        self.company_summary_text.config(state='normal')
        self.company_summary_text.delete('1.0', tk.END)
        self.company_summary_text.insert('1.0', summary)
        self.company_summary_text.config(state='disabled')
    
    def _update_accounts_checkboxes(self):
        """Grey out accounts-dependent checkboxes if no accounts loaded."""
        accounts_dependent = ['solvency', 'liquidity', 'revenue_trends', 'predictive_outlook']
        state = 'normal' if self.accounts_loaded else 'disabled'
        
        for key in accounts_dependent:
            if key in self.check_widgets:
                self.check_widgets[key].config(state=state)
    
    def load_accounts(self):
        """Load iXBRL accounts files."""
        filepaths = filedialog.askopenfilenames(
            title="Select iXBRL account files",
            filetypes=[("XHTML files", "*.xhtml"), ("HTML files", "*.html")]
        )
        
        if not filepaths:
            return
        
        self.status_var.set("Parsing accounts files...")
        self.app.update_idletasks()
        
        try:
            self.financial_analyzer = FinancialAnalyzer()
            df = self.financial_analyzer.load_files(list(filepaths))
            
            if not df.empty:
                self.accounts_loaded = True
                years = sorted(df['Year'].unique())
                self.accounts_status_label.config(
                    text=f"Loaded {len(years)} years ({years[0]}-{years[-1]})",
                    foreground='green'
                )
                
                # Validate accounts match company
                if self.company_data:  # Only validate if company already loaded
                    is_valid, message = self._validate_accounts_match_company()
                    
                    if not is_valid:
                        response = messagebox.askyesno(
                            "Company Mismatch Warning",
                            f"The uploaded accounts may not match the selected company:\n\n{message}\n\nDo you want to continue anyway?",
                            icon='warning'
                        )
                        
                        if not response:
                            self.clear_accounts()
                            return
                        else:
                            self.accounts_status_label.config(
                                text=f"Loaded {len(years)} years ({years[0]}-{years[-1]}) - WARNING: Possible mismatch",
                                foreground='orange'
                            )
                
                self.status_var.set("Accounts loaded successfully.")
                self._update_accounts_checkboxes()
            else:
                raise ValueError("No valid data found in accounts files.")
                
        except Exception as e:
            log_message(f"Error loading accounts: {e}")
            messagebox.showerror("Error", f"Could not load accounts: {e}")
            self.accounts_status_label.config(text="Error loading files.", foreground='red')
    
    def start_report_generation(self):
        """Kick off report generation in background thread."""
        self.generate_btn.config(state='disabled')
        self.cancel_flag.clear()

        # Collect manual input on main thread (thread-safe)
        self._manual_data = self._collect_manual_input()
        self._proposed_award = 0.0
        self._payment_mechanism = 'Unknown'
        raw_award = self.proposed_award_var.get().strip().replace(',', '').replace('£', '')
        if raw_award:
            try:
                self._proposed_award = float(raw_award)
            except ValueError:
                log_message(f"Invalid proposed award amount: {raw_award}")
        self._payment_mechanism = self.payment_mechanism_var.get() or 'Unknown'
        self._igm_mode = self.check_vars.get('igm_mode', tk.BooleanVar(value=False)).get()
        # Build cross-analysis thresholds snapshot from current self.thresholds dict
        _ca_keys = set(CrossAnalysisThresholds.__dataclass_fields__)
        self._ca_thresholds = CrossAnalysisThresholds(
            **{k: self.thresholds[k] for k in _ca_keys if k in self.thresholds}
        )

        threading.Thread(target=self._generate_report_thread, daemon=True).start()
    
    def _generate_report_thread(self):
        """Main report generation logic."""
        if self._entity_type == 'charity':
            return self._generate_charity_report_thread()
        try:
            self.safe_update(self.status_var.set, "Analyzing company data...")
            
            # Run all enabled checks
            findings = []
            
            if self.check_vars['company_status'].get():
                findings.extend(self._check_company_status())
            
            if self.check_vars['filing_status'].get():
                findings.extend(self._check_filing_compliance())
            
            if self.check_vars['solvency'].get() and self.accounts_loaded:
                findings.extend(self._check_solvency())
            
            if self.check_vars['liquidity'].get() and self.accounts_loaded:
                findings.extend(self._check_liquidity())
            
            if self.check_vars['director_churn'].get():
                findings.extend(self._check_director_churn())
            
            if self.check_vars['revenue_trends'].get() and self.accounts_loaded:
                findings.extend(self._check_revenue_trends())
            
            if self.check_vars['predictive_outlook'].get() and self.accounts_loaded:
                findings.extend(self._check_predictive_outlook())
            
            if self.check_vars['default_address'].get():
                findings.extend(self._check_default_address())
                findings.extend(self._check_director_psc_addresses())
            
            if self.check_vars['accounting_changes'].get():
                findings.extend(self._check_accounting_changes())
            
            if self.check_vars['offshore_pscs'].get():
                findings.extend(self._check_offshore_pscs())
            
            # Tier 3 (expensive checks)
            if self.check_vars['director_history'].get():
                self.safe_update(self.status_var.set, "Analyzing director history (this may take a while)...")
                findings.extend(self._check_director_insolvency_history())
            
            if self.check_vars['phoenix_check'].get():
                findings.extend(self._check_phoenix_companies())

            # Filing pattern analysis
            if self.check_vars.get('filing_patterns', tk.BooleanVar(value=False)).get():
                findings.extend(self._check_filing_patterns())

            # Fetch grants data if enabled
            self._grants_data = None
            if self.check_vars.get('grants_lookup', tk.BooleanVar(value=False)).get():
                self.safe_update(self.status_var.set, "Fetching grants data from GrantNav...")
                cnum = self.company_data['profile']['company_number']
                try:
                    self._grants_data = fetch_grants_for_company(cnum)
                except Exception as e:
                    log_message(f"Error fetching grants data: {e}")

            # Cross-analysis rules
            self._cross_analysis_report = None
            if self.check_vars.get('cross_analysis', tk.BooleanVar(value=False)).get():
                self.safe_update(self.status_var.set, "Running cross-analysis rules...")
                try:
                    unified = UnifiedFinancialData(
                        auto_analyzer=self.financial_analyzer,
                        manual_data=self._manual_data,
                    )
                    # Detect late filing for quality gate
                    late_filing_detected = any(
                        f.get('severity') in ('Critical', 'Elevated') and 'late' in f.get('title', '').lower()
                        for f in findings
                    )
                    # Company age
                    company_age_months = None
                    inc_date_str = self.company_data.get('profile', {}).get('date_of_creation', '')
                    if inc_date_str:
                        try:
                            inc_date = datetime.strptime(inc_date_str, '%Y-%m-%d')
                            company_age_months = (datetime.now() - inc_date).days / 30.0
                        except ValueError:
                            pass
                    # Accounts type from profile
                    accounts_type = self.company_data.get('profile', {}).get('accounts', {}).get('type')

                    self._cross_analysis_report = run_cross_analysis(
                        unified=unified,
                        grants_data=self._grants_data,
                        proposed_award=self._proposed_award,
                        payment_mechanism=self._payment_mechanism,
                        late_filing_detected=late_filing_detected,
                        company_age_months=company_age_months,
                        accounts_type=accounts_type,
                        igm_mode=self._igm_mode,
                        thresholds=self._ca_thresholds,
                    )
                except Exception as e:
                    log_message(f"Error running cross-analysis: {e}")

            # Generate company timeline
            self.safe_update(self.status_var.set, "Generating company timeline...")
            try:
                self._timeline_b64 = generate_company_timeline(
                    self.company_data['profile'],
                    self.company_data.get('officers', {}),
                    self.company_data.get('pscs', {}),
                    self.company_data.get('filing_history', {}),
                    grants_data=self._grants_data,
                )
            except Exception as e:
                log_message(f"Error generating timeline: {e}")
                self._timeline_b64 = None

            # Trace ownership structure if enabled
            self._ownership_data = None
            self._ownership_b64 = None
            if self.check_vars.get('ownership_graph', tk.BooleanVar(value=False)).get():
                self.safe_update(self.status_var.set, "Tracing corporate ownership structure...")
                cnum = self.company_data['profile']['company_number']
                try:
                    self._ownership_data = trace_ownership_chain(
                        self.api_key, self.ch_token_bucket, cnum, self.cancel_flag,
                        status_callback=lambda msg: self.safe_update(self.status_var.set, msg),
                    )
                    if self._ownership_data:
                        self._ownership_b64 = generate_static_ownership_graph(
                            self.company_data['profile'].get('company_name', 'Unknown'),
                            cnum,
                            self._ownership_data,
                        )
                except Exception as e:
                    log_message(f"Error generating ownership graph: {e}")

            # Generate report HTML
            self.safe_update(self.status_var.set, "Generating report...")
            html_content = self._build_report_html(findings)
            
            # Save and open
            filename = os.path.join(
                CONFIG_DIR,
                f"DD_Report_{self.company_data['profile']['company_number']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            )
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            self.safe_update(self.status_var.set, "Report generated! Opening in browser...")
            webbrowser.open(f"file://{os.path.realpath(filename)}")

            # Record in app_state for home screen
            self.app_state.recent_edd_reports.insert(0, {
                "name": self.company_data['profile'].get('company_name', 'Unknown'),
                "path": os.path.realpath(filename),
                "date": datetime.now().strftime('%Y-%m-%d %H:%M'),
            })
            self.app_state.recent_edd_reports = self.app_state.recent_edd_reports[:5]

        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            log_message(f"Error generating report: {e}\n{error_details}")
            self.safe_update(messagebox.showerror, "Error", f"Report generation failed: {e}")
        finally:
        # Add this finally block
            self.safe_update(self._finish_report_generation)
    # --- Charity Report Generation ---

    def _generate_charity_report_thread(self):
        """Generate a full EDD report for a charity."""
        try:
            self.safe_update(self.status_var.set, "Analyzing charity data...")

            # Merge default thresholds with charity-specific overrides
            charity_thresholds = dict(self.thresholds)
            charity_thresholds.update(CHARITY_EDD_THRESHOLDS)

            # Fetch additional data needed for report generation
            api_key = self.charity_api_key
            reg_num = self.charity_data['details'].get('reg_charity_number', '')

            # Regulatory reports
            self.safe_update(self.status_var.set, "Fetching regulatory reports...")
            reg_reports, _ = cc_get_regulatory_report(api_key, reg_num)
            self.charity_data['regulatory_reports'] = reg_reports

            # Policy information
            self.safe_update(self.status_var.set, "Fetching policy information...")
            policies, _ = cc_get_policy_information(api_key, reg_num)
            self.charity_data['policies'] = policies

            # Other regulators
            other_regs, _ = cc_get_other_regulators(api_key, reg_num)
            self.charity_data['other_regulators'] = other_regs

            # Other names
            other_names, _ = cc_get_other_names(api_key, reg_num)
            self.charity_data['other_names'] = other_names

            # Areas of operation
            areas, _ = cc_get_area_of_operation(api_key, reg_num)
            self.charity_data['area_of_operation'] = areas

            # Run all charity checks
            self.safe_update(self.status_var.set, "Running charity risk checks...")
            findings = []

            check_funcs = [
                check_charity_status,
                check_reporting_status,
                check_regulatory_reports,
                check_accounts_qualified,
                check_accounts_submission_pattern,
                check_net_assets,
                check_reserves_ratio,
                check_income_expenditure_trends,
                check_income_volatility,
                check_fundraising_cost_ratio,
                check_government_funding_concentration,
                check_trustee_remuneration,
                check_policies,
                check_trustee_count,
                check_contact_transparency,
                check_charity_default_address,
                check_area_of_operation,
                check_professional_fundraiser,
            ]

            for check_fn in check_funcs:
                if self.cancel_flag.is_set():
                    return
                try:
                    findings.extend(check_fn(self.charity_data, charity_thresholds))
                except Exception as e:
                    log_message(f"Error in charity check {check_fn.__name__}: {e}")

            # Fetch grants data if enabled
            self._grants_data = None
            if self.check_vars.get('grants_lookup', tk.BooleanVar(value=False)).get():
                self.safe_update(self.status_var.set, "Fetching grants data from GrantNav...")
                try:
                    # Fetch by charity identifier
                    grants = fetch_grants_for_org(f"GB-CHC-{reg_num}")
                    # Also try linked company number if available
                    linked_co = self.charity_data.get('details', {}).get(
                        'company_number') or ''
                    linked_co = linked_co.strip()
                    if linked_co:
                        co_grants = fetch_grants_for_company(linked_co)
                        if co_grants:
                            seen_ids = {g.get('id') for g in grants if g.get('id')}
                            for g in co_grants:
                                if g.get('id') not in seen_ids:
                                    grants.append(g)
                    self._grants_data = grants
                except Exception as e:
                    log_message(f"Error fetching grants data for charity: {e}")

            # Cross-analysis rules
            self._cross_analysis_report = None
            if self.check_vars.get('cross_analysis', tk.BooleanVar(value=False)).get():
                self.safe_update(self.status_var.set, "Running cross-analysis rules...")
                try:
                    charity_financial = CharityFinancialData(
                        financial_history=self.charity_data.get('financial_history'),
                        assets_liabilities=self.charity_data.get('assets_liabilities'),
                        overview=self.charity_data.get('overview'),
                        manual_data=getattr(self, '_manual_data', None),
                    )
                    late_filing_detected = any(
                        f.get('severity') in ('Critical', 'Elevated')
                        and 'late' in f.get('title', '').lower()
                        for f in findings
                    )
                    # Charity age
                    charity_age_months = None
                    reg_date_str = self.charity_data.get('details', {}).get(
                        'date_of_registration', '')
                    if reg_date_str:
                        try:
                            reg_date = datetime.strptime(reg_date_str[:10], '%Y-%m-%d')
                            charity_age_months = (datetime.now() - reg_date).days / 30.0
                        except ValueError:
                            pass

                    self._cross_analysis_report = run_cross_analysis(
                        unified=charity_financial,
                        grants_data=self._grants_data,
                        proposed_award=self._proposed_award,
                        payment_mechanism=self._payment_mechanism,
                        late_filing_detected=late_filing_detected,
                        company_age_months=charity_age_months,
                        accounts_type=None,
                        igm_mode=self._igm_mode,
                        thresholds=self._ca_thresholds,
                    )
                except Exception as e:
                    log_message(f"Error running cross-analysis for charity: {e}")

            # Generate report HTML
            self.safe_update(self.status_var.set, "Generating report...")
            html_content = self._build_charity_report_html(findings)

            # Save and open
            filename = os.path.join(
                CONFIG_DIR,
                f"DD_Report_Charity_{reg_num}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            )
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html_content)

            self.safe_update(self.status_var.set, "Report generated! Opening in browser...")
            webbrowser.open(f"file://{os.path.realpath(filename)}")

            # Record in app_state for home screen
            details = self.charity_data.get('details', {})
            self.app_state.recent_edd_reports.insert(0, {
                "name": details.get('charity_name', 'Unknown Charity'),
                "path": os.path.realpath(filename),
                "date": datetime.now().strftime('%Y-%m-%d %H:%M'),
            })
            self.app_state.recent_edd_reports = self.app_state.recent_edd_reports[:5]

        except Exception as e:
            error_details = traceback.format_exc()
            log_message(f"Error generating charity report: {e}\n{error_details}")
            self.safe_update(messagebox.showerror, "Error",
                             f"Report generation failed: {e}")
        finally:
            self.safe_update(self._finish_report_generation)

    def _build_charity_report_html(self, findings):
        """Generate the full HTML report for a charity."""
        details = self.charity_data.get('details', {})
        charity_name = html.escape(details.get('charity_name', 'Unknown Charity'))
        reg_number = html.escape(str(details.get('reg_charity_number', 'N/A')))

        # Categorize findings
        critical = [f for f in findings if f['severity'] == 'Critical']
        elevated = [f for f in findings if f['severity'] == 'Elevated']
        moderate = [f for f in findings if f['severity'] == 'Moderate']
        positive = [f for f in findings if f['severity'] == 'Positive']

        # Generate charity-specific charts
        chart_html = generate_charity_chart_html(self.charity_data)

        # Reuse the same CSS from the company report
        html_output = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Due Diligence Report - {charity_name}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            max-width: 1200px;
            margin: 20px auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
        }}
        .header h1 {{ margin: 0 0 10px 0; }}
        .header p {{ margin: 5px 0; opacity: 0.9; }}
        .section {{
            background: white;
            padding: 25px;
            margin-bottom: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .section h2 {{
            color: #333;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
            margin-top: 0;
        }}
        .finding {{
            margin: 20px 0;
            padding: 15px;
            border-left: 4px solid #ccc;
            background: #f9f9f9;
        }}
        .finding.critical {{ border-left-color: #dc3545; background: #fff5f5; }}
        .finding.elevated {{ border-left-color: #fd7e14; background: #fff9f5; }}
        .finding.moderate {{ border-left-color: #ffc107; background: #fffef5; }}
        .finding.positive {{ border-left-color: #28a745; background: #f5fff5; }}
        .finding h3 {{ margin: 0 0 10px 0; color: #333; }}
        .finding .severity {{
            display: inline-block;
            padding: 3px 10px;
            border-radius: 3px;
            font-size: 12px;
            font-weight: bold;
            margin-right: 10px;
        }}
        .severity.critical {{ background: #dc3545; color: white; }}
        .severity.elevated {{ background: #fd7e14; color: white; }}
        .severity.moderate {{ background: #ffc107; color: #333; }}
        .severity.positive {{ background: #28a745; color: white; }}
        .recommendation {{
            margin-top: 10px;
            padding: 10px;
            background: white;
            border-left: 3px solid #667eea;
            font-style: italic;
        }}
        .company-profile {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 15px;
        }}
        .profile-item {{
            padding: 10px;
            background: #f9f9f9;
            border-radius: 5px;
        }}
        .profile-item strong {{
            display: block;
            color: #667eea;
            margin-bottom: 5px;
        }}
        .executive-summary {{
            font-size: 16px;
            line-height: 1.6;
            padding: 20px;
            background: #f0f4ff;
            border-radius: 5px;
            border-left: 4px solid #667eea;
        }}
        .chart-container {{
            margin: 20px 0;
            text-align: center;
        }}
        .chart-container img {{
            max-width: 100%;
            border: 1px solid #ddd;
            border-radius: 5px;
        }}
        .grants-summary {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 15px;
            margin: 20px 0;
        }}
        .grants-stat {{
            padding: 15px;
            background: #f0f4ff;
            border-radius: 5px;
            text-align: center;
            font-size: 18px;
        }}
        .grants-stat strong {{
            display: block;
            color: #667eea;
            font-size: 12px;
            margin-bottom: 5px;
            text-transform: uppercase;
        }}
        .grants-table {{
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
            font-size: 13px;
        }}
        .grants-table th {{
            background: #667eea;
            color: white;
            padding: 10px;
            text-align: left;
        }}
        .grants-table td {{
            padding: 8px 10px;
            border-bottom: 1px solid #eee;
        }}
        .grants-table tr:hover td {{ background: #f5f7ff; }}
        .cross-analysis-summary {{
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
        }}
        .cross-analysis-summary th {{
            background: #667eea;
            color: white;
            padding: 10px;
            text-align: left;
            font-size: 13px;
        }}
        .cross-analysis-summary td {{
            padding: 8px 10px;
            border-bottom: 1px solid #eee;
            font-size: 13px;
        }}
        .risk-high {{ background: #dc3545; color: white; padding: 3px 10px; border-radius: 3px; font-size: 12px; font-weight: bold; display: inline-block; }}
        .risk-medium {{ background: #ffc107; color: #333; padding: 3px 10px; border-radius: 3px; font-size: 12px; font-weight: bold; display: inline-block; }}
        .risk-low {{ background: #28a745; color: white; padding: 3px 10px; border-radius: 3px; font-size: 12px; font-weight: bold; display: inline-block; }}
        .risk-not-assessed {{ background: #6c757d; color: white; padding: 3px 10px; border-radius: 3px; font-size: 12px; font-weight: bold; display: inline-block; }}
        .confidence-auto {{ background: #667eea; color: white; padding: 2px 8px; border-radius: 3px; font-size: 11px; display: inline-block; }}
        .confidence-enriched {{ background: #28a745; color: white; padding: 2px 8px; border-radius: 3px; font-size: 11px; display: inline-block; }}
        .confidence-limited {{ background: #fd7e14; color: white; padding: 2px 8px; border-radius: 3px; font-size: 11px; display: inline-block; }}
        .confidence-skipped {{ background: #6c757d; color: white; padding: 2px 8px; border-radius: 3px; font-size: 11px; display: inline-block; }}
        .composite-warning {{
            background: #fff5f5;
            border: 2px solid #dc3545;
            border-radius: 8px;
            padding: 15px;
            margin: 15px 0;
            font-weight: bold;
            color: #dc3545;
        }}
        .pattern-warning {{
            background: #fff9f5;
            border: 2px solid #fd7e14;
            border-radius: 8px;
            padding: 15px;
            margin: 15px 0;
            font-weight: bold;
            color: #856404;
        }}
        .cross-rule-card {{
            margin: 20px 0;
            padding: 15px;
            border-left: 4px solid #ccc;
            background: #f9f9f9;
        }}
        .cross-rule-card.high {{ border-left-color: #dc3545; background: #fff5f5; }}
        .cross-rule-card.medium {{ border-left-color: #ffc107; background: #fffef5; }}
        .cross-rule-card.low {{ border-left-color: #28a745; background: #f5fff5; }}
        .cross-rule-card.not-assessed {{ border-left-color: #6c757d; background: #f9f9f9; }}
        .rule-id-badge {{
            display: inline-block;
            background: #667eea;
            color: white;
            padding: 2px 8px;
            border-radius: 3px;
            font-size: 12px;
            font-weight: bold;
            margin-right: 8px;
        }}
        .quality-caveat {{
            background: #fff3cd;
            border: 1px solid #ffc107;
            border-radius: 5px;
            padding: 10px;
            margin: 10px 0;
            font-size: 13px;
        }}
        .grant-detail {{
            margin: 15px 0;
            padding: 15px;
            background: #f9f9f9;
            border-left: 3px solid #667eea;
            border-radius: 0 5px 5px 0;
        }}
        .grant-detail h4 {{ margin: 0 0 5px 0; color: #333; }}
        .grant-meta {{ font-size: 12px; color: #666; margin: 0 0 10px 0; }}
        .trend-table {{
            width: auto;
            border-collapse: collapse;
            margin: 10px 0;
            font-size: 12px;
        }}
        .trend-table th {{
            background: #f0f4ff;
            padding: 6px 12px;
            text-align: right;
            border: 1px solid #ddd;
        }}
        .trend-table td {{
            padding: 6px 12px;
            text-align: right;
            border: 1px solid #eee;
        }}
        @media print {{
            body {{ background: white; }}
            .section {{ box-shadow: none; border: 1px solid #ddd; }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Enhanced Due Diligence Report</h1>
        <p><strong>{charity_name}</strong></p>
        <p>Charity Registration Number: {reg_number}</p>
        <p>Report Generated: {datetime.now().strftime('%d %B %Y at %H:%M')}</p>
    </div>

    <div class="section">
        <h2>1. Executive Summary</h2>
        <div class="executive-summary">
            {self._generate_charity_executive_summary(critical, elevated, moderate, charity_name)}
        </div>
    </div>

    <div class="section">
        <h2>2. Charity Profile</h2>
        {generate_charity_profile_html(self.charity_data)}
    </div>

    {self._generate_findings_section('Critical Risk Indicators', critical)}
    {self._generate_findings_section('Elevated Risk Indicators', elevated)}
    {self._generate_findings_section('Moderate Risk Indicators', moderate)}

    {chart_html}

    {self._generate_grants_section()}

    {self._generate_cross_analysis_section()}

    {self._generate_charity_positive_indicators_html(positive)}

    <div class="section">
        <h2>Data Limitations & Disclaimers</h2>
        {generate_charity_limitations_html(self.charity_data, self._grants_data is not None)}
    </div>

    <div class="section" style="background: #f0f4ff; text-align: center;">
        <p style="margin: 0; color: #666;">
            Report generated by Data Investigation Multi-Tool<br>
            This report is based on publicly available information and should not be the sole basis for decision-making.
        </p>
    </div>
</body>
</html>
"""
        return html_output

    def _generate_charity_executive_summary(self, critical, elevated, moderate, charity_name):
        """Generate executive summary for a charity report."""
        total_concerns = len(critical) + len(elevated) + len(moderate)

        if total_concerns == 0:
            summary = (
                f"Based on the analysis performed, {html.escape(charity_name)} shows no "
                "significant risk indicators in the areas examined. However, this assessment "
                "is based on available public information and should be supplemented with "
                "additional due diligence as appropriate for your specific requirements."
            )
        else:
            summary = (
                f"Based on available data, {charity_name} shows "
                f"<strong>{total_concerns} concerning indicator(s)</strong> that warrant "
                "further investigation"
            )

            if critical:
                key_issues = [f['title'] for f in critical[:2]]
                summary += f", particularly around: <strong>{', '.join(key_issues)}</strong>"

            summary += ".<br><br>"

            if critical:
                summary += (
                    f"<strong>Critical findings ({len(critical)}):</strong> "
                    "These are severe red flags that require immediate attention and may "
                    "indicate the charity is unsuitable for the intended funding "
                    "relationship.<br><br>"
                )
            if elevated:
                summary += (
                    f"<strong>Elevated risk findings ({len(elevated)}):</strong> "
                    "These indicators suggest heightened risk that should be investigated "
                    "further before proceeding.<br><br>"
                )
            if moderate:
                summary += (
                    f"<strong>Moderate concerns ({len(moderate)}):</strong> "
                    "These factors should be considered and may require additional "
                    "information or monitoring."
                )

        # Cross-analysis summary
        report = getattr(self, '_cross_analysis_report', None)
        if report:
            high_count = sum(1 for r in report.results if r.risk_flag == 'HIGH')
            medium_count = sum(1 for r in report.results if r.risk_flag == 'MEDIUM')
            assessed = sum(
                1 for r in report.results
                if r.risk_flag != 'NOT_ASSESSED' and r.confidence != 'SKIPPED'
            )
            if assessed > 0:
                summary += f"<br><br><strong>Cross-analysis ({assessed} rules assessed):</strong> "
                if high_count > 0:
                    summary += f"{high_count} high-risk and {medium_count} medium-risk indicator(s) identified. "
                elif medium_count > 0:
                    summary += f"{medium_count} medium-risk indicator(s) identified. "
                else:
                    summary += "No high or medium-risk indicators identified. "
                summary += "See the Financial &amp; Grant Cross-Analysis section for details."

        return summary

    def _generate_charity_positive_indicators_html(self, positive_findings):
        """Generate positive indicators section for charity report."""
        html_output = '<div class="section"><h2>Positive Indicators</h2>'

        if positive_findings:
            for finding in positive_findings:
                html_output += f'''
                <div class="finding positive">
                    <h3>{html.escape(finding['title'])}</h3>
                    <p>{html.escape(finding['narrative'])}</p>
                </div>
                '''
        else:
            html_output += (
                '<p>No specific positive indicators were identified in the analysis '
                'performed. This does not indicate problems, but rather reflects the '
                'focus of due diligence on identifying risks.</p>'
            )

        html_output += '</div>'
        return html_output

    # --- Risk Check Functions ---
    
    def _check_company_status(self):
        """Check for concerning company statuses."""
        findings = []
        profile = self.company_data['profile']
        status = profile.get('company_status', '').lower()
        
        if 'liquidation' in status:
            findings.append({
                'category': 'Governance',
                'severity': 'Critical',
                'title': 'Company in Liquidation',
                'narrative': f"The company status is currently '{profile.get('company_status')}'. This indicates the company is being wound up and assets are being distributed to creditors.",
                'recommendation': 'Do not proceed with any new commitments. Existing contracts should be reviewed urgently with legal counsel.'
            })
        
        if 'administration' in status:
            findings.append({
                'category': 'Governance',
                'severity': 'Critical',
                'title': 'Company in Administration',
                'narrative': "The company is under administration, meaning it is insolvent and control has passed to an appointed administrator.",
                'recommendation': 'Existing arrangements should be reviewed immediately. New business should not be conducted without administrator approval.'
            })
        
        if 'dissolved' in status:
            findings.append({
                'category': 'Governance',
                'severity': 'Critical',
                'title': 'Company Dissolved',
                'narrative': "The company has been dissolved and no longer exists as a legal entity.",
                'recommendation': 'This company cannot enter into contracts or conduct business.'
            })
        
        # Check for active notice to strike off
        filing_history = self.company_data.get('filing_history', {})
        for filing in filing_history.get('items', [])[:20]:  # Check recent filings
            if 'GAZ1' in filing.get('type', '') or 'notice' in filing.get('description', '').lower():
                findings.append({
                    'category': 'Governance',
                    'severity': 'Elevated',
                    'title': 'Notice to Strike Off Filed',
                    'narrative': f"A notice regarding strike-off action was filed on {format_display_date(filing.get('date', ''))}. This may indicate the company is being dissolved.",
                    'recommendation': 'Verify current company status directly with Companies House and assess whether the company is actively trading.'
                })
                break
        
        # Check if recently incorporated
        if profile.get('date_of_creation'):
            from datetime import datetime
            inc_date = datetime.strptime(profile['date_of_creation'], '%Y-%m-%d')
            age_months = (datetime.now() - inc_date).days / 30
            
            if age_months < 6:
                findings.append({
                    'category': 'Governance',
                    'severity': 'Moderate',
                    'title': 'Recently Incorporated Company',
                    'narrative': f"The company was incorporated on {format_display_date(profile['date_of_creation'])}, approximately {int(age_months)} months ago. Recently incorporated companies have limited trading history.",
                    'recommendation': 'Request additional due diligence on the directors and any parent/sister companies. Consider tighter credit terms or guarantees.'
                })
        
        return findings

    def _check_filing_patterns(self):
        """Analyze filing history for concerning patterns."""
        findings = []
        filing_history = self.company_data.get('filing_history', {})
        items = filing_history.get('items', [])
        if not items:
            return findings

        # Parse incorporation date for first-year accounts deadline calculation
        inc_date_str = self.company_data.get('profile', {}).get('date_of_creation', '')
        inc_date = None
        if inc_date_str:
            try:
                inc_date = datetime.strptime(inc_date_str, '%Y-%m-%d')
            except ValueError:
                pass

        late_filings = []
        accounts_years = []
        late_cs_filings = []
        charge_filings = []
        resolution_count = 0

        # Collect all AA filing action_dates to identify the earliest (first-year filing)
        aa_action_dates = []
        for filing in items:
            if filing.get('type', '').startswith('AA') and filing.get('action_date'):
                try:
                    aa_action_dates.append(datetime.strptime(filing['action_date'], '%Y-%m-%d'))
                except ValueError:
                    pass
        earliest_aa_action_date = min(aa_action_dates) if aa_action_dates else None

        for filing in items:
            f_type = filing.get('type', '')
            f_date_str = filing.get('date', '')
            f_action_date_str = filing.get('action_date', '')

            f_date = None
            f_action_date = None
            if f_date_str:
                try:
                    f_date = datetime.strptime(f_date_str, '%Y-%m-%d')
                except ValueError:
                    pass
            if f_action_date_str:
                try:
                    f_action_date = datetime.strptime(f_action_date_str, '%Y-%m-%d')
                except ValueError:
                    pass

            # Check accounts filings (AA type)
            if f_type.startswith('AA'):
                # Use made-up-to year (action_date) for gap detection, with fallback
                if f_action_date:
                    accounts_years.append(f_action_date.year)
                elif f_date:
                    accounts_years.append(f_date.year)

                # Late filing: deadline is 9 months after made-up-to date,
                # or 21 months after incorporation for first-year accounts
                if f_date and f_action_date:
                    is_first_year = (
                        inc_date is not None
                        and earliest_aa_action_date is not None
                        and f_action_date == earliest_aa_action_date
                    )
                    if is_first_year and inc_date:
                        deadline = inc_date + relativedelta(months=21)
                    else:
                        deadline = f_action_date + relativedelta(months=9)

                    if f_date > deadline:
                        days_late = (f_date - deadline).days
                        late_filings.append({
                            'date': format_display_date(f_date_str),
                            'days_late': days_late,
                            'description': filing.get('description', ''),
                        })

            # Check confirmation statement filings (CS01 type)
            # Deadline: 14 days after the review period end date (action_date)
            if f_type.startswith('CS01'):
                if f_date and f_action_date:
                    cs_deadline = f_action_date + timedelta(days=14)
                    if f_date > cs_deadline:
                        days_late = (f_date - cs_deadline).days
                        late_cs_filings.append({
                            'date': format_display_date(f_date_str),
                            'days_late': days_late,
                        })

            # Track charges
            if f_type.startswith('MR01'):
                charge_filings.append(filing)

            # Track resolutions
            if f_type.startswith('RES') or 'resolution' in filing.get('description', '').lower():
                resolution_count += 1

        # Report late accounts filings
        if late_filings:
            severity = 'Elevated' if len(late_filings) >= 3 else 'Moderate'
            late_details = '; '.join(
                [f"{lf['date']} ({lf['days_late']} days late)" for lf in late_filings[:5]]
            )
            findings.append({
                'category': 'Governance',
                'severity': severity,
                'title': f'Late Accounts Filings Detected ({len(late_filings)})',
                'narrative': f"Analysis of filing history shows {len(late_filings)} instance(s) where accounts were filed after their statutory deadline (9 months from the accounting period end, or 21 months from incorporation for first-year accounts): {late_details}. A pattern of late filing may indicate poor financial management or administrative difficulties.",
                'recommendation': 'Investigate reasons for late filing and assess current financial management capabilities.',
            })

        # Report late confirmation statement filings
        if late_cs_filings:
            severity = 'Elevated' if len(late_cs_filings) >= 3 else 'Moderate'
            cs_details = '; '.join(
                [f"{lf['date']} ({lf['days_late']} days late)" for lf in late_cs_filings[:5]]
            )
            findings.append({
                'category': 'Governance',
                'severity': severity,
                'title': f'Late Confirmation Statements Detected ({len(late_cs_filings)})',
                'narrative': f"Analysis of filing history shows {len(late_cs_filings)} instance(s) where confirmation statements were filed after their statutory deadline (14 days after the review period end): {cs_details}. Late confirmation statements may indicate administrative neglect.",
                'recommendation': 'Verify that the company maintains timely corporate governance filings.',
            })

        # Accounts filing gaps (using made-up-to year)
        if len(accounts_years) >= 2:
            accounts_years_sorted = sorted(set(accounts_years))
            gaps = []
            for i in range(1, len(accounts_years_sorted)):
                if accounts_years_sorted[i] - accounts_years_sorted[i - 1] > 1:
                    gaps.append(f"{accounts_years_sorted[i - 1]}-{accounts_years_sorted[i]}")
            if gaps:
                findings.append({
                    'category': 'Governance',
                    'severity': 'Moderate',
                    'title': 'Gaps in Accounts Filing History',
                    'narrative': f"There appear to be gaps in the accounts filing history for the following periods: {', '.join(gaps)}. Missing accounts filings may indicate periods of non-compliance or dormancy.",
                    'recommendation': 'Request clarification on any periods where accounts were not filed.',
                })

        # Charge registrations
        if charge_filings:
            findings.append({
                'category': 'Financial',
                'severity': 'Moderate',
                'title': f'Secured Charges Registered ({len(charge_filings)})',
                'narrative': f"The company has {len(charge_filings)} charge registration(s) in its filing history, indicating secured borrowing. Charges grant creditors priority over company assets.",
                'recommendation': 'Review the nature and extent of secured borrowing and assess whether existing charges might affect the company\'s ability to meet new obligations.',
            })

        # High resolution activity
        if resolution_count >= 5:
            findings.append({
                'category': 'Governance',
                'severity': 'Moderate',
                'title': f'Frequent Special Resolutions ({resolution_count})',
                'narrative': f"The company has filed {resolution_count} special resolutions, which may indicate significant corporate changes such as share restructuring, articles amendments, or changes to company objects.",
                'recommendation': 'Review the nature of resolutions filed to understand the corporate changes that have occurred.',
            })

        return findings

    def _fetch_all_appointments(self, appointments_link):
        """
        Fetches all appointment items for an officer, handling API pagination.
        Returns a list of appointment items or an empty list on error.
        """
        if not appointments_link:
            return []

        page_size = 100  # Max items per page
        start_index = 0
        all_items = []

        while not self.cancel_flag.is_set():
            path = f"{appointments_link}?items_per_page={page_size}&start_index={start_index}"
            
            data, err = ch_get_data(self.api_key, self.ch_token_bucket, path)
            if err or not data or not data.get("items"):
                log_message(f"Could not fetch appointments from {path}: {err}")
                break # Stop if there's an error or no more items

            page_items = data.get("items", [])
            all_items.extend(page_items)

            # Stop if the last page was not full, indicating the end
            if len(page_items) < page_size:
                break

            start_index += page_size
        
        return all_items
    
    def _check_filing_compliance(self):
        """Check filing history for late submissions."""
        findings = []
        profile = self.company_data['profile']
        
        accounts = profile.get('accounts', {})
        if accounts.get('overdue'):
            findings.append({
                'category': 'Governance',
                'severity': 'Elevated',
                'title': 'Accounts Currently Overdue',
                'narrative': f"The company's accounts are currently overdue. The next accounts were due on {format_display_date(accounts.get('next_due', ''))}.",
                'recommendation': 'Late filing may indicate administrative difficulties or financial stress. Request current management accounts.'
            })
        
        cs = profile.get('confirmation_statement', {})
        if cs.get('overdue'):
            findings.append({
                'category': 'Governance',
                'severity': 'Moderate',
                'title': 'Confirmation Statement Overdue',
                'narrative': "The company's confirmation statement is overdue, indicating possible administrative neglect.",
                'recommendation': 'Verify that the company is actively managed and trading.'
            })
        
        # Check filing history for pattern of late filings
        filing_history = self.company_data.get('filing_history', {})
        late_count = 0
        for filing in filing_history.get('items', []):
            if filing.get('action_date') and filing.get('date'):
                # This is a simplification - actual late filing detection would need more logic
                pass  # Implement if API provides clear late indicators
        
        return findings
    
    def _check_solvency(self):
        """Check net asset position."""
        findings = []
        
        if not self.financial_analyzer or self.financial_analyzer.data.empty:
            return findings
        
        df = self.financial_analyzer.data.sort_values('Year')
        latest = df.iloc[-1]
        
        # Calculate net assets if possible
        if 'NetAssets' in latest:
            net_assets = latest['NetAssets']
            
            if net_assets < 0:
                findings.append({
                    'category': 'Financial',
                    'severity': 'Critical',
                    'title': 'Negative Net Assets',
                    'narrative': f"The company reported net assets of £{net_assets:,.0f} in {int(latest['Year'])}. This negative position indicates the company is technically insolvent, with liabilities exceeding assets.",
                    'recommendation': 'This is a critical red flag. Request an explanation from management and updated financial projections. Consider requiring personal guarantees or security.'
                })
            elif len(df) >= 2:
                previous = df.iloc[-2]
                if 'NetAssets' in previous:
                    change_pct = ((net_assets - previous['NetAssets']) / abs(previous['NetAssets'])) * 100
                    
                    if net_assets > 0 and change_pct < -self.thresholds['solvency_decline_pct']:
                        findings.append({
                            'category': 'Financial',
                            'severity': 'Elevated',
                            'title': 'Significant Decline in Net Assets',
                            'narrative': f"Net assets declined by {abs(change_pct):.1f}% from £{previous['NetAssets']:,.0f} ({int(previous['Year'])}) to £{net_assets:,.0f} ({int(latest['Year'])}). This substantial erosion of shareholder equity suggests financial difficulties.",
                            'recommendation': 'Investigate the cause of the decline. Request management accounts and cash flow forecasts.'
                        })
        
        return findings
    
    def _check_liquidity(self):
        """Check liquidity ratios."""
        findings = []
        
        if not self.financial_analyzer:
            return findings
        
        df_ratios = self.financial_analyzer.calculate_ratios()
        if df_ratios.empty:
            return findings
        
        df_ratios = df_ratios.sort_values('Year')
        latest = df_ratios.iloc[-1]
        
        # Current Ratio
        if 'CurrentRatio' in latest and pd.notna(latest['CurrentRatio']):
            current_ratio = latest['CurrentRatio']
            threshold = self.thresholds['current_ratio_min']
            
            if current_ratio < threshold:
                severity = 'Critical' if current_ratio < self.thresholds['current_ratio_critical'] else 'Elevated'
                
                narrative = f"The current ratio (current assets ÷ current liabilities) stands at {current_ratio:.2f} in {int(latest['Year'])}. "
                narrative += f"A ratio below {threshold} indicates potential difficulty meeting short-term obligations. "
                
                if len(df_ratios) >= 2:
                    trend = "declining" if latest['CurrentRatio'] < df_ratios.iloc[-2].get('CurrentRatio', float('inf')) else "stable"
                    narrative += f"The ratio has been {trend} over recent years."
                
                findings.append({
                    'category': 'Financial',
                    'severity': severity,
                    'title': 'Low Current Ratio',
                    'narrative': narrative,
                    'recommendation': 'Request a detailed breakdown of current assets and liabilities, plus a cash flow forecast for the next 12 months.'
                })
        
        # Quick Ratio
        if 'QuickRatio' in latest and pd.notna(latest['QuickRatio']):
            quick_ratio = latest['QuickRatio']
            threshold = self.thresholds['quick_ratio_min']
            
            if quick_ratio < threshold:
                findings.append({
                    'category': 'Financial',
                    'severity': 'Elevated',
                    'title': 'Low Quick Ratio',
                    'narrative': f"The quick ratio (liquid assets ÷ current liabilities) is {quick_ratio:.2f}, below the {threshold} threshold. This suggests limited immediate liquidity even before considering inventory.",
                    'recommendation': 'Assess the company\'s access to credit facilities and ability to convert debtors to cash quickly.'
                })
        
        # Cash position check
        if 'CashBankInHand' in latest and 'CurrentLiabilities' in latest:
            cash = latest['CashBankInHand']
            liabilities = latest['CurrentLiabilities']
            
            if pd.notna(cash) and pd.notna(liabilities) and liabilities > 0:
                cash_pct = (cash / liabilities) * 100
                
                if cash_pct < self.thresholds['cash_pct_min']:
                    findings.append({
                        'category': 'Financial',
                        'severity': 'Elevated',
                        'title': 'Very Low Cash Reserves',
                        'narrative': f"Cash holdings of £{cash:,.0f} represent only {cash_pct:.1f}% of current liabilities (£{liabilities:,.0f}). This minimal cash buffer creates vulnerability if creditors demand payment or revenue is disrupted.",
                        'recommendation': 'Request information on available credit facilities, debtor collection periods, and creditor payment terms.'
                    })
        
        return findings
    def _check_director_churn(self):
        """Analyze director and PSC turnover."""
        findings = []
        
        officers = self.company_data.get('officers', {})
        if not officers or not officers.get('items'):
            return findings
        
        # Count recent resignations
        from datetime import datetime, timedelta
        cutoff_date = datetime.now() - timedelta(days=self.thresholds['director_churn_months'] * 30)
        
        recent_resignations = []
        recent_appointments = []
        
        for officer in officers.get('items', []):
            if officer.get('resigned_on'):
                try:
                    resigned_date = datetime.strptime(officer['resigned_on'], '%Y-%m-%d')
                    if resigned_date >= cutoff_date:
                        recent_resignations.append(officer)
                except ValueError:
                    pass
            
            if officer.get('appointed_on'):
                try:
                    appointed_date = datetime.strptime(officer['appointed_on'], '%Y-%m-%d')
                    if appointed_date >= cutoff_date:
                        recent_appointments.append(officer)
                except ValueError:
                    pass
        
        total_changes = len(recent_resignations) + len(recent_appointments)
        threshold = self.thresholds['director_churn_count']
        
        if total_changes >= threshold:
            severity = 'Critical' if total_changes >= threshold * 2 else 'Elevated'
            
            narrative = f"The company has experienced {total_changes} director changes ({len(recent_resignations)} resignations, {len(recent_appointments)} appointments) in the past {self.thresholds['director_churn_months']} months. "
            
            if len(recent_resignations) >= 2:
                narrative += "Multiple resignations can indicate internal disputes, strategic disagreements, or concerns about the company's direction."
            
            findings.append({
                'category': 'Governance',
                'severity': severity,
                'title': 'High Director Turnover',
                'narrative': narrative,
                'recommendation': 'Request explanations for recent departures and assess the stability of the current management team. Consider whether institutional knowledge has been lost.'
            })
        
        # Check PSC stability
        pscs = self.company_data.get('pscs', {})
        if pscs and pscs.get('items'):
            ceased_pscs = [p for p in pscs['items'] if p.get('ceased_on')]
            
            if len(ceased_pscs) >= 2:
                findings.append({
                    'category': 'Governance',
                    'severity': 'Moderate',
                    'title': 'Changes in Ownership Control',
                    'narrative': f"The company has had {len(ceased_pscs)} changes in Persons with Significant Control, indicating shifts in ownership or control structure.",
                    'recommendation': 'Understand the reasons for ownership changes and assess the stability and commitment of current controllers.'
                })
        
        return findings
    
    def _check_revenue_trends(self):
        """Analyze revenue and profitability trends."""
        findings = []
        
        if not self.financial_analyzer or self.financial_analyzer.data.empty:
            return findings
        
        df = self.financial_analyzer.data.sort_values('Year')
        
        # Check for consecutive losses
        if 'ProfitLoss' in df.columns:
            consecutive_losses = 0
            loss_years = []
            
            for _, row in df.iterrows():
                if pd.notna(row['ProfitLoss']) and row['ProfitLoss'] < 0:
                    consecutive_losses += 1
                    loss_years.append(int(row['Year']))
                else:
                    if consecutive_losses >= self.thresholds['consecutive_loss_years']:
                        break
                    consecutive_losses = 0
                    loss_years = []
            
            if consecutive_losses >= self.thresholds['consecutive_loss_years']:
                total_losses = df[df['Year'].isin(loss_years)]['ProfitLoss'].sum()
                
                findings.append({
                    'category': 'Financial',
                    'severity': 'Elevated',
                    'title': 'Consecutive Trading Losses',
                    'narrative': f"The company has reported losses for {consecutive_losses} consecutive years ({', '.join(map(str, loss_years))}), totaling £{abs(total_losses):,.0f}. Sustained losses may indicate structural issues with the business model or market position.",
                    'recommendation': 'Request management\'s plan for returning to profitability, including specific actions and timelines. Assess whether the company has sufficient capital to sustain further losses.'
                })
        
        # Check revenue decline
        if 'Revenue' in df.columns and len(df) >= 2:
            df_with_revenue = df[df['Revenue'].notna()].copy()
            
            if len(df_with_revenue) >= 2:
                # Calculate growth rates
                df_with_revenue['Revenue_Growth'] = df_with_revenue['Revenue'].pct_change() * 100
                
                # Check for sustained decline
                recent_years = df_with_revenue.tail(self.thresholds['revenue_decline_years'])
                
                if all(recent_years['Revenue_Growth'] < self.thresholds['revenue_decline_pct']):
                    total_decline = ((recent_years.iloc[-1]['Revenue'] - recent_years.iloc[0]['Revenue']) / 
                                   recent_years.iloc[0]['Revenue']) * 100
                    
                    findings.append({
                        'category': 'Financial',
                        'severity': 'Elevated',
                        'title': 'Declining Revenue Trend',
                        'narrative': f"Revenue has declined for {self.thresholds['revenue_decline_years']} consecutive years, with a cumulative decline of {abs(total_decline):.1f}% from £{recent_years.iloc[0]['Revenue']:,.0f} to £{recent_years.iloc[-1]['Revenue']:,.0f}. Persistent revenue decline suggests loss of market share, customer attrition, or market contraction.",
                        'recommendation': 'Understand the drivers of revenue decline and assess management\'s strategy for stabilizing and growing the business.'
                    })
        
        return findings
    
    def _check_predictive_outlook(self):
        """Generate predictive outlook based on financial trends."""
        findings = []
        
        if not self.financial_analyzer or self.financial_analyzer.data.empty:
            log_message("Predictive outlook: No financial analyzer or empty data")
            return findings
        
        df = self.financial_analyzer.data.sort_values('Year')
        log_message(f"Predictive outlook: {len(df)} years of data, columns: {list(df.columns)}")
        
        # Need at least 2 years for meaningful predictions
        if len(df) < 2:
            log_message("Predictive outlook: Less than 2 years of data")
            available_metrics = [col for col in ['NetAssets', 'Revenue', 'ProfitLoss', 'CashBankInHand'] if col in df.columns]
            findings.append({
                'category': 'Financial',
                'severity': 'Moderate',
                'title': 'Insufficient Data for Projections',
                'narrative': f"Only {len(df)} year(s) of accounts data available. At least 2 years are required to generate meaningful financial projections. Available metrics: {', '.join(available_metrics) if available_metrics else 'None detected'}.",
                'recommendation': 'Obtain additional years of accounts or request management forecasts to assess financial trajectory.'
            })
            return findings
        
        latest_year = int(df['Year'].max())
        predictions = []
        concerns = []
        
        # Generate predictions for key metrics
        metrics_to_predict = [
            ('NetAssets', 'Net Assets', 'solvency'),
            ('Revenue', 'Revenue', 'growth'),
            ('ProfitLoss', 'Profit/Loss', 'profitability'),
            ('CashBankInHand', 'Cash Position', 'liquidity'),
        ]
        
        for metric, display_name, category in metrics_to_predict:
            if metric not in df.columns:
                log_message(f"Predictive outlook: {metric} not in columns, skipping")
                continue
            
            # Use linear regression for prediction
            prediction = self.financial_analyzer.predict_next_year(metric, method='linear')
            
            if not prediction:
                log_message(f"Predictive outlook: {metric} prediction returned empty")
                continue
            
            log_message(f"Predictive outlook: {metric} predicted {prediction['predicted_value']} for {prediction['next_year']}")
            
            predicted_value = prediction['predicted_value']
            next_year = prediction['next_year']
            
            # Get the most recent actual value for comparison
            recent_values = df[df[metric].notna()][metric]
            if recent_values.empty:
                continue
            
            last_actual = recent_values.iloc[-1]
            
            # Calculate predicted change
            if last_actual != 0:
                pct_change = ((predicted_value - last_actual) / abs(last_actual)) * 100
            else:
                pct_change = 0
            
            predictions.append({
                'metric': display_name,
                'last_actual': last_actual,
                'predicted': predicted_value,
                'next_year': next_year,
                'pct_change': pct_change,
                'category': category
            })
            
            # Flag specific concerns
            if metric == 'NetAssets' and predicted_value < 0 and last_actual >= 0:
                concerns.append(f"Net assets projected to turn negative (£{predicted_value:,.0f}) in {next_year}, indicating potential balance sheet insolvency")
            
            if metric == 'ProfitLoss' and predicted_value < 0 and pct_change < -self.thresholds['predictive_profit_decline_pct']:
                concerns.append(f"Losses projected to worsen to £{abs(predicted_value):,.0f} in {next_year}")

            if metric == 'CashBankInHand' and predicted_value < 0:
                concerns.append(f"Cash position projected to turn negative in {next_year}, suggesting potential cash flow crisis")

            if metric == 'Revenue' and pct_change < -self.thresholds['predictive_revenue_decline_pct']:
                concerns.append(f"Revenue projected to decline by {abs(pct_change):.0f}% to £{predicted_value:,.0f} in {next_year}")
        
        # Generate findings based on predictions
        if concerns:
            findings.append({
                'category': 'Financial',
                'severity': 'Elevated',
                'title': 'Concerning Financial Trajectory',
                'narrative': f"Based on linear extrapolation of the last {len(df)} years of filed accounts, the following concerns are projected for {predictions[0]['next_year'] if predictions else latest_year + 1}: " + "; ".join(concerns) + ". Note: These projections assume historical trends continue unchanged and do not account for management actions, market changes, or other factors.",
                'recommendation': 'Request current management accounts and forward-looking cash flow forecasts. Assess whether management has plans to address the projected trajectory.'
            })
        # Fallback: If we have financial data but couldn't generate predictions
        if not predictions and not findings:
            available_metrics = [col for col in ['NetAssets', 'Revenue', 'ProfitLoss', 'CashBankInHand'] if col in df.columns]

            if not available_metrics:
                findings.append({
                    'category': 'Financial',
                    'severity': 'Moderate',
                    'title': 'Limited Financial Disclosure',
                    'narrative': "The uploaded accounts do not contain the standard financial metrics (Net Assets, Revenue, Profit/Loss, Cash) in a format that could be extracted. This may indicate micro-entity or heavily abbreviated accounts.",
                    'recommendation': 'Request full statutory accounts or management accounts for a complete financial picture.'
                })
        
        log_message(f"Predictive outlook: Returning {len(findings)} findings, {len(predictions)} predictions generated")
        return findings
    
    def _check_default_address(self):
        """Check for Companies House default address or PO Box."""
        findings = []
        
        profile = self.company_data['profile']
        addr = profile.get('registered_office_address', {})
        
        # Build full address string
        full_address = ' '.join(filter(None, [
            addr.get('address_line_1', ''),
            addr.get('address_line_2', ''),
            addr.get('locality', ''),
            addr.get('postal_code', '')
        ])).lower()
        
        # Known default addresses
        default_addresses = [
            'companies house',
            'crown way',
            'cf14 3uz',
            'companies house default address',
            'cf14 8lh',
            'po box 4385',
        ]
        
        if any(default in full_address for default in default_addresses):
            findings.append({
                'category': 'Governance',
                'severity': 'Moderate',
                'title': 'Companies House Default Address',
                'narrative': "The registered address appears to be the Companies House default address. This may indicate the company does not have a permanent trading address or office.",
                'recommendation': 'Request the actual trading address and verify the company has a physical presence.'
            })
        
        # Check for PO Box
        if 'po box' in full_address or 'p.o. box' in full_address:
            findings.append({
                'category': 'Governance',
                'severity': 'Moderate',
                'title': 'PO Box Registered Address',
                'narrative': "The registered address is a PO Box, which provides limited ability to verify a physical business presence.",
                'recommendation': 'Request the actual trading address and consider a site visit if material amounts are at stake.'
            })
        
        return findings
    
    def _check_accounting_changes(self):
        """Check for changes in accounting reference date or filing category."""
        findings = []
        
        profile = self.company_data['profile']
        
        # Check for accounting reference date changes
        # Note: The API doesn't directly show historical changes, so this is limited
        if profile.get('accounts', {}).get('accounting_reference_date'):
            ard = profile['accounts']['accounting_reference_date']
            # You'd need to compare against historical data if you have it stored
            # For now, just note it in the report
            pass
        
        # Check for filing category changes (if accounts loaded)
        if self.financial_analyzer and not self.financial_analyzer.data.empty:
            df = self.financial_analyzer.data.sort_values('Year')
            
            if 'accounts_type' in df.columns:
                # Look for downgrades
                filing_types = df[df['accounts_type'].notna()]['accounts_type'].tolist()
                
                # Define hierarchy (higher number = more transparent)
                type_hierarchy = {
                    'full': 4,
                    'small': 3,
                    'micro': 2,
                    'abridged': 1
                }
                
                for i in range(1, len(filing_types)):
                    prev_type = filing_types[i-1].lower()
                    curr_type = filing_types[i].lower()
                    
                    prev_level = type_hierarchy.get(prev_type, 0)
                    curr_level = type_hierarchy.get(curr_type, 0)
                    
                    if curr_level < prev_level and prev_level > 0 and curr_level > 0:
                        findings.append({
                            'category': 'Financial',
                            'severity': 'Moderate',
                            'title': 'Downgrade in Accounts Filing Category',
                            'narrative': f"The company moved from filing '{prev_type}' accounts to '{curr_type}' accounts. This reduction in disclosure may indicate a shrinking business or reduced transparency.",
                            'recommendation': 'Request full financial statements to understand the complete financial position.'
                        })
                        break
        
        return findings
    
    def _check_offshore_pscs(self):
        """Check for PSCs in offshore jurisdictions."""
        findings = []
        
        # List of common tax havens / low-transparency jurisdictions
        offshore_jurisdictions = [
            'jersey', 'guernsey', 'isle of man',
            'british virgin islands', 'bvi', 'cayman islands',
            'bermuda', 'bahamas', 'panama', 'seychelles',
            'gibraltar', 'malta', 'cyprus', 'luxembourg',
            'liechtenstein', 'monaco', 'andorra'
        ]
        
        pscs = self.company_data.get('pscs', {})
        if not pscs or not pscs.get('items'):
            return findings
        
        offshore_pscs = []
        for psc in pscs['items']:
            if psc.get('ceased_on'):
                continue
            
            country = (psc.get('country_of_residence') or '').lower()
            reg_country = (psc.get('identification', {}).get('country_registered') or '').lower()
            
            if any(jurisdiction in country or jurisdiction in reg_country 
                   for jurisdiction in offshore_jurisdictions):
                offshore_pscs.append({
                    'name': psc.get('name'),
                    'jurisdiction': country or reg_country
                })
        
        if offshore_pscs:
            severity = 'Elevated' if len(offshore_pscs) > 1 else 'Moderate'
            
            psc_list = ', '.join([f"{p['name']} ({p['jurisdiction']})" for p in offshore_pscs[:3]])
            
            findings.append({
                'category': 'Governance',
                'severity': severity,
                'title': 'Offshore Ultimate Controllers',
                'narrative': f"The company has {len(offshore_pscs)} Person(s) with Significant Control registered in jurisdictions with limited transparency: {psc_list}. While not inherently problematic, this structure can complicate due diligence on ultimate beneficial ownership.",
                'recommendation': 'Request additional documentation on ultimate beneficial owners and the corporate structure. Consider enhanced monitoring requirements.'
            })
        
        return findings
    
    def _check_director_insolvency_history(self):
        """Check if directors have history with insolvent companies (Tier 3 - expensive)."""
        findings = []
        
        officers = self.company_data.get('officers', {})
        if not officers or not officers.get('items'):
            return findings
        
        # Only check current, active directors
        active_officers = [o for o in officers['items'] if not o.get('resigned_on')]
        
        directors_with_issues = []
        
        for i, officer in enumerate(active_officers):
            if self.cancel_flag.is_set():
                break
            
            self.safe_update(
                self.status_var.set,
                f"Checking director history {i+1}/{len(active_officers)}..."
            )
            
            # Get all appointments for this director
            appointments_link = officer.get('links', {}).get('officer', {}).get('appointments')
            if not appointments_link:
                continue
            
            all_director_appointments = self._fetch_all_appointments(appointments_link)
            if not all_director_appointments:
                continue
            
            # Check each company they're/were involved with
            insolvent_companies = []
            
            for appointment in all_director_appointments:
                company_status = appointment.get('appointed_to', {}).get('company_status', '').lower()
                company_name = appointment.get('appointed_to', {}).get('company_name', '')
                
                if any(term in company_status for term in ['liquidation', 'dissolved', 'administration']):
                    insolvent_companies.append(company_name)
            
            if len(insolvent_companies) >= self.thresholds['insolvency_company_count']:
                directors_with_issues.append({
                    'name': officer.get('name'),
                    'count': len(insolvent_companies),
                    'examples': insolvent_companies[:3]
                })
        
        if directors_with_issues:
            severity = 'Critical' if any(d['count'] >= self.thresholds['insolvency_critical_count'] for d in directors_with_issues) else 'Elevated'
            
            details = []
            for director in directors_with_issues[:3]:  # Show top 3
                examples = ', '.join(director['examples'])
                details.append(f"{director['name']}: associated with {director['count']} insolvent companies including {examples}")
            
            narrative = "The following director(s) have been associated with multiple companies that entered insolvency:\n\n" + '\n'.join(details)
            narrative += "\n\nWhile business failures can occur for legitimate reasons, a pattern of multiple insolvencies may indicate elevated risk or poor business judgment."
            
            findings.append({
                'category': 'Governance',
                'severity': severity,
                'title': 'Directors with Insolvency History',
                'narrative': narrative,
                'recommendation': 'Conduct enhanced due diligence on the circumstances of previous company failures. Consider requiring personal guarantees or additional security.'
            })
        
        return findings
    
    def _normalise_company_name_for_comparison(self, name):
        """Strip legal suffixes and generic terms to get distinctive company name."""
        if not name:
            return ""
        
        name = name.lower().strip()
        
        # Legal suffixes to remove (order matters - longer first)
        legal_suffixes = [
            'public limited company', 'private limited company',
            'community interest company', 'charitable incorporated organisation',
            'limited liability partnership', 'limited partnership',
            'limited', 'ltd', 'plc', 'llp', 'lp', 'cic', 'cio', 'inc', 'corp'
        ]
        
        # Generic business words that don't indicate distinctiveness
        generic_terms = {
            'services', 'solutions', 'group', 'holdings', 'uk', 'gb', 'international',
            'consulting', 'consultants', 'management', 'associates', 'partners',
            'enterprises', 'ventures', 'trading', 'company', 'co', '&', 'and', 'the'
        }
        
        # Remove legal suffixes
        for suffix in legal_suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)].strip()
                break  # Only remove one suffix
        
        # Remove generic terms
        words = name.split()
        distinctive_words = [w for w in words if w not in generic_terms]
        
        # If we've stripped everything, fall back to original words minus suffix
        if not distinctive_words:
            distinctive_words = words
        
        return ' '.join(distinctive_words).strip()

    def _check_phoenix_companies(self):
        """Check for phoenix company patterns (similar names to dissolved companies)."""
        findings = []
        
        officers = self.company_data.get('officers', {})
        if not officers or not officers.get('items'):
            return findings
        
        current_company_name = self.company_data['profile'].get('company_name', '')
        current_company_number = self.company_data['profile'].get('company_number', '')
        current_normalised = self._normalise_company_name_for_comparison(current_company_name)
        
        # Skip if we can't extract a distinctive name
        if not current_normalised:
            return findings
        
        # Get appointments for key directors
        similar_dissolved_companies = []
        
        for officer in officers['items'][:self.thresholds['phoenix_officer_count']]:
            if self.cancel_flag.is_set():
                break
            
            appointments_link = officer.get('links', {}).get('officer', {}).get('appointments')
            if not appointments_link:
                continue
            
            all_director_appointments = self._fetch_all_appointments(appointments_link)
            if not all_director_appointments:
                continue

            for appointment in all_director_appointments:
                company_number = appointment.get('appointed_to', {}).get('company_number', '')
                if company_number == current_company_number:
                    continue
                company_status = appointment.get('appointed_to', {}).get('company_status', '').lower()
                company_name = appointment.get('appointed_to', {}).get('company_name', '')
                
                if 'dissolved' in company_status or 'liquidation' in company_status:
                    # Normalise the dissolved company name and compare
                    company_normalised = self._normalise_company_name_for_comparison(company_name)
                    
                    # Only compare if we have distinctive content in both names
                    if company_normalised:
                        similarity = WRatio(current_normalised, company_normalised)
                        
                        if similarity >= self.thresholds['phoenix_similarity_pct']:
                            similar_dissolved_companies.append({
                                'name': company_name,
                                'name_normalised': company_normalised,
                                'similarity': round(similarity),
                                'status': company_status
                            })
        
        if similar_dissolved_companies:
            # Deduplicate by company name
            seen_names = set()
            unique_matches = []
            for c in similar_dissolved_companies:
                if c['name'] not in seen_names:
                    seen_names.add(c['name'])
                    unique_matches.append(c)
            
            examples_text = ', '.join([
                f"{c['name']} ({c['similarity']}% match on '{c['name_normalised']}')" 
                for c in unique_matches[:3]
            ])
            
            findings.append({
                'category': 'Governance',
                'severity': 'Critical',
                'title': 'Potential Phoenix Company Pattern',
                'narrative': f"Directors of this company have previously been involved with {len(unique_matches)} dissolved/insolvent companies with similar distinctive names (after removing generic terms like 'Limited', 'Services', etc.). This pattern may indicate 'phoenixing' - the practice of abandoning a company with debts and starting a new one with a similar name and business. Matches found: {examples_text}.",
                'recommendation': 'This is a serious red flag. Conduct thorough investigations into the circumstances of the previous company failures and the transfer of any assets or business. Seek legal advice before proceeding.'
            })
        
        return findings
    
    def _build_report_html(self, findings):
        """Generate HTML report from findings."""
        profile = self.company_data['profile']
        company_name = html.escape(profile.get('company_name', 'Unknown Company'))
        company_number = html.escape(profile.get('company_number', 'N/A'))
        
        # Categorize findings
        critical = [f for f in findings if f['severity'] == 'Critical']
        elevated = [f for f in findings if f['severity'] == 'Elevated']
        moderate = [f for f in findings if f['severity'] == 'Moderate']
        positive = [f for f in findings if f['severity'] == 'Positive']
        
        # Generate charts if accounts loaded
        chart_html = ""
        if self.financial_analyzer and not self.financial_analyzer.data.empty:
            chart_html = self._generate_chart_html()
        
        # Build HTML
        html_output = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Due Diligence Report - {company_name}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            max-width: 1200px;
            margin: 20px auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
        }}
        .header h1 {{
            margin: 0 0 10px 0;
        }}
        .header p {{
            margin: 5px 0;
            opacity: 0.9;
        }}
        .section {{
            background: white;
            padding: 25px;
            margin-bottom: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .section h2 {{
            color: #333;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
            margin-top: 0;
        }}
        .finding {{
            margin: 20px 0;
            padding: 15px;
            border-left: 4px solid #ccc;
            background: #f9f9f9;
        }}
        .finding.critical {{
            border-left-color: #dc3545;
            background: #fff5f5;
        }}
        .finding.elevated {{
            border-left-color: #fd7e14;
            background: #fff9f5;
        }}
        .finding.moderate {{
            border-left-color: #ffc107;
            background: #fffef5;
        }}
        .finding.positive {{
            border-left-color: #28a745;
            background: #f5fff5;
        }}
        .finding h3 {{
            margin: 0 0 10px 0;
            color: #333;
        }}
        .finding .severity {{
            display: inline-block;
            padding: 3px 10px;
            border-radius: 3px;
            font-size: 12px;
            font-weight: bold;
            margin-right: 10px;
        }}
        .severity.critical {{ background: #dc3545; color: white; }}
        .severity.elevated {{ background: #fd7e14; color: white; }}
        .severity.moderate {{ background: #ffc107; color: #333; }}
        .severity.positive {{ background: #28a745; color: white; }}
        .recommendation {{
            margin-top: 10px;
            padding: 10px;
            background: white;
            border-left: 3px solid #667eea;
            font-style: italic;
        }}
        .company-profile {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 15px;
        }}
        .profile-item {{
            padding: 10px;
            background: #f9f9f9;
            border-radius: 5px;
        }}
        .profile-item strong {{
            display: block;
            color: #667eea;
            margin-bottom: 5px;
        }}
        .executive-summary {{
            font-size: 16px;
            line-height: 1.6;
            padding: 20px;
            background: #f0f4ff;
            border-radius: 5px;
            border-left: 4px solid #667eea;
        }}
        .chart-container {{
            margin: 20px 0;
            text-align: center;
        }}
        .chart-container img {{
            max-width: 100%;
            border: 1px solid #ddd;
            border-radius: 5px;
        }}
        .grants-summary {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 15px;
            margin: 20px 0;
        }}
        .grants-stat {{
            padding: 15px;
            background: #f0f4ff;
            border-radius: 5px;
            text-align: center;
            font-size: 18px;
        }}
        .grants-stat strong {{
            display: block;
            color: #667eea;
            font-size: 12px;
            margin-bottom: 5px;
            text-transform: uppercase;
        }}
        .grants-table {{
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
            font-size: 13px;
        }}
        .grants-table th {{
            background: #667eea;
            color: white;
            padding: 10px;
            text-align: left;
        }}
        .grants-table td {{
            padding: 8px 10px;
            border-bottom: 1px solid #eee;
        }}
        .grants-table tr:hover td {{
            background: #f5f7ff;
        }}
        .cross-analysis-summary {{
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
        }}
        .cross-analysis-summary th {{
            background: #667eea;
            color: white;
            padding: 10px;
            text-align: left;
            font-size: 13px;
        }}
        .cross-analysis-summary td {{
            padding: 8px 10px;
            border-bottom: 1px solid #eee;
            font-size: 13px;
        }}
        .risk-high {{ background: #dc3545; color: white; padding: 3px 10px; border-radius: 3px; font-size: 12px; font-weight: bold; display: inline-block; }}
        .risk-medium {{ background: #ffc107; color: #333; padding: 3px 10px; border-radius: 3px; font-size: 12px; font-weight: bold; display: inline-block; }}
        .risk-low {{ background: #28a745; color: white; padding: 3px 10px; border-radius: 3px; font-size: 12px; font-weight: bold; display: inline-block; }}
        .risk-not-assessed {{ background: #6c757d; color: white; padding: 3px 10px; border-radius: 3px; font-size: 12px; font-weight: bold; display: inline-block; }}
        .confidence-auto {{ background: #667eea; color: white; padding: 2px 8px; border-radius: 3px; font-size: 11px; display: inline-block; }}
        .confidence-enriched {{ background: #28a745; color: white; padding: 2px 8px; border-radius: 3px; font-size: 11px; display: inline-block; }}
        .confidence-limited {{ background: #fd7e14; color: white; padding: 2px 8px; border-radius: 3px; font-size: 11px; display: inline-block; }}
        .confidence-skipped {{ background: #6c757d; color: white; padding: 2px 8px; border-radius: 3px; font-size: 11px; display: inline-block; }}
        .composite-warning {{
            background: #fff5f5;
            border: 2px solid #dc3545;
            border-radius: 8px;
            padding: 15px;
            margin: 15px 0;
            font-weight: bold;
            color: #dc3545;
        }}
        .pattern-warning {{
            background: #fff9f5;
            border: 2px solid #fd7e14;
            border-radius: 8px;
            padding: 15px;
            margin: 15px 0;
            font-weight: bold;
            color: #856404;
        }}
        .trend-table {{
            width: auto;
            border-collapse: collapse;
            margin: 10px 0;
            font-size: 12px;
        }}
        .trend-table th {{
            background: #f0f4ff;
            padding: 6px 12px;
            text-align: right;
            border: 1px solid #ddd;
        }}
        .trend-table td {{
            padding: 6px 12px;
            text-align: right;
            border: 1px solid #eee;
        }}
        .cross-rule-card {{
            margin: 20px 0;
            padding: 15px;
            border-left: 4px solid #ccc;
            background: #f9f9f9;
        }}
        .cross-rule-card.high {{ border-left-color: #dc3545; background: #fff5f5; }}
        .cross-rule-card.medium {{ border-left-color: #ffc107; background: #fffef5; }}
        .cross-rule-card.low {{ border-left-color: #28a745; background: #f5fff5; }}
        .cross-rule-card.not-assessed {{ border-left-color: #6c757d; background: #f9f9f9; }}
        .rule-id-badge {{
            display: inline-block;
            background: #667eea;
            color: white;
            padding: 2px 8px;
            border-radius: 3px;
            font-size: 12px;
            font-weight: bold;
            margin-right: 8px;
        }}
        .quality-caveat {{
            background: #fff3cd;
            border: 1px solid #ffc107;
            border-radius: 5px;
            padding: 10px;
            margin: 10px 0;
            font-size: 13px;
        }}
        .grant-detail {{
            margin: 15px 0;
            padding: 15px;
            background: #f9f9f9;
            border-left: 3px solid #667eea;
            border-radius: 0 5px 5px 0;
        }}
        .grant-detail h4 {{
            margin: 0 0 5px 0;
            color: #333;
        }}
        .grant-meta {{
            font-size: 12px;
            color: #666;
            margin: 0 0 10px 0;
        }}
        @media print {{
            body {{ background: white; }}
            .section {{ box-shadow: none; border: 1px solid #ddd; }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Enhanced Due Diligence Report</h1>
        <p><strong>{company_name}</strong></p>
        <p>Company Number: {company_number}</p>
        <p>Report Generated: {datetime.now().strftime('%d %B %Y at %H:%M')}</p>
    </div>
    
    <div class="section">
        <h2>1. Executive Summary</h2>
        <div class="executive-summary">
            {self._generate_executive_summary(critical, elevated, moderate, company_name)}
        </div>
    </div>
    
    <div class="section">
        <h2>2. Company Profile</h2>
        {self._generate_company_profile_html()}
    </div>

    {self._generate_timeline_section()}

    {self._generate_ownership_section()}

    {self._generate_findings_section('Critical Risk Indicators', critical)}
    {self._generate_findings_section('Elevated Risk Indicators', elevated)}
    {self._generate_findings_section('Moderate Risk Indicators', moderate)}

    {chart_html}

    {self._generate_grants_section()}

    {self._generate_cross_analysis_section()}

    {self._generate_positive_indicators_html(positive)}

    <div class="section">
        <h2>Data Limitations & Disclaimers</h2>
        {self._generate_limitations_html()}
    </div>
    
    <div class="section" style="background: #f0f4ff; text-align: center;">
        <p style="margin: 0; color: #666;">
            Report generated by Data Investigation Multi-Tool<br>
            This report is based on publicly available information and should not be the sole basis for decision-making.
        </p>
    </div>
</body>
</html>
"""
        return html_output
    
    def _generate_executive_summary(self, critical, elevated, moderate, company_name):
        """Generate plain English executive summary."""
        total_concerns = len(critical) + len(elevated) + len(moderate)

        if total_concerns == 0:
            summary = f"Based on the analysis performed, {html.escape(company_name)} shows no significant risk indicators in the areas examined. However, this assessment is based on available public information and should be supplemented with additional due diligence as appropriate for your specific requirements."
        else:
            summary = f"Based on available data, {company_name} shows <strong>{total_concerns} concerning indicator(s)</strong> that warrant further investigation"

            if critical:
                key_issues = [f['title'] for f in critical[:2]]
                summary += f", particularly around: <strong>{', '.join(key_issues)}</strong>"

            summary += ".<br><br>"

            if critical:
                summary += f"<strong>Critical findings ({len(critical)}):</strong> These are severe red flags that require immediate attention and may indicate the company is unsuitable for the intended transaction or relationship.<br><br>"

            if elevated:
                summary += f"<strong>Elevated risk findings ({len(elevated)}):</strong> These indicators suggest heightened risk that should be investigated further before proceeding.<br><br>"

            if moderate:
                summary += f"<strong>Moderate concerns ({len(moderate)}):</strong> These factors should be considered and may require additional information or monitoring."

        # Cross-analysis summary
        report = getattr(self, '_cross_analysis_report', None)
        if report:
            high_count = sum(1 for r in report.results if r.risk_flag == 'HIGH')
            medium_count = sum(1 for r in report.results if r.risk_flag == 'MEDIUM')
            assessed = sum(1 for r in report.results if r.risk_flag != 'NOT_ASSESSED' and r.confidence != 'SKIPPED')
            if assessed > 0:
                summary += f"<br><br><strong>Cross-analysis ({assessed} rules assessed):</strong> "
                if high_count > 0:
                    summary += f"{high_count} high-risk and {medium_count} medium-risk indicator(s) identified. "
                elif medium_count > 0:
                    summary += f"{medium_count} medium-risk indicator(s) identified. "
                else:
                    summary += "No high or medium-risk indicators identified. "
                summary += "See the Financial &amp; Grant Cross-Analysis section for details."

        return summary
    
    def _generate_company_profile_html(self):
        """Generate company profile section."""
        profile = self.company_data['profile']
        
        addr = profile.get('registered_office_address', {})
        address = ', '.join(filter(None, [
            addr.get('address_line_1'),
            addr.get('locality'),
            addr.get('postal_code')
        ]))
        
        officers = self.company_data.get('officers', {})
        active_officers = len([o for o in officers.get('items', []) if not o.get('resigned_on')])
        
        pscs = self.company_data.get('pscs', {})
        active_pscs = len([p for p in pscs.get('items', []) if not p.get('ceased_on')])
        
        html_output = '<div class="company-profile">'
        
        profile_items = [
            ('Company Name', html.escape(profile.get('company_name', 'N/A'))),
            ('Company Number', html.escape(profile.get('company_number', 'N/A'))),
            ('Status', html.escape(profile.get('company_status', 'N/A'))),
            ('Type', html.escape(profile.get('type', 'N/A'))),
            ('Incorporated', html.escape(format_display_date(profile.get('date_of_creation', '')))),
            ('Jurisdiction', html.escape(profile.get('jurisdiction', 'N/A'))),
            ('Registered Address', html.escape(address)),
            ('Active Officers', str(active_officers)),
            ('Active PSCs', str(active_pscs)),
        ]
        
        for label, value in profile_items:
            html_output += f'<div class="profile-item"><strong>{label}</strong>{value}</div>'

        # Previous company names — oldest first (ascending by ceased_on date)
        previous_names = profile.get('previous_company_names', [])
        if previous_names:
            sorted_names = sorted(previous_names, key=lambda p: p.get('ceased_on', '') or '')
            names_html = '; '.join(
                html.escape(f"{p.get('name', 'Unknown')} (until {format_display_date(p.get('ceased_on', ''))})")
                for p in sorted_names
            )
            html_output += f'<div class="profile-item"><strong>Previous Names</strong>{names_html}</div>'

        html_output += '</div>'
        return html_output

    def _generate_findings_section(self, title, findings):
        """Generate HTML for a findings section."""
        if not findings:
            return ""
        
        severity_class = findings[0]['severity'].lower()
        
        html_output = f'<div class="section"><h2>{len(findings)}. {title}</h2>'
        
        for finding in findings:
            html_output += f'''
            <div class="finding {severity_class}">
                <h3>
                    <span class="severity {severity_class}">{finding['severity'].upper()}</span>
                    {html.escape(finding['title'])}
                </h3>
                <p>{html.escape(finding['narrative'])}</p>
                <div class="recommendation">
                    <strong>Recommendation:</strong> {html.escape(finding['recommendation'])}
                </div>
            </div>
            '''
        
        html_output += '</div>'
        return html_output

    def _check_director_psc_addresses(self):
        """Check if any directors or PSCs have Companies House default address."""
        findings = []
        
        # Known default/suspicious addresses
        default_addresses = [
            'companies house',
            'crown way',
            'cf14 3uz',
            'cf14 8lh',
            'po box 4385',
            'default address',
        ]
        
        officers = self.company_data.get('officers', {})
        if officers and officers.get('items'):
            officers_with_default = []
            
            for officer in officers['items']:
                if officer.get('resigned_on'):
                    continue  # Skip resigned officers
                addr = officer.get('address', {})
                full_address = ' '.join(filter(None, [
                    addr.get('address_line_1', ''),
                    addr.get('address_line_2', ''),
                    addr.get('locality', ''),
                    addr.get('postal_code', '')
                ])).lower()
                if any(default in full_address for default in default_addresses):
                    officers_with_default.append(officer.get('name', 'Unknown'))
            
            if officers_with_default:
                findings.append({
                    'category': 'Governance',
                    'severity': 'Moderate',
                    'title': 'Directors Using Default Address',
                    'narrative': f"The following director(s) have the Companies House default address listed: {', '.join(officers_with_default)}. This may indicate the individual does not have a stable residential address or is attempting to obscure their location.",
                    'recommendation': 'Request proof of actual residential addresses for verification purposes.'
                })
        
        # Check PSCs
        pscs = self.company_data.get('pscs', {})
        if pscs and pscs.get('items'):
            pscs_with_default = []
            
            for psc in pscs['items']:
                if psc.get('ceased_on'):
                    continue  # Skip ceased PSCs
                
                addr = psc.get('address', {})
                full_address = ' '.join(filter(None, [
                    addr.get('address_line_1', ''),
                    addr.get('address_line_2', ''),
                    addr.get('locality', ''),
                    addr.get('postal_code', '')
                ])).lower()
                
                if any(default in full_address for default in default_addresses):
                    pscs_with_default.append(psc.get('name', 'Unknown'))
            
            if pscs_with_default:
                findings.append({
                    'category': 'Governance',
                    'severity': 'Moderate',
                    'title': 'PSCs Using Default Address',
                    'narrative': f"The following Person(s) with Significant Control have the Companies House default address listed: {', '.join(pscs_with_default)}. This reduces transparency regarding the beneficial owner's actual location.",
                    'recommendation': 'Request verified residential addresses for all PSCs.'
                })
        
        return findings
    
    def _generate_positive_indicators_html(self, positive_findings):
        """Generate section for positive indicators."""
        # Even if no specific positive findings, generate based on absence of negative ones
        html_output = '<div class="section"><h2>Positive Indicators</h2>'
        
        if positive_findings:
            for finding in positive_findings:
                html_output += f'''
                <div class="finding positive">
                    <h3>{finding['title']}</h3>
                    <p>{finding['narrative']}</p>
                </div>
                '''
        else:
            # Generate generic positive notes based on checks performed
            positives = []
            
            if self.check_vars['filing_status'].get():
                compliance_findings = self._check_filing_compliance()
                pattern_findings = self._check_filing_patterns()
                filing_history = self.company_data.get('filing_history', {})
                has_strikeoff = any(
                    'GAZ1' in filing.get('type', '') or 'DISS' in filing.get('type', '')
                    for filing in filing_history.get('items', [])
                )
                has_late_filing = (
                    any('late' in f['title'].lower() or 'overdue' in f['title'].lower()
                        for f in compliance_findings)
                    or any('late' in f['title'].lower() for f in pattern_findings)
                )
                if not has_late_filing and not has_strikeoff:
                    positives.append("The company has maintained timely filing compliance with Companies House with no late filings or strike-off notifications on record.")
            
            if self.accounts_loaded and self.check_vars['solvency'].get():
                df = self.financial_analyzer.data.sort_values('Year')
                latest = df.iloc[-1]
                if 'NetAssets' in latest and latest['NetAssets'] > 0:
                    positives.append(f"The company maintains a positive net asset position of £{latest['NetAssets']:,.0f}.")
            
            if positives:
                html_output += '<p>' + '<br><br>'.join(positives) + '</p>'
            else:
                html_output += '<p>No specific positive indicators were identified in the analysis performed. This does not indicate problems, but rather reflects the focus of due diligence on identifying risks.</p>'
        
        html_output += '</div>'
        return html_output
    
    def _generate_chart_html(self):
        """Generate embedded charts from financial data."""
        if not self.financial_analyzer or self.financial_analyzer.data.empty:
            return ""
        
        html_output = '<div class="section"><h2>Financial Analysis Charts</h2>'

        try:
            df = self.financial_analyzer.data.sort_values('Year')
        
            # Revenue & Profit chart
            if 'Revenue' in df.columns or 'ProfitLoss' in df.columns:
                html_output += '''
                <h3>Revenue & Profitability</h3>
                <p>This chart shows the company's revenue and profit/loss trends over time. Consistent growth in revenue 
                with positive profitability indicates a healthy, expanding business. Declining revenue or sustained losses 
                may signal operational difficulties or market challenges.</p>
                '''
                fig, ax = plt.subplots(figsize=(10, 5))
                
                if 'Revenue' in df.columns:
                    revenue_data = df[['Year', 'Revenue']].dropna()
                    ax.plot(revenue_data['Year'], revenue_data['Revenue'], 
                           marker='o', label='Revenue', linewidth=2)
                
                if 'ProfitLoss' in df.columns:
                    profit_data = df[['Year', 'ProfitLoss']].dropna()
                    ax.plot(profit_data['Year'], profit_data['ProfitLoss'], 
                           marker='s', label='Profit/Loss', linewidth=2)
                
                ax.set_xlabel('Year')
                ax.set_ylabel('£')
                ax.set_title('Revenue & Profitability Trend')
                ax.legend()
                ax.grid(True, alpha=0.3)

                from matplotlib.ticker import MaxNLocator
                ax.xaxis.set_major_locator(MaxNLocator(integer=True))
                ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'£{x:,.0f}'))
                
                # Convert to base64
                buffer = BytesIO()
                plt.tight_layout()
                plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
                buffer.seek(0)
                image_base64 = base64.b64encode(buffer.getvalue()).decode()
                plt.close()
                
                html_output += f'<div class="chart-container"><img src="data:image/png;base64,{image_base64}" alt="Revenue and Profit Chart"></div>'
            
            # Liquidity ratios chart
            df_ratios = self.financial_analyzer.calculate_ratios()
            if not df_ratios.empty and 'CurrentRatio' in df_ratios.columns:
                html_output += '''
                <h3>Current Ratio (Liquidity)</h3>
                <p><strong>What it measures:</strong> The current ratio shows the company's ability to pay short-term obligations. 
                It is calculated as current assets ÷ current liabilities.</p>
                <p><strong>Why it matters:</strong> A ratio below 1.0 (the red line) indicates the company may struggle to pay 
                its debts as they fall due. A ratio above 1.5 suggests healthy liquidity. Declining trends are concerning even 
                if the absolute ratio remains acceptable.</p>
                '''
                fig, ax = plt.subplots(figsize=(10, 5))
                
                ratio_data = df_ratios[['Year', 'CurrentRatio']].dropna()
                if not ratio_data.empty:
                    ax.plot(ratio_data['Year'], ratio_data['CurrentRatio'], 
                           marker='o', label='Current Ratio', linewidth=2, color='#667eea')
                    ax.axhline(y=1.0, color='red', linestyle='--', alpha=0.5, label='Critical Threshold (1.0)')
                    ax.set_xlabel('Year')
                    ax.set_ylabel('Ratio')
                    ax.set_title('Current Ratio Trend')
                    ax.legend()
                    ax.grid(True, alpha=0.3)
                    from matplotlib.ticker import MaxNLocator
                    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
                    buffer = BytesIO()
                    plt.tight_layout()
                    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
                    buffer.seek(0)
                    image_base64 = base64.b64encode(buffer.getvalue()).decode()
                    plt.close()
                    
                    html_output += f'<div class="chart-container"><img src="data:image/png;base64,{image_base64}" alt="Current Ratio Chart"></div>'

            # Net Assets Trend
            if 'NetAssets' in df.columns:
                html_output += '''
                <h3>Net Assets</h3>
                <p><strong>What it measures:</strong> Net assets (also called shareholder equity) represent the company's total 
                assets minus total liabilities - essentially what the company "owns" after all debts are paid.</p>
                <p><strong>Why it matters:</strong> Positive and growing net assets indicate financial stability and value creation. 
                Negative net assets (technical insolvency) or declining trends suggest the company is eroding shareholder value.</p>
                '''
                
                fig, ax = plt.subplots(figsize=(10, 5))
                net_assets_data = df[['Year', 'NetAssets']].dropna()
                
                if not net_assets_data.empty:
                    ax.plot(net_assets_data['Year'], net_assets_data['NetAssets'], 
                           marker='o', linewidth=2, color='#28a745')
                    ax.axhline(y=0, color='red', linestyle='--', alpha=0.5, label='Break-even (0)')
                    ax.set_xlabel('Year')
                    ax.set_ylabel('£')
                    ax.set_title('Net Assets Trend')
                    ax.legend()
                    ax.grid(True, alpha=0.3)
                    from matplotlib.ticker import MaxNLocator
                    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
                    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'£{x:,.0f}'))
                    
                    buffer = BytesIO()
                    plt.tight_layout()
                    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
                    buffer.seek(0)
                    image_base64 = base64.b64encode(buffer.getvalue()).decode()
                    plt.close()
                    
                    html_output += f'<div class="chart-container"><img src="data:image/png;base64,{image_base64}" alt="Net Assets Chart"></div>'
            
            # Asset vs Liability Composition
            if 'CurrentAssets' in df.columns and 'CurrentLiabilities' in df.columns:
                html_output += '''
                <h3>Current Assets vs Current Liabilities</h3>
                <p><strong>What it measures:</strong> This chart compares the company's short-term assets (cash, debtors, inventory) 
                against short-term liabilities (creditors, loans due within a year).</p>
                <p><strong>Why it matters:</strong> The gap between these lines indicates working capital health. When liabilities 
                exceed assets (lines cross), the company faces a potential cash crisis.</p>
                '''
                
                fig, ax = plt.subplots(figsize=(10, 5))
                
                assets_data = df[['Year', 'CurrentAssets']].dropna()
                liabilities_data = df[['Year', 'CurrentLiabilities']].dropna()
                
                if not assets_data.empty and not liabilities_data.empty:
                    ax.plot(assets_data['Year'], assets_data['CurrentAssets'], 
                           marker='o', label='Current Assets', linewidth=2, color='#28a745')
                    ax.plot(liabilities_data['Year'], liabilities_data['CurrentLiabilities'], 
                           marker='s', label='Current Liabilities', linewidth=2, color='#dc3545')
                    ax.fill_between(assets_data['Year'], assets_data['CurrentAssets'], 
                                   liabilities_data['CurrentLiabilities'], alpha=0.2)
                    ax.set_xlabel('Year')
                    ax.set_ylabel('£')
                    ax.set_title('Current Assets vs Current Liabilities')
                    ax.legend()
                    ax.grid(True, alpha=0.3)
                    from matplotlib.ticker import MaxNLocator
                    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
                    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'£{x:,.0f}'))
                    
                    buffer = BytesIO()
                    plt.tight_layout()
                    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
                    buffer.seek(0)
                    image_base64 = base64.b64encode(buffer.getvalue()).decode()
                    plt.close()
                    
                    html_output += f'<div class="chart-container"><img src="data:image/png;base64,{image_base64}" alt="Assets vs Liabilities Chart"></div>'
            
            # Cash Position
            if 'CashBankInHand' in df.columns:
                html_output += '''
                <h3>Cash Holdings</h3>
                <p><strong>What it measures:</strong> The company's cash and bank balances over time.</p>
                <p><strong>Why it matters:</strong> Cash is the lifeblood of a business. Declining cash reserves, especially 
                when combined with high liabilities, can indicate imminent financial distress. Growing cash suggests strong 
                operational performance and financial discipline.</p>
                '''
                
                fig, ax = plt.subplots(figsize=(10, 5))
                cash_data = df[['Year', 'CashBankInHand']].dropna()
                
                if not cash_data.empty:
                    ax.plot(cash_data['Year'], cash_data['CashBankInHand'], 
                           marker='o', linewidth=2, color='#ffc107')
                    ax.set_xlabel('Year')
                    ax.set_ylabel('£')
                    ax.set_title('Cash Holdings Trend')
                    ax.grid(True, alpha=0.3)
                    from matplotlib.ticker import MaxNLocator
                    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
                    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'£{x:,.0f}'))
                    
                    buffer = BytesIO()
                    plt.tight_layout()
                    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
                    buffer.seek(0)
                    image_base64 = base64.b64encode(buffer.getvalue()).decode()
                    plt.close()
                    
                    html_output += f'<div class="chart-container"><img src="data:image/png;base64,{image_base64}" alt="Cash Holdings Chart"></div>'
        except Exception as e:
            log_message(f"Error generating financial charts: {e}")
            html_output += '<p>Unable to generate financial charts. Please check the uploaded accounts files.</p>'
        html_output += '</div>'
        return html_output
    
    def _generate_timeline_section(self):
        """Generate the company timeline section HTML."""
        if not getattr(self, '_timeline_b64', None):
            return ''
        return f'''
        <div class="section">
            <h2>3. Company Timeline</h2>
            <p>This chart shows key events and periods in the company's history, including director and PSC
            tenure periods, filing events, notices, and (if enabled) grants received.
            The chart is rendered as a vector graphic &mdash; zoom in via your browser (Ctrl/Cmd +) for detail.</p>
            <div class="chart-container" style="overflow:auto; max-width:100%;">
                {self._timeline_b64}
            </div>
        </div>
        '''

    def _generate_ownership_section(self):
        """Generate the corporate ownership structure section HTML."""
        ownership_data = getattr(self, '_ownership_data', None)
        # Check was not enabled — omit section entirely
        if ownership_data is None:
            return ''
        # Check was enabled but no PSCs found
        if not ownership_data:
            return '''
        <div class="section">
            <h2>Corporate Ownership Structure</h2>
            <p>No Persons with Significant Control (PSCs) detected. Consult Companies House directly for further information.</p>
        </div>
        '''
        if not getattr(self, '_ownership_b64', None):
            return ''
        return f'''
        <div class="section">
            <h2>Corporate Ownership Structure</h2>
            <p>This diagram shows the corporate ownership chain traced through Persons with Significant
            Control (PSC) data. Arrows indicate control relationships. The investigated company is shown
            at the base of the tree.</p>
            <div class="chart-container" style="overflow:auto; max-width:100%;">
                {self._ownership_b64}
            </div>
        </div>
        '''

    def _generate_grants_section(self):
        """Generate the grants data section HTML."""
        grants_data = getattr(self, '_grants_data', None)
        if grants_data is None:
            return ''
        return generate_grants_report_html(grants_data)

    def _generate_cross_analysis_section(self):
        """Generate the cross-analysis report section HTML."""
        report = getattr(self, '_cross_analysis_report', None)
        if report is None:
            return ''

        risk_emoji = {
            'HIGH': '\U0001F534',      # Red circle
            'MEDIUM': '\U0001F7E1',    # Yellow circle
            'LOW': '\U0001F7E2',       # Green circle
            'NOT_ASSESSED': '\u26AA',  # White circle
        }

        html_out = '<div class="section"><h2>Financial &amp; Grant Cross-Analysis</h2>'

        # Accounts type
        if report.accounts_type:
            html_out += f'<p><strong>Detected accounts type:</strong> {html.escape(str(report.accounts_type))}</p>'

        # Company age note
        if report.company_age_note:
            html_out += f'<div class="quality-caveat">{html.escape(report.company_age_note)}</div>'

        # Filing quality caveat
        if report.filing_quality_caveat:
            html_out += f'<div class="quality-caveat">{html.escape(report.filing_quality_caveat)}</div>'

        # Composite warning
        if report.composite_warning:
            html_out += f'<div class="composite-warning">{html.escape(report.composite_warning)}</div>'

        # Pattern warnings
        for pw in report.pattern_warnings:
            html_out += f'<div class="pattern-warning">{html.escape(pw)}</div>'

        # Summary table
        html_out += '''
        <table class="cross-analysis-summary">
            <tr><th>Check</th><th>Risk</th><th>Confidence</th></tr>
        '''
        for r in report.results:
            risk_class = r.risk_flag.lower().replace('_', '-')
            conf_class = r.confidence.lower()
            emoji = risk_emoji.get(r.risk_flag, '')
            html_out += f'''
            <tr>
                <td>{html.escape(r.title)}</td>
                <td><span class="risk-{risk_class}">{emoji} {html.escape(r.risk_flag)}</span></td>
                <td><span class="confidence-{conf_class}">{html.escape(r.confidence)}</span></td>
            </tr>
            '''
        html_out += '</table>'

        # Individual rule cards
        for r in report.results:
            if r.risk_flag == 'NOT_ASSESSED':
                continue

            risk_class = r.risk_flag.lower().replace('_', '-')
            conf_class = r.confidence.lower()
            emoji = risk_emoji.get(r.risk_flag, '')

            html_out += f'''
            <div class="cross-rule-card {risk_class}">
                <h3>
                    {html.escape(r.title)}
                    <span class="risk-{risk_class}">{emoji} {html.escape(r.risk_flag)}</span>
                    <span class="confidence-{conf_class}">{html.escape(r.confidence)}</span>
                </h3>
                <p>{html.escape(r.narrative)}</p>
            '''

            # Trend data table
            if r.trend_data:
                vfmt = getattr(r, 'value_format', 'currency')
                if vfmt == 'percentage':
                    col_header = 'Value (%)'
                elif vfmt == 'multiplier':
                    col_header = 'Value (\u00d7)'
                else:
                    col_header = 'Value (\u00a3)'
                html_out += f'<table class="trend-table"><tr><th>Year</th><th>{col_header}</th><th>YoY Change</th></tr>'
                for td in r.trend_data:
                    change_str = f"{td['change_pct']:+.1f}%" if td.get('change_pct') is not None else '\u2014'
                    v = td['value']
                    if vfmt == 'percentage':
                        val_str = f"{v:.1f}%"
                    elif vfmt == 'multiplier':
                        val_str = f"{v:.2f}\u00d7"
                    else:
                        val_str = f"\u00a3{v:,.0f}"
                    html_out += f"<tr><td>{td['year']}</td><td>{val_str}</td><td>{change_str}</td></tr>"
                html_out += '</table>'

            html_out += f'''
                <div class="recommendation">
                    <strong>Recommendation:</strong> {html.escape(r.recommendation)}
                </div>
            </div>
            '''

        html_out += '</div>'
        return html_out

    def _generate_limitations_html(self):
        """Generate data limitations section."""
        html_output = '<p>This report is based on the following data sources and is subject to these limitations:</p><ul>'
        
        html_output += '<li><strong>Companies House Data:</strong> Information retrieved from the public register on ' + datetime.now().strftime('%d %B %Y') + '. This reflects the position as filed and may not capture very recent changes.</li>'
        
        if self.accounts_loaded:
            df = self.financial_analyzer.data
            years = sorted(df['Year'].unique())
            html_output += f'<li><strong>Financial Accounts:</strong> Analysis based on {len(years)} year(s) of filed accounts covering {years[0]} to {years[-1]}. '
            
            # Check account types
            if 'accounts_type' in df.columns:
                account_types = df['accounts_type'].dropna().unique()
                if len(account_types) > 0:
                    html_output += f'Account types filed: {", ".join(account_types)}. '
                    if any('micro' in str(t).lower() or 'abridged' in str(t).lower() for t in account_types):
                        html_output += 'Micro or abridged accounts provide limited financial detail. '
            
            html_output += '</li>'
        else:
            html_output += '<li><strong>No Financial Accounts:</strong> No iXBRL accounts were uploaded. Financial analysis is therefore not available and the assessment is based solely on Companies House registry data.</li>'
        
        if getattr(self, '_grants_data', None) is not None:
            grant_count = len(self._grants_data) if self._grants_data else 0
            html_output += f'<li><strong>Grants Data (360Giving GrantNav):</strong> {grant_count} grant(s) found. GrantNav coverage depends on funders reporting to the 360Giving standard; not all UK grants are captured.</li>'

        if getattr(self, '_ownership_data', None) is not None:
            depth = max((r['level'] for r in self._ownership_data), default=0) if self._ownership_data else 0
            html_output += f'<li><strong>Ownership Structure:</strong> Traced up to {depth} level(s) of corporate ownership via PSC data. Ownership chains involving non-UK entities or unregistered entities may be incomplete.</li>'

        # Cross-analysis provenance
        report = getattr(self, '_cross_analysis_report', None)
        if report:
            assessed_rules = [r for r in report.results if r.confidence != 'SKIPPED']
            skipped_rules = [r for r in report.results if r.confidence == 'SKIPPED']
            auto_rules = [r for r in report.results if r.confidence == 'AUTO']
            enriched_rules = [r for r in report.results if r.confidence == 'ENRICHED']
            limited_rules = [r for r in report.results if r.confidence == 'LIMITED']

            html_output += '<li><strong>Cross-Analysis:</strong> '
            html_output += f'{len(assessed_rules)} of 6 rules were assessed. '
            if auto_rules:
                html_output += f'{len(auto_rules)} based entirely on auto-parsed data. '
            if enriched_rules:
                html_output += f'{len(enriched_rules)} enhanced by user-provided data. '
            if limited_rules:
                html_output += f'{len(limited_rules)} ran with limited data. '
            if skipped_rules:
                html_output += f'{len(skipped_rules)} skipped due to insufficient data. '
            html_output += '</li>'

        html_output += '<li><strong>Scope Limitations:</strong> This report does not include: site visits, management interviews, verification of trading activity, credit reference checks, industry benchmarking, assessment of directors\' personal financial positions, or review of legal proceedings beyond what appears in the public registry.</li>'
        
        if self.industry_context_var.get():
            html_output += f'<li><strong>Industry Context:</strong> User indicated industry context as "{self.industry_context_var.get()}". Financial ratios should be interpreted with industry norms in mind.</li>'
        
        html_output += '<li><strong>Point-in-Time Assessment:</strong> This report reflects the position at the time of generation. Company circumstances can change rapidly, particularly for financially distressed businesses.</li>'
        
        html_output += '</ul><p><strong>Recommendation:</strong> This report should form part of a broader due diligence process and not be relied upon as the sole basis for decision-making. Professional advice should be sought where material amounts or significant relationships are involved.</p>'
        
        return html_output
    

    def _finish_report_generation(self):
        """Re-enable UI after report generation completes or fails."""
        try:
            self.generate_btn.config(state='normal')
        except tk.TclError:
            # Widget was destroyed
            pass

