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
from ..constants import (
    CONFIG_DIR,
)
from .base import InvestigationModuleBase
from ..utils.helpers import log_message, clean_company_number

class EnhancedDueDiligence(InvestigationModuleBase):
    def __init__(self, parent_app, api_key, back_callback, ch_token_bucket):
        super().__init__(parent_app, back_callback, api_key, help_key=None)
        self.ch_token_bucket = ch_token_bucket
        self.company_data = {}
        self.financial_analyzer = None
        self.accounts_loaded = False
        
        # Default thresholds
        self.thresholds = {
            'current_ratio_min': 1.0,
            'quick_ratio_min': 0.5,
            'debt_to_equity_max': 2.0,
            'revenue_decline_pct': -10,
            'revenue_decline_years': 2,
            'consecutive_loss_years': 2,
            'late_filings_count': 2,
            'late_filings_period': 5,
            'director_churn_count': 3,
            'director_churn_months': 12,
        }
        
        self._build_ui()
    
    def _build_ui(self):
        # Step 1: Company Lookup
        lookup_frame = ttk.LabelFrame(
            self.content_frame, text="Step 1: Enter Company Number", padding=10
        )
        lookup_frame.pack(fill=tk.X, pady=5, padx=10)
        
        input_frame = ttk.Frame(lookup_frame)
        input_frame.pack(fill=tk.X, pady=5)
        ttk.Label(input_frame, text="Company Number:").pack(side=tk.LEFT, padx=(0, 5))
        self.company_num_var = tk.StringVar()
        company_entry = ttk.Entry(input_frame, textvariable=self.company_num_var, width=15)
        company_entry.pack(side=tk.LEFT, padx=5)
        self.fetch_btn = ttk.Button(
            input_frame, text="Fetch Company Data", command=self.fetch_company_profile
        )
        self.fetch_btn.pack(side=tk.LEFT, padx=5)
        
        self.company_summary_text = tk.Text(lookup_frame, height=6, wrap=tk.WORD, state='disabled')
        self.company_summary_text.pack(fill=tk.X, pady=5)
        
        # Step 2: Upload Accounts
        upload_frame = ttk.LabelFrame(
            self.content_frame, text="Step 2: Upload iXBRL Accounts (Optional)", padding=10
        )
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
        
        # Step 3: Configure Analysis
        config_frame = ttk.LabelFrame(
            self.content_frame, text="Step 3: Configure Analysis", padding=10
        )
        config_frame.pack(fill=tk.X, pady=5, padx=10)
        
        # Tier 1 checks (always available)
        tier1_frame = ttk.LabelFrame(config_frame, text="Core Checks", padding=5)
        tier1_frame.pack(fill=tk.X, pady=2)
        
        self.check_vars = {}
        self.check_widgets = {}  # Store widget references
        
        tier1_checks = [
            ('solvency', 'Net asset position (requires accounts)', True),
            ('liquidity', 'Liquidity ratios (requires accounts)', True),
            ('filing_status', 'Filing compliance & late submissions', True),
            ('company_status', 'Company status warnings', True),
        ]
        
        for key, label, default in tier1_checks:
            var = tk.BooleanVar(value=default)
            self.check_vars[key] = var
            widget = ttk.Checkbutton(tier1_frame, text=label, variable=var)
            widget.pack(anchor='w')
            self.check_widgets[key] = widget
        
        # Tier 2 checks
        tier2_frame = ttk.LabelFrame(config_frame, text="Enhanced Checks", padding=5)
        tier2_frame.pack(fill=tk.X, pady=2)
        
        tier2_checks = [
            ('director_churn', 'Director/PSC turnover analysis', True),
            ('revenue_trends', 'Revenue & profitability trends (requires accounts)', True),
            ('predictive_outlook', 'Predictive financial outlook (requires accounts)', True),
            ('default_address', 'Companies House default address check', True),
            ('accounting_changes', 'Accounting date & filing category changes', True),
            ('offshore_pscs', 'Offshore PSC analysis', True),
        ]
        
        for key, label, default in tier2_checks:
            var = tk.BooleanVar(value=default)
            self.check_vars[key] = var
            widget = ttk.Checkbutton(tier2_frame, text=label, variable=var)
            widget.pack(anchor='w')
            self.check_widgets[key] = widget
        
        # Tier 3 checks (expensive)
        tier3_frame = ttk.LabelFrame(config_frame, text="Deep Investigation (Slower)", padding=5)
        tier3_frame.pack(fill=tk.X, pady=2)
        
        tier3_checks = [
            ('director_history', 'Director insolvency history (many API calls)', False),
            ('phoenix_check', 'Phoenix company name matching', False),
        ]
        
        for key, label, default in tier3_checks:
            var = tk.BooleanVar(value=default)
            self.check_vars[key] = var
            widget = ttk.Checkbutton(tier3_frame, text=label, variable=var)
            widget.pack(anchor='w')
            self.check_widgets[key] = widget
        
        # Advanced settings (collapsible)
        self.show_advanced = tk.BooleanVar(value=False)
        advanced_toggle = ttk.Checkbutton(
            config_frame,
            text="▶ Show Advanced Threshold Settings",
            variable=self.show_advanced,
            command=self._toggle_advanced_settings
        )
        advanced_toggle.pack(anchor='w', pady=(10, 0))
        
        self.advanced_frame = ttk.Frame(config_frame)
        # Will be packed/unpacked by toggle
        
        self._build_advanced_settings()
        
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

    def _build_advanced_settings(self):
        """Build the advanced threshold configuration UI."""
        # Financial ratios
        ratios_frame = ttk.LabelFrame(self.advanced_frame, text="Financial Ratios", padding=5)
        ratios_frame.pack(fill=tk.X, pady=2)
        
        self.threshold_vars = {}
        
        ratio_configs = [
            ('current_ratio_min', 'Current Ratio (warn if below):', 1.0),
            ('quick_ratio_min', 'Quick Ratio (warn if below):', 0.5),
            ('debt_to_equity_max', 'Debt-to-Equity (warn if above):', 2.0),
        ]
        
        for key, label, default in ratio_configs:
            frame = ttk.Frame(ratios_frame)
            frame.pack(fill=tk.X, pady=2)
            ttk.Label(frame, text=label, width=30).pack(side=tk.LEFT)
            var = tk.DoubleVar(value=default)
            self.threshold_vars[key] = var
            ttk.Entry(frame, textvariable=var, width=10).pack(side=tk.LEFT)
        
        # Trend analysis
        trends_frame = ttk.LabelFrame(self.advanced_frame, text="Trend Analysis", padding=5)
        trends_frame.pack(fill=tk.X, pady=2)
        
        trend_configs = [
            ('revenue_decline_pct', 'Revenue decline % (flag if worse than):', -10),
            ('revenue_decline_years', 'Over this many years:', 2),
            ('consecutive_loss_years', 'Consecutive loss years (flag if ≥):', 2),
        ]
        
        for key, label, default in trend_configs:
            frame = ttk.Frame(trends_frame)
            frame.pack(fill=tk.X, pady=2)
            ttk.Label(frame, text=label, width=35).pack(side=tk.LEFT)
            var = tk.IntVar(value=default) if 'years' in key else tk.DoubleVar(value=default)
            self.threshold_vars[key] = var
            ttk.Entry(frame, textvariable=var, width=10).pack(side=tk.LEFT)
        
        # Governance
        governance_frame = ttk.LabelFrame(self.advanced_frame, text="Governance", padding=5)
        governance_frame.pack(fill=tk.X, pady=2)
        
        gov_configs = [
            ('late_filings_count', 'Late filings (flag if ≥):', 2),
            ('late_filings_period', 'In last N years:', 5),
            ('director_churn_count', 'Director changes (flag if ≥):', 3),
            ('director_churn_months', 'In last N months:', 12),
        ]
        
        for key, label, default in gov_configs:
            frame = ttk.Frame(governance_frame)
            frame.pack(fill=tk.X, pady=2)
            ttk.Label(frame, text=label, width=30).pack(side=tk.LEFT)
            var = tk.IntVar(value=default)
            self.threshold_vars[key] = var
            ttk.Entry(frame, textvariable=var, width=10).pack(side=tk.LEFT)
    
    def _toggle_advanced_settings(self):
        """Show/hide advanced settings panel."""
        if self.show_advanced.get():
            self.advanced_frame.pack(fill=tk.X, pady=5)
            # Update checkbox text
            for widget in self.content_frame.winfo_children():
                if isinstance(widget, ttk.LabelFrame) and "Configure Analysis" in str(widget):
                    for child in widget.winfo_children():
                        if isinstance(child, ttk.Checkbutton):
                            if self.show_advanced.get():
                                child.config(text="▼ Hide Advanced Threshold Settings")
                            break
        else:
            self.advanced_frame.pack_forget()
    
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
        summary += f"Incorporated: {profile.get('date_of_creation', 'N/A')}\n"
        
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
        
        # Update thresholds from UI
        if self.show_advanced.get():
            for key, var in self.threshold_vars.items():
                self.thresholds[key] = var.get()
        
        threading.Thread(target=self._generate_report_thread, daemon=True).start()
    
    def _generate_report_thread(self):
        """Main report generation logic."""
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
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            log_message(f"Error generating report: {e}\n{error_details}")
            self.safe_update(messagebox.showerror, "Error", f"Report generation failed: {e}")
        finally:
        # Add this finally block
            self.safe_update(self._finish_report_generation)
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
                    'narrative': f"A notice regarding strike-off action was filed on {filing.get('date', 'unknown date')}. This may indicate the company is being dissolved.",
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
                    'narrative': f"The company was incorporated on {profile['date_of_creation']}, approximately {int(age_months)} months ago. Recently incorporated companies have limited trading history.",
                    'recommendation': 'Request additional due diligence on the directors and any parent/sister companies. Consider tighter credit terms or guarantees.'
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
                'narrative': f"The company's accounts are currently overdue. The next accounts were due on {accounts.get('next_due', 'unknown date')}.",
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
                    
                    if net_assets > 0 and change_pct < -30:
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
                severity = 'Critical' if current_ratio < 0.5 else 'Elevated'
                
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
                
                if cash_pct < 10:
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
            
            if metric == 'ProfitLoss' and predicted_value < 0 and pct_change < -20:
                concerns.append(f"Losses projected to worsen to £{abs(predicted_value):,.0f} in {next_year}")
            
            if metric == 'CashBankInHand' and predicted_value < 0:
                concerns.append(f"Cash position projected to turn negative in {next_year}, suggesting potential cash flow crisis")
            
            if metric == 'Revenue' and pct_change < -15:
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
        elif predictions:
            # Check for positive trajectory
            positive_indicators = []
            for p in predictions:
                if p['category'] == 'growth' and p['pct_change'] > 10:
                    positive_indicators.append(f"Revenue growth of {p['pct_change']:.0f}% projected")
                if p['category'] == 'profitability' and p['predicted'] > 0 and p['last_actual'] <= 0:
                    positive_indicators.append("Return to profitability projected")
                if p['category'] == 'solvency' and p['pct_change'] > 15:
                    positive_indicators.append(f"Net assets projected to grow by {p['pct_change']:.0f}%")
            
            if positive_indicators:
                findings.append({
                    'category': 'Financial',
                    'severity': 'Positive',
                    'title': 'Positive Financial Trajectory',
                    'narrative': f"Linear projection of historical trends suggests: " + "; ".join(positive_indicators) + f". These projections are based on {len(df)} years of filed accounts and assume trends continue. Actual results will depend on management execution and market conditions.",
                    'recommendation': 'While the projected trajectory is positive, verify with current trading performance and management accounts.'
                })
        
        # Always add a summary of projections if we have them (informational)
        if predictions and not concerns:
            # Build a summary table of all projections
            projection_lines = []
            for p in predictions:
                direction = "↑" if p['pct_change'] > 0 else "↓" if p['pct_change'] < 0 else "→"
                projection_lines.append(
                    f"{p['metric']}: £{p['last_actual']:,.0f} → £{p['predicted']:,.0f} ({direction} {abs(p['pct_change']):.1f}%)"
                )
            
            if projection_lines:
                findings.append({
                    'category': 'Financial',
                    'severity': 'Moderate',
                    'title': 'Financial Projections Summary',
                    'narrative': f"Based on linear extrapolation of {len(df)} years of filed accounts, the projected values for {predictions[0]['next_year']} are: " + "; ".join(projection_lines) + ". These projections assume historical trends continue unchanged and should be verified against current trading performance.",
                    'recommendation': 'Compare these projections against management forecasts and current trading data to assess whether historical trends remain valid.'
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
            else:
                # Metrics exist but predictions still failed - unusual case
                findings.append({
                    'category': 'Financial',
                    'severity': 'Moderate',
                    'title': 'Unable to Generate Projections',
                    'narrative': f"Financial metrics were found ({', '.join(available_metrics)}) but projections could not be generated. This may be due to incomplete data across multiple years.",
                    'recommendation': 'Review the uploaded accounts files for completeness.'
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
            
            if len(insolvent_companies) >= 3:
                directors_with_issues.append({
                    'name': officer.get('name'),
                    'count': len(insolvent_companies),
                    'examples': insolvent_companies[:3]
                })
        
        if directors_with_issues:
            severity = 'Critical' if any(d['count'] >= 5 for d in directors_with_issues) else 'Elevated'
            
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
        
        for officer in officers['items'][:5]:  # Check top 5 officers
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
                        
                        if similarity >= 80:  # High similarity threshold
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
    
    {self._generate_findings_section('Critical Risk Indicators', critical)}
    {self._generate_findings_section('Elevated Risk Indicators', elevated)}
    {self._generate_findings_section('Moderate Risk Indicators', moderate)}
    
    {chart_html}
    
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
            return f"Based on the analysis performed, {html.escape(company_name)} shows no significant risk indicators in the areas examined. However, this assessment is based on available public information and should be supplemented with additional due diligence as appropriate for your specific requirements."
        
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
            ('Incorporated', html.escape(profile.get('date_of_creation', 'N/A'))),
            ('Jurisdiction', html.escape(profile.get('jurisdiction', 'N/A'))),
            ('Registered Address', html.escape(address)),
            ('Active Officers', str(active_officers)),
            ('Active PSCs', str(active_pscs)),
        ]
        
        for label, value in profile_items:
            html_output += f'<div class="profile-item"><strong>{label}</strong>{value}</div>'
        
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
                if not any('late' in f['title'].lower() or 'overdue' in f['title'].lower() 
                          for f in self._check_filing_compliance()):
                    positives.append("The company appears to have maintained timely filing compliance with Companies House.")
            
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
            print("DEBUG: No financial analyzer or empty data")
            return ""
        
        print(f"DEBUG: Financial data shape: {self.financial_analyzer.data.shape}")
        print(f"DEBUG: Columns: {self.financial_analyzer.data.columns.tolist()}")
        print(f"DEBUG: Data preview:\n{self.financial_analyzer.data.head()}")
        
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

