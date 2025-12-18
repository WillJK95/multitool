# multitool/modules/contracts_finder.py
"""
Contracts Finder Investigation Module.

This module allows users to:
1. Search awarded contracts by buyer organisation
2. Extract and enrich supplier information via Companies House
3. Export data for use with Data Match and Network Analytics modules
"""

import csv
import os
import re
import threading
import tkinter as tk
from datetime import datetime, timedelta
from tkinter import ttk, filedialog, messagebox
from typing import Dict, List, Optional

from ..api.contracts_finder import (
    search_notices,
    get_notice,
    extract_supplier_info,
    search_awarded_by_buyer,
    check_api_status,
)
from ..api.companies_house import ch_get_data
from ..utils.helpers import log_message, clean_company_number, clean_address_string
from ..ui.tooltip import Tooltip

from .base import InvestigationModuleBase


class ContractsFinderInvestigation(InvestigationModuleBase):
    """Investigation module for searching and enriching contract data."""
    
    def __init__(self, parent_app, back_callback, ch_token_bucket, api_key=None):
        super().__init__(parent_app, back_callback, api_key, help_key="contracts_finder")
        self.ch_token_bucket = ch_token_bucket
        
        # Data storage
        self.contracts_data = []  # Raw contracts from API
        self.suppliers_data = []  # Enriched supplier data
        
        # --- UI Setup ---
        self._build_ui()
    
    def _build_ui(self):
        """Build the module UI."""
        
        # === Step 1: Search Contracts ===
        search_frame = ttk.LabelFrame(
            self.content_frame, 
            text="Step 1: Search Awarded Contracts", 
            padding=10
        )
        search_frame.pack(fill=tk.X, pady=5, padx=10)
        
        # Buyer name
        buyer_row = ttk.Frame(search_frame)
        buyer_row.pack(fill=tk.X, pady=2)
        ttk.Label(buyer_row, text="Buyer Organisation:").pack(side=tk.LEFT, padx=(0, 5))
        self.buyer_name_var = tk.StringVar()
        buyer_entry = ttk.Entry(buyer_row, textvariable=self.buyer_name_var, width=50)
        buyer_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        Tooltip(buyer_entry, "Enter the name of the contracting authority (e.g., 'Department for Education', 'Manchester City Council')")
        
        # --- BIND ENTER KEY ---
        buyer_entry.bind("<Return>", self.start_contract_search)

        # Date range
        date_row = ttk.Frame(search_frame)
        date_row.pack(fill=tk.X, pady=5)
        
        ttk.Label(date_row, text="From Date:").pack(side=tk.LEFT, padx=(0, 5))
        self.from_date_var = tk.StringVar()
        # Default to 1 year ago
        default_from = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        self.from_date_var.set(default_from)
        from_entry = ttk.Entry(date_row, textvariable=self.from_date_var, width=12)
        from_entry.pack(side=tk.LEFT, padx=(0, 15))
        Tooltip(from_entry, "Start date for contract search (YYYY-MM-DD)")
        
        # --- BIND ENTER KEY ---
        from_entry.bind("<Return>", self.start_contract_search)

        ttk.Label(date_row, text="To Date:").pack(side=tk.LEFT, padx=(0, 5))
        self.to_date_var = tk.StringVar()
        self.to_date_var.set(datetime.now().strftime("%Y-%m-%d"))
        to_entry = ttk.Entry(date_row, textvariable=self.to_date_var, width=12)
        to_entry.pack(side=tk.LEFT)
        Tooltip(to_entry, "End date for contract search (YYYY-MM-DD)")
        
        # --- BIND ENTER KEY ---
        to_entry.bind("<Return>", self.start_contract_search)

        # Search button
        search_btn_row = ttk.Frame(search_frame)
        search_btn_row.pack(fill=tk.X, pady=5)
        self.search_btn = ttk.Button(
            search_btn_row,
            text="Search Contracts",
            command=self.start_contract_search,
        )
        self.search_btn.pack(side=tk.LEFT)
        
        self.contracts_status_label = ttk.Label(search_btn_row, text="")
        self.contracts_status_label.pack(side=tk.LEFT, padx=10)
        
        # === Step 2: Enrich Suppliers ===
        enrich_frame = ttk.LabelFrame(
            self.content_frame,
            text="Step 2: Enrich Supplier Data (Companies House)",
            padding=10
        )
        enrich_frame.pack(fill=tk.X, pady=5, padx=10)
        
        enrich_options_row = ttk.Frame(enrich_frame)
        enrich_options_row.pack(fill=tk.X, pady=2)
        
        self.fetch_officers_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            enrich_options_row,
            text="Fetch Officers/Directors",
            variable=self.fetch_officers_var
        ).pack(side=tk.LEFT, padx=(0, 15))
        
        self.fetch_pscs_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            enrich_options_row,
            text="Fetch PSCs",
            variable=self.fetch_pscs_var
        ).pack(side=tk.LEFT, padx=(0, 15))
        
        self.fetch_address_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            enrich_options_row,
            text="Fetch Registered Address",
            variable=self.fetch_address_var
        ).pack(side=tk.LEFT)
        
        enrich_btn_row = ttk.Frame(enrich_frame)
        enrich_btn_row.pack(fill=tk.X, pady=5)
        
        self.enrich_btn = ttk.Button(
            enrich_btn_row,
            text="Enrich Suppliers via Companies House",
            command=self.start_enrichment,
            state="disabled"
        )
        self.enrich_btn.pack(side=tk.LEFT)
        
        self.enrich_status_label = ttk.Label(enrich_btn_row, text="")
        self.enrich_status_label.pack(side=tk.LEFT, padx=10)
        
        # === Step 3: Export Results ===
        export_frame = ttk.LabelFrame(
            self.content_frame,
            text="Step 3: Export Results",
            padding=10
        )
        export_frame.pack(fill=tk.X, pady=5, padx=10)
        
        # Export buttons row 1
        export_btn_row1 = ttk.Frame(export_frame)
        export_btn_row1.pack(fill=tk.X, pady=2)
        
        self.export_contracts_btn = ttk.Button(
            export_btn_row1,
            text="Export Contracts",
            command=self.export_contracts,
            state="disabled"
        )
        self.export_contracts_btn.pack(side=tk.LEFT, padx=(0, 10))
        Tooltip(self.export_contracts_btn, "Export full contract details (one row per contract/supplier)")
        
        self.export_suppliers_btn = ttk.Button(
            export_btn_row1,
            text="Export Suppliers",
            command=self.export_suppliers,
            state="disabled"
        )
        self.export_suppliers_btn.pack(side=tk.LEFT, padx=(0, 10))
        Tooltip(self.export_suppliers_btn, "Export unique suppliers (basic info only)")
        
        self.export_enriched_btn = ttk.Button(
            export_btn_row1,
            text="Export Enriched Suppliers",
            command=self.export_enriched_suppliers,
            state="disabled"
        )
        self.export_enriched_btn.pack(side=tk.LEFT)
        Tooltip(self.export_enriched_btn, "Export suppliers with Companies House data (officers, PSCs, addresses)")
        
        # Export buttons row 2
        export_btn_row2 = ttk.Frame(export_frame)
        export_btn_row2.pack(fill=tk.X, pady=2)
        
        self.export_graph_btn = ttk.Button(
            export_btn_row2,
            text="Export Graph Data",
            command=self.export_graph_data,
            state="disabled"
        )
        self.export_graph_btn.pack(side=tk.LEFT)
        Tooltip(self.export_graph_btn, "Export data in format compatible with Network Analytics module")
        
        # Help text
        help_text = ttk.Label(
            export_frame,
            text="Tip: Use exported data with Data Match for conflict detection, or Network Analytics for visualisation.",
            foreground="gray",
            font=("Segoe UI", 8, "italic")
        )
        help_text.pack(anchor="w", pady=(5, 0))
        
        # === Progress Bar ===
        progress_frame = ttk.Frame(self.content_frame)
        progress_frame.pack(fill=tk.X, pady=10, padx=10)
        
        self.progress_bar = ttk.Progressbar(
            progress_frame,
            orient="horizontal",
            length=300,
            mode="determinate"
        )
        self.progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        
        self.status_var = tk.StringVar(value="Ready. Enter a buyer organisation name to begin.")
        ttk.Label(progress_frame, textvariable=self.status_var).pack(side=tk.LEFT)
    
    # === Contract Search ===
    
    def start_contract_search(self, event=None):
        """Start the contract search in a background thread."""
        buyer_name = self.buyer_name_var.get().strip()
        if not buyer_name:
            messagebox.showerror("Input Error", "Please enter a buyer organisation name.")
            return
        
        from_date = self.from_date_var.get().strip()
        to_date = self.to_date_var.get().strip()
        
        # Validate dates
        try:
            if from_date:
                datetime.strptime(from_date, "%Y-%m-%d")
            if to_date:
                datetime.strptime(to_date, "%Y-%m-%d")
        except ValueError:
            messagebox.showerror("Date Error", "Dates must be in YYYY-MM-DD format.")
            return
        
        self.search_btn.config(state="disabled")
        self.contracts_data = []
        self.suppliers_data = []
        self._disable_export_buttons()
        self.enrich_btn.config(state="disabled")
        
        self.status_var.set(f"Searching for contracts from '{buyer_name}'...")
        self.contracts_status_label.config(text="Searching...", foreground="orange")
        
        # --- CHANGED: Ensure bar is static and empty ---
        self.progress_bar.config(mode="determinate", value=0)
        
        threading.Thread(
            target=self._contract_search_thread,
            args=(buyer_name, from_date, to_date),
            daemon=True
        ).start()

    def _get_canonical_name_key(self, name: str, dob_obj: dict = None) -> str:
        """
        Generate a canonical key for a person based on name and DOB.
        Ported from UBO module to ensure graph compatibility.
        """
        if not name:
            return ""
        cleaned_name = name.lower()
        titles = ["mr", "mrs", "ms", "miss", "dr", "prof", "sir", "dame", "rev"]
        for title in titles:
            cleaned_name = re.sub(
                r"\b" + re.escape(title) + r"\b\.?", "", cleaned_name
            ).strip()

        if "," in cleaned_name:
            parts = cleaned_name.split(",", 1)
            cleaned_name = f"{parts[1].strip()} {parts[0].strip()}"

        cleaned_name = re.sub(r"[^a-z0-9\s]", "", cleaned_name)
        tokens = cleaned_name.split()
        if not tokens:
            return ""

        name_key = tokens[0] + tokens[-1] if len(tokens) > 1 else tokens[0]

        if dob_obj and "year" in dob_obj and "month" in dob_obj:
            return f"{name_key}-{dob_obj['year']}-{dob_obj['month']:02d}"
        else:
            return name_key
    
    def _contract_search_thread(self, buyer_name: str, from_date: str, to_date: str):
        """Background thread for contract searching."""
        
        # --- NEW: Progress Callback Function ---
        def search_progress_callback(current_contracts_list):
            """
            Called by the API whenever a new page of results is fetched.
            Updates the UI with the running total.
            """
            contract_count = len(current_contracts_list)
            
            # Count unique suppliers so far for the live display
            unique_suppliers_so_far = set()
            for c in current_contracts_list:
                for s in c.get("suppliers", []):
                    # Use name/number as a key
                    key = s.get("company_number") or s.get("name", "").upper()
                    if key:
                        unique_suppliers_so_far.add(key)
            
            supplier_count = len(unique_suppliers_so_far)
            
            # Update the status label on the main thread
            self.safe_update(
                self.contracts_status_label.config, 
                text=f"Searching... Found {contract_count} contracts, {supplier_count} suppliers..."
            )

        try:
            # --- Pass the callback to the API function ---
            contracts, error = search_awarded_by_buyer(
                buyer_name=buyer_name,
                from_date=from_date or None,
                to_date=to_date or None,
                max_results=1000,
                progress_callback=search_progress_callback # <--- The live link
            )
            
            if error and not contracts:
                self.safe_update(
                    messagebox.showerror,
                    "Search Error",
                    f"Failed to search contracts: {error}"
                )
                self.safe_update(self.contracts_status_label.config, 
                               {"text": "Search failed.", "foreground": "red"})
                return
            
            self.contracts_data = contracts

            # The API hard limit is 1000. If we are anywhere near that, 
            # we likely lost data due to pagination limits.
            if len(contracts) >= 900: 
                self.safe_update(
                    messagebox.showwarning,
                    "Potential Data Truncation",
                    f"Warning: This search returned {len(contracts)} records, which is close to the system limit (1000).\n\n"
                    "Some older contracts may have been dropped.\n\n"
                    "Recommended Action: Narrow your 'From' and 'To' dates to search in smaller chunks."
                )
                
            # --- Extract unique suppliers (Standard logic) ---
            unique_suppliers = {}
            for contract in contracts:
                for supplier in contract.get("suppliers", []):
                    key = supplier.get("company_number") or supplier.get("name", "").upper()
                    if key and key not in unique_suppliers:
                        unique_suppliers[key] = {
                            "name": supplier.get("name", ""),
                            "company_number": supplier.get("company_number"),
                            "charity_number": supplier.get("charity_number"),
                            "address": supplier.get("address"),
                            "contracts": []
                        }
                    if key:
                        unique_suppliers[key]["contracts"].append({
                            "notice_id": contract.get("notice_id"),
                            "title": contract.get("title"),
                            "value": supplier.get("awarded_value"),
                            "date": supplier.get("awarded_date"),
                        })
            
            self.suppliers_data = list(unique_suppliers.values())
            
            # Final Status Update
            self.safe_update(
                self.contracts_status_label.config,
                {
                    "text": f"Found {len(contracts)} contracts, {len(self.suppliers_data)} unique suppliers.",
                    "foreground": "green"
                }
            )
            self.safe_update(
                self.status_var.set,
                f"Contract search complete. Found {len(self.suppliers_data)} unique suppliers."
            )
            
            if self.suppliers_data:
                self.safe_update(self.enrich_btn.config, {"state": "normal"})
                self.safe_update(self.export_suppliers_btn.config, {"state": "normal"})
                self.safe_update(self.export_contracts_btn.config, {"state": "normal"})
            
        except Exception as e:
            # Helpful error if API module hasn't been updated yet
            if "unexpected keyword argument 'progress_callback'" in str(e):
                 self.safe_update(
                    messagebox.showerror,
                    "API Update Required",
                    "The API module 'contracts_finder.py' needs to be updated to accept the 'progress_callback' argument."
                )
            else:
                log_message(f"Contract search error: {e}")
                self.safe_update(
                    messagebox.showerror,
                    "Error",
                    f"An unexpected error occurred: {e}"
                )
        finally:
            self.safe_update(self.search_btn.config, {"state": "normal"})
    
    # === Enrichment ===
    
    def start_enrichment(self):
        """Start enriching suppliers via Companies House."""
        if not self.suppliers_data:
            messagebox.showinfo("No Data", "Please search for contracts first.")
            return
        
        if not self.api_key:
            messagebox.showerror("API Key Required", "Companies House API key is required for enrichment.")
            return
        
        self.enrich_btn.config(state="disabled")
        self.status_var.set("Enriching supplier data via Companies House...")
        self.enrich_status_label.config(text="Enriching...", foreground="orange")
        self.progress_bar["value"] = 0
        self.progress_bar["maximum"] = len(self.suppliers_data)
        
        threading.Thread(target=self._enrichment_thread, daemon=True).start()
    
    def _enrichment_thread(self):
        """Background thread for supplier enrichment."""
        try:
            fetch_officers = self.fetch_officers_var.get()
            fetch_pscs = self.fetch_pscs_var.get()
            fetch_address = self.fetch_address_var.get()
            
            for i, supplier in enumerate(self.suppliers_data):
                self.safe_update(self.progress_bar.config, {"value": i + 1})
                self.safe_update(
                    self.status_var.set,
                    f"Enriching supplier {i + 1}/{len(self.suppliers_data)}: {supplier.get('name', 'Unknown')[:30]}..."
                )
                
                company_number = supplier.get("company_number")
                
                # If no company number, try to find one by name
                if not company_number and supplier.get("name"):
                    company_number = self._search_company_by_name(supplier["name"])
                    if company_number:
                        supplier["company_number"] = company_number
                        supplier["ch_match_method"] = "name_search"
                
                if not company_number:
                    supplier["ch_status"] = "No company number"
                    continue
                
                # Clean company number
                company_number = clean_company_number(company_number)
                supplier["company_number"] = company_number # Update stored number to be clean
                
                # Fetch company profile
                profile, error = ch_get_data(
                    self.api_key,
                    self.ch_token_bucket,
                    f"/company/{company_number}"
                )
                
                if error or not profile:
                    supplier["ch_status"] = f"Not found: {error or 'No data'}"
                    continue
                
                supplier["ch_status"] = "Found"
                supplier["ch_company_name"] = profile.get("company_name")
                supplier["ch_company_status"] = profile.get("company_status")
                supplier["ch_incorporation_date"] = profile.get("date_of_creation")
                
                # Fetch registered address
                if fetch_address:
                    addr = profile.get("registered_office_address", {})
                    # Create the raw string first
                    raw_address = ", ".join(filter(None, [
                        addr.get("address_line_1"),
                        addr.get("address_line_2"),
                        addr.get("locality"),
                        addr.get("postal_code"),
                    ]))
                    
                    # Store RAW for display labels and CLEAN for IDs/matching
                    supplier["ch_registered_address_raw"] = raw_address
                    supplier["ch_registered_address"] = clean_address_string(raw_address)
                
                # Fetch officers
                if fetch_officers:
                    officers, _ = ch_get_data(
                        self.api_key,
                        self.ch_token_bucket,
                        f"/company/{company_number}/officers?items_per_page=100"
                    )
                    if officers and officers.get("items"):
                        # Get active officers
                        active_officers = [
                            o for o in officers["items"]
                            if not o.get("resigned_on")
                        ]
                        supplier["ch_officers"] = active_officers
                        supplier["ch_officers_names"] = "; ".join([
                            o.get("name", "") for o in active_officers
                        ])
                
                # Fetch PSCs
                if fetch_pscs:
                    pscs, _ = ch_get_data(
                        self.api_key,
                        self.ch_token_bucket,
                        f"/company/{company_number}/persons-with-significant-control?items_per_page=100"
                    )
                    if pscs and pscs.get("items"):
                        # Get active PSCs
                        active_pscs = [
                            p for p in pscs["items"]
                            if not p.get("ceased_on")
                        ]
                        supplier["ch_pscs"] = active_pscs
                        supplier["ch_pscs_names"] = "; ".join([
                            p.get("name", "") for p in active_pscs
                        ])
            
            self.safe_update(
                self.enrich_status_label.config,
                {"text": "Enrichment complete.", "foreground": "green"}
            )
            self.safe_update(
                self.status_var.set,
                "Enrichment complete. Export data for use with other modules."
            )
            self.safe_update(self.export_enriched_btn.config, {"state": "normal"})
            self.safe_update(self.export_graph_btn.config, {"state": "normal"})
            
        except Exception as e:
            log_message(f"Enrichment error: {e}")
            self.safe_update(
                messagebox.showerror,
                "Error",
                f"Enrichment failed: {e}"
            )
        finally:
            self.safe_update(self.enrich_btn.config, {"state": "normal"})
    
    def _search_company_by_name(self, name: str) -> Optional[str]:
        """Search Companies House for a company by name and return the best match."""
        import urllib.parse
        encoded_name = urllib.parse.quote(name)
        
        results, error = ch_get_data(
            self.api_key,
            self.ch_token_bucket,
            f"/search/companies?q={encoded_name}&items_per_page=5"
        )
        
        if error or not results:
            return None
        
        items = results.get("items", [])
        if not items:
            return None
        
        # Return the first active company
        for item in items:
            if item.get("company_status") == "active":
                return item.get("company_number")
        
        # Fallback to first result
        return items[0].get("company_number")
    
    # === Export Functions ===
    
    def _disable_export_buttons(self):
        """Disable all export buttons."""
        self.export_contracts_btn.config(state="disabled")
        self.export_suppliers_btn.config(state="disabled")
        self.export_enriched_btn.config(state="disabled")
        self.export_graph_btn.config(state="disabled")
    
    def export_contracts(self):
        """Export full contracts data to CSV (one row per contract/supplier combination)."""
        if not self.contracts_data:
            messagebox.showinfo("No Data", "No contracts data to export.")
            return
        
        filepath = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            title="Save Contracts As"
        )
        if not filepath:
            return
        
        headers = [
            "notice_id", "title", "description", "buyer_name",
            "published_date", "awarded_date", "awarded_value",
            "value_low", "value_high", "cpv_codes", "region",
            "supplier_name", "supplier_company_number", "supplier_charity_number",
            "supplier_address", "supplier_awarded_value", "supplier_awarded_date",
            "contract_start", "contract_end"
        ]
        
        try:
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                
                for contract in self.contracts_data:
                    suppliers = contract.get("suppliers", [])
                    
                    # If no suppliers, still output the contract
                    if not suppliers:
                        suppliers = [{}]
                    
                    for supplier in suppliers:
                        row = {
                            "notice_id": contract.get("notice_id"),
                            "title": contract.get("title"),
                            "description": contract.get("description", "")[:500],
                            "buyer_name": contract.get("buyer_name"),
                            "published_date": contract.get("published_date"),
                            "awarded_date": contract.get("awarded_date"),
                            "awarded_value": contract.get("awarded_value"),
                            "value_low": contract.get("value_low"),
                            "value_high": contract.get("value_high"),
                            "cpv_codes": "; ".join(contract.get("cpv_codes", [])),
                            "region": contract.get("region"),
                            "supplier_name": supplier.get("name"),
                            "supplier_company_number": supplier.get("company_number"),
                            "supplier_charity_number": supplier.get("charity_number"),
                            "supplier_address": supplier.get("address"),
                            "supplier_awarded_value": supplier.get("awarded_value"),
                            "supplier_awarded_date": supplier.get("awarded_date"),
                            "contract_start": supplier.get("contract_start"),
                            "contract_end": supplier.get("contract_end"),
                        }
                        writer.writerow(row)
            
            total_rows = sum(len(c.get("suppliers", [])) or 1 for c in self.contracts_data)
            messagebox.showinfo(
                "Export Complete", 
                f"Exported {len(self.contracts_data)} contracts ({total_rows} rows) to:\n{filepath}"
            )
            
        except IOError as e:
            messagebox.showerror("Export Error", f"Could not write file: {e}")
    
    def export_suppliers(self):
        """Export supplier data to CSV (works with or without enrichment)."""
        if not self.suppliers_data:
            messagebox.showinfo("No Data", "No supplier data to export.")
            return
        
        filepath = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            title="Save Suppliers As"
        )
        if not filepath:
            return
        
        headers = [
            "supplier_name", "company_number", "charity_number",
            "contract_count", "total_value", "contract_titles"
        ]
        
        try:
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                
                for supplier in self.suppliers_data:
                    contracts = supplier.get("contracts", [])
                    total_value = sum(c.get("value") or 0 for c in contracts)
                    contract_titles = "; ".join(c.get("title", "")[:50] for c in contracts)
                    
                    row = {
                        "supplier_name": supplier.get("name"),
                        "company_number": supplier.get("company_number"),
                        "charity_number": supplier.get("charity_number"),
                        "contract_count": len(contracts),
                        "total_value": total_value,
                        "contract_titles": contract_titles,
                    }
                    writer.writerow(row)
            
            messagebox.showinfo(
                "Export Complete", 
                f"Exported {len(self.suppliers_data)} suppliers to:\n{filepath}"
            )
            
        except IOError as e:
            messagebox.showerror("Export Error", f"Could not write file: {e}")
    
    def export_enriched_suppliers(self):
        """Export supplier data with full Companies House enrichment to CSV."""
        if not self.suppliers_data:
            messagebox.showinfo("No Data", "No supplier data to export.")
            return
        
        # Check if enrichment has been done
        if not any(s.get("ch_status") for s in self.suppliers_data):
            messagebox.showinfo(
                "Not Enriched", 
                "Please run Companies House enrichment first (Step 2)."
            )
            return
        
        filepath = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            title="Save Enriched Suppliers As"
        )
        if not filepath:
            return
        
        headers = [
            "supplier_name", "company_number", "charity_number",
            "ch_status", "ch_company_name", "ch_company_status",
            "ch_incorporation_date", "ch_registered_address",
            "ch_officers_names", "ch_pscs_names",
            "contract_count", "total_value", "contract_titles"
        ]
        
        try:
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                
                for supplier in self.suppliers_data:
                    contracts = supplier.get("contracts", [])
                    total_value = sum(c.get("value") or 0 for c in contracts)
                    contract_titles = "; ".join(c.get("title", "")[:50] for c in contracts)
                    
                    row = {
                        "supplier_name": supplier.get("name"),
                        "company_number": supplier.get("company_number"),
                        "charity_number": supplier.get("charity_number"),
                        "ch_status": supplier.get("ch_status"),
                        "ch_company_name": supplier.get("ch_company_name"),
                        "ch_company_status": supplier.get("ch_company_status"),
                        "ch_incorporation_date": supplier.get("ch_incorporation_date"),
                        "ch_registered_address": supplier.get("ch_registered_address"),
                        "ch_officers_names": supplier.get("ch_officers_names"),
                        "ch_pscs_names": supplier.get("ch_pscs_names"),
                        "contract_count": len(contracts),
                        "total_value": total_value,
                        "contract_titles": contract_titles,
                    }
                    writer.writerow(row)
            
            messagebox.showinfo(
                "Export Complete", 
                f"Exported {len(self.suppliers_data)} enriched suppliers to:\n{filepath}"
            )
            
        except IOError as e:
            messagebox.showerror("Export Error", f"Could not write file: {e}")
    
    def export_graph_data(self):
        """
        Export data in an Edge List format compatible with the UBO Graph module.
        Includes Companies, Officers, PSCs, and Registered Addresses.
        """
        if not self.suppliers_data:
            messagebox.showinfo("No Data", "No supplier data to export.")
            return
        
        # Check if enrichment has been done
        if not any(s.get("ch_status") for s in self.suppliers_data):
            messagebox.showinfo(
                "Not Enriched", 
                "Please run Companies House enrichment first (Step 2)."
            )
            return
        
        filepath = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            title="Save Graph Edge List As"
        )
        if not filepath:
            return

        # Headers matching UBO module export
        headers = [
            "source_id",
            "source_label",
            "source_type",
            "target_id",
            "target_label",
            "target_type",
            "relationship"
        ]
        
        count = 0
        try:
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                
                for supplier in self.suppliers_data:
                    # Skip if we didn't find the company in CH
                    if supplier.get("ch_status") != "Found":
                        continue
                    
                    # --- Source Node (The Company) ---
                    # Ensure company number is strictly cleaned (UBO style)
                    raw_cnum = supplier.get("company_number", "UNKNOWN")
                    company_number = clean_company_number(raw_cnum)
                    
                    company_name = supplier.get("ch_company_name") or supplier.get("name")
                    
                    source_id = company_number
                    source_label = company_name
                    source_type = "company"

                    # --- 1. Address Edge ---
                    # Use stored cleaned address for ID, and raw for Label (if available)
                    address_clean = supplier.get("ch_registered_address")
                    address_raw = supplier.get("ch_registered_address_raw", address_clean)

                    if address_clean:
                        # Re-clean just to be absolutely safe/idempotent
                        final_address_id = clean_address_string(address_clean)
                        
                        writer.writerow([
                            source_id,          # Source: Company Number (Clean)
                            source_label,       # Source: Company Name
                            source_type,        # Source Type: company
                            final_address_id,   # Target ID: Cleaned/Lowercase Address
                            address_raw,        # Target Label: Pretty Address
                            "address",          # Target Type: address
                            "registered_at"     # Relationship
                        ])
                        count += 1

                    # --- 2. Officer Edges ---
                    for officer in supplier.get("ch_officers", []):
                        officer_name = officer.get("name", "Unknown")
                        dob = officer.get("date_of_birth")

                        # Use canonical key (Name + DOB) for ID to match UBO
                        target_id = self._get_canonical_name_key(officer_name, dob)
                        
                        # Use Original Name for Label (UBO Style)
                        target_label = officer_name
                        target_type = "person"
                        relationship = officer.get("officer_role", "officer")

                        writer.writerow([
                            source_id,
                            source_label,
                            source_type,
                            target_id,
                            target_label,
                            target_type,
                            relationship
                        ])
                        count += 1

                    # --- 3. PSC Edges ---
                    for psc in supplier.get("ch_pscs", []):
                        psc_name = psc.get("name", "Unknown")
                        dob = psc.get("date_of_birth")

                        # Use canonical key
                        target_id = self._get_canonical_name_key(psc_name, dob)
                        target_label = psc_name
                        target_type = "person"
                        relationship = "psc"
                        
                        writer.writerow([
                            source_id,
                            source_label,
                            source_type,
                            target_id,
                            target_label,
                            target_type,
                            relationship
                        ])
                        count += 1
            
            messagebox.showinfo(
                "Export Complete", 
                f"Successfully exported {count} connections to:\n{filepath}"
            )
            
        except IOError as e:
            messagebox.showerror("Export Error", f"Could not write file: {e}")
    
    # === Utility Methods ===
    
    def safe_update(self, func, *args, **kwargs):
        """Safely call a function on the main thread."""
        try:
            self.app.after(0, lambda: func(*args, **kwargs))
        except tk.TclError:
            pass  # Widget was destroyed
