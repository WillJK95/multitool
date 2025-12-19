# modules/unified_search.py
"""Unified Search Module"""

# --- Standard Library ---
import csv
import html
import os
import re
import textwrap
import threading
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Third-Party ---
import requests
from rapidfuzz.fuzz import WRatio

# --- Tkinter ---
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

# --- From Our Package ---
# API functions (were global functions in original file)
from ..api.companies_house import ch_get_data
from ..api.charity_commission import cc_get_data
from ..utils.enrichment import enrich_with_company_data, enrich_with_charity_data
from ..utils.helpers import clean_company_number

# Constants (were at top of original file)
from ..constants import (
    COMPANY_DATA_FIELDS, 
    CHARITY_DATA_FIELDS,
)

# Utility functions (were global functions or duplicated in classes)
from ..utils.helpers import log_message, clean_address_string

# UI components (were classes in original file)
from ..ui.tooltip import Tooltip

# Base class (was in original file)
from .base import InvestigationModuleBase


class CompanyCharitySearch(InvestigationModuleBase):
    def __init__(
        self,
        parent_app,
        back_callback,
        api_key,
        charity_api_key,
        ch_token_bucket,
        help_key=None,
    ):
        super().__init__(parent_app, back_callback, api_key, help_key=help_key)
        self.charity_api_key = charity_api_key
        self.ch_token_bucket = ch_token_bucket
        # --- UI Setup ---
        # Step 1: Upload File
        upload_frame = ttk.LabelFrame(
            self.content_frame, text="Step 1: Upload File", padding=10
        )
        upload_frame.pack(fill=tk.X, pady=5, padx=10)
        ttk.Button(
            upload_frame, text="Upload Input File (.csv)", command=self.load_file
        ).pack(pady=5)
        self.file_status_label = ttk.Label(upload_frame, text="No file loaded.")
        self.file_status_label.pack(pady=5)

        # Step 2: Select Databases & Priority
        db_frame = ttk.LabelFrame(
            self.content_frame,
            text="Step 2: Select Databases & Search Priority",
            padding=10,
        )
        db_frame.pack(fill=tk.X, pady=5, padx=10)

        db_select_frame = ttk.Frame(db_frame)
        db_select_frame.pack(fill=tk.X)
        self.search_ch_var = tk.BooleanVar(value=True)
        self.ch_check = ttk.Checkbutton(
            db_select_frame,
            text="Companies House (for Companies)",
            variable=self.search_ch_var,
            command=self._update_field_states,
        )
        self.ch_check.pack(anchor="w")

        self.search_cc_var = tk.BooleanVar(value=True)
        self.cc_check = ttk.Checkbutton(
            db_select_frame,
            text="Charity Commission (for Charities)",
            variable=self.search_cc_var,
            command=self._update_field_states,
        )
        self.cc_check.pack(anchor="w")

        ttk.Separator(db_frame, orient="horizontal").pack(fill="x", pady=10)

        priority_frame = ttk.Frame(db_frame)
        priority_frame.pack(fill=tk.X)
        ttk.Label(
            priority_frame, text="When using a single column, search first in:"
        ).pack(anchor="w")
        self.search_priority_var = tk.StringVar(value="ch")
        self.ch_radio = ttk.Radiobutton(
            priority_frame,
            text="Companies House (Default)",
            variable=self.search_priority_var,
            value="ch",
        )
        self.ch_radio.pack(anchor="w", padx=20)
        self.cc_radio = ttk.Radiobutton(
            priority_frame,
            text="Charity Commission",
            variable=self.search_priority_var,
            value="cc",
        )
        self.cc_radio.pack(anchor="w", padx=20)
        Tooltip(
            priority_frame,
            "For single-column searches, this determines which database to check first.\nPrioritizing the correct database can significantly speed up fuzzy matching.",
        )

        # Step 3: Fuzzy Matching
        fuzzy_frame = ttk.LabelFrame(
            self.content_frame, text="Step 3: Fuzzy Matching (Optional)", padding=10
        )
        fuzzy_frame.pack(fill=tk.X, pady=5, padx=10)

        self.fuzzy_match_var = tk.BooleanVar(value=False)
        fuzzy_checkbox = ttk.Checkbutton(
            fuzzy_frame,
            text="Use fuzzy name matching if an exact ID match fails",
            variable=self.fuzzy_match_var,
            command=self._toggle_fuzzy_controls  # Add command to handle state changes
        )
        fuzzy_checkbox.pack(anchor="w", pady=(0, 5))

        self.accuracy_frame = ttk.Frame(fuzzy_frame)
        self.accuracy_frame.pack(fill=tk.X, padx=20, pady=5)

        # Accuracy presets (reversed so 0=highest, 3=lowest)
        self.accuracy_presets = {
            0: {"threshold": 100, "label": "Exact matches only"},
            1: {"threshold": 95, "label": "Very high accuracy (95%)"},
            2: {"threshold": 90, "label": "High accuracy (90%)"},
            3: {"threshold": 85, "label": "Moderate accuracy (85%)"}
        }

        default_preset = 1  # Default to preset 1 (95% accuracy)
        self.accuracy_preset_var = tk.IntVar(value=default_preset)
        self.accuracy_var = tk.IntVar(value=self.accuracy_presets[default_preset]["threshold"])

        ttk.Label(self.accuracy_frame, text="Match Accuracy:").pack(side=tk.LEFT, padx=(0, 10))

        # Create a frame to hold the slider and label
        slider_container = ttk.Frame(self.accuracy_frame)
        slider_container.pack(side=tk.LEFT)

        self.accuracy_slider = ttk.Scale(
            slider_container,
            from_=0,
            to=3,
            orient=tk.HORIZONTAL,
            variable=self.accuracy_preset_var,
            length=150,
            command=self._update_accuracy_label,
            state='disabled'  # Start disabled
        )
        self.accuracy_slider.pack(side=tk.TOP, padx=5)  # Add horizontal padding

        # Label to show the current selection
        self.accuracy_description_label = ttk.Label(
            slider_container, 
            text=self.accuracy_presets[default_preset]["label"],
            foreground="grey",  # Start grey since disabled
            font=("Segoe UI", 9, "italic")
        )
        self.accuracy_description_label.pack(side=tk.TOP, pady=(2, 0))

        # Add helper text
        helper_frame = ttk.Frame(fuzzy_frame)
        helper_frame.pack(fill=tk.X, padx=20, pady=(0, 5))
        helper_text = ttk.Label(
            helper_frame,
            text="ℹ Lower accuracy may find more matches but increases false positives",
            foreground="gray",
            font=("Segoe UI", 8)
        )
        helper_text.pack(anchor="w")

        # Step 4: Select Columns
        self.column_selection_frame = ttk.LabelFrame(
            self.content_frame, text="Step 4: Select Columns", padding=10
        )
        self.column_selection_frame.pack(fill=tk.X, pady=5, padx=10)

        # Step 5: Configure Data Fields
        self.fields_frame = ttk.LabelFrame(
            self.content_frame,
            text="Step 5: Configure Data Fields to Return",
            padding=10,
        )
        self.fields_frame.pack(fill=tk.X, pady=5, padx=10)

        self.ch_fields_frame = ttk.LabelFrame(
            self.fields_frame, text="Companies House Fields", padding=5
        )
        self.ch_fields_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        self.company_data_fields_vars = {
            k: tk.BooleanVar(value=True) for k in COMPANY_DATA_FIELDS
        }
        for key, text in COMPANY_DATA_FIELDS.items():
            ttk.Checkbutton(
                self.ch_fields_frame,
                text=text,
                variable=self.company_data_fields_vars[key],
            ).pack(anchor="w")

        self.cc_fields_frame = ttk.LabelFrame(
            self.fields_frame, text="Charity Commission Fields", padding=5
        )
        self.cc_fields_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(5, 0))
        self.charity_data_fields_vars = {
            k: tk.BooleanVar(value=True) for k in CHARITY_DATA_FIELDS
        }
        for key, text in CHARITY_DATA_FIELDS.items():
            ttk.Checkbutton(
                self.cc_fields_frame,
                text=text,
                variable=self.charity_data_fields_vars[key],
            ).pack(anchor="w")

        # Step 6: Run & Export
        run_frame = ttk.LabelFrame(
            self.content_frame, text="Step 6: Run & Export", padding=10
        )
        run_frame.pack(fill=tk.BOTH, expand=True, pady=5, padx=10)

        run_buttons_frame = ttk.Frame(run_frame)
        run_buttons_frame.pack(pady=5)
        self.run_btn = ttk.Button(
            run_buttons_frame,
            text="Run Investigation",
            state="disabled",
            command=self.start_investigation,
        )
        self.run_btn.pack(side=tk.LEFT, padx=5)
        self.cancel_btn = ttk.Button(
            run_buttons_frame, text="Cancel", command=self.cancel_investigation
        )

        self.export_btn = ttk.Button(
            run_frame, text="Export Results", state="disabled", command=self.export_csv
        )
        self.export_btn.pack(pady=5)

        self.progress_bar = ttk.Progressbar(
            run_frame, orient="horizontal", length=300, mode="determinate"
        )
        self.progress_bar.pack(pady=10)
        self.status_var = tk.StringVar(value="Ready.")
        ttk.Label(run_frame, textvariable=self.status_var).pack()

        # --- NEW: Logic to disable UI based on available API keys ---
        self._configure_ui_for_keys()
        self._update_field_states()

    def _update_accuracy_label(self, value):
        """Update the accuracy description label when slider moves."""
        # Round to nearest integer preset
        preset_index = round(float(value))
        self.accuracy_preset_var.set(preset_index)
        
        # Update the label
        preset = self.accuracy_presets[preset_index]
        self.accuracy_description_label.config(text=preset["label"])
        
        # Update the actual threshold value for compatibility
        self.accuracy_var.set(preset["threshold"])


    def _toggle_fuzzy_controls(self):
        """Enable/disable fuzzy matching controls based on checkbox state."""
        if self.fuzzy_match_var.get():
            # Enable slider and make label blue
            self.accuracy_slider.config(state='normal')
            self.accuracy_description_label.config(foreground='#667eea')
        else:
            # Disable slider and make label grey
            self.accuracy_slider.config(state='disabled')
            self.accuracy_description_label.config(foreground='grey')

    def _disable_frame_widgets(self, frame):
        """Recursively disables all widgets within a given frame."""
        for widget in frame.winfo_children():
            if isinstance(widget, (ttk.Frame, ttk.LabelFrame)):
                self._disable_frame_widgets(widget)
            else:
                try:
                    widget.config(state="disabled")
                except tk.TclError:
                    pass


    def _set_frame_widget_state(self, frame, state):
        """Recursively sets the state of all widgets within a given frame."""
        for widget in frame.winfo_children():
            if isinstance(widget, (ttk.Frame, ttk.LabelFrame)):
                self._set_frame_widget_state(widget, state)
            else:
                try:
                    widget.config(state=state)
                except tk.TclError:
                    pass

    def _update_field_states(self):
        """Enables or disables the data field selection frames based on UI."""
        # Update Companies House fields
        ch_state = "normal" if self.search_ch_var.get() else "disabled"
        self._set_frame_widget_state(self.ch_fields_frame, ch_state)

        # Update Charity Commission fields
        cc_state = "normal" if self.search_cc_var.get() else "disabled"
        self._set_frame_widget_state(self.cc_fields_frame, cc_state)

    def _configure_ui_for_keys(self):
        """Disables parts of the UI if the relevant API key is missing."""
        if not self.api_key:
            self.search_ch_var.set(False)
            self.ch_check.config(state="disabled")
            self.ch_radio.config(state="disabled")
            self._disable_frame_widgets(self.ch_fields_frame)
            if self.search_priority_var.get() == "ch":
                self.search_priority_var.set("cc")

        if not self.charity_api_key:
            self.search_cc_var.set(False)
            self.cc_check.config(state="disabled")
            self.cc_radio.config(state="disabled")
            self._disable_frame_widgets(self.cc_fields_frame)
            if self.search_priority_var.get() == "cc":
                self.search_priority_var.set("ch")

    def load_file(self):
        path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if not path:
            return
        if self.load_file_logic(path):
            self.file_status_label.config(
                text=f"File loaded: {len(self.original_data)} rows found.",
                foreground="green",
            )
            self._display_column_selection_ui()
            self.run_btn.config(state="disabled")
        else:
            self.file_status_label.config(text="Error loading file.", foreground="red")

    def _display_column_selection_ui(self):
        """Display dropdown menus for column selection."""
        # Clear existing widgets
        for widget in self.column_selection_frame.winfo_children():
            widget.destroy()

        self.company_num_col_var = tk.StringVar()
        self.charity_num_col_var = tk.StringVar()
        self.name_col_var = tk.StringVar()

        # Create options list with a "None" option at the start
        options = ["___NONE___"] + self.original_headers

        # Container to hold the three columns side-by-side
        columns_container = ttk.Frame(self.column_selection_frame)
        columns_container.pack(fill="x", expand=True, pady=5, anchor="n")

        # --- Column 1: Company Number ---
        cnum_frame = ttk.LabelFrame(
            columns_container, text="Company Number", padding=5
        )
        cnum_frame.pack(side=tk.LEFT, fill="x", expand=True, padx=5)
        
        cnum_combo = ttk.Combobox(
            cnum_frame, 
            textvariable=self.company_num_col_var, 
            values=options,
            state="readonly"
        )
        cnum_combo.pack(fill="x", pady=5)
        cnum_combo.set("___NONE___")  # Default value

        # --- Column 2: Charity Number ---
        ccnum_frame = ttk.LabelFrame(
            columns_container, text="Charity Number", padding=5
        )
        ccnum_frame.pack(side=tk.LEFT, fill="x", expand=True, padx=5)

        ccnum_combo = ttk.Combobox(
            ccnum_frame, 
            textvariable=self.charity_num_col_var, 
            values=options,
            state="readonly"
        )
        ccnum_combo.pack(fill="x", pady=5)
        ccnum_combo.set("___NONE___")

        # --- Column 3: Name (Fuzzy) ---
        name_frame = ttk.LabelFrame(
            columns_container, text="Name (for Fuzzy)", padding=5
        )
        name_frame.pack(side=tk.LEFT, fill="x", expand=True, padx=5)

        name_combo = ttk.Combobox(
            name_frame, 
            textvariable=self.name_col_var, 
            values=options,
            state="readonly"
        )
        name_combo.pack(fill="x", pady=5)
        name_combo.set("___NONE___")

        # --- Confirm Button ---
        ttk.Button(
            self.column_selection_frame,
            text="Confirm Columns",
            command=self._confirm_columns,
        ).pack(side=tk.BOTTOM, pady=10)

        # Force UI update
        self.app.after(1, self._update_scrollregion)

    def _confirm_columns(self):
        self.company_col = self.company_num_col_var.get()
        self.charity_col = self.charity_num_col_var.get()
        self.name_col = self.name_col_var.get()
        if self.company_col == "___NONE___":
            self.company_col = None
        if self.charity_col == "___NONE___":
            self.charity_col = None
        if self.name_col == "___NONE___":
            self.name_col = None

        if not self.company_col and not self.charity_col and not self.name_col:
            messagebox.showerror(
                "Selection Error", "You must select at least one column to match on."
            )
            return

        messagebox.showinfo(
            "Columns Confirmed", "Column selection confirmed. Ready to run."
        )
        self.run_btn.config(state="normal")

    def start_investigation(self):
        # --- NEW: Check for fuzzy match logic error ---
        company_col_selected = self.company_col and self.company_col != "___NONE___"
        charity_col_selected = self.charity_col and self.charity_col != "___NONE___"
        name_col_selected = self.name_col and self.name_col != "___NONE___"

        # If only a name column is selected but fuzzy match is off, ask the user
        if (
            name_col_selected
            and not company_col_selected
            and not charity_col_selected
            and not self.fuzzy_match_var.get()
        ):
            confirm = messagebox.askyesno(
                "Fuzzy Match Confirmation",
                f"You have only selected the name column '{self.name_col}', but fuzzy matching is not enabled.\n\nDid you mean to run a fuzzy match on this column?",
                icon="question",
            )
            if confirm:
                self.fuzzy_match_var.set(True)  # Enable fuzzy matching
            else:
                return  # Cancel the investigation
        # --- END NEW ---

        if not self.search_ch_var.get() and not self.search_cc_var.get():
            messagebox.showerror(
                "Selection Error", "You must select at least one database to search."
            )
            return

        self.cancel_flag.clear()
        self.run_btn.pack_forget()
        self.cancel_btn.pack(side=tk.LEFT, padx=5)
        self.export_btn.config(state="disabled")
        self.progress_bar["value"] = 0
        self.results_data = []
        threading.Thread(target=self._run_investigation_thread, daemon=True).start()

    def cancel_investigation(self):
        if messagebox.askyesno("Cancel", "Are you sure?"):
            self.cancel_flag.set()

    def _run_investigation_thread(self):

        self.progress_bar["maximum"] = len(self.original_data)

        if self.search_cc_var.get():
            # If Charity Commission is being searched, use the slower, safer limit
            MAX_WORKERS = 2
        else:
            # If ONLY Companies House is searched, use the faster limit
            MAX_WORKERS = 4

        self.app.after(
            0,
            lambda: self.status_var.set(
                f"Processing {len(self.original_data)} rows..."
            ),
        )

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(self._process_single_row, row): row
                for row in self.original_data
            }
            try:
                for future in as_completed(futures):
                    if self.cancel_flag.is_set():
                        break
                    
                    # --- NEW: Check for exceptions on each future individually ---
                    try:
                        result = future.result()
                        if result:
                            self.results_data.append(result)
                    except Exception as exc:
                        row_data = futures[future]
                        log_message(f"Could not process row {row_data}. Error: {exc}")
                    # --- END NEW ---

                    self.app.after(0, self.progress_bar.step, 1)
            except Exception as e:
                log_message(f"A fatal error occurred during unified search: {e}")
                messagebox.showerror("Error", f"A processing error occurred: {e}")

        self.after(100, self._finish_investigation)

    def _finish_investigation(self):
        if self.cancel_flag.is_set():
            self.app.after(0, lambda: self.status_var.set("Investigation cancelled."))
        else:
            self.app.after(0, lambda: self.status_var.set("Investigation complete!"))

        self.cancel_btn.pack_forget()
        self.run_btn.pack(side=tk.LEFT, padx=5)
        if self.results_data:
            self.export_btn.config(state="normal")

    def _process_single_row(self, row):
        if self.cancel_flag.is_set():
            return None

        enriched_row = row.copy()
        enriched_row["match_source"] = ""
        enriched_row["match_status"] = "Not Searched"
        match_found = False

        search_ch = self.search_ch_var.get()
        search_cc = self.search_cc_var.get()
        priority = self.search_priority_var.get()

        cnum_raw = row.get(self.company_col) if self.company_col else None
        ccnum_raw = row.get(self.charity_col) if self.charity_col else None
        name_to_search = row.get(self.name_col) if self.name_col else None

        # --- Part 1: Exact Match Logic (Unchanged) ---
        search_order = []
        if priority == "ch":
            if search_ch: search_order.append(self._search_companies_house_by_number)
            if search_cc: search_order.append(self._search_charity_commission_by_number)
        else:
            if search_cc: search_order.append(self._search_charity_commission_by_number)
            if search_ch: search_order.append(self._search_companies_house_by_number)

        if self.company_col and self.charity_col and self.company_col != self.charity_col:
            if search_ch and cnum_raw:
                if self._search_companies_house_by_number(enriched_row, cnum_raw):
                    match_found = True
            if search_cc and ccnum_raw:
                if self._search_charity_commission_by_number(enriched_row, ccnum_raw):
                    match_found = True
        else:
            identifier = cnum_raw or ccnum_raw
            if identifier:
                for search_func in search_order:
                    if search_func(enriched_row, identifier):
                        match_found = True
                        break

        # --- Part 2: Fuzzy Match on Name (Completely Rewritten Logic) ---
        if not match_found and self.fuzzy_match_var.get() and name_to_search:
            potential_matches = []
            
            # Step A: Gather all possible matches above threshold
            if search_ch:
                ch_match, ch_score, _ = self._match_company(name_to_search, self.accuracy_var.get())
                if ch_match:
                    potential_matches.append({'score': ch_score, 'type': 'ch', 'data': ch_match})
            
            if search_cc:
                cc_match, cc_score, _ = self._match_charity(name_to_search, self.accuracy_var.get())
                if cc_match:
                    potential_matches.append({'score': cc_score, 'type': 'cc', 'data': cc_match})

            # Step B: If we found any, pick the best one and enrich the row
            if potential_matches:
                best_match = max(potential_matches, key=lambda x: x['score'])
                match_found = True
                
                enriched_row["match_score"] = best_match['score']
                
                if best_match['type'] == 'ch':
                    enriched_row["match_source"] += "Companies House (Fuzzy); "
                    enriched_row["matched_name"] = best_match['data'].get("title")
                    profile, _ = ch_get_data(
                        self.api_key, self.ch_token_bucket, f"/company/{best_match['data'].get('company_number')}"
                    )
                    if profile:
                        enrich_with_company_data(
                            enriched_row, self.api_key, self.ch_token_bucket, profile, self.company_data_fields_vars, ch_get_data_func=ch_get_data,
                        )
                
                elif best_match['type'] == 'cc':
                    enriched_row["match_source"] += "Charity Commission (Fuzzy); "
                    enriched_row["matched_name"] = best_match['data'].get("charity_name")
                    reg_num = best_match['data'].get("reg_charity_number")
                    if reg_num:
                        enrich_with_charity_data(
                            enriched_row, self.charity_api_key, str(reg_num), self.charity_data_fields_vars, cc_get_data_func=cc_get_data,
                        )

        # --- Part 3: Final Status (Unchanged) ---
        if not match_found:
            enriched_row["match_status"] = "No Match Found"
        else:
            enriched_row["match_status"] = "Match Found"

        return enriched_row

    def _search_companies_house_by_number(self, row, identifier):
        cnum = clean_company_number(identifier)
        if not cnum:
            return False
        profile, error = ch_get_data(
            self.api_key, self.ch_token_bucket, f"/company/{cnum}"
        )
        if profile:
            row["match_source"] += "Companies House (Exact); "
            row["match_status"] = "Match Found"
            enrich_with_company_data(
                row,
                self.api_key,
                self.ch_token_bucket,
                profile,
                self.company_data_fields_vars,
                ch_get_data_func=ch_get_data,
            )
            return True
        return False

    def _search_charity_commission_by_number(self, row, identifier):
        ccnum = identifier.strip()
        if not ccnum.isdigit():
            return False
        details, error = cc_get_data(self.charity_api_key, f"/charitydetails/{ccnum}/0")
        if details:
            row["match_source"] += "Charity Commission (Exact); "
            row["match_status"] = "Match Found"
            enrich_with_charity_data(
                row, self.charity_api_key, ccnum, self.charity_data_fields_vars, cc_get_data_func=cc_get_data,
            )
            return True
        return False

    def _search_companies_house_by_name(self, row, name):
        match, score, error = self._match_company(name, self.accuracy_var.get())
        if error:
            log_message(f"Fuzzy match CH error for '{name}': {error}")
        if match:
            row["match_source"] += "Companies House (Fuzzy); "
            row["match_status"] = "Match Found"
            row["match_score"] = score
            row["matched_name"] = match.get("title")
            profile, _ = ch_get_data(
                self.api_key,
                self.ch_token_bucket,
                f"/company/{match.get('company_number')}",
            )
            if profile:
                enrich_with_company_data(
                    row,
                    self.api_key,
                    self.ch_token_bucket,
                    profile,
                    self.company_data_fields_vars,
                    ch_get_data_func=ch_get_data,
                )
            return True
        return False
    
    def _search_charity_commission_by_name(self, row, name):
        match, score, error = self._match_charity(name, self.accuracy_var.get())
        if error:
            log_message(f"Fuzzy match CC error for '{name}': {error}")
        if match:
            row["match_source"] += "Charity Commission (Fuzzy); "
            row["match_status"] = "Match Found"
            row["match_score"] = score
            row["matched_name"] = match.get("charity_name")
            reg_num = match.get("reg_charity_number")
            if reg_num:
                enrich_with_charity_data(
                    row,
                    self.charity_api_key,
                    str(reg_num),
                    self.charity_data_fields_vars,
                    cc_get_data_func=cc_get_data,
                )
            return True
        return False

    def _match_company(self, name, threshold):
        # --- NEW: Generate local variations for comparison ---
        name_lower = name.lower()
        search_variants = {name_lower}  # Use a set to handle duplicates automatically

        # Handle Ltd / Limited
        if " limited" in name_lower:
            search_variants.add(name_lower.replace(" limited", " ltd"))
        elif " ltd" in name_lower:
            search_variants.add(name_lower.replace(" ltd", " limited"))

        # Handle PLC / Public Limited Company
        if " public limited company" in name_lower:
            search_variants.add(name_lower.replace(" public limited company", " plc"))
        elif " plc" in name_lower:
            search_variants.add(name_lower.replace(" plc", " public limited company"))

        # The rest of the function performs a single API search as before
        all_results, start_index = [], 0
        while True:
            path = f"/search/companies?q={name}&items_per_page=100&start_index={start_index}"
            data, error = ch_get_data(self.api_key, self.ch_token_bucket, path)
            if error:
                if "Error 50" in error:
                    log_message(
                        f"Server-side API error during paged company search: {error}. Continuing with {len(all_results)} results found so far."
                    )
                    break
                else:
                    return None, 0, error
            if not data or not data.get("items"):
                break
            all_results.extend(data["items"])
            total_results = data.get("total_results", 0)
            start_index += len(data["items"])
            if start_index >= total_results or start_index >= 500:
                break

        if not all_results:
            return None, 0, None

        best, best_score = None, 0

        # --- MODIFIED: Compare each result against all local variations ---
        for item in all_results:
            item_title_lower = item.get("title", "").lower()

            # Find the highest score for this item against any of our search variants
            current_item_max_score = 0
            for variant in search_variants:
                score = WRatio(variant, item_title_lower)
                if score > current_item_max_score:
                    current_item_max_score = score

            # If this item's best score is the highest we've seen overall, update the best match
            if current_item_max_score > best_score:
                best, best_score = item, current_item_max_score

        # Check for a perfect match, which should always take precedence
        for item in all_results:
            if name.lower() == item.get("title", "").lower():
                best, best_score = item, 100
                break

        return (
            (best, best_score, None)
            if best and best_score >= threshold
            else (None, best_score, None)
        )

    def _match_charity(self, name, threshold):
        path = f"/searchCharityName/{requests.utils.quote(name)}"
        data, error = cc_get_data(self.charity_api_key, path)
        if error or not data:
            return None, 0, error or "No data returned"

        best_match, best_score = None, 0
        for item in data:
            score = WRatio(name.lower(), item.get("charity_name", "").lower())
            if score > best_score:
                best_match, best_score = item, score

        return (
            (best_match, best_score, None)
            if best_match and best_score >= threshold
            else (None, best_score, None)
        )

    def export_csv(self):
        if not self.results_data:
            return
        all_headers = set(self.original_headers)
        for row in self.results_data:
            all_headers.update(row.keys())

        ordered_headers = list(self.original_headers)
        new_headers = sorted(list(all_headers - set(self.original_headers)))

        for key in ["match_status", "match_source", "match_score", "matched_name"]:
            if key in new_headers:
                new_headers.insert(0, new_headers.pop(new_headers.index(key)))

        final_headers = ordered_headers + new_headers
        self.generic_export_csv(final_headers)

