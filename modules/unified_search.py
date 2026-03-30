# modules/unified_search.py
"""Unified Search Module"""

# --- Standard Library ---
import csv
import html
import os
import re
import tempfile
import textwrap
import threading
import time
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

# --- Third-Party ---
import networkx as nx
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
from ..utils.helpers import log_message, clean_address_string, get_canonical_name_key, extract_address_string, format_address_label, format_error_summary, format_eta
from ..utils.fuzzy_match import normalize_person_name

# UI components (were classes in original file)
from ..ui.tooltip import Tooltip

# Base class (was in original file)
from .base import InvestigationModuleBase

# Auto-mapping patterns for column header detection
_COMPANY_NUM_PATTERNS = [
    "company_number", "company number", "company no", "company_no",
    "companynumber", "co number", "ch number", "crn",
    "company registration", "company registration number",
]
_CHARITY_NUM_PATTERNS = [
    "charity_number", "charity number", "charity no", "charity_no",
    "charitynumber", "cc number", "charity registration",
    "reg_charity_number", "registered charity number",
]
_NAME_PATTERNS = [
    "name", "company_name", "company name", "entity_name", "entity name",
    "organisation", "organization", "charity_name", "charity name",
    "organisation_name", "organization_name",
]


class CompanyCharitySearch(InvestigationModuleBase):
    def __init__(
        self,
        parent_app,
        back_callback,
        api_key,
        charity_api_key,
        ch_token_bucket,
        help_key=None,
        prefill_entities=None,
    ):
        super().__init__(parent_app, back_callback, api_key, help_key=help_key)
        self.charity_api_key = charity_api_key
        self.ch_token_bucket = ch_token_bucket
        self._prefill_entities = prefill_entities

        # --- Notebook with Configuration and Results tabs ---
        self.notebook = ttk.Notebook(self.content_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        self.config_tab = ttk.Frame(self.notebook)
        self.results_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.config_tab, text="Configuration")
        self.notebook.add(self.results_tab, text="Results")

        # Disable outer scroller when Results tab is active
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

        # Column selection state (initialised before UI build)
        self.company_col = None
        self.charity_col = None
        self.name_col = None
        self._name_combo = None  # reference stored for greying out

        # Results tab state
        self._results_sort_col = None
        self._results_sort_reverse = False
        self._results_columns = []
        self._row_index_map = {}  # treeview iid -> index in self.results_data
        self._shared_connection_rows = set()

        # --- Build Results tab (static skeleton) ---
        self._build_results_tab()

        # --- UI Setup (Configuration tab) ---
        # Step 1: Upload File
        upload_frame = ttk.LabelFrame(
            self.config_tab, text="Step 1: Upload File", padding=10
        )
        upload_frame.pack(fill=tk.X, pady=5, padx=10)
        ttk.Button(
            upload_frame, text="Upload Input File (.csv)", command=self.load_file
        ).pack(pady=5)
        self.file_status_label = ttk.Label(upload_frame, text="No file loaded.")
        self.file_status_label.pack(pady=5)

        # Step 2: Select Databases & Priority
        db_frame = ttk.LabelFrame(
            self.config_tab,
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
            self.config_tab, text="Step 3: Fuzzy Matching (Optional)", padding=10
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
            self.config_tab, text="Step 4: Select Columns", padding=10
        )
        self.column_selection_frame.pack(fill=tk.X, pady=5, padx=10)

        # Step 5: Configure Data Fields
        self.fields_frame = ttk.LabelFrame(
            self.config_tab,
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

        # Step 6: Run
        run_frame = ttk.LabelFrame(
            self.config_tab, text="Step 6: Run", padding=10
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

        self.progress_bar = ttk.Progressbar(
            run_frame, orient="horizontal", length=300, mode="determinate"
        )
        self.progress_bar.pack(pady=10)
        self.status_entity_var = tk.StringVar(value="")
        ttk.Label(run_frame, textvariable=self.status_entity_var).pack(anchor=tk.W)
        self.status_var = tk.StringVar(value="Ready.")
        ttk.Label(run_frame, textvariable=self.status_var).pack(anchor=tk.W)

        # Compact completion status with clickable link to Results tab
        self._config_completion_frame = ttk.Frame(run_frame)
        self._config_completion_var = tk.StringVar(value="")
        self._config_completion_label = ttk.Label(
            self._config_completion_frame,
            textvariable=self._config_completion_var,
            foreground="green",
        )
        self._config_completion_label.pack(side=tk.LEFT)
        self._config_results_link = ttk.Label(
            self._config_completion_frame,
            text="View Results",
            foreground="blue",
            cursor="hand2",
            font=("Segoe UI", 9, "underline"),
        )
        self._config_results_link.pack(side=tk.LEFT, padx=(5, 0))
        self._config_results_link.bind(
            "<Button-1>", lambda e: self.notebook.select(self.results_tab)
        )

        # --- NEW: Logic to disable UI based on available API keys ---
        self._configure_ui_for_keys()
        self._update_field_states()

        # Apply prefill from working set if provided
        if self._prefill_entities:
            self.app.after(100, self._apply_ws_prefill)

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
            # Enable name combo
            if self._name_combo:
                self._name_combo.config(state='readonly')
        else:
            # Disable slider and make label grey
            self.accuracy_slider.config(state='disabled')
            self.accuracy_description_label.config(foreground='grey')
            # Grey out name combo (value persists for auto-mapping)
            if self._name_combo:
                self._name_combo.config(state='disabled')

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

    def _apply_ws_prefill(self):
        """Auto-load working set entities as a temporary CSV and map columns."""
        import tempfile
        entities = self._prefill_entities
        if not entities:
            return

        # Write a temporary CSV with company_number and name columns
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8"
        )
        try:
            writer = csv.DictWriter(tmp, fieldnames=["company_number", "name"])
            writer.writeheader()
            for ent in entities:
                writer.writerow({
                    "company_number": ent.get("company_number", ent.get("number", "")),
                    "name": ent.get("name", ""),
                })
            tmp.close()

            if self.load_file_logic(tmp.name):
                self.file_status_label.config(
                    text=f"Working set loaded: {len(self.original_data)} entities.",
                    foreground="green",
                )
                self._display_column_selection_ui()
                self.run_btn.config(state="disabled")
        except Exception:
            pass

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
        """Display dropdown menus for column selection with auto-mapping."""
        # Clear existing widgets
        for widget in self.column_selection_frame.winfo_children():
            widget.destroy()

        self.company_num_col_var = tk.StringVar()
        self.charity_num_col_var = tk.StringVar()
        self.name_col_var = tk.StringVar()

        NOT_SELECTED = "\u2014 Not Selected \u2014"
        options = [NOT_SELECTED] + self.original_headers

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
        cnum_combo.set(NOT_SELECTED)
        cnum_combo.bind("<<ComboboxSelected>>", lambda e: self._on_column_selection_changed())

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
        ccnum_combo.set(NOT_SELECTED)
        ccnum_combo.bind("<<ComboboxSelected>>", lambda e: self._on_column_selection_changed())

        # --- Column 3: Name (Fuzzy) ---
        name_frame = ttk.LabelFrame(
            columns_container, text="Name (for Fuzzy)", padding=5
        )
        name_frame.pack(side=tk.LEFT, fill="x", expand=True, padx=5)

        self._name_combo = ttk.Combobox(
            name_frame,
            textvariable=self.name_col_var,
            values=options,
            state="disabled" if not self.fuzzy_match_var.get() else "readonly"
        )
        self._name_combo.pack(fill="x", pady=5)
        self._name_combo.set(NOT_SELECTED)
        self._name_combo.bind("<<ComboboxSelected>>", lambda e: self._on_column_selection_changed())

        # --- Auto-mapping ---
        self._auto_map_columns()

        # Trigger initial validation
        self._on_column_selection_changed()

        # Force UI update
        self.app.after(1, self._update_scrollregion)

    def _auto_map_columns(self):
        """Auto-detect column mappings from CSV header names."""
        NOT_SELECTED = "\u2014 Not Selected \u2014"
        mapped_company = False
        mapped_charity = False
        mapped_name = False

        for header in self.original_headers:
            h = header.lower().strip()
            if not mapped_company and h in _COMPANY_NUM_PATTERNS:
                self.company_num_col_var.set(header)
                mapped_company = True
            elif not mapped_charity and h in _CHARITY_NUM_PATTERNS:
                self.charity_num_col_var.set(header)
                mapped_charity = True
            elif not mapped_name and h in _NAME_PATTERNS:
                self.name_col_var.set(header)
                mapped_name = True

        # Second pass: substring matching for headers with prefixes/suffixes
        # e.g. "Org:Company Number" contains the known pattern "company number"
        for header in self.original_headers:
            if mapped_company and mapped_charity and mapped_name:
                break
            h = header.lower().strip()
            if not mapped_company and any(pat in h for pat in _COMPANY_NUM_PATTERNS):
                self.company_num_col_var.set(header)
                mapped_company = True
            elif not mapped_charity and any(pat in h for pat in _CHARITY_NUM_PATTERNS):
                self.charity_num_col_var.set(header)
                mapped_charity = True
            elif not mapped_name and any(pat in h for pat in _NAME_PATTERNS):
                self.name_col_var.set(header)
                mapped_name = True

    def _on_column_selection_changed(self):
        """Auto-validate column selections and enable/disable run button."""
        NOT_SELECTED = "\u2014 Not Selected \u2014"
        company = self.company_num_col_var.get()
        charity = self.charity_num_col_var.get()
        name = self.name_col_var.get()

        self.company_col = None if company == NOT_SELECTED else company
        self.charity_col = None if charity == NOT_SELECTED else charity
        self.name_col = None if name == NOT_SELECTED else name

        if self.company_col or self.charity_col or self.name_col:
            self.run_btn.config(state="normal")
        else:
            self.run_btn.config(state="disabled")

    # ------------------------------------------------------------------
    # Results tab
    # ------------------------------------------------------------------

    def _build_results_tab(self):
        """Build the static skeleton of the Results tab (called once from __init__)."""
        # --- Top bar: Select All / Deselect All / Filters ---
        top_bar = ttk.Frame(self.results_tab)
        top_bar.pack(fill=tk.X, padx=10, pady=(10, 5))

        ttk.Button(
            top_bar, text="Select All", command=self._select_all_results
        ).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(
            top_bar, text="Deselect All", command=self._deselect_all_results
        ).pack(side=tk.LEFT, padx=(0, 15))

        # Filter toggle buttons (non-mutually-exclusive)
        self._filter_inactive_var = tk.BooleanVar(value=False)
        self._filter_shared_var = tk.BooleanVar(value=False)
        self._filter_outdated_var = tk.BooleanVar(value=False)

        inactive_btn = ttk.Checkbutton(
            top_bar, text="Show Inactive",
            variable=self._filter_inactive_var,
            command=self._apply_results_filters,
            bootstyle="warning-outline-toolbutton",
        )
        inactive_btn.pack(side=tk.LEFT, padx=(0, 5))

        shared_btn = ttk.Checkbutton(
            top_bar, text="Show Shared Connections",
            variable=self._filter_shared_var,
            command=self._apply_results_filters,
            bootstyle="info-outline-toolbutton",
        )
        shared_btn.pack(side=tk.LEFT, padx=(0, 5))
        Tooltip(
            shared_btn,
            "Basic name matching only. For accurate entity resolution, send to Network Analytics Workbench.",
        )

        outdated_btn = ttk.Checkbutton(
            top_bar, text="Show Outdated Filings",
            variable=self._filter_outdated_var,
            command=self._apply_results_filters,
            bootstyle="danger-outline-toolbutton",
        )
        outdated_btn.pack(side=tk.LEFT, padx=(0, 5))

        # --- Treeview area ---
        tree_frame = ttk.Frame(self.results_tab)
        tree_frame.pack(fill=tk.X, padx=10, pady=5)
        tree_frame.columnconfigure(0, weight=1)

        self.results_tree = ttk.Treeview(
            tree_frame, columns=[], show="headings", selectmode="extended", height=28
        )
        yscroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.results_tree.yview)
        xscroll = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=self.results_tree.xview)
        self.results_tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        self.results_tree.grid(row=0, column=0, sticky="ew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")

        # --- Progress area for graph-data retrieval (hidden by default) ---
        self._results_progress_frame = ttk.Frame(self.results_tab)
        self._results_progress_var = tk.StringVar(value="")
        ttk.Label(
            self._results_progress_frame, textvariable=self._results_progress_var
        ).pack(anchor=tk.W, padx=10)

        # --- Bottom bar: Export + Send to ---
        bottom_bar = ttk.Frame(self.results_tab)
        bottom_bar.pack(fill=tk.X, padx=10, pady=(5, 10))

        ttk.Button(
            bottom_bar, text="Export Results", command=self.export_csv
        ).pack(side=tk.LEFT, padx=(0, 10))

        # "Send to… ▼" dropdown
        self._send_menu_btn = ttk.Menubutton(
            bottom_bar, text="Send to\u2026 \u25BC", bootstyle="primary-outline"
        )
        send_menu = tk.Menu(self._send_menu_btn, tearoff=0)
        send_menu.add_command(label="Working Set", command=self._send_to_working_set)
        send_menu.add_command(
            label="Network Analytics Workbench", command=self._send_to_network_analytics
        )
        send_menu.add_command(label="Enhanced Due Diligence", command=self._send_to_edd)
        send_menu.add_command(label="UBO Tracer", command=self._send_to_ubo_tracer)
        send_menu.add_command(label="Grants Search", command=self._send_to_grants_search)
        self._send_menu_btn.configure(menu=send_menu)
        self._send_menu_btn.pack(side=tk.LEFT, padx=(0, 5))
        Tooltip(
            self._send_menu_btn,
            "Send selected entities to another module.",
        )

    def _on_tab_changed(self, event=None):
        """Toggle outer scroller when switching between Configuration and Results."""
        selected = self.notebook.select()
        on_results = (selected == str(self.results_tab))
        if on_results:
            # Disable outer scrollbar and mousewheel
            self.scroller.scrollbar.pack_forget()
            self.scroller.canvas.yview_moveto(0)
            self.scroller.canvas.configure(yscrollcommand=lambda *a: None)
            self.scroller._disabled = True
        else:
            # Re-enable outer scrollbar
            self.scroller.scrollbar.pack(side="right", fill="y")
            self.scroller.canvas.configure(yscrollcommand=self.scroller.scrollbar.set)
            self.scroller._disabled = False
            self._update_scrollregion()

    def _select_all_results(self):
        """Select all visible items in the results treeview."""
        children = self.results_tree.get_children()
        if children:
            self.results_tree.selection_set(children)

    def _deselect_all_results(self):
        """Clear selection in the results treeview."""
        self.results_tree.selection_remove(self.results_tree.selection())

    # ------------------------------------------------------------------
    # Populate results
    # ------------------------------------------------------------------

    def _determine_results_columns(self):
        """Determine which columns to show, based on user's field selections."""
        if not self.results_data:
            return []

        # Collect all keys that actually appear in results
        present_keys = set()
        for row in self.results_data:
            present_keys.update(row.keys())

        cols = []
        # Match meta-columns first
        for key in ["match_status", "match_source", "match_score", "matched_name"]:
            if key in present_keys:
                cols.append(key)
        # Original CSV headers
        for h in self.original_headers:
            if h not in cols and h in present_keys:
                cols.append(h)
        # Selected CH fields (in definition order)
        if self.search_ch_var.get():
            for key in COMPANY_DATA_FIELDS:
                if key not in cols and key in present_keys:
                    if key in self.company_data_fields_vars and self.company_data_fields_vars[key].get():
                        cols.append(key)
        # Selected CC fields (in definition order)
        if self.search_cc_var.get():
            for key in CHARITY_DATA_FIELDS:
                if key not in cols and key in present_keys:
                    if key in self.charity_data_fields_vars and self.charity_data_fields_vars[key].get():
                        cols.append(key)
        # Any remaining keys not yet included
        for key in sorted(present_keys):
            if key not in cols:
                cols.append(key)

        return cols

    def _populate_results_tab(self):
        """Fill the results treeview after investigation completes."""
        cols = self._determine_results_columns()
        self._results_columns = cols

        # Configure treeview columns
        self.results_tree.configure(columns=cols)
        for col in cols:
            display = COMPANY_DATA_FIELDS.get(col, CHARITY_DATA_FIELDS.get(col, col.replace("_", " ").title()))
            self.results_tree.heading(
                col, text=display,
                command=lambda c=col: self._sort_results_tree(c),
            )
            self.results_tree.column(col, width=130, minwidth=60)

        # Reset sort state
        self._results_sort_col = None
        self._results_sort_reverse = False

        # Reset filter state
        self._filter_inactive_var.set(False)
        self._filter_shared_var.set(False)
        self._filter_outdated_var.set(False)

        # Pre-compute shared connections index
        self._compute_shared_connections()

        # Insert all rows
        self._insert_results_rows(self.results_data)

        # Auto-sort: unmatched and dissolved at top
        self._auto_sort_initial()

    def _insert_results_rows(self, rows):
        """Clear and insert the given rows into the treeview."""
        for item in self.results_tree.get_children():
            self.results_tree.delete(item)
        self._row_index_map.clear()

        for idx, row in enumerate(rows):
            values = [str(row.get(c, "")) for c in self._results_columns]
            iid = self.results_tree.insert("", tk.END, values=values)
            self._row_index_map[iid] = idx

    def _auto_sort_initial(self):
        """Sort so unmatched and dissolved entities appear at the top."""
        if not self._results_columns:
            return

        def sort_key(item):
            vals = {c: self.results_tree.set(item, c) for c in self._results_columns}
            status = vals.get("match_status", "").lower()
            company_status = vals.get("company_status", "").lower()
            # Priority: 0 = unmatched, 1 = dissolved/inactive, 2 = matched/active
            if "no match" in status:
                return (0, company_status)
            if company_status and company_status != "active":
                return (1, company_status)
            return (2, company_status)

        sorted_items = sorted(self.results_tree.get_children(), key=sort_key)
        for idx, item in enumerate(sorted_items):
            self.results_tree.move(item, "", idx)

    # ------------------------------------------------------------------
    # Sorting
    # ------------------------------------------------------------------

    def _sort_results_tree(self, col):
        """Sort results treeview by clicked column header."""
        if self._results_sort_col == col:
            self._results_sort_reverse = not self._results_sort_reverse
        else:
            self._results_sort_col = col
            self._results_sort_reverse = False
        self._update_results_sort_headings()
        self._reapply_results_sort()

    def _update_results_sort_headings(self):
        """Update column header text to show sort indicator."""
        for c in self._results_columns:
            display = COMPANY_DATA_FIELDS.get(c, CHARITY_DATA_FIELDS.get(c, c.replace("_", " ").title()))
            if c == self._results_sort_col:
                display += " \u2193" if self._results_sort_reverse else " \u2191"
            self.results_tree.heading(c, text=display)

    def _reapply_results_sort(self):
        """Sort the results treeview in-place."""
        if not self._results_sort_col:
            return
        col = self._results_sort_col

        def sort_key(item):
            val = self.results_tree.set(item, col)
            try:
                return (0, float(val))
            except (ValueError, TypeError):
                return (1, val.lower())

        sorted_items = sorted(
            self.results_tree.get_children(),
            key=sort_key, reverse=self._results_sort_reverse,
        )
        for idx, item in enumerate(sorted_items):
            self.results_tree.move(item, "", idx)

    # ------------------------------------------------------------------
    # Filters
    # ------------------------------------------------------------------

    def _compute_shared_connections(self):
        """Pre-compute which rows share an address, director, owner, or trustee."""
        self._shared_connection_rows = set()
        address_index = {}   # normalised_address -> set of row indices
        person_index = {}    # normalised_name -> set of row indices

        for idx, row in enumerate(self.results_data):
            # Addresses
            addr = row.get("registered_address", "")
            if addr:
                key = clean_address_string(addr)
                if key:
                    address_index.setdefault(key, set()).add(idx)

            # Officers
            for field in ("officers", "persons_with_significant_control", "trustee_names"):
                raw = row.get(field, "")
                if not raw:
                    continue
                names = [n.strip() for n in str(raw).split(";") if n.strip()]
                for name in names:
                    nkey = normalize_person_name(name)
                    if nkey:
                        person_index.setdefault(nkey, set()).add(idx)

        # Any row that shares a connection with another row
        for indices in address_index.values():
            if len(indices) > 1:
                self._shared_connection_rows.update(indices)
        for indices in person_index.values():
            if len(indices) > 1:
                self._shared_connection_rows.update(indices)

    def _parse_date(self, date_str):
        """Try to parse a date string, return date object or None."""
        if not date_str:
            return None
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(date_str.strip(), fmt).date()
            except (ValueError, TypeError):
                continue
        return None

    def _apply_results_filters(self):
        """Re-populate treeview showing only rows that pass all active filters."""
        show_inactive = self._filter_inactive_var.get()
        show_shared = self._filter_shared_var.get()
        show_outdated = self._filter_outdated_var.get()

        # If no filters active, show all
        if not show_inactive and not show_shared and not show_outdated:
            self._insert_results_rows(self.results_data)
            self._reapply_results_sort()
            return

        today = date.today()
        filtered = []
        for idx, row in enumerate(self.results_data):
            # Each active filter is a requirement (AND logic)
            if show_inactive:
                cs = row.get("company_status", "").lower()
                if not cs or cs == "active":
                    continue  # skip active/unknown companies

            if show_shared:
                if idx not in self._shared_connection_rows:
                    continue

            if show_outdated:
                is_outdated = False
                for field in ("accounts_next_due", "confirmation_statement_next_due"):
                    d = self._parse_date(row.get(field, ""))
                    if d and d < today:
                        is_outdated = True
                        break
                if not is_outdated:
                    continue

            filtered.append(row)

        self._insert_results_rows(filtered)
        self._reapply_results_sort()

    # ------------------------------------------------------------------
    # Send-to helpers
    # ------------------------------------------------------------------

    def _get_selected_entities(self):
        """Return list of (index, row_dict) for selected treeview items."""
        sel = self.results_tree.selection()
        if not sel:
            messagebox.showinfo("No Selection", "Please select one or more rows first.")
            return []
        entities = []
        for iid in sel:
            idx = self._row_index_map.get(iid)
            if idx is not None and idx < len(self.results_data):
                entities.append((idx, self.results_data[idx]))
        return entities

    def _entity_to_ws_dict(self, row):
        """Convert a results row to a working-set entity dict."""
        company_number = row.get("company_number", "")
        charity_number = row.get("charity_number", row.get("reg_charity_number", ""))
        name = row.get("matched_name", "") or row.get(
            self.name_col, "") if self.name_col else ""
        if not name:
            # Try to get any name-like field
            for k in self.original_headers:
                if "name" in k.lower():
                    name = row.get(k, "")
                    if name:
                        break

        cs = row.get("company_status", "").lower()
        is_active = cs == "active" if cs else True

        # Determine entity type
        match_source = row.get("match_source", "")
        if "Charity Commission" in match_source and not company_number:
            return {
                "name": name or f"Charity {charity_number}",
                "company_number": str(charity_number),
                "active": is_active,
                "entity_type": "charity",
            }
        return {
            "name": name or f"Company {company_number}",
            "company_number": str(company_number) if company_number else str(charity_number),
            "active": is_active,
            "entity_type": "company" if company_number else "charity",
        }

    def _send_to_working_set(self):
        """Append selected entities to the global working set (no duplicates)."""
        entities = self._get_selected_entities()
        if not entities:
            return
        if self.app_state.ubo_working_set is None:
            self.app_state.ubo_working_set = []
        existing = set()
        for ent in self.app_state.ubo_working_set:
            key = (ent.get("name", ""), ent.get("company_number", ""), ent.get("entity_type", ""))
            existing.add(key)
        added = 0
        for _, row in entities:
            ws_dict = self._entity_to_ws_dict(row)
            key = (ws_dict.get("name", ""), ws_dict.get("company_number", ""), ws_dict.get("entity_type", ""))
            if key not in existing:
                self.app_state.ubo_working_set.append(ws_dict)
                existing.add(key)
                added += 1
        self.app._refresh_working_set_indicator()
        try:
            self.app._refresh_home_working_set()
        except Exception:
            pass
        if added:
            messagebox.showinfo("Working Set", f"Added {added} entities to working set.")

    def _send_to_network_analytics(self):
        """Build graph data for selected entities and navigate to Network Analytics."""
        entities = self._get_selected_entities()
        if not entities:
            return

        ws_entities = [self._entity_to_ws_dict(row) for _, row in entities]
        entity_count = len(ws_entities)

        # Show progress on results tab
        self._results_progress_frame.pack(fill=tk.X, padx=10, pady=5)
        self._results_progress_var.set("Building network data...")

        # Proxy label that forwards configure() calls to our StringVar
        outer = self

        class _ProgressProxy:
            def configure(self, **kw):
                txt = kw.get("text", "")
                if txt:
                    outer.safe_ui_call(outer._results_progress_var.set, txt)

        def _build():
            try:
                csv_path = self.app._build_ws_graph_csv(ws_entities, _ProgressProxy())
            except Exception as e:
                log_message(f"Network send failed: {e}")
                csv_path = None

            def _navigate():
                self._results_progress_frame.pack_forget()
                self._results_progress_var.set("")
                if csv_path:
                    self.app._navigate_network_with_csv(
                        csv_path,
                        source_label=f"Working set: {entity_count} entities from Bulk Entity Search",
                    )
                else:
                    messagebox.showwarning("Network Analytics", "No graph data could be generated.")

            self.app.after(0, _navigate)

        threading.Thread(target=_build, daemon=True).start()

    def _send_to_ubo_tracer(self):
        """Send selected companies to UBO Tracer (charities filtered with confirmation)."""
        entities = self._get_selected_entities()
        if not entities:
            return

        companies = []
        charities = []
        for _, row in entities:
            ws = self._entity_to_ws_dict(row)
            if ws["entity_type"] == "charity":
                charities.append(ws)
            else:
                companies.append(ws)

        if charities and not companies:
            messagebox.showinfo(
                "UBO Tracer",
                "UBO Tracer supports companies only. No companies were selected.",
            )
            return

        if charities:
            ok = messagebox.askyesno(
                "UBO Tracer",
                f"{len(companies)} companies and {len(charities)} charities selected. "
                f"UBO Tracer supports companies only. Send {len(companies)} companies?",
            )
            if not ok:
                return

        if len(companies) == 1:
            c = companies[0]
            self.app.show_ubo_investigation(
                prefill_company=c["company_number"],
                prefill_company_name=c["name"],
            )
        else:
            # Multiple companies — pass directly to UBO Tracer
            self.app.show_ubo_investigation(prefill_entities=companies)

    def _send_to_edd(self):
        """Send selected companies/charities to Enhanced Due Diligence."""
        entities = self._get_selected_entities()
        if not entities:
            return

        payload = []
        for _, row in entities:
            ws = self._entity_to_ws_dict(row)
            etype = ws.get("entity_type", "company")
            if etype == "person":
                continue
            dd_type = "charity" if etype == "charity" else "company"
            eid = str(ws.get("company_number", "")).strip()
            if not eid:
                continue
            payload.append({"type": dd_type, "id": eid})

        if not payload:
            messagebox.showinfo(
                "EDD",
                "No compatible companies or charities were selected.",
            )
            return

        self.app.show_enhanced_dd(prefill_entities=payload)

    def _send_to_grants_search(self):
        """Send selected entities to Grants Search with prefill."""
        entities = self._get_selected_entities()
        if not entities:
            return
        ws_entities = [self._entity_to_ws_dict(row) for _, row in entities]
        self.app.show_grants_investigation(prefill_entities=ws_entities)

    # ------------------------------------------------------------------
    # Investigation
    # ------------------------------------------------------------------

    def start_investigation(self):
        # --- NEW: Check for fuzzy match logic error ---
        company_col_selected = self.company_col and self.company_col != "\u2014 Not Selected \u2014"
        charity_col_selected = self.charity_col and self.charity_col != "\u2014 Not Selected \u2014"
        name_col_selected = self.name_col and self.name_col != "\u2014 Not Selected \u2014"

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
        # Reset run button in case of re-run
        self.run_btn.config(text="Run Investigation", command=self.start_investigation)
        self.run_btn.pack_forget()
        self.cancel_btn.pack(side=tk.LEFT, padx=5)
        self._config_completion_frame.pack_forget()
        self.progress_bar["value"] = 0
        self.results_data = []
        threading.Thread(target=self._run_investigation_thread, daemon=True).start()

    def cancel_investigation(self):
        if messagebox.askyesno("Cancel", "Are you sure?"):
            self.cancel_flag.set()

    def _run_investigation_thread(self):

        self.safe_ui_call(self.progress_bar.config, maximum=len(self.original_data))

        if self.search_cc_var.get():
            # If Charity Commission is being searched, use the slower, safer limit
            MAX_WORKERS = 2
        else:
            # If ONLY Companies House is searched, use the configurable limit
            MAX_WORKERS = self.app.ch_max_workers

        total = len(self.original_data)
        self.app.after(0, lambda: self.status_var.set(f"Processing {total} rows..."))

        start_time = time.monotonic()
        processed_count = 0
        found_count = 0
        failed_rows = []
        self._ratelimit_ticking = False

        # Rate-limit ticker: runs on main thread, counts down independently of
        # the worker loop (which blocks when the bucket is empty).
        _CALLS_PER_ITEM = 2  # estimated CH API calls per row

        def _start_ratelimit_ticker():
            if self._ratelimit_ticking:
                return
            self._ratelimit_ticking = True

            def _tick():
                if self.cancel_flag.is_set():
                    self._ratelimit_ticking = False
                    self.status_entity_var.set("")
                    return
                if not self.ch_token_bucket.is_paused:
                    # Pause lifted (window reset or fresh headers); resume normal updates
                    self._ratelimit_ticking = False
                    self.status_entity_var.set("")
                    return
                secs = self.ch_token_bucket.seconds_until_reset
                self.status_entity_var.set("Waiting for API usage limit to refresh")
                self.status_var.set(
                    f"~{int(secs)} seconds remaining \u2013 processing will resume automatically"
                    if secs else "Processing will resume automatically when the limit refreshes"
                )
                self._tracked_after(1000, _tick)

            self._tracked_after(0, _tick)

        # Watchdog: polls is_paused every 500 ms on the main thread so the ticker
        # starts even when all worker threads are blocked inside consume() and
        # as_completed() hasn't yielded a result (which is what happens on the first
        # stop-loss hit in burst mode).
        search_active = [True]

        def _watchdog():
            if not search_active[0] or self.cancel_flag.is_set():
                return
            if (hasattr(self, "ch_token_bucket")
                    and self.ch_token_bucket.is_paused
                    and not self._ratelimit_ticking):
                _start_ratelimit_ticker()
            self._tracked_after(500, _watchdog)

        self._tracked_after(500, _watchdog)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(self._process_single_row, row): row
                for row in self.original_data
            }
            try:
                for future in as_completed(futures):
                    if self.cancel_flag.is_set():
                        break

                    row_data = futures[future]
                    try:
                        result = future.result()
                        if result:
                            self.results_data.append(result)
                            found_count += 1
                    except Exception as exc:
                        failed_rows.append((str(row_data), str(exc)))

                    processed_count += 1
                    self.app.after(0, self.progress_bar.step, 1)

                    elapsed = time.monotonic() - start_time
                    error_count = len(failed_rows)

                    if (
                        hasattr(self, "ch_token_bucket")
                        and self.ch_token_bucket.is_paused
                    ):
                        self.safe_ui_call(_start_ratelimit_ticker)
                    elif not self._ratelimit_ticking:
                        row_name = (
                            row_data.get(self.name_col, "") if self.name_col else ""
                        ) or str(list(row_data.values())[:1])[2:32]
                        remaining = total - processed_count
                        rate_wait = self.ch_token_bucket.estimate_wait_seconds(
                            remaining * _CALLS_PER_ITEM
                        )
                        eta = format_eta(elapsed, processed_count, total,
                                         rate_limit_wait=rate_wait)
                        entity = f"Searching: {row_name} ({processed_count} of {total})"
                        stats = f"ETA: {eta} | Found: {found_count} matches | Errors: {error_count}"
                        self.app.after(0, lambda e=entity: self.status_entity_var.set(e))
                        self.app.after(0, lambda s=stats: self.status_var.set(s))

            except Exception as e:
                log_message(f"A fatal error occurred during unified search: {e}")
                self.safe_ui_call(
                    messagebox.showerror, "Error", f"A processing error occurred: {e}"
                )

        search_active[0] = False
        self._api_failures = failed_rows
        self.safe_ui_call(self._finish_investigation)

    def _finish_investigation(self):
        self.status_entity_var.set("")
        cancelled = self.cancel_flag.is_set()

        if cancelled:
            self.app.after(0, lambda: self.status_var.set("Investigation cancelled."))
        else:
            failed = getattr(self, "_api_failures", [])
            if failed:
                warning = format_error_summary(failed, "row")
                msg = f"Investigation complete! {warning}"
            else:
                msg = "Investigation complete!"
            self.app.after(0, lambda m=msg: self.status_var.set(m))

        self.cancel_btn.pack_forget()
        self.run_btn.pack(side=tk.LEFT, padx=5)

        if self.results_data:
            # Populate results tab
            self._populate_results_tab()

            # Change Run button to "Results →"
            self.run_btn.config(
                text="Results \u2192",
                command=lambda: self.notebook.select(self.results_tab),
                state="normal",
            )

            # Show compact completion status on config tab
            match_count = sum(
                1 for r in self.results_data
                if r.get("match_status", "").lower() != "no match found"
            )
            self._config_completion_var.set(
                f"Search complete \u2014 {match_count} matches found."
            )
            self._config_completion_frame.pack(pady=(5, 0))

            # Auto-switch to Results tab
            if not cancelled:
                self.notebook.select(self.results_tab)
        elif cancelled:
            # Cancelled with no data - just restore run button
            self.run_btn.config(
                text="Run Investigation",
                command=self.start_investigation,
                state="normal",
            )

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
            messagebox.showinfo("No Data", "There is no data to export.")
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

    # --- Network Data Methods (used by Send to Network Analytics) ---

    def _fetch_company_network_data(self, company_number):
        """Worker function to fetch profile, officers, and PSCs for one company."""
        profile, profile_err = ch_get_data(
            self.api_key, self.ch_token_bucket, f"/company/{company_number}"
        )
        officers, _ = ch_get_data(
            self.api_key,
            self.ch_token_bucket,
            f"/company/{company_number}/officers?items_per_page=100",
        )
        pscs, _ = ch_get_data(
            self.api_key,
            self.ch_token_bucket,
            f"/company/{company_number}/persons-with-significant-control?items_per_page=100",
        )
        return profile, officers, pscs, profile_err

    def _fetch_charity_network_data(self, charity_number):
        """Worker function to fetch charity details and trustees for one charity."""
        details, _ = cc_get_data(self.charity_api_key, f"/charitydetails/{charity_number}/0")
        trustees, _ = cc_get_data(self.charity_api_key, f"/charitytrustees/{charity_number}/0")
        return details, trustees

    def _build_unified_graph_object(self):
        """
        Builds a comprehensive network graph for unified search results.
        Handles Companies, Charities, and Charitable Companies (Both).
        """
        G = nx.DiGraph()

        # Collect unique company and charity numbers from results
        company_numbers = set()
        charity_numbers = set()
        company_charity_links = []  # Track rows that have both company and charity

        for row in self.results_data:
            match_source = row.get("match_source", "")

            # Check for company number (from Companies House match)
            company_number = row.get("company_number")
            if company_number and "Companies House" in match_source:
                company_numbers.add(company_number)

            # Check for charity number (from Charity Commission match)
            charity_number = row.get("charity_number")
            if charity_number and "Charity Commission" in match_source:
                charity_numbers.add(str(charity_number))

            # Track if both are present (Charitable Company)
            if company_number and charity_number and "Companies House" in match_source and "Charity Commission" in match_source:
                company_charity_links.append((company_number, str(charity_number)))

        total_entities = len(company_numbers) + len(charity_numbers)
        if total_entities == 0:
            self.app.after(
                0, lambda: self.status_var.set("No matched entities found to build graph.")
            )
            return None

        self.app.after(
            0,
            lambda: self.status_var.set(
                f"Fetching network data for {len(company_numbers)} companies and {len(charity_numbers)} charities..."
            ),
        )

        processed_count = 0

        # --- Process Companies ---
        failed_companies = []
        if company_numbers:
            with ThreadPoolExecutor(max_workers=self.app.ch_max_workers) as executor:
                future_to_cnum = {
                    executor.submit(self._fetch_company_network_data, cnum): cnum
                    for cnum in company_numbers
                }

                for future in as_completed(future_to_cnum):
                    if self.cancel_flag.is_set():
                        return None

                    cnum = future_to_cnum[future]
                    processed_count += 1
                    self.app.after(
                        0,
                        lambda c=processed_count, t=total_entities: self.status_var.set(
                            f"Processing entity {c}/{t}..."
                        ),
                    )

                    profile, officers, pscs, profile_err = future.result()
                    if not profile:
                        failed_companies.append((cnum, profile_err))
                        continue

                    company_name = profile.get("company_name", cnum)
                    G.add_node(cnum, label=company_name, type="company")

                    # Add company registered address
                    addr_data = profile.get("registered_office_address", {})
                    raw_address_str = extract_address_string(addr_data)
                    if raw_address_str:
                        address_id = clean_address_string(raw_address_str)
                        if address_id and not G.has_node(address_id):
                            G.add_node(
                                address_id,
                                label=format_address_label(raw_address_str),
                                type="address",
                            )
                        if address_id:
                            G.add_edge(cnum, address_id, label="registered_at")

                    # Add Officers
                    if officers:
                        for officer in officers.get("items", []):
                            name = officer.get("name")
                            if not name:
                                continue
                            dob = officer.get("date_of_birth")
                            person_key = get_canonical_name_key(name, dob)
                            if not G.has_node(person_key):
                                G.add_node(person_key, label=name, type="person", dob=dob)

                            # Check for existing edge before adding
                            if G.has_edge(cnum, person_key):
                                G[cnum][person_key]["label"] += f", {officer.get('officer_role', 'officer')}"
                            else:
                                G.add_edge(cnum, person_key, label=officer.get("officer_role", "officer"))

                            # Add officer correspondence address
                            officer_addr_raw = extract_address_string(officer.get("address"))
                            if officer_addr_raw:
                                officer_addr_clean = clean_address_string(officer_addr_raw)
                                if officer_addr_clean and not G.has_node(officer_addr_clean):
                                    G.add_node(
                                        officer_addr_clean,
                                        label=format_address_label(officer_addr_raw),
                                        type="address",
                                    )
                                if officer_addr_clean:
                                    G.add_edge(person_key, officer_addr_clean, label="correspondence_at")

                    # Add PSCs
                    if pscs:
                        for psc in pscs.get("items", []):
                            name = psc.get("name")
                            if not name:
                                continue
                            dob = psc.get("date_of_birth")
                            person_key = get_canonical_name_key(name, dob)
                            if not G.has_node(person_key):
                                G.add_node(person_key, label=name, type="person", dob=dob)

                            # Check for existing edge before adding
                            if G.has_edge(cnum, person_key):
                                G[cnum][person_key]["label"] += ", psc"
                            else:
                                G.add_edge(cnum, person_key, label="psc")

                            # Add PSC correspondence address
                            psc_addr_raw = extract_address_string(psc.get("address"))
                            if psc_addr_raw:
                                psc_addr_clean = clean_address_string(psc_addr_raw)
                                if psc_addr_clean and not G.has_node(psc_addr_clean):
                                    G.add_node(
                                        psc_addr_clean,
                                        label=format_address_label(psc_addr_raw),
                                        type="address",
                                    )
                                if psc_addr_clean:
                                    G.add_edge(person_key, psc_addr_clean, label="correspondence_at")

        # --- Process Charities ---
        if charity_numbers:
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_to_ccnum = {
                    executor.submit(self._fetch_charity_network_data, ccnum): ccnum
                    for ccnum in charity_numbers
                }

                for future in as_completed(future_to_ccnum):
                    if self.cancel_flag.is_set():
                        return None

                    ccnum = future_to_ccnum[future]
                    processed_count += 1
                    self.app.after(
                        0,
                        lambda c=processed_count, t=total_entities: self.status_var.set(
                            f"Processing entity {c}/{t}..."
                        ),
                    )

                    details, trustees = future.result()
                    if not details:
                        continue

                    charity_name = details.get("charity_name", f"Charity {ccnum}")
                    charity_node_id = f"CC-{ccnum}"
                    G.add_node(charity_node_id, label=charity_name, type="charity")

                    # Add charity address
                    charity_address = details.get("charity_contact_address")
                    if charity_address:
                        addr_clean = clean_address_string(charity_address)
                        if addr_clean and not G.has_node(addr_clean):
                            G.add_node(
                                addr_clean,
                                label=format_address_label(charity_address),
                                type="address",
                            )
                        if addr_clean:
                            G.add_edge(charity_node_id, addr_clean, label="registered_at")

                    # Add Trustees
                    if trustees:
                        for trustee in trustees:
                            name = trustee.get("trustee_name")
                            if not name:
                                continue
                            # Charities don't provide DOB for trustees
                            person_key = get_canonical_name_key(name, None)
                            if not G.has_node(person_key):
                                G.add_node(person_key, label=name, type="person")

                            G.add_edge(charity_node_id, person_key, label="trustee")

                            # Add trustee correspondence address if available
                            trustee_addr = trustee.get("trustee_address")
                            if trustee_addr:
                                trustee_addr_clean = clean_address_string(trustee_addr)
                                if trustee_addr_clean and not G.has_node(trustee_addr_clean):
                                    G.add_node(
                                        trustee_addr_clean,
                                        label=format_address_label(trustee_addr),
                                        type="address",
                                    )
                                if trustee_addr_clean:
                                    G.add_edge(person_key, trustee_addr_clean, label="correspondence_at")

        # --- Link Charitable Companies (Company <-> Charity) ---
        for company_number, charity_number in company_charity_links:
            charity_node_id = f"CC-{charity_number}"
            if G.has_node(company_number) and G.has_node(charity_node_id):
                G.add_edge(company_number, charity_node_id, label="registered_as")

        if failed_companies:
            warning = format_error_summary(failed_companies, "company")
            self.app.after(
                0,
                lambda w=warning: self.status_var.set(f"Graph built. {w}"),
            )

        return G

