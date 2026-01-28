# module/network_analytics.py

import csv
import html
import math
import os
import re
import textwrap
import threading
import time
import datetime
import webbrowser
import difflib
import tkinter as tk
from tkinter import font as tkfont
from typing import List, Dict, Optional, Tuple, Set
from tkinter import ttk, filedialog, messagebox
from concurrent.futures import ThreadPoolExecutor, as_completed

import networkx as nx
import pgeocode
from pyvis.network import Network

from ..ui.searchable_entry import SearchableEntry
# --- From Our Package ---
# API functions
from ..api.companies_house import ch_get_data

# Constants
from ..constants import (
    CONFIG_DIR,
)

# Utility functions
from ..utils.helpers import log_message, clean_address_string, get_canonical_name_key, clean_company_number, extract_address_string, format_address_label

# UI components
from ..ui.tooltip import Tooltip

from .base import InvestigationModuleBase

class CollapsibleSection(ttk.Frame):
    """A collapsible frame with a clickable header."""
    
    def __init__(self, parent, title, expanded=False, enabled=True):
        super().__init__(parent)
        
        self.title = title
        self._expanded = tk.BooleanVar(value=expanded)
        self._enabled = enabled
        self._status_text = ""
        self._warning_text = ""
        self._on_toggle_callback = None
        self._on_expand_callback = None
        
        # Header frame
        self.header_frame = ttk.Frame(self)
        self.header_frame.pack(fill=tk.X)
        
        # Toggle button
        self.toggle_btn = ttk.Label(
            self.header_frame,
            text="▶" if not expanded else "▼",
            width=2,
            cursor="hand2"
        )
        self.toggle_btn.pack(side=tk.LEFT, padx=(5, 5))
        
        # Title label
        self.title_label = ttk.Label(
            self.header_frame,
            text=title,
            font=("", 10, "bold"),
            cursor="hand2"
        )
        self.title_label.pack(side=tk.LEFT)
        
        # Status label (shows node/edge counts, etc.)
        self.status_label = ttk.Label(
            self.header_frame,
            text="",
            foreground="gray"
        )
        self.status_label.pack(side=tk.LEFT, padx=(10, 0))
        
        # Warning label (shows "Files changed" etc.)
        self.warning_label = ttk.Label(
            self.header_frame,
            text="",
            foreground="orange"
        )
        self.warning_label.pack(side=tk.LEFT, padx=(10, 0))
        
        # Rebuild button (hidden by default)
        self.rebuild_btn = ttk.Button(
            self.header_frame,
            text="Rebuild",
            command=self._on_rebuild_click,
            width=8
        )
        self._rebuild_callback = None
        
        # Content frame
        self.content_frame = ttk.Frame(self)
        if expanded:
            self.content_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(5, 10))
        
        # Separator
        self.separator = ttk.Separator(self, orient="horizontal")
        self.separator.pack(fill=tk.X, pady=(5, 0))
        
        # Bind click events
        self.toggle_btn.bind("<Button-1>", self._toggle)
        self.title_label.bind("<Button-1>", self._toggle)
        
        # Set initial enabled state
        self.set_enabled(enabled)
    
    def _toggle(self, event=None):
        if not self._enabled:
            return
        
        if self._expanded.get():
            self.collapse()
        else:
            self.expand()
        if self._on_toggle_callback:
            self._on_toggle_callback()
    
    def expand(self):
        if not self._enabled:
            return
        self._expanded.set(True)
        self.toggle_btn.config(text="▼")
        self.content_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(5, 10))

        # Force geometry update to fix tkinter scroll region bug
        self.update_idletasks()

        # Fire callback if set
        if self._on_expand_callback:
            self._on_expand_callback()
    
    def collapse(self):
        self._expanded.set(False)
        self.toggle_btn.config(text="▶")
        self.content_frame.pack_forget()

        # Force geometry update to fix tkinter scroll region bug
        self.update_idletasks()
    
    def is_expanded(self):
        return self._expanded.get()
    
    def set_enabled(self, enabled):
        self._enabled = enabled
        if enabled:
            self.title_label.config(foreground="")
            self.toggle_btn.config(foreground="")
            self.toggle_btn.config(cursor="hand2")
            self.title_label.config(cursor="hand2")
        else:
            self.title_label.config(foreground="gray")
            self.toggle_btn.config(foreground="gray")
            self.toggle_btn.config(cursor="")
            self.title_label.config(cursor="")
            if self._expanded.get():
                self.collapse()
    
    def set_status(self, text):
        """Set the status text shown after the title."""
        self._status_text = text
        self.status_label.config(text=f"— {text}" if text else "")
    
    def set_warning(self, text, show_rebuild=False, rebuild_callback=None):
        """Set warning text and optionally show rebuild button."""
        self._warning_text = text
        self.warning_label.config(text=f"⚠️ {text}" if text else "")
        
        if show_rebuild and rebuild_callback:
            self._rebuild_callback = rebuild_callback
            self.rebuild_btn.pack(side=tk.LEFT, padx=(10, 0))
        else:
            self.rebuild_btn.pack_forget()
    
    def clear_warning(self):
        self._warning_text = ""
        self.warning_label.config(text="")
        self.rebuild_btn.pack_forget()
    
    def _on_rebuild_click(self):
        if self._rebuild_callback:
            self._rebuild_callback()
    
    def set_on_expand(self, callback):
        """Set a callback to fire when the section is expanded."""
        self._on_expand_callback = callback

    def set_on_toggle(self, callback):
        """Set a callback to fire when the section is toggled (expanded OR collapsed)."""
        self._on_toggle_callback = callback


class NetworkAnalytics(InvestigationModuleBase):
    def __init__(
        self, parent_app, back_callback, ch_token_bucket, api_key=None, help_key=None
    ):
        super().__init__(parent_app, back_callback, api_key, help_key=help_key)
        self.ch_token_bucket = ch_token_bucket
        self.source_files = []
        self.full_graph = nx.DiGraph()
        self.all_node_labels = []
        
        # --- Exclusion tracking (soft exclusions) ---
        self.highly_connected_exclusions = set()  # Node IDs excluded as highly connected
        self.peripheral_exclusions = set()        # Node IDs excluded as peripheral
        self.manual_exclusions = set()            # Node IDs manually excluded
        
        # --- State tracking ---
        self.graph_built = False
        self.files_changed_since_build = False
        self.analyse_entity_list = None           # Entity list loaded in Analyse section
        self.analyse_entity_list_path = None      # Path for display
        
        # --- Legacy cohort support (for cohort A/B comparison) ---
        self.cohort_a_ids = set()
        self.cohort_b_ids = set()

        # --- Hidden links discovery ---
        self.discovered_hidden_links = []
        
        # --- Converter state variables ---
        self.converter_source_data = []
        self.converter_headers = []
        
        # --- Tabbed Interface Setup ---
        self.notebook = ttk.Notebook(self.content_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=5)

        # Tab 1: Network Analytics
        self.analytics_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.analytics_tab, text="Network Analytics")
        self._setup_analytics_tab()

        # Tab 2: Data Converter
        self.converter_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.converter_tab, text="Data Converter")
        self._setup_converter_tab()
        
    def _setup_analytics_tab(self):
        """Builds the network analytics UI with collapsible sections."""
        container = self.analytics_tab
        
        # Simple frame for sections (no canvas/scrollbar)
        self.sections_frame = ttk.Frame(container)
        self.sections_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # --- Section 1: Data Sources ---
        self.data_sources_section = CollapsibleSection(
            self.sections_frame,
            "DATA SOURCES",
            expanded=True,
            enabled=True
        )
        self.data_sources_section.pack(fill=tk.X, pady=(5, 0))
        self._build_data_sources_content(self.data_sources_section.content_frame)
        self.data_sources_section.set_on_toggle(self._update_scrollregion)

        # --- Section 2: Build & Refine ---
        self.refine_section = CollapsibleSection(
            self.sections_frame,
            "BUILD & REFINE",
            expanded=False,
            enabled=False  # Disabled until files loaded
        )
        self.refine_section.pack(fill=tk.X, pady=(5, 0))
        self.refine_section.set_on_expand(self._on_refine_section_expanded)
        self._build_refine_content(self.refine_section.content_frame)
        self.refine_section.set_on_toggle(self._update_scrollregion)

        # --- Section 3: Analyse ---
        self.analyse_section = CollapsibleSection(
            self.sections_frame,
            "ANALYSE",
            expanded=False,
            enabled=False  # Disabled until graph built
        )
        self.analyse_section.pack(fill=tk.X, pady=(5, 0))
        self._build_analyse_content(self.analyse_section.content_frame)
        self.analyse_section.set_on_toggle(self._update_scrollregion)

        # --- Section 4: Visualise ---
        self.visualise_section = CollapsibleSection(
            self.sections_frame,
            "VISUALISE",
            expanded=False,
            enabled=False  # Disabled until graph built
        )
        self.visualise_section.pack(fill=tk.X, pady=(5, 0))
        self._build_visualise_content(self.visualise_section.content_frame)
        self.visualise_section.set_on_toggle(self._update_scrollregion)

    def _setup_converter_tab(self):
        """Builds the Data Converter wizard UI."""
        container = self.converter_tab

        # Step 1: Load
        step1_frame = ttk.LabelFrame(container, text="1. Load Source File", padding=10)
        step1_frame.pack(fill=tk.X, pady=10, padx=10)
        
        load_btn = ttk.Button(step1_frame, text="Load CSV...", command=self._converter_load_file)
        load_btn.pack(side=tk.LEFT, padx=5)
        self.converter_file_label = ttk.Label(step1_frame, text="No file loaded.")
        self.converter_file_label.pack(side=tk.LEFT, padx=10)

        # Preview Treeview
        self.converter_preview_tree = ttk.Treeview(step1_frame, height=5, show="headings")
        self.converter_preview_tree.pack(fill=tk.X, pady=5, padx=5)

        # Step 2: Map Columns
        step2_frame = ttk.LabelFrame(container, text="2. Map Columns", padding=10)
        step2_frame.pack(fill=tk.X, pady=10, padx=10)

        type_frame = ttk.Frame(step2_frame)
        type_frame.pack(fill=tk.X, pady=5)
        ttk.Label(type_frame, text="Entity Type:").pack(side=tk.LEFT, padx=5)
        self.converter_entity_type = tk.StringVar(value="person")
        
        def on_type_change():
            if self.converter_entity_type.get() == "person":
                self.lbl_id_col.config(text="Full Name:")
                self.lbl_sec_col.config(text="Date of Birth (DD/MM/YYYY):")
            else:
                self.lbl_id_col.config(text="Company Number:")
                self.lbl_sec_col.config(text="Company Name (Optional):")

        ttk.Radiobutton(type_frame, text="Person", variable=self.converter_entity_type, value="person", command=on_type_change).pack(side=tk.LEFT, padx=10)
        ttk.Radiobutton(type_frame, text="Company", variable=self.converter_entity_type, value="company", command=on_type_change).pack(side=tk.LEFT, padx=10)

        map_grid = ttk.Frame(step2_frame)
        map_grid.pack(fill=tk.X, pady=5)

        self.lbl_id_col = ttk.Label(map_grid, text="Full Name:")
        self.lbl_id_col.grid(row=0, column=0, sticky="w", padx=5, pady=5)
        self.combo_id_col = ttk.Combobox(map_grid, state="readonly", width=30)
        self.combo_id_col.grid(row=0, column=1, sticky="w", padx=5, pady=5)

        self.lbl_sec_col = ttk.Label(map_grid, text="Date of Birth (DD/MM/YYYY):")
        self.lbl_sec_col.grid(row=1, column=0, sticky="w", padx=5, pady=5)
        self.combo_sec_col = ttk.Combobox(map_grid, state="readonly", width=30)
        self.combo_sec_col.grid(row=1, column=1, sticky="w", padx=5, pady=5)

        ttk.Separator(map_grid, orient="horizontal").grid(row=2, column=0, columnspan=2, sticky="ew", pady=10)
        
        ttk.Label(map_grid, text="Address Mapping Mode:").grid(row=3, column=0, sticky="w", padx=5)
        self.address_mode_var = tk.StringVar(value="single")
        
        def toggle_addr_inputs():
            if self.address_mode_var.get() == "single":
                self.combo_addr_full.state(["!disabled"])
                self.combo_addr_line1.state(["disabled"])
                self.combo_addr_postcode.state(["disabled"])
            else:
                self.combo_addr_full.state(["disabled"])
                self.combo_addr_line1.state(["!disabled"])
                self.combo_addr_postcode.state(["!disabled"])

        mode_frame = ttk.Frame(map_grid)
        mode_frame.grid(row=3, column=1, sticky="w")
        ttk.Radiobutton(mode_frame, text="Single Column", variable=self.address_mode_var, value="single", command=toggle_addr_inputs).pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(mode_frame, text="Composite (Line 1 + Postcode)", variable=self.address_mode_var, value="composite", command=toggle_addr_inputs).pack(side=tk.LEFT, padx=5)

        ttk.Label(map_grid, text="Full Address:").grid(row=4, column=0, sticky="w", padx=5, pady=2)
        self.combo_addr_full = ttk.Combobox(map_grid, state="readonly", width=30)
        self.combo_addr_full.grid(row=4, column=1, sticky="w", padx=5, pady=2)

        ttk.Label(map_grid, text="Address Line 1:").grid(row=5, column=0, sticky="w", padx=5, pady=2)
        self.combo_addr_line1 = ttk.Combobox(map_grid, state="disabled", width=30)
        self.combo_addr_line1.grid(row=5, column=1, sticky="w", padx=5, pady=2)

        ttk.Label(map_grid, text="Postcode:").grid(row=6, column=0, sticky="w", padx=5, pady=2)
        self.combo_addr_postcode = ttk.Combobox(map_grid, state="disabled", width=30)
        self.combo_addr_postcode.grid(row=6, column=1, sticky="w", padx=5, pady=2)

        # Step 3: Convert
        step3_frame = ttk.LabelFrame(container, text="3. Generate Output", padding=10)
        step3_frame.pack(fill=tk.X, pady=10, padx=10)

        self.convert_mode = tk.StringVar(value="cohort")
        ttk.Radiobutton(step3_frame, text="Create Entity List (IDs Only)", variable=self.convert_mode, value="cohort").pack(anchor="w", padx=5)
        ttk.Radiobutton(step3_frame, text="Create Graph File (Nodes & Links)", variable=self.convert_mode, value="graph").pack(anchor="w", padx=5)
        
        btn_frame = ttk.Frame(step3_frame)
        btn_frame.pack(fill=tk.X, pady=10)
        
        self.convert_btn = ttk.Button(btn_frame, text="Convert & Save File", command=self._converter_run, state="disabled", bootstyle="success")
        self.convert_btn.pack(side=tk.LEFT)
        self.converter_status = ttk.Label(btn_frame, text="", foreground="green")
        self.converter_status.pack(side=tk.LEFT, padx=10)

    def _build_data_sources_content(self, container):
        """Builds the Data Sources section content."""
        
        # --- Seed from Company ---
        seed_frame = ttk.LabelFrame(
            container,
            text="Seed from Company (Optional)",
            padding=10,
        )
        seed_frame.pack(fill=tk.X, pady=(0, 10))
        
        seed_top_row = ttk.Frame(seed_frame)
        seed_top_row.pack(fill=tk.X, pady=(0, 5))
        self.seed_cnum_var = tk.StringVar()
        ttk.Label(seed_top_row, text="Company Number:").pack(side=tk.LEFT, padx=(0, 5))
        seed_entry = ttk.Entry(seed_top_row, textvariable=self.seed_cnum_var, width=20)
        seed_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        seed_entry.bind("<Return>", lambda event: self.start_seed_fetch())
        seed_btn_state = "normal" if self.api_key else "disabled"
        self.seed_btn = ttk.Button(
            seed_top_row,
            text="Fetch & Add Network Data",
            state=seed_btn_state,
            command=self.start_seed_fetch,
        )
        self.seed_btn.pack(side=tk.LEFT, padx=5)
        
        seed_options_row = ttk.Frame(seed_frame)
        seed_options_row.pack(fill=tk.X)
        ttk.Label(seed_options_row, text="Include:").pack(side=tk.LEFT, padx=(0, 10))
        
        self.seed_fetch_pscs_var = tk.BooleanVar(value=False)
        seed_pscs_cb = ttk.Checkbutton(
            seed_options_row,
            text="Fetch PSCs",
            variable=self.seed_fetch_pscs_var,
        )
        seed_pscs_cb.pack(side=tk.LEFT, padx=(0, 15))
        
        self.seed_fetch_associated_var = tk.BooleanVar(value=False)
        seed_associated_cb = ttk.Checkbutton(
            seed_options_row,
            text="Fetch all associated companies",
            variable=self.seed_fetch_associated_var,
        )
        seed_associated_cb.pack(side=tk.LEFT, padx=(0, 5))
        
        self.seed_warning_label = ttk.Label(
            seed_options_row,
            text="⚠️ May result in many API calls",
            foreground="orange",
        )
        
        def toggle_warning(*args):
            if self.seed_fetch_associated_var.get():
                self.seed_warning_label.pack(side=tk.LEFT, padx=5)
            else:
                self.seed_warning_label.pack_forget()
        self.seed_fetch_associated_var.trace_add("write", toggle_warning)
        
        # Status bar for seeding (moved here from bottom)
        status_frame = ttk.Frame(seed_frame)
        status_frame.pack(fill=tk.X, pady=(10, 0))
        self.seed_progress_bar = ttk.Progressbar(
            status_frame, orient="horizontal", length=200, mode="indeterminate"
        )
        self.seed_progress_bar.pack(side=tk.LEFT, padx=(0, 10))
        self.seed_status_var = tk.StringVar(value="")
        ttk.Label(status_frame, textvariable=self.seed_status_var).pack(side=tk.LEFT)
        
        # --- Import Network Files ---
        import_frame = ttk.LabelFrame(
            container,
            text="Import Graph Data Files",
            padding=10,
        )
        import_frame.pack(fill=tk.X, pady=(0, 10))
        
        buttons_frame = ttk.Frame(import_frame)
        buttons_frame.pack(fill=tk.X, pady=(0, 5))
        ttk.Button(buttons_frame, text="Add File(s)...", command=self.add_files).pack(
            side=tk.LEFT, padx=(0, 10)
        )
        ttk.Button(buttons_frame, text="Clear All", command=self.clear_files).pack(
            side=tk.LEFT
        )
        
        file_list_frame = ttk.Frame(import_frame)
        file_list_frame.pack(fill=tk.X, expand=True, pady=5)
        file_scrollbar = ttk.Scrollbar(file_list_frame, orient=tk.VERTICAL)
        file_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.file_listbox = tk.Listbox(
            file_list_frame, height=4, yscrollcommand=file_scrollbar.set
        )
        self.file_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        file_scrollbar.config(command=self.file_listbox.yview)
        
        # Info tooltip about Data Converter
        info_frame = ttk.Frame(import_frame)
        info_frame.pack(fill=tk.X, pady=(5, 0))
        info_label = ttk.Label(
            info_frame,
            text="ℹ️",
            foreground="blue",
            cursor="hand2",
            font=("", 11)
        )
        info_label.pack(side=tk.LEFT)
        Tooltip(
            info_label,
            "Have a list of names, companies, or addresses from another system?\n"
            "Use the Data Converter tab to prepare it for network analysis."
        )
        ttk.Label(
            info_frame,
            text="Working with external data?",
            foreground="gray"
        ).pack(side=tk.LEFT, padx=(5, 0))


    def _build_refine_content(self, container):
        """Builds the Build & Refine section content."""
        
        # --- Node Exclusions ---
        exclusions_frame = ttk.LabelFrame(
            container,
            text="Node Exclusions",
            padding=10,
        )
        exclusions_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(
            exclusions_frame,
            text="Excluded nodes are hidden from analysis and visualisation but remain in the underlying data.",
            foreground="gray",
            wraplength=500
        ).pack(anchor="w", pady=(0, 10))
        
        # Highly connected nodes
        hc_frame = ttk.Frame(exclusions_frame)
        hc_frame.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(hc_frame, text="Highly connected nodes (more than").pack(side=tk.LEFT)
        self.highly_connected_threshold_var = tk.StringVar(value="50")
        hc_entry = ttk.Entry(hc_frame, textvariable=self.highly_connected_threshold_var, width=5)
        hc_entry.pack(side=tk.LEFT, padx=5)
        ttk.Label(hc_frame, text="connections):").pack(side=tk.LEFT)
        self.scan_hc_btn = ttk.Button(
            hc_frame,
            text="Scan...",
            command=self._open_highly_connected_dialog
        )
        self.scan_hc_btn.pack(side=tk.LEFT, padx=(15, 5))
        self.hc_status_label = ttk.Label(hc_frame, text="No exclusions", foreground="gray")
        self.hc_status_label.pack(side=tk.LEFT, padx=(5, 0))
        
        # Peripheral nodes
        pn_frame = ttk.Frame(exclusions_frame)
        pn_frame.pack(fill=tk.X, pady=(5, 5))
        ttk.Label(pn_frame, text="Peripheral nodes (fewer than").pack(side=tk.LEFT)
        self.peripheral_threshold_var = tk.StringVar(value="2")
        pn_entry = ttk.Entry(pn_frame, textvariable=self.peripheral_threshold_var, width=5)
        pn_entry.pack(side=tk.LEFT, padx=5)
        ttk.Label(pn_frame, text="connections):").pack(side=tk.LEFT)
        self.scan_pn_btn = ttk.Button(
            pn_frame,
            text="Scan...",
            command=self._open_peripheral_dialog
        )
        self.scan_pn_btn.pack(side=tk.LEFT, padx=(15, 5))
        self.pn_status_label = ttk.Label(pn_frame, text="No exclusions", foreground="gray")
        self.pn_status_label.pack(side=tk.LEFT, padx=(5, 0))
        
        # Manage exclusions button
        manage_frame = ttk.Frame(exclusions_frame)
        manage_frame.pack(fill=tk.X, pady=(10, 0))
        self.manage_exclusions_btn = ttk.Button(
            manage_frame,
            text="Manage All Exclusions...",
            command=self._open_exclusion_manager
        )
        self.manage_exclusions_btn.pack(side=tk.LEFT)
        
        # --- Entity Resolution ---
        resolution_frame = ttk.LabelFrame(
            container,
            text="Entity Resolution",
            padding=10,
        )
        resolution_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(
            resolution_frame,
            text="Find and merge duplicate entities (e.g. name variants, address formatting differences).",
            foreground="gray",
            wraplength=500
        ).pack(anchor="w", pady=(0, 10))
        
        self.scan_dupes_btn = ttk.Button(
            resolution_frame,
            text="Scan for Duplicates...",
            command=self._open_deduplication_dialog
        )
        self.scan_dupes_btn.pack(side=tk.LEFT)

        # --- Hidden Links ---
        hidden_links_frame = ttk.LabelFrame(
            container,
            text="Inferred Links",
            padding=10,
        )
        hidden_links_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(
            hidden_links_frame,
            text="Discover potential connections between entities based on proximity and surname matching. Entities separated by a single node only (e.g. shared directorship) are not be included.",
            foreground="gray",
            wraplength=500
        ).pack(anchor="w", pady=(0, 10))

        # Proximity radius input
        radius_frame = ttk.Frame(hidden_links_frame)
        radius_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(radius_frame, text="Proximity radius:").pack(side=tk.LEFT)
        self.proximity_radius_var = tk.StringVar(value="1.0")
        self.proximity_radius_entry = ttk.Entry(
            radius_frame,
            textvariable=self.proximity_radius_var,
            width=6
        )
        self.proximity_radius_entry.pack(side=tk.LEFT, padx=(5, 5))
        ttk.Label(radius_frame, text="km").pack(side=tk.LEFT)
        Tooltip(self.proximity_radius_entry, "Distance in kilometres (0.1 to 50.0)")

        # Status and buttons
        status_frame = ttk.Frame(hidden_links_frame)
        status_frame.pack(fill=tk.X, pady=(0, 5))

        self.hidden_links_status = ttk.Label(
            status_frame,
            text="No hidden links discovered yet.",
            foreground="gray"
        )
        self.hidden_links_status.pack(side=tk.LEFT)

        btn_frame = ttk.Frame(hidden_links_frame)
        btn_frame.pack(fill=tk.X, pady=(5, 0))

        self.scan_hidden_links_btn = ttk.Button(
            btn_frame,
            text="Scan for Inferred Links",
            command=self._scan_for_hidden_links
        )
        self.scan_hidden_links_btn.pack(side=tk.LEFT, padx=(0, 10))

        self.view_hidden_results_btn = ttk.Button(
            btn_frame,
            text="View/Edit Results",
            command=lambda: self._show_hidden_links_dialog(self.discovered_hidden_links),
            state="disabled"
        )
        self.view_hidden_results_btn.pack(side=tk.LEFT)

        # --- Advanced (collapsed) ---
        self.advanced_section = CollapsibleSection(
            container,
            "Advanced",
            expanded=False,
            enabled=True
        )
        self.advanced_section.pack(fill=tk.X, pady=(0, 5))
        
        advanced_content = self.advanced_section.content_frame
        ttk.Label(
            advanced_content,
            text="Manually exclude a specific node:",
            foreground="gray"
        ).pack(anchor="w", pady=(0, 5))
        
        manual_frame = ttk.Frame(advanced_content)
        manual_frame.pack(fill=tk.X, pady=(0, 5))
        
        self.manual_exclude_entry = SearchableEntry(manual_frame)
        self.manual_exclude_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        ttk.Button(
            manual_frame,
            text="Exclude",
            command=self._add_manual_exclusion
        ).pack(side=tk.LEFT)


    def _build_analyse_content(self, container):
        """Builds the Analyse section content."""
        
        # --- Find Connections ---
        connections_frame = ttk.LabelFrame(
            container,
            text="Find Connections",
            padding=10,
        )
        connections_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Search mode selection
        mode_frame = ttk.Frame(connections_frame)
        mode_frame.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(mode_frame, text="Search for connections:").pack(anchor="w")
        
        self.analyse_mode_var = tk.StringVar(value="two_entities")
        
        ttk.Radiobutton(
            mode_frame,
            text="Between two specific entities",
            variable=self.analyse_mode_var,
            value="two_entities",
            command=self._update_analyse_mode_ui
        ).pack(anchor="w", padx=(20, 0))
        
        ttk.Radiobutton(
            mode_frame,
            text="Within a single entity list",
            variable=self.analyse_mode_var,
            value="single_list",
            command=self._update_analyse_mode_ui
        ).pack(anchor="w", padx=(20, 0))
        
        ttk.Radiobutton(
            mode_frame,
            text="Between two entity lists",
            variable=self.analyse_mode_var,
            value="two_lists",
            command=self._update_analyse_mode_ui
        ).pack(anchor="w", padx=(20, 0))

        ttk.Separator(connections_frame, orient="horizontal").pack(fill=tk.X, pady=10)
        
        # Dynamic content area (changes based on mode)
        self.analyse_dynamic_frame = ttk.Frame(connections_frame)
        self.analyse_dynamic_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Build all mode UIs, show/hide as needed
        self._build_two_entities_ui(self.analyse_dynamic_frame)
        self._build_single_list_ui(self.analyse_dynamic_frame)
        self._build_two_lists_ui(self.analyse_dynamic_frame)
        
        ttk.Separator(connections_frame, orient="horizontal").pack(fill=tk.X, pady=10)
        
        # Options
        options_frame = ttk.Frame(connections_frame)
        options_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(options_frame, text="Max hops:").pack(side=tk.LEFT)
        self.max_hops_var = tk.IntVar(value=10)
        self.max_hops_combo = ttk.Combobox(
            options_frame,
            textvariable=self.max_hops_var,
            values=list(range(1, 21)),
            state="readonly",
            width=5,
        )
        self.max_hops_combo.pack(side=tk.LEFT, padx=(5, 20))
        
        self.shortest_only_var = tk.BooleanVar(value=True)
        self.shortest_only_check = ttk.Checkbutton(
            options_frame,
            text="Shortest path only",
            variable=self.shortest_only_var
        )
        self.shortest_only_check.pack(side=tk.LEFT, padx=(0, 20))

        self.enforce_direction_var = tk.BooleanVar(value=False)
        self.enforce_direction_check = ttk.Checkbutton(
            options_frame,
            text="Enforce edge direction",
            variable=self.enforce_direction_var
        )
        self.enforce_direction_check.pack(side=tk.LEFT)
        
        # Action button
        self.find_connections_btn = ttk.Button(
            connections_frame,
            text="Find Connections",
            command=self._execute_find_connections
        )
        self.find_connections_btn.pack(pady=(5, 0))
        
        # Progress/status area for connection searches
        self.analyse_status_frame = ttk.Frame(connections_frame)
        self.analyse_status_frame.pack(fill=tk.X, pady=(10, 0))
        
        self.analyse_progress_bar = ttk.Progressbar(
            self.analyse_status_frame,
            orient="horizontal",
            length=300,
            mode="determinate"
        )
        self.analyse_progress_bar.pack(side=tk.LEFT, padx=(0, 10))
        
        self.analyse_status_var = tk.StringVar(value="")
        ttk.Label(
            self.analyse_status_frame,
            textvariable=self.analyse_status_var
        ).pack(side=tk.LEFT)
        
        # Show initial mode
        self._update_analyse_mode_ui()


    def _build_two_entities_ui(self, parent):
        """Builds UI for 'Between two specific entities' mode."""
        self.two_entities_frame = ttk.Frame(parent)
        
        row1 = ttk.Frame(self.two_entities_frame)
        row1.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(row1, text="Start entity:", width=12).pack(side=tk.LEFT)
        self.start_node_entry = SearchableEntry(row1)
        self.start_node_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        row2 = ttk.Frame(self.two_entities_frame)
        row2.pack(fill=tk.X)
        ttk.Label(row2, text="End entity:", width=12).pack(side=tk.LEFT)
        self.end_node_entry = SearchableEntry(row2)
        self.end_node_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)


    def _build_single_list_ui(self, parent):
        """Builds UI for 'Within a single entity list' mode."""
        self.single_list_frame = ttk.Frame(parent)
        
        row1 = ttk.Frame(self.single_list_frame)
        row1.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(row1, text="Entity list:").pack(side=tk.LEFT)
        ttk.Button(
            row1,
            text="Upload List...",
            command=self._upload_single_entity_list
        ).pack(side=tk.LEFT, padx=(10, 5))
        
        # Info tooltip
        info_label = ttk.Label(row1, text="ℹ️", foreground="blue", cursor="hand2", font=("", 11))
        info_label.pack(side=tk.LEFT, padx=(0, 10))
        Tooltip(info_label, self._get_entity_list_tooltip())
        
        self.single_list_status = ttk.Label(row1, text="No list loaded", foreground="gray")
        self.single_list_status.pack(side=tk.LEFT)
        
        ttk.Label(
            self.single_list_frame,
            text="Finds connections between members of this list who appear in your network.",
            foreground="gray",
            wraplength=450
        ).pack(anchor="w", pady=(5, 0))


    def _build_two_lists_ui(self, parent):
        """Builds UI for 'Between two entity lists' mode."""
        self.two_lists_frame = ttk.Frame(parent)
        
        # List A
        row_a = ttk.Frame(self.two_lists_frame)
        row_a.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(row_a, text="List A:").pack(side=tk.LEFT)
        ttk.Button(
            row_a,
            text="Upload List A...",
            command=self._upload_list_a
        ).pack(side=tk.LEFT, padx=(10, 5))
        info_label_a = ttk.Label(row_a, text="ℹ️", foreground="blue", cursor="hand2", font=("", 11))
        info_label_a.pack(side=tk.LEFT, padx=(0, 10))
        Tooltip(info_label_a, self._get_entity_list_tooltip())
        self.list_a_status = ttk.Label(row_a, text="No list loaded", foreground="gray")
        self.list_a_status.pack(side=tk.LEFT)
        
        # List B
        row_b = ttk.Frame(self.two_lists_frame)
        row_b.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(row_b, text="List B:").pack(side=tk.LEFT)
        ttk.Button(
            row_b,
            text="Upload List B...",
            command=self._upload_list_b
        ).pack(side=tk.LEFT, padx=(10, 5))
        info_label_b = ttk.Label(row_b, text="ℹ️", foreground="blue", cursor="hand2", font=("", 11))
        info_label_b.pack(side=tk.LEFT, padx=(0, 10))
        Tooltip(info_label_b, self._get_entity_list_tooltip())
        self.list_b_status = ttk.Label(row_b, text="No list loaded", foreground="gray")
        self.list_b_status.pack(side=tk.LEFT)
        
        ttk.Label(
            self.two_lists_frame,
            text="Finds connections between any entity in List A and any entity in List B.",
            foreground="gray",
            wraplength=450
        ).pack(anchor="w", pady=(5, 0))


    def _get_entity_list_tooltip(self):
        """Returns the standard entity list tooltip text."""
        return (
            "An entity list is a CSV file with one entity identifier per row.\n\n"
            "Entity identifiers are internal IDs used by the tool:\n"
            "• Companies: Company number (e.g. \"06836076\")\n"
            "• Persons: Generated key (e.g. \"johnsmith-1980-06\")\n"
            "• Addresses: Normalised address string\n\n"
            "Unless you have a list of company numbers, you'll need to use the\n"
            "Data Converter tab to transform your data into a compatible entity list."
        )


    def _update_analyse_mode_ui(self):
        """Shows/hides the appropriate UI based on selected analysis mode."""
        mode = self.analyse_mode_var.get()

        # Hide all frames first
        self.two_entities_frame.pack_forget()
        self.single_list_frame.pack_forget()
        self.two_lists_frame.pack_forget()

        # All modes use path-finding options
        self.max_hops_combo.config(state="readonly")
        self.shortest_only_check.config(state="normal")
        self.enforce_direction_check.config(state="normal")

        # Show the selected frame
        if mode == "two_entities":
            self.two_entities_frame.pack(fill=tk.X)
            self.find_connections_btn.config(text="Find Connections")
        elif mode == "single_list":
            self.single_list_frame.pack(fill=tk.X)
            self.find_connections_btn.config(text="Find Connections & Export...")
        elif mode == "two_lists":
            self.two_lists_frame.pack(fill=tk.X)
            self.find_connections_btn.config(text="Find Connections & Export...")
        self._update_visualise_checkbox_state()

    def _build_visualise_content(self, container):
        """Builds the Visualise section content."""
        
        # --- Display Options ---
        options_frame = ttk.LabelFrame(
            container,
            text="Display Options",
            padding=10,
        )
        options_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.distinguish_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            options_frame,
            text="Colour-code nodes by source file",
            variable=self.distinguish_var
        ).pack(anchor="w", pady=(0, 5))
        
        # Isolated networks option with entity type selection
        isolated_frame = ttk.Frame(options_frame)
        isolated_frame.pack(fill=tk.X, pady=(5, 0))
        
        self.hide_isolated_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            isolated_frame,
            text="Hide isolated networks",
            variable=self.hide_isolated_var,
            command=self._toggle_isolated_options
        ).pack(anchor="w")
        
        self.isolated_options_frame = ttk.Frame(options_frame)
        self.isolated_options_frame.pack(fill=tk.X, padx=(25, 0), pady=(2, 0))
        
        ttk.Label(
            self.isolated_options_frame,
            text="Only show networks containing at least 2:",
            foreground="gray"
        ).pack(anchor="w")
        
        entity_types_frame = ttk.Frame(self.isolated_options_frame)
        entity_types_frame.pack(anchor="w", pady=(2, 0))
        
        self.isolated_companies_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            entity_types_frame,
            text="Companies",
            variable=self.isolated_companies_var
        ).pack(side=tk.LEFT, padx=(0, 15))
        
        self.isolated_persons_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            entity_types_frame,
            text="Persons",
            variable=self.isolated_persons_var
        ).pack(side=tk.LEFT, padx=(0, 15))
        
        self.isolated_addresses_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            entity_types_frame,
            text="Addresses",
            variable=self.isolated_addresses_var
        ).pack(side=tk.LEFT)
        
        # Initially hide isolated options until checkbox ticked
        self.isolated_options_frame.pack_forget()
        
        # Show only highlighted networks option
        self.show_highlighted_only_var = tk.BooleanVar(value=False)
        self.show_highlighted_check = ttk.Checkbutton(
            options_frame,
            text="Show only networks containing connections",
            variable=self.show_highlighted_only_var,
            state="disabled"
        )
        self.show_highlighted_check.pack(anchor="w", pady=(10, 0))

        # Show inferred/hidden connections option
        self.show_inferred_var = tk.BooleanVar(value=False)
        self.show_inferred_check = ttk.Checkbutton(
            options_frame,
            text="Show inferred connections (dotted lines)",
            variable=self.show_inferred_var,
            state="disabled"
        )
        self.show_inferred_check.pack(anchor="w", pady=(5, 0))

        # Scale node size by connection count option
        self.scale_by_connections_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            options_frame,
            text="Scale node size by connection count",
            variable=self.scale_by_connections_var
        ).pack(anchor="w", pady=(5, 0))

        # Generate button
        btn_frame = ttk.Frame(container)
        btn_frame.pack(fill=tk.X, pady=(5, 0))
        
        self.generate_graph_btn = ttk.Button(
            btn_frame,
            text="Generate Network Graph",
            command=self.generate_full_graph
        )
        self.generate_graph_btn.pack(side=tk.LEFT)
        
        ttk.Label(
            btn_frame,
            text="ℹ️ The graph will open in your default web browser.",
            foreground="gray"
        ).pack(side=tk.LEFT, padx=(15, 0))


    def _toggle_isolated_options(self):
        """Shows/hides the entity type options for isolated network filtering."""
        if self.hide_isolated_var.get():
            self.isolated_options_frame.pack(fill=tk.X, padx=(25, 0), pady=(2, 0))
        else:
            self.isolated_options_frame.pack_forget()



    # --- Deduplication Logic ---

    def _open_deduplication_dialog(self):
        """Opens the UI for finding and merging duplicates."""
        
        dialog = tk.Toplevel(self.app)
        dialog.title("Entity Resolution")
        dialog.geometry("950x650")
        
        # --- Configuration Section ---
        config_frame = ttk.LabelFrame(dialog, text="Match Sensitivity", padding=10)
        config_frame.pack(fill=tk.X, padx=10, pady=10)
        
        # Person threshold
        person_row = ttk.Frame(config_frame)
        person_row.pack(fill=tk.X, pady=2)
        ttk.Label(person_row, text="Person match threshold:", width=25, anchor="w").pack(side=tk.LEFT)
        self.person_threshold_var = tk.IntVar(value=85)
        person_spin = ttk.Spinbox(
            person_row,
            from_=50,
            to=99,
            textvariable=self.person_threshold_var,
            width=5
        )
        person_spin.pack(side=tk.LEFT, padx=5)
        ttk.Label(person_row, text="%").pack(side=tk.LEFT)
        Tooltip(person_spin, "Higher = stricter matching (fewer false positives)\nLower = looser matching (catches more variants)")
        
        # Address threshold
        address_row = ttk.Frame(config_frame)
        address_row.pack(fill=tk.X, pady=2)
        ttk.Label(address_row, text="Address match threshold:", width=25, anchor="w").pack(side=tk.LEFT)
        self.address_threshold_var = tk.IntVar(value=80)
        address_spin = ttk.Spinbox(
            address_row,
            from_=50,
            to=99,
            textvariable=self.address_threshold_var,
            width=5
        )
        address_spin.pack(side=tk.LEFT, padx=5)
        ttk.Label(address_row, text="%").pack(side=tk.LEFT)
        Tooltip(address_spin, "Addresses within the same postcode are compared.\nHigher = stricter matching, Lower = looser matching")
        
        # Scan button
        scan_btn_frame = ttk.Frame(config_frame)
        scan_btn_frame.pack(fill=tk.X, pady=(10, 0))
        
        scan_btn = ttk.Button(
            scan_btn_frame,
            text="Scan for Duplicates",
            command=lambda: self._run_duplicate_scan(dialog, results_frame, tree, merge_btn)
        )
        scan_btn.pack(side=tk.LEFT)
        
        self.dedup_status_var = tk.StringVar(value="Adjust thresholds and click 'Scan for Duplicates'.")
        ttk.Label(scan_btn_frame, textvariable=self.dedup_status_var, foreground="gray").pack(side=tk.LEFT, padx=10)

        # --- Results Section ---
        results_frame = ttk.LabelFrame(dialog, text="Potential Duplicates", padding=10)
        results_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        
        ttk.Label(
            results_frame, 
            text="Select the pairs you wish to merge. 'Node B' will be merged into 'Node A'.",
            foreground="gray"
        ).pack(anchor="w", pady=(0, 5))

        # Treeview with scrollbar
        tree_container = ttk.Frame(results_frame)
        tree_container.pack(fill=tk.BOTH, expand=True)
        
        tree_scroll_y = ttk.Scrollbar(tree_container, orient=tk.VERTICAL)
        tree_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        
        cols = ("Type", "Node A", "Node B", "Reason", "Score")
        tree = ttk.Treeview(
            tree_container, 
            columns=cols, 
            show="headings", 
            height=15,
            yscrollcommand=tree_scroll_y.set
        )
        tree_scroll_y.config(command=tree.yview)
        
        tree.heading("Type", text="Type")
        tree.heading("Node A", text="Keep This Node (A)")
        tree.heading("Node B", text="Merge/Delete This Node (B)")
        tree.heading("Reason", text="Reason")
        tree.heading("Score", text="Score")
        
        tree.column("Type", width=70, anchor="center")
        tree.column("Node A", width=280)
        tree.column("Node B", width=280)
        tree.column("Reason", width=180)
        tree.column("Score", width=60, anchor="center")
        
        tree.pack(fill=tk.BOTH, expand=True)
        
        # Store reference to candidates for merging
        self._dedup_candidates = []

        # --- Action Buttons ---
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)
        
        ttk.Button(btn_frame, text="Close", command=dialog.destroy).pack(side=tk.RIGHT, padx=5)
        
        merge_btn = ttk.Button(
            btn_frame, 
            text="Merge Selected", 
            command=lambda: self._execute_merge_from_dialog(dialog, tree),
            state="disabled"
        )
        merge_btn.pack(side=tk.RIGHT, padx=5)
        
        select_all_btn = ttk.Button(
            btn_frame,
            text="Select All",
            command=lambda: tree.selection_set(tree.get_children()),
            state="disabled"
        )
        select_all_btn.pack(side=tk.LEFT, padx=5)
        
        deselect_all_btn = ttk.Button(
            btn_frame,
            text="Deselect All", 
            command=lambda: tree.selection_remove(tree.get_children()),
            state="disabled"
        )
        deselect_all_btn.pack(side=tk.LEFT, padx=5)
        
        # Store button references for enabling after scan
        self._dedup_select_all_btn = select_all_btn
        self._dedup_deselect_all_btn = deselect_all_btn


    def _scan_for_duplicates(
        self, 
        person_threshold: int = 85, 
        address_threshold: int = 80
    ) -> List[Tuple]:
        candidates = []
        
        people_blocks = {}
        addr_blocks = {}
        
        # Titles to strip for cleaner parsing
        TITLES = ["MR", "MRS", "MS", "MISS", "DR", "PROF", "SIR", "DAME", "REV", "CLLR", "CAPTAIN"]
        
        for node_id, attrs in self.full_graph.nodes(data=True):
            ntype = attrs.get("type")
            label = attrs.get("label", "")
            
            if ntype == "address":
                pc = self._extract_postcode(label) or self._extract_postcode(node_id)
                if pc:
                    key = pc 
                    if key not in addr_blocks: 
                        addr_blocks[key] = []
                    addr_blocks[key].append((node_id, label))
            
            elif ntype == "person":
                # --- STEP 1: Parse Name correctly (Handle "Surname, Firstname") ---
                raw_surname = ""
                raw_forenames = ""
                
                if "," in label:
                    # Format: "LORD-MARCHIONNE, Sacha John"
                    parts = label.split(",", 1)
                    raw_surname = parts[0].strip()
                    raw_forenames = parts[1].strip()
                else:
                    # Format: "Sacha John Lord-Marchionne" (Fallback)
                    parts = label.split()
                    if parts:
                        raw_surname = parts[-1]
                        raw_forenames = " ".join(parts[:-1])
                
                # --- STEP 2: Extract Year ---
                year = "UNKNOWN"
                if "-" in str(node_id):
                    try:
                        parts = str(node_id).rsplit("-", 2)
                        if len(parts) == 3 and len(parts[1]) == 4:
                            year = parts[1]
                    except (ValueError, IndexError):
                        pass

                # --- STEP 3: Generate Blocking Keys (Surnames) ---
                # Clean surname: "LORD-MARCHIONNE" -> "LORD MARCHIONNE"
                clean_surname = raw_surname.replace("-", " ").upper()
                surname_tokens = re.sub(r'[^a-zA-Z\s]', '', clean_surname).split()
                
                if not surname_tokens: continue

                # Generate keys for EVERY part of the surname
                # e.g. "LORD MARCHIONNE" -> blocks "LORD" and "MARCHIONNE"
                surname_candidates = set(surname_tokens)
                
                # Also add the hyphenated original if it existed
                if "-" in raw_surname:
                     surname_candidates.add(re.sub(r'[^a-zA-Z-]', '', raw_surname).upper())

                # Get Initial from forename (strip titles first)
                clean_forenames = re.sub(r'[^a-zA-Z\s]', '', raw_forenames).upper().split()
                filtered_forenames = [n for n in clean_forenames if n not in TITLES]
                
                initial = "?"
                if filtered_forenames:
                    initial = filtered_forenames[0][0]
                elif clean_forenames:
                    initial = clean_forenames[0][0]

                # Create normalized "First Last" string for the fuzzy comparison step later
                # This ensures "Sacha Lord MARCHIONNE" and "LORD-MARCHIONNE, Sacha" look similar
                normalized_name = f"{' '.join(filtered_forenames)} {' '.join(surname_tokens)}"

                # Add to buckets
                for s in surname_candidates:
                    key = f"{s}|{initial}"
                    if key not in people_blocks: people_blocks[key] = []
                    
                    # Store (ID, Original Label, Normalized Name, Year)
                    entry = (node_id, label, normalized_name, year)
                    # Simple de-dupe to avoid adding same node to same bucket twice
                    if entry not in people_blocks[key]:
                        people_blocks[key].append(entry)

        # --- 1. Address Matching ---
        for key, nodes in addr_blocks.items():
            if len(nodes) < 2: continue
            for i in range(len(nodes)):
                for j in range(i + 1, len(nodes)):
                    id_a, lbl_a = nodes[i]
                    id_b, lbl_b = nodes[j]
                    
                    ratio = difflib.SequenceMatcher(None, lbl_a.lower(), lbl_b.lower()).ratio()
                    score = ratio * 100
                    
                    if score > address_threshold: 
                        reason = f"Same Postcode ({key})"
                        candidates.append((id_a, lbl_a, id_b, lbl_b, reason, score, "address"))

        # --- 2. Person Matching ---
        processed_pairs = set()
        for key, nodes in people_blocks.items():
            if len(nodes) < 2: continue
            for i in range(len(nodes)):
                for j in range(i + 1, len(nodes)):
                    # Unpack the new 4-item tuple
                    id_a, lbl_a, norm_a, year_a = nodes[i]
                    id_b, lbl_b, norm_b, year_b = nodes[j]
                    
                    if id_a == id_b: continue
                    
                    pair_key = tuple(sorted([id_a, id_b]))
                    if pair_key in processed_pairs: continue
                    processed_pairs.add(pair_key)
                    
                    if year_a != "UNKNOWN" and year_b != "UNKNOWN" and year_a != year_b:
                        continue 

                    # Compare the NORMALIZED strings (Firstname Surname format)
                    # This is key: it makes "LORD, Sacha" look like "Sacha LORD"
                    ratio = difflib.SequenceMatcher(None, norm_a, norm_b).ratio()
                    score = ratio * 100
                    
                    # Token Subset Check
                    tokens_a = set(norm_a.split())
                    tokens_b = set(norm_b.split())
                    
                    is_subset = tokens_a.issubset(tokens_b) or tokens_b.issubset(tokens_a)
                    
                    reason_extra = ""
                    if is_subset and len(tokens_a.intersection(tokens_b)) >= 2:
                        score = max(score, 98) # Boost confidence
                        reason_extra = " (Name Subset)"

                    if score > person_threshold: 
                        if year_a == year_b and year_a != "UNKNOWN":
                            match_type = f"Same Year ({year_a})"
                        elif year_a == "UNKNOWN" or year_b == "UNKNOWN":
                            match_type = "Potential Match (One DOB Missing)"
                        else:
                            match_type = "Name Match"

                        reason = f"{match_type}{reason_extra} [Block: {key.replace('|', ' ')}]"
                        candidates.append((id_a, lbl_a, id_b, lbl_b, reason, score, "person"))

        candidates.sort(key=lambda x: x[5], reverse=True)
        return candidates

    def _run_duplicate_scan(self, dialog, results_frame, tree, merge_btn):
        """Runs the duplicate scan with current threshold settings."""
        # Clear previous results
        for item in tree.get_children():
            tree.delete(item)
        self._dedup_candidates = []
        
        self.dedup_status_var.set("Scanning...")
        dialog.update_idletasks()
        
        # Get thresholds
        person_threshold = self.person_threshold_var.get()
        address_threshold = self.address_threshold_var.get()
        
        # Run scan
        candidates = self._scan_for_duplicates(
            person_threshold=person_threshold,
            address_threshold=address_threshold
        )
        
        if not candidates:
            self.dedup_status_var.set("No duplicates found at current thresholds. Try lowering the values.")
            merge_btn.config(state="disabled")
            self._dedup_select_all_btn.config(state="disabled")
            self._dedup_deselect_all_btn.config(state="disabled")
            return
        
        # Populate tree
        self._dedup_candidates = candidates
        for idx, (id_a, label_a, id_b, label_b, reason, score, entity_type) in enumerate(candidates):
            tree.insert(
                "", 
                "end", 
                iid=str(idx), 
                values=(
                    entity_type.title(),
                    label_a[:50] + "..." if len(label_a) > 50 else label_a,
                    label_b[:50] + "..." if len(label_b) > 50 else label_b,
                    reason,
                    f"{int(score)}%"
                )
            )
        
        self.dedup_status_var.set(f"Found {len(candidates)} potential duplicate pairs.")
        merge_btn.config(state="normal")
        self._dedup_select_all_btn.config(state="normal")
        self._dedup_deselect_all_btn.config(state="normal")


    def _execute_merge_from_dialog(self, dialog, tree):
        """Executes merges for selected items in the deduplication dialog."""
        selected_items = tree.selection()
        if not selected_items:
            messagebox.showwarning("No Selection", "Please select rows to merge.")
            return
        
        confirm = messagebox.askyesno(
            "Confirm Merge", 
            f"Are you sure you want to merge {len(selected_items)} pairs?\n\nThis cannot be undone."
        )
        if not confirm:
            return
        
        pairs_to_merge = []
        for item in selected_items:
            idx = int(item)
            # Extract the first 6 elements (excluding entity_type which we added)
            candidate = self._dedup_candidates[idx]
            pairs_to_merge.append(candidate[:6])
        
        self._execute_merges(pairs_to_merge)
        self._populate_node_dropdowns()
        
        messagebox.showinfo("Success", f"Merged {len(pairs_to_merge)} pairs.")
        dialog.destroy()


    def _execute_merges(self, pairs):
        """Merges node B into node A for all pairs."""
        G = self.full_graph
        
        for (id_a, _, id_b, _, _, _) in pairs:
            if not G.has_node(id_a) or not G.has_node(id_b):
                continue # Already merged or gone
                
            # Rewire edges
            # Incoming to B -> Point to A
            in_edges = list(G.in_edges(id_b, data=True))
            for src, _, data in in_edges:
                if not G.has_edge(src, id_a):
                    G.add_edge(src, id_a, **data)
            
            # Outgoing from B -> Start from A
            out_edges = list(G.out_edges(id_b, data=True))
            for _, tgt, data in out_edges:
                if not G.has_edge(id_a, tgt):
                    G.add_edge(id_a, tgt, **data)
            
            # Merge attributes (Source files)
            sf_a = G.nodes[id_a].get("source_files", set())
            sf_b = G.nodes[id_b].get("source_files", set())
            G.nodes[id_a]["source_files"] = sf_a.union(sf_b)
            
            # Remove B
            G.remove_node(id_b)

    def _extract_postcode(self, text):
        # Robust UK Postcode Regex
        # Matches: SW1A 1AA, M1 1AA, etc.
        pattern = r'\b[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}\b'
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(0).upper().replace(" ", "") # Return normalized
        return None

    def _extract_house_number(self, text):
        # Find first sequence of digits
        match = re.search(r'\b\d+\b', text)
        if match:
            return match.group(0)
        return None

    # --- Converter Logic ---

    def _converter_load_file(self):
        path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if not path:
            return
        
        try:
            self.converter_source_data = []
            with open(path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                self.converter_headers = reader.fieldnames
                for row in reader:
                    self.converter_source_data.append(row)
            
            if not self.converter_headers:
                raise ValueError("No headers found.")

            # Update UI
            self.converter_file_label.config(text=os.path.basename(path))
            
            # Update Combos
            combos = [self.combo_id_col, self.combo_sec_col, self.combo_addr_full, self.combo_addr_line1, self.combo_addr_postcode]
            options = [""] + self.converter_headers
            for c in combos:
                c['values'] = options
                c.set("")

            # Update Preview
            self.converter_preview_tree['columns'] = self.converter_headers
            for col in self.converter_headers:
                self.converter_preview_tree.heading(col, text=col)
                self.converter_preview_tree.column(col, width=100)
            
            for item in self.converter_preview_tree.get_children():
                self.converter_preview_tree.delete(item)
            
            for i, row in enumerate(self.converter_source_data[:5]):
                vals = [row.get(h, "") for h in self.converter_headers]
                self.converter_preview_tree.insert("", "end", values=vals)

            self.convert_btn.config(state="normal")
            self.converter_status.config(text="File loaded. Please map columns.")

        except Exception as e:
            self.app.after(0, lambda: messagebox.showerror("Load Error", f"Could not load CSV: {e}"))

    # --- Section State Management ---

    def _on_refine_section_expanded(self):
        """Called when Build & Refine section is expanded. Triggers auto-build if needed."""
        if not self.graph_built or self.files_changed_since_build:
            self._auto_build_graph()


    def _auto_build_graph(self):
        """Automatically builds the graph when entering Build & Refine."""
        if not self.source_files:
            return
        
        self.full_graph.clear()
        self.highly_connected_exclusions.clear()
        self.peripheral_exclusions.clear()
        self.manual_exclusions.clear()
        
        # Define the absolute minimum headers required for the code to not crash
        REQUIRED_HEADERS = {"source_id", "source_label", "source_type"}
        
        try:
            for filepath in self.source_files:
                filename = os.path.basename(filepath)
                
                # Pre-scan check for headers
                with open(filepath, "r", encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    
                    # If file is empty or headers are missing
                    if not reader.fieldnames:
                         messagebox.showerror(
                            "File Error", 
                            f"The file '{filename}' appears to be empty or unreadable."
                        )
                         return

                    # Check for missing columns
                    file_headers = set(reader.fieldnames)
                    missing = REQUIRED_HEADERS - file_headers
                    
                    if missing:
                        missing_str = ", ".join(list(missing))
                        messagebox.showerror(
                            "Invalid File Format",
                            f"The file '{filename}' cannot be loaded.\n\n"
                            f"Missing required columns: {missing_str}\n\n"
                            "Expected columns:\n"
                            "source_id, source_label, source_type, target_id, target_label, target_type, relationship\n\n"
                            "Please use the 'Data Converter' tab to format this file correctly."
                        )
                        return

                    # If headers are good, process the rows
                    for row in reader:
                        self._add_edge_to_graph(row, filename)
            
            self.graph_built = True
            self.files_changed_since_build = False
            
            # Update section header
            node_count = self.full_graph.number_of_nodes()
            edge_count = self.full_graph.number_of_edges()
            self.refine_section.set_status(f"{node_count:,} nodes · {edge_count:,} edges")
            self.refine_section.clear_warning()
            
            # Enable other sections
            self.analyse_section.set_enabled(True)
            self.visualise_section.set_enabled(True)
            
            # Populate dropdowns
            self._populate_node_dropdowns()
            
            # Update exclusion status labels
            self._update_exclusion_status_labels()
            
        except Exception as e:
            log_message(f"Error auto-building graph: {e}")
            messagebox.showerror("Build Error", f"Could not build graph: {e}")


    def _update_section_header_status(self):
        """Updates the Build & Refine section header with current stats."""
        if not self.graph_built:
            self.refine_section.set_status("")
            return
        
        node_count = self.full_graph.number_of_nodes()
        edge_count = self.full_graph.number_of_edges()
        total_exclusions = len(self.highly_connected_exclusions) + len(self.peripheral_exclusions) + len(self.manual_exclusions)
        
        status = f"{node_count:,} nodes · {edge_count:,} edges"
        if total_exclusions > 0:
            status += f" ({total_exclusions} excluded)"
        
        self.refine_section.set_status(status)


    def _mark_files_changed(self):
        """Called when Data Sources change to trigger rebuild requirement."""
        if self.graph_built:
            self.files_changed_since_build = True
            self.refine_section.set_warning("Files changed", show_rebuild=True, rebuild_callback=self._auto_build_graph)


    def _update_exclusion_status_labels(self):
        """Updates the exclusion count labels in Build & Refine."""
        hc_count = len(self.highly_connected_exclusions)
        pn_count = len(self.peripheral_exclusions)
        
        if hc_count > 0:
            self.hc_status_label.config(text=f"Excluding {hc_count} node(s)", foreground="green")
        else:
            self.hc_status_label.config(text="No exclusions", foreground="gray")
        
        if pn_count > 0:
            self.pn_status_label.config(text=f"Excluding {pn_count} node(s)", foreground="green")
        else:
            self.pn_status_label.config(text="No exclusions", foreground="gray")
        
        self._update_section_header_status()


    # --- Highly Connected Nodes ---

    def _open_highly_connected_dialog(self):
        """Opens dialog to scan and exclude highly connected nodes."""
        try:
            threshold = int(self.highly_connected_threshold_var.get())
        except ValueError:
            messagebox.showerror("Invalid Threshold", "Please enter a valid number for the threshold.")
            return
        
        if not self.graph_built:
            messagebox.showwarning("No Graph", "Please load data first.")
            return
        
        # Find nodes above threshold
        candidates = []
        for node_id, attrs in self.full_graph.nodes(data=True):
            degree = self.full_graph.degree(node_id)
            if degree > threshold and node_id not in self.highly_connected_exclusions:
                candidates.append((
                    node_id,
                    attrs.get("label", node_id),
                    attrs.get("type", "unknown"),
                    degree
                ))
        
        candidates.sort(key=lambda x: x[3], reverse=True)
        
        if not candidates:
            messagebox.showinfo(
                "No Results",
                f"No nodes found with more than {threshold} connections\n"
                f"(excluding already excluded nodes)."
            )
            return
        
        # Create dialog
        dialog = tk.Toplevel(self.app)
        dialog.title("Highly Connected Nodes")
        dialog.geometry("700x500")
        
        ttk.Label(
            dialog,
            text=f"Found {len(candidates)} node(s) with more than {threshold} connections.",
            padding=10
        ).pack(anchor="w")
        
        # Treeview
        tree_frame = ttk.Frame(dialog)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        
        tree_scroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
        tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        cols = ("Type", "Node", "Connections")
        tree = ttk.Treeview(tree_frame, columns=cols, show="headings", yscrollcommand=tree_scroll.set)
        tree_scroll.config(command=tree.yview)
        
        tree.heading("Type", text="Type")
        tree.heading("Node", text="Node")
        tree.heading("Connections", text="Connections")
        
        tree.column("Type", width=80, anchor="center")
        tree.column("Node", width=450)
        tree.column("Connections", width=100, anchor="center")
        
        tree.pack(fill=tk.BOTH, expand=True)
        
        for node_id, label, ntype, degree in candidates:
            display_label = label[:60] + "..." if len(label) > 60 else label
            tree.insert("", "end", iid=node_id, values=(ntype.title(), display_label, degree))
        
        # Buttons
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)
        
        def exclude_selected():
            selected = tree.selection()
            if not selected:
                messagebox.showwarning("No Selection", "Please select nodes to exclude.")
                return
            
            for node_id in selected:
                self.highly_connected_exclusions.add(node_id)
            
            self._update_exclusion_status_labels()
            messagebox.showinfo("Success", f"Excluded {len(selected)} node(s).")
            dialog.destroy()
        
        ttk.Button(btn_frame, text="Exclude Selected", command=exclude_selected).pack(side=tk.RIGHT, padx=5)
        ttk.Button(btn_frame, text="Close", command=dialog.destroy).pack(side=tk.RIGHT, padx=5)
        ttk.Button(
            btn_frame,
            text="Select All",
            command=lambda: tree.selection_set(tree.get_children())
        ).pack(side=tk.LEFT, padx=5)


    def _open_peripheral_dialog(self):
        """Opens dialog to scan and exclude peripheral nodes."""
        try:
            threshold = int(self.peripheral_threshold_var.get())
        except ValueError:
            messagebox.showerror("Invalid Threshold", "Please enter a valid number for the threshold.")
            return
        
        if not self.graph_built:
            messagebox.showwarning("No Graph", "Please load data first.")
            return
        
        # Find nodes below threshold
        candidates = []
        for node_id, attrs in self.full_graph.nodes(data=True):
            degree = self.full_graph.degree(node_id)
            if degree < threshold and node_id not in self.peripheral_exclusions:
                candidates.append((
                    node_id,
                    attrs.get("label", node_id),
                    attrs.get("type", "unknown"),
                    degree
                ))
        
        candidates.sort(key=lambda x: x[3])
        
        if not candidates:
            messagebox.showinfo(
                "No Results",
                f"No nodes found with fewer than {threshold} connections\n"
                f"(excluding already excluded nodes)."
            )
            return
        
        # Create dialog
        dialog = tk.Toplevel(self.app)
        dialog.title("Peripheral Nodes")
        dialog.geometry("700x500")
        
        ttk.Label(
            dialog,
            text=f"Found {len(candidates)} node(s) with fewer than {threshold} connections.",
            padding=10
        ).pack(anchor="w")
        
        # Treeview
        tree_frame = ttk.Frame(dialog)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        
        tree_scroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
        tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        cols = ("Type", "Node", "Connections")
        tree = ttk.Treeview(tree_frame, columns=cols, show="headings", yscrollcommand=tree_scroll.set)
        tree_scroll.config(command=tree.yview)
        
        tree.heading("Type", text="Type")
        tree.heading("Node", text="Node")
        tree.heading("Connections", text="Connections")
        
        tree.column("Type", width=80, anchor="center")
        tree.column("Node", width=450)
        tree.column("Connections", width=100, anchor="center")
        
        tree.pack(fill=tk.BOTH, expand=True)
        
        for node_id, label, ntype, degree in candidates:
            display_label = label[:60] + "..." if len(label) > 60 else label
            tree.insert("", "end", iid=node_id, values=(ntype.title(), display_label, degree))
        
        # Buttons
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)
        
        def exclude_selected():
            selected = tree.selection()
            if not selected:
                messagebox.showwarning("No Selection", "Please select nodes to exclude.")
                return
            
            for node_id in selected:
                self.peripheral_exclusions.add(node_id)
            
            self._update_exclusion_status_labels()
            messagebox.showinfo("Success", f"Excluded {len(selected)} node(s).")
            dialog.destroy()
        
        ttk.Button(btn_frame, text="Exclude Selected", command=exclude_selected).pack(side=tk.RIGHT, padx=5)
        ttk.Button(btn_frame, text="Close", command=dialog.destroy).pack(side=tk.RIGHT, padx=5)
        ttk.Button(
            btn_frame,
            text="Select All",
            command=lambda: tree.selection_set(tree.get_children())
        ).pack(side=tk.LEFT, padx=5)


    def _open_exclusion_manager(self):
        """Opens dialog to manage all exclusions."""
        if not self.graph_built:
            messagebox.showwarning("No Graph", "Please load data first.")
            return
        
        all_exclusions = []
        
        for node_id in self.highly_connected_exclusions:
            if self.full_graph.has_node(node_id):
                attrs = self.full_graph.nodes[node_id]
                degree = self.full_graph.degree(node_id)
                all_exclusions.append((
                    node_id,
                    attrs.get("label", node_id),
                    attrs.get("type", "unknown"),
                    f"{degree} connections",
                    "Highly connected"
                ))
        
        for node_id in self.peripheral_exclusions:
            if self.full_graph.has_node(node_id):
                attrs = self.full_graph.nodes[node_id]
                degree = self.full_graph.degree(node_id)
                all_exclusions.append((
                    node_id,
                    attrs.get("label", node_id),
                    attrs.get("type", "unknown"),
                    f"{degree} connections",
                    "Peripheral"
                ))
        
        for node_id in self.manual_exclusions:
            if self.full_graph.has_node(node_id):
                attrs = self.full_graph.nodes[node_id]
                degree = self.full_graph.degree(node_id)
                all_exclusions.append((
                    node_id,
                    attrs.get("label", node_id),
                    attrs.get("type", "unknown"),
                    f"{degree} connections",
                    "Manual"
                ))
        
        if not all_exclusions:
            messagebox.showinfo("No Exclusions", "No nodes are currently excluded.")
            return
        
        # Create dialog
        dialog = tk.Toplevel(self.app)
        dialog.title("Manage Exclusions")
        dialog.geometry("800x500")
        
        # Filter dropdown
        filter_frame = ttk.Frame(dialog)
        filter_frame.pack(fill=tk.X, padx=10, pady=10)
        
        ttk.Label(filter_frame, text="Filter:").pack(side=tk.LEFT)
        filter_var = tk.StringVar(value="All")
        filter_combo = ttk.Combobox(
            filter_frame,
            textvariable=filter_var,
            values=["All", "Highly connected", "Peripheral", "Manual"],
            state="readonly",
            width=20
        )
        filter_combo.pack(side=tk.LEFT, padx=(5, 0))
        
        # Treeview
        tree_frame = ttk.Frame(dialog)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        
        tree_scroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
        tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        cols = ("Type", "Node", "Details", "Reason")
        tree = ttk.Treeview(tree_frame, columns=cols, show="headings", yscrollcommand=tree_scroll.set)
        tree_scroll.config(command=tree.yview)
        
        tree.heading("Type", text="Type")
        tree.heading("Node", text="Node")
        tree.heading("Details", text="Details")
        tree.heading("Reason", text="Reason")
        
        tree.column("Type", width=80, anchor="center")
        tree.column("Node", width=350)
        tree.column("Details", width=120, anchor="center")
        tree.column("Reason", width=120, anchor="center")
        
        tree.pack(fill=tk.BOTH, expand=True)
        
        # Store node_id -> reason mapping for restoration
        exclusion_map = {}
        
        def populate_tree(filter_value="All"):
            tree.delete(*tree.get_children())
            exclusion_map.clear()
            
            for node_id, label, ntype, details, reason in all_exclusions:
                if filter_value != "All" and reason != filter_value:
                    continue
                display_label = label[:50] + "..." if len(label) > 50 else label
                tree.insert("", "end", iid=node_id, values=(ntype.title(), display_label, details, reason))
                exclusion_map[node_id] = reason
        
        populate_tree()
        
        def on_filter_change(event=None):
            populate_tree(filter_var.get())
        
        filter_combo.bind("<<ComboboxSelected>>", on_filter_change)
        
        # Buttons
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)
        
        def restore_selected():
            selected = tree.selection()
            if not selected:
                messagebox.showwarning("No Selection", "Please select nodes to restore.")
                return
            
            for node_id in selected:
                reason = exclusion_map.get(node_id)
                if reason == "Highly connected":
                    self.highly_connected_exclusions.discard(node_id)
                elif reason == "Peripheral":
                    self.peripheral_exclusions.discard(node_id)
                elif reason == "Manual":
                    self.manual_exclusions.discard(node_id)
            
            self._update_exclusion_status_labels()
            messagebox.showinfo("Success", f"Restored {len(selected)} node(s).")
            dialog.destroy()
        
        def clear_all():
            if not messagebox.askyesno("Confirm", "Clear all exclusions?"):
                return
            
            self.highly_connected_exclusions.clear()
            self.peripheral_exclusions.clear()
            self.manual_exclusions.clear()
            
            self._update_exclusion_status_labels()
            messagebox.showinfo("Success", "All exclusions cleared.")
            dialog.destroy()
        
        ttk.Button(btn_frame, text="Close", command=dialog.destroy).pack(side=tk.RIGHT, padx=5)
        ttk.Button(btn_frame, text="Restore Selected", command=restore_selected).pack(side=tk.RIGHT, padx=5)
        ttk.Button(btn_frame, text="Clear All Exclusions", command=clear_all).pack(side=tk.LEFT, padx=5)


    def _add_manual_exclusion(self):
        """Adds a manually selected node to exclusions."""
        selection = self.manual_exclude_entry.get()
        if not selection:
            return
        
        try:
            node_id = selection.split("(")[-1].strip(")")
            if node_id in self.full_graph:
                self.manual_exclusions.add(node_id)
                self.manual_exclude_entry.var.set("")
                self._update_exclusion_status_labels()
                messagebox.showinfo("Success", f"Node excluded.")
            else:
                messagebox.showwarning("Not Found", "Node not found in graph.")
        except (IndexError, ValueError):
            messagebox.showwarning("Invalid Selection", "Please select a valid node.")


    # --- Entity List Upload Handlers ---

    def _upload_single_entity_list(self):
        """Uploads entity list for single-list analysis mode."""
        path = filedialog.askopenfilename(
            title="Select Entity List CSV",
            filetypes=[("CSV Files", "*.csv")]
        )
        if not path:
            return
        
        try:
            entity_ids = self._load_entity_list_file(path)
            self.analyse_entity_list = entity_ids
            self.analyse_entity_list_path = path
            self.single_list_status.config(
                text=f"{len(entity_ids)} entities loaded",
                foreground="green"
            )
            self._update_visualise_checkbox_state()
            
            
        except Exception as e:
            messagebox.showerror("Load Error", f"Could not load entity list: {e}")


    def _upload_list_a(self):
        """Uploads List A for two-list analysis mode."""
        path = filedialog.askopenfilename(
            title="Select List A CSV",
            filetypes=[("CSV Files", "*.csv")]
        )
        if not path:
            return
        
        try:
            entity_ids = self._load_entity_list_file(path)
            self.cohort_a_ids = entity_ids
            self.list_a_status.config(
                text=f"{len(entity_ids)} entities loaded",
                foreground="green"
            )
            self._update_visualise_checkbox_state()
        except Exception as e:
            messagebox.showerror("Load Error", f"Could not load entity list: {e}")


    def _upload_list_b(self):
        """Uploads List B for two-list analysis mode."""
        path = filedialog.askopenfilename(
            title="Select List B CSV",
            filetypes=[("CSV Files", "*.csv")]
        )
        if not path:
            return
        
        try:
            entity_ids = self._load_entity_list_file(path)
            self.cohort_b_ids = entity_ids
            self.list_b_status.config(
                text=f"{len(entity_ids)} entities loaded",
                foreground="green"
            )
            self._update_visualise_checkbox_state()
        except Exception as e:
            messagebox.showerror("Load Error", f"Could not load entity list: {e}")


    def _load_entity_list_file(self, path):
        """Loads entity IDs from a CSV file. Returns a set of IDs."""
        entity_ids = set()
        
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        if not rows:
            raise ValueError("File is empty.")
        
        # Heuristic: skip header if it looks like one
        first_val = rows[0][0].strip() if rows[0] else ""
        is_likely_header = (
            first_val.lower() in ("id", "entity_id", "company_number", "name", "cohort_id", "identifier", "source_id")
            or not first_val
        )
        
        start_idx = 1 if is_likely_header else 0
        
        for row in rows[start_idx:]:
            if row:
                entity_id = row[0].strip()
                if entity_id:
                    entity_ids.add(entity_id)
        
        return entity_ids


    # --- Connection Finding ---

    def _execute_find_connections(self):
        """Executes the connection search based on selected mode."""
        mode = self.analyse_mode_var.get()

        if mode == "two_entities":
            self._find_connection_two_entities()
        elif mode == "single_list":
            self._find_connections_single_list()
        elif mode == "two_lists":
            self._find_connections_two_lists()


    def _find_connection_two_entities(self):
        """Finds path between two specific entities, respecting Max Hops limit."""
        pruned_graph = self._get_pruned_graph()
        max_hops = self.max_hops_var.get()

        start_selection = self.start_node_entry.get()
        end_selection = self.end_node_entry.get()

        if not start_selection or not end_selection:
            messagebox.showerror("Input Error", "Please select both entities.")
            return

        try:
            start_id = start_selection.split("(")[-1].strip(")")
            end_id = end_selection.split("(")[-1].strip(")")
        except IndexError:
            messagebox.showerror("Input Error", "Invalid node selection.")
            return

        if start_id not in pruned_graph or end_id not in pruned_graph:
            messagebox.showwarning("Node Not Found", "Selected nodes do not exist in graph.")
            return

        try:
            graph_to_search = (
                pruned_graph.to_undirected()
                if not self.enforce_direction_var.get()
                else pruned_graph
            )
            path = nx.shortest_path(graph_to_search, source=start_id, target=end_id)

            # Check if path exceeds max hops (path length - 1 = number of hops)
            path_hops = len(path) - 1
            if path_hops > max_hops:
                messagebox.showinfo(
                    "No Path Within Limit",
                    f"The shortest path is {path_hops} hops, which exceeds the maximum of {max_hops} hops.\n\n"
                    "Increase Max Hops or try different entities."
                )
                return

            path_details = f"Connection Path Found ({path_hops} hops):\n\n"
            for i, node_id in enumerate(path):
                node_label = pruned_graph.nodes[node_id].get("label", node_id)
                path_details += f"{i+1}. {node_label}\n"

            result = messagebox.askyesno(
                "Path Found",
                f"{path_details}\n\nGenerate visual graph with path highlighted?"
            )

            if result:
                self._generate_highlighted_graph(pruned_graph, path)

        except nx.NetworkXNoPath:
            messagebox.showinfo("No Path", "No connection could be found between these entities.")
        except Exception as e:
            log_message(f"Pathfinding error: {e}")
            messagebox.showerror("Error", f"An error occurred: {e}")


    def _find_connections_single_list(self):
        """Finds connections within a single entity list."""
        if not self.analyse_entity_list:
            messagebox.showwarning("No List", "Please upload an entity list first.")
            return
        
        if not self.graph_built:
            messagebox.showwarning("No Graph", "Please build the graph first.")
            return
        
        # Use cohort A/B infrastructure but with same list
        self.cohort_a_ids = self.analyse_entity_list.copy()
        self.cohort_b_ids = self.analyse_entity_list.copy()
        
        output_filepath = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            title="Save Connection Paths As",
        )
        if not output_filepath:
            return
        
        # Use existing cohort connection thread
        self.find_connections_btn.config(state="disabled")
        self.analyse_status_var.set("Starting search...")
        threading.Thread(
            target=self._run_cohort_connection_thread,
            args=(output_filepath,),
            daemon=True,
        ).start()


    def _find_connections_two_lists(self):
        """Finds connections between two entity lists."""
        if not self.cohort_a_ids or not self.cohort_b_ids:
            messagebox.showwarning("Missing Lists", "Please upload both List A and List B.")
            return
        
        if not self.graph_built:
            messagebox.showwarning("No Graph", "Please build the graph first.")
            return
        
        output_filepath = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            title="Save Connection Paths As",
        )
        if not output_filepath:
            return
        
        self.find_connections_btn.config(state="disabled")
        self.analyse_status_var.set("Starting search...")
        threading.Thread(
            target=self._run_cohort_connection_thread,
            args=(output_filepath,),
            daemon=True,
        ).start()


    def _haversine_distance(self, lat1, lon1, lat2, lon2):
        """Calculate the great-circle distance between two points in kilometres."""
        R = 6371  # Earth's radius in kilometres

        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)

        a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return R * c

    def _get_all_neighbors(self, entity_id, graph):
        """Get all neighbors (any type) of an entity for 2-hop check."""
        neighbors = set()
        for neighbor in list(graph.successors(entity_id)) + list(graph.predecessors(entity_id)):
            neighbors.add(neighbor)
        return neighbors

    def _scan_for_hidden_links(self):
        """
        Scans for hidden links.
        Logic:
        1. Address-to-Address: Proximity Only (Neighbours)
        2. Person-to-Person: Surname AND (Proximity OR Shared Postcode)
        """
        if not self.graph_built:
            messagebox.showwarning("No Graph", "Please build the graph first.")
            return

        try:
            radius_km = float(self.proximity_radius_var.get())
        except ValueError:
            messagebox.showwarning("Invalid Radius", "Please enter a valid number for the radius.")
            return

        self.discovered_hidden_links = []
        pruned_graph = self._get_pruned_graph()

        try:
            nomi = pgeocode.Nominatim('gb')
        except Exception as e:
            messagebox.showerror("Error", f"Could not initialize postcode lookup: {e}")
            return

        # --- Step 1: Map Addresses & Postcodes ---
        address_nodes = [] 
        postcode_coords = {}
        failed_geocodes = 0
        
        for node_id, attrs in pruned_graph.nodes(data=True):
            if attrs.get("type") == "address":
                raw_label = attrs.get("label", node_id)
                
                # Extract and normalize postcode
                pc = self._extract_postcode(raw_label) or self._extract_postcode(node_id)
                clean_pc = pc.upper().replace(" ", "") if pc else None
                
                if clean_pc and len(clean_pc) >= 5:
                    # Format for pgeocode (e.g., "SW1A 1AA")
                    formatted_pc = clean_pc[:-3] + " " + clean_pc[-3:]
                    
                    # Store (ID, Label, CleanPostcode, FormattedPostcode)
                    address_nodes.append((node_id, raw_label, clean_pc, formatted_pc))
                    
                    # Geocode if new
                    if formatted_pc not in postcode_coords:
                        res = nomi.query_postal_code(formatted_pc)
                        if res is not None and not math.isnan(res.latitude):
                            postcode_coords[formatted_pc] = (res.latitude, res.longitude)
                        else:
                            failed_geocodes += 1
                            postcode_coords[formatted_pc] = (None, None)

        # --- Step 2: Address-to-Address (Neighbours) ---
        # (Same logic as before - linking buildings via proximity)
        for i in range(len(address_nodes)):
            for j in range(i + 1, len(address_nodes)):
                id_a, label_a, _, pc_fmt_a = address_nodes[i]
                id_b, label_b, _, pc_fmt_b = address_nodes[j]
                
                if pruned_graph.has_edge(id_a, id_b) or pruned_graph.has_edge(id_b, id_a):
                    continue
                
                lat_a, lon_a = postcode_coords.get(pc_fmt_a, (None, None))
                lat_b, lon_b = postcode_coords.get(pc_fmt_b, (None, None))
                
                if lat_a and lat_b:
                    dist = self._haversine_distance(lat_a, lon_a, lat_b, lon_b)
                    if dist <= radius_km:
                        self.discovered_hidden_links.append({
                            "Entity A": label_a, "Type A": "Address",
                            "Entity B": label_b, "Type B": "Address",
                            "ID A": id_a, "ID B": id_b,
                            "Type": "Neighbouring Address",
                            "Detail": f"Distance: {dist:.2f} km",
                            "Method": "proximity_address"
                        })

        # --- Step 3: Person-to-Person (Surname AND (Proximity OR Postcode)) ---
        
        person_nodes = []
        corporate_suffixes = ("LIMITED", "LTD", "PLC", "LLP", "COUNCIL")
        
        # Build lookup: Address ID -> (Lat, Lon, CleanPostcode)
        addr_lookup = {}
        for nid, _, clean_pc, fmt_pc in address_nodes:
            coords = postcode_coords.get(fmt_pc, (None, None))
            addr_lookup[nid] = (coords, clean_pc)

        for node_id, attrs in pruned_graph.nodes(data=True):
            if attrs.get("type") == "person":
                label = attrs.get("label", "").upper()
                if any(s in label for s in corporate_suffixes): continue
                
                surname = ""
                if "," in label: surname = label.split(",", 1)[0].strip()
                else: surname = label.split()[-1].strip() if label.split() else ""
                
                if len(surname) < 3: continue 

                # Find Linked Address ID
                my_addr_id = None
                for neighbor in self.full_graph.neighbors(node_id):
                     if self.full_graph.nodes[neighbor].get("type") == "address":
                         my_addr_id = neighbor
                         break
                
                if my_addr_id and my_addr_id in addr_lookup:
                    person_nodes.append({
                        "id": node_id, "label": attrs.get("label"), 
                        "surname": surname, "addr_id": my_addr_id
                    })

        # Group by Surname
        from collections import defaultdict
        surname_buckets = defaultdict(list)
        for p in person_nodes:
            surname_buckets[p["surname"]].append(p)

        # Comparison Loop
        for surname, people in surname_buckets.items():
            if len(people) < 2: continue
            
            for i in range(len(people)):
                for j in range(i + 1, len(people)):
                    p_a = people[i]
                    p_b = people[j]
                    
                    if pruned_graph.has_edge(p_a["id"], p_b["id"]) or pruned_graph.has_edge(p_b["id"], p_a["id"]):
                        continue
                    
                    neigh_a = set(self.full_graph.neighbors(p_a["id"]))
                    neigh_b = set(self.full_graph.neighbors(p_b["id"]))
                    if not neigh_a.isdisjoint(neigh_b):
                        continue

                    # Get Location Data
                    (coords_a, pc_a) = addr_lookup[p_a["addr_id"]]
                    (coords_b, pc_b) = addr_lookup[p_b["addr_id"]]
                    
                    match_found = False
                    detail = ""

                    # CHECK 1: Postcode Match (Strongest Signal, works without Geocoding)
                    if pc_a and pc_b and pc_a == pc_b:
                        match_found = True
                        detail = f"Surname: {surname}, Same Postcode ({pc_a})"

                    # CHECK 2: Proximity (Fallback if postcodes differ but are close)
                    elif coords_a[0] and coords_b[0]:
                        dist = self._haversine_distance(coords_a[0], coords_a[1], coords_b[0], coords_b[1])
                        if dist <= radius_km:
                            match_found = True
                            detail = f"Surname: {surname}, Distance: {dist:.2f} km"

                    if match_found:
                        self.discovered_hidden_links.append({
                            "Entity A": p_a["label"], "Type A": "Person",
                            "Entity B": p_b["label"], "Type B": "Person",
                            "ID A": p_a["id"], "ID B": p_b["id"],
                            "Type": "Potential Relative",
                            "Detail": detail,
                            "Method": "surname_match"
                        })

        # --- Step 4: UI Updates ---
        # (Same as before)
        count = len(self.discovered_hidden_links)
        msg = f"Found {count} links."
        if failed_geocodes > 0: msg += f" ({failed_geocodes} postcodes failed)"
        
        self.hidden_links_status.config(text=msg, foreground="green" if count > 0 else "gray")
        if count > 0:
            self.view_hidden_results_btn.config(state="normal")
            if hasattr(self, 'show_inferred_check'):
                self.show_inferred_check.config(state="normal")
                self.show_inferred_var.set(True)
            messagebox.showinfo("Scan Complete", msg)
        else:
            messagebox.showinfo("Scan Complete", "No hidden links found.")


    def _show_hidden_links_dialog(self, links_data):
        """Opens a dialog to view/edit discovered hidden links."""
        if not links_data:
            messagebox.showinfo("No Data", "No hidden links to display.")
            return

        dialog = tk.Toplevel(self.app)
        dialog.title("Discovered Hidden Links")
        dialog.geometry("1200x550")

        pruned_graph = self._get_pruned_graph()

        # Description
        ttk.Label(
            dialog,
            text="Select links to add to the graph. Click column headers to sort.",
            foreground="gray",
            wraplength=1150
        ).pack(anchor="w", padx=10, pady=(10, 5))

        # Treeview with scrollbar
        tree_frame = ttk.Frame(dialog)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        tree_scroll_y = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
        tree_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)

        cols = ("Entity A", "Type A", "Entity B", "Type B", "Link Type", "Detail")
        tree = ttk.Treeview(
            tree_frame,
            columns=cols,
            show="headings",
            height=15,
            yscrollcommand=tree_scroll_y.set,
            selectmode="extended"
        )
        tree_scroll_y.config(command=tree.yview)

        # Track sort state for each column
        sort_state = {col: False for col in cols}

        def sort_by_column(col):
            """Sort treeview by column."""
            items = [(tree.set(item, col), item) for item in tree.get_children("")]
            sort_state[col] = not sort_state[col]
            items.sort(reverse=sort_state[col])
            for index, (_, item) in enumerate(items):
                tree.move(item, "", index)

        tree.heading("Entity A", text="Entity A", command=lambda: sort_by_column("Entity A"))
        tree.heading("Type A", text="Type A", command=lambda: sort_by_column("Type A"))
        tree.heading("Entity B", text="Entity B", command=lambda: sort_by_column("Entity B"))
        tree.heading("Type B", text="Type B", command=lambda: sort_by_column("Type B"))
        tree.heading("Link Type", text="Link Type", command=lambda: sort_by_column("Link Type"))
        tree.heading("Detail", text="Detail", command=lambda: sort_by_column("Detail"))

        tree.column("Entity A", width=200)
        tree.column("Type A", width=70)
        tree.column("Entity B", width=200)
        tree.column("Type B", width=70)
        tree.column("Link Type", width=130)
        tree.column("Detail", width=250)

        tree.pack(fill=tk.BOTH, expand=True)

        def populate_tree():
            """Populate tree from discovered_hidden_links."""
            for item in tree.get_children():
                tree.delete(item)
            for idx, link in enumerate(self.discovered_hidden_links):   
                tree.insert("", "end", iid=str(idx), values=(
                    link.get("Entity A", ""),
                    link.get("Type A", ""),
                    link.get("Entity B", ""),
                    link.get("Type B", ""),
                    link.get("Type", ""),
                    link.get("Detail", "")
                ))

        # Initial population
        populate_tree()

        # Button frame
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)

        def add_selected_to_graph():
            selected = tree.selection()
            if not selected:
                messagebox.showwarning("No Selection", "Please select links to add to the graph.")
                return

            added_count = 0
            for iid in selected:
                idx = int(iid)
                if 0 <= idx < len(self.discovered_hidden_links):
                    link = self.discovered_hidden_links[idx]
                    id_a = link.get("ID A")
                    id_b = link.get("ID B")
                    method = link.get("Method", "inferred")
                    link_type = link.get("Type", "Inferred")

                    # Add edge to the full graph with inferred attributes
                    if not self.full_graph.has_edge(id_a, id_b) and not self.full_graph.has_edge(id_b, id_a):
                        self.full_graph.add_edge(
                            id_a,
                            id_b,
                            label=f"inferred ({link_type})",
                            type="inferred",
                            method=method
                        )
                        added_count += 1

            if added_count > 0:
                # Remove added links from discovered list
                indices_to_remove = sorted([int(iid) for iid in selected], reverse=True)
                for idx in indices_to_remove:
                    if 0 <= idx < len(self.discovered_hidden_links):
                        del self.discovered_hidden_links[idx]

                # Rebuild tree
                populate_tree()

                # Update status
                count = len(self.discovered_hidden_links)
                self.hidden_links_status.config(
                    text=f"{count} link(s) remaining to review." if count > 0 else "All links processed.",
                    foreground="green" if count > 0 else "gray"
                )

                if count == 0:
                    self.view_hidden_results_btn.config(state="disabled")

                # Update section header
                self._update_section_header_status()

                messagebox.showinfo("Success", f"Added {added_count} link(s) to the graph.")
            else:
                messagebox.showinfo("No Changes", "Selected links were already in the graph.")

        def remove_selected():
            selected = tree.selection()
            if not selected:
                messagebox.showwarning("No Selection", "Please select rows to remove.")
                return

            indices_to_remove = sorted([int(iid) for iid in selected], reverse=True)
            for idx in indices_to_remove:
                if 0 <= idx < len(self.discovered_hidden_links):
                    del self.discovered_hidden_links[idx]

            populate_tree()

            count = len(self.discovered_hidden_links)
            if count > 0:
                self.hidden_links_status.config(
                    text=f"Found {count} potential hidden link(s).",
                    foreground="green"
                )
            else:
                self.hidden_links_status.config(
                    text="No hidden links discovered.",
                    foreground="gray"
                )
                self.view_hidden_results_btn.config(state="disabled")

        def export_to_csv():
            if not self.discovered_hidden_links:
                messagebox.showwarning("No Data", "No hidden links to export.")
                return

            filepath = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv")],
                title="Export Hidden Links"
            )
            if not filepath:
                return

            try:
                with open(filepath, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=["Entity A", "Type A", "Entity B", "Type B", "ID A", "ID B", "Type", "Detail", "Method"])
                    writer.writeheader()
                    for link in self.discovered_hidden_links:
                        writer.writerow(link)
                messagebox.showinfo("Export Complete", f"Exported {len(self.discovered_hidden_links)} link(s) to:\n{filepath}")
            except Exception as e:
                messagebox.showerror("Export Error", f"Could not export: {e}")

        def select_all():
            for item in tree.get_children():
                tree.selection_add(item)

        ttk.Button(btn_frame, text="Add Selected to Graph", command=add_selected_to_graph).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Remove Selected", command=remove_selected).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Select All", command=select_all).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Export to CSV", command=export_to_csv).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Close", command=dialog.destroy).pack(side=tk.RIGHT, padx=5)


    def _converter_run(self):
        """Main execution logic for the Data Converter."""
        entity_type = self.converter_entity_type.get() # "person" or "company"
        output_mode = self.convert_mode.get() # "cohort" or "graph"
        
        col_id = self.combo_id_col.get() 
        col_sec = self.combo_sec_col.get() 
        
        addr_mode = self.address_mode_var.get()
        col_addr_full = self.combo_addr_full.get()
        col_addr_l1 = self.combo_addr_line1.get()
        col_addr_pc = self.combo_addr_postcode.get()

        if not col_id:
            messagebox.showwarning("Missing Map", "Please select the primary ID column.")
            return

        save_path = filedialog.asksaveasfilename(
            defaultextension=".csv", 
            filetypes=[("CSV Files", "*.csv")],
            title=f"Save {output_mode.title()} File"
        )
        if not save_path:
            return

        converted_rows = []
        unique_ids = set()
        count_skipped = 0

        for row in self.converter_source_data:
            # 1. Generate Entity ID
            entity_id = ""
            entity_label = ""
            entity_node_type = entity_type

            raw_id_val = row.get(col_id, "").strip()
            if not raw_id_val:
                count_skipped += 1
                continue

            if entity_type == "person":
                entity_label = raw_id_val
                dob_str = row.get(col_sec, "").strip()
                dob_obj = {}
                if dob_str:
                    try:
                        dt = datetime.datetime.strptime(dob_str, "%d/%m/%Y")
                        dob_obj = {"year": str(dt.year), "month": f"{dt.month:02d}"}
                    except ValueError:
                        pass 
                
                entity_id = get_canonical_name_key(raw_id_val, dob_obj)
            
            else: # Company
                cnum = clean_company_number(raw_id_val)
                if not cnum: 
                    count_skipped += 1
                    continue
                entity_id = cnum
                comp_name = row.get(col_sec, "").strip()
                entity_label = comp_name if comp_name else cnum

            if not entity_id:
                count_skipped += 1
                continue

            unique_ids.add(entity_id)

            # 2. Process Address
            addr_id = ""
            addr_label = ""
            raw_addr_str = ""
            if addr_mode == "single" and col_addr_full:
                raw_addr_str = row.get(col_addr_full, "")
            elif addr_mode == "composite" and col_addr_l1 and col_addr_pc:
                p1 = row.get(col_addr_l1, "").strip()
                p2 = row.get(col_addr_pc, "").strip()
                if p1 or p2:
                    raw_addr_str = f"{p1}, {p2}"
            
            if raw_addr_str:
                addr_id = clean_address_string(raw_addr_str)
                addr_label = raw_addr_str.strip().strip(",").strip()

            # 3. Build Output
            if output_mode == "cohort":
                pass 
            else: # Graph
                if addr_id:
                    rel_type = "recorded_at" if entity_type == "person" else "registered_at"
                    converted_rows.append({
                        "source_id": entity_id,
                        "source_label": entity_label,
                        "source_type": entity_node_type,
                        "target_id": addr_id,
                        "target_label": addr_label,
                        "target_type": "address",
                        "relationship": rel_type
                    })
                else:
                    converted_rows.append({
                        "source_id": entity_id, 
                        "source_label": entity_label, 
                        "source_type": entity_node_type,
                        "target_id": "", 
                        "target_label": "", 
                        "target_type": "", 
                        "relationship": ""
                    })
                
        try:
            with open(save_path, "w", newline="", encoding="utf-8") as f:
                if output_mode == "cohort":
                    writer = csv.writer(f)
                    for uid in sorted(unique_ids):
                        writer.writerow([uid])
                else:
                    fieldnames = ["source_id", "source_label", "source_type", "target_id", "target_label", "target_type", "relationship"]
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for r in converted_rows:
                        writer.writerow(r)
            
            count = len(unique_ids) if output_mode == "cohort" else len(converted_rows)
            self.converter_status.config(text=f"Success! Saved {count} items.")
            messagebox.showinfo("Conversion Complete", f"Successfully exported data to:\n{save_path}\n\nItems Processed: {count}")
        except Exception as e:
            self.app.after(0, lambda: messagebox.showerror("Write Error", f"Could not save file: {e}"))


    def add_files(self):
        """Modified: Tracks file changes for rebuild prompt."""
        filepaths = filedialog.askopenfilenames(
            title="Select exported graph CSV files", filetypes=[("CSV files", "*.csv")]
        )
        if not filepaths:
            return

        for path in filepaths:
            if path not in self.source_files:
                self.source_files.append(path)
                self.file_listbox.insert(tk.END, f"FILE: {os.path.basename(path)}")

        if self.source_files:
            # Enable Build & Refine section
            self.refine_section.set_enabled(True)
            self._mark_files_changed()

    def clear_files(self):
        """Modified: Resets all state and disables sections."""
        for f in self.source_files:
            if "Seed-" in f and os.path.exists(f):
                try:
                    os.remove(f)
                except OSError as e:
                    log_message(f"Could not delete temp seed file {f}: {e}")

        self.source_files = []
        self.file_listbox.delete(0, tk.END)
        self.full_graph.clear()
        
        # Reset state
        self.graph_built = False
        self.files_changed_since_build = False
        self.highly_connected_exclusions.clear()
        self.peripheral_exclusions.clear()
        self.manual_exclusions.clear()
        
        # Reset section states
        self.refine_section.set_enabled(False)
        self.refine_section.set_status("")
        self.refine_section.clear_warning()
        self.analyse_section.set_enabled(False)
        self.visualise_section.set_enabled(False)
        
        # Reset exclusion labels
        self.hc_status_label.config(text="No exclusions", foreground="gray")
        self.pn_status_label.config(text="No exclusions", foreground="gray")


    def _get_pruned_graph(self):
        """Modified: Uses all three exclusion sets."""
        if not self.full_graph:
            return nx.DiGraph()

        # Combine all exclusions
        all_exclusions = (
            self.highly_connected_exclusions |
            self.peripheral_exclusions |
            self.manual_exclusions
        )
        
        if not all_exclusions:
            return self.full_graph.copy()
        
        pruned_graph = self.full_graph.copy()
        nodes_to_remove = [n for n in all_exclusions if n in pruned_graph]
        if nodes_to_remove:
            pruned_graph.remove_nodes_from(nodes_to_remove)
        
        return pruned_graph


    def _get_edge_type_description(self, graph, node_a, node_b):
        """Get the edge type description for an edge between two nodes."""
        # Check both directions since graph may be directed
        edge_data = None
        if graph.has_edge(node_a, node_b):
            edge_data = graph.edges[node_a, node_b]
        elif graph.has_edge(node_b, node_a):
            edge_data = graph.edges[node_b, node_a]

        if edge_data:
            edge_type = edge_data.get("type", "")
            if edge_type == "inferred":
                method = edge_data.get("method", "inferred")
                return f"Inferred ({method})"
            else:
                return "Explicit"
        return "Unknown"

    def _run_cohort_connection_thread(self, output_filepath):
        """Runs the cohort connection search in a background thread."""
        try:
            self.app.after(0, lambda: self.analyse_status_var.set("Preparing graph for analysis..."))

            pruned_graph = self._get_pruned_graph()
            undirected_graph = pruned_graph.to_undirected()

            max_hops = self.max_hops_var.get()
            shortest_only = self.shortest_only_var.get()

            cohort_a = {node for node in self.cohort_a_ids if node in undirected_graph}
            cohort_b = {node for node in self.cohort_b_ids if node in undirected_graph}

            total_pairs = len(cohort_a) * len(cohort_b)
            self.app.after(0, lambda: self.analyse_progress_bar.config(maximum=total_pairs, value=0))

            headers = [f"Hop {i+1}" for i in range(max_hops + 1)] + ["Edge Types"]

            with open(output_filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)

                processed_pairs = 0
                for start_node in cohort_a:
                    for end_node in cohort_b:
                        if start_node == end_node:
                            continue

                        processed_pairs += 1
                        if processed_pairs % 20 == 0:
                            self.app.after(
                                0,
                                lambda p=processed_pairs, t=total_pairs: self.analyse_status_var.set(
                                    f"Checking pair {p}/{t}..."
                                ),
                            )
                            self.app.after(0, lambda p=processed_pairs: self.analyse_progress_bar.config(value=p))

                        if shortest_only:
                            try:
                                path = nx.shortest_path(
                                    undirected_graph, source=start_node, target=end_node
                                )
                                if len(path) - 1 <= max_hops:
                                    labeled_path = [
                                        pruned_graph.nodes[node_id].get("label", node_id)
                                        for node_id in path
                                    ]
                                    # Build edge types string
                                    edge_types = []
                                    for i in range(len(path) - 1):
                                        edge_type = self._get_edge_type_description(pruned_graph, path[i], path[i+1])
                                        edge_types.append(edge_type)
                                    edge_types_str = ", ".join(edge_types)
                                    padded_path = labeled_path + [''] * (max_hops + 1 - len(labeled_path))
                                    writer.writerow(padded_path + [edge_types_str])
                            except nx.NetworkXNoPath:
                                continue
                        else:
                            all_paths = nx.all_simple_paths(
                                undirected_graph,
                                source=start_node,
                                target=end_node,
                                cutoff=max_hops,
                            )
                            for path in all_paths:
                                labeled_path = [
                                    pruned_graph.nodes[node_id].get("label", node_id)
                                    for node_id in path
                                ]
                                # Build edge types string
                                edge_types = []
                                for i in range(len(path) - 1):
                                    edge_type = self._get_edge_type_description(pruned_graph, path[i], path[i+1])
                                    edge_types.append(edge_type)
                                edge_types_str = ", ".join(edge_types)
                                padded_path = labeled_path + [''] * (max_hops + 1 - len(labeled_path))
                                writer.writerow(padded_path + [edge_types_str])

            self.app.after(0, lambda: self.analyse_progress_bar.config(value=total_pairs))
            self.app.after(
                0,
                lambda: self.analyse_status_var.set(
                    f"Complete! Results saved to {os.path.basename(output_filepath)}"
                )
            )
            self.app.after(
                0,
                lambda: messagebox.showinfo(
                    "Success",
                    f"Processing complete. The results have been saved to:\n{output_filepath}",
                )
            )

        except Exception as e:
            log_message(f"Fatal error during cohort connection search: {e}")
            self.app.after(0, lambda: messagebox.showerror("Error", f"An unexpected error occurred: {e}"))
            self.app.after(0, lambda: self.analyse_status_var.set("Error during search."))
        finally:
            self.app.after(0, lambda: self.find_connections_btn.config(state="normal"))

    def _add_edge_to_graph(self, edge_data, source_name):
        target_id = edge_data.get("target_id")
        if not target_id:
             # Just add the source node
             if self.full_graph.has_node(edge_data["source_id"]):
                 self.full_graph.nodes[edge_data["source_id"]]["source_files"].add(source_name)
             else:
                 self.full_graph.add_node(
                     edge_data["source_id"],
                     label=edge_data["source_label"],
                     type=edge_data["source_type"],
                     source_files={source_name},
                 )
             return 
        if self.full_graph.has_node(edge_data["source_id"]):
            self.full_graph.nodes[edge_data["source_id"]]["source_files"].add(
                source_name
            )
        else:
            self.full_graph.add_node(
                edge_data["source_id"],
                label=edge_data["source_label"],
                type=edge_data["source_type"],
                source_files={source_name},
            )

        if self.full_graph.has_node(edge_data["target_id"]):
            self.full_graph.nodes[edge_data["target_id"]]["source_files"].add(
                source_name
            )
        else:
            self.full_graph.add_node(
                edge_data["target_id"],
                label=edge_data["target_label"],
                type=edge_data["target_type"],
                source_files={source_name},
            )

        self.full_graph.add_edge(
            edge_data["source_id"],
            edge_data["target_id"],
            label=edge_data["relationship"],
        )

    def _populate_node_dropdowns(self):
        """Modified: Updates new widget references."""
        if not self.full_graph:
            return

        self.all_node_labels = sorted(
            [
                f"{attrs['label']} ({node_id})"
                for node_id, attrs in self.full_graph.nodes(data=True)
            ]
        )

        # Update searchable entries
        self.start_node_entry.set_values(self.all_node_labels)
        self.end_node_entry.set_values(self.all_node_labels)
        self.manual_exclude_entry.set_values(self.all_node_labels)

    def generate_full_graph(self):
        """Modified: Uses new highlight list and isolated network options."""
        if not self.graph_built:
            messagebox.showwarning("No Graph", "Please build the graph in Build & Refine first.")
            return
        
        pruned_graph = self._get_pruned_graph()
        self._generate_highlighted_graph(pruned_graph, path=None)


    def _fetch_company_network_data(self, company_number, fetch_pscs=True):
        profile, _ = ch_get_data(
            self.api_key, self.ch_token_bucket, f"/company/{company_number}"
        )
        officers, _ = ch_get_data(
            self.api_key,
            self.ch_token_bucket,
            f"/company/{company_number}/officers?items_per_page=100",
        )
        pscs = None
        if fetch_pscs:
            pscs, _ = ch_get_data(
                self.api_key,
                self.ch_token_bucket,
                f"/company/{company_number}/persons-with-significant-control?items_per_page=100",
            )
        return profile, officers, pscs

    def _update_visualise_checkbox_state(self):
        """
        Enables/Disables the 'Show only networks...' checkbox based on 
        whether the required entity lists for the current mode are loaded.
        """
        if not hasattr(self, 'show_highlighted_check'):
            return

        mode = self.analyse_mode_var.get()
        should_enable = False

        if mode == "single_list":
            # Enable if we have the single list
            if self.analyse_entity_list:
                should_enable = True
        
        elif mode == "two_lists":
            # Enable if we have BOTH lists (since we need to check connections BETWEEN them)
            if self.cohort_a_ids and self.cohort_b_ids:
                should_enable = True
        
        elif mode == "two_entities":
            # This filter doesn't make sense for just two specific entities
            should_enable = False

        # Apply state
        state = "normal" if should_enable else "disabled"
        self.show_highlighted_check.config(state=state)
        
        # If disabling, uncheck it to avoid confusion
        if not should_enable:
            self.show_highlighted_only_var.set(False)


    def _generate_highlighted_graph(self, graph_to_render, path=None):
        """Modified: Uses new highlight list and entity type filtering for isolated networks."""
        
        # 1. Determine "Active" Entities based on Analyse Mode
        mode = self.analyse_mode_var.get()
        highlight_ids = set()
        list_a = set()
        list_b = set()

        if mode == "single_list" and self.analyse_entity_list:
            highlight_ids = self.analyse_entity_list
        elif mode == "two_lists":
            if self.cohort_a_ids: list_a = self.cohort_a_ids
            if self.cohort_b_ids: list_b = self.cohort_b_ids
            highlight_ids = list_a | list_b
        
        # 2. Handle "Show Only Networks Containing Connections" Filter
        if self.show_highlighted_only_var.get():
            undirected_view = graph_to_render.to_undirected()
            connected_components = list(nx.connected_components(undirected_view))
            valid_nodes = set()

            for component in connected_components:
                # Mode A: Single List (Standard "Cohort" logic)
                if mode == "single_list":
                    hits = [node for node in component if node in highlight_ids]
                    if len(hits) >= 2:
                        valid_nodes.update(component)
                
                # Mode B: Two Lists (Intersection logic)
                elif mode == "two_lists":
                    has_a = any(node in list_a for node in component)
                    has_b = any(node in list_b for node in component)
                    if has_a and has_b:
                        valid_nodes.update(component)
                
                # Mode C: Two Specific Entities (Just ensure they are present)
                elif mode == "two_entities":
                     # Fallback to standard behavior if someone tries this
                     pass

            if not valid_nodes:
                messagebox.showinfo("No Networks", "No networks matching the connection criteria found.")
                return

            graph_to_render = graph_to_render.subgraph(valid_nodes).copy()

        # Handle "hide isolated networks" filter with entity type selection
        elif self.hide_isolated_var.get():
            # Determine which entity types to check for
            required_types = []
            if self.isolated_companies_var.get():
                required_types.append("company")
            if self.isolated_persons_var.get():
                required_types.append("person")
            if self.isolated_addresses_var.get():
                required_types.append("address")
            
            if not required_types:
                messagebox.showwarning("Warning", "Please select at least one entity type.")
                return

            undirected_view = graph_to_render.to_undirected()
            connected_components = list(nx.connected_components(undirected_view))

            valid_nodes = set()
            for component in connected_components:
                # Check if component has at least 2 of any required type
                has_enough = False
                for req_type in required_types:
                    type_nodes = [
                        node for node in component
                        if graph_to_render.nodes[node].get("type") == req_type
                    ]
                    if len(type_nodes) >= 2:
                        has_enough = True
                        break
                
                if has_enough:
                    valid_nodes.update(component)

            if not valid_nodes:
                messagebox.showinfo("No Networks", "No networks matching criteria found.")
                return

            graph_to_render = graph_to_render.subgraph(valid_nodes).copy()

        # Create a working copy for visualization
        viz_graph = graph_to_render.copy()

        # Filter out inferred edges if the checkbox is unchecked
        if not self.show_inferred_var.get():
            inferred_edges = [
                (u, v) for u, v, attrs in viz_graph.edges(data=True)
                if attrs.get("type") == "inferred"
            ]
            viz_graph.remove_edges_from(inferred_edges)

        # Build the visual network
        net = Network(height="95vh", width="100%", directed=True, notebook=False, cdn_resources="local")

        # Configure options including node scaling
        scale_by_connections = self.scale_by_connections_var.get()
        if scale_by_connections:
            net.set_options("""{
                "configure": {"enabled": true},
                "physics": {"solver": "forceAtlas2Based"},
                "nodes": {
                    "scaling": {
                        "min": 15,
                        "max": 40
                    }
                }
            }""")
        else:
            net.set_options("""{"configure": {"enabled": true}, "physics": {"solver": "forceAtlas2Based"}}""")

        path_edges = set()
        if path:
            for i in range(len(path) - 1):
                u, v = path[i], path[i + 1]
                path_edges.add((u, v))
                if not self.enforce_direction_var.get():
                    path_edges.add((v, u))

        distinguish_by_file = self.distinguish_var.get()
        file_color_map = {}
        if distinguish_by_file:
            border_colors = ["#00FFFF", "#FFD700", "#ADFF2F", "#FF69B4", "#BA55D3", "#00FF00"]
            unique_sources = sorted(
                list({name for attrs in viz_graph.nodes.values() for name in attrs.get("source_files", set())})
            )
            file_color_map = {source: color for source, color in zip(unique_sources, border_colors)}

        # Set node values for connection-based scaling using nx.set_node_attributes
        if scale_by_connections:
            degree_dict = {n: self.full_graph.degree(n) if n in self.full_graph else 1
                           for n in viz_graph.nodes()}
            nx.set_node_attributes(viz_graph, degree_dict, 'value')

        for node_id, attrs in viz_graph.nodes(data=True):
            node_type = attrs.get("type")
            base_color = "#B9D9EB" if node_type == "company" else ("#FFB347" if node_type == "address" else "#D9E8B9")
            size = 15
            shape = "box" if node_type in ["company", "address"] else "ellipse"
            final_color = base_color
            border_width = 1
            shape_properties = {}

            if highlight_ids and node_id in highlight_ids:
                shape_properties["borderDashes"] = [10, 10]
                border_width = 5
                size = max(size, 30)

            if path and node_id in path:
                final_color = "#FF0000"
                size = max(size, 25)

            if node_id in highlight_ids:
                shape_properties["borderDashes"] = [10, 10]
                border_width = 5
                size = max(size, 30)
                
                # Optional: Distinguish List A vs List B with border colors?
                if mode == "two_lists":
                    if node_id in list_a: 
                        final_color = {"background": base_color, "border": "#0000FF"} # Blue for A
                    elif node_id in list_b:
                        final_color = {"background": base_color, "border": "#FF0000"} # Red for B

            if distinguish_by_file:
                source_files = attrs.get("source_files", set())
                if border_width == 1:
                    border_width = 3
                border_color = "#FFFFFF"
                if len(source_files) > 1:
                    border_color = "#000000"
                elif len(source_files) == 1:
                    filename = list(source_files)[0]
                    border_color = file_color_map.get(filename, "#FFFFFF")
                bg_color = final_color if path and node_id in path else base_color
                final_color = {"background": bg_color, "border": border_color}

            label_lines = []
            raw_label = attrs.get("label", "")
            if node_type == "company":
                wrapped = "\n".join(textwrap.wrap(raw_label, width=25))
                label_lines.append(html.escape(wrapped))
                label_lines.append(f"({html.escape(node_id)})")
            elif node_type == "address":
                wrapped = "\n".join(textwrap.wrap(raw_label, width=25))
                label_lines.append(html.escape(wrapped))
            else:
                label_lines.append(html.escape(raw_label))
                if "-" in str(node_id):
                    try:
                        parts = str(node_id).rsplit("-", 2)
                        if len(parts) == 3:
                            name_key, year, month = parts
                            label_lines.append(f"DOB: {month}/{year}")
                    except (ValueError, TypeError):
                        pass

            safe_label_multiline = "\n".join(label_lines)
            safe_title = html.escape(raw_label)

            # Build node kwargs - use 'value' for scaling when enabled
            node_kwargs = {
                "label": safe_label_multiline,
                "title": safe_title,
                "color": final_color,
                "borderWidth": border_width,
                "shape": shape,
                "shapeProperties": shape_properties,
                "size": size,
            }
            if scale_by_connections:
                # Read value from attrs (set via nx.set_node_attributes)
                node_kwargs["value"] = attrs.get('value', 1)

            net.add_node(node_id, **node_kwargs)

        for source, target, edge_attrs in viz_graph.edges(data=True):
            width = 1
            edge_color = "#848484"
            dashes = False

            # Check if this is an inferred edge
            if edge_attrs.get("type") == "inferred":
                edge_color = "#FF00FF"  # Magenta
                width = 3
                dashes = [10, 10]
                # Check label or method to distinguish Relatives vs Neighbours
                lbl = edge_attrs.get("label", "").lower()
                method = edge_attrs.get("method", "")
                
                # If it involves a Surname match (Relative), make it Yellow/Gold
                if "surname" in lbl or "surname" in method:
                    edge_color = "#FFD700"  # Gold
                else:
                    # Otherwise it's just a Neighbour/Proximity match
                    edge_color = "#FF00FF"  # Magenta
            if path and (source, target) in path_edges:
                width = 5
                edge_color = "#FF0000"

            safe_title = html.escape(edge_attrs.get("label", ""))
            net.add_edge(source, target, title=safe_title, width=width, color=edge_color, dashes=dashes)

        try:
            filename = os.path.join(CONFIG_DIR, "combined_network_graph.html")
            net.write_html(filename, notebook=False)
            webbrowser.open(f"file://{os.path.realpath(filename)}")
        except Exception as e:
            log_message(f"Failed to save or open combined graph: {e}")
            messagebox.showerror("Graph Error", f"Could not save graph: {e}")

    def start_seed_fetch(self):
        """Modified: Uses new seed status variable and progress bar."""
        seed_cnum_raw = self.seed_cnum_var.get()
        seed_cnum = clean_company_number(seed_cnum_raw)
        if not seed_cnum:
            messagebox.showerror("Input Error", "Please enter a valid company number.")
            return

        self.seed_btn.config(state="disabled")
        self.seed_status_var.set(f"Seeding network with {seed_cnum}...")
        self.seed_progress_bar.start(10)
        self.cancel_flag.clear()
        threading.Thread(target=self._run_seed_fetch_thread, args=(seed_cnum,), daemon=True).start()

    def _run_seed_fetch_thread(self, seed_cnum):
        """Modified: Uses new status variable."""
        fetch_pscs = self.seed_fetch_pscs_var.get()
        fetch_associated = self.seed_fetch_associated_var.get()

        try:
            self.app.after(0, lambda: self.seed_status_var.set(f"Fetching officers for {seed_cnum}..."))
            officers, error = ch_get_data(
                self.api_key,
                self.ch_token_bucket,
                f"/company/{seed_cnum}/officers?items_per_page=100",
            )
            if error or not officers or not officers.get("items"):
                raise ValueError(f"Could not fetch officers for {seed_cnum}.")

            if fetch_associated:
                self.app.after(0, lambda: self.seed_status_var.set(f"Found {len(officers['items'])} officers. Fetching appointments..."))
                all_appointments = []
                with ThreadPoolExecutor(max_workers=2) as executor:
                    future_to_officer = {
                        executor.submit(self._fetch_officer_appointments, o.get("links", {})): o
                        for o in officers["items"]
                    }
                    for future in as_completed(future_to_officer):
                        if self.cancel_flag.is_set():
                            return
                        appointments = future.result()
                        if appointments:
                            all_appointments.extend(appointments)

                unique_company_numbers = {
                    app.get("appointed_to", {}).get("company_number")
                    for app in all_appointments
                }
                unique_company_numbers.add(seed_cnum)
            else:
                unique_company_numbers = {seed_cnum}

            self.app.after(0, lambda: self.seed_status_var.set(f"Found {len(unique_company_numbers)} companies. Building network..."))
            temp_graph = nx.DiGraph()
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_to_cnum = {
                    executor.submit(self._fetch_company_network_data, cnum, fetch_pscs): cnum
                    for cnum in unique_company_numbers if cnum
                }
                for i, future in enumerate(as_completed(future_to_cnum)):
                    if self.cancel_flag.is_set():
                        return
                    self.app.after(0, lambda i=i: self.seed_status_var.set(f"Processing company {i+1}/{len(unique_company_numbers)}..."))
                    profile, officers_data, pscs_data = future.result()
                    if profile:
                        self._add_company_to_graph(temp_graph, profile, officers_data, pscs_data)

            self.app.after(100, lambda: self._save_graph_to_temp_csv(temp_graph, seed_cnum))

        except Exception as e:
            self.app.after(0, lambda: messagebox.showerror("Error", f"Failed to seed network: {e}"))
            self.app.after(0, lambda: self.seed_status_var.set("Error during seeding."))
            self.app.after(0, lambda: self.seed_progress_bar.stop())
        finally:
            self.app.after(100, lambda: self.seed_btn.config(state="normal"))

    def _fetch_officer_appointments(self, officer_links: dict) -> List[dict]:
        base_path = officer_links.get("officer", {}).get("appointments")
        if not base_path: return []
        page_size = 100 
        start_index = 0
        all_items = []
        while True:
            path = f"{base_path}?items_per_page={page_size}&start_index={start_index}"
            data, err = ch_get_data(self.api_key, self.ch_token_bucket, path)
            if err or not data: break
            page_items = data.get("items", [])
            all_items.extend(page_items)
            if len(page_items) < page_size: break
            start_index += page_size
        return all_items


    def _add_company_to_graph(self, G, profile, officers, pscs):
        cnum = profile.get("company_number")
        G.add_node(cnum, label=profile.get("company_name", cnum), type="company")
        addr_data = profile.get("registered_office_address", {})
        raw_address_str = ", ".join(filter(None, [addr_data.get("address_line_1"), addr_data.get("locality"), addr_data.get("postal_code")]))
        address_str = clean_address_string(raw_address_str)
        if address_str:
            G.add_node(address_str, label=raw_address_str, type="address")
            G.add_edge(cnum, address_str, label="registered_at")
        if officers:
            for o in officers.get("items", []):
                name = o.get("name")
                if not name: continue
                dob = o.get("date_of_birth")
                key = get_canonical_name_key(name, dob)
                G.add_node(key, label=name, type="person")
                G.add_edge(cnum, key, label=o.get("officer_role", "officer"))
                # Add officer correspondence address
                officer_addr_raw = extract_address_string(o.get("address"))
                if officer_addr_raw:
                    officer_addr_clean = clean_address_string(officer_addr_raw)
                    if officer_addr_clean and not G.has_node(officer_addr_clean):
                        G.add_node(officer_addr_clean, label=format_address_label(officer_addr_raw), type="address")
                    if officer_addr_clean:
                        G.add_edge(key, officer_addr_clean, label="correspondence_at")
        if pscs:
            for p in pscs.get("items", []):
                name = p.get("name")
                if not name: continue
                dob = p.get("date_of_birth")
                key = get_canonical_name_key(name, dob)
                G.add_node(key, label=name, type="person")
                G.add_edge(cnum, key, label="psc")
                # Add PSC correspondence address
                psc_addr_raw = extract_address_string(p.get("address"))
                if psc_addr_raw:
                    psc_addr_clean = clean_address_string(psc_addr_raw)
                    if psc_addr_clean and not G.has_node(psc_addr_clean):
                        G.add_node(psc_addr_clean, label=format_address_label(psc_addr_raw), type="address")
                    if psc_addr_clean:
                        G.add_edge(key, psc_addr_clean, label="correspondence_at")



    def _save_graph_to_temp_csv(self, G, seed_cnum):
        if G.number_of_edges() == 0:
            self.app.after(0, lambda: self.status_var.set(f"Seed for {seed_cnum} found no connections."))
            return
        filename = f"Seed-{seed_cnum}-{int(time.time())}.csv"
        filepath = os.path.join(CONFIG_DIR, filename)
        headers = ["source_id", "source_label", "source_type", "target_id", "target_label", "target_type", "relationship"]
        try:
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                for u, v, data in G.edges(data=True):
                    writer.writerow([u, G.nodes[u].get("label", ""), G.nodes[u].get("type", ""), v, G.nodes[v].get("label", ""), G.nodes[v].get("type", ""), data.get("label", "")])
            self.app.after(100, lambda: self._add_seed_file_to_list(filepath))
        except IOError as e:
            log_message(f"Could not write temp seed file {filepath}: {e}")

    def _add_seed_file_to_list(self, filepath):
        """Modified: Uses new status variable and triggers rebuild tracking."""
        self.source_files.append(filepath)
        self.file_listbox.insert(tk.END, f"SEED: {os.path.basename(filepath)}")
        
        # Enable Build & Refine section
        self.refine_section.set_enabled(True)
        self._mark_files_changed()
        
        # Update seed status
        seed_cnum = os.path.basename(filepath).split("-")[1] if "-" in filepath else ""
        self.seed_status_var.set(f"Successfully seeded network for {seed_cnum}. Ready to build.")
        self.seed_progress_bar.stop()

