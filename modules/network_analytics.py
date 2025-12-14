# module/network_analytics.py

import csv
import html
import os
import re
import textwrap
import threading
import time
import datetime
import webbrowser
import difflib 
import tkinter as tk
from typing import List, Dict, Optional, Tuple, Set
from tkinter import ttk, filedialog, messagebox
from concurrent.futures import ThreadPoolExecutor, as_completed

import networkx as nx
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
from ..utils.helpers import log_message, clean_address_string

# UI components
from ..ui.tooltip import Tooltip

from .base import InvestigationModuleBase

class NetworkAnalytics(InvestigationModuleBase):
    def __init__(
        self, parent_app, back_callback, ch_token_bucket, api_key=None, help_key=None
    ):
        super().__init__(parent_app, back_callback, api_key, help_key=help_key)
        self.ch_token_bucket = ch_token_bucket
        self.source_files = []
        self.full_graph = nx.DiGraph()
        self.all_node_labels = []
        self.cohort_ids = set()
        self.nodes_to_remove = set()
        self.cohort_a_ids = set()
        self.cohort_b_ids = set()
        
        # Converter state variables
        self.converter_source_data = []  
        self.converter_headers = []

        # --- Tabbed Interface Setup ---
        self.notebook = ttk.Notebook(self.content_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=5)

        # Tab 1: Network Analytics (Original Functionality)
        self.analytics_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.analytics_tab, text="Network Analytics")
        self._setup_analytics_tab()

        # Tab 2: Data Converter (New Functionality)
        self.converter_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.converter_tab, text="Data Converter")
        self._setup_converter_tab()

    def _setup_analytics_tab(self):
        """Builds the network analytics UI."""
        container = self.analytics_tab

        # --- Step 1: Seed Network ---
        seed_frame = ttk.LabelFrame(
            container,
            text="Step 1: Seed Network with a Company (Optional)",
            padding=10,
        )
        seed_frame.pack(fill=tk.X, pady=5, padx=10)
        
        seed_top_row = ttk.Frame(seed_frame)
        seed_top_row.pack(fill=tk.X, pady=(0, 5))
        self.seed_cnum_var = tk.StringVar()
        ttk.Label(seed_top_row, text="Company Number:").pack(side=tk.LEFT, padx=(0, 5))
        seed_entry = ttk.Entry(seed_top_row, textvariable=self.seed_cnum_var, width=20)
        seed_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
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

        # --- Step 2: Add Graph & Cohort Data ---
        upload_frame = ttk.LabelFrame(
            container, text="Step 2: Add Exported Graph Files", padding=10
        )
        upload_frame.pack(fill=tk.X, pady=5, padx=10)
        buttons_frame = ttk.Frame(upload_frame)
        buttons_frame.pack(fill=tk.X, pady=5)
        ttk.Button(buttons_frame, text="Add File(s)...", command=self.add_files).pack(
            side=tk.LEFT, padx=(0, 10)
        )
        ttk.Button(
            buttons_frame, text="Clear File List", command=self.clear_files
        ).pack(side=tk.LEFT)
        
        file_list_frame = ttk.Frame(upload_frame)
        file_list_frame.pack(fill=tk.X, expand=True, pady=5)
        file_scrollbar = ttk.Scrollbar(file_list_frame, orient=tk.VERTICAL)
        file_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.file_listbox = tk.Listbox(
            file_list_frame, height=4, yscrollcommand=file_scrollbar.set
        )
        self.file_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        file_scrollbar.config(command=self.file_listbox.yview)

        cohort_frame = ttk.LabelFrame(
            container,
            text="Step 2b: Add Cohort File to Highlight (Optional)",
            padding=10,
        )
        cohort_frame.pack(fill=tk.X, pady=5, padx=10)
        cohort_buttons_frame = ttk.Frame(cohort_frame)
        cohort_buttons_frame.pack(fill=tk.X, pady=5)
        ttk.Button(
            cohort_buttons_frame,
            text="Add Cohort File...",
            command=self.load_cohort_file,
        ).pack(side=tk.LEFT)
        self.cohort_status_label = ttk.Label(
            cohort_buttons_frame, text="No cohort file loaded."
        )
        self.cohort_status_label.pack(side=tk.LEFT, padx=10)

        # --- Step 3: Build Combined Network ---
        build_frame = ttk.LabelFrame(
            container, text="Step 3: Build Combined Network", padding=10
        )
        build_frame.pack(fill=tk.X, pady=5, padx=10)
        self.build_btn = ttk.Button(
            build_frame,
            text="Build Combined Network",
            state="disabled",
            command=self.build_combined_graph,
            bootstyle="success",
        )
        self.build_btn.pack(pady=5, ipady=5)
        
        # --- Step 4: Refine & Deduplicate ---
        refine_frame = ttk.LabelFrame(
            container, text="Step 4: Refine & Deduplicate", padding=10
        )
        refine_frame.pack(fill=tk.X, pady=5, padx=10)
        
        # Sub-frame for Pruning
        prune_frame = ttk.Frame(refine_frame)
        prune_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(prune_frame, text="Remove specific node:").pack(side=tk.LEFT, padx=(0,5))
        self.prune_entry = SearchableEntry(prune_frame)
        self.prune_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        self.prune_entry.config(state="disabled")
        add_prune_btn = ttk.Button(
            prune_frame,
            text="Add to Removal List",
            command=self._add_node_to_removal_list,
        )
        add_prune_btn.pack(side=tk.LEFT)
        
        self.prune_listbox = tk.Listbox(refine_frame, height=3)
        self.prune_listbox.pack(fill=tk.X, expand=True, pady=(0,5))
        
        prune_actions = ttk.Frame(refine_frame)
        prune_actions.pack(fill=tk.X, pady=5)
        ttk.Button(prune_actions, text="Remove Selected", command=self._remove_node_from_removal_list).pack(side=tk.LEFT)
        ttk.Button(prune_actions, text="Clear List", command=self._clear_removal_list).pack(side=tk.LEFT, padx=5)

        # Deduplication Controls
        dedup_frame = ttk.Frame(refine_frame)
        dedup_frame.pack(fill=tk.X, pady=(10, 0))
        ttk.Separator(dedup_frame, orient="horizontal").pack(fill=tk.X, pady=5)
        
        ttk.Label(dedup_frame, text="Entity Resolution:").pack(side=tk.LEFT, padx=(0, 10))
        
        self.scan_dupes_btn = ttk.Button(
            dedup_frame,
            text="Scan for Duplicates...",
            state="disabled",
            command=self._open_deduplication_dialog,
            bootstyle="warning"
        )
        self.scan_dupes_btn.pack(side=tk.LEFT)
        Tooltip(self.scan_dupes_btn, "Analyzes addresses and names to find likely duplicates and offers a merge tool.")

        # --- Step 5: Visualise ---
        visualize_frame = ttk.LabelFrame(
            container, text="Step 5: Generate Full Visual Graph", padding=10
        )
        visualize_frame.pack(fill=tk.X, pady=5, padx=10)
        self.distinguish_var = tk.BooleanVar(value=True)
        distinguish_check = ttk.Checkbutton(
            visualize_frame,
            text="Visually distinguish nodes by source file",
            variable=self.distinguish_var,
        )
        distinguish_check.pack(pady=5, anchor="w")
        self.eliminate_unconnected_var = tk.BooleanVar(value=False)
        eliminate_check = ttk.Checkbutton(
            visualize_frame,
            text="Eliminate unconnected companies from visualisation",
            variable=self.eliminate_unconnected_var,
        )
        eliminate_check.pack(pady=5, anchor="w")
        self.show_cohort_networks_only_var = tk.BooleanVar(value=False)
        self.cohort_only_check = ttk.Checkbutton(
            visualize_frame,
            text="Show only networks connecting cohort members",
            variable=self.show_cohort_networks_only_var,
            state="disabled",
        )
        self.cohort_only_check.pack(pady=5, anchor="w")
        self.generate_full_graph_btn = ttk.Button(
            visualize_frame,
            text="Generate Full Visual Graph",
            state="disabled",
            command=self.generate_full_graph,
        )
        self.generate_full_graph_btn.pack(pady=5)

        # --- Step 6: Find Connection ---
        path_frame = ttk.LabelFrame(container, text="Step 6: Find Connection Between Two Entities", padding=10)
        path_frame.pack(fill=tk.X, pady=5, padx=10)
        ttk.Label(path_frame, text="Start Entity:").grid(row=0, column=0, sticky="w", padx=5)
        self.start_node_entry = SearchableEntry(path_frame)
        self.start_node_entry.grid(row=1, column=0, sticky="ew", padx=5)
        self.start_node_entry.config(state="disabled")
        ttk.Label(path_frame, text="End Entity:").grid(row=0, column=1, sticky="w", padx=5)
        self.end_node_entry = SearchableEntry(path_frame)
        self.end_node_entry.grid(row=1, column=1, sticky="ew", padx=5)
        self.end_node_entry.config(state="disabled")
        path_frame.columnconfigure(0, weight=1)
        path_frame.columnconfigure(1, weight=1)
        self.enforce_direction_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(path_frame, text="Enforce direction", variable=self.enforce_direction_var).grid(row=2, column=0, sticky="w", padx=5)
        self.find_path_btn = ttk.Button(path_frame, text="Find Path & Generate Graph", state="disabled", command=self.find_and_highlight_path)
        self.find_path_btn.grid(row=3, column=0, columnspan=2, pady=5)

        # --- Step 7: Cohort Connections (Restored) ---
        cohort_connect_frame = ttk.LabelFrame(
            container,
            text="Step 7: Find Connections Between Cohorts",
            padding=10,
        )
        cohort_connect_frame.pack(fill=tk.X, pady=5, padx=10)

        cohort_a_frame = ttk.Frame(cohort_connect_frame)
        cohort_a_frame.pack(fill=tk.X, pady=2)
        ttk.Button(
            cohort_a_frame, text="Upload Cohort A File...", command=self._load_cohort_a
        ).pack(side=tk.LEFT, padx=5)
        self.cohort_a_status_label = ttk.Label(cohort_a_frame, text="No file loaded.")
        self.cohort_a_status_label.pack(side=tk.LEFT)

        cohort_b_frame = ttk.Frame(cohort_connect_frame)
        cohort_b_frame.pack(fill=tk.X, pady=2)
        ttk.Button(
            cohort_b_frame, text="Upload Cohort B File...", command=self._load_cohort_b
        ).pack(side=tk.LEFT, padx=5)
        self.cohort_b_status_label = ttk.Label(cohort_b_frame, text="No file loaded.")
        self.cohort_b_status_label.pack(side=tk.LEFT)

        options_frame = ttk.Frame(cohort_connect_frame)
        options_frame.pack(fill=tk.X, pady=10)

        ttk.Label(options_frame, text="Max Hops:").pack(side=tk.LEFT, padx=(0, 5))
        self.max_hops_var = tk.IntVar(value=5)
        self.max_hops_combo = ttk.Combobox(
            options_frame,
            textvariable=self.max_hops_var,
            values=list(range(1, 11)),
            state="readonly",
            width=5,
        )
        self.max_hops_combo.pack(side=tk.LEFT)

        self.shortest_only_var = tk.BooleanVar(value=True)
        self.shortest_only_check = ttk.Checkbutton(
            options_frame,
            text="Shortest Connection Only",
            variable=self.shortest_only_var,
        )
        self.shortest_only_check.pack(side=tk.LEFT, padx=20)

        self.find_cohort_paths_btn = ttk.Button(
            cohort_connect_frame,
            text="Find Connections & Export...",
            state="disabled",
            command=self._start_cohort_connection_search,
        )
        self.find_cohort_paths_btn.pack(pady=5, ipady=5)

        # --- Status Bar ---
        status_frame = ttk.Frame(container)
        status_frame.pack(fill=tk.X, pady=10, padx=10, side=tk.BOTTOM)
        self.progress_bar = ttk.Progressbar(status_frame, orient="horizontal", length=300, mode="determinate")
        self.progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        self.status_var = tk.StringVar(value="Ready.")
        ttk.Label(status_frame, textvariable=self.status_var).pack(side=tk.LEFT)

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
        ttk.Radiobutton(step3_frame, text="Create Cohort File (IDs Only)", variable=self.convert_mode, value="cohort").pack(anchor="w", padx=5)
        ttk.Radiobutton(step3_frame, text="Create Graph File (Nodes & Links)", variable=self.convert_mode, value="graph").pack(anchor="w", padx=5)
        
        btn_frame = ttk.Frame(step3_frame)
        btn_frame.pack(fill=tk.X, pady=10)
        
        self.convert_btn = ttk.Button(btn_frame, text="Convert & Save File", command=self._converter_run, state="disabled", bootstyle="success")
        self.convert_btn.pack(side=tk.LEFT)
        self.converter_status = ttk.Label(btn_frame, text="", foreground="green")
        self.converter_status.pack(side=tk.LEFT, padx=10)

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
                
                entity_id = self._get_canonical_name_key(raw_id_val, dob_obj)
            
            else: # Company
                cnum = self._clean_company_number(raw_id_val)
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
            self.build_btn.config(state="normal")
            self.app.after(
                0,
                lambda: self.status_var.set(
                    f"{len(self.source_files)} data source(s) loaded. Ready to build graph."
                ),
            )

    def clear_files(self):
        for f in self.source_files:
            if "Seed-" in f and os.path.exists(f):
                try:
                    os.remove(f)
                except OSError as e:
                    log_message(f"Could not delete temp seed file {f}: {e}")

        self.source_files = []
        self.file_listbox.delete(0, tk.END)
        self.full_graph.clear()
        self.cohort_ids = set()
        self.cohort_status_label.config(text="No cohort file loaded.")
        self.cohort_only_check.config(state="disabled")
        self.build_btn.config(state="disabled")
        self.generate_full_graph_btn.config(state="disabled")
        self.start_node_entry.config(state="disabled")
        self.end_node_entry.config(state="disabled")
        self.find_path_btn.config(state="disabled")
        self.scan_dupes_btn.config(state="disabled") # Disable dedup scan
        self.app.after(
            0,
            lambda: self.status_var.set(
                "File list cleared. Please add or seed data to begin."
            ),
        )

    def load_cohort_file(self):
        path = filedialog.askopenfilename(
            title="Select Cohort CSV File", filetypes=[("CSV Files", "*.csv")]
        )
        if not path:
            return

        try:
            temp_ids = set()
            with open(path, "r", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                rows = list(reader)
            
            if not rows:
                raise ValueError("File is empty.")
            
            # Heuristic: skip first row if it looks like a header
            # (headers typically won't match ID patterns like company numbers or canonical name keys)
            first_val = rows[0][0].strip() if rows[0] else ""
            is_likely_header = (
                first_val.lower() in ("id", "entity_id", "company_number", "name", "cohort_id", "identifier")
                or not first_val  # Empty first cell suggests header row
            )
            
            start_idx = 1 if is_likely_header else 0
            
            for row in rows[start_idx:]:
                if row:
                    entity_id = row[0].strip()
                    if entity_id:
                        temp_ids.add(entity_id)

            self.cohort_ids = temp_ids
            self.cohort_status_label.config(
                text=f"Loaded {len(self.cohort_ids)} cohort IDs.", foreground="green"
            )
            self.cohort_only_check.config(state="normal")

        except Exception as e:
            self.cohort_status_label.config(
                text="Error loading file.", foreground="red"
            )
            self.app.after(0, lambda: messagebox.showerror("File Error", f"Could not read cohort file: {e}"))

    def _add_node_to_removal_list(self):
        selection = self.prune_entry.get()
        if not selection:
            return
        try:
            node_id = selection.split("(")[-1].strip(")")
            self.nodes_to_remove.add(node_id)
            self.prune_listbox.delete(0, tk.END)
            for node in sorted(list(self.nodes_to_remove)):
                label = self.full_graph.nodes.get(node, {}).get("label", node)
                self.prune_listbox.insert(tk.END, f"{label} ({node})")
            self.prune_entry.var.set("") 
        except IndexError:
            pass

    def _remove_node_from_removal_list(self):
        selection = self.prune_listbox.curselection()
        if not selection:
            return
        selected_text = self.prune_listbox.get(selection[0])
        try:
            node_id = selected_text.split("(")[-1].strip(")")
            self.nodes_to_remove.discard(node_id)
            self.prune_listbox.delete(selection[0])
        except IndexError:
            pass 

    def _clear_removal_list(self):
        self.nodes_to_remove.clear()
        self.prune_listbox.delete(0, tk.END)

    def _get_pruned_graph(self):
        if not self.full_graph:
            return nx.DiGraph() 

        pruned_graph = self.full_graph.copy()
        if self.nodes_to_remove:
            nodes_in_graph_to_remove = [
                n for n in self.nodes_to_remove if n in pruned_graph
            ]
            if nodes_in_graph_to_remove:
                pruned_graph.remove_nodes_from(nodes_in_graph_to_remove)
                self.app.after(
                    0,
                    lambda: self.status_var.set(
                        f"Temporarily removed {len(nodes_in_graph_to_remove)} node(s) for this action."
                    ),
                )
        return pruned_graph

    def build_combined_graph(self):
        self.full_graph.clear()
        self.app.after(
            0,
            lambda: self.status_var.set("Building combined graph from all sources..."),
        )
        self.app.update_idletasks()

        try:
            for filepath in self.source_files:
                filename = os.path.basename(filepath)
                with open(filepath, "r", encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        self._add_edge_to_graph(row, filename)

            if self.full_graph.number_of_nodes() > 0:
                self.app.after(
                    0,
                    lambda: self.status_var.set(
                        f"Graph built successfully with {self.full_graph.number_of_nodes()} nodes and {self.full_graph.number_of_edges()} edges."
                    ),
                )
                messagebox.showinfo(
                    "Success",
                    f"Combined graph built successfully.\n\nNodes: {self.full_graph.number_of_nodes()}\nEdges: {self.full_graph.number_of_edges()}",
                )

                self._populate_node_dropdowns()
                self.generate_full_graph_btn.config(state="normal")
                self.find_path_btn.config(state="normal")
                self.scan_dupes_btn.config(state="normal") # Enable scan
            else:
                self.app.after(
                    0,
                    lambda: self.status_var.set(
                        "No data loaded. Please add files or seed a company."
                    ),
                )

        except Exception as e:
            log_message(f"Error building combined graph: {e}")
            self.app.after(0, lambda: messagebox.showerror("File Read Error", f"Could not build graph: {e}"))
            self.app.after(0, lambda: self.status_var.set("Error building graph."))

    def _load_cohort_a(self):
        path = filedialog.askopenfilename(
            title="Select Cohort A CSV File", filetypes=[("CSV Files", "*.csv")]
        )
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                next(reader, None)
                self.cohort_a_ids = {row[0].strip() for row in reader if row}
            self.cohort_a_status_label.config(
                text=f"Loaded {len(self.cohort_a_ids)} entities.", foreground="green"
            )
            if self.cohort_b_ids:
                self.find_cohort_paths_btn.config(state="normal")
        except Exception as e:
            self.cohort_a_status_label.config(
                text="Error loading file.", foreground="red"
            )

    def _load_cohort_b(self):
        path = filedialog.askopenfilename(
            title="Select Cohort B CSV File", filetypes=[("CSV Files", "*.csv")]
        )
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                next(reader, None)
                self.cohort_b_ids = {row[0].strip() for row in reader if row}
            self.cohort_b_status_label.config(
                text=f"Loaded {len(self.cohort_b_ids)} entities.", foreground="green"
            )
            if self.cohort_a_ids:
                self.find_cohort_paths_btn.config(state="normal")
        except Exception as e:
            self.cohort_b_status_label.config(
                text="Error loading file.", foreground="red"
            )

    def _start_cohort_connection_search(self):
        if not self.full_graph.number_of_nodes() > 0:
            self.app.after(0, lambda: messagebox.showerror("Error", "Please build the combined network graph first."))
            return

        output_filepath = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            title="Save Connection Paths As",
        )
        if not output_filepath:
            return

        self.find_cohort_paths_btn.config(state="disabled")
        threading.Thread(
            target=self._run_cohort_connection_thread,
            args=(output_filepath,),
            daemon=True,
        ).start()

    def _run_cohort_connection_thread(self, output_filepath):
        try:
            self.app.after(0, lambda: self.status_var.set("Preparing graph for analysis..."))

            pruned_graph = self._get_pruned_graph()
            undirected_graph = pruned_graph.to_undirected() 

            max_hops = self.max_hops_var.get()
            shortest_only = self.shortest_only_var.get()

            cohort_a = {node for node in self.cohort_a_ids if node in undirected_graph}
            cohort_b = {node for node in self.cohort_b_ids if node in undirected_graph}

            total_pairs = len(cohort_a) * len(cohort_b)
            self.app.after(0, self.progress_bar.config, {"maximum": total_pairs, "value": 0})

            headers = [f"Hop {i+1}" for i in range(max_hops + 1)]

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
                                lambda p=processed_pairs, t=total_pairs: self.status_var.set(
                                    f"Checking pair {p}/{t}..."
                                ),
                            )
                            self.app.after(0, lambda p=processed_pairs: self.progress_bar.config(value=p))

                        if shortest_only:
                            try:
                                path = nx.shortest_path(
                                    undirected_graph, source=start_node, target=end_node
                                )
                                if len(path) - 1 <= max_hops:
                                    labeled_path = [
                                        pruned_graph.nodes[node_id].get(
                                            "label", node_id
                                        )
                                        for node_id in path
                                    ]
                                    writer.writerow(labeled_path)
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
                                writer.writerow(labeled_path)

            self.app.after(0, self.progress_bar.config, {"value": total_pairs})
            self.app.after(
                0, 
                lambda: self.status_var.set(
                    f"Search complete! Results saved to {os.path.basename(output_filepath)}"
                )
            )
            self.safe_update(
                messagebox.showinfo,
                "Success",
                f"Processing complete. The results have been saved to:\n{output_filepath}",
            )

        except Exception as e:
            log_message(f"Fatal error during cohort connection search: {e}")
            self.safe_update(messagebox.showerror, "Error", f"An unexpected error occurred: {e}")
        finally:
            self.safe_update(self.find_cohort_paths_btn.config, {"state": "normal"})

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
        if not self.full_graph:
            return

        self.all_node_labels = sorted(
            [
                f"{attrs['label']} ({node_id})"
                for node_id, attrs in self.full_graph.nodes(data=True)
            ]
        )

        self.prune_entry.set_values(self.all_node_labels)
        self.prune_entry.config(state="normal")
        self.start_node_entry.set_values(self.all_node_labels)
        self.end_node_entry.set_values(self.all_node_labels)
        self.start_node_entry.config(state="normal")
        self.end_node_entry.config(state="normal")

    def generate_full_graph(self):
        self.app.after(0, lambda: self.status_var.set("Generating full visual graph..."))
        self.app.update_idletasks()
        pruned_graph = self._get_pruned_graph() 
        self._generate_highlighted_graph(pruned_graph, path=None) 
        self.app.after(0, lambda: self.status_var.set("Full graph generation complete."))

    def find_and_highlight_path(self):
        pruned_graph = self._get_pruned_graph()
        start_selection = self.start_node_entry.get()
        end_selection = self.end_node_entry.get()
        if not start_selection or not end_selection:
            self.app.after(0, lambda: messagebox.showerror("Input Error", "Please select both entities."))
            return

        try:
            start_id = start_selection.split("(")[-1].strip(")")
            end_id = end_selection.split("(")[-1].strip(")")
        except IndexError:
            self.app.after(0, lambda: messagebox.showerror("Input Error", "Invalid node selection."))
            return

        if start_id not in pruned_graph or end_id not in pruned_graph:
            messagebox.showwarning("Node Not Found", "Selected nodes do not exist in graph.")
            return

        self.app.after(0, lambda: self.status_var.set(f"Finding shortest path..."))
        self.app.update_idletasks()

        try:
            graph_to_search = (
                pruned_graph.to_undirected()
                if not self.enforce_direction_var.get()
                else pruned_graph
            )
            path = nx.shortest_path(graph_to_search, source=start_id, target=end_id)

            path_details = "Connection Path Found:\n\n"
            for i, node_id in enumerate(path):
                node_label = pruned_graph.nodes[node_id].get("label", node_id)
                path_details += f"{i+1}. {node_label}\n"

            messagebox.showinfo("Path Found", path_details)
            self.app.after(
                0,
                lambda: self.status_var.set(f"Path found! Generating visual graph..."),
            )
            self._generate_highlighted_graph(pruned_graph, path)

        except nx.NetworkXNoPath:
            messagebox.showinfo("No Path", "No connection could be found.")
            self.app.after(0, lambda: self.status_var.set("No path found."))
        except Exception as e:
            log_message(f"Pathfinding error: {e}")
            self.app.after(0, lambda: messagebox.showerror("Error", f"An error occurred: {e}"))

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

    def _generate_highlighted_graph(self, graph_to_render, path=None):
        if self.show_cohort_networks_only_var.get():
            if not self.cohort_ids:
                messagebox.showwarning("Warning", "Please load a cohort file first.")
                return

            self.app.after(0, lambda: self.status_var.set("Filtering for cohort networks..."))
            self.app.update_idletasks()

            undirected_view = graph_to_render.to_undirected()
            connected_components = list(nx.connected_components(undirected_view))

            valid_nodes = set()
            for component in connected_components:
                cohort_nodes_in_component = [
                    node for node in component if node in self.cohort_ids
                ]
                if len(cohort_nodes_in_component) >= 2:
                    valid_nodes.update(component)

            if not valid_nodes:
                messagebox.showinfo("No Networks", "No networks connecting cohort members found.")
                self.app.after(0, lambda: self.status_var.set("Filtering complete. None found."))
                return

            graph_to_render = graph_to_render.subgraph(valid_nodes).copy()

        elif self.eliminate_unconnected_var.get():
            self.app.after(0, lambda: self.status_var.set("Filtering for connected company networks..."))
            self.app.update_idletasks()

            undirected_view = graph_to_render.to_undirected()
            connected_components = list(nx.connected_components(undirected_view))

            valid_nodes = set()
            for component in connected_components:
                company_nodes_in_component = [
                    node
                    for node in component
                    if graph_to_render.nodes[node].get("type") == "company"
                ]
                if len(company_nodes_in_component) >= 2:
                    valid_nodes.update(component)

            if not valid_nodes:
                messagebox.showinfo("No Networks", "No networks connecting companies found.")
                return

            graph_to_render = graph_to_render.subgraph(valid_nodes).copy()

        net = Network(height="95vh", width="100%", directed=True, notebook=False, cdn_resources="local")
        net.set_options("""var options = {"configure": {"enabled": true }, "physics": {"solver": "forceAtlas2Based"}}""")

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
            border_colors = ["#FF00FF", "#00FFFF", "#FFD700", "#ADFF2F", "#FF69B4", "#BA55D3"]
            unique_sources = sorted(
                list({name for attrs in graph_to_render.nodes.values() for name in attrs.get("source_files", set())})
            )
            file_color_map = {source: color for source, color in zip(unique_sources, border_colors)}

        for node_id, attrs in graph_to_render.nodes(data=True):
            node_type = attrs.get("type")
            base_color = "#B9D9EB" if node_type == "company" else ("#FFB347" if node_type == "address" else "#D9E8B9")
            size = 15
            shape = "box" if node_type in ["company", "address"] else "ellipse"
            final_color = base_color
            border_width = 1
            shape_properties = {}

            if self.cohort_ids and node_id in self.cohort_ids:
                shape_properties["borderDashes"] = [10, 10]
                border_width = 5
                size = 30

            if path and node_id in path:
                final_color = "#FF0000"
                size = 25

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
            net.add_node(
                node_id,
                label=safe_label_multiline,
                title=safe_title,
                color=final_color,
                borderWidth=border_width,
                size=size,
                shape=shape,
                shapeProperties=shape_properties,
            )

        for source, target, edge_attrs in graph_to_render.edges(data=True):
            width = 1
            edge_color = "#848484"
            if path and (source, target) in path_edges:
                width = 5
                edge_color = "#FF0000"
            safe_title = html.escape(edge_attrs.get("label", ""))
            net.add_edge(source, target, title=safe_title, width=width, color=edge_color)

        try:
            filename = os.path.join(CONFIG_DIR, "combined_network_graph.html")
            net.write_html(filename, notebook=False)
            self.app.after(0, lambda: self.status_var.set("Graph generated! Opening in browser..."))
            webbrowser.open(f"file://{os.path.realpath(filename)}")
        except Exception as e:
            log_message(f"Failed to save or open combined graph: {e}")
            self.app.after(0, lambda: messagebox.showerror("Graph Error", f"Could not save graph: {e}"))

    def start_seed_fetch(self):
        seed_cnum_raw = self.seed_cnum_var.get()
        seed_cnum = self._clean_company_number(seed_cnum_raw)
        if not seed_cnum:
            self.app.after(0, lambda: messagebox.showerror("Input Error", "Please enter a valid company number."))
            return

        self.seed_btn.config(state="disabled")
        self.app.after(0, lambda: self.status_var.set(f"Seeding network with {seed_cnum}..."))
        self.cancel_flag.clear()
        threading.Thread(target=self._run_seed_fetch_thread, args=(seed_cnum,), daemon=True).start()

    def _run_seed_fetch_thread(self, seed_cnum):
        fetch_pscs = self.seed_fetch_pscs_var.get()
        fetch_associated = self.seed_fetch_associated_var.get()

        try:
            self.app.after(0, lambda: self.status_var.set(f"Fetching officers for {seed_cnum}..."))
            officers, error = ch_get_data(
                self.api_key,
                self.ch_token_bucket,
                f"/company/{seed_cnum}/officers?items_per_page=100",
            )
            if error or not officers or not officers.get("items"):
                raise ValueError(f"Could not fetch officers for {seed_cnum}.")

            if fetch_associated:
                self.app.after(0, lambda: self.status_var.set(f"Found {len(officers['items'])} officers. Fetching appointments..."))
                all_appointments = []
                with ThreadPoolExecutor(max_workers=2) as executor:
                    future_to_officer = {
                        executor.submit(self._fetch_officer_appointments, o.get("links", {})): o
                        for o in officers["items"]
                    }
                    for future in as_completed(future_to_officer):
                        if self.cancel_flag.is_set(): return
                        appointments = future.result()
                        if appointments: all_appointments.extend(appointments)

                unique_company_numbers = {
                    app.get("appointed_to", {}).get("company_number")
                    for app in all_appointments
                }
                unique_company_numbers.add(seed_cnum)
            else:
                unique_company_numbers = {seed_cnum}

            self.app.after(0, lambda: self.status_var.set(f"Found {len(unique_company_numbers)} companies. Building network..."))
            temp_graph = nx.DiGraph()
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_to_cnum = {
                    executor.submit(self._fetch_company_network_data, cnum, fetch_pscs): cnum
                    for cnum in unique_company_numbers if cnum
                }
                for i, future in enumerate(as_completed(future_to_cnum)):
                    if self.cancel_flag.is_set(): return
                    self.app.after(0, lambda: self.status_var.set(f"Processing company {i+1}/{len(unique_company_numbers)}..."))
                    profile, officers_data, pscs_data = future.result()
                    if profile:
                        self._add_company_to_graph(temp_graph, profile, officers_data, pscs_data)

            self.app.after(100, lambda: self._save_graph_to_temp_csv(temp_graph, seed_cnum))

        except Exception as e:
            self.app.after(0, lambda: messagebox.showerror("Error", f"Failed to seed network: {e}"))
            self.app.after(0, lambda: self.status_var.set("Error during seeding."))
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
                key = self._get_canonical_name_key(name, dob)
                G.add_node(key, label=name, type="person")
                G.add_edge(cnum, key, label=o.get("officer_role", "officer"))
        if pscs:
            for p in pscs.get("items", []):
                name = p.get("name")
                if not name: continue
                dob = p.get("date_of_birth")
                key = self._get_canonical_name_key(name, dob)
                G.add_node(key, label=name, type="person")
                G.add_edge(cnum, key, label="psc")

    def _get_canonical_name_key(self, name: str, dob_obj: dict) -> str:
        if not name: return ""
        cleaned_name = name.lower()
        titles = ["mr", "mrs", "ms", "miss", "dr", "prof", "sir", "dame", "rev"]
        for title in titles:
            cleaned_name = re.sub(r"\b" + re.escape(title) + r"\b\.?", "", cleaned_name).strip()
        if "," in cleaned_name:
            parts = cleaned_name.split(",", 1)
            cleaned_name = f"{parts[1].strip()} {parts[0].strip()}"
        cleaned_name = re.sub(r"[^a-z0-9\s]", "", cleaned_name)
        tokens = cleaned_name.split()
        if not tokens: return ""
        name_key = tokens[0] + tokens[-1] if len(tokens) > 1 else tokens[0]
        if dob_obj and "year" in dob_obj and "month" in dob_obj:
            return f"{name_key}-{dob_obj['year']}-{dob_obj['month']}"
        else:
            return name_key

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
        self.source_files.append(filepath)
        self.file_listbox.insert(tk.END, f"Seed: {os.path.basename(filepath)}")
        self.build_btn.config(state="normal")
        self.app.after(0, lambda: self.status_var.set(f"Successfully seeded network for {os.path.basename(filepath).split('-')[1]}. Ready to build."))

    def _clean_company_number(self, cnum_raw):
        if not cnum_raw or not isinstance(cnum_raw, str): return None
        cleaned_num = cnum_raw.strip().upper()
        if cleaned_num.startswith(("SC", "NI", "OC", "LP", "SL", "SO", "NC", "NL")): return cleaned_num
        elif cleaned_num.isdigit(): return cleaned_num.zfill(8)
        return cleaned_num
