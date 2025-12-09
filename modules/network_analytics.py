# module/network_analytics.py

import csv
import html
import os
import re
import textwrap
import threading
import time
import webbrowser
import tkinter as tk
from typing import List
from tkinter import ttk, filedialog, messagebox
from concurrent.futures import ThreadPoolExecutor, as_completed

import networkx as nx
from pyvis.network import Network

from ..ui.searchable_entry import SearchableEntry
# --- From Our Package ---
# API functions (were global functions in original file)
from ..api.companies_house import ch_get_data

# Constants (were at top of original file)
from ..constants import (
    CONFIG_DIR,
)

# Utility functions (were global functions or duplicated in classes)
from ..utils.helpers import log_message, clean_address_string

# UI components (were classes in original file)
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

        # --- UI Setup ---
        # Step 1: Seed Network
        seed_frame = ttk.LabelFrame(
            self.content_frame,
            text="Step 1: Seed Network with a Company (Optional)",
            padding=10,
        )
        seed_frame.pack(fill=tk.X, pady=5, padx=10)
        
        # Top row: Company number entry and button
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
        Tooltip(
            seed_entry,
            "Enter a company number to fetch its directors and registered address, adding them to the graph.",
        )
        
        # Bottom row: Checkboxes for optional data
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
        Tooltip(seed_pscs_cb, "Also fetch Persons with Significant Control for each company.")
        
        self.seed_fetch_associated_var = tk.BooleanVar(value=False)
        seed_associated_cb = ttk.Checkbutton(
            seed_options_row,
            text="Fetch all associated companies",
            variable=self.seed_fetch_associated_var,
        )
        seed_associated_cb.pack(side=tk.LEFT, padx=(0, 5))
        Tooltip(
            seed_associated_cb, 
            "Fetch all other companies where each director holds an appointment. Warning: may result in a large number of API calls."
        )
        
        # Warning label that appears when associated companies is checked
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

        # Step 2: Add Graph & Cohort Data (Unchanged)
        upload_frame = ttk.LabelFrame(
            self.content_frame, text="Step 2: Add Exported Graph Files", padding=10
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
        # Create a frame to hold the listbox and scrollbar together
        file_list_frame = ttk.Frame(upload_frame)
        file_list_frame.pack(fill=tk.X, expand=True, pady=5)

        # Create the scrollbar and place it in the frame
        file_scrollbar = ttk.Scrollbar(file_list_frame, orient=tk.VERTICAL)
        file_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Create the listbox, place it in the frame, and link it to the scrollbar
        self.file_listbox = tk.Listbox(
            file_list_frame, height=4, yscrollcommand=file_scrollbar.set
        )
        self.file_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Tell the scrollbar what to control
        file_scrollbar.config(command=self.file_listbox.yview)

        cohort_frame = ttk.LabelFrame(
            self.content_frame,
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
        Tooltip(
            cohort_buttons_frame,
            "Upload a simple, one-column CSV of company numbers or target IDs (for people or addresses).\nThese will be highlighted as 'root' nodes on the final graph.",
        )

        # --- NEW Step 3: Build Combined Network ---
        build_frame = ttk.LabelFrame(
            self.content_frame, text="Step 3: Build Combined Network", padding=10
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
        Tooltip(
            self.build_btn,
            "Read all source files and build the master graph object in memory.\nThis must be done before you can analyze or visualize the network.",
        )

        # --- NEW Step 4: Remove Entities (Optional) ---
        prune_frame = ttk.LabelFrame(
            self.content_frame, text="Step 4: Remove Entities (Optional)", padding=10
        )
        prune_frame.pack(fill=tk.X, pady=5, padx=10)
        prune_top_frame = ttk.Frame(prune_frame)
        prune_top_frame.pack(fill=tk.X, expand=True)
        self.prune_entry = SearchableEntry(prune_top_frame)
        self.prune_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        self.prune_entry.config(state="disabled")
        add_prune_btn = ttk.Button(
            prune_top_frame,
            text="Add to Removal List",
            command=self._add_node_to_removal_list,
        )
        add_prune_btn.pack(side=tk.LEFT)
        Tooltip(
            add_prune_btn,
            "Select an entity and add it to a list of nodes that will be excluded from the graph.",
        )
        self.prune_listbox = tk.Listbox(prune_frame, height=4)
        self.prune_listbox.pack(fill=tk.X, expand=True, pady=5)
        prune_bottom_frame = ttk.Frame(prune_frame)
        prune_bottom_frame.pack(fill=tk.X, expand=True)
        remove_prune_btn = ttk.Button(
            prune_bottom_frame,
            text="Remove Selected",
            command=self._remove_node_from_removal_list,
        )
        remove_prune_btn.pack(side=tk.LEFT)
        clear_prune_btn = ttk.Button(
            prune_bottom_frame, text="Clear List", command=self._clear_removal_list
        )
        clear_prune_btn.pack(side=tk.LEFT, padx=5)

        # --- NEW Step 5: Generate Full Visual Graph ---
        visualize_frame = ttk.LabelFrame(
            self.content_frame, text="Step 5: Generate Full Visual Graph", padding=10
        )
        visualize_frame.pack(fill=tk.X, pady=5, padx=10)
        self.distinguish_var = tk.BooleanVar(value=True)
        distinguish_check = ttk.Checkbutton(
            visualize_frame,
            text="Visually distinguish nodes by source file",
            variable=self.distinguish_var,
        )
        distinguish_check.pack(pady=5, anchor="w")
        Tooltip(
            distinguish_check,
            "Adds a colored border to nodes to show which file they originated from.\nShared nodes (appearing in multiple files) will have a black border.",
        )
        self.eliminate_unconnected_var = tk.BooleanVar(value=False)
        eliminate_check = ttk.Checkbutton(
            visualize_frame,
            text="Eliminate unconnected companies from visualisation",
            variable=self.eliminate_unconnected_var,
        )
        eliminate_check.pack(pady=5, anchor="w")
        Tooltip(
            eliminate_check,
            "If checked, the visual graph will only show networks that connect at least two companies.\nEntities that are not part of a larger company network will be hidden.",
        )
        self.show_cohort_networks_only_var = tk.BooleanVar(value=False)
        self.cohort_only_check = ttk.Checkbutton(
            visualize_frame,
            text="Show only networks connecting cohort members",
            variable=self.show_cohort_networks_only_var,
            state="disabled",
        )
        self.cohort_only_check.pack(pady=5, anchor="w")
        Tooltip(
            self.cohort_only_check,
            "Requires a cohort file to be loaded.\nFilters the graph to show only the networks that connect two or more of your cohort.",
        )
        self.generate_full_graph_btn = ttk.Button(
            visualize_frame,
            text="Generate Full Visual Graph",
            state="disabled",
            command=self.generate_full_graph,
        )
        self.generate_full_graph_btn.pack(pady=5)
        Tooltip(
            self.generate_full_graph_btn,
            "Generate and open a visual graph of the entire combined network.",
        )

        # --- NEW Step 6: Find Connection Between Two Entities ---
        path_frame = ttk.LabelFrame(
            self.content_frame,
            text="Step 6: Find Connection Between Two Entities",
            padding=10,
        )
        path_frame.pack(fill=tk.X, pady=5, padx=10)
        ttk.Label(path_frame, text="Start Entity:").grid(
            row=0, column=0, sticky="w", padx=5, pady=2
        )
        self.start_node_entry = SearchableEntry(path_frame)
        self.start_node_entry.grid(row=1, column=0, sticky="ew", padx=5, pady=2)
        self.start_node_entry.config(state="disabled")
        ttk.Label(path_frame, text="End Entity:").grid(
            row=0, column=1, sticky="w", padx=5, pady=2
        )
        self.end_node_entry = SearchableEntry(path_frame)
        self.end_node_entry.grid(row=1, column=1, sticky="ew", padx=5, pady=2)
        self.end_node_entry.config(state="disabled")
        path_frame.columnconfigure(0, weight=1)
        path_frame.columnconfigure(1, weight=1)
        self.enforce_direction_var = tk.BooleanVar(value=False)
        enforce_check = ttk.Checkbutton(
            path_frame,
            text="Enforce connection direction (strict one-way path)",
            variable=self.enforce_direction_var,
        )
        enforce_check.grid(row=2, column=0, columnspan=2, sticky="w", padx=5)
        Tooltip(
            enforce_check,
            "If checked, the search will only find a path that follows the formal\ndirection of the relationships (e.g., from a company to its director).\nLeave unchecked (default) to find any potential link, however indirect.",
        )
        self.find_path_btn = ttk.Button(
            path_frame,
            text="Find & Highlight Shortest Path",
            state="disabled",
            command=self.find_and_highlight_path,
        )
        self.find_path_btn.grid(row=3, column=0, columnspan=2, pady=10)

        # --- NEW: Step 7 for Finding Connections Between Cohorts ---
        cohort_connect_frame = ttk.LabelFrame(
            self.content_frame,
            text="Step 7: Find Connections Between Cohorts",
            padding=10,
        )
        cohort_connect_frame.pack(fill=tk.X, pady=5, padx=10)

        # File Uploads for Cohorts A and B
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

        # Options Frame for Hops and Toggle
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
        Tooltip(
            self.shortest_only_check,
            "If checked, finds only the single shortest path for each pair.\nIf unchecked, finds ALL possible paths up to the hop limit (can be very slow).",
        )

        # Action Button
        self.find_cohort_paths_btn = ttk.Button(
            cohort_connect_frame,
            text="Find Connections & Export...",
            state="disabled",
            command=self._start_cohort_connection_search,
        )
        self.find_cohort_paths_btn.pack(pady=5, ipady=5)

        # Status Bar
        status_frame = ttk.Frame(self.content_frame)
        status_frame.pack(fill=tk.X, pady=10, padx=10, side=tk.BOTTOM)
        self.progress_bar = ttk.Progressbar(
            status_frame, orient="horizontal", length=300, mode="determinate"
        )
        self.progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        self.status_var = tk.StringVar(value="Ready. Please add or seed data to begin.")
        ttk.Label(status_frame, textvariable=self.status_var).pack(side=tk.LEFT)

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
        # Clean up temporary seed files
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
        self.app.after(
            0,
            lambda: self.status_var.set(
                "File list cleared. Please add or seed data to begin."
            ),
        )

    def load_cohort_file(self):
        """Loads a single-column CSV of entity IDs to be highlighted."""
        path = filedialog.askopenfilename(
            title="Select Cohort CSV File", filetypes=[("CSV Files", "*.csv")]
        )
        if not path:
            return

        try:
            temp_ids = set()
            with open(path, "r", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                # Skip header if it exists
                try:
                    next(reader)
                except StopIteration:
                    pass  # File is empty

                for row in reader:
                    if row:  # Ensure row is not empty
                        # --- FIX: Read the raw ID without cleaning/padding ---
                        entity_id = row[0].strip()
                        if entity_id:
                            temp_ids.add(entity_id)

            self.cohort_ids = temp_ids
            self.cohort_status_label.config(
                text=f"Loaded {len(self.cohort_ids)} cohort IDs.", foreground="green"
            )
            log_message(
                f"Loaded {len(self.cohort_ids)} cohort IDs from {os.path.basename(path)}"
            )
            self.cohort_only_check.config(state="normal")

        except Exception as e:
            self.cohort_status_label.config(
                text="Error loading file.", foreground="red"
            )
            messagebox.showerror(
                "File Error",
                f"Could not read cohort file. Ensure it is a single-column CSV.\n\nError: {e}",
            )
            log_message(f"Failed to load cohort file: {e}")

    def _add_node_to_removal_list(self):
        """Adds the selected node from the entry to the removal set and updates the listbox."""
        selection = self.prune_entry.get()
        if not selection:
            return

        try:
            # Parse the ID from the string format: "Label (ID)"
            node_id = selection.split("(")[-1].strip(")")
            self.nodes_to_remove.add(node_id)

            # Refresh the listbox
            self.prune_listbox.delete(0, tk.END)
            for node in sorted(list(self.nodes_to_remove)):
                # Find the full label from the graph data for display
                label = self.full_graph.nodes.get(node, {}).get("label", node)
                self.prune_listbox.insert(tk.END, f"{label} ({node})")

            self.prune_entry.var.set("")  # Clear the entry box
        except IndexError:
            messagebox.showwarning(
                "Invalid Selection",
                "Please select a valid node from the dropdown list.",
            )

    def _remove_node_from_removal_list(self):
        """Removes the selected item from the removal listbox and the underlying set."""
        selection = self.prune_listbox.curselection()
        if not selection:
            return

        selected_text = self.prune_listbox.get(selection[0])
        try:
            node_id = selected_text.split("(")[-1].strip(")")
            self.nodes_to_remove.discard(node_id)
            self.prune_listbox.delete(selection[0])
        except IndexError:
            pass  # Should not happen, but safe to ignore

    def _clear_removal_list(self):
        """Clears the entire removal list and set."""
        self.nodes_to_remove.clear()
        self.prune_listbox.delete(0, tk.END)

    def _get_pruned_graph(self):
        """Creates a copy of the full graph and removes nodes from the removal list."""
        if not self.full_graph:
            return nx.DiGraph()  # Return empty graph if base isn't built

        pruned_graph = self.full_graph.copy()
        if self.nodes_to_remove:
            # Get a list of nodes to remove that actually exist in the graph
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
            else:
                self.app.after(
                    0,
                    lambda: self.status_var.set(
                        "No data loaded. Please add files or seed a company."
                    ),
                )

        except Exception as e:
            log_message(f"Error building combined graph: {e}")
            messagebox.showerror(
                "File Read Error",
                f"Could not build graph. Ensure files are valid edge lists.\n\nError: {e}",
            )
            self.app.after(0, lambda: self.status_var.set("Error building graph."))

    def _load_cohort_a(self):
        """Loads entity IDs for Cohort A from a single-column CSV."""
        path = filedialog.askopenfilename(
            title="Select Cohort A CSV File", filetypes=[("CSV Files", "*.csv")]
        )
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                # Skip header
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
            messagebox.showerror("File Error", f"Could not read Cohort A file: {e}")

    def _load_cohort_b(self):
        """Loads entity IDs for Cohort B from a single-column CSV."""
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
            messagebox.showerror("File Error", f"Could not read Cohort B file: {e}")

    def _start_cohort_connection_search(self):
        """Prompts for save location and starts the background thread for pathfinding."""
        if not self.full_graph.number_of_nodes() > 0:
            messagebox.showerror(
                "Error", "Please build the combined network graph first (Step 3)."
            )
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
        """The main background process for finding and streaming cohort connections."""
        try:
            self.app.after(
                0, lambda: self.status_var.set("Preparing graph for analysis...")
            )

            pruned_graph = self._get_pruned_graph()
            undirected_graph = pruned_graph.to_undirected()  # Paths can go either way

            max_hops = self.max_hops_var.get()
            shortest_only = self.shortest_only_var.get()

            # Filter cohort IDs to only those that actually exist in the pruned graph
            cohort_a = {node for node in self.cohort_a_ids if node in undirected_graph}
            cohort_b = {node for node in self.cohort_b_ids if node in undirected_graph}

            total_pairs = len(cohort_a) * len(cohort_b)
            self.app.after(
                0, self.progress_bar.config, {"maximum": total_pairs, "value": 0}
            )

            # Prepare CSV headers up to the max hop limit
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
                        if processed_pairs % 20 == 0:  # Update status bar periodically
                            self.app.after(
                                0,
                                lambda: self.status_var.set(
                                    f"Checking pair {processed_pairs}/{total_pairs}..."
                                ),
                            )
                            self.app.after(
                                0, self.progress_bar.config, {"value": processed_pairs}
                            )

                        if shortest_only:
                            try:
                                path = nx.shortest_path(
                                    undirected_graph, source=start_node, target=end_node
                                )
                                if len(path) - 1 <= max_hops:
                                    # Get labels for the path
                                    labeled_path = [
                                        pruned_graph.nodes[node_id].get(
                                            "label", node_id
                                        )
                                        for node_id in path
                                    ]
                                    writer.writerow(labeled_path)
                            except nx.NetworkXNoPath:
                                continue
                        else:  # All paths
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
            self.safe_update(
                self.status_var.set,
                f"Search complete! Results saved to {os.path.basename(output_filepath)}",
            )
            self.safe_update(
                messagebox.showinfo,
                "Success",
                f"Processing complete. The results have been saved to:\n{output_filepath}",
            )

        except Exception as e:
            log_message(f"Fatal error during cohort connection search: {e}")
            self.safe_update(
                messagebox.showerror, "Error", f"An unexpected error occurred: {e}"
            )
            self.safe_update(self.status_var.set, "An error occurred.")
        finally:
            self.safe_update(self.find_cohort_paths_btn.config, {"state": "normal"})

    def _add_edge_to_graph(self, edge_data, source_name):
        """Helper to add a single edge and its nodes to the graph."""
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
        """Populates the searchable entry widgets with the list of all nodes."""
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
        self.app.after(
            0, lambda: self.status_var.set("Generating full visual graph...")
        )
        self.app.update_idletasks()
        pruned_graph = self._get_pruned_graph()  # Get the pruned copy
        self._generate_highlighted_graph(
            pruned_graph, path=None
        )  # Pass it to the renderer
        self.app.after(
            0, lambda: self.status_var.set("Full graph generation complete.")
        )

    def find_and_highlight_path(self):
        """Finds shortest path, applying pruning first and updating dropdowns."""
        # 1. Create a pruned graph for this specific action
        pruned_graph = self._get_pruned_graph()

        start_selection = self.start_node_entry.get()
        end_selection = self.end_node_entry.get()
        if not start_selection or not end_selection:
            messagebox.showerror(
                "Input Error", "Please select both a start and an end entity."
            )
            return

        try:
            start_id = start_selection.split("(")[-1].strip(")")
            end_id = end_selection.split("(")[-1].strip(")")
        except IndexError:
            messagebox.showerror(
                "Input Error", "Invalid node selection. Please choose from the list."
            )
            return

        # 2. Check if the selected nodes still exist in the pruned graph
        if start_id not in pruned_graph or end_id not in pruned_graph:
            messagebox.showwarning(
                "Node Not Found",
                "One or both selected nodes were removed from the graph or do not exist. Please choose different entities.",
            )
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

            # 3. Pass the pruned graph and the found path to the renderer
            self._generate_highlighted_graph(pruned_graph, path)

        except nx.NetworkXNoPath:
            messagebox.showinfo(
                "No Path",
                "No connection could be found between the selected entities in the (potentially pruned) graph.",
            )
            self.app.after(0, lambda: self.status_var.set("No path found."))
        except Exception as e:
            log_message(f"Pathfinding error: {e}")
            messagebox.showerror("Error", f"An error occurred during pathfinding: {e}")

    def _fetch_company_network_data(self, company_number):
        """Worker to fetch profile, officers, and PSCs for one company."""
        profile, _ = ch_get_data(
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
        return profile, officers, pscs

    def _generate_highlighted_graph(self, graph_to_render, path=None):

        # --- MODIFIED: Filtering logic is now at the top ---
        if self.show_cohort_networks_only_var.get():
            if not self.cohort_ids:
                messagebox.showwarning(
                    "Warning",
                    "Please load a cohort file to use the 'Show only cohort networks' feature.",
                )
                return

            self.app.after(
                0, lambda: self.status_var.set("Filtering for cohort networks...")
            )
            self.app.update_idletasks()

            undirected_view = graph_to_render.to_undirected()
            connected_components = list(nx.connected_components(undirected_view))

            valid_nodes = set()
            for component in connected_components:
                # Find cohort members within this component
                cohort_nodes_in_component = [
                    node for node in component if node in self.cohort_ids
                ]
                # If there are 2 or more cohort members, it's a valid network
                if len(cohort_nodes_in_component) >= 2:
                    valid_nodes.update(component)

            if not valid_nodes:
                messagebox.showinfo(
                    "No Cohort Networks Found",
                    "After filtering, no networks connecting two or more of your cohort members were found.",
                )
                self.app.after(
                    0,
                    lambda: self.status_var.set(
                        "Filtering complete. No connected cohort networks found."
                    ),
                )
                return

            graph_to_render = graph_to_render.subgraph(valid_nodes).copy()

        elif self.eliminate_unconnected_var.get():
            self.app.after(
                0,
                lambda: self.status_var.set(
                    "Filtering for connected company networks..."
                ),
            )
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
                messagebox.showinfo(
                    "No Networks Found",
                    "After filtering, no networks connecting two or more companies were found.",
                )
                self.app.after(
                    0,
                    lambda: self.status_var.set(
                        "Filtering complete. No connected company networks found."
                    ),
                )
                return

            graph_to_render = graph_to_render.subgraph(valid_nodes).copy()

        net = Network(
            height="95vh",
            width="100%",
            directed=True,
            notebook=False,
            cdn_resources="local",
        )

        net.set_options(
            """var options = {"configure": {"enabled": true }, "physics": {"solver": "forceAtlas2Based"}}"""
        )

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
            border_colors = [
                "#FF00FF",
                "#00FFFF",
                "#FFD700",
                "#ADFF2F",
                "#FF69B4",
                "#BA55D3",
            ]
            unique_sources = sorted(
                list(
                    {
                        name
                        for attrs in graph_to_render.nodes.values()
                        for name in attrs.get("source_files", set())
                    }
                )
            )
            file_color_map = {
                source: color for source, color in zip(unique_sources, border_colors)
            }

        for node_id, attrs in graph_to_render.nodes(data=True):
            node_type = attrs.get("type")
            base_color = (
                "#B9D9EB"
                if node_type == "company"
                else ("#FFB347" if node_type == "address" else "#D9E8B9")
            )
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
                label_lines.append(f"({html.escape(node_id)})")  # company number
            elif node_type == "address":
                wrapped = "\n".join(textwrap.wrap(raw_label, width=25))
                label_lines.append(html.escape(wrapped))  # NO id appended
            else:  # person / other
                label_lines.append(html.escape(raw_label))
                if "-" in str(node_id):
                    try:
                        name_key, year, month = str(node_id).rsplit("-", 2)
                        label_lines.append(f"({month}/{year})")
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
            net.add_edge(
                source, target, title=safe_title, width=width, color=edge_color
            )

        try:
            filename = os.path.join(CONFIG_DIR, "combined_network_graph.html")
            net.write_html(filename, notebook=False)
            self.app.after(
                0, lambda: self.status_var.set("Graph generated! Opening in browser...")
            )
            webbrowser.open(f"file://{os.path.realpath(filename)}")
        except Exception as e:
            log_message(f"Failed to save or open combined graph: {e}")
            messagebox.showerror(
                "Graph Error", f"Could not save or open the graph file: {e}"
            )

    def start_seed_fetch(self):
        seed_cnum_raw = self.seed_cnum_var.get()
        seed_cnum = self._clean_company_number(seed_cnum_raw)
        if not seed_cnum:
            messagebox.showerror(
                "Input Error",
                "Please enter a valid company number to seed the network.",
            )
            return

        self.seed_btn.config(state="disabled")
        self.app.after(
            0, lambda: self.status_var.set(f"Seeding network with {seed_cnum}...")
        )
        self.cancel_flag.clear()
        threading.Thread(
            target=self._run_seed_fetch_thread, args=(seed_cnum,), daemon=True
        ).start()

    def _run_seed_fetch_thread(self, seed_cnum):
        # Read checkbox values at the start of the thread
        fetch_pscs = self.seed_fetch_pscs_var.get()
        fetch_associated = self.seed_fetch_associated_var.get()

        try:
            self.app.after(
                0, lambda: self.status_var.set(f"Fetching officers for {seed_cnum}...")
            )
            officers, error = ch_get_data(
                self.api_key,
                self.ch_token_bucket,
                f"/company/{seed_cnum}/officers?items_per_page=100",
            )
            if error or not officers or not officers.get("items"):
                raise ValueError(
                    f"Could not fetch officers for {seed_cnum}. Is it a valid company number?"
                )

            # If fetching associated companies, get all appointments for each officer
            if fetch_associated:
                self.app.after(
                    0,
                    lambda: self.status_var.set(
                        f"Found {len(officers['items'])} officers. Fetching all their appointments..."
                    ),
                )
                all_appointments = []
                with ThreadPoolExecutor(max_workers=2) as executor:
                    future_to_officer = {
                        executor.submit(
                            self._fetch_officer_appointments, o.get("links", {})
                        ): o
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
                # Only fetch the seed company
                unique_company_numbers = {seed_cnum}

            self.app.after(
                0,
                lambda: self.status_var.set(
                    f"Found {len(unique_company_numbers)} {'related companies' if fetch_associated else 'company'}. Building network..."
                ),
            )
            temp_graph = nx.DiGraph()
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_to_cnum = {
                    executor.submit(
                        self._fetch_company_network_data, cnum, fetch_pscs
                    ): cnum
                    for cnum in unique_company_numbers
                    if cnum
                }
                for i, future in enumerate(as_completed(future_to_cnum)):
                    if self.cancel_flag.is_set():
                        return
                    self.app.after(
                        0,
                        lambda: self.status_var.set(
                            f"Processing company {i+1}/{len(unique_company_numbers)}..."
                        ),
                    )
                    profile, officers_data, pscs_data = future.result()
                    if profile:
                        self._add_company_to_graph(
                            temp_graph, profile, officers_data, pscs_data
                        )

            self.after(100, self._save_graph_to_temp_csv, temp_graph, seed_cnum)

        except Exception as e:
            messagebox.showerror("Error", f"Failed to seed network: {e}")
            self.app.after(0, lambda: self.status_var.set("Error during seeding."))
        finally:
            self.after(100, lambda: self.seed_btn.config(state="normal"))

    def _fetch_officer_appointments(self, officer_links: dict) -> List[dict]:
        """
        Return *all* appointment objects for a single natural‑person officer,
        following the `links.officer.appointments` URL and iterating through
        every page.

        officer_links – the 'links' dict from each item in
                        /company/{cnum}/officers

        Returns an empty list on error.
        """
        # 1. Locate the correct URL
        base_path = officer_links.get("officer", {}).get("appointments")
        if not base_path:  # corporate PSCs can lack this link
            return []

        page_size = 100  # CH maximum
        start_index = 0
        all_items = []

        while True:
            # Build the paged URL
            path = (
                f"{base_path}?items_per_page={page_size}" f"&start_index={start_index}"
            )

            data, err = ch_get_data(
                self.api_key, self.ch_token_bucket, path  # same helper you already use
            )
            if err or not data:
                log_message(
                    f"Officer‐appointments fetch failed for "
                    f"{base_path} (page {start_index}): {err}"
                )
                break

            page_items = data.get("items", [])
            all_items.extend(page_items)

            # If we’ve reached the end, stop.
            if len(page_items) < page_size:
                break

            start_index += page_size  # next page

        return all_items

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

    def _add_company_to_graph(self, G, profile, officers, pscs):
        cnum = profile.get("company_number")
        G.add_node(cnum, label=profile.get("company_name", cnum), type="company")
        addr_data = profile.get("registered_office_address", {})
        raw_address_str = ", ".join(
            filter(
                None,
                [
                    addr_data.get("address_line_1"),
                    addr_data.get("locality"),
                    addr_data.get("postal_code"),
                ],
            )
        )

        # --- APPLY THE CLEANING FUNCTION ---
        address_str = _clean_address_string(raw_address_str)

        if address_str:
            # Use the raw string for the visual label, but the clean string for the node ID
            G.add_node(address_str, label=raw_address_str, type="address")
            G.add_edge(cnum, address_str, label="registered_at")
        if officers:
            for o in officers.get("items", []):
                name = o.get("name")
                if not name:
                    continue
                dob = o.get("date_of_birth")
                key = self._get_canonical_name_key(name, dob)
                G.add_node(key, label=name, type="person")
                G.add_edge(cnum, key, label=o.get("officer_role", "officer"))
        if pscs:
            for p in pscs.get("items", []):
                name = p.get("name")
                if not name:
                    continue
                dob = p.get("date_of_birth")
                key = self._get_canonical_name_key(name, dob)
                G.add_node(key, label=name, type="person")
                G.add_edge(cnum, key, label="psc")

    def _get_canonical_name_key(self, name: str, dob_obj: dict) -> str:
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
            return f"{name_key}-{dob_obj['year']}-{dob_obj['month']}"
        else:
            return name_key

    def _save_graph_to_temp_csv(self, G, seed_cnum):
        """Saves a graph object to a temporary edge list CSV file."""
        if G.number_of_edges() == 0:
            self.app.after(
                0,
                lambda: self.status_var.set(
                    f"Seed for {seed_cnum} found no connections."
                ),
            )
            return

        filename = f"Seed-{seed_cnum}-{int(time.time())}.csv"
        filepath = os.path.join(CONFIG_DIR, filename)

        headers = [
            "source_id",
            "source_label",
            "source_type",
            "target_id",
            "target_label",
            "target_type",
            "relationship",
        ]
        try:
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                for u, v, data in G.edges(data=True):
                    writer.writerow(
                        [
                            u,
                            G.nodes[u].get("label", ""),
                            G.nodes[u].get("type", ""),
                            v,
                            G.nodes[v].get("label", ""),
                            G.nodes[v].get("type", ""),
                            data.get("label", ""),
                        ]
                    )
            self.after(100, self._add_seed_file_to_list, filepath)
        except IOError as e:
            log_message(f"Could not write temp seed file {filepath}: {e}")
            self.app.after(0, lambda: self.status_var.set("Error creating seed file."))

    def _add_seed_file_to_list(self, filepath):
        """Adds the path of a newly created seed file to the list."""
        self.source_files.append(filepath)
        self.file_listbox.insert(tk.END, f"Seed: {os.path.basename(filepath)}")
        self.build_btn.config(state="normal")
        self.app.after(
            0,
            lambda: self.status_var.set(
                f"Successfully seeded network for {os.path.basename(filepath).split('-')[1]}. Ready to build."
            ),
        )

    def _clean_company_number(self, cnum_raw):
        """Applies robust cleaning and padding for UK company numbers."""
        if not cnum_raw or not isinstance(cnum_raw, str):
            return None
        cleaned_num = cnum_raw.strip().upper()
        if cleaned_num.startswith(("SC", "NI", "OC", "LP", "SL", "SO", "NC", "NL")):
            return cleaned_num
        elif cleaned_num.isdigit():
            return cleaned_num.zfill(8)
        return cleaned_num
