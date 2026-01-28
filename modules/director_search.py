# modules/director_search.py
"""Director search module."""
import csv
import html
import os
import re
import textwrap
import threading
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Third-Party ---
import networkx as nx
from pyvis.network import Network

# --- Tkinter ---
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

# --- From Our Package ---
# API functions (were global functions in original file)
from ..api.companies_house import ch_get_data
from ..api.grantnav import grantnav_get_data

# Constants (were at top of original file)
from ..constants import (
    CONFIG_DIR,
    API_BASE_URL,
    GRANTNAV_API_BASE_URL,
    GRANT_DATA_FIELDS,
)

# Utility functions (were global functions or duplicated in classes)
from ..utils.helpers import log_message, clean_address_string, get_canonical_name_key, extract_address_string, format_address_label

# UI components (were classes in original file)
from ..ui.tooltip import Tooltip

from .base import InvestigationModuleBase


class DirectorSearch(InvestigationModuleBase):
    def __init__(self, parent_app, api_key, back_callback, ch_token_bucket):
        super().__init__(parent_app, back_callback, api_key, help_key="director")
        self.ch_token_bucket = ch_token_bucket
        # --- Add a new instance variable for grant results ---
        self.grants_results = []
        # --- Track explicit row selection for selective export ---
        self.explicit_selection_made = False

        input_frame = ttk.LabelFrame(
            self.content_frame, text="Director Search", padding=10
        )
        input_frame.pack(fill=tk.X, pady=5, padx=10)
        input_frame.grid_columnconfigure(1, weight=1)

        self.full_name_var, self.year_var, self.month_var = (
            tk.StringVar(),
            tk.StringVar(),
            tk.StringVar(),
        )

        ttk.Label(input_frame, text="Full Name:").grid(
            row=0, column=0, sticky="w", padx=5, pady=5
        )
        self.name_entry = ttk.Entry(input_frame, textvariable=self.full_name_var)
        self.name_entry.grid(row=0, column=1, columnspan=3, sticky="ew", padx=5)

        ttk.Label(input_frame, text="Year of Birth (Optional):").grid(
            row=1, column=0, sticky="w", padx=5, pady=5
        )
        vcmd = (self.register(self.validate_year), "%P")
        self.year_entry = ttk.Entry(
            input_frame,
            textvariable=self.year_var,
            validate="key",
            validatecommand=vcmd,
            width=10,
        )
        self.year_entry.grid(row=1, column=1, sticky="w", padx=5)

        ttk.Label(input_frame, text="Month:").grid(
            row=1, column=2, sticky="w", padx=(10, 5), pady=5
        )
        months = [
            "Any",
            "01 - January",
            "02 - February",
            "03 - March",
            "04 - April",
            "05 - May",
            "06 - June",
            "07 - July",
            "08 - August",
            "09 - September",
            "10 - October",
            "11 - November",
            "12 - December",
        ]
        self.month_combo = ttk.Combobox(
            input_frame,
            textvariable=self.month_var,
            values=months,
            state="readonly",
            width=15,
        )
        self.month_combo.set("Any")
        self.month_combo.grid(row=1, column=3, sticky="w", padx=5)

        self.search_buttons_frame = ttk.Frame(input_frame)
        self.search_buttons_frame.grid(row=0, column=4, rowspan=2, sticky="ns", padx=5)

        self.search_btn = ttk.Button(
            self.search_buttons_frame,
            text="Search",
            command=lambda: self.start_search(),
        )
        self.search_btn.pack(ipady=10)
        self.cancel_btn = ttk.Button(
            self.search_buttons_frame, text="Cancel", command=self.cancel_search
        )

        self.name_entry.bind("<Return>", self.start_search)
        self.year_entry.bind("<Return>", self.start_search)
        self.month_combo.bind("<Return>", self.start_search)

        results_frame = ttk.LabelFrame(self.content_frame, text="Results", padding=10)
        results_frame.pack(fill=tk.BOTH, expand=True, pady=10, padx=10)
        self.tree = self._create_treeview(results_frame)

        # --- Selection controls frame ---
        selection_frame = ttk.Frame(self.content_frame)
        selection_frame.pack(fill=tk.X, pady=(0, 5), padx=10)

        self.selection_label_var = tk.StringVar(value="Selected: All (0 rows)")
        ttk.Label(selection_frame, textvariable=self.selection_label_var).pack(side=tk.LEFT)

        ttk.Button(
            selection_frame, text="Clear Selection", command=self._clear_selection
        ).pack(side=tk.RIGHT, padx=(5, 0))
        ttk.Button(
            selection_frame, text="Select All", command=self._select_all
        ).pack(side=tk.RIGHT)

        # Bind selection event
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_selection_changed)

        status_export_frame = ttk.Frame(self.content_frame)
        status_export_frame.pack(fill=tk.X, pady=5, side="bottom")
        self.status_var = tk.StringVar(value="Ready.")
        ttk.Label(status_export_frame, textvariable=self.status_var).pack(
            side=tk.LEFT, fill=tk.X, expand=True
        )

        # --- Button Frame for Exports ---
        button_export_frame = ttk.Frame(status_export_frame)
        button_export_frame.pack(side=tk.RIGHT)

        self.export_btn = ttk.Button(
            button_export_frame,
            text="Export Directorships",
            state="disabled",
            command=self.export_csv,
        )
        self.export_btn.pack(side=tk.RIGHT, padx=5)
        Tooltip(
            self.export_btn,
            "Export selected directorship rows to CSV. If no rows are selected, all rows are exported.",
        )

        self.grants_btn = ttk.Button(
            button_export_frame,
            text="Obtain Grants Data & Export",
            state="disabled",
            command=self.start_grants_investigation,
        )
        self.grants_btn.pack(side=tk.RIGHT, padx=(5, 0))
        Tooltip(
            self.grants_btn,
            "For selected companies, fetch all associated grant data from the 360Giving API. If no rows are selected, all companies are searched.",
        )

        # --- MODIFIED: Graph and Export Buttons ---
        self.graph_btn = ttk.Button(
            button_export_frame,
            text="Generate Visual Graph",
            state="disabled",
            command=self.start_visual_graph_generation,
        )
        self.graph_btn.pack(side=tk.RIGHT, padx=(5, 0))
        Tooltip(
            self.graph_btn,
            "Generate an interactive network graph for selected companies, including their directors, PSCs, and addresses. If no rows are selected, all companies are included.",
        )

        self.export_graph_data_btn = ttk.Button(
            button_export_frame,
            text="Export Graph Data (CSV)",
            state="disabled",
            command=self.start_graph_data_export,
        )
        self.export_graph_data_btn.pack(side=tk.RIGHT, padx=(5, 0))
        Tooltip(
            self.export_graph_data_btn,
            "Export the network graph data (companies, people, addresses) for selected rows to CSV. If no rows are selected, all data is exported.",
        )

    def cancel_search(self):
        """Called when the user clicks the Cancel button."""
        if messagebox.askyesno(
            "Cancel", "Are you sure you want to cancel the current operation?"
        ):
            self.cancel_flag.set()

    def _finish_search(self):
        """Safely resets the UI after a search completes or is cancelled."""
        try:
            self.cancel_btn.pack_forget()
            self.search_btn.pack(ipady=10)
        except tk.TclError:
            pass

    def _disable_all_buttons(self):
        """Helper to disable all action buttons during processing."""
        self.search_btn.config(state="disabled")
        self.export_btn.config(state="disabled")
        self.grants_btn.config(state="disabled")
        self.graph_btn.config(state="disabled")
        self.export_graph_data_btn.config(state="disabled")

    def _restore_button_states(self):
        """Helper to re-enable buttons after a process finishes."""
        self.search_btn.config(state="normal")
        if self.results_data:
            self.export_btn.config(state="normal")
            self.grants_btn.config(state="normal")
            self.graph_btn.config(state="normal")
            self.export_graph_data_btn.config(state="normal")

    def validate_year(self, P):
        return (str.isdigit(P) or P == "") and len(P) <= 4

    # --- Selection handling methods ---

    def _on_tree_selection_changed(self, event=None):
        """Called when the treeview selection changes."""
        self.explicit_selection_made = True
        self._update_selection_label()

    def _select_all(self):
        """Select all rows in the treeview."""
        all_items = self.tree.get_children()
        if all_items:
            self.tree.selection_set(all_items)
        self.explicit_selection_made = False
        self._update_selection_label()

    def _clear_selection(self):
        """Clear all selections in the treeview."""
        self.tree.selection_remove(self.tree.selection())
        self.explicit_selection_made = True
        self._update_selection_label()

    def _update_selection_label(self):
        """Update the selection status label."""
        total_rows = len(self.tree.get_children())
        selected_items = self.tree.selection()
        selected_count = len(selected_items)

        if not self.explicit_selection_made or selected_count == total_rows:
            self.selection_label_var.set(f"Selected: All ({total_rows} rows)")
        elif selected_count == 0:
            self.selection_label_var.set(f"Selected: None (0 of {total_rows} rows)")
        else:
            self.selection_label_var.set(f"Selected: {selected_count} of {total_rows} rows")

    def _get_selected_results(self):
        """
        Returns the results data based on current selection.
        If no explicit selection made, returns all results.
        Otherwise returns only the selected rows.
        """
        if not self.explicit_selection_made:
            return self.results_data

        selected_items = self.tree.selection()
        if not selected_items:
            # User explicitly cleared selection - return empty to trigger warning
            return []

        # Build list of selected results by matching treeview values
        selected_results = []
        for item_id in selected_items:
            values = self.tree.item(item_id, "values")
            # Find matching record in results_data
            for record in self.results_data:
                record_values = tuple(str(v) for v in record.values())
                if record_values == values:
                    selected_results.append(record)
                    break

        return selected_results

    def _create_treeview(self, parent):
        cols = (
            "officer_name",
            "date_of_birth",
            "company_name",
            "company_number",
            "company_status",
            "role",
            "address",
        )
        tree = ttk.Treeview(parent, columns=cols, show="headings", selectmode="extended")
        for col in cols:
            tree.heading(col, text=col.replace("_", " ").title())
            tree.column(col, width=150)
        yscroll = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=tree.yview)
        xscroll = ttk.Scrollbar(parent, orient=tk.HORIZONTAL, command=tree.xview)
        tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)
        parent.grid_rowconfigure(0, weight=1)
        parent.grid_columnconfigure(0, weight=1)
        tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")
        return tree

    def start_search(self, event=None):
        if not self.full_name_var.get():
            messagebox.showerror("Input Error", "Full Name is required.")
            return

        self.cancel_flag.clear()
        self.results_data = []
        # Reset selection state for new search
        self.explicit_selection_made = False

        self.search_btn.pack_forget()
        self.cancel_btn.pack(ipady=10)
        self._disable_all_buttons()

        for item in self.tree.get_children():
            self.tree.delete(item)
        self._update_selection_label()
        threading.Thread(target=self._run_search, daemon=True).start()

    def _run_search(self):

        try:
            officers, error = self._find_matching_officers(
                self.full_name_var.get(), self.year_var.get(), self.month_var.get()
            )
            if error:
                raise ValueError(error)

            # --- NEW: Add a warning for large result sets ---
            if len(officers) > 200: # Set a reasonable threshold
                confirm = messagebox.askyesno(
                    "Large Search Warning",
                    f"This search returned {len(officers)} potential officers.\n\n"
                    "Fetching all appointments will be slow and may take several minutes.\n\n"
                    "Do you wish to continue?",
                    icon="warning"
                )
                if not confirm:
                    # User chose to cancel, so we abort the operation gracefully.
                    self.after(100, self._finish_search)
                    self.app.after(0, lambda: self.status_var.set("Search aborted by user. Please refine your search criteria."))
                    return # Stop the function here

            MAX_WORKERS = 2  # Respect API rate limits
            self.app.after(
                0,
                lambda: self.status_var.set(
                    f"Found {len(officers)} potential officers. Fetching appointments..."
                ),
            )

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(self._process_officer, officer): officer
                    for officer in officers
                }

                processed_count = 0
                for future in as_completed(futures):
                    if self.cancel_flag.is_set():
                        for f in futures:
                            f.cancel()
                        break

                    processed_list, error = future.result()
                    if error:
                        log_message(f"Error processing officer appointments: {error}")
                    if processed_list:
                        self.results_data.extend(processed_list)

                    processed_count += 1
                    self.app.after(
                        0,
                        lambda: self.status_var.set(
                            f"Fetched appointments for {processed_count}/{len(officers)} officers..."
                        ),
                    )

            self.after(100, self._populate_results)

        except Exception as e:
            messagebox.showerror("Error", str(e))
        finally:
            self.after(100, self._finish_search)

    def _populate_results(self):
        unique_records = {tuple(d.values()): d for d in self.results_data}.values()
        self.results_data = list(unique_records)
        for record in self.results_data:
            self.tree.insert("", tk.END, values=list(record.values()))

        # Update selection label after populating results
        self._update_selection_label()

        if self.cancel_flag.is_set():
            self.app.after(0, lambda: self.status_var.set("Search cancelled."))
        else:
            self.app.after(
                0,
                lambda: self.status_var.set(
                    f"Search complete. Found {len(self.results_data)} unique appointments."
                ),
            )

        self._restore_button_states()

    def start_grants_investigation(self):
        """Kicks off the grant fetching process in a new thread."""
        if not self.results_data:
            messagebox.showinfo("No Data", "Please run a director search first.")
            return

        selected_results = self._get_selected_results()
        if not selected_results:
            messagebox.showinfo(
                "No Selection",
                "No rows selected for grants search. Please select rows or use 'Select All'."
            )
            return

        self._disable_all_buttons()
        self.cancel_flag.clear()
        self.grants_results = []
        # Store selected results for the thread to use
        self._selected_for_processing = selected_results

        threading.Thread(target=self._run_grants_thread, daemon=True).start()

    def _run_grants_thread(self):

        unique_companies = {
            d["company_number"]: d for d in self._selected_for_processing if d.get("company_number")
        }.values()

        if not unique_companies:
            self.app.after(
                0,
                lambda: self.status_var.set(
                    "No companies with numbers to search for grants."
                ),
            )
            self.after(100, self._finish_grants_investigation)
            return

        self.app.after(
            0,
            lambda: self.status_var.set(
                f"Fetching grants for {len(unique_companies)} companies..."
            ),
        )

        with ThreadPoolExecutor(
            max_workers=2
        ) as executor:  # Respect GrantNav rate limits
            futures = {
                executor.submit(
                    self._process_company_for_grants, company_row
                ): company_row
                for company_row in unique_companies
            }

            for i, future in enumerate(as_completed(futures)):
                if self.cancel_flag.is_set():
                    break
                company_row = futures[future]
                cnum = company_row.get("company_number")
                self.app.after(
                    0,
                    lambda: self.status_var.set(
                        f"Processing {cnum} ({i + 1}/{len(unique_companies)})..."
                    ),
                )
                try:
                    new_rows = future.result()
                    if new_rows:
                        self.grants_results.extend(new_rows)
                except Exception as e:
                    log_message(f"Error processing grants for {cnum}: {e}")

        self.after(100, self._finish_grants_investigation)

    def _process_company_for_grants(self, company_row):
        """For a single company row, find all grants and create combined result rows."""
        if self.cancel_flag.is_set():
            return []

        company_number = company_row.get("company_number")
        if not company_number:
            return []

        org_id = f"GB-COH-{company_number}"
        grants = self._fetch_all_grants(org_id)

        new_rows = []
        if grants:
            for grant in grants:
                new_row = company_row.copy()
                self._add_selected_grant_data(new_row, grant)
                new_rows.append(new_row)
        return new_rows

    def _finish_grants_investigation(self):
        """Finalizes the grant search, triggers export, and resets the UI."""
        self._restore_button_states()

        if self.cancel_flag.is_set():
            self.app.after(0, lambda: self.status_var.set("Grant search cancelled."))
            return

        if not self.grants_results:
            self.app.after(
                0,
                lambda: self.status_var.set("Grant search complete. No grants found."),
            )
            messagebox.showinfo(
                "No Grants Found",
                "The search finished, but no grant data was found for the listed companies.",
            )
        else:
            self.app.after(
                0,
                lambda: self.status_var.set(
                    f"Grant search complete. Found {len(self.grants_results)} grants."
                ),
            )
            self.export_grants_csv()

    def export_grants_csv(self):
        """Exports the combined director and grant data to a CSV file."""
        if not self.grants_results:
            return

        original_headers = [
            "officer_name",
            "date_of_birth",
            "company_name",
            "company_number",
            "company_status",
            "role",
            "address",
        ]
        grant_headers = list(GRANT_DATA_FIELDS.values())
        all_headers = original_headers + grant_headers

        original_results = self.results_data
        self.results_data = self.grants_results
        self.generic_export_csv(all_headers)
        self.results_data = original_results

    def _fetch_all_grants(self, org_id):
        """Helper to fetch all grants from GrantNav API, handling pagination."""
        all_results = []
        url = f"{GRANTNAV_API_BASE_URL}/org/{org_id}/grants_received?limit=1000"
        while url:
            if self.cancel_flag.is_set():
                break
            data, error = grantnav_get_data(url)
            if error:
                log_message(f"GrantNav error for {org_id}: {error}")
                return {"error_reason": error}
                break
            if data and "results" in data:
                all_results.extend(item.get("data", {}) for item in data["results"])
                url = data.get("next")
            else:
                break
        return all_results

    def _add_selected_grant_data(self, row, grant_data):
        """Flattens nested grant data into the provided row."""
        for key, text in GRANT_DATA_FIELDS.items():
            row[text] = self.get_nested_value(grant_data, key)

    def _format_address_label(self, address_str: str, line_length: int = 25) -> str:
        """Wraps a long address string into multiple lines for graph readability."""
        import textwrap

        return textwrap.fill(address_str, width=line_length)

    # --- REFACTORED: Graphing Logic ---

    def start_visual_graph_generation(self):
        """Initiates the visual graph generation process."""
        self._start_graph_process(self._run_visual_graph_thread)

    def start_graph_data_export(self):
        """Initiates the graph data export process."""
        self._start_graph_process(self._run_export_graph_thread)

    def _start_graph_process(self, target_thread_function):
        """Generic starter for any graph-related process."""
        if not self.results_data:
            messagebox.showinfo(
                "No Data", "Please run a director search before generating graph data."
            )
            return

        selected_results = self._get_selected_results()
        if not selected_results:
            messagebox.showinfo(
                "No Selection",
                "No rows selected for graph generation. Please select rows or use 'Select All'."
            )
            return

        self._disable_all_buttons()
        self.cancel_flag.clear()
        # Store selected results for the thread to use
        self._selected_for_processing = selected_results

        self.app.after(
            0, lambda: self.status_var.set("Starting network data collection...")
        )
        threading.Thread(target=target_thread_function, daemon=True).start()

    def _run_visual_graph_thread(self):
        """Thread for building the graph object and then rendering it."""
        try:
            graph_object = self._build_network_graph_object()
            if graph_object is not None and not self.cancel_flag.is_set():
                self.after(100, self._generate_and_open_graph, graph_object)
        except Exception as e:
            log_message(f"Visual graph generation failed: {e}")
            messagebox.showerror(
                "Error", f"An error occurred during graph generation: {e}"
            )
        finally:
            self.after(200, self._finish_graph_generation)

    def _run_export_graph_thread(self):
        """Thread for building the graph object and then exporting it."""
        try:
            graph_object = self._build_network_graph_object()
            if graph_object is not None and not self.cancel_flag.is_set():
                self.after(100, self._export_graph_to_csv, graph_object)
        except Exception as e:
            log_message(f"Graph data export failed: {e}")
            messagebox.showerror(
                "Error", f"An error occurred during graph data export: {e}"
            )
        finally:
            self.after(200, self._finish_graph_generation)

    def _build_network_graph_object(self):
        """
        Central function to fetch all data and build the networkx.DiGraph object.
        This is now the single source of truth for graph data.
        Returns the graph object on success, or None on failure/cancellation.
        """

        G = nx.DiGraph()
        unique_company_numbers = list(
            {d["company_number"] for d in self._selected_for_processing if d.get("company_number")}
        )

        if not unique_company_numbers:
            self.app.after(
                0,
                lambda: self.status_var.set(
                    "No valid company numbers found in results."
                ),
            )
            return None

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_cnum = {
                executor.submit(self._fetch_company_network_data, cnum): cnum
                for cnum in unique_company_numbers
            }

            for i, future in enumerate(as_completed(future_to_cnum)):
                if self.cancel_flag.is_set():
                    return None  # Stop processing

                cnum = future_to_cnum[future]
                self.app.after(
                    0,
                    lambda: self.status_var.set(
                        f"Fetching network data for {cnum} ({i + 1}/{len(unique_company_numbers)})..."
                    ),
                )
                profile, officers, pscs = future.result()

                if not profile:
                    continue

                company_name = profile.get("company_name", cnum)
                is_active = profile.get("company_status", "active") == "active"
                company_status = profile.get("company_status")
                has_liquidated = profile.get("has_been_liquidated", False)
                G.add_node(
                    cnum,
                    label=company_name,
                    type="company",
                    active=is_active,
                    status=company_status,
                    liquidated=has_liquidated,
                )

                addr_data = profile.get("registered_office_address", {})
                raw_address_str = ", ".join(
                    filter(
                        None,
                        [
                            addr_data.get("address_line_1"),
                            addr_data.get("address_line_2"),
                            addr_data.get("locality"),
                            addr_data.get("region"),
                            addr_data.get("postal_code"),
                        ],
                    )
                )

                # --- APPLY THE CLEANING FUNCTION ---
                address_str = clean_address_string(raw_address_str)

                if address_str:
                    # Use the raw string for the visual label, but the clean string for the node ID
                    formatted_label = self._format_address_label(raw_address_str)
                    G.add_node(address_str, label=formatted_label, type="address")
                    G.add_edge(cnum, address_str, label="registered_at")

                if officers:
                    for officer in officers.get("items", []):
                        name = officer.get("name")
                        if not name:
                            continue
                        dob = officer.get("date_of_birth")
                        person_key = get_canonical_name_key(name, dob)
                        if not G.has_node(person_key):
                            G.add_node(person_key, label=name, type="person", dob=dob)
                        elif dob and not G.nodes[person_key].get("dob"):
                            G.nodes[person_key]["dob"] = dob
                        G.add_edge(
                            cnum,
                            person_key,
                            label=officer.get("officer_role", "officer"),
                        )

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

                if pscs:
                    for psc in pscs.get("items", []):
                        name = psc.get("name")
                        if not name:
                            continue
                        dob = psc.get("date_of_birth")
                        person_key = get_canonical_name_key(name, dob)
                        if not G.has_node(person_key):
                            G.add_node(person_key, label=name, type="person", dob=dob)
                        elif dob and not G.nodes[person_key].get("dob"):
                            G.nodes[person_key]["dob"] = dob
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


        return G

    def _fetch_company_network_data(self, company_number):
        """Worker function to fetch profile, officers, and PSCs for one company."""
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

    def _generate_and_open_graph(self, G):
        """Converts the networkx graph to a pyvis graph and opens it."""
        if G is None or G.number_of_nodes() == 0:
            self.app.after(
                0,
                lambda: self.status_var.set(
                    "Graph generation complete. No data to display."
                ),
            )
            return

        self.app.after(
            0, lambda: self.status_var.set("Rendering graph... Please wait.")
        )
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

        for node_id, attrs in G.nodes(data=True):
            node_type = attrs.get("type")
            color = "#D9E8B9"  # Default Green (Person)
            shape = "ellipse"
            font_options = {}

            safe_name = html.escape(attrs.get("label", ""))

            label_lines = [safe_name]
            if node_type == "person":
                dob_obj = attrs.get("dob")
                if dob_obj and "year" in dob_obj and "month" in dob_obj:
                    dob_str = f"DOB: {dob_obj['month']:02d}-{dob_obj['year']}"
                    label_lines.append(dob_str)

            final_label = "\n".join(label_lines)
            title = final_label

            if node_type == "company":
                color = "#B9D9EB" if attrs.get("active") else "#E0E0E0"
                shape = "box"
                if attrs.get("status") in [
                    "liquidation",
                    "administration",
                ] or attrs.get("liquidated"):
                    bolded_name = f"<b>{label_lines[0]}</b>"
                    remaining_lines = label_lines[1:]
                    final_label = bolded_name + (
                        "\n" + "\n".join(remaining_lines) if remaining_lines else ""
                    )
                    font_options = {"multi": True, "color": "red"}

            elif node_type == "address":
                color = "#FFB347"
                shape = "box"

            net.add_node(
                node_id,
                label=final_label,
                title=title,
                shape=shape,
                color=color,
                font=font_options,
            )

        for source, target, attrs in G.edges(data=True):
            net.add_edge(source, target, title=attrs.get("label", ""))

        try:
            filename = os.path.join(CONFIG_DIR, "director_network_graph.html")
            net.write_html(filename, notebook=False)
            self.app.after(
                0, lambda: self.status_var.set("Graph generated! Opening in browser...")
            )
            webbrowser.open(f"file://{os.path.realpath(filename)}")
        except Exception as e:
            log_message(f"Failed to save or open graph: {e}")
            messagebox.showerror(
                "Graph Error", f"Could not save or open the graph file: {e}"
            )
            self.app.after(
                0, lambda: self.status_var.set("Error generating graph file.")
            )

    # --- NEW: Rewritten CSV Export Function ---
    def _export_graph_to_csv(self, G):
        """
        Exports the graph's connections (edges) to a CSV file.
        This format is ideal for combining multiple exports later.
        """
        if G is None or G.number_of_edges() == 0:
            self.app.after(
                0,
                lambda: self.status_var.set(
                    "Export complete. No connections to export."
                ),
            )
            return

        filepath = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            title="Save Graph Edge List As",
        )
        if not filepath:
            self.app.after(0, lambda: self.status_var.set("Export cancelled by user."))
            return

        self.app.after(
            0, lambda: self.status_var.set("Exporting graph connections to CSV...")
        )
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

                # Iterate over edges to capture the connections
                for source_id, target_id, edge_attrs in G.edges(data=True):
                    source_attrs = G.nodes[source_id]
                    target_attrs = G.nodes[target_id]

                    row = [
                        source_id,
                        source_attrs.get("label", "").replace("\n", " "),
                        source_attrs.get("type", ""),
                        target_id,
                        target_attrs.get("label", "").replace("\n", " "),
                        target_attrs.get("type", ""),
                        edge_attrs.get("label", ""),
                    ]
                    writer.writerow(row)

            log_message(
                f"Successfully exported {G.number_of_edges()} graph connections to {os.path.basename(filepath)}."
            )
            messagebox.showinfo(
                "Export Successful",
                f"Successfully exported {G.number_of_edges()} connections to CSV.",
            )
            self.app.after(
                0, lambda: self.status_var.set("Graph data export complete.")
            )

        except IOError as e:
            log_message(f"Graph data export failed: {e}")
            messagebox.showerror("Export Error", f"Could not write to file: {e}")
            self.app.after(0, lambda: self.status_var.set("Error during CSV export."))

    def _finish_graph_generation(self):
        """Resets the UI after any graph process completes or is cancelled."""
        if self.cancel_flag.is_set():
            self.app.after(0, lambda: self.status_var.set("Operation cancelled."))
        else:
            # Don't overwrite success messages from export/generation
            if "..." not in self.status_var.get():
                self.app.after(0, lambda: self.status_var.set("Ready."))

        self._restore_button_states()

    def export_csv(self):
        """Export selected directorship rows to CSV."""
        selected_results = self._get_selected_results()

        if not selected_results:
            messagebox.showinfo(
                "No Selection",
                "No rows selected for export. Please select rows or use 'Select All'."
            )
            return

        headers = [
            "officer_name",
            "date_of_birth",
            "company_name",
            "company_number",
            "company_status",
            "role",
            "address",
        ]

        # Temporarily swap results_data for export
        original_results = self.results_data
        self.results_data = selected_results
        self.generic_export_csv(headers)
        self.results_data = original_results

    def _find_matching_officers(self, full_name, year_of_birth, month_of_birth_str):
        self.app.after(
            0,
            lambda: self.status_var.set(
                f"Searching for officers named '{full_name}'..."
            ),
        )
        all_results, start_index = [], 0

        search_limit = 999
        log_message(
            f"Starting officer search. Search limit capped at {search_limit} due to API constraints."
        )

        while True:
            if self.cancel_flag.is_set():
                break
            path = f"/search/officers?q={full_name}&items_per_page=100&start_index={start_index}"
            data, error = ch_get_data(self.api_key, self.ch_token_bucket, path)

            if error:
                if "Error 50" in error:
                    log_message(
                        f"Server-side API error during paged director search: {error}. Continuing with {len(all_results)} results found so far."
                    )
                    break
                else:
                    return [], error

            if not data or not data.get("items"):
                break

            items = data.get("items", [])
            all_results.extend(items)
            total_results = data.get("total_results", 0)
            start_index += len(items)

            if start_index >= total_results or start_index >= search_limit:
                break

        self.app.after(
            0,
            lambda: self.status_var.set(
                f"API returned {len(all_results)} total results. Filtering..."
            ),
        )

        name_matches, search_tokens = [], set(full_name.lower().split())
        for officer in all_results:
            # --- FIX: Improved name cleaning to handle hyphens ---
            raw_title = officer.get("title", "").lower()
            # 1. Remove titles and punctuation
            cleaned_title = re.sub(
                r"\b(mr|mrs|ms|miss|dr|prof)\b|[.,]", "", raw_title
            ).strip()
            # 2. Replace hyphens with spaces
            cleaned_title = cleaned_title.replace("-", " ")
            # --- END FIX ---

            officer_tokens = set(cleaned_title.split())

            if search_tokens.issubset(officer_tokens):
                name_matches.append(officer)

        if not year_of_birth and month_of_birth_str == "Any":
            return name_matches, None

        selected_month_num = (
            int(month_of_birth_str.split(" ")[0])
            if month_of_birth_str != "Any"
            else None
        )

        filtered_results = []
        for officer in name_matches:
            dob = officer.get("date_of_birth")
            if not dob:
                continue

            year_match = not year_of_birth or str(dob.get("year")) == str(year_of_birth)
            month_match = (
                not selected_month_num or dob.get("month") == selected_month_num
            )

            if year_match and month_match:
                filtered_results.append(officer)

        return filtered_results, None

    def _process_officer(self, officer):
        officer_name, appointments_link = officer.get("title", "N/A"), officer.get(
            "links", {}
        ).get("self", "")
        dob_str = "N/A"
        dob = officer.get("date_of_birth")
        if dob and dob.get("year") and dob.get("month"):
            dob_str = f"{dob.get('month'):02d}-{dob.get('year')}"
        company_list, error = [], None
        if "/officers/" in appointments_link:
            path = appointments_link.replace(API_BASE_URL, "")
            if not path.endswith("/appointments"):
                path += "/appointments"

            data, error = ch_get_data(self.api_key, self.ch_token_bucket, path)
            if data:
                for app in data.get("items", []):
                    company_list.append(
                        {
                            "officer_name": officer_name,
                            "date_of_birth": dob_str,
                            "company_name": app.get("appointed_to", {}).get(
                                "company_name"
                            ),
                            "company_number": app.get("appointed_to", {}).get(
                                "company_number"
                            ),
                            "company_status": app.get("appointed_to", {}).get(
                                "company_status"
                            ),
                            "role": app.get("officer_role", "Director")
                            .replace("-", " ")
                            .title(),
                            "address": ", ".join(
                                filter(None, (app.get("address") or {}).values())
                            ),
                        }
                    )
        return company_list, error
