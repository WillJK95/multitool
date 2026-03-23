# modules/ubo_tracer.py
"""UBO module"""

import csv
import html
import os
import re
import threading
import time
import webbrowser
import tkinter as tk
from datetime import datetime
from tkinter import ttk, filedialog, messagebox
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Third-Party ---
import networkx as nx
from pyvis.network import Network

# --- From Our Package ---
# API functions (were global functions in original file)
from ..api.companies_house import ch_get_data

# Constants (were at top of original file)
from ..constants import (
    CONFIG_DIR,
)

# Utility functions (were global functions or duplicated in classes)
from ..utils.helpers import log_message, clean_address_string, get_canonical_name_key, extract_address_string, format_address_label, format_error_summary, format_eta

# UI components (were classes in original file)
from ..ui.tooltip import Tooltip

from .base import InvestigationModuleBase

class UltimateBeneficialOwnershipTracer(InvestigationModuleBase):
    def __init__(self, parent_app, api_key, back_callback, ch_token_bucket):
        super().__init__(parent_app, back_callback, api_key, help_key="ubo")
        self.ch_token_bucket = ch_token_bucket
        # --- UI Setup ---
        upload_frame = ttk.LabelFrame(
            self.content_frame, text="Step 1: Upload File", padding=10
        )
        upload_frame.pack(fill=tk.X, pady=5, padx=10)
        ttk.Button(
            upload_frame, text="Upload Input File (.csv)", command=self.load_file
        ).pack(pady=5)
        self.file_status_label = ttk.Label(upload_frame, text="No file loaded.")
        self.file_status_label.pack(pady=5)

        self.column_selection_frame = ttk.LabelFrame(
            self.content_frame, text="Step 2: Select Columns", padding=10
        )
        self.column_selection_frame.pack(fill=tk.X, pady=5, padx=10)

        run_frame = ttk.LabelFrame(
            self.content_frame, text="Step 3: Run & Export", padding=10
        )
        run_frame.pack(fill=tk.BOTH, expand=True, pady=5, padx=10)

        # --- Snapshot Date Option ---
        snapshot_frame = ttk.Frame(run_frame)
        snapshot_frame.pack(pady=(5, 10), fill=tk.X)
        ttk.Label(snapshot_frame, text="Optional Snapshot Date (DD/MM/YYYY):").pack(
            side=tk.LEFT, padx=(0, 5)
        )
        self.snapshot_date_var = tk.StringVar()
        date_entry = ttk.Entry(
            snapshot_frame, textvariable=self.snapshot_date_var, width=15
        )
        date_entry.pack(side=tk.LEFT)
        Tooltip(
            date_entry,
            "Enter a date to see the ownership structure at that point in time.\n\n"
            "If a date is entered, only PSCs active on that date will be shown.\n"
            "If left blank, all current and historic PSCs will be returned.",
        )

        # --- REMOVED: Graph Generation Checkbox ---

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

        # --- MODIFIED: Export and Graph Buttons ---
        export_buttons_frame = ttk.Frame(run_frame)
        export_buttons_frame.pack(pady=5)
        self.export_btn = ttk.Button(
            export_buttons_frame,
            text="Export PSC List",
            state="disabled",
            command=self.export_csv,
        )
        self.export_btn.pack(side=tk.LEFT, padx=5)
        Tooltip(
            self.export_btn,
            "Export a flat list of all PSCs found in the ownership chain.",
        )

        self.generate_graph_btn = ttk.Button(
            export_buttons_frame,
            text="Generate Visual Graph",
            state="disabled",
            command=self.start_visual_graph_generation,
        )
        self.generate_graph_btn.pack(side=tk.LEFT, padx=5)
        Tooltip(
            self.generate_graph_btn,
            "Generate a visual graph of the PSC ownership chain.",
        )

        self.export_graph_data_btn = ttk.Button(
            export_buttons_frame,
            text="Export Graph Data (CSV)",
            state="disabled",
            command=self.start_graph_data_export,
        )
        self.export_graph_data_btn.pack(side=tk.LEFT, padx=5)
        Tooltip(
            self.export_graph_data_btn,
            "Export the network connections (edge list) to a CSV file for combined analysis.",
        )

        self.progress_bar = ttk.Progressbar(
            run_frame, orient="horizontal", length=300, mode="determinate"
        )
        self.progress_bar.pack(pady=10)
        self.status_entity_var = tk.StringVar(value="")
        ttk.Label(run_frame, textvariable=self.status_entity_var).pack(anchor=tk.W)
        self.status_var = tk.StringVar(value="Ready.")
        ttk.Label(run_frame, textvariable=self.status_var).pack(anchor=tk.W)

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
        for widget in self.column_selection_frame.winfo_children():
            widget.destroy()

        self.number_col_var = tk.StringVar()
        self.name_col_var = tk.StringVar()

        # Container
        container = ttk.Frame(self.column_selection_frame)
        container.pack(fill="x", expand=True, pady=5, anchor="n")

        # Company Number (Required)
        number_frame = ttk.LabelFrame(
            container, text="Select Company Number Column (Required)", padding=5
        )
        number_frame.pack(side=tk.LEFT, fill="x", expand=True, padx=5)
        
        headers = getattr(self, "original_headers", [])
        
        num_combo = ttk.Combobox(
            number_frame,
            textvariable=self.number_col_var,
            values=headers,
            state="readonly"
        )
        num_combo.pack(fill="x", pady=5)
        if headers:
            num_combo.current(0)

        # Company Name (Optional)
        name_frame = ttk.LabelFrame(
            container, text="Select Company Name Column (Optional)", padding=5
        )
        name_frame.pack(side=tk.LEFT, fill="x", expand=True, padx=5)
        
        name_options = ["\u2014 Not Selected \u2014"] + headers
        name_combo = ttk.Combobox(
            name_frame,
            textvariable=self.name_col_var,
            values=name_options,
            state="readonly"
        )
        name_combo.pack(fill="x", pady=5)
        name_combo.set("\u2014 Not Selected \u2014")

        # Confirm Button
        ttk.Button(
            self.column_selection_frame,
            text="Confirm Columns",
            command=self._confirm_columns,
        ).pack(side=tk.BOTTOM, pady=10)
        
        # Force geometry calculation then update scroll region
        self.app.after(50, self._force_scroll_update)
    
    def _force_scroll_update(self):
        """Force the scrollable frame to recalculate its geometry."""
        # Force full geometry calculation
        self.scroller.update_idletasks()
        # Update scroll region
        self.scroller.canvas.configure(scrollregion=self.scroller.canvas.bbox("all"))
        # Get actual canvas width and set frame to match
        canvas_width = self.scroller.canvas.winfo_width()
        if canvas_width > 1:
            self.scroller.canvas.itemconfig(self.scroller.frame_id, width=canvas_width)
        # Recalculate required height and update
        required_height = self.scroller.scrollable_frame.winfo_reqheight()
        visible_height = self.scroller.canvas.winfo_height()
        self.scroller.canvas.itemconfig(self.scroller.frame_id, height=max(required_height, visible_height))

    def _confirm_columns(self):
        self.number_col = self.number_col_var.get()
        self.name_col = self.name_col_var.get()
        if self.name_col == "\u2014 Not Selected \u2014":
            self.name_col = None
        if not self.number_col:
            messagebox.showerror(
                "Selection Error", "You must select a column for the company number."
            )
            return
        messagebox.showinfo("Columns Confirmed", "Column selection confirmed.")
        self.run_btn.config(state="normal")

    def start_investigation(self):
        self.cancel_flag.clear()
        self.run_btn.pack_forget()
        self.cancel_btn.pack(side=tk.LEFT, padx=5)
        self.export_btn.config(state="disabled")
        self.export_graph_data_btn.config(state="disabled")
        self.generate_graph_btn.config(state="disabled")  # Disable new button
        self.progress_bar["value"] = 0
        self.results_data = []
        threading.Thread(target=self._run_investigation_thread, daemon=True).start()

    def cancel_investigation(self):
        if messagebox.askyesno(
            "Cancel", "Are you sure you want to cancel the investigation?"
        ):
            self.cancel_flag.set()

    def _run_investigation_thread(self):

        root_companies, name_map = [], {}
        for row in self.original_data:
            cnum = row.get(self.number_col, "").strip()
            if cnum:
                cnum = (
                    cnum.zfill(8) if cnum.isdigit() and len(cnum) < 8 else cnum.upper()
                )
                root_companies.append(cnum)
                name_map[cnum] = (
                    row.get(self.name_col, "").strip() if self.name_col else ""
                )

        if len(root_companies) > 20:
            self.safe_ui_call(
                messagebox.showwarning,
                "Large Input File",
                f"You have loaded {len(root_companies)} root companies. The investigation and graph may take a long time to process.",
            )

        snapshot_date_str = self.snapshot_date_var.get()
        snapshot_date = None
        if snapshot_date_str:
            try:
                snapshot_date = datetime.strptime(snapshot_date_str, "%d/%m/%Y")
            except ValueError:
                self.safe_ui_call(
                    messagebox.showerror,
                    "Invalid Date",
                    "The date format must be DD/MM/YYYY. Investigation cancelled.",
                )
                self.after(100, self._finish_investigation)
                return

        total = len(root_companies)
        self.safe_ui_call(self.progress_bar.config, maximum=total)
        self.app.after(
            0,
            lambda: self.status_var.set(
                f"Starting UBO investigation for {total} root companies."
            ),
        )

        start_time = time.monotonic()
        found_count = 0
        error_count = 0
        self._ratelimit_ticking = False
        failed_companies = []

        # Rate-limit ticker: runs on main thread to count down while workers block
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

        # Watchdog: starts the ticker even when all worker threads are blocked in
        # consume() and the as_completed loop hasn't yielded (first stop-loss in burst).
        search_active = [True]

        def _watchdog():
            if not search_active[0] or self.cancel_flag.is_set():
                return
            if self.ch_token_bucket.is_paused and not self._ratelimit_ticking:
                _start_ratelimit_ticker()
            self._tracked_after(500, _watchdog)

        self._tracked_after(500, _watchdog)

        for i, root_cnum in enumerate(root_companies):
            if self.cancel_flag.is_set():
                break
            current_root_name = name_map.get(root_cnum, root_cnum)
            self.app.after(0, lambda v=i + 1: self.progress_bar.config(value=v))

            if self.ch_token_bucket.is_paused:
                self.safe_ui_call(_start_ratelimit_ticker)
            elif not self._ratelimit_ticking:
                elapsed = time.monotonic() - start_time
                remaining = total - i
                rate_wait = self.ch_token_bucket.estimate_wait_seconds(remaining * 5)
                eta = format_eta(elapsed, i, total, rate_limit_wait=rate_wait)
                display_name = current_root_name or root_cnum
                entity = f"Processing: {display_name} ({i + 1} of {total})"
                stats = f"ETA: {eta} | Found: {found_count} UBOs | Errors: {error_count}"
                self.app.after(0, lambda e=entity: self.status_entity_var.set(e))
                self.app.after(0, lambda s=stats: self.status_var.set(s))

            # --- MODIFICATION: Track if PSCs are found for this root ---
            pscs_found_for_this_root = False

            level_num, companies_this_level, seen_companies = (
                1,
                [root_cnum],
                {root_cnum},
            )

            while (
                companies_this_level
                and not self.cancel_flag.is_set()
                and level_num <= 20
            ):
                companies_next_level = []
                with ThreadPoolExecutor(max_workers=self.app.ch_max_workers) as executor:
                    tasks_to_run = [
                        (cnum, level_num, root_cnum, current_root_name, snapshot_date)
                        for cnum in companies_this_level
                    ]
                    futures = {
                        executor.submit(self._process_ubo_company, task): task
                        for task in tasks_to_run
                    }
                    for future in as_completed(futures):
                        if self.cancel_flag.is_set():
                            break
                        task = futures[future]
                        generated_rows, next_level_cnums, err = future.result()
                        if not generated_rows and not next_level_cnums:
                            failed_companies.append((task[0], err))
                            error_count += 1
                        if generated_rows:
                            self.results_data.extend(generated_rows)
                            found_count += len(generated_rows)
                            pscs_found_for_this_root = True  # Mark as found
                        if next_level_cnums:
                            companies_next_level.extend(next_level_cnums)

                unique_next_level = {
                    cnum for cnum in companies_next_level if cnum not in seen_companies
                }
                seen_companies.update(unique_next_level)
                companies_this_level = list(unique_next_level)
                level_num += 1

            # --- NEW: Add placeholder if no PSCs were found for this root company ---
            if not pscs_found_for_this_root and not self.cancel_flag.is_set():
                placeholder_row = {
                    "root_company": root_cnum,
                    "root_company_name": current_root_name,
                    "level": 0,
                    "parent_company_number": root_cnum,  # Self-reference for graph building
                }
                self.results_data.append(placeholder_row)

        search_active[0] = False
        self.safe_ui_call(self.status_entity_var.set, "")
        if not self.cancel_flag.is_set():
            if failed_companies:
                # Deduplicate by company number, keeping first error
                seen = {}
                for cnum, err in failed_companies:
                    if cnum not in seen:
                        seen[cnum] = err
                unique_failures = list(seen.items())
                warning = format_error_summary(unique_failures, "company")
                msg = f"Investigation complete! {warning}"
            else:
                msg = "Investigation complete!"
            self.app.after(0, lambda m=msg: self.status_var.set(m))
        else:
            self.app.after(0, lambda: self.status_var.set("Investigation cancelled."))

        self.after(100, self._finish_investigation)

    def _process_ubo_company(self, company_tuple):
        company_num, level, root_cnum, root_name, snapshot_date = company_tuple
        if self.cancel_flag.is_set():
            return [], [], None

        generated_rows, next_level_companies = [], []
        pscs_list, error = ch_get_data(
            self.api_key,
            self.ch_token_bucket,
            f"/company/{company_num}/persons-with-significant-control?items_per_page=100",
            is_psc=True,
        )
        if error or not pscs_list:
            log_message(
                f"UBO Chain Error for {company_num}: {error or 'No PSC list found'}"
            )
            return [], [], error or "No PSC data"

        for p_summary in pscs_list.get("items", []):
            if self.cancel_flag.is_set():
                break
            psc_self_link = p_summary.get("links", {}).get("self")
            if not psc_self_link:
                continue

            p, err = ch_get_data(
                self.api_key, self.ch_token_bucket, psc_self_link, is_psc=True
            )
            if err or not p:
                log_message(
                    f"Could not fetch full PSC details from {psc_self_link}: {err}"
                )
                continue

            notified_on_str = p.get("notified_on", "").strip()
            ceased_on_str = p.get("ceased_on", "").strip()
            if snapshot_date:
                try:
                    notified_date = datetime.strptime(notified_on_str, "%Y-%m-%d")
                    if notified_date > snapshot_date:
                        continue
                    if ceased_on_str:
                        ceased_date = datetime.strptime(ceased_on_str, "%Y-%m-%d")
                        if ceased_date <= snapshot_date:
                            continue
                except (ValueError, TypeError):
                    continue

            name = p.get("name", "").strip()
            kind = p.get("kind", "")
            is_corporate = "corporate" in kind
            unique_id = ""

            if is_corporate:
                raw_cnum = (p.get("identification", {}) or {}).get(
                    "registration_number", ""
                )
                unique_id = (
                    raw_cnum.strip().zfill(8)
                    if raw_cnum
                    and raw_cnum.strip().isdigit()
                    and len(raw_cnum.strip()) < 8
                    else raw_cnum.strip()
                ).upper()
            else:
                dob = p.get("date_of_birth")
                unique_id = get_canonical_name_key(name, dob)

            psc_cnum = unique_id if is_corporate else ""
            country = p.get("country_of_residence", "") or (
                p.get("identification", {}) or {}
            ).get("country_of_residence", "")
            shares = " | ".join(p.get("natures_of_control", []))

            acct_type = ""
            if is_corporate and psc_cnum:
                cp, _ = ch_get_data(
                    self.api_key,
                    self.ch_token_bucket,
                    f"/company/{psc_cnum}",
                    is_psc=True,
                )
                if cp:
                    acct_type = (
                        cp.get("accounts", {}).get("last_accounts", {}).get("type", "")
                    )
                next_level_companies.append(psc_cnum)

            row = {
                "root_company": root_cnum,
                "root_company_name": root_name,
                "level": level,
                "parent_company_number": company_num,
                "psc_name": name,
                "psc_company_number": psc_cnum,
                "psc_kind": kind,
                "psc_unique_id": unique_id,
                "country": country,
                "shareholding": shares,
                "accounts_type": acct_type,
                "notified_on": notified_on_str,
                "ceased_on": ceased_on_str,
            }
            generated_rows.append(row)

        return generated_rows, next_level_companies, None

    # --- NEW AND REFACTORED GRAPHING/EXPORT LOGIC ---

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
                "No Data", "Please run an investigation before generating graph data."
            )
            return

        # Disable all buttons to prevent concurrent operations
        self.run_btn.config(state="disabled")
        self.export_btn.config(state="disabled")
        self.export_graph_data_btn.config(state="disabled")
        self.generate_graph_btn.config(state="disabled")
        self.cancel_flag.clear()

        self.app.after(0, lambda: self.status_var.set("Starting graph process..."))
        threading.Thread(target=target_thread_function, daemon=True).start()

    def _run_visual_graph_thread(self):
        """
        Builds the graph object from the flat results list and passes it to the
        rendering function. This thread does not do any new API calls.
        """
        try:
            # This is a quick operation, so we don't need a complex progress bar
            self.app.after(
                0, lambda: self.status_var.set("Analyzing network structure...")
            )
            self.after(10, self._generate_and_open_graph)  # Call the final renderer
        except Exception as e:
            log_message(f"UBO visual graph generation failed: {e}")
            self.safe_ui_call(
                messagebox.showerror,
                "Error", f"An error occurred during graph generation: {e}"
            )
        finally:
            self.after(200, self._finish_graph_process)

    def _run_export_graph_thread(self):
        """
        Thread for building the full, detailed graph object (with extra API calls)
        and then exporting it to CSV.
        """
        try:
            # Build the complete, detailed graph object
            full_graph_object = self._build_ubo_network_graph_object()

            # Pass the full graph directly to the export function
            if full_graph_object is not None and not self.cancel_flag.is_set():
                self.after(100, self._export_graph_to_csv, full_graph_object)
        except Exception as e:
            log_message(f"UBO graph data export failed: {e}")
            self.safe_ui_call(
                messagebox.showerror,
                "Error", f"An error occurred during graph data export: {e}"
            )
        finally:
            self.after(200, self._finish_graph_process)

    def _fetch_company_network_data(self, company_number, snapshot_date=None):
        """Worker to fetch profile, officers, and PSCs for one company."""
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
        # --- NEW: If no snapshot date is provided, return all data immediately ---
        if not snapshot_date:
            return profile, officers, pscs, profile_err

        # --- NEW: Filter officers based on the snapshot date ---
        if officers and officers.get("items"):
            active_officers = []
            for officer in officers["items"]:
                resigned_on_str = officer.get("resigned_on")
                if not resigned_on_str:
                    active_officers.append(officer)  # Still active
                    continue
                try:
                    resigned_date = datetime.strptime(resigned_on_str, "%Y-%m-%d")
                    if resigned_date > snapshot_date:
                        active_officers.append(
                            officer
                        )  # Was still active on snapshot date
                except (ValueError, TypeError):
                    active_officers.append(officer)  # Keep if date is malformed
            officers["items"] = active_officers

        # --- NEW: Filter PSCs based on the snapshot date ---
        # This requires an extra API call per PSC to get detailed date info.
        if pscs and pscs.get("items"):
            active_pscs = []
            for psc_summary in pscs["items"]:
                psc_self_link = psc_summary.get("links", {}).get("self")
                if not psc_self_link:
                    continue

                p_details, _ = ch_get_data(
                    self.api_key, self.ch_token_bucket, psc_self_link
                )
                if not p_details:
                    continue

                try:
                    notified_date = datetime.strptime(
                        p_details.get("notified_on", ""), "%Y-%m-%d"
                    )
                    if notified_date > snapshot_date:
                        continue  # Not a PSC yet on the snapshot date

                    ceased_on_str = p_details.get("ceased_on")
                    if ceased_on_str:
                        ceased_date = datetime.strptime(ceased_on_str, "%Y-%m-%d")
                        if ceased_date <= snapshot_date:
                            continue  # Had already ceased being a PSC

                    # If all checks pass, it was an active PSC on the snapshot date
                    active_pscs.append(psc_summary)

                except (ValueError, TypeError):
                    # If dates are missing/malformed, include it by default
                    active_pscs.append(psc_summary)
            pscs["items"] = active_pscs

        return profile, officers, pscs, profile_err

    def _build_ubo_network_graph_object(self):
        """
        Builds a comprehensive network graph including companies, addresses,
        directors, and PSCs for all companies in the UBO chain.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # --- NEW: Parse the snapshot date from the UI ---
        snapshot_date_str = self.snapshot_date_var.get()
        snapshot_date = None
        if snapshot_date_str:
            try:
                snapshot_date = datetime.strptime(snapshot_date_str, "%d/%m/%Y")
            except ValueError:
                # Show an error if the date is invalid but continue without filtering
                self.safe_ui_call(
                    messagebox.showwarning,
                    "Invalid Date for Graph",
                    f"The date '{snapshot_date_str}' is not a valid format (DD/MM/YYYY). "
                    "The graph will be generated with all historic data.",
                    parent=self,
                )

        G = nx.DiGraph()

        all_company_numbers = {
            row["parent_company_number"]
            for row in self.results_data
            if "parent_company_number" in row
        }
        all_company_numbers.update(
            {
                row["psc_company_number"]
                for row in self.results_data
                if row.get("psc_company_number")
            }
        )
        all_company_numbers.update({row["root_company"] for row in self.results_data})

        if not all_company_numbers:
            self.app.after(
                0, lambda: self.status_var.set("No companies found to build graph.")
            )
            return None

        self.app.after(
            0,
            lambda: self.status_var.set(
                f"Fetching full details for {len(all_company_numbers)} companies in the network..."
            ),
        )

        failed_graph_companies = []
        with ThreadPoolExecutor(max_workers=self.app.ch_max_workers) as executor:
            future_to_cnum = {
                executor.submit(
                    self._fetch_company_network_data, cnum, snapshot_date
                ): cnum
                for cnum in all_company_numbers
                if cnum
            }

            for i, future in enumerate(as_completed(future_to_cnum)):
                if self.cancel_flag.is_set():
                    return None
                cnum = future_to_cnum[future]
                self.app.after(
                    0,
                    lambda: self.status_var.set(
                        f"Processing company {i+1}/{len(all_company_numbers)}..."
                    ),
                )

                profile, officers, pscs, profile_err = future.result()
                if not profile:
                    failed_graph_companies.append((cnum, profile_err))
                    continue

                G.add_node(
                    cnum, label=profile.get("company_name", cnum), type="company"
                )

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
                # FIX: Changed from _clean_address_string to clean_address_string
                address_str = clean_address_string(raw_address_str)
                if address_str:
                    G.add_node(address_str, label=raw_address_str, type="address")
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

                        # --- FIX: Check for existing edge before adding ---
                        if G.has_edge(cnum, person_key):
                            # If edge exists, append the new role to the label
                            G[cnum][person_key][
                                "label"
                            ] += f", {officer.get('officer_role', 'officer')}"
                        else:
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

                        # --- FIX: Check for existing edge before adding ---
                        if G.has_edge(cnum, person_key):
                            # If edge exists, append 'psc' to the label
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
        if failed_graph_companies:
            warning = format_error_summary(failed_graph_companies, "company")
            self.app.after(
                0,
                lambda w=warning: self.status_var.set(f"Graph built. {w}"),
            )

        return G

    def _fetch_full_company_details(self, company_number):
        """Worker to fetch profile and officers for a single company."""
        profile, profile_err = ch_get_data(
            self.api_key, self.ch_token_bucket, f"/company/{company_number}"
        )
        officers, _ = ch_get_data(
            self.api_key,
            self.ch_token_bucket,
            f"/company/{company_number}/officers?items_per_page=100",
        )
        return profile, officers, profile_err

    def _generate_and_open_graph(self):
        """Renders and opens the visual graph with specific PSC chain styling."""
        try:
            import networkx as nx
            from pyvis.network import Network
            import webbrowser
            import html
        except ImportError:
            messagebox.showerror(
                "Missing Libraries",
                "The 'networkx' and 'pyvis' libraries are required.",
            )
            return

        if not self.results_data:
            messagebox.showinfo("No Data", "No data available to generate a graph.")
            return

        self.app.after(0, lambda: self.status_var.set("Analyzing network structure..."))
        self.app.update_idletasks()

        G = nx.DiGraph()
        company_name_map = {
            row["root_company"]: row["root_company_name"]
            for row in self.results_data
            if row.get("root_company_name")
        }

        # --- NEW: Ensure all root companies are added as nodes initially ---
        root_companies = {row["root_company"] for row in self.results_data}
        for root_cnum in root_companies:
            G.add_node(
                root_cnum,
                type="company",
                level=0,
                label=company_name_map.get(root_cnum, root_cnum),
            )

        for row in self.results_data:
            # Skip placeholders that don't represent a real connection
            if "psc_unique_id" not in row:
                continue

            parent_id = row["parent_company_number"]
            child_id = row["psc_unique_id"]

            # Add nodes if they don't already exist from the root company pass
            if not G.has_node(parent_id):
                G.add_node(parent_id, type="company", level=row["level"] - 1)
            if not G.has_node(child_id):
                G.add_node(
                    child_id,
                    type="psc",
                    level=row["level"],
                    psc_name=row["psc_name"],
                    psc_kind=row["psc_kind"],
                )

            G.add_edge(parent_id, child_id, title=row.get("shareholding"))
            if "corporate" in row.get("psc_kind", "") and row.get("psc_company_number"):
                company_name_map[row["psc_company_number"]] = row["psc_name"]

        shared_nodes = {node for node, degree in G.in_degree() if degree > 1}
        historic_nodes = set()
        if not self.snapshot_date_var.get():
            for row in self.results_data:
                if row.get("ceased_on"):
                    historic_nodes.add(row["psc_unique_id"])
            for _ in range(10):  # Propagate historic status up the chain
                for u, v in G.edges():
                    if u in historic_nodes:
                        historic_nodes.add(v)

        self.app.after(
            0, lambda: self.status_var.set("Generating interactive graph...")
        )
        self.app.update_idletasks()
        net = Network(
            height="95vh",
            width="100%",
            directed=True,
            notebook=False,
            cdn_resources="local",
        )
        net.set_options(
            """
        var options = {
            "layout": { "hierarchical": { "enabled": true, "levelSeparation": 250, "nodeSpacing": 150, "direction": "UD", "sortMethod": "directed" }},
            "physics": { "enabled": false }
        }
        """
        )

        for node_id, attrs in G.nodes(data=True):
            is_company = attrs.get("type") == "company"

            label_lines = []
            if is_company:
                safe_name = html.escape(company_name_map.get(node_id, "Unknown"))
                label_lines.append(safe_name)
                label_lines.append(f"({html.escape(node_id)})")
            else:  # It's a PSC
                safe_name = html.escape(attrs.get("psc_name", ""))
                label_lines.append(safe_name)
                if "-" in str(node_id):
                    try:
                        name_key, year, month = str(node_id).rsplit("-", 2)
                        label_lines.append(f"({month}/{year})")
                    except (ValueError, TypeError):
                        pass

            label = "\n".join(label_lines)

            is_shared, is_historic = node_id in shared_nodes, node_id in historic_nodes
            is_corporate_psc = "corporate" in attrs.get("psc_kind", "")
            node_size = 25 if is_shared else 15
            node_color = (
                "#E0E0E0"
                if is_historic
                else (
                    "#FFB347"
                    if is_shared
                    else ("#B9D9EB" if is_company or is_corporate_psc else "#D9E8B9")
                )
            )
            shape = "box" if is_company or is_corporate_psc else "ellipse"

            net.add_node(
                node_id,
                label=label,
                title=label,
                level=attrs.get("level", 0),
                shape=shape,
                color=node_color,
                size=node_size,
            )

        for u, v, attrs in G.edges(data=True):
            net.add_edge(u, v, title=attrs.get("title"))

        try:
            filename = os.path.join(CONFIG_DIR, "ubo_ownership_graph.html")
            net.write_html(filename, notebook=False)
            self.app.after(
                0, lambda: self.status_var.set("Graph generated! Opening in browser...")
            )
            webbrowser.open(f"file://{os.path.realpath(filename)}")
        except Exception as e:
            log_message(f"Failed to generate or open graph: {e}")
            messagebox.showerror(
                "Graph Error", f"Could not save or open the graph file: {e}"
            )

    def _export_graph_to_csv(self, G):
        """Exports the graph's connections (edges) to a CSV file."""
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
                f"Successfully exported {G.number_of_edges()} UBO graph connections."
            )
            messagebox.showinfo(
                "Export Successful",
                f"Successfully exported {G.number_of_edges()} connections to CSV.",
            )
            self.app.after(
                0, lambda: self.status_var.set("Graph data export complete.")
            )

        except IOError as e:
            log_message(f"UBO graph data export failed: {e}")
            messagebox.showerror("Export Error", f"Could not write to file: {e}")

    def _finish_investigation(self):
        try:
            self.cancel_btn.pack_forget()
            self.run_btn.pack(side=tk.LEFT, padx=5)

            if self.results_data:
                self.export_btn.config(state="normal")
                self.export_graph_data_btn.config(state="normal")
                self.generate_graph_btn.config(state="normal")
            elif not self.cancel_flag.is_set():
                messagebox.showinfo(
                    "No Results", "The investigation completed, but no PSCs were found."
                )
        except tk.TclError:
            log_message(
                "UI was already destroyed; _finish_investigation for UBO aborted."
            )

    def _finish_graph_process(self):
        """Resets the UI after any graph process completes or is cancelled."""
        if self.cancel_flag.is_set():
            self.app.after(0, lambda: self.status_var.set("Operation cancelled."))
        else:
            if "..." not in self.status_var.get():
                self.app.after(0, lambda: self.status_var.set("Ready."))

        # Restore button states
        self.run_btn.config(state="normal")
        if self.results_data:
            self.export_btn.config(state="normal")
            self.export_graph_data_btn.config(state="normal")
            self.generate_graph_btn.config(state="normal")

    def export_csv(self):
        if not self.results_data:
            messagebox.showinfo("No Data", "There is no data to export.")
            return

        snapshot_date_str = self.snapshot_date_var.get()
        data_to_export = self.results_data
        snapshot_date = None

        if snapshot_date_str:
            try:
                snapshot_date = datetime.strptime(snapshot_date_str, "%d/%m/%Y")
            except ValueError:
                messagebox.showerror(
                    "Invalid Date",
                    f"The date '{snapshot_date_str}' is not a valid format (DD/MM/YYYY). Exporting unfiltered data.",
                )

        if snapshot_date:
            filtered_data = []
            for row in self.results_data:
                notified_on = row.get("notified_on")
                ceased_on = row.get("ceased_on")

                try:
                    # PSC must have been notified on or before the snapshot date
                    if (
                        notified_on
                        and datetime.strptime(notified_on, "%Y-%m-%d") > snapshot_date
                    ):
                        continue

                    # If PSC has ceased, it must be after the snapshot date
                    if (
                        ceased_on
                        and datetime.strptime(ceased_on, "%Y-%m-%d") <= snapshot_date
                    ):
                        continue

                    filtered_data.append(row)
                except (ValueError, TypeError):
                    # If dates are invalid, include the row by default
                    filtered_data.append(row)

            data_to_export = filtered_data

        headers = [
            "root_company",
            "root_company_name",
            "level",
            "parent_company_number",
            "psc_name",
            "psc_company_number",
            "psc_kind",
            "psc_unique_id",
            "country",
            "shareholding",
            "accounts_type",
            "notified_on",
            "ceased_on",
        ]

        # Temporarily set self.results_data for the generic export function
        original_data = self.results_data
        self.results_data = data_to_export
        self.generic_export_csv(headers)
        self.results_data = original_data  # Restore original data
