# modules/grants_search.py
"""Grants Search"""

# --- Standard Library ---
import csv
import textwrap
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Tkinter ---
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

# --- From Our Package ---
# API functions (were global functions in original file)
from ..api.grantnav import grantnav_get_data

# Constants (were at top of original file)
from ..constants import (
    GRANT_DATA_FIELDS,
    GRANTNAV_API_BASE_URL
)

# Utility functions (were global functions or duplicated in classes)
from ..utils.helpers import log_message, format_eta

# Base class (was in original file)
from .base import InvestigationModuleBase


class GrantsSearch(InvestigationModuleBase):
    def __init__(self, parent_app, api_key, back_callback, prefill_entities=None):
        super().__init__(parent_app, back_callback, api_key)
        self._prefill_entities = prefill_entities

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
            self.content_frame, text="Step 2: Select Company Number Column", padding=10
        )
        self.column_selection_frame.pack(fill=tk.X, pady=5, padx=10)

        config_frame = ttk.LabelFrame(
            self.content_frame, text="Step 3: Select Grant Data Fields", padding=10
        )
        config_frame.pack(fill=tk.X, pady=5, padx=10)
        self.data_fields_vars = {
            k: tk.BooleanVar(value=True) for k in GRANT_DATA_FIELDS
        }
        select_all_var = tk.BooleanVar(value=True)

        # Main "Select All" checkbox at the top
        ttk.Checkbutton(
            config_frame,
            text="Select/Deselect All",
            variable=select_all_var,
            command=lambda: self.toggle_all_fields(select_all_var.get()),
        ).pack(anchor="w", padx=5, pady=(0, 5))

        # A container to hold the two columns
        columns_container = ttk.Frame(config_frame)
        columns_container.pack(fill="x", expand=True)

        # --- Column 1: Grant & Beneficiary Details ---
        col1_frame = ttk.LabelFrame(
            columns_container, text="Grant & Beneficiary", padding=10
        )
        col1_frame.pack(side="left", fill="both", expand=True, padx=(0, 5), anchor="n")

        # --- Column 2: Funder & Date Details ---
        col2_frame = ttk.LabelFrame(
            columns_container, text="Funder & Dates", padding=10
        )
        col2_frame.pack(side="left", fill="both", expand=True, padx=(5, 0), anchor="n")

        # Distribute the checkboxes between the two new frames
        field_items = list(GRANT_DATA_FIELDS.items())
        mid_point = (len(field_items) + 1) // 2

        for key, text in field_items[:mid_point]:
            ttk.Checkbutton(
                col1_frame, text=text, variable=self.data_fields_vars[key]
            ).pack(anchor="w")

        for key, text in field_items[mid_point:]:
            ttk.Checkbutton(
                col2_frame, text=text, variable=self.data_fields_vars[key]
            ).pack(anchor="w")

        run_frame = ttk.LabelFrame(
            self.content_frame, text="Step 4: Run & Export", padding=10
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
        self.status_entity_var = tk.StringVar(value="")
        ttk.Label(run_frame, textvariable=self.status_entity_var).pack(anchor=tk.W)
        self.status_var = tk.StringVar(value="Ready.")
        ttk.Label(run_frame, textvariable=self.status_var).pack(anchor=tk.W)

        # Apply prefill from Bulk Entity Search
        if self._prefill_entities:
            self._apply_prefill()

    def _apply_prefill(self):
        """Pre-populate data from entities sent by another module."""
        rows = []
        headers_set = set()
        for ent in self._prefill_entities:
            row = {}
            cnum = ent.get("company_number", "")
            name = ent.get("name", "")
            etype = ent.get("entity_type", "company")
            if etype == "charity":
                row["charity_number"] = str(cnum)
                headers_set.add("charity_number")
            else:
                row["company_number"] = str(cnum)
                headers_set.add("company_number")
            if name:
                row["entity_name"] = name
                headers_set.add("entity_name")
            rows.append(row)

        if not rows:
            return

        # Determine consistent headers
        headers = []
        for h in ["company_number", "charity_number", "entity_name"]:
            if h in headers_set:
                headers.append(h)

        self.original_data = rows
        self.original_headers = headers
        self.file_status_label.config(
            text=f"Prefilled: {len(rows)} entities from Bulk Entity Search.",
            foreground="green",
        )
        self._display_column_selection_ui()
        # Auto-select columns
        if "company_number" in headers:
            self.company_num_col_var.set("company_number")
        if "charity_number" in headers:
            self.charity_num_col_var.set("charity_number")
        # Auto-confirm
        self._confirm_column()

    def toggle_all_fields(self, select):
        for var in self.data_fields_vars.values():
            var.set(select)

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

        self.company_num_col_var = tk.StringVar()
        self.charity_num_col_var = tk.StringVar()

        # Options include "None"
        options = ["___NONE___"] + self.original_headers

        # --- UI Setup for two columns ---
        # Container
        container = ttk.Frame(self.column_selection_frame)
        container.pack(fill="x", expand=True, pady=5)

        # Company Number Dropdown
        company_frame = ttk.LabelFrame(
            container, text="Select Company Number Column", padding=5
        )
        company_frame.pack(side=tk.LEFT, fill="x", expand=True, padx=5)
        
        c_combo = ttk.Combobox(
            company_frame,
            textvariable=self.company_num_col_var,
            values=options,
            state="readonly"
        )
        c_combo.pack(fill="x", pady=5)
        c_combo.set("___NONE___")

        # Charity Number Dropdown
        charity_frame = ttk.LabelFrame(
            container, text="Select Charity Number Column", padding=5
        )
        charity_frame.pack(side=tk.LEFT, fill="x", expand=True, padx=5)
        
        ch_combo = ttk.Combobox(
            charity_frame,
            textvariable=self.charity_num_col_var,
            values=options,
            state="readonly"
        )
        ch_combo.pack(fill="x", pady=5)
        ch_combo.set("___NONE___")

        # Confirm Button
        ttk.Button(
            self.column_selection_frame,
            text="Confirm Columns",
            command=self._confirm_column,
        ).pack(side=tk.BOTTOM, pady=10)

        self.app.after(1, self._update_scrollregion)

    def _confirm_column(self):
        self.company_num_col = self.company_num_col_var.get()
        self.charity_num_col = self.charity_num_col_var.get()

        # Handle the 'None' case
        if self.company_num_col == "___NONE___":
            self.company_num_col = None
        if self.charity_num_col == "___NONE___":
            self.charity_num_col = None

        if not self.company_num_col and not self.charity_num_col:
            messagebox.showerror(
                "Selection Error",
                "You must select a column for Company Number OR Charity Number.",
            )
            return

        messagebox.showinfo(
            "Columns Confirmed",
            "Column selection confirmed. You can now run the investigation.",
        )
        self.run_btn.config(state="normal")

    def start_investigation(self):
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

    def _process_grant_row(self, row_tuple):
        i, row = row_tuple
        if self.cancel_flag.is_set():
            return []

        all_found_grants = []
        search_status = "No identifier provided in selected columns"

        # --- Step 1: Get identifiers from the row ---
        company_number = (
            row.get(self.company_num_col, "").strip() if self.company_num_col else None
        )
        charity_number = (
            row.get(self.charity_num_col, "").strip() if self.charity_num_col else None
        )

        # --- Step 2: Prioritize Company Number Search ---
        if company_number:
            search_status = f"No grants found for Company Number {company_number}"
            # Normalize to 8-digit format (GB-COH- always uses 8 digits)
            cnum_upper = company_number.upper().strip()
            # Handle numeric-only company numbers by padding to 8 digits
            if cnum_upper.isdigit():
                cnum_upper = cnum_upper.zfill(8)
            
            org_id = f"GB-COH-{cnum_upper}"
            grants = self._fetch_all_grants(org_id)
            if grants:
                all_found_grants.extend(grants)
                search_status = f"Grants found via Company Number ({company_number})"

        # --- Step 3: Fallback to Charity Number Search if no grants found yet ---
        if not all_found_grants and charity_number:
            search_status = f"No grants found for Charity Number {charity_number}"
            org_id = f"GB-CHC-{charity_number}"
            grants = self._fetch_all_grants(org_id)
            if grants:
                all_found_grants.extend(grants)
                search_status = f"Grants found via Charity Number ({charity_number})"

        # --- Step 4: Process results ---
        new_rows = []
        if not all_found_grants:
            new_row = row.copy()
            new_row["grant_search_status"] = search_status
            new_rows.append(new_row)
        else:
            # De-duplicate the grants in case of overlapping searches
            filtered = [
                g for g in all_found_grants if isinstance(g, dict) and "id" in g
            ]

            if len(filtered) < len(all_found_grants):
                skipped = len(all_found_grants) - len(filtered)
                log_message(
                    f"Skipped {skipped} malformed grant object(s) "
                    f"for org_id search – they were not dicts with an 'id' key."
                )

            unique_grants = {g["id"]: g for g in filtered}.values()

            for grant in unique_grants:
                new_row = row.copy()
                new_row["grant_search_status"] = search_status
                self._add_selected_grant_data(new_row, grant)
                new_rows.append(new_row)

        return new_rows

    def _run_investigation_thread(self):

        self.safe_ui_call(
            self.progress_bar.config, maximum=len(self.original_data), value=0
        )

        # --- RATE LIMITING ---
        # The GrantNav API has a strict 2 requests/second limit.
        # We will set MAX_WORKERS to 2 to respect this.
        MAX_WORKERS = 2

        total = len(self.original_data)
        self.app.after(0, lambda: self.status_var.set(f"Processing {total} rows..."))
        rows_to_process = list(enumerate(self.original_data))

        start_time = time.monotonic()
        processed_count = 0
        found_count = 0
        error_count = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(self._process_grant_row, row): row
                for row in rows_to_process
            }

            try:
                for future in as_completed(futures):
                    if self.cancel_flag.is_set():
                        for f in futures:
                            f.cancel()
                        log_message("Grant investigation cancelled by user.")
                        self.app.after(0, lambda: self.status_entity_var.set(""))
                        self.app.after(0, lambda: self.status_var.set("Investigation cancelled."))
                        break

                    row_tuple = futures[future]
                    _, row_dict = row_tuple
                    try:
                        list_of_new_rows = future.result()
                    except Exception as exc:
                        log_message(f"Grant row error: {exc}")
                        list_of_new_rows = []
                        error_count += 1

                    if list_of_new_rows:
                        self.results_data.extend(list_of_new_rows)
                        found_count += 1

                    processed_count += 1
                    elapsed = time.monotonic() - start_time
                    eta = format_eta(elapsed, processed_count, total)

                    row_name = (
                        (self.company_num_col and row_dict.get(self.company_num_col, ""))
                        or (self.charity_num_col and row_dict.get(self.charity_num_col, ""))
                        or f"row {processed_count}"
                    )
                    entity = f"Searching: {row_name} ({processed_count} of {total})"
                    stats = f"ETA: {eta} | Found: {found_count} matches | Errors: {error_count}"

                    def update_progress(p=processed_count, e=entity, s=stats):
                        self.progress_bar.configure(value=p)
                        self.status_entity_var.set(e)
                        self.status_var.set(s)
                        self.app.update_idletasks()
                    self.app.after(0, update_progress)

            except Exception as e:
                log_message(f"An error occurred during grant investigation: {e}")
                self.safe_ui_call(
                    messagebox.showerror, "Error", f"A processing error occurred: {e}"
                )

        self.safe_ui_call(self.status_entity_var.set, "")
        if not self.cancel_flag.is_set():
            self.app.after(0, lambda: self.status_var.set("Investigation complete!"))

        self.after(100, self._finish_investigation)

    def _fetch_all_grants(self, org_id):
        all_results = []
        url = f"{GRANTNAV_API_BASE_URL}/org/{org_id}/grants_received?limit=1000"

        while url:
            if self.cancel_flag.is_set():
                break

            data, error = grantnav_get_data(url)
            if error:
                log_message(f"GrantNav error for {org_id}: {error}")
                break

            if data and "results" in data:
                for item in data["results"]:
                    # item['data'] is a dict; append keeps it as one list element
                    all_results.append(item.get("data", {}))
                url = data.get("next")  # pagination
            else:
                break

        return all_results

    def _finish_investigation(self):
        """
        Safely resets the UI after the investigation, handling cases
        where the process was cancelled or the UI was destroyed.
        """
        # If the process was cancelled and we navigated away, do nothing.
        if self.cancel_flag.is_set():
            return

        try:
            # Swap the Cancel button back to the Run button
            self.cancel_btn.pack_forget()
            self.run_btn.pack(side=tk.LEFT, padx=5)

            # Enable the export button if there are results to export
            if self.results_data:
                self.export_btn.config(state="normal")

        except tk.TclError:
            # This is a safeguard. If the widgets were destroyed by another
            # process (like clicking "Back"), just ignore the error.
            log_message(
                "UI was already destroyed; _finish_investigation for Grants aborted."
            )
            pass

    def _add_selected_grant_data(self, row, grant_data):
        for key, text in GRANT_DATA_FIELDS.items():
            if self.data_fields_vars[key].get():
                row[text] = self.get_nested_value(grant_data, key)

    def export_csv(self):
        if not self.results_data:
            return
        all_headers = list(self.original_headers) + ["grant_search_status"]
        for key, text in GRANT_DATA_FIELDS.items():
            if self.data_fields_vars[key].get():
                all_headers.append(text)
        unique_headers = list(dict.fromkeys(all_headers))
        self.generic_export_csv(unique_headers)
