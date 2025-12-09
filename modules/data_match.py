# modules/data_match.py
"""Data Match"""

import csv
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from rapidfuzz import process, fuzz
from .base import InvestigationModuleBase


class DataMatch(InvestigationModuleBase):
    def __init__(self, parent_app, back_callback, api_key=None):
        super().__init__(parent_app, back_callback, api_key)

        # State for two files
        self.primary_data, self.primary_headers = [], []
        self.matching_data, self.matching_headers = [], []

        # --- UI Setup ---
        # Step 1: File Uploads
        upload_frame = ttk.LabelFrame(
            self.content_frame, text="Step 1: Upload Files", padding=10
        )
        upload_frame.pack(fill=tk.X, pady=5, padx=10)

        primary_frame = ttk.Frame(upload_frame)
        primary_frame.pack(fill=tk.X, pady=2)
        ttk.Button(
            primary_frame, text="Upload Primary (Left) File", command=self.load_primary
        ).pack(side=tk.LEFT, padx=5)
        self.primary_status_label = ttk.Label(primary_frame, text="No file loaded.")
        self.primary_status_label.pack(side=tk.LEFT)

        matching_frame = ttk.Frame(upload_frame)
        matching_frame.pack(fill=tk.X, pady=2)
        ttk.Button(
            matching_frame,
            text="Upload Matching (Right) File",
            command=self.load_matching,
        ).pack(side=tk.LEFT, padx=5)
        self.matching_status_label = ttk.Label(matching_frame, text="No file loaded.")
        self.matching_status_label.pack(side=tk.LEFT)

        # Step 2: Logic Selection
        logic_frame = ttk.LabelFrame(
            self.content_frame, text="Step 2: Choose Matching Logic", padding=10
        )
        logic_frame.pack(fill=tk.X, pady=5, padx=10)
        self.match_logic_var = tk.StringVar(value="number")
        ttk.Radiobutton(
            logic_frame,
            text="Exact Match on a Unique Identifier",
            variable=self.match_logic_var,
            value="number",
            command=self.toggle_slider,
        ).pack(anchor="w")
        self.pad_numbers_var = tk.BooleanVar(value=False)
        self.pad_check = ttk.Checkbutton(
            logic_frame,
            text="Pad numerical identifiers to 8 digits (for UK Company Numbers)",
            variable=self.pad_numbers_var,
        )
        self.pad_check.pack(anchor="w", padx=20)
        ttk.Radiobutton(
            logic_frame,
            text="Fuzzy Match on a Text Field (e.g., name)",
            variable=self.match_logic_var,
            value="name",
            command=self.toggle_slider,
        ).pack(anchor="w")
        self.accuracy_frame = ttk.Frame(logic_frame)
        self.accuracy_frame.pack(fill=tk.X, padx=20, pady=5)
        self.accuracy_label = ttk.Label(
            self.accuracy_frame, text="Fuzzy Match Accuracy:"
        )
        self.accuracy_label.pack(side=tk.LEFT)
        self.accuracy_var = tk.IntVar(value=85)
        self.accuracy_slider = ttk.Scale(
            self.accuracy_frame,
            from_=0,
            to=100,
            orient=tk.HORIZONTAL,
            variable=self.accuracy_var,
            length=150,
            command=lambda val: self.accuracy_var.set(round(float(val))),
        )
        self.accuracy_slider.pack(side=tk.LEFT, padx=5)
        self.accuracy_value_label = ttk.Label(
            self.accuracy_frame, textvariable=self.accuracy_var
        )
        self.accuracy_value_label.pack(side=tk.LEFT)
        self.toggle_slider()

        # Step 3: Column Selection
        self.column_selection_frame = ttk.LabelFrame(
            self.content_frame, text="Step 3: Select Matching Columns", padding=10
        )
        self.column_selection_frame.pack(fill=tk.X, pady=5, padx=10)

        # Step 4: Run & Export
        run_frame = ttk.LabelFrame(
            self.content_frame, text="Step 4: Run & Export", padding=10
        )
        run_frame.pack(fill=tk.BOTH, expand=True, pady=5, padx=10)
        run_buttons_frame = ttk.Frame(run_frame)
        run_buttons_frame.pack(pady=5)
        self.run_btn = ttk.Button(
            run_buttons_frame,
            text="Run Match",
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

    def load_primary(self):
        path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if not path:
            return
        data, headers = self._load_file_data(path)
        if data:
            self.primary_data, self.primary_headers = data, headers
            self.primary_status_label.config(
                text=f"OK: {len(self.primary_data)} rows", foreground="green"
            )
            self._check_and_display_columns()
        else:
            self.primary_status_label.config(
                text="Error loading file.", foreground="red"
            )

    def load_matching(self):
        path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if not path:
            return
        data, headers = self._load_file_data(path)
        if data:
            self.matching_data, self.matching_headers = data, headers
            self.matching_status_label.config(
                text=f"OK: {len(self.matching_data)} rows", foreground="green"
            )
            self._check_and_display_columns()
        else:
            self.matching_status_label.config(
                text="Error loading file.", foreground="red"
            )

    def _load_file_data(self, path):
        try:
            encodings = ["utf-8-sig", "cp1252"]
            for enc in encodings:
                try:
                    with open(path, "r", encoding=enc, newline="") as f:
                        reader = csv.DictReader(f)
                        headers = reader.fieldnames
                        data = list(reader)
                    if not headers or not data:
                        raise ValueError("CSV file is empty or invalid.")
                    return data, headers
                except UnicodeDecodeError:
                    continue
            raise ValueError("Could not decode file.")
        except Exception as e:
            messagebox.showerror("File Error", f"Could not read file: {e}")
            return [], []

    def _check_and_display_columns(self):
        if self.primary_data and self.matching_data:
            self._display_column_selection_ui()

    def _display_column_selection_ui(self):
        for widget in self.column_selection_frame.winfo_children():
            widget.destroy()

        self.primary_key_var = tk.StringVar(value="___NONE___")
        self.match_key_var = tk.StringVar(value="___NONE___")

        # Primary File Columns
        p_frame = ttk.LabelFrame(
            self.column_selection_frame, text="Primary File Column", padding=5
        )
        p_frame.pack(side=tk.LEFT, fill=tk.Y, padx=5, pady=5)
        for h in self.primary_headers:
            ttk.Radiobutton(
                p_frame, text=h, variable=self.primary_key_var, value=h
            ).pack(anchor="w")

        # Matching File Columns
        m_frame = ttk.LabelFrame(
            self.column_selection_frame, text="Matching File Column", padding=5
        )
        m_frame.pack(side=tk.LEFT, fill=tk.Y, padx=5, pady=5)
        for h in self.matching_headers:
            ttk.Radiobutton(m_frame, text=h, variable=self.match_key_var, value=h).pack(
                anchor="w"
            )

        ttk.Button(
            self.column_selection_frame,
            text="Confirm Columns",
            command=self._confirm_columns,
        ).pack(side=tk.LEFT, padx=20, expand=True)

    def _confirm_columns(self):
        p_key_col = self.primary_key_var.get()
        m_key_col = self.match_key_var.get()

        if p_key_col == "___NONE___" or m_key_col == "___NONE___":
            messagebox.showerror(
                "Selection Error", "You must select a column from BOTH files."
            )
            return

        messagebox.showinfo(
            "Columns Confirmed", "Column selections confirmed. Ready to run."
        )
        self.run_btn.config(state="normal")

    def toggle_slider(self):
        if self.match_logic_var.get() == "name":
            self.accuracy_slider.config(state="normal")
            self.accuracy_label.config(foreground="black")
        else:
            self.accuracy_slider.config(state="disabled")
            self.accuracy_label.config(foreground="grey")

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

    def _run_investigation_thread(self):
        from rapidfuzz import process, fuzz

        self.progress_bar["maximum"] = len(self.primary_data)
        self.results_data = []

        logic = self.match_logic_var.get()
        primary_key = self.primary_key_var.get()
        match_key = self.match_key_var.get()
        should_pad = self.pad_numbers_var.get()

        if logic == "number":
            self.app.after(
                0, lambda: self.status_var.set("Building lookup table...")
            )
            match_lookup = {}
            for row in self.matching_data:
                # Always use the un-padded version as the definitive key
                num_raw = row.get(match_key, "").strip().upper()
                if not num_raw:
                    continue
                
                lookup_key = num_raw
                if should_pad and num_raw.isdigit():
                    lookup_key = num_raw.lstrip("0") or "0"

                match_lookup.setdefault(lookup_key, []).append(row)

            for i, p_row in enumerate(self.primary_data):
                if self.cancel_flag.is_set():
                    break
                self.safe_update(
                    self.status_var.set,
                    f"Processing primary row {i + 1}/{len(self.primary_data)}...",
                )
                self.app.after(0, self.progress_bar.step, 1)

                p_num_raw = p_row.get(primary_key, "").strip().upper()
                
                # Also use the un-padded version for lookup
                lookup_key = p_num_raw
                if should_pad and p_num_raw.isdigit():
                    lookup_key = p_num_raw.lstrip("0") or "0"

                if lookup_key in match_lookup:
                    for matched_row in match_lookup[lookup_key]:
                        new_row = p_row.copy()
                        renamed_matched_row = {
                            f"{key}_match": val for key, val in matched_row.items()
                        }
                        new_row.update(renamed_matched_row)
                        new_row["match_score"] = 100
                        self.results_data.append(new_row)
                else:
                    new_row = p_row.copy()
                    new_row["match_score"] = 0
                    self.results_data.append(new_row)

        elif logic == "name":
            self.app.after(
                0,
                lambda: self.status_var.set(
                    "Indexing matching file for fast lookups..."
                ),
            )
            match_choices = [row.get(match_key, "") for row in self.matching_data]
            threshold = self.accuracy_var.get()

            for i, p_row in enumerate(self.primary_data):
                if self.cancel_flag.is_set():
                    break

                self.safe_update(
                    self.status_var.set,
                    f"Processing primary row {i + 1}/{len(self.primary_data)}...",
                )
                self.app.after(0, self.progress_bar.step, 1)

                p_name = p_row.get(primary_key, "")
                if not p_name:
                    continue

                matches = process.extract(
                    p_name,
                    match_choices,
                    scorer=fuzz.WRatio,
                    limit=None,
                    score_cutoff=threshold,
                )

                if not matches:
                    new_row = p_row.copy()
                    new_row["match_score"] = 0
                    self.results_data.append(new_row)
                else:
                    for match_string, score, index in matches:
                        new_row = p_row.copy()
                        matched_row = self.matching_data[index]
                        renamed_matched_row = {
                            f"{key}_match": val for key, val in matched_row.items()
                        }
                        new_row.update(renamed_matched_row)
                        new_row["match_score"] = round(score, 2)
                        self.results_data.append(new_row)

        if not self.cancel_flag.is_set():
            self.safe_update(self.status_var.set, "Match complete!")
        else:
            self.safe_update(self.status_var.set, "Match cancelled.")

        self.after(100, self._finish_investigation)

    def _finish_investigation(self):
        self.cancel_btn.pack_forget()
        self.run_btn.pack(side=tk.LEFT, padx=5)
        if self.results_data:
            self.export_btn.config(state="normal")

    def export_csv(self):
        if not self.results_data:
            return

        all_headers_set = set()
        for row in self.results_data:
            all_headers_set.update(row.keys())

        all_headers = sorted(list(all_headers_set))

        if "match_score" in all_headers:
            all_headers.insert(0, all_headers.pop(all_headers.index("match_score")))

        primary_key_col = self.primary_key_var.get()
        if primary_key_col in all_headers:
            all_headers.insert(0, all_headers.pop(all_headers.index(primary_key_col)))

        self.generic_export_csv(all_headers)
