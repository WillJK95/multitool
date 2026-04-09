# modules/grants_search.py
"""Grants Search"""

# --- Standard Library ---
import csv
import textwrap
import threading
import time
from collections import OrderedDict
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
    def __init__(self, parent_app, api_key, back_callback, prefill_entities=None, prefill_source=None):
        super().__init__(parent_app, back_callback, api_key, help_key="grants_search")
        self._prefill_entities = prefill_entities
        self._prefill_source = prefill_source
        self._tree_row_data = {}
        self._tree_root_entities = {}
        self._selected_grant_fields = []
        self._results_view_mode = "recipient"
        self._results_sort_col = None
        self._results_sort_reverse = False

        # --- Notebook with Configuration and Results tabs ---
        self.notebook = ttk.Notebook(self.content_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        self.config_tab = ttk.Frame(self.notebook)
        self.results_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.config_tab, text="Configuration")
        self.notebook.add(self.results_tab, text="Results")
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

        # --- Configuration tab ---
        upload_frame = ttk.LabelFrame(
            self.config_tab, text="Step 1: Upload File", padding=10
        )
        upload_frame.pack(fill=tk.X, pady=5, padx=10)
        ttk.Button(
            upload_frame, text="Upload Input File (.csv)", command=self.load_file
        ).pack(pady=5)
        self.file_status_label = ttk.Label(upload_frame, text="No file loaded.")
        self.file_status_label.pack(pady=5)

        self.column_selection_frame = ttk.LabelFrame(
            self.config_tab, text="Step 2: Select Company Number Column", padding=10
        )
        self.column_selection_frame.pack(fill=tk.X, pady=5, padx=10)

        config_frame = ttk.LabelFrame(
            self.config_tab, text="Step 3: Select Grant Data Fields", padding=10
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
            self.config_tab, text="Step 4: Run Investigation", padding=10
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

        self._build_results_tab()

        # Apply prefill from Bulk Entity Search
        if self._prefill_entities:
            self._apply_prefill()

    # ------------------------------------------------------------------
    # Tab management
    # ------------------------------------------------------------------

    def _on_tab_changed(self, event=None):
        """Toggle outer scroller when switching between Configuration and Results."""
        selected = self.notebook.select()
        on_results = (selected == str(self.results_tab))
        if on_results:
            self.scroller.scrollbar.pack_forget()
            self.scroller.canvas.yview_moveto(0)
            self.scroller.canvas.configure(yscrollcommand=lambda *a: None)
            self.scroller._disabled = True
        else:
            self.scroller.scrollbar.pack(side="right", fill="y")
            self.scroller.canvas.configure(yscrollcommand=self.scroller.scrollbar.set)
            self.scroller._disabled = False
            self._update_scrollregion()

    # ------------------------------------------------------------------
    # Results tab
    # ------------------------------------------------------------------

    def _build_results_tab(self):
        """Build Results tab widgets."""
        tree_frame = ttk.Frame(self.results_tab)
        tree_frame.pack(fill=tk.X, padx=10, pady=(10, 5))
        tree_frame.columnconfigure(0, weight=1)

        self.results_tree = ttk.Treeview(
            tree_frame,
            columns=(),
            show="tree headings",
            selectmode="extended",
            height=24,
        )
        self.results_tree.heading("#0", text="Entity / Grant", anchor=tk.W)
        self.results_tree.column("#0", width=360, minwidth=220)

        yscroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.results_tree.yview)
        xscroll = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=self.results_tree.xview)
        self.results_tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        self.results_tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")

        self.results_tree.bind("<<TreeviewSelect>>", self._on_tree_selection_changed)
        self.results_tree.bind(
            "<Button-1>", lambda e: self.app._toggle_tree_selection(e, self.results_tree)
        )

        bottom_bar = ttk.Frame(self.results_tab)
        bottom_bar.pack(fill=tk.X, padx=10, pady=(5, 10))
        self.export_btn = ttk.Button(
            bottom_bar, text="Export Results", state="disabled", command=self.export_csv
        )
        self.export_btn.pack(side=tk.LEFT, padx=(0, 8))

        self._sort_mode_btn = ttk.Button(
            bottom_bar,
            text="Sort by Funder",
            command=self._toggle_results_grouping_mode,
            state="disabled",
        )
        self._sort_mode_btn.pack(side=tk.LEFT, padx=(0, 8))

        self._send_menu_btn = ttk.Menubutton(bottom_bar, text="Send to… ▼", bootstyle="primary-outline")
        self._send_menu = tk.Menu(self._send_menu_btn, tearoff=0)
        self._send_menu.add_command(label="Working Set", command=self._send_to_working_set)
        self._send_menu.add_command(label="Enhanced Due Diligence", command=self._send_to_edd)
        self._send_menu.add_command(label="UBO Tracer (companies only)", command=self._send_to_ubo_tracer)
        self._send_menu.add_command(label="Bulk Entity Search", command=self._send_to_bulk_entity_search)
        self._send_menu_btn.configure(menu=self._send_menu)
        self._send_menu_btn.pack(side=tk.LEFT, padx=(0, 5))

        self._update_send_menu_state()

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
        source_text = self._prefill_source or "another module"
        self.file_status_label.config(
            text=f"Prefilled: {len(rows)} entities from {source_text}.",
            foreground="green",
        )
        self._display_column_selection_ui()
        # Auto-select columns
        if "company_number" in headers:
            self.company_num_col_var.set("company_number")
        if "charity_number" in headers:
            self.charity_num_col_var.set("charity_number")
        # Auto-confirm
        self._auto_confirm_columns()

    def _get_selected_grant_fields(self):
        """Return selected grant field tuples preserving configured order."""
        selected = []
        for key, label in GRANT_DATA_FIELDS.items():
            if self.data_fields_vars[key].get():
                selected.append((key, label))
        return selected

    def _configure_results_tree_columns(self):
        """Apply dynamic tree columns based on selected grant fields."""
        selected = self._get_selected_grant_fields()
        self._selected_grant_fields = selected
        col_ids = [f"col_{idx}" for idx, _ in enumerate(selected)]
        self.results_tree.configure(columns=tuple(col_ids))
        for idx, (_, label) in enumerate(selected):
            col_id = col_ids[idx]
            self.results_tree.heading(
                col_id,
                text=label,
                anchor=tk.W,
                command=lambda c=col_id: self._sort_results_tree(c),
            )
            self.results_tree.column(col_id, width=180, minwidth=120, anchor=tk.W)
        self.results_tree.heading(
            "#0",
            text="Entity / Grant",
            anchor=tk.W,
            command=lambda: self._sort_results_tree("#0"),
        )

    def _funder_sort_available(self):
        return any(label == "Funder Name" for _, label in self._selected_grant_fields)

    def _toggle_results_grouping_mode(self):
        self._results_view_mode = "funder" if self._results_view_mode == "recipient" else "recipient"
        self._populate_results_tree()

    def _update_results_toolbar_state(self):
        funder_available = self._funder_sort_available()
        if funder_available:
            self._sort_mode_btn.config(
                state="normal",
                text="Sort by Recipient" if self._results_view_mode == "funder" else "Sort by Funder",
            )
        else:
            self._results_view_mode = "recipient"
            self._sort_mode_btn.config(state="disabled", text="Sort by Funder")

    def _sort_results_tree(self, col_id):
        if self._results_sort_col == col_id:
            self._results_sort_reverse = not self._results_sort_reverse
        else:
            self._results_sort_col = col_id
            self._results_sort_reverse = False
        self._populate_results_tree()

    def _row_sort_key(self, row):
        if not self._results_sort_col:
            return ""
        if self._results_sort_col == "#0":
            if self._results_view_mode == "funder":
                return str(row.get("Funder Name", "") or row.get("Title", "")).lower()
            return str(self._row_to_entity(row).get("name", "")).lower()
        col_ids = [f"col_{idx}" for idx, _ in enumerate(self._selected_grant_fields)]
        if self._results_sort_col in col_ids:
            col_idx = col_ids.index(self._results_sort_col)
            _, label = self._selected_grant_fields[col_idx]
            return str(row.get(label, "")).lower()
        return ""

    def _populate_results_tree(self):
        """Populate results tree grouped by root entity."""
        self._configure_results_tree_columns()
        tree = self.results_tree
        tree.delete(*tree.get_children())
        self._tree_row_data.clear()
        self._tree_root_entities.clear()
        self._update_results_toolbar_state()

        if not self.results_data:
            self._update_send_menu_state()
            return

        rows = list(self.results_data)
        if self._results_sort_col:
            rows.sort(key=self._row_sort_key, reverse=self._results_sort_reverse)

        grouped = OrderedDict()
        if self._results_view_mode == "funder" and self._funder_sort_available():
            for row in rows:
                funder = str(row.get("Funder Name", "")).strip() or "Unknown funder"
                if funder not in grouped:
                    grouped[funder] = {"funder": funder, "rows": []}
                grouped[funder]["rows"].append(row)

            for group in grouped.values():
                funder = group["funder"]
                root_iid = tree.insert("", "end", text=funder, open=True, values=("",) * len(self._selected_grant_fields))
                pseudo_entity = {"name": funder, "company_number": "", "entity_type": "funder"}
                self._tree_row_data[root_iid] = dict(pseudo_entity)
                self._tree_root_entities[root_iid] = dict(pseudo_entity)

                grants = [r for r in group["rows"] if r.get("grant_search_status", "").startswith("Grants found via")]
                for row in grants:
                    recipient = self._row_to_entity(row)
                    display = f'{recipient["name"]} ({recipient["company_number"]})'
                    values = tuple(row.get(label, "") for _, label in self._selected_grant_fields)
                    child_iid = tree.insert(root_iid, "end", text=display, values=values)
                    grant_entry = row.copy()
                    grant_entry["_root_iid"] = root_iid
                    grant_entry["_recipient_entity"] = recipient
                    self._tree_row_data[child_iid] = grant_entry
        else:
            for row in rows:
                root_entity = self._row_to_entity(row)
                key = (
                    root_entity.get("name", ""),
                    root_entity.get("company_number", ""),
                    root_entity.get("entity_type", ""),
                )
                if key not in grouped:
                    grouped[key] = {"entity": root_entity, "rows": []}
                grouped[key]["rows"].append(row)

            for group in grouped.values():
                entity = group["entity"]
                label = f'{entity["name"]} ({entity["company_number"]})'
                root_iid = tree.insert("", "end", text=label, open=True, values=("",) * len(self._selected_grant_fields))
                self._tree_row_data[root_iid] = dict(entity)
                self._tree_root_entities[root_iid] = dict(entity)

                grants = [r for r in group["rows"] if r.get("grant_search_status", "").startswith("Grants found via")]
                if not grants:
                    child_iid = tree.insert(
                        root_iid,
                        "end",
                        text="No grants found",
                        values=("",) * len(self._selected_grant_fields),
                    )
                    self._tree_row_data[child_iid] = {"_is_placeholder": True, "_root_iid": root_iid}
                    continue

                for row in grants:
                    display = row.get("Title") or row.get("Funder Name") or "Grant"
                    values = tuple(row.get(label, "") for _, label in self._selected_grant_fields)
                    child_iid = tree.insert(root_iid, "end", text=display, values=values)
                    grant_entry = row.copy()
                    grant_entry["_root_iid"] = root_iid
                    self._tree_row_data[child_iid] = grant_entry

        self._update_send_menu_state()

    def _row_to_entity(self, row):
        """Convert a results row to standard entity dictionary format."""
        company_number = str(row.get(self.company_num_col, "")).strip() if self.company_num_col else ""
        charity_number = str(row.get(self.charity_num_col, "")).strip() if self.charity_num_col else ""
        entity_type = "company" if company_number else "charity"
        number = company_number if company_number else charity_number

        name = ""
        for key in ("entity_name", "name", "company_name", "charity_name"):
            if row.get(key):
                name = str(row.get(key)).strip()
                break

        if not name:
            prefix = "Company" if entity_type == "company" else "Charity"
            name = f"{prefix} {number}" if number else "Unknown Entity"

        return {"name": name, "company_number": number, "entity_type": entity_type}

    def _resolve_selected_root_entities(self):
        """Resolve current tree selection to unique root entities."""
        selection = self.results_tree.selection()
        if not selection:
            messagebox.showinfo("No Selection", "Please select one or more entities first.")
            return []

        resolved = []
        seen = set()
        for iid in selection:
            row = self._tree_row_data.get(iid, {})
            if row.get("_is_placeholder"):
                root_iid = row.get("_root_iid")
            elif iid in self._tree_root_entities:
                root_iid = iid
            elif self._results_view_mode == "funder" and row.get("_recipient_entity"):
                entity = row.get("_recipient_entity")
                key = (entity.get("name", ""), entity.get("company_number", ""), entity.get("entity_type", ""))
                if key in seen:
                    continue
                seen.add(key)
                resolved.append(dict(entity))
                continue
            else:
                root_iid = row.get("_root_iid") or self.results_tree.parent(iid)
            entity = self._tree_root_entities.get(root_iid)
            if not entity:
                continue
            if entity.get("entity_type") == "funder":
                continue
            key = (entity.get("name", ""), entity.get("company_number", ""), entity.get("entity_type", ""))
            if key in seen:
                continue
            seen.add(key)
            resolved.append(dict(entity))
        return resolved

    def _on_tree_selection_changed(self, event=None):
        self._update_send_menu_state()

    def _update_send_menu_state(self):
        """Enable/disable send menu options based on selected items."""
        selection = self.results_tree.selection()
        has_selection = bool(selection)
        has_company = False
        has_charity = False
        for entity in self._resolve_entities_for_state():
            if entity.get("entity_type") == "company":
                has_company = True
            if entity.get("entity_type") == "charity":
                has_charity = True
        self._send_menu.entryconfigure(0, state="normal" if has_selection else "disabled")
        self._send_menu.entryconfigure(1, state="normal" if has_selection else "disabled")
        self._send_menu.entryconfigure(
            2,
            state="normal" if has_selection and has_company else "disabled",
        )
        self._send_menu.entryconfigure(3, state="normal" if has_selection else "disabled")
        self._send_menu_btn.configure(text="Send to… ▼")

    def _resolve_entities_for_state(self):
        """Resolve selected entities without UI prompts, used for state changes."""
        selection = self.results_tree.selection()
        if not selection:
            return []
        resolved = []
        seen = set()
        for iid in selection:
            row = self._tree_row_data.get(iid, {})
            if row.get("_is_placeholder"):
                root_iid = row.get("_root_iid")
            elif iid in self._tree_root_entities:
                root_iid = iid
            elif self._results_view_mode == "funder" and row.get("_recipient_entity"):
                entity = row.get("_recipient_entity")
                key = (entity.get("name", ""), entity.get("company_number", ""), entity.get("entity_type", ""))
                if key not in seen:
                    seen.add(key)
                    resolved.append(dict(entity))
                continue
            else:
                root_iid = row.get("_root_iid") or self.results_tree.parent(iid)
            entity = self._tree_root_entities.get(root_iid)
            if not entity:
                continue
            if entity.get("entity_type") == "funder":
                continue
            key = (entity.get("name", ""), entity.get("company_number", ""), entity.get("entity_type", ""))
            if key not in seen:
                seen.add(key)
                resolved.append(dict(entity))
        return resolved

    def _send_to_working_set(self):
        entities = self._resolve_selected_root_entities()
        if not entities:
            return
        if self.app_state.ubo_working_set is None:
            self.app_state.ubo_working_set = []
        existing = {
            (e.get("name", ""), e.get("company_number", ""), e.get("entity_type", ""))
            for e in self.app_state.ubo_working_set
        }
        added = 0
        for ent in entities:
            key = (ent.get("name", ""), ent.get("company_number", ""), ent.get("entity_type", ""))
            if key not in existing:
                self.app_state.ubo_working_set.append(ent)
                existing.add(key)
                added += 1
        self.app._refresh_working_set_indicator()
        try:
            self.app._refresh_home_working_set()
        except Exception:
            pass
        if added:
            messagebox.showinfo("Working Set", f"Added {added} entities to working set.")

    def _send_to_ubo_tracer(self):
        entities = self._resolve_selected_root_entities()
        if not entities:
            return
        companies = [e for e in entities if e.get("entity_type") == "company"]
        charities = [e for e in entities if e.get("entity_type") == "charity"]

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
            self.after(
                0,
                lambda: self.app.show_ubo_investigation(
                    prefill_company=c["company_number"],
                    prefill_company_name=c["name"],
                ),
            )
        else:
            self.after(0, lambda: self.app.show_ubo_investigation(prefill_entities=companies))

    def _send_to_edd(self):
        entities = self._resolve_selected_root_entities()
        if not entities:
            return
        payload = []
        for ent in entities:
            etype = ent.get("entity_type", "company")
            if etype == "funder":
                continue
            dd_type = "charity" if etype == "charity" else "company"
            eid = str(ent.get("company_number", "")).strip()
            if not eid:
                continue
            payload.append({"type": dd_type, "id": eid})
        if not payload:
            messagebox.showinfo("EDD", "No compatible companies or charities were selected.")
            return
        self.after(0, lambda: self.app.show_enhanced_dd(prefill_entities=payload))

    def _send_to_bulk_entity_search(self):
        entities = self._resolve_selected_root_entities()
        if not entities:
            return
        self.after(0, lambda: self.app.show_unified_search(prefill_entities=entities))

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

        c_combo.bind("<<ComboboxSelected>>", self._auto_confirm_columns)
        ch_combo.bind("<<ComboboxSelected>>", self._auto_confirm_columns)

        self._auto_map_columns()
        self._auto_confirm_columns()

        self.app.after(1, self._update_scrollregion)

    def _auto_map_columns(self):
        """Try to auto-detect identifier columns from header names."""
        headers = getattr(self, "original_headers", [])
        if not headers:
            return
        headers_lower = {h: h.lower() for h in headers}

        company_keywords = [
            "company_number", "company number", "company no", "company_no",
            "registration number", "crn", "comp no",
        ]
        charity_keywords = [
            "charity_number", "charity number", "charity no", "charity_no",
            "registered charity number", "reg_charity_number",
        ]

        for keyword in company_keywords:
            for header, lower in headers_lower.items():
                if keyword == lower or keyword in lower:
                    self.company_num_col_var.set(header)
                    break
            if self.company_num_col_var.get():
                break

        for keyword in charity_keywords:
            for header, lower in headers_lower.items():
                if keyword == lower or keyword in lower:
                    if header != self.company_num_col_var.get():
                        self.charity_num_col_var.set(header)
                        break
            if self.charity_num_col_var.get():
                break

    def _auto_confirm_columns(self, event=None):
        self.company_num_col = self.company_num_col_var.get()
        self.charity_num_col = self.charity_num_col_var.get()

        # Handle the 'None' case
        if self.company_num_col == "___NONE___":
            self.company_num_col = None
        if self.charity_num_col == "___NONE___":
            self.charity_num_col = None

        if self.company_num_col or self.charity_num_col:
            self.run_btn.config(state="normal")
        else:
            self.run_btn.config(state="disabled")

    def start_investigation(self):
        if not self.company_num_col and not self.charity_num_col:
            messagebox.showerror(
                "Selection Error",
                "You must map a Company Number or Charity Number column.",
            )
            return
        self.cancel_flag.clear()
        self.run_btn.pack_forget()
        self.cancel_btn.pack(side=tk.LEFT, padx=5)
        self.export_btn.config(state="disabled")
        self.progress_bar["value"] = 0
        self.results_data = []
        self.results_tree.delete(*self.results_tree.get_children())
        self._tree_row_data.clear()
        self._tree_root_entities.clear()
        self._update_send_menu_state()
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
        self.safe_ui_call(self.status_var.set, f"Processing {total} rows...")
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
                        self.safe_ui_call(self.status_entity_var.set, "")
                        self.safe_ui_call(self.status_var.set, "Investigation cancelled.")
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
                    self.safe_ui_call(update_progress)

            except Exception as e:
                log_message(f"An error occurred during grant investigation: {e}")
                self.safe_ui_call(
                    messagebox.showerror, "Error", f"A processing error occurred: {e}"
                )

        self.safe_ui_call(self.status_entity_var.set, "")
        if not self.cancel_flag.is_set():
            self.safe_ui_call(self.status_var.set, "Investigation complete!")
            self.safe_ui_call(self._populate_results_tree)
            self.safe_ui_call(self.notebook.select, self.results_tab)

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
                self._update_send_menu_state()

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
