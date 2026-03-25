# modules/ubo_tracer.py
"""UBO module"""

import os
import re
import threading
import time
import tkinter as tk
from datetime import datetime
from tkinter import ttk, filedialog, messagebox
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- From Our Package ---
# API functions (were global functions in original file)
from ..api.companies_house import ch_get_data

# Constants (were at top of original file)
from ..constants import (
    CONFIG_DIR,
)

# Utility functions (were global functions or duplicated in classes)
from ..utils.helpers import log_message, get_canonical_name_key, format_error_summary, format_eta

# UI components (were classes in original file)
from ..ui.tooltip import Tooltip

from .base import InvestigationModuleBase

class UltimateBeneficialOwnershipTracer(InvestigationModuleBase):
    def __init__(self, parent_app, api_key, back_callback, ch_token_bucket,
                 prefill_company=None, prefill_company_name=None):
        super().__init__(parent_app, back_callback, api_key, help_key="ubo")
        self.ch_token_bucket = ch_token_bucket
        self._prefill_company = prefill_company
        self._prefill_company_name = prefill_company_name

        # --- Notebook with Configuration and Results tabs ---
        self.notebook = ttk.Notebook(self.content_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        self.config_tab = ttk.Frame(self.notebook)
        self.results_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.config_tab, text="Configuration")
        self.notebook.add(self.results_tab, text="Results")
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

        # --- UI Setup (Configuration tab) ---
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
            self.config_tab, text="Step 2: Select Columns", padding=10
        )
        self.column_selection_frame.pack(fill=tk.X, pady=5, padx=10)

        run_frame = ttk.LabelFrame(
            self.config_tab, text="Step 3: Run Investigation", padding=10
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

        self.progress_bar = ttk.Progressbar(
            run_frame, orient="horizontal", length=300, mode="determinate"
        )
        self.progress_bar.pack(pady=10)
        self.status_entity_var = tk.StringVar(value="")
        ttk.Label(run_frame, textvariable=self.status_entity_var).pack(anchor=tk.W)
        self.status_var = tk.StringVar(value="Ready.")
        ttk.Label(run_frame, textvariable=self.status_var).pack(anchor=tk.W)

        # Apply prefill from Quick Launch
        if self._prefill_company:
            row_data = {"company_number": self._prefill_company}
            headers = ["company_number"]
            if self._prefill_company_name:
                row_data["company_name"] = self._prefill_company_name
                headers.append("company_name")
            self.original_data = [row_data]
            self.original_headers = headers
            display_name = self._prefill_company_name or self._prefill_company
            self.file_status_label.config(
                text=f"Quick Launch: {display_name}", foreground="green"
            )
            self._display_column_selection_ui()
            self.number_col_var.set("company_number")
            if self._prefill_company_name:
                self.name_col_var.set("company_name")
                self.name_col = "company_name"
            else:
                self.name_col_var.set("")
                self.name_col = None
            self.number_col = "company_number"
            self.run_btn.config(state="normal")

        # Build Results tab skeleton
        self._build_results_tab()

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
        """Build the static skeleton of the Results tab (called once from __init__)."""
        # --- Shared Ownership section (collapsible) ---
        self._shared_frame = ttk.LabelFrame(
            self.results_tab, text="Shared Ownership", padding=8
        )
        # Not packed initially — shown only after trace completion
        self._shared_expanded = True
        toggle_row = ttk.Frame(self._shared_frame)
        toggle_row.pack(fill=tk.X)
        self._shared_toggle_btn = ttk.Label(
            toggle_row, text="\u25BC  Hide", cursor="hand2",
            font=("Segoe UI", 8), foreground="gray",
        )
        self._shared_toggle_btn.pack(anchor=tk.E)
        self._shared_toggle_btn.bind("<Button-1>", lambda e: self._toggle_shared_section())
        self._shared_inner = ttk.Frame(self._shared_frame)
        self._shared_inner.pack(fill=tk.X)

        # --- Top bar: Select All / Deselect All ---
        top_bar = ttk.Frame(self.results_tab)
        top_bar.pack(fill=tk.X, padx=10, pady=(5, 5))
        ttk.Button(
            top_bar, text="Select All", command=self._select_all_results
        ).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(
            top_bar, text="Deselect All", command=self._deselect_all_results
        ).pack(side=tk.LEFT, padx=(0, 15))

        # --- Hierarchical Treeview ---
        tree_frame = ttk.Frame(self.results_tab)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        self.results_tree = ttk.Treeview(
            tree_frame,
            columns=("kind", "nationality", "shareholding"),
            show="tree headings",
            selectmode="extended",
            height=22,
        )
        self.results_tree.heading("#0", text="Entity", anchor=tk.W)
        self.results_tree.heading("kind", text="Kind", anchor=tk.W)
        self.results_tree.heading("nationality", text="Nationality", anchor=tk.W)
        self.results_tree.heading("shareholding", text="Shareholding", anchor=tk.W)
        self.results_tree.column("#0", width=350, minwidth=200)
        self.results_tree.column("kind", width=200, minwidth=100)
        self.results_tree.column("nationality", width=120, minwidth=80)
        self.results_tree.column("shareholding", width=280, minwidth=150)

        yscroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.results_tree.yview)
        xscroll = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=self.results_tree.xview)
        self.results_tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        self.results_tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")

        # Treeview data tracking
        self._tree_node_map = {}   # company_number -> treeview iid
        self._tree_row_data = {}   # iid -> result row dict

        # Bind selection change for Send to… menu state
        self.results_tree.bind("<<TreeviewSelect>>", self._on_tree_selection_changed)

        # --- Progress area for graph-data retrieval (hidden by default) ---
        self._results_progress_frame = ttk.Frame(self.results_tab)
        self._results_progress_var = tk.StringVar(value="")
        ttk.Label(
            self._results_progress_frame, textvariable=self._results_progress_var
        ).pack(anchor=tk.W, padx=10)

        # --- Bottom bar: Export + Generate Graph + Send to ---
        bottom_bar = ttk.Frame(self.results_tab)
        bottom_bar.pack(fill=tk.X, padx=10, pady=(5, 10))

        self.export_btn = ttk.Button(
            bottom_bar, text="Export Results", command=self.export_csv
        )
        self.export_btn.pack(side=tk.LEFT, padx=(0, 5))
        Tooltip(self.export_btn, "Export a flat list of all PSCs found in the ownership chain.")

        self.generate_graph_btn = ttk.Button(
            bottom_bar, text="Generate Visual Graph",
            command=self.start_visual_graph_generation
        )
        self.generate_graph_btn.pack(side=tk.LEFT, padx=(0, 10))
        Tooltip(self.generate_graph_btn, "Generate a visual graph of the PSC ownership chain.")

        # "Send to… ▼" dropdown
        self._send_menu_btn = ttk.Menubutton(
            bottom_bar, text="Send to\u2026 \u25BC", bootstyle="primary-outline"
        )
        self._send_menu = tk.Menu(self._send_menu_btn, tearoff=0)
        self._send_menu.add_command(label="Working Set", command=self._send_to_working_set)
        self._send_menu.add_command(label="Network Analytics Workbench", command=self._send_to_network_analytics)
        self._send_menu.add_command(label="Enhanced Due Diligence", command=self._send_to_edd)
        self._send_menu.add_command(label="Grants Search", command=self._send_to_grants_search)
        self._send_menu.add_command(label="Director Search", command=self._send_to_director_search)
        self._send_menu_btn.configure(menu=self._send_menu)
        self._send_menu_btn.pack(side=tk.LEFT, padx=(0, 5))
        Tooltip(
            self._send_menu_btn,
            "Triggers additional API calls to retrieve director, address and\n"
            "ownership data. This may take several minutes for large result sets.",
        )

        # Initial menu state — no results yet, all disabled except WS + NA
        self._update_send_menu_state()

    def _populate_results_tree(self):
        """Populate the hierarchical treeview from self.results_data."""
        tree = self.results_tree
        tree.delete(*tree.get_children())
        self._tree_node_map.clear()
        self._tree_row_data.clear()

        if not self.results_data:
            return

        # Group results by root_company, preserving insertion order
        from collections import OrderedDict
        grouped = OrderedDict()
        for row in self.results_data:
            rc = row.get("root_company", "")
            if rc not in grouped:
                grouped[rc] = []
            grouped[rc].append(row)

        for root_cnum, rows in grouped.items():
            # Determine root company display name
            root_name = ""
            for r in rows:
                if r.get("root_company_name"):
                    root_name = r["root_company_name"]
                    break
            display = f"{root_name} ({root_cnum})" if root_name else root_cnum

            # Insert root company node
            root_iid = tree.insert(
                "", "end", text=display,
                values=("Root Company", "", ""),
                open=True,
            )
            self._tree_node_map[root_cnum] = root_iid
            # Store a synthetic row for root company nodes
            self._tree_row_data[root_iid] = {
                "root_company": root_cnum,
                "root_company_name": root_name,
                "psc_name": root_name or root_cnum,
                "psc_company_number": root_cnum,
                "psc_kind": "root-company",
                "psc_unique_id": root_cnum,
                "country": "",
                "shareholding": "",
                "_is_root": True,
            }

            # Insert PSC rows as children, nested by level
            for row in rows:
                if "psc_unique_id" not in row:
                    continue  # Skip placeholder rows

                parent_cnum = row.get("parent_company_number", root_cnum)
                parent_iid = self._tree_node_map.get(parent_cnum, root_iid)

                psc_name = row.get("psc_name", "Unknown")
                kind = row.get("psc_kind", "")
                country = row.get("country", "")
                shareholding = row.get("shareholding", "")

                child_iid = tree.insert(
                    parent_iid, "end", text=psc_name,
                    values=(kind, country, shareholding),
                    open=True,
                )
                self._tree_row_data[child_iid] = row

                # If corporate PSC, register in node map for deeper nesting
                psc_cnum = row.get("psc_company_number", "")
                if psc_cnum and psc_cnum not in self._tree_node_map:
                    self._tree_node_map[psc_cnum] = child_iid

        # Build shared ownership analysis
        self._build_shared_ownership()

    # ------------------------------------------------------------------
    # Select All / Deselect All
    # ------------------------------------------------------------------

    def _select_all_results(self):
        """Select all items in the results treeview (recursively)."""
        all_items = []
        def _collect(parent=""):
            for child in self.results_tree.get_children(parent):
                all_items.append(child)
                _collect(child)
        _collect()
        if all_items:
            self.results_tree.selection_set(all_items)

    def _deselect_all_results(self):
        """Clear selection in the results treeview."""
        self.results_tree.selection_remove(self.results_tree.selection())

    # ------------------------------------------------------------------
    # Tree selection → Send-to menu state
    # ------------------------------------------------------------------

    def _on_tree_selection_changed(self, event=None):
        """Update Send to… menu enable/disable state based on current selection."""
        self._update_send_menu_state()

    def _classify_selection(self):
        """Return (entities, has_companies, has_persons, count) for current selection."""
        sel = self.results_tree.selection()
        entities = []
        has_companies = False
        has_persons = False
        for iid in sel:
            row = self._tree_row_data.get(iid)
            if not row:
                continue
            entities.append(row)
            kind = row.get("psc_kind", "")
            if row.get("_is_root") or row.get("psc_company_number") or "corporate" in kind:
                has_companies = True
            elif "individual" in kind:
                has_persons = True
            else:
                # Default: if has company number treat as company, else person
                if row.get("psc_company_number"):
                    has_companies = True
                else:
                    has_persons = True
        return entities, has_companies, has_persons, len(entities)

    def _update_send_menu_state(self):
        """Enable/disable Send to… menu items based on selection."""
        entities, has_companies, has_persons, count = self._classify_selection()
        menu = self._send_menu

        # Default: Working Set + Network Analytics always enabled
        ws_state = "normal"
        na_state = "normal"
        edd_state = "disabled"
        grants_state = "disabled"
        director_state = "disabled"

        if count == 0:
            # No selection = "all" → only WS + NA
            pass
        elif has_companies and has_persons:
            # Mixed → only WS + NA
            pass
        elif has_companies and not has_persons:
            if count == 1:
                edd_state = "normal"
                grants_state = "normal"
            else:
                grants_state = "normal"
        elif has_persons and not has_companies:
            if count == 1:
                director_state = "normal"

        menu.entryconfigure(0, state=ws_state)       # Working Set
        menu.entryconfigure(1, state=na_state)        # Network Analytics
        menu.entryconfigure(2, state=edd_state)       # EDD
        menu.entryconfigure(3, state=grants_state)    # Grants Search
        menu.entryconfigure(4, state=director_state)  # Director Search

    # ------------------------------------------------------------------
    # Shared Ownership analysis
    # ------------------------------------------------------------------

    def _build_shared_ownership(self):
        """Populate the Shared Ownership collapsible section after trace completion."""
        from ..utils.fuzzy_match import normalize_person_name

        # Clear previous content
        for w in self._shared_inner.winfo_children():
            w.destroy()

        if not self.results_data:
            self._shared_frame.pack_forget()
            return

        # --- Group corporate PSCs by company number ---
        corp_groups = {}   # psc_company_number → {name, set of root_companies}
        # --- Group individual PSCs by normalised name + DOB ---
        person_groups = {}  # group_key → {display_name, set of root_companies}

        for row in self.results_data:
            if "psc_unique_id" not in row:
                continue
            root = row.get("root_company", "")
            kind = row.get("psc_kind", "")
            psc_cnum = row.get("psc_company_number", "")
            psc_name = row.get("psc_name", "")
            uid = row.get("psc_unique_id", "")

            if psc_cnum and "corporate" in kind:
                if psc_cnum not in corp_groups:
                    corp_groups[psc_cnum] = {"name": psc_name, "roots": set()}
                corp_groups[psc_cnum]["roots"].add(root)
            elif "individual" in kind or (not psc_cnum and psc_name):
                # Group key: normalised name + DOB portion of unique_id
                norm_name = normalize_person_name(psc_name)
                # Extract DOB from unique_id (format: "namename-YYYY-MM")
                dob_part = ""
                if uid:
                    parts = uid.rsplit("-", 2)
                    if len(parts) == 3 and parts[1].isdigit() and parts[2].isdigit():
                        dob_part = f"{parts[1]}-{parts[2]}"
                group_key = f"{norm_name}|{dob_part}"
                if group_key not in person_groups:
                    person_groups[group_key] = {"name": psc_name, "roots": set()}
                person_groups[group_key]["roots"].add(root)

        # Filter to only shared (2+ root companies)
        shared_corps = {k: v for k, v in corp_groups.items() if len(v["roots"]) >= 2}
        shared_persons = {k: v for k, v in person_groups.items() if len(v["roots"]) >= 2}

        # Count root companies with/without shared ownership
        all_roots = {row.get("root_company", "") for row in self.results_data if row.get("root_company")}
        roots_with_shared = set()
        for v in shared_corps.values():
            roots_with_shared.update(v["roots"])
        for v in shared_persons.values():
            roots_with_shared.update(v["roots"])
        roots_without = all_roots - roots_with_shared

        has_shared = bool(shared_corps or shared_persons)

        # Show/hide frame
        self._shared_frame.pack(fill=tk.X, padx=10, pady=(10, 5))

        inner = self._shared_inner

        if not has_shared:
            ttk.Label(
                inner, text="No shared ownership identified.",
                font=("Segoe UI", 9, "italic"), foreground="gray",
            ).pack(anchor=tk.W, pady=2)
            # Summary
            ttk.Label(
                inner,
                text=f"0 root companies share at least one owner\n"
                     f"{len(all_roots)} root companies have no identified shared ownership",
                font=("Segoe UI", 9),
            ).pack(anchor=tk.W, pady=(5, 0))
            return

        ttk.Label(
            inner, text="Shared owners identified:",
            font=("Segoe UI", 9, "bold"),
        ).pack(anchor=tk.W, pady=(0, 4))

        # --- Corporate section ---
        if shared_corps:
            ttk.Label(
                inner, text="\u2500\u2500 Corporate " + "\u2500" * 40,
                font=("Segoe UI", 9), foreground="gray",
            ).pack(anchor=tk.W)

            for cnum, info in sorted(shared_corps.items(), key=lambda x: -len(x[1]["roots"])):
                count = len(info["roots"])
                text = f'{info["name"]} ({cnum})     controls {count} root companies'
                lbl = ttk.Label(
                    inner, text=text, font=("Segoe UI", 9),
                    cursor="hand2", foreground="#0066cc",
                )
                lbl.pack(anchor=tk.W, padx=(10, 0))
                lbl.bind("<Button-1>", lambda e, cn=cnum: self._scroll_to_shared_entity(
                    "corporate", cn))
                lbl.bind("<Enter>", lambda e, l=lbl: l.configure(font=("Segoe UI", 9, "underline")))
                lbl.bind("<Leave>", lambda e, l=lbl: l.configure(font=("Segoe UI", 9)))

        # --- Individual section ---
        if shared_persons:
            ind_header = ttk.Label(
                inner, text="\u2500\u2500 Individual (name-match only) " + "\u2500" * 25,
                font=("Segoe UI", 9), foreground="gray",
            )
            ind_header.pack(anchor=tk.W, pady=(4, 0))
            Tooltip(
                ind_header,
                "Name-based matching only. For accurate entity resolution,\n"
                "send to Network Analytics Workbench.",
            )

            for key, info in sorted(shared_persons.items(), key=lambda x: -len(x[1]["roots"])):
                count = len(info["roots"])
                text = f'{info["name"]}     appears in {count} root companies  \u24d8'
                lbl = ttk.Label(
                    inner, text=text, font=("Segoe UI", 9),
                    cursor="hand2", foreground="#0066cc",
                )
                lbl.pack(anchor=tk.W, padx=(10, 0))
                lbl.bind("<Button-1>", lambda e, k=key: self._scroll_to_shared_entity(
                    "person", k))
                lbl.bind("<Enter>", lambda e, l=lbl: l.configure(font=("Segoe UI", 9, "underline")))
                lbl.bind("<Leave>", lambda e, l=lbl: l.configure(font=("Segoe UI", 9)))
                Tooltip(
                    lbl,
                    "Name-based matching only. For accurate entity resolution,\n"
                    "send to Network Analytics Workbench.",
                )

        # --- Summary stats ---
        ttk.Separator(inner, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=(6, 4))
        ttk.Label(
            inner,
            text=f"{len(roots_with_shared)} root companies share at least one owner\n"
                 f"{len(roots_without)} root companies have no identified shared ownership",
            font=("Segoe UI", 9),
        ).pack(anchor=tk.W)

    def _scroll_to_shared_entity(self, entity_type, key):
        """Scroll treeview to first instance of a shared entity, highlight all instances."""
        from ..utils.fuzzy_match import normalize_person_name
        matching_iids = []
        for iid, row in self._tree_row_data.items():
            if entity_type == "corporate":
                if row.get("psc_company_number") == key:
                    matching_iids.append(iid)
            elif entity_type == "person":
                psc_name = row.get("psc_name", "")
                uid = row.get("psc_unique_id", "")
                norm = normalize_person_name(psc_name)
                dob_part = ""
                if uid:
                    parts = uid.rsplit("-", 2)
                    if len(parts) == 3 and parts[1].isdigit() and parts[2].isdigit():
                        dob_part = f"{parts[1]}-{parts[2]}"
                if f"{norm}|{dob_part}" == key:
                    matching_iids.append(iid)

        if matching_iids:
            self.results_tree.selection_set(matching_iids)
            self.results_tree.see(matching_iids[0])

    def _toggle_shared_section(self):
        """Collapse or expand the Shared Ownership inner content."""
        if self._shared_expanded:
            self._shared_inner.pack_forget()
            self._shared_toggle_btn.configure(text="\u25B6  Show")
            self._shared_expanded = False
        else:
            self._shared_inner.pack(fill=tk.X)
            self._shared_toggle_btn.configure(text="\u25BC  Hide")
            self._shared_expanded = True

    # ------------------------------------------------------------------
    # Send-to stubs (filled in Chunk 4)
    # ------------------------------------------------------------------

    def _get_selected_entities(self):
        """Return list of row dicts for selected items, or all if none selected."""
        sel = self.results_tree.selection()
        if not sel:
            # No selection → return all non-placeholder entities
            return [row for row in self._tree_row_data.values()
                    if "psc_unique_id" in row or row.get("_is_root")]
        return [self._tree_row_data[iid] for iid in sel
                if iid in self._tree_row_data]

    def _entity_to_ws_dict(self, row):
        """Convert a results/tree row to a working-set entity dict."""
        kind = row.get("psc_kind", "")
        is_root = row.get("_is_root", False)
        psc_cnum = row.get("psc_company_number", "")

        if is_root or psc_cnum or "corporate" in kind:
            # Company entity
            num = psc_cnum or row.get("root_company", "")
            name = row.get("psc_name", "") or row.get("root_company_name", "") or num
            return {
                "name": name,
                "company_number": num,
                "active": True,
                "entity_type": "company",
            }
        else:
            # Person entity — use month/year of birth from unique_id if available
            name = row.get("psc_name", "")
            uid = row.get("psc_unique_id", "")
            # unique_id format: "namename-YYYY-MM" → extract "MM/YYYY"
            dob_str = ""
            if uid:
                parts = uid.rsplit("-", 2)
                if len(parts) == 3:
                    try:
                        year, month = parts[1], parts[2]
                        if year.isdigit() and month.isdigit():
                            dob_str = f"{month}/{year}"
                    except (ValueError, IndexError):
                        pass
                if not dob_str:
                    dob_str = uid  # Fallback to unique_id itself
            return {
                "name": name,
                "company_number": dob_str,
                "active": True,
                "entity_type": "person",
            }

    def _send_to_working_set(self):
        """Append selected entities to the global working set."""
        entities = self._get_selected_entities()
        if not entities:
            return
        if self.app_state.ubo_working_set is None:
            self.app_state.ubo_working_set = []
        added = 0
        for row in entities:
            ws_dict = self._entity_to_ws_dict(row)
            self.app_state.ubo_working_set.append(ws_dict)
            added += 1
        self.app._refresh_working_set_indicator()
        try:
            self.app._refresh_home_working_set()
        except Exception:
            pass
        messagebox.showinfo("Working Set", f"Added {added} entities to working set.")

    def _send_to_network_analytics(self):
        """Build graph data for selected entities and navigate to Network Analytics."""
        entities = self._get_selected_entities()
        if not entities:
            return

        ws_entities = [self._entity_to_ws_dict(row) for row in entities]
        entity_count = len(ws_entities)

        # Show progress on results tab
        self._results_progress_frame.pack(fill=tk.X, padx=10, pady=5)
        self._results_progress_var.set("Building network data...")

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
                        source_label=f"Working set: {entity_count} entities from UBO Tracer",
                    )
                else:
                    messagebox.showwarning("Network Analytics", "No graph data could be generated.")

            self.app.after(0, _navigate)

        threading.Thread(target=_build, daemon=True).start()

    def _send_to_edd(self):
        """Send single selected company to Enhanced Due Diligence."""
        entities = self._get_selected_entities()
        if not entities:
            return
        row = entities[0]
        cnum = row.get("psc_company_number", "") or row.get("root_company", "")
        if not cnum:
            messagebox.showwarning("EDD", "No company number found for this entity.")
            return
        self.app.show_enhanced_dd(prefill_entity={"type": "company", "id": cnum})

    def _send_to_grants_search(self):
        """Send selected companies to Grants Search."""
        entities = self._get_selected_entities()
        if not entities:
            return
        ws_entities = []
        skipped_persons = 0
        for row in entities:
            ws = self._entity_to_ws_dict(row)
            if ws["entity_type"] == "person":
                skipped_persons += 1
                continue
            ws_entities.append(ws)
        if skipped_persons:
            messagebox.showinfo(
                "Grants Search",
                f"{skipped_persons} person entit{'y' if skipped_persons == 1 else 'ies'} "
                "skipped — only companies and charities are compatible with Grants Search.",
            )
        if not ws_entities:
            return
        self.app.show_grants_investigation(prefill_entities=ws_entities)

    def _send_to_director_search(self):
        """Send single selected person to Director Search."""
        entities = self._get_selected_entities()
        if not entities:
            return
        row = entities[0]
        name = row.get("psc_name", "")
        if not name:
            messagebox.showwarning("Director Search", "No person name found.")
            return
        self.app.show_director_investigation(prefill_name=name)

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
        self.progress_bar["value"] = 0
        self.results_data = []
        # Clear previous results from treeview
        if hasattr(self, 'results_tree'):
            self.results_tree.delete(*self.results_tree.get_children())
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

    def _start_graph_process(self, target_thread_function):
        """Generic starter for any graph-related process."""
        if not self.results_data:
            messagebox.showinfo(
                "No Data", "Please run an investigation before generating graph data."
            )
            return

        self.run_btn.config(state="disabled")
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

    def _finish_investigation(self):
        try:
            self.cancel_btn.pack_forget()
            self.run_btn.pack(side=tk.LEFT, padx=5)

            if self.results_data:
                self._populate_results_tree()
                if not self.cancel_flag.is_set():
                    self.notebook.select(self.results_tab)
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

        self.run_btn.config(state="normal")

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
