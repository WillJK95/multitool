# multitool/ui/connection_matrix.py
"""Connection matrix window and drill-down detail view."""

import csv
import tkinter as tk
from collections import defaultdict
from tkinter import ttk, filedialog, messagebox

from .tooltip import Tooltip

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CELL_WIDTH = 70
CELL_HEIGHT = 28
ROW_HEADER_WIDTH = 200
COL_HEADER_HEIGHT = 90
SYMBOL_DIRECT = "\u25cf"   # ●
SYMBOL_INDIRECT = "\u25cb"  # ○


def _theme_colors():
    """Query current ttkbootstrap theme for foreground / background."""
    style = ttk.Style()
    bg = style.lookup("TFrame", "background") or "#FFFFFF"
    fg = style.lookup("TLabel", "foreground") or "#000000"
    return bg, fg


def _is_dark_bg(hex_color):
    """Return True if *hex_color* is perceptually dark."""
    h = hex_color.lstrip("#")
    if len(h) != 6:
        return False
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    luminance = 0.299 * r + 0.587 * g + 0.114 * b
    return luminance < 128


def _derived_colors(bg):
    """Return grid, alt-row, summary, and self-bg colours derived from *bg*."""
    dark = _is_dark_bg(bg)
    if dark:
        return {
            "grid": "#555555",
            "alt_row": "#2A2A2A",
            "summary": "#1E3A5F",
            "self_bg": "#3A3A3A",
        }
    return {
        "grid": "#CCCCCC",
        "alt_row": "#F5F5F5",
        "summary": "#E8F0FE",
        "self_bg": "#D0D0D0",
    }


def _pick_best_path(paths):
    """Return the best path: prefer direct (all-explicit) then shortest."""
    if not paths:
        return None
    direct = [p for p in paths if p["is_direct"]]
    pool = direct if direct else paths
    return min(pool, key=lambda p: p["hops"])


def _structural_key(path_info):
    """Build a grouping key collapsing person nodes to a placeholder."""
    parts = []
    for nid, ntype in zip(path_info["node_ids"], path_info["node_types"]):
        if ntype == "person":
            parts.append(("person", None))
        else:
            parts.append((ntype, nid))
    return tuple(parts)


# ===================================================================
# ConnectionMatrixWindow
# ===================================================================
class ConnectionMatrixWindow:
    """Non-modal Toplevel displaying a connection matrix."""

    def __init__(self, parent, connection_results, row_entities,
                 col_entities, is_within_mode):
        """
        Parameters
        ----------
        parent : tk.Tk or tk.Toplevel
        connection_results : dict[(row_id, col_id)] -> list[path_dict]
        row_entities : list[dict] with keys id, label, type
        col_entities : list[dict]
        is_within_mode : bool
        """
        self.parent = parent
        self.connection_results = connection_results
        self.all_row_entities = list(row_entities)
        self.all_col_entities = list(col_entities)
        self.is_within_mode = is_within_mode

        # Active (possibly filtered) entity lists
        self.row_entities = list(self.all_row_entities)
        self.col_entities = list(self.all_col_entities)

        self._filter_active = False
        self._redraw_after_id = None
        self._drilldown_window = None

        # Pre-compute summary counts (against full entity lists)
        self._row_counts = {}  # row_id -> int
        self._col_counts = {}  # col_id -> int
        self._compute_summary_counts()

        self._build_window()

    # ------------------------------------------------------------------
    # Window construction
    # ------------------------------------------------------------------
    def _build_window(self):
        # Resolve theme colours
        self._bg, self._fg = _theme_colors()
        self._colors = _derived_colors(self._bg)
        self._last_theme = ttk.Style().theme_use()

        self.win = tk.Toplevel(self.parent)
        self.win.title("Connection Matrix")
        n_rows = len(self.all_row_entities)
        n_cols = len(self.all_col_entities)
        width = min(1200, ROW_HEADER_WIDTH + n_cols * CELL_WIDTH + 80)
        height = min(750, COL_HEADER_HEIGHT + n_rows * CELL_HEIGHT + 120)
        self.win.geometry(f"{max(width, 640)}x{max(height, 400)}")
        self.win.minsize(640, 400)

        # --- Toolbar ---
        toolbar = ttk.Frame(self.win)
        toolbar.pack(fill=tk.X, padx=8, pady=(8, 4))

        self._filter_btn = ttk.Button(
            toolbar, text="Show Only Connected",
            command=self._toggle_filter,
        )
        self._filter_btn.pack(side=tk.LEFT, padx=(0, 8))

        ttk.Button(
            toolbar, text="Export to CSV\u2026",
            command=self._export_csv,
        ).pack(side=tk.LEFT, padx=(0, 8))

        connected_pairs = sum(
            1 for v in self.connection_results.values() if v
        )
        self._summary_label = ttk.Label(
            toolbar,
            text=f"{connected_pairs} connected pair(s) found",
            foreground="gray",
        )
        self._summary_label.pack(side=tk.LEFT, padx=(8, 0))

        ttk.Button(
            toolbar, text="Close", command=self.win.destroy,
        ).pack(side=tk.RIGHT)

        # --- Legend ---
        legend = ttk.Frame(self.win)
        legend.pack(fill=tk.X, padx=8, pady=(0, 4))
        ttk.Label(legend, text=f"{SYMBOL_DIRECT} = direct (all explicit edges)", foreground="gray").pack(side=tk.LEFT, padx=(0, 16))
        ttk.Label(legend, text=f"{SYMBOL_INDIRECT} = indirect (uses inferred edge)", foreground="gray").pack(side=tk.LEFT, padx=(0, 16))
        ttk.Label(legend, text="X = self", foreground="gray").pack(side=tk.LEFT, padx=(0, 16))
        ttk.Label(legend, text="(n) = hop count", foreground="gray").pack(side=tk.LEFT)

        # --- Matrix frame (4-quadrant canvas layout) ---
        matrix_frame = ttk.Frame(self.win)
        matrix_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))
        matrix_frame.rowconfigure(1, weight=1)
        matrix_frame.columnconfigure(1, weight=1)

        bg = self._bg

        # Corner
        self.corner_canvas = tk.Canvas(
            matrix_frame, width=ROW_HEADER_WIDTH,
            height=COL_HEADER_HEIGHT, highlightthickness=0, bg=bg,
        )
        self.corner_canvas.grid(row=0, column=0, sticky="nsew")

        # Column headers
        self.col_header_canvas = tk.Canvas(
            matrix_frame, height=COL_HEADER_HEIGHT,
            highlightthickness=0, bg=bg,
        )
        self.col_header_canvas.grid(row=0, column=1, sticky="nsew")

        # Row headers
        self.row_header_canvas = tk.Canvas(
            matrix_frame, width=ROW_HEADER_WIDTH,
            highlightthickness=0, bg=bg,
        )
        self.row_header_canvas.grid(row=1, column=0, sticky="nsew")

        # Body
        self.body_canvas = tk.Canvas(
            matrix_frame, highlightthickness=0, bg=bg,
        )
        self.body_canvas.grid(row=1, column=1, sticky="nsew")

        # Scrollbars
        self.v_scroll = ttk.Scrollbar(
            matrix_frame, orient=tk.VERTICAL, command=self._on_yscroll,
        )
        self.v_scroll.grid(row=1, column=2, sticky="ns")

        self.h_scroll = ttk.Scrollbar(
            matrix_frame, orient=tk.HORIZONTAL, command=self._on_xscroll,
        )
        self.h_scroll.grid(row=2, column=1, sticky="ew")

        self.body_canvas.configure(
            xscrollcommand=self._sync_h_scroll,
            yscrollcommand=self._sync_v_scroll,
        )

        # --- Status bar ---
        ttk.Label(
            self.win,
            text="Click a connection cell to see path details",
            foreground="gray",
        ).pack(fill=tk.X, padx=8, pady=(0, 6))

        # --- Bindings ---
        self.body_canvas.bind("<Button-1>", self._on_body_click)
        self.body_canvas.bind("<Configure>", lambda e: self._schedule_redraw())
        # Mousewheel
        for canvas in (self.body_canvas, self.row_header_canvas,
                       self.col_header_canvas):
            canvas.bind("<MouseWheel>", self._on_mousewheel)
            canvas.bind("<Button-4>", self._on_mousewheel)
            canvas.bind("<Button-5>", self._on_mousewheel)

        # Initial draw
        self._update_scroll_region()
        self.win.after(50, self._redraw_all)

        # Poll for theme changes so colours update on toggle
        self._theme_poll_id = self.win.after(500, self._poll_theme)

    def _poll_theme(self):
        """Check if the ttkbootstrap theme changed and refresh colours."""
        try:
            current = ttk.Style().theme_use()
        except tk.TclError:
            return  # window gone
        if current != self._last_theme:
            self._last_theme = current
            self._refresh_theme()
        try:
            self._theme_poll_id = self.win.after(500, self._poll_theme)
        except tk.TclError:
            pass  # window destroyed

    def _refresh_theme(self):
        """Re-resolve theme colours and repaint everything."""
        self._bg, self._fg = _theme_colors()
        self._colors = _derived_colors(self._bg)
        bg = self._bg
        for canvas in (self.corner_canvas, self.col_header_canvas,
                       self.row_header_canvas, self.body_canvas):
            canvas.configure(bg=bg)
        self._redraw_all()

    # ------------------------------------------------------------------
    # Scroll helpers
    # ------------------------------------------------------------------
    def _on_xscroll(self, *args):
        self.body_canvas.xview(*args)
        self.col_header_canvas.xview(*args)

    def _on_yscroll(self, *args):
        self.body_canvas.yview(*args)
        self.row_header_canvas.yview(*args)

    def _sync_h_scroll(self, first, last):
        self.h_scroll.set(first, last)
        self.col_header_canvas.xview_moveto(first)
        self._schedule_redraw()

    def _sync_v_scroll(self, first, last):
        self.v_scroll.set(first, last)
        self.row_header_canvas.yview_moveto(first)
        self._schedule_redraw()

    def _on_mousewheel(self, event):
        if event.num == 4 or (hasattr(event, "delta") and event.delta > 0):
            delta = -3
        elif event.num == 5 or (hasattr(event, "delta") and event.delta < 0):
            delta = 3
        else:
            delta = -int(event.delta / 120) * 3
        self.body_canvas.yview_scroll(delta, "units")
        self.row_header_canvas.yview_scroll(delta, "units")
        self._schedule_redraw()

    # ------------------------------------------------------------------
    # Virtual-scroll redraw
    # ------------------------------------------------------------------
    def _update_scroll_region(self):
        n_rows = len(self.row_entities) + 1  # +1 for summary row
        n_cols = len(self.col_entities) + 1  # +1 for summary col
        body_w = n_cols * CELL_WIDTH
        body_h = n_rows * CELL_HEIGHT
        self.body_canvas.configure(scrollregion=(0, 0, body_w, body_h))
        self.col_header_canvas.configure(
            scrollregion=(0, 0, body_w, COL_HEADER_HEIGHT))
        self.row_header_canvas.configure(
            scrollregion=(0, 0, ROW_HEADER_WIDTH, body_h))

    def _schedule_redraw(self):
        if self._redraw_after_id:
            self.win.after_cancel(self._redraw_after_id)
        self._redraw_after_id = self.win.after(16, self._redraw_all)

    def _get_visible_range(self):
        x1 = self.body_canvas.canvasx(0)
        y1 = self.body_canvas.canvasy(0)
        x2 = self.body_canvas.canvasx(self.body_canvas.winfo_width())
        y2 = self.body_canvas.canvasy(self.body_canvas.winfo_height())
        n_rows = len(self.row_entities) + 1
        n_cols = len(self.col_entities) + 1
        first_col = max(0, int(x1 // CELL_WIDTH))
        last_col = min(n_cols, int(x2 // CELL_WIDTH) + 1)
        first_row = max(0, int(y1 // CELL_HEIGHT))
        last_row = min(n_rows, int(y2 // CELL_HEIGHT) + 1)
        return first_row, last_row, first_col, last_col

    def _redraw_all(self):
        self._redraw_after_id = None
        self._redraw_body()
        self._redraw_row_headers()
        self._redraw_col_headers()
        self._redraw_corner()

    def _redraw_corner(self):
        c = self.corner_canvas
        c.delete("all")
        c.create_text(
            ROW_HEADER_WIDTH // 2, COL_HEADER_HEIGHT // 2,
            text="Entity", font=("", 9, "bold"), anchor="center",
            fill=self._fg,
        )

    def _redraw_col_headers(self):
        c = self.col_header_canvas
        c.delete("all")
        first_row, last_row, first_col, last_col = self._get_visible_range()
        n_ent = len(self.col_entities)
        fg = self._fg
        grid = self._colors["grid"]

        for ci in range(first_col, last_col):
            x = ci * CELL_WIDTH
            x_mid = x + CELL_WIDTH // 2
            if ci < n_ent:
                ent = self.col_entities[ci]
                label = ent["label"]
                # Horizontal wrapped text — width constrains to cell
                c.create_text(
                    x_mid, COL_HEADER_HEIGHT - 4,
                    text=label, anchor="s", width=CELL_WIDTH - 4,
                    font=("", 7), fill=fg, justify=tk.CENTER,
                )
            else:
                c.create_text(
                    x_mid, COL_HEADER_HEIGHT - 4,
                    text="Total", anchor="s",
                    font=("", 7, "bold"), fill=fg,
                )
            # Column gridline
            c.create_line(x, 0, x, COL_HEADER_HEIGHT, fill=grid)

    def _redraw_row_headers(self):
        c = self.row_header_canvas
        c.delete("all")
        first_row, last_row, first_col, last_col = self._get_visible_range()
        n_ent = len(self.row_entities)
        fg = self._fg
        grid = self._colors["grid"]

        for ri in range(first_row, last_row):
            y = ri * CELL_HEIGHT
            y_mid = y + CELL_HEIGHT // 2
            if ri < n_ent:
                ent = self.row_entities[ri]
                label = ent["label"]
                if len(label) > 28:
                    label = label[:26] + "\u2026"
                c.create_text(
                    ROW_HEADER_WIDTH - 6, y_mid,
                    text=label, anchor="e", font=("", 8), fill=fg,
                )
            else:
                c.create_text(
                    ROW_HEADER_WIDTH - 6, y_mid,
                    text="Total", anchor="e", font=("", 8, "bold"),
                    fill=fg,
                )
            # Row gridline
            c.create_line(0, y, ROW_HEADER_WIDTH, y, fill=grid)

    def _redraw_body(self):
        c = self.body_canvas
        c.delete("all")
        first_row, last_row, first_col, last_col = self._get_visible_range()
        n_row_ent = len(self.row_entities)
        n_col_ent = len(self.col_entities)

        base_bg = self._bg
        fg = self._fg
        grid = self._colors["grid"]
        alt_bg = self._colors["alt_row"]
        summary_bg = self._colors["summary"]
        self_bg = self._colors["self_bg"]

        for ri in range(first_row, last_row):
            y = ri * CELL_HEIGHT
            for ci in range(first_col, last_col):
                x = ci * CELL_WIDTH
                is_summary_row = ri >= n_row_ent
                is_summary_col = ci >= n_col_ent

                # Background
                bg = base_bg
                if is_summary_row or is_summary_col:
                    bg = summary_bg
                elif ri % 2 == 1:
                    bg = alt_bg

                text, color = self._get_cell_content(
                    ri, ci, n_row_ent, n_col_ent, fg)

                if text == "X":
                    bg = self_bg

                c.create_rectangle(
                    x, y, x + CELL_WIDTH, y + CELL_HEIGHT,
                    fill=bg, outline=grid,
                )
                if text:
                    c.create_text(
                        x + CELL_WIDTH // 2, y + CELL_HEIGHT // 2,
                        text=text, fill=color, font=("", 9),
                        anchor="center",
                    )

    def _get_cell_content(self, ri, ci, n_row_ent, n_col_ent, fg):
        """Return (display_text, color) for the cell at visual index (ri, ci)."""
        is_summary_row = ri >= n_row_ent
        is_summary_col = ci >= n_col_ent

        if is_summary_row and is_summary_col:
            # Bottom-right corner: intentionally blank
            return ("", fg)

        if is_summary_row:
            col_id = self.col_entities[ci]["id"]
            return (str(self._col_counts.get(col_id, 0)), fg)

        if is_summary_col:
            row_id = self.row_entities[ri]["id"]
            return (str(self._row_counts.get(row_id, 0)), fg)

        row_id = self.row_entities[ri]["id"]
        col_id = self.col_entities[ci]["id"]

        if row_id == col_id:
            return ("X", "#888888")

        paths = self._lookup_paths(row_id, col_id)
        if not paths:
            return ("", fg)

        best = _pick_best_path(paths)
        symbol = SYMBOL_DIRECT if best["is_direct"] else SYMBOL_INDIRECT
        return (f"{symbol}({best['hops']})", fg)

    def _lookup_paths(self, row_id, col_id):
        """Look up paths for a pair, checking both orderings for within-mode."""
        paths = self.connection_results.get((row_id, col_id))
        if paths:
            return paths
        if self.is_within_mode:
            paths = self.connection_results.get((col_id, row_id))
            if paths:
                return paths
        return None

    # ------------------------------------------------------------------
    # Summary counts
    # ------------------------------------------------------------------
    def _compute_summary_counts(self):
        """Compute connection count per entity across active entity lists."""
        self._row_counts = {}
        self._col_counts = {}

        for rent in self.row_entities:
            rid = rent["id"]
            count = 0
            for cent in self.col_entities:
                cid = cent["id"]
                if rid == cid:
                    continue
                if self._lookup_paths(rid, cid):
                    count += 1
            self._row_counts[rid] = count

        for cent in self.col_entities:
            cid = cent["id"]
            count = 0
            for rent in self.row_entities:
                rid = rent["id"]
                if cid == rid:
                    continue
                if self._lookup_paths(rid, cid):
                    count += 1
            self._col_counts[cid] = count

    # ------------------------------------------------------------------
    # Filter
    # ------------------------------------------------------------------
    def _toggle_filter(self):
        self._filter_active = not self._filter_active
        if self._filter_active:
            self._filter_btn.configure(text="Show All")
            connected_row_ids = set()
            connected_col_ids = set()
            for ent in self.all_row_entities:
                for oent in self.all_col_entities:
                    if ent["id"] == oent["id"]:
                        continue
                    if self._lookup_paths_full(ent["id"], oent["id"]):
                        connected_row_ids.add(ent["id"])
                        connected_col_ids.add(oent["id"])
            self.row_entities = [
                e for e in self.all_row_entities
                if e["id"] in connected_row_ids
            ]
            self.col_entities = [
                e for e in self.all_col_entities
                if e["id"] in connected_col_ids
            ]
        else:
            self._filter_btn.configure(text="Show Only Connected")
            self.row_entities = list(self.all_row_entities)
            self.col_entities = list(self.all_col_entities)

        self._compute_summary_counts()
        self._update_scroll_region()
        self._redraw_all()

    def _lookup_paths_full(self, id_a, id_b):
        """Look up paths using full results dict (ignoring current filter)."""
        paths = self.connection_results.get((id_a, id_b))
        if paths:
            return paths
        if self.is_within_mode:
            paths = self.connection_results.get((id_b, id_a))
            if paths:
                return paths
        return None

    # ------------------------------------------------------------------
    # Click handling
    # ------------------------------------------------------------------
    def _on_body_click(self, event):
        cx = self.body_canvas.canvasx(event.x)
        cy = self.body_canvas.canvasy(event.y)
        ci = int(cx // CELL_WIDTH)
        ri = int(cy // CELL_HEIGHT)
        n_row_ent = len(self.row_entities)
        n_col_ent = len(self.col_entities)

        if ri >= n_row_ent or ci >= n_col_ent:
            return  # clicked summary area
        if ri < 0 or ci < 0:
            return

        row_id = self.row_entities[ri]["id"]
        col_id = self.col_entities[ci]["id"]
        if row_id == col_id:
            return

        paths = self._lookup_paths(row_id, col_id)
        if not paths:
            return

        row_label = self.row_entities[ri]["label"]
        col_label = self.col_entities[ci]["label"]
        self._open_drilldown(row_label, col_label, paths)

    def _open_drilldown(self, row_label, col_label, paths):
        if self._drilldown_window is not None:
            try:
                self._drilldown_window.win.destroy()
            except tk.TclError:
                pass
        self._drilldown_window = ConnectionDrilldownWindow(
            self.win, row_label, col_label, paths)

    # ------------------------------------------------------------------
    # CSV Export
    # ------------------------------------------------------------------
    def _export_csv(self):
        filepath = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            title="Export Connection Matrix",
        )
        if not filepath:
            return

        try:
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "Entity A", "Entity A Type",
                    "Entity B", "Entity B Type",
                    "Hops", "Connection Type", "Path",
                ])
                for (src, tgt), paths in self.connection_results.items():
                    if not paths:
                        continue
                    best = _pick_best_path(paths)
                    conn_type = "Direct" if best["is_direct"] else "Indirect"
                    path_str = " -> ".join(best["node_labels"])
                    # Find entity metadata
                    src_type = ""
                    tgt_type = ""
                    src_label = src
                    tgt_label = tgt
                    for e in self.all_row_entities:
                        if e["id"] == src:
                            src_label = e["label"]
                            src_type = e["type"]
                            break
                    for e in self.all_col_entities:
                        if e["id"] == tgt:
                            tgt_label = e["label"]
                            tgt_type = e["type"]
                            break
                    writer.writerow([
                        src_label, src_type,
                        tgt_label, tgt_type,
                        best["hops"], conn_type, path_str,
                    ])
            messagebox.showinfo(
                "Export Complete",
                f"Matrix data exported to:\n{filepath}",
            )
        except Exception as e:
            messagebox.showerror("Export Error", f"Could not export: {e}")

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def destroy(self):
        try:
            self.win.destroy()
        except tk.TclError:
            pass


# ===================================================================
# ConnectionDrilldownWindow
# ===================================================================
class ConnectionDrilldownWindow:
    """Shows grouped path details between two entities."""

    def __init__(self, parent, entity_a_label, entity_b_label, paths):
        self.paths = paths

        self.win = tk.Toplevel(parent)
        self.win.title(f"Connections: {entity_a_label} \u2194 {entity_b_label}")
        self.win.geometry("750x500")
        self.win.minsize(500, 300)

        # Header
        hdr = ttk.Frame(self.win, padding=10)
        hdr.pack(fill=tk.X)
        ttk.Label(
            hdr,
            text=f"{entity_a_label}  \u2194  {entity_b_label}",
            font=("", 11, "bold"),
        ).pack(anchor="w")
        ttk.Label(
            hdr,
            text=f"{len(paths)} path(s) found. "
                 "Expand a group to see specific routes.",
            foreground="gray",
        ).pack(anchor="w", pady=(4, 0))

        ttk.Separator(self.win).pack(fill=tk.X)

        # Treeview for grouped paths
        tree_frame = ttk.Frame(self.win)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=8)
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        self.tree = ttk.Treeview(
            tree_frame,
            columns=("detail",),
            show="tree",
            selectmode="browse",
        )
        self.tree.column("#0", width=700, stretch=True)
        self.tree.column("detail", width=0, stretch=False)

        yscroll = ttk.Scrollbar(
            tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=yscroll.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")

        # Button bar
        btn_frame = ttk.Frame(self.win, padding=(10, 0, 10, 10))
        btn_frame.pack(fill=tk.X)
        ttk.Button(
            btn_frame, text="Close", command=self.win.destroy,
        ).pack(side=tk.RIGHT)

        # Populate
        self._populate_tree()

    def _populate_tree(self):
        # Group paths by structural key
        groups = defaultdict(list)
        for path in self.paths:
            key = _structural_key(path)
            groups[key].append(path)

        # Sort groups: shortest hop count first
        sorted_groups = sorted(
            groups.items(),
            key=lambda item: min(p["hops"] for p in item[1]),
        )

        for g_idx, (key, group_paths) in enumerate(sorted_groups):
            min_hops = min(p["hops"] for p in group_paths)
            is_shortest = g_idx == 0

            # Build structural summary line
            summary = self._build_group_summary(key, group_paths, min_hops)
            section_label = (
                "Shortest connection" if is_shortest
                else "Alternative connection"
            )

            # Section header
            section_id = self.tree.insert(
                "", "end",
                text=f"{section_label}: {summary}",
                open=is_shortest,
            )

            # Determine if this is a simple person-collapsed group
            # where we just list the people
            person_slots = [
                i for i, (ntype, _) in enumerate(key)
                if ntype == "person"
            ]

            if len(person_slots) == 1 and len(key) <= 3:
                # Simple case: Entity A -> [persons] -> Entity B
                seen = set()
                for p in sorted(group_paths, key=lambda x: x["hops"]):
                    slot_idx = person_slots[0]
                    person_label = p["node_labels"][slot_idx]
                    person_id = p["node_ids"][slot_idx]
                    if person_id not in seen:
                        seen.add(person_id)
                        edge_desc = ""
                        if slot_idx > 0 and slot_idx - 1 < len(p["edge_types"]):
                            edge_desc = f"  [{p['edge_types'][slot_idx - 1]}]"
                        self.tree.insert(
                            section_id, "end",
                            text=f"    {person_label}{edge_desc}",
                        )
            else:
                # Complex case: show each route vertically
                for p_idx, p in enumerate(
                    sorted(group_paths, key=lambda x: x["hops"])
                ):
                    route_id = self.tree.insert(
                        section_id, "end",
                        text=f"    Route {p_idx + 1}  ({p['hops']} hop{'s' if p['hops'] != 1 else ''})",
                        open=is_shortest and p_idx == 0,
                    )
                    # One child row per hop in the path
                    for i, label in enumerate(p["node_labels"]):
                        node_type = p["node_types"][i].title() if i < len(p["node_types"]) else ""
                        if i < len(p["edge_types"]):
                            edge = p["edge_types"][i]
                            line = f"{label}  ({node_type})  \u2500\u2500 {edge} \u2500\u2500\u25b8"
                        else:
                            line = f"{label}  ({node_type})"
                        self.tree.insert(
                            route_id, "end",
                            text=f"        {line}",
                        )

    def _build_group_summary(self, key, group_paths, min_hops):
        """Build a human-readable summary for a structural group."""
        parts = []
        # key entries map 1:1 to path node positions
        for pos, (ktype, kid) in enumerate(key):
            if ktype == "person":
                unique_persons = {}
                for p in group_paths:
                    if pos < len(p["node_ids"]):
                        unique_persons[p["node_ids"][pos]] = p["node_labels"][pos]
                n = len(unique_persons)
                if n == 1:
                    parts.append(next(iter(unique_persons.values())))
                else:
                    parts.append(f"[{n} person(s)]")
            else:
                label = kid
                for p in group_paths:
                    if pos < len(p["node_ids"]):
                        label = p["node_labels"][pos]
                        break
                parts.append(label)

        hops_str = f"({min_hops} hop{'s' if min_hops != 1 else ''})"
        all_direct = all(p["is_direct"] for p in group_paths)
        type_marker = SYMBOL_DIRECT if all_direct else SYMBOL_INDIRECT
        arrow = " \u2192 "
        return f"{type_marker} {arrow.join(parts)} {hops_str}"
