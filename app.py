# multitool/app.py
"""
Main Application Class

This module contains the main App class that manages the application window,
navigation, API keys, and module loading.
"""

import os
import re
import subprocess
import sys
import threading
import tkinter as tk
from tkinter import ttk, messagebox
import tkinter.font
import ttkbootstrap as tb
import keyring
import requests
import webbrowser
from datetime import datetime

from .constants import (
    CONFIG_DIR,
    SERVICE_NAME,
    CH_ACCOUNT_NAME,
    CC_ACCOUNT_NAME,
    API_BASE_URL,
    CHARITY_API_BASE_URL,
    GRANTNAV_API_BASE_URL,
    DEFAULT_CH_PACING_MODE,
    DEFAULT_CH_MAX_WORKERS,
    MAX_CH_MAX_WORKERS,
    MIN_CH_MAX_WORKERS,
)
from .help_content import HELP_CONTENT
from .utils.helpers import log_message, clean_company_number
from .utils.token_bucket import TokenBucket
from .utils.settings import load_settings, save_settings, derive_initial_params, load_recent_reports
from .utils.app_state import AppState
from .ui.tooltip import Tooltip
from .ui.help_window import HelpWindow
from .api.companies_house import ch_get_company, ch_search_companies, ch_get_data
from .api.charity_commission import (
    cc_get_data as _cc_get_data,
    cc_search_charities, cc_get_charity_details, cc_search_charity_by_name,
)
from rapidfuzz.fuzz import WRatio


class App(tk.Tk):
    """
    Main application class.
    
    Manages the application window, menu bar, API key storage,
    and navigation between investigation modules.
    
    Attributes:
        api_key: Companies House API key
        charity_api_key: Charity Commission API key
        ch_token_bucket: Rate limiter for Companies House API
        container: Main content container frame
    """
    
    def __init__(self):
        """Initialize the application."""
        super().__init__()

        # Global exception handler for tkinter callbacks/after() —
        # without this, unhandled errors silently kill the app.
        self.report_callback_exception = self._on_tk_error

        self.title("Multi-Tool")
        self.geometry("1320x780")
        self.minsize(1320, 780)
        # Open maximised — try platform-appropriate methods
        try:
            self.state('zoomed')
        except tk.TclError:
            try:
                self.attributes('-zoomed', True)
            except tk.TclError:
                pass
        
        # Load persisted settings
        self._settings = load_settings()

        # Initialize ttkbootstrap style with persisted theme
        initial_theme = "superhero" if self._settings["dark_theme"] else "litera"
        self.style = tb.Style(theme=initial_theme)
        self.themed_window = self.style.master

        # Appearance settings
        self.dark_theme_enabled = tk.BooleanVar(value=self._settings["dark_theme"])
        self.font_size = tk.IntVar(value=self._settings["font_size"])

        # Apply persisted font size if non-default
        if self._settings["font_size"] != 10:
            self.after(100, self._update_font_size)

        # Rate limiter for Companies House API (auto-detected from headers)
        initial = derive_initial_params(self._settings["ch_pacing_mode"])
        self.ch_token_bucket = TokenBucket(
            capacity=initial["capacity"],
            refill_rate=initial["refill_rate"],
            pacing_mode=self._settings["ch_pacing_mode"],
        )
        self.ch_max_workers = self._settings["ch_max_workers"]
        
        # API keys
        self.api_key = ""
        self.charity_api_key = ""
        self.api_key_saved = False
        self.charity_api_key_saved = False
        
        # Persistent cross-module state (survives module navigation)
        self.app_state = AppState()
        self.app_state.recent_edd_reports = load_recent_reports()

        # API status cache
        self.api_statuses = None
        self.api_status_timestamp = None

        # API status panel widget references (for in-place updates)
        self.status_canvases = {}
        self.status_tooltips = {}
        self.status_timestamp_label = None

        # Main frame holding sidebar + content side-by-side
        self._main_frame = ttk.Frame(self)
        self._main_frame.pack(fill=tk.BOTH, expand=True)

        # Sidebar — fixed width, left, persistent (never destroyed)
        self.sidebar = ttk.Frame(self._main_frame, width=220, padding=(10, 10, 5, 10))
        self.sidebar.pack_propagate(False)

        # Vertical separator between sidebar and content
        self._sidebar_sep = ttk.Separator(self._main_frame, orient=tk.VERTICAL)

        # Content area — modules load here (this IS self.container)
        self.container = ttk.Frame(self._main_frame, padding=10)
        self.container.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Sidebar state tracking
        self._active_module_name = None
        self._sidebar_buttons = {}
        self._sidebar_default_styles = {}
        self._sidebar_visible = False
        self._working_set_label = None
        self._working_set_dropdown = None
        self._working_set_tree = None
        self._working_set_send_menu = None

        # Bind Return key to invoke buttons
        self.bind_class("TButton", "<Return>", lambda e: e.widget.invoke())

        log_message("Application started.")

        # Create menu bar
        self._create_menu_bar()

        # Create API status variables
        self.api_statuses = {}
        self.api_status_timestamp = None
        self.status_panel = None

        # Build sidebar (initially hidden until API keys are loaded)
        self._build_sidebar()

        # Load API keys and show appropriate screen
        self.load_api_keys()

    # ── Sidebar ──────────────────────────────────────────────────────

    def _build_sidebar(self) -> None:
        """Populate the persistent sidebar with navigation buttons."""
        sb = self.sidebar

        # App name
        ttk.Label(
            sb, text="Multi-Tool", font=("Helvetica", 14, "bold"),
            bootstyle="primary"
        ).pack(anchor="w", pady=(5, 8))

        ttk.Separator(sb, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=4)

        # Home button — distinctive
        self._add_sidebar_button(sb, "Home", "home", "info",
                                 self.show_main_menu)

        ttk.Separator(sb, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=4)

        # Enhanced Due Diligence — standalone, prominent
        self._add_sidebar_button(sb, "Enhanced Due Diligence", "edd", "info",
                                 self.show_enhanced_dd)

        ttk.Separator(sb, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=4)

        # Network Compatible section
        ttk.Label(
            sb, text="Network Compatible",
            font=("Segoe UI", 9, "italic"), foreground="gray"
        ).pack(anchor="w", pady=(6, 2))

        self._add_sidebar_button(sb, "Bulk Entity Search",
                                 "bulk_entity_search", "primary-outline",
                                 self.show_unified_search)
        self._add_sidebar_button(sb, "Director Search",
                                 "director_search", "primary-outline",
                                 self.show_director_investigation)
        self._add_sidebar_button(sb, "Contracts Finder",
                                 "contracts_finder", "primary-outline",
                                 self.show_contracts_finder)
        self._add_sidebar_button(sb, "UBO Tracer",
                                 "ubo_tracer", "primary-outline",
                                 self.show_ubo_investigation)

        # Standalone Tools section
        ttk.Label(
            sb, text="Standalone Tools",
            font=("Segoe UI", 9, "italic"), foreground="gray"
        ).pack(anchor="w", pady=(10, 2))

        self._add_sidebar_button(sb, "Grants Search",
                                 "grants_search", "secondary-outline",
                                 self.show_grants_investigation)
        self._add_sidebar_button(sb, "Data Match",
                                 "data_match", "secondary-outline",
                                 self.show_data_match_investigation)

        ttk.Separator(sb, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=(10, 4))

        # Network Analytics Workbench — green, at bottom
        self._add_sidebar_button(sb, "Network Analytics\n     Workbench",
                                 "network_workbench", "success",
                                 self.show_network_graph_creator)
        # Centre the two-line label; anchor must go through the style system
        self.style.configure("Centered.success.TButton", anchor="center")
        self._sidebar_buttons["network_workbench"].configure(
            style="Centered.success.TButton"
        )

        # Working Set indicator — below Network Workbench
        self._build_working_set_indicator(sb)

    def _add_sidebar_button(self, parent, text, key, bootstyle, command):
        """Add a single navigation button to the sidebar and register it."""
        btn = ttk.Button(
            parent, text=text, command=command,
            bootstyle=bootstyle, width=24
        )
        btn.pack(fill=tk.X, pady=2)
        self._sidebar_buttons[key] = btn
        self._sidebar_default_styles[key] = bootstyle

    def _show_sidebar(self) -> None:
        """Pack the sidebar and separator so they are visible."""
        if not self._sidebar_visible:
            self.sidebar.pack(side=tk.LEFT, fill=tk.Y, before=self.container)
            self._sidebar_sep.pack(side=tk.LEFT, fill=tk.Y, before=self.container)
            self._sidebar_visible = True
            self._update_sidebar_button_states()

    def _hide_sidebar(self) -> None:
        """Remove the sidebar from view (e.g. during API key prompt)."""
        if self._sidebar_visible:
            self.sidebar.pack_forget()
            self._sidebar_sep.pack_forget()
            self._sidebar_visible = False

    def _update_sidebar_active(self, module_name: str) -> None:
        """Highlight the active module button in the sidebar."""
        self._active_module_name = module_name
        for name, btn in self._sidebar_buttons.items():
            if name == module_name:
                base = self._sidebar_default_styles[name].replace("-outline", "")
                btn.configure(bootstyle=base)
            else:
                btn.configure(bootstyle=self._sidebar_default_styles[name])

    def _update_sidebar_button_states(self) -> None:
        """Enable/disable sidebar buttons based on available API keys."""
        ch = tk.NORMAL if self.api_key else tk.DISABLED
        unified = tk.NORMAL if (self.api_key or self.charity_api_key) else tk.DISABLED

        state_map = {
            "bulk_entity_search": unified,
            "director_search": ch,
            "contracts_finder": ch,
            "ubo_tracer": ch,
            "edd": ch,
        }
        for key, state in state_map.items():
            if key in self._sidebar_buttons:
                self._sidebar_buttons[key].configure(state=state)

    # ── Working Set Indicator ─────────────────────────────────────────

    def _build_working_set_indicator(self, parent) -> None:
        """Build the working-set indicator widget in the sidebar."""
        ws_frame = ttk.Frame(parent)
        ws_frame.pack(fill=tk.X, pady=(4, 6))

        # Summary label — clickable
        self._working_set_label = ttk.Label(
            ws_frame, text="No entities in working set",
            font=("Segoe UI", 8), foreground="gray", cursor="hand2"
        )
        self._working_set_label.pack(anchor="w")
        self._working_set_label.bind("<Button-1>", lambda e: self._toggle_working_set_dropdown())

        # Dropdown panel — initially hidden
        self._working_set_dropdown = ttk.Frame(ws_frame)

        # Treeview for entity list
        tree_frame = ttk.Frame(self._working_set_dropdown)
        tree_frame.pack(fill=tk.X)

        self._working_set_tree = ttk.Treeview(
            tree_frame, columns=("name", "number"), show="headings",
            height=8, selectmode="extended"
        )
        self._working_set_tree.heading(
            "name", text="Name",
            command=lambda: self._sort_ws_tree(self._working_set_tree, "name"))
        self._working_set_tree.heading(
            "number", text="Number",
            command=lambda: self._sort_ws_tree(self._working_set_tree, "number"))
        self._working_set_tree.column("name", width=120, minwidth=80)
        self._working_set_tree.column("number", width=70, minwidth=60)
        self._working_set_tree.pack(side=tk.LEFT, fill=tk.X, expand=True)

        tree_scroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL,
                                    command=self._working_set_tree.yview)
        tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self._working_set_tree.configure(yscrollcommand=tree_scroll.set)

        # Action buttons row
        btn_row = ttk.Frame(self._working_set_dropdown)
        btn_row.pack(fill=tk.X, pady=(4, 0))

        self._working_set_send_menu = ttk.Menubutton(
            btn_row, text="Send to\u2026 \u25bc", bootstyle="primary-outline"
        )
        self._ws_send_menu_obj = tk.Menu(self._working_set_send_menu, tearoff=0)
        # Index 0
        self._ws_send_menu_obj.add_command(
            label="Network Analytics Workbench",
            command=lambda: self._send_working_set_to_network(self._working_set_tree))
        # Index 1
        self._ws_send_menu_obj.add_command(
            label="Enhanced Due Diligence",
            command=lambda: self._send_ws_selection_to_edd())
        # Index 2
        self._ws_send_menu_obj.add_command(
            label="UBO Tracer",
            command=lambda: self._send_ws_to_ubo(self._working_set_tree))
        # Index 3
        self._ws_send_menu_obj.add_command(
            label="Bulk Entity Search",
            command=lambda: self._send_ws_to_bulk_search(self._working_set_tree))
        # Index 4
        self._ws_send_menu_obj.add_command(
            label="Grants Search",
            command=lambda: self._send_ws_to_grants(self._working_set_tree))
        # Index 5
        self._ws_send_menu_obj.add_command(
            label="Director Search",
            command=lambda: self._send_ws_to_director(self._working_set_tree))
        self._working_set_send_menu.configure(menu=self._ws_send_menu_obj)
        self._working_set_send_menu.pack(side=tk.LEFT, padx=(0, 4))

        # Bind selection change to update menu state
        self._working_set_tree.bind(
            "<<TreeviewSelect>>",
            lambda e: self._update_ws_send_menu_state(
                self._working_set_tree, self._ws_send_menu_obj)
        )
        # Click-to-deselect toggle
        self._working_set_tree.bind(
            "<Button-1>", lambda e: self._toggle_tree_selection(e, self._working_set_tree)
        )

        ttk.Button(
            btn_row, text="Clear", bootstyle="danger-outline",
            command=self._clear_working_set
        ).pack(side=tk.LEFT)

    def _toggle_working_set_dropdown(self) -> None:
        """Show or hide the working-set dropdown panel."""
        if self._working_set_dropdown is None:
            return
        if self._working_set_dropdown.winfo_manager():
            self._working_set_dropdown.pack_forget()
        else:
            self._working_set_dropdown.pack(fill=tk.X, pady=(4, 0))

    def _refresh_working_set_indicator(self) -> None:
        """Update the working-set label and treeview from app_state."""
        entities = self._collect_working_set_entities()
        count = len(entities)

        if count > 0:
            self._working_set_label.configure(
                text=f"Working set ({count})",
                foreground="", font=("Segoe UI", 8, "bold")
            )
        else:
            self._working_set_label.configure(
                text="No entities in working set",
                foreground="gray", font=("Segoe UI", 8)
            )

        # Refresh treeview
        if self._working_set_tree:
            self._working_set_tree.delete(
                *self._working_set_tree.get_children()
            )
            for ent in entities:
                self._working_set_tree.insert(
                    "", tk.END,
                    values=(ent.get("name", "Unknown"),
                            ent.get("company_number", ent.get("number", "")))
                )

    def _collect_working_set_entities(self):
        """Return a flat list of entity dicts from the working set."""
        entities = []
        ws = self.app_state.network_working_set
        if ws and isinstance(ws, dict):
            # network_working_set is graph-ready payload with a nodes list
            nodes = ws.get("nodes", [])
            if isinstance(nodes, list):
                entities.extend(nodes)
        elif ws and isinstance(ws, list):
            entities.extend(ws)

        if not entities and self.app_state.ubo_working_set:
            entities = list(self.app_state.ubo_working_set)

        return entities

    def _clear_working_set(self) -> None:
        """Clear the working set and refresh the indicator."""
        self.app_state.ubo_working_set = None
        self.app_state.network_working_set = None
        self.app_state.network_working_set_source = None
        self._refresh_working_set_indicator()
        # Collapse dropdown
        if (self._working_set_dropdown and
                self._working_set_dropdown.winfo_manager()):
            self._working_set_dropdown.pack_forget()

    def _create_menu_bar(self) -> None:
        """Create the application menu bar."""
        menu_bar = tk.Menu(self)
        self.config(menu=menu_bar)
        
        # File menu
        file_menu = tk.Menu(menu_bar, tearoff=0)
        menu_bar.add_cascade(label="File", menu=file_menu)
        
        file_menu.add_command(label="Settings...", command=self._open_settings_window)
        file_menu.add_separator()
        file_menu.add_command(label="Clear Cache & Logs", command=self.clear_cache_and_logs)
        file_menu.add_command(label="Open Config Folder", command=self.open_config_folder)
        file_menu.add_command(label="Manage API Keys", command=self.manage_api_keys)
        file_menu.add_separator()
        file_menu.add_command(label="View Licenses", command=self.show_licenses)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.destroy)
    
    def _open_settings_window(self) -> None:
        """Open the settings dialog."""
        settings_win = tk.Toplevel(self, padx=20, pady=20)
        settings_win.title("Settings")
        settings_win.transient(self)
        settings_win.grab_set()

        # ── Appearance ──────────────────────────────────────────
        theme_frame = ttk.LabelFrame(settings_win, text="Appearance", padding=10)
        theme_frame.pack(fill="x", expand=True)

        theme_toggle = ttk.Checkbutton(
            theme_frame,
            text="Colour Theme",
            variable=self.dark_theme_enabled,
            command=self._toggle_theme,
            bootstyle="round-toggle",
        )
        theme_toggle.pack(side="left", padx=5)

        font_slider_frame = ttk.Frame(theme_frame)
        font_slider_frame.pack(side="left", padx=20)
        ttk.Label(font_slider_frame, text="Text Size:").pack(side="left", padx=(0, 5))
        ttk.Scale(
            font_slider_frame,
            from_=8,
            to=16,
            variable=self.font_size,
            command=lambda val: self.font_size.set(round(float(val))),
        ).pack(side="left")

        # ── Companies House API ─────────────────────────────────
        api_frame = ttk.LabelFrame(
            settings_win, text="Companies House API", padding=10
        )
        api_frame.pack(fill="x", expand=True, pady=(10, 0))

        # Auto-detection note
        ttk.Label(
            api_frame,
            text="Rate limits are detected automatically from the API.",
            font=("Segoe UI", 9), foreground="gray",
        ).pack(anchor="w", pady=(0, 8))

        # Pacing strategy
        pacing_var = tk.StringVar(value=self._settings["ch_pacing_mode"])

        ttk.Label(api_frame, text="Pacing strategy:").pack(anchor="w")

        smooth_frame = ttk.Frame(api_frame)
        smooth_frame.pack(fill="x", padx=(10, 0), pady=(4, 0))
        ttk.Radiobutton(
            smooth_frame, text="Smooth (Recommended)",
            variable=pacing_var, value="smooth",
        ).pack(anchor="w")
        ttk.Label(
            smooth_frame,
            text=("Requests are spread evenly over time. Best for large "
                  "queries \u2014 avoids long idle periods between batches."),
            wraplength=420, foreground="gray", font=("Segoe UI", 8),
        ).pack(anchor="w", padx=(20, 0))

        burst_frame = ttk.Frame(api_frame)
        burst_frame.pack(fill="x", padx=(10, 0), pady=(4, 0))
        ttk.Radiobutton(
            burst_frame, text="Burst",
            variable=pacing_var, value="burst",
        ).pack(anchor="w")
        ttk.Label(
            burst_frame,
            text=("Sends requests as fast as allowed, then waits for the "
                  "rate limit to reset. Fine for small queries of a few "
                  "companies."),
            wraplength=420, foreground="gray", font=("Segoe UI", 8),
        ).pack(anchor="w", padx=(20, 0))

        # Advanced toggle (parallel workers only)
        show_advanced = tk.BooleanVar(value=False)
        advanced_frame = ttk.Frame(api_frame, padding=(15, 5, 0, 0))
        workers_var = tk.IntVar(value=self._settings["ch_max_workers"])

        def toggle_advanced():
            if show_advanced.get():
                advanced_frame.pack(fill="x", after=adv_toggle)
            else:
                advanced_frame.pack_forget()

        adv_toggle = ttk.Checkbutton(
            api_frame, text="Show advanced settings",
            variable=show_advanced, command=toggle_advanced,
            bootstyle="round-toggle",
        )
        adv_toggle.pack(anchor="w", pady=(8, 0))

        workers_row = ttk.Frame(advanced_frame)
        workers_row.pack(fill="x", pady=2)
        ttk.Label(workers_row, text="Parallel workers:").pack(side="left", padx=(0, 5))
        ttk.Spinbox(
            workers_row, from_=MIN_CH_MAX_WORKERS, to=MAX_CH_MAX_WORKERS,
            increment=1, textvariable=workers_var, width=6
        ).pack(side="left", padx=(0, 5))
        ttk.Label(
            workers_row, text="(concurrent API threads)",
            foreground="gray", font=("Segoe UI", 8)
        ).pack(side="left")

        # Effect summary
        summary_frame = ttk.LabelFrame(api_frame, text="Effect Summary", padding=8)
        summary_frame.pack(fill="x", pady=(10, 0))
        summary_label = ttk.Label(
            summary_frame, text="", wraplength=450, font=("Segoe UI", 9)
        )
        summary_label.pack(anchor="w")

        def update_summary(*_args):
            mode = pacing_var.get()
            try:
                workers = workers_var.get()
            except (tk.TclError, ValueError):
                workers = DEFAULT_CH_MAX_WORKERS

            if mode == "burst":
                desc = (
                    "All available tokens are used immediately. The app "
                    "waits when the limit is reached."
                )
            else:
                desc = (
                    "Requests flow at a steady rate with small bursts. "
                    "A 10% safety margin is applied automatically."
                )

            summary_label.config(
                text=(
                    f"{desc}\n"
                    f"Parallel threads: {workers}  |  "
                    f"Rate limit is auto-detected from the API."
                )
            )

        pacing_var.trace_add("write", update_summary)
        workers_var.trace_add("write", update_summary)
        show_advanced.trace_add("write", update_summary)
        update_summary()  # initial render

        # ── Buttons ─────────────────────────────────────────────
        button_frame = ttk.Frame(settings_win)
        button_frame.pack(pady=20)

        def apply_all():
            self._apply_settings(settings_win, pacing_var, workers_var)

        def reset_defaults():
            pacing_var.set(DEFAULT_CH_PACING_MODE)
            workers_var.set(DEFAULT_CH_MAX_WORKERS)

        ttk.Button(
            button_frame, text="Apply",
            command=apply_all, bootstyle="success",
        ).pack(side="left", padx=5)

        ttk.Button(
            button_frame, text="Reset to Defaults",
            command=reset_defaults, bootstyle="warning-outline",
        ).pack(side="left", padx=5)

        ttk.Button(
            button_frame, text="Close",
            command=settings_win.destroy, bootstyle="primary",
        ).pack(side="left", padx=5)

    def _apply_settings(self, settings_win, pacing_var, workers_var):
        """Validate and apply all settings from the dialog."""
        try:
            pacing_mode = pacing_var.get()
            workers = workers_var.get()
        except (tk.TclError, ValueError):
            messagebox.showerror("Error", "Please enter valid numbers.", parent=settings_win)
            return

        if pacing_mode not in ("smooth", "burst"):
            messagebox.showerror("Error", "Invalid pacing mode.", parent=settings_win)
            return
        if workers < MIN_CH_MAX_WORKERS or workers > MAX_CH_MAX_WORKERS:
            messagebox.showerror(
                "Error",
                f"Workers must be between {MIN_CH_MAX_WORKERS} and {MAX_CH_MAX_WORKERS}.",
                parent=settings_win,
            )
            return

        # Apply rate limiting changes
        self.ch_token_bucket.update_pacing_mode(pacing_mode)
        self.ch_max_workers = workers

        # Apply appearance
        self._update_font_size()

        # Persist everything
        self._settings = {
            "dark_theme": self.dark_theme_enabled.get(),
            "font_size": self.font_size.get(),
            "ch_pacing_mode": pacing_mode,
            "ch_max_workers": workers,
        }
        save_settings(self._settings)

        log_message(
            f"Settings applied: pacing_mode={pacing_mode}, workers={workers}"
        )
        messagebox.showinfo(
            "Settings Applied",
            "Your settings have been saved and will take effect immediately.",
            parent=settings_win,
        )
    
    def _toggle_theme(self) -> None:
        """Toggle between dark and light themes."""
        self.update_idletasks()
        theme = "superhero" if self.dark_theme_enabled.get() else "litera"
        
        try:
            self.style.theme_use(theme)
        except tk.TclError as e:
            log_message(f"Non-fatal TclError during theme change: {e}")
    
    def _update_font_size(self) -> None:
        """Update font size for all widgets."""
        new_size = self.font_size.get()
        
        default_font = tkinter.font.nametofont("TkDefaultFont")
        font_family = default_font.actual()["family"]
        new_font = (font_family, new_size)
        
        self.style.configure(".", font=new_font)
        
        named_fonts = [
            "TkDefaultFont", "TkTextFont", "TkFixedFont", "TkMenuFont",
            "TkHeadingFont", "TkCaptionFont", "TkTooltipFont",
        ]
        for font_name in named_fonts:
            tkinter.font.nametofont(font_name).configure(size=new_size)
    
    def load_api_keys(self) -> None:
        """Load API keys from secure credential store."""
        log_message("Loading API keys from secure credential store.")
        
        self.api_key = keyring.get_password(SERVICE_NAME, CH_ACCOUNT_NAME) or ""
        self.charity_api_key = keyring.get_password(SERVICE_NAME, CC_ACCOUNT_NAME) or ""
        
        if not self.api_key and not self.charity_api_key:
            log_message("No API keys found. Prompting user for first-time setup.")
            self.show_api_key_prompt()
        else:
            log_message(
                f"CH Key Loaded: {bool(self.api_key)}, "
                f"CC Key Loaded: {bool(self.charity_api_key)}"
            )
            self.show_main_menu()
    
    def manage_api_keys(self) -> None:
        """Open window to manage API keys."""
        self._update_sidebar_button_states()
        manager_window = tk.Toplevel(self)
        manager_window.title("Manage API Keys")
        manager_window.geometry("500x500")
        manager_window.transient(self)
        manager_window.grab_set()
        
        main_frame = ttk.Frame(manager_window, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(
            main_frame,
            text="Manage API Keys",
            font=("Helvetica", 14, "bold")
        ).pack(pady=(0, 20))
        
        # Companies House section
        ch_frame = ttk.LabelFrame(main_frame, text="Companies House API", padding=15)
        ch_frame.pack(fill=tk.X, pady=10)
        
        ch_status = "✓ Key stored" if self.api_key else "✗ No key stored"
        ch_status_color = "green" if self.api_key else "red"
        ttk.Label(ch_frame, text=ch_status, foreground=ch_status_color).pack(anchor="w")
        
        ch_entry = ttk.Entry(ch_frame, width=50, show="*")
        ch_entry.pack(fill=tk.X, pady=5)
        
        ch_btn_frame = ttk.Frame(ch_frame)
        ch_btn_frame.pack(fill=tk.X)
        
        def save_ch_key():
            key = ch_entry.get().strip()
            if key:
                keyring.set_password(SERVICE_NAME, CH_ACCOUNT_NAME, key)
                self.api_key = key
                messagebox.showinfo("Success", "Companies House API key saved.")
                manager_window.destroy()
                self.manage_api_keys()
        
        def delete_ch_key():
            if messagebox.askyesno("Confirm", "Delete Companies House API key?"):
                try:
                    keyring.delete_password(SERVICE_NAME, CH_ACCOUNT_NAME)
                except keyring.errors.PasswordDeleteError:
                    pass
                self.api_key = ""
                manager_window.destroy()
                self.manage_api_keys()
        
        ttk.Button(ch_btn_frame, text="Save Key", command=save_ch_key).pack(side=tk.LEFT, padx=5)
        if self.api_key:
            ttk.Button(ch_btn_frame, text="Delete Key", command=delete_ch_key, bootstyle="danger").pack(side=tk.LEFT)
        
        # Charity Commission section
        cc_frame = ttk.LabelFrame(main_frame, text="Charity Commission API", padding=15)
        cc_frame.pack(fill=tk.X, pady=10)
        
        cc_status = "✓ Key stored" if self.charity_api_key else "✗ No key stored"
        cc_status_color = "green" if self.charity_api_key else "red"
        ttk.Label(cc_frame, text=cc_status, foreground=cc_status_color).pack(anchor="w")
        
        cc_entry = ttk.Entry(cc_frame, width=50, show="*")
        cc_entry.pack(fill=tk.X, pady=5)
        
        cc_btn_frame = ttk.Frame(cc_frame)
        cc_btn_frame.pack(fill=tk.X)
        
        def save_cc_key():
            key = cc_entry.get().strip()
            if key:
                keyring.set_password(SERVICE_NAME, CC_ACCOUNT_NAME, key)
                self.charity_api_key = key
                messagebox.showinfo("Success", "Charity Commission API key saved.")
                manager_window.destroy()
                self.manage_api_keys()
        
        def delete_cc_key():
            if messagebox.askyesno("Confirm", "Delete Charity Commission API key?"):
                try:
                    keyring.delete_password(SERVICE_NAME, CC_ACCOUNT_NAME)
                except keyring.errors.PasswordDeleteError:
                    pass
                self.charity_api_key = ""
                manager_window.destroy()
                self.manage_api_keys()
        
        ttk.Button(cc_btn_frame, text="Save Key", command=save_cc_key).pack(side=tk.LEFT, padx=5)
        if self.charity_api_key:
            ttk.Button(cc_btn_frame, text="Delete Key", command=delete_cc_key, bootstyle="danger").pack(side=tk.LEFT)
        
        ttk.Button(main_frame, text="Close", command=manager_window.destroy).pack(pady=20)
    
    def _on_tk_error(self, exc_type, exc_value, exc_tb):
        """Global handler for unhandled tkinter callback exceptions."""
        import traceback
        traceback.print_exception(exc_type, exc_value, exc_tb)
        try:
            messagebox.showerror(
                "Error",
                f"An unexpected error occurred:\n{exc_value}")
        except Exception:
            pass

    def clear_container(self) -> None:
        """Clear all widgets from the container."""
        for widget in self.container.winfo_children():
            widget.destroy()
    
    def show_api_key_prompt(self) -> None:
        """Show the first-time API key setup screen."""
        self._hide_sidebar()
        self.clear_container()
        self.title("API Key Setup")
        
        frame = ttk.Frame(self.container)
        frame.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        
        ttk.Label(
            frame,
            text="Welcome to Multi-Tool",
            font=("Helvetica", 16, "bold")
        ).pack(pady=(0, 20))
        
        ttk.Label(
            frame,
            text="Please enter your API keys to get started.\n"
                 "At least one key is required.",
            justify="center"
        ).pack(pady=(0, 20))
        
        # Companies House
        ch_frame = ttk.LabelFrame(frame, text="Companies House API Key", padding=10)
        ch_frame.pack(fill=tk.X, pady=5)
        ch_entry = ttk.Entry(ch_frame, width=50, show="*")
        ch_entry.pack(pady=5)
        
        # Charity Commission
        cc_frame = ttk.LabelFrame(frame, text="Charity Commission API Key", padding=10)
        cc_frame.pack(fill=tk.X, pady=5)
        cc_entry = ttk.Entry(cc_frame, width=50, show="*")
        cc_entry.pack(pady=5)
        
        def save_and_continue():
            ch_key = ch_entry.get().strip()
            cc_key = cc_entry.get().strip()
            
            if not ch_key and not cc_key:
                messagebox.showerror("Error", "Please enter at least one API key.")
                return
            
            if ch_key:
                keyring.set_password(SERVICE_NAME, CH_ACCOUNT_NAME, ch_key)
                self.api_key = ch_key
            
            if cc_key:
                keyring.set_password(SERVICE_NAME, CC_ACCOUNT_NAME, cc_key)
                self.charity_api_key = cc_key
            
            log_message("API keys saved successfully.")
            self.show_main_menu()
        
        ttk.Button(
            frame,
            text="Save & Continue",
            command=save_and_continue,
            bootstyle="success"
        ).pack(pady=20)
        
        ttk.Button(
            frame,
            text="Help - How to get API keys",
            command=lambda: HelpWindow(self, "API Keys Help", HELP_CONTENT["api_keys"]),
            bootstyle="info-outline"
        ).pack()
    
    def show_main_menu(self) -> None:
        """Display the home screen with Quick Launch, status, and panels."""
        self.unbind("<Return>")
        self.clear_container()
        self.title("Multi-Tool - Dashboard")
        self._show_sidebar()

        # Footer first (pack at bottom so it stays pinned)
        footer_frame = ttk.Frame(self.container)
        footer_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=20, pady=(0, 10))
        ttk.Button(
            footer_frame, text="\U0001f4d6 Open User Guide",
            command=self.show_main_guide, bootstyle="link",
        ).pack(side=tk.LEFT, anchor="sw")

        # Main content — scrollable area for the three zones
        content = ttk.Frame(self.container)
        content.pack(fill=tk.BOTH, expand=True, padx=20, pady=(10, 0))

        # Zone 1 — Quick Launch (dominant)
        self._build_quick_launch_zone(content)

        # Zone 2 — System Status (compact)
        self._build_system_status_zone(content)

        # Zone 3 — Working Set + Recent Reports (lower)
        self._build_lower_panels_zone(content)

        # Sidebar state
        self._update_sidebar_active("home")
        self._refresh_working_set_indicator()

        # Trigger background API status check if no cache
        if not self.api_statuses:
            def run_check():
                self.check_api_status()
                self.after(0, self._update_home_status_display)
            threading.Thread(target=run_check, daemon=True).start()
        else:
            self._update_home_status_display()

    # ── Zone 1: Quick Launch ─────────────────────────────────────────

    def _build_quick_launch_zone(self, parent) -> None:
        """Build the Quick Launch search zone (dominant centrepiece)."""
        zone = ttk.LabelFrame(parent, text="", padding=20, bootstyle="primary")
        zone.pack(fill=tk.BOTH, expand=True, pady=(0, 8))

        ttk.Label(
            zone, text="Quick Launch",
            font=("Helvetica", 16, "bold"), bootstyle="primary"
        ).pack(anchor="w")
        ttk.Label(
            zone, text="Resolve a company or charity and jump straight into analysis.",
            font=("Segoe UI", 10), foreground="gray"
        ).pack(anchor="w", pady=(0, 10))

        # Entity type toggle row
        toggle_row = ttk.Frame(zone)
        toggle_row.pack(anchor="w", pady=(0, 8))

        self._ql_entity_type = tk.StringVar(value="company")
        self._ql_toggle_btns = {}

        for etype, label, needs_key in [
            ("company", "Company", self.api_key),
            ("charity", "Charity", self.charity_api_key),
        ]:
            btn = ttk.Button(
                toggle_row, text=label, width=12,
                command=lambda t=etype: self._ql_set_entity_type(t),
                state=tk.NORMAL if needs_key else tk.DISABLED,
            )
            btn.pack(side=tk.LEFT, padx=(0, 6))
            self._ql_toggle_btns[etype] = btn

        self._ql_update_toggle_styles()

        # Search row
        search_row = ttk.Frame(zone)
        search_row.pack(fill=tk.X, pady=(0, 8))

        self._ql_search_var = tk.StringVar()
        self._ql_entry = ttk.Entry(
            search_row, textvariable=self._ql_search_var,
            font=("Segoe UI", 11), width=50
        )
        self._ql_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 8))
        self._ql_entry.insert(0, "Enter number or name...")
        self._ql_entry.configure(foreground="gray")
        self._ql_entry.bind("<FocusIn>", self._ql_focus_in)
        self._ql_entry.bind("<FocusOut>", self._ql_focus_out)
        self._ql_entry.bind("<Return>", lambda e: self._quick_launch_search())

        self._ql_search_btn = ttk.Button(
            search_row, text="Search", bootstyle="primary",
            command=self._quick_launch_search
        )
        self._ql_search_btn.pack(side=tk.LEFT)

        # Result area (populated after search)
        self._ql_result_frame = ttk.Frame(zone)
        self._ql_result_frame.pack(fill=tk.X, pady=(8, 0))
        self._ql_resolved_entity = None

    def _ql_set_entity_type(self, etype: str) -> None:
        self._ql_entity_type.set(etype)
        self._ql_update_toggle_styles()

    def _ql_update_toggle_styles(self) -> None:
        active = self._ql_entity_type.get()
        for etype, btn in self._ql_toggle_btns.items():
            if str(btn.cget("state")) == tk.DISABLED:
                continue
            btn.configure(bootstyle="primary" if etype == active else "primary-outline")

    def _ql_focus_in(self, event) -> None:
        if self._ql_entry.get() == "Enter number or name...":
            self._ql_entry.delete(0, tk.END)
            self._ql_entry.configure(foreground="")

    def _ql_focus_out(self, event) -> None:
        if not self._ql_entry.get().strip():
            self._ql_entry.insert(0, "Enter number or name...")
            self._ql_entry.configure(foreground="gray")

    def _quick_launch_search(self) -> None:
        """Trigger entity resolution in a background thread."""
        query = self._ql_search_var.get().strip()
        if not query or query == "Enter number or name...":
            return
        entity_type = self._ql_entity_type.get()

        # Show searching state
        for w in self._ql_result_frame.winfo_children():
            w.destroy()
        ttk.Label(
            self._ql_result_frame, text="Searching...",
            font=("Segoe UI", 10, "italic"), foreground="gray"
        ).pack(anchor="w")
        self._ql_search_btn.configure(state=tk.DISABLED)

        threading.Thread(
            target=self._ql_resolve, args=(query, entity_type), daemon=True
        ).start()

    def _ql_resolve(self, query: str, entity_type: str) -> None:
        """Background: resolve entity via API and schedule UI update."""
        result = None
        error = None

        try:
            if entity_type == "company":
                result, error = self._ql_resolve_company(query)
            elif entity_type == "charity":
                result, error = self._ql_resolve_charity(query)
        except Exception as e:
            error = str(e)

        self.after(0, self._ql_display_result, result, entity_type, error)

    def _ql_resolve_company(self, query: str):
        """Resolve a company by number or name search."""
        # Check if query looks like a company number
        stripped = query.strip().upper()
        if re.match(r'^[A-Z]{0,2}\d{5,8}$', stripped):
            number = clean_company_number(stripped)
            data, err = ch_get_company(self.api_key, self.ch_token_bucket, number)
            if err:
                return None, err
            return data, None

        # Otherwise search by name
        data, err = ch_search_companies(
            self.api_key, self.ch_token_bucket, query, items_per_page=5
        )
        if err:
            return None, err
        items = (data or {}).get("items", [])
        if not items:
            return None, None

        # Fetch full profile of first match
        top_match = items[0]
        number = top_match.get("company_number", "")
        match_note = None
        matches = top_match.get("matches") or {}
        title_matches = matches.get("title") or []
        snippet_matches = matches.get("snippet") or []
        description_identifiers = top_match.get("description_identifier") or []
        if isinstance(description_identifiers, str):
            description_identifiers = [description_identifiers]
        query_norm = re.sub(r"[^a-z0-9]+", "", query.lower())
        title_norm = re.sub(r"[^a-z0-9]+", "", str(top_match.get("title", "")).lower())
        query_not_in_current_title = bool(query_norm and query_norm not in title_norm)
        former_hint = any("former" in str(v).lower() for v in description_identifiers)
        if query_not_in_current_title and (snippet_matches or former_hint or not title_matches):
            match_note = "Matched via a former company name."

        if number:
            profile, perr = ch_get_company(
                self.api_key, self.ch_token_bucket, number
            )
            if profile and match_note:
                profile["_quick_launch_match_note"] = match_note
            return profile, perr
        if match_note:
            top_match["_quick_launch_match_note"] = match_note
        return top_match, None

    def _ql_resolve_charity(self, query: str):
        """Resolve a charity by registration number or name search."""
        stripped = query.strip()
        if stripped.isdigit():
            data, err = cc_get_charity_details(self.charity_api_key, stripped)
            if err:
                return None, err
            return data, None

        # Search by name using /searchCharityName/ endpoint (same as Bulk Entity Search)
        data, err = cc_search_charity_by_name(self.charity_api_key, stripped)
        if err:
            return None, err
        if not data or not isinstance(data, list):
            return None, None

        # Exact match: find best match using fuzzy matching at 100% threshold
        best_match, best_score = None, 0
        for item in data:
            score = WRatio(stripped.lower(), item.get("charity_name", "").lower())
            if score > best_score:
                best_match, best_score = item, score

        if best_match:
            best_match["_quick_launch_match_score"] = round(best_score)
            return best_match, None
        return None, None

    def _ql_display_result(self, result, entity_type: str, error) -> None:
        """Show the resolved entity card and action buttons."""
        self._ql_search_btn.configure(state=tk.NORMAL)

        for w in self._ql_result_frame.winfo_children():
            w.destroy()

        if error:
            ttk.Label(
                self._ql_result_frame, text=f"Error: {error}",
                foreground="red", font=("Segoe UI", 10)
            ).pack(anchor="w")
            return

        if not result:
            ttk.Label(
                self._ql_result_frame, text="No match found",
                foreground="gray", font=("Segoe UI", 10, "italic")
            ).pack(anchor="w")
            return

        # Store resolved entity for action buttons
        self._ql_resolved_entity = result
        self._ql_resolved_entity["_entity_type"] = entity_type

        # Entity card
        card = ttk.Frame(self._ql_result_frame, padding=(10, 8))
        card.pack(fill=tk.X, pady=(4, 0))

        if entity_type == "company":
            name = result.get("company_name", "Unknown")
            number = result.get("company_number", "N/A")
            status = result.get("company_status", "unknown").replace("_", " ").title()
            date_raw = result.get("date_of_creation", "")
            try:
                date_disp = datetime.strptime(date_raw, "%Y-%m-%d").strftime("%d %b %Y")
            except (ValueError, TypeError):
                date_disp = date_raw or "N/A"

            ttk.Label(
                card, text=name, font=("Helvetica", 14, "bold"), bootstyle="primary"
            ).pack(anchor="w")
            ttk.Label(
                card,
                text=f"Company No: {number}  |  Status: {status}  |  Incorporated: {date_disp}",
                font=("Segoe UI", 9), foreground="gray"
            ).pack(anchor="w", pady=(2, 0))
            if result.get("_quick_launch_match_note"):
                ttk.Label(
                    card,
                    text=f"ℹ {result.get('_quick_launch_match_note')}",
                    font=("Segoe UI", 9),
                    foreground="#fd7e14",
                ).pack(anchor="w", pady=(2, 0))

        elif entity_type == "charity":
            name = result.get("charity_name", result.get("name", "Unknown"))
            reg_num = str(result.get("reg_charity_number",
                          result.get("registered_charity_number", "N/A")))
            reg_status_code = (result.get("reg_status") or "").upper()
            _STATUS_MAP = {"R": "Registered", "RM": "Removed"}
            status = result.get("charity_registration_status",
                        result.get("registration_status",
                        _STATUS_MAP.get(reg_status_code, reg_status_code or "Unknown")))
            is_active = reg_status_code != "RM" and "removed" not in (status or "").lower()
            result["_is_active"] = is_active

            ttk.Label(
                card, text=name, font=("Helvetica", 14, "bold"), bootstyle="primary"
            ).pack(anchor="w")
            ttk.Label(
                card, text=f"Reg No: {reg_num}  |  Status: {status}",
                font=("Segoe UI", 9), foreground="gray"
            ).pack(anchor="w", pady=(2, 0))
            if not is_active:
                ttk.Label(
                    card,
                    text="\u26a0 This charity is no longer registered",
                    font=("Segoe UI", 9, "bold"), foreground="#fd7e14"
                ).pack(anchor="w", pady=(2, 0))

        # Record in quick launch history
        self._ql_record_history(result, entity_type)

        # Action buttons
        btn_row = ttk.Frame(self._ql_result_frame)
        btn_row.pack(anchor="w", pady=(8, 0))

        ttk.Button(
            btn_row, text="Run Enhanced Due Diligence", bootstyle="info-outline",
            command=lambda: self._ql_open_in_edd(result, entity_type)
        ).pack(side=tk.LEFT, padx=(0, 6))

        trace_btn = ttk.Button(
            btn_row, text="Trace Ownership", bootstyle="primary-outline",
            command=lambda: self._ql_open_in_ubo(result),
            state=tk.NORMAL if entity_type == "company" else tk.DISABLED,
        )
        trace_btn.pack(side=tk.LEFT, padx=(0, 6))
        if entity_type != "company":
            Tooltip(trace_btn, "UBO Tracer supports companies only")

        ttk.Button(
            btn_row, text="Add to Working Set", bootstyle="info-outline",
            command=lambda: self._ql_add_to_working_set(result, entity_type)
        ).pack(side=tk.LEFT, padx=(0, 6))

    def _ql_record_history(self, result, entity_type: str) -> None:
        """Record resolved entity in quick launch history (max 5, dedup)."""
        if entity_type == "company":
            key = result.get("company_number", "")
            name = result.get("company_name", "Unknown")
        else:
            key = str(result.get("reg_charity_number",
                       result.get("registered_charity_number", "")))
            name = result.get("charity_name", result.get("name", "Unknown"))

        history = self.app_state.quick_launch_history
        # Remove existing entry with same key
        history[:] = [e for e in history if e.get("key") != key]
        history.insert(0, {"key": key, "name": name, "type": entity_type})
        self.app_state.quick_launch_history = history[:5]

    # ── Quick Launch Action Handlers ─────────────────────────────────

    def _ql_open_in_edd(self, entity, entity_type: str) -> None:
        eid = (entity.get("company_number") or
               str(entity.get("reg_charity_number",
                   entity.get("registered_charity_number", ""))))
        etype = "company" if entity_type == "company" else "charity"
        self.show_enhanced_dd(prefill_entity={"type": etype, "id": eid})

    def _ql_open_in_ubo(self, entity) -> None:
        number = entity.get("company_number", "")
        name = entity.get("company_name", "")
        self.show_ubo_investigation(prefill_company=number,
                                     prefill_company_name=name)

    def _ql_open_in_director(self, entity) -> None:
        name = entity.get("company_name", "")
        self.show_director_investigation(prefill_name=name)

    def _ql_add_to_working_set(self, entity, entity_type: str) -> None:
        if self.app_state.ubo_working_set is None:
            self.app_state.ubo_working_set = []

        if entity_type == "company":
            num = entity.get("company_number", "")
            name = entity.get("company_name", "Unknown")
        else:
            num = str(entity.get("reg_charity_number",
                       entity.get("registered_charity_number", "")))
            name = entity.get("charity_name", entity.get("name", "Unknown"))

        is_active = entity.get("_is_active", True)

        existing = {
            (e.get("name", ""), e.get("company_number", ""), e.get("entity_type", ""))
            for e in self.app_state.ubo_working_set
        }
        if num and (name, num, entity_type) not in existing:
            self.app_state.ubo_working_set.append({
                "name": name, "company_number": num, "active": is_active,
                "entity_type": entity_type,
            })
        self._refresh_working_set_indicator()
        # Refresh home screen lower panel if visible
        if hasattr(self, "_home_ws_tree") and self._home_ws_tree:
            self._refresh_home_working_set()

    # ── Zone 2: System Status ────────────────────────────────────────

    def _build_system_status_zone(self, parent) -> None:
        """Build compact system status panels for all 4 APIs."""
        zone = ttk.Frame(parent)
        zone.pack(fill=tk.X, pady=(0, 8))

        # --- Companies House panel ---
        ch_panel = ttk.LabelFrame(zone, text="Companies House", padding=8)
        ch_panel.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 3))

        self._home_ch_key_lbl = ttk.Label(ch_panel, text="", font=("Segoe UI", 9))
        self._home_ch_key_lbl.pack(anchor="w")

        # Connection + Mode/Workers on one row for height alignment
        ch_conn_row = ttk.Frame(ch_panel)
        ch_conn_row.pack(anchor="w", fill=tk.X)
        self._home_ch_conn_lbl = ttk.Label(ch_conn_row, text="", font=("Segoe UI", 9))
        self._home_ch_conn_lbl.pack(side=tk.LEFT)


        self._home_ch_test_btn = ttk.Button(
            ch_panel, text="Test connection", bootstyle="link",
            command=lambda: self._test_single_api("companies_house")
        )
        self._home_ch_test_btn.pack(anchor="w")

        # --- Charity Commission panel ---
        cc_panel = ttk.LabelFrame(zone, text="Charity Commission", padding=8)
        cc_panel.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(3, 3))

        self._home_cc_key_lbl = ttk.Label(cc_panel, text="", font=("Segoe UI", 9))
        self._home_cc_key_lbl.pack(anchor="w")
        self._home_cc_conn_lbl = ttk.Label(cc_panel, text="", font=("Segoe UI", 9))
        self._home_cc_conn_lbl.pack(anchor="w")

        self._home_cc_test_btn = ttk.Button(
            cc_panel, text="Test connection", bootstyle="link",
            command=lambda: self._test_single_api("charity_commission")
        )
        self._home_cc_test_btn.pack(anchor="w")

        # --- GrantNav 360Giving panel ---
        gn_panel = ttk.LabelFrame(zone, text="GrantNav 360Giving", padding=8)
        gn_panel.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(3, 3))

        ttk.Label(gn_panel, text="No key required", font=("Segoe UI", 9),
                  foreground="gray").pack(anchor="w")
        self._home_gn_conn_lbl = ttk.Label(gn_panel, text="", font=("Segoe UI", 9))
        self._home_gn_conn_lbl.pack(anchor="w")

        self._home_gn_test_btn = ttk.Button(
            gn_panel, text="Test connection", bootstyle="link",
            command=lambda: self._test_single_api("grantnav")
        )
        self._home_gn_test_btn.pack(anchor="w")

        # --- Contracts Finder panel ---
        cf_panel = ttk.LabelFrame(zone, text="Contracts Finder", padding=8)
        cf_panel.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(3, 0))

        ttk.Label(cf_panel, text="No key required", font=("Segoe UI", 9),
                  foreground="gray").pack(anchor="w")
        self._home_cf_conn_lbl = ttk.Label(cf_panel, text="", font=("Segoe UI", 9))
        self._home_cf_conn_lbl.pack(anchor="w")

        self._home_cf_test_btn = ttk.Button(
            cf_panel, text="Test connection", bootstyle="link",
            command=lambda: self._test_single_api("contracts_finder")
        )
        self._home_cf_test_btn.pack(anchor="w")

        # Initial display
        self._update_home_status_display()

    def _update_home_status_display(self) -> None:
        """Refresh the system status labels from cached API statuses."""
        try:
            if (not hasattr(self, "_home_ch_key_lbl")
                    or not self._home_ch_key_lbl
                    or not self._home_ch_key_lbl.winfo_exists()):
                return
        except tk.TclError:
            return

        def _conn_text(status):
            if status == "ok":
                return "Connection: ✓ OK", "green"
            elif status == "error":
                return "Connection: ✗ Failed", "red"
            elif status == "no_key":
                return "Connection: — No key", "gray"
            return "Connection: …", "gray"

        try:
            statuses = self.api_statuses or {}

            # Companies House
            ch_key_ok = bool(self.api_key)
            self._home_ch_key_lbl.configure(
                text=f"Key loaded: {'✓' if ch_key_ok else '✗'}",
                foreground="green" if ch_key_ok else "red"
            )
            ct, cc = _conn_text(statuses.get("companies_house", "unknown"))
            self._home_ch_conn_lbl.configure(text=ct, foreground=cc)

            # Charity Commission
            cc_key_ok = bool(self.charity_api_key)
            self._home_cc_key_lbl.configure(
                text=f"Key loaded: {'✓' if cc_key_ok else '✗'}",
                foreground="green" if cc_key_ok else "red"
            )
            ct, cc = _conn_text(statuses.get("charity_commission", "unknown"))
            self._home_cc_conn_lbl.configure(text=ct, foreground=cc)

            # GrantNav
            ct, cc = _conn_text(statuses.get("grantnav", "unknown"))
            self._home_gn_conn_lbl.configure(text=ct, foreground=cc)

            # Contracts Finder
            ct, cc = _conn_text(statuses.get("contracts_finder", "unknown"))
            self._home_cf_conn_lbl.configure(text=ct, foreground=cc)
        except tk.TclError:
            pass  # Widgets destroyed during navigation

    def _test_single_api(self, api_name: str) -> None:
        """Re-test a single API connection and update status with feedback."""
        btn_map = {
            "companies_house": "_home_ch_test_btn",
            "charity_commission": "_home_cc_test_btn",
            "grantnav": "_home_gn_test_btn",
            "contracts_finder": "_home_cf_test_btn",
        }
        btn_attr = btn_map.get(api_name)
        btn = getattr(self, btn_attr, None) if btn_attr else None

        # Show "Testing..." state
        if btn:
            try:
                btn.configure(text="Testing...", state=tk.DISABLED)
            except tk.TclError:
                pass

        def _run():
            self.check_api_status()
            status = (self.api_statuses or {}).get(api_name, "unknown")

            def _update_ui():
                try:
                    self._update_home_status_display()
                    if btn and btn.winfo_exists():
                        if status == "ok":
                            btn.configure(text="✓ Connected", state=tk.NORMAL)
                        else:
                            btn.configure(text="✗ Failed", state=tk.NORMAL)
                        # Revert after 2 seconds
                        self.after(2000, lambda: self._reset_test_btn(btn))
                except tk.TclError:
                    pass

            self.after(0, _update_ui)

        threading.Thread(target=_run, daemon=True).start()

    def _reset_test_btn(self, btn) -> None:
        """Reset a test connection button back to its default text."""
        try:
            if btn and btn.winfo_exists():
                btn.configure(text="Test connection")
        except tk.TclError:
            pass

    # ── Zone 3: Working Set + Recent Reports ─────────────────────────

    def _build_lower_panels_zone(self, parent) -> None:
        """Build the working set and recent reports panels."""
        zone = ttk.Frame(parent)
        zone.pack(fill=tk.BOTH, expand=True, pady=(0, 4))
        zone.columnconfigure(0, weight=1, uniform="lower_panels")
        zone.columnconfigure(1, weight=1, uniform="lower_panels")
        zone.rowconfigure(0, weight=1)

        # Left — Working Set
        ws_panel = ttk.LabelFrame(zone, text="Working Set", padding=8)
        ws_panel.grid(row=0, column=0, sticky="nsew", padx=(0, 4))

        self._home_ws_header = ttk.Label(ws_panel, text="", font=("Segoe UI", 9, "bold"))
        self._home_ws_header.pack(anchor="w")

        self._home_ws_tree = ttk.Treeview(
            ws_panel, columns=("name", "number"), show="headings",
            height=6, selectmode="extended"
        )
        self._home_ws_tree.heading(
            "name", text="Name",
            command=lambda: self._sort_ws_tree(self._home_ws_tree, "name"))
        self._home_ws_tree.heading(
            "number", text="Number",
            command=lambda: self._sort_ws_tree(self._home_ws_tree, "number"))
        self._home_ws_tree.column("name", width=200, minwidth=100)
        self._home_ws_tree.column("number", width=100, minwidth=70)
        self._home_ws_tree.pack(fill=tk.BOTH, expand=True, pady=(4, 4))

        ws_btn_row = ttk.Frame(ws_panel)
        ws_btn_row.pack(fill=tk.X)

        self._home_ws_send_menu = ttk.Menubutton(
            ws_btn_row, text="Send to\u2026 \u25bc", bootstyle="primary-outline"
        )
        self._home_ws_send_menu_obj = tk.Menu(self._home_ws_send_menu, tearoff=0)
        # Index 0
        self._home_ws_send_menu_obj.add_command(
            label="Network Analytics Workbench",
            command=lambda: self._send_working_set_to_network(self._home_ws_tree))
        # Index 1
        self._home_ws_send_menu_obj.add_command(
            label="Enhanced Due Diligence",
            command=lambda: self._send_home_ws_selection_to_edd())
        # Index 2
        self._home_ws_send_menu_obj.add_command(
            label="UBO Tracer",
            command=lambda: self._send_ws_to_ubo(self._home_ws_tree))
        # Index 3
        self._home_ws_send_menu_obj.add_command(
            label="Bulk Entity Search",
            command=lambda: self._send_ws_to_bulk_search(self._home_ws_tree))
        # Index 4
        self._home_ws_send_menu_obj.add_command(
            label="Grants Search",
            command=lambda: self._send_ws_to_grants(self._home_ws_tree))
        # Index 5
        self._home_ws_send_menu_obj.add_command(
            label="Director Search",
            command=lambda: self._send_ws_to_director(self._home_ws_tree))
        self._home_ws_send_menu.configure(menu=self._home_ws_send_menu_obj)
        self._home_ws_send_menu.pack(side=tk.LEFT, padx=(0, 6))

        # Bind selection change to update menu state
        self._home_ws_tree.bind(
            "<<TreeviewSelect>>",
            lambda e: self._update_ws_send_menu_state(
                self._home_ws_tree, self._home_ws_send_menu_obj)
        )
        # Click-to-deselect toggle
        self._home_ws_tree.bind(
            "<Button-1>", lambda e: self._toggle_tree_selection(e, self._home_ws_tree)
        )

        ttk.Button(
            ws_btn_row, text="Clear", bootstyle="danger-outline",
            command=self._clear_home_working_set
        ).pack(side=tk.LEFT)

        self._refresh_home_working_set()

        # Right — Recent EDD Reports
        rr_panel = ttk.LabelFrame(zone, text="Recent Reports", padding=8)
        rr_panel.grid(row=0, column=1, sticky="nsew", padx=(4, 0))

        self._home_reports_frame = ttk.Frame(rr_panel)
        self._home_reports_frame.pack(fill=tk.BOTH, expand=True)
        self._refresh_home_reports()

    def _refresh_home_working_set(self) -> None:
        """Refresh the home screen working set list."""
        try:
            if (not hasattr(self, "_home_ws_tree")
                    or not self._home_ws_tree
                    or not self._home_ws_tree.winfo_exists()):
                return
        except tk.TclError:
            return

        try:
            entities = self._collect_working_set_entities()
            count = len(entities)

            if count > 0:
                self._home_ws_header.configure(text=f"Working set ({count})")
            else:
                self._home_ws_header.configure(text="No entities in working set")

            self._home_ws_tree.delete(*self._home_ws_tree.get_children())
            for ent in entities:
                self._home_ws_tree.insert("", tk.END, values=(
                    ent.get("name", "Unknown"),
                    ent.get("company_number", ent.get("number", ""))
                ))
        except tk.TclError:
            pass

    def _clear_home_working_set(self) -> None:
        """Clear working set from both home display and sidebar."""
        self._clear_working_set()
        self._refresh_home_working_set()

    # ── Working Set Selection & Send Logic ──────────────────────────

    def _toggle_tree_selection(self, event, tree) -> None:
        """Toggle selection on click: deselect if already selected, else default."""
        region = tree.identify_region(event.x, event.y)
        if region in ("heading", "separator"):
            return
        item = tree.identify_row(event.y)
        if not item:
            # Click on empty area — clear all selection
            tree.selection_set([])
            return "break"
        if item in tree.selection():
            # Already selected — deselect
            tree.selection_remove(item)
            return "break"
        # Not selected — let default Treeview behavior handle it

    def _get_ws_selected_entities(self, tree):
        """Return selected entities from a working set tree, or all if none selected."""
        entities = self._collect_working_set_entities()
        if not entities:
            return []
        try:
            sel = tree.selection()
        except tk.TclError:
            sel = ()
        if sel:
            indices = [tree.index(item) for item in sel]
            return [entities[i] for i in indices if i < len(entities)]
        return entities

    def _ensure_ws_selection(self, tree) -> bool:
        """Ensure the working set has an explicit selection before Send To actions."""
        try:
            selected = tree.selection()
        except tk.TclError:
            selected = ()
        if not selected:
            messagebox.showinfo("Working Set", "No entities selected.")
            return False
        return True

    def _classify_ws_selection(self, tree):
        """Classify the current selection in a working set tree.

        Returns (selected_entities, has_companies, has_charities, has_persons, count).
        """
        selected = self._get_ws_selected_entities(tree)
        has_companies = False
        has_charities = False
        has_persons = False
        for ent in selected:
            etype = ent.get("entity_type", "company")
            if etype == "company":
                has_companies = True
            elif etype == "charity":
                has_charities = True
            elif etype == "person":
                has_persons = True
        return selected, has_companies, has_charities, has_persons, len(selected)

    def _update_ws_send_menu_state(self, tree, menu) -> None:
        """Enable/disable Send To menu items based on selection in a working set tree.

        Menu indices:
          0 = Network Analytics Workbench  — always enabled if entities exist
          1 = Enhanced Due Diligence       — companies/charities only (bulk supported)
          2 = UBO Tracer                   — companies only (no charities/persons)
          3 = Bulk Entity Search           — companies + charities (no persons)
          4 = Grants Search                — companies + charities (no persons)
          5 = Director Search              — single person only
        """
        try:
            selected, has_co, has_ch, has_per, count = self._classify_ws_selection(tree)
        except (tk.TclError, AttributeError):
            return

        def _set(idx, label, enabled):
            try:
                menu.entryconfigure(idx, label=label,
                                    state=tk.NORMAL if enabled else tk.DISABLED)
            except tk.TclError:
                pass

        has_any = count > 0

        # 0: Network Analytics — always if entities exist
        _set(0, "Network Analytics Workbench", has_any)

        # 1: EDD — companies/charities only, no persons
        edd_ok = has_any and (has_co or has_ch) and not has_per
        _set(1, "Enhanced Due Diligence" if edd_ok else
             "Enhanced Due Diligence (companies/charities only)", edd_ok)

        # 2: UBO Tracer — companies only, no charities or persons
        ubo_ok = has_any and has_co and not has_per and not has_ch
        _set(2, "UBO Tracer" if ubo_ok else
             "UBO Tracer (companies only)", ubo_ok)

        # 3: Bulk Entity Search — companies + charities, no persons
        bulk_ok = has_any and (has_co or has_ch) and not has_per
        _set(3, "Bulk Entity Search" if bulk_ok else
             "Bulk Entity Search (companies/charities only)", bulk_ok)

        # 4: Grants Search — companies + charities, no persons
        grants_ok = has_any and (has_co or has_ch) and not has_per
        _set(4, "Grants Search" if grants_ok else
             "Grants Search (companies/charities only)", grants_ok)

        # 5: Director Search — single person only
        dir_ok = (count == 1 and has_per and not has_co and not has_ch)
        _set(5, "Director Search" if dir_ok else
             "Director Search (select 1 person)", dir_ok)

    def _sort_ws_tree(self, tree, col) -> None:
        """Sort working set treeview by column, toggling A-Z / Z-A."""
        try:
            items = [(tree.set(iid, col), iid) for iid in tree.get_children("")]
        except tk.TclError:
            return
        # Toggle sort direction using a stored attribute
        attr = f"_ws_sort_reverse_{id(tree)}_{col}"
        reverse = getattr(self, attr, False)
        items.sort(key=lambda t: t[0].lower(), reverse=reverse)
        for idx, (_, iid) in enumerate(items):
            tree.move(iid, "", idx)
        setattr(self, attr, not reverse)

    def _send_ws_selection_to_edd(self) -> None:
        """Send the selected sidebar working set entity to EDD."""
        self._send_tree_selection_to_edd(self._working_set_tree)

    def _send_home_ws_selection_to_edd(self) -> None:
        """Send the selected home working set entity to EDD."""
        self._send_tree_selection_to_edd(self._home_ws_tree)

    def _send_tree_selection_to_edd(self, tree) -> None:
        """Send selected companies/charities from any working set tree to EDD."""
        if not self._ensure_ws_selection(tree):
            return
        selected = self._get_ws_selected_entities(tree)
        if not selected:
            return

        valid = []
        skipped_persons = 0
        for ent in selected:
            etype = ent.get("entity_type", "company")
            if etype == "person":
                skipped_persons += 1
                continue
            num = str(ent.get("company_number", ent.get("number", ""))).strip()
            if not num:
                continue
            dd_type = "charity" if etype == "charity" else "company"
            valid.append({"type": dd_type, "id": num})

        if skipped_persons:
            messagebox.showinfo(
                "EDD",
                f"{skipped_persons} person entit{'y' if skipped_persons == 1 else 'ies'} skipped — "
                "Enhanced Due Diligence supports companies and charities only.",
            )

        if not valid:
            messagebox.showinfo(
                "EDD", "No compatible companies or charities were selected."
            )
            return
        self.show_enhanced_dd(prefill_entities=valid)

    def _send_ws_to_ubo(self, tree) -> None:
        """Send selected companies from working set to UBO Tracer."""
        try:
            if not self._ensure_ws_selection(tree):
                return
            selected = self._get_ws_selected_entities(tree)
            if not selected:
                return

            companies = [e for e in selected if e.get("entity_type", "company") == "company"]
            others = [e for e in selected if e.get("entity_type", "company") != "company"]

            if not companies:
                messagebox.showinfo("UBO Tracer",
                                    "UBO Tracer supports companies only. No companies in selection.")
                return
            if others:
                ok = messagebox.askyesno(
                    "UBO Tracer",
                    f"{len(companies)} companies and {len(others)} non-companies selected. "
                    f"UBO Tracer supports companies only. Send {len(companies)} companies?")
                if not ok:
                    return

            if len(companies) == 1:
                c = companies[0]
                prefill_company = c.get("company_number", c.get("number", ""))
                prefill_name = c.get("name", "")
                # Defer navigation to idle so widget teardown does not happen
                # mid menu/tree callback.
                self.after_idle(
                    lambda: self.show_ubo_investigation(
                        prefill_company=prefill_company,
                        prefill_company_name=prefill_name
                    )
                )
            else:
                payload = [dict(c) for c in companies]
                # Defer navigation to idle so widget teardown does not happen
                # mid menu/tree callback.
                self.after_idle(
                    lambda: self.show_ubo_investigation(prefill_entities=payload)
                )
        except Exception as e:
            messagebox.showerror("Error", f"Failed to send to UBO Tracer:\n{e}")
            import traceback
            traceback.print_exc()

    def _send_ws_to_bulk_search(self, tree) -> None:
        """Send selected companies/charities from working set to Bulk Entity Search."""
        if not self._ensure_ws_selection(tree):
            return
        selected = self._get_ws_selected_entities(tree)
        if not selected:
            return

        valid = [e for e in selected
                 if e.get("entity_type", "company") in ("company", "charity")]
        persons = [e for e in selected if e.get("entity_type") == "person"]

        if not valid:
            messagebox.showinfo("Bulk Entity Search",
                                "No companies or charities in selection.")
            return
        if persons:
            messagebox.showinfo("Bulk Entity Search",
                                f"Skipping {len(persons)} person(s). "
                                f"Sending {len(valid)} companies/charities.")

        self.show_unified_search(prefill_entities=valid)

    def _send_ws_to_grants(self, tree) -> None:
        """Send selected companies/charities from working set to Grants Search."""
        if not self._ensure_ws_selection(tree):
            return
        selected = self._get_ws_selected_entities(tree)
        if not selected:
            return

        valid = [e for e in selected
                 if e.get("entity_type", "company") in ("company", "charity")]
        persons = [e for e in selected if e.get("entity_type") == "person"]

        if not valid:
            messagebox.showinfo("Grants Search",
                                "No companies or charities in selection.")
            return
        if persons:
            messagebox.showinfo("Grants Search",
                                f"Skipping {len(persons)} person(s). "
                                f"Sending {len(valid)} companies/charities.")

        self.show_grants_investigation(prefill_entities=valid, prefill_source="Working Set")

    def _send_ws_to_director(self, tree) -> None:
        """Send selected person from working set to Director Search."""
        if not self._ensure_ws_selection(tree):
            return
        selected = self._get_ws_selected_entities(tree)
        if not selected:
            return

        persons = [e for e in selected if e.get("entity_type") == "person"]
        if not persons or len(persons) != 1:
            messagebox.showinfo("Director Search",
                                "Please select exactly 1 person for Director Search.")
            return

        ent = persons[0]
        from .utils.fuzzy_match import normalize_person_name
        name = normalize_person_name(ent.get("name", ""))

        # Extract year/month from company_number field (stored as "MM/YYYY" for persons)
        dob_str = ent.get("company_number", "")
        year, month = None, None
        if dob_str and "/" in dob_str:
            parts = dob_str.split("/")
            if len(parts) == 2:
                try:
                    month = str(int(parts[0]))
                    year = str(int(parts[1]))
                except ValueError:
                    pass

        self.show_director_investigation(
            prefill_name=name, prefill_year=year, prefill_month=month)

    def _refresh_home_reports(self) -> None:
        """Refresh the recent EDD reports list on the home screen."""
        try:
            if (not hasattr(self, "_home_reports_frame")
                    or not self._home_reports_frame
                    or not self._home_reports_frame.winfo_exists()):
                return
        except tk.TclError:
            return

        for w in self._home_reports_frame.winfo_children():
            w.destroy()

        reports = self.app_state.recent_edd_reports[:5]
        if not reports:
            ttk.Label(
                self._home_reports_frame, text="No recent reports",
                foreground="gray", font=("Segoe UI", 9, "italic")
            ).pack(anchor="w", pady=4)
            return

        for report in reports:
            row = ttk.Frame(self._home_reports_frame)
            row.pack(fill=tk.X, pady=2)
            name = report.get("name", "Unknown")
            date = report.get("date", "")
            path = report.get("path", "")
            lbl = ttk.Label(
                row, text=f"{name}  —  {date}",
                font=("Segoe UI", 9), cursor="hand2", foreground="#0d6efd"
            )
            lbl.pack(anchor="w")
            if path:
                lbl.bind("<Button-1>",
                         lambda e, p=path: webbrowser.open(f"file://{p}"))
    
    def show_main_guide(self) -> None:
        """Show the main help window."""
        HelpWindow(self, "User Guide", HELP_CONTENT["main"])
    
    def clear_cache_and_logs(self) -> None:
        """Clear temporary files and logs, preserving user settings."""
        if not os.path.exists(CONFIG_DIR):
            messagebox.showinfo("Info", "No cache directory found.")
            return

        if messagebox.askyesno("Confirm", "Delete all cached files and logs?\n(Your settings will be preserved.)"):
            import shutil
            try:
                shutil.rmtree(CONFIG_DIR)
                os.makedirs(CONFIG_DIR)
                # Re-save current settings so they survive the clear
                save_settings(self._settings)
                messagebox.showinfo("Success", "Cache and logs cleared.")
            except Exception as e:
                messagebox.showerror("Error", f"Could not clear cache: {e}")

    def open_config_folder(self) -> None:
        """Open the config folder in the system file explorer."""
        if not os.path.exists(CONFIG_DIR):
            os.makedirs(CONFIG_DIR, exist_ok=True)

        try:
            if sys.platform == "win32":
                os.startfile(CONFIG_DIR)
            elif sys.platform == "darwin":
                subprocess.run(["open", CONFIG_DIR], check=True)
            else:  # Linux and other Unix-like systems
                subprocess.run(["xdg-open", CONFIG_DIR], check=True)
        except Exception as e:
            messagebox.showerror("Error", f"Could not open config folder: {e}")

    
    def show_licenses(self) -> None:
        """Show third-party licenses window."""
        from .ui.licenses_window import LicensesWindow
        LicensesWindow(self)

    def check_api_status(self):
        """Lightweight check of API availability. Returns dict of statuses."""
        statuses = {
            'companies_house': 'unknown',
            'charity_commission': 'unknown',
            'grantnav': 'unknown',
            'contracts_finder': 'unknown'  # <--- Added new key
        }
        
        # Test Companies House (only if key exists)
        if self.api_key:
            try:
                # Quick test: Search for a known company (Google UK)
                response = requests.get(
                    f"{API_BASE_URL}/company/00445790",
                    auth=(self.api_key, ""),
                    timeout=3
                )
                statuses['companies_house'] = 'ok' if response.status_code == 200 else 'error'
            except requests.RequestException:
                statuses['companies_house'] = 'error'
        else:
            statuses['companies_house'] = 'no_key'
        
        # Test Charity Commission (only if key exists)
        if self.charity_api_key:
            try:
                # Quick test: Known charity (British Red Cross)
                response = requests.get(
                    f"{CHARITY_API_BASE_URL}/charitydetails/220949/0",
                    headers={'Ocp-Apim-Subscription-Key': self.charity_api_key},
                    timeout=3
                )
                statuses['charity_commission'] = 'ok' if response.status_code == 200 else 'error'
            except requests.RequestException:
                statuses['charity_commission'] = 'error'
        else:
            statuses['charity_commission'] = 'no_key'
        
        # Test GrantNav (no key required)
        try:
            # Quick test: Simple search
            response = requests.get(
                f"{GRANTNAV_API_BASE_URL}/org/GB-CHC-220949/grants_received?limit=1",
                timeout=3
            )
            statuses['grantnav'] = 'ok' if response.status_code == 200 else 'error'
        except requests.RequestException:
            statuses['grantnav'] = 'error'

        # --- NEW: Test Contracts Finder (no key required) ---
        try:
            # Import here to avoid top-level dependency
            from .api.contracts_finder import check_api_status as check_cf
            is_online = check_cf()
            statuses['contracts_finder'] = 'ok' if is_online else 'error'
        except Exception as e:
            log_message(f"Contracts Finder check failed: {e}")
            statuses['contracts_finder'] = 'error'
        
        # Cache results
        self.api_statuses = statuses
        self.api_status_timestamp = datetime.now()
        
        return statuses


    def _build_api_status_panel(self, status_panel):
        """Build the static structure of the API status panel.

        Shows grey dots for APIs that need a network check, and orange dots
        immediately for APIs with no key configured. The panel is always fully
        populated so the layout never shifts when results arrive.
        """
        self.status_canvases = {}
        self.status_tooltips = {}

        style = ttk.Style()
        bg_color = style.lookup('TFrame', 'background')

        color_map = {
            'ok': '#28a745',
            'error': '#dc3545',
            'no_key': '#fd7e14',
            'unknown': '#6c757d',
        }

        # Determine initial dot color: orange if key is known missing, grey otherwise
        initial_statuses = {
            'companies_house': 'unknown' if self.api_key else 'no_key',
            'charity_commission': 'unknown' if self.charity_api_key else 'no_key',
            'grantnav': 'unknown',
            'contracts_finder': 'unknown',
        }

        api_configs = [
            ('companies_house',   'Companies House',      0, 0, (0, 15)),
            ('charity_commission','Charity Commission',   0, 1, (0, 0)),
            ('grantnav',         'GrantNav (360Giving)', 1, 0, (0, 15)),
            ('contracts_finder', 'Contracts Finder',     1, 1, (0, 0)),
        ]

        grid_frame = ttk.Frame(status_panel)
        grid_frame.pack(fill=tk.X)

        for key, label_text, row, col, padx in api_configs:
            frame = ttk.Frame(grid_frame)
            frame.grid(row=row, column=col, sticky='w', padx=padx, pady=2)

            status = initial_statuses[key]
            color = color_map.get(status, '#6c757d')

            canvas = tk.Canvas(frame, width=12, height=12, highlightthickness=0)
            canvas.configure(background=bg_color)
            canvas.pack(side=tk.LEFT, padx=(0, 3))
            canvas.create_oval(2, 2, 10, 10, fill=color, outline=color, tags='dot')
            self.status_canvases[key] = canvas

            ttk.Label(frame, text=label_text, font=('Segoe UI', 7)).pack(side=tk.LEFT)

            tooltip_texts = {
                'ok':      f'{label_text}: Connected ✓',
                'error':   f'{label_text}: Connection failed',
                'no_key':  f'{label_text}: No API key configured',
                'unknown': f'{label_text}: Status unknown',
            }
            self.status_tooltips[key] = Tooltip(frame, tooltip_texts.get(status, label_text))

        # Footer: timestamp + refresh button
        footer_frame = ttk.Frame(status_panel)
        footer_frame.pack(fill=tk.X, pady=(5, 0))

        self.status_timestamp_label = ttk.Label(
            footer_frame,
            text="Checking APIs...",
            font=('Segoe UI', 7),
            foreground='gray'
        )
        self.status_timestamp_label.pack(side=tk.LEFT)

        refresh_btn = ttk.Button(
            footer_frame,
            text="↻",
            width=2,
            command=self.refresh_api_status
        )
        refresh_btn.pack(side=tk.RIGHT)
        Tooltip(refresh_btn, "Refresh API status")

    def _update_api_status_display(self):
        """Update dot colours and timestamp in place after a status check."""
        if not self.status_canvases or not self.api_statuses:
            return

        color_map = {
            'ok':      '#28a745',
            'error':   '#dc3545',
            'no_key':  '#fd7e14',
            'unknown': '#6c757d',
        }

        api_labels = {
            'companies_house':   'Companies House',
            'charity_commission':'Charity Commission',
            'grantnav':          'GrantNav (360Giving)',
            'contracts_finder':  'Contracts Finder',
        }

        tooltip_templates = {
            'ok':      '{}: Connected ✓',
            'error':   '{}: Connection failed',
            'no_key':  '{}: No API key configured',
            'unknown': '{}: Status unknown',
        }

        for key, canvas in self.status_canvases.items():
            if not canvas.winfo_exists():
                continue
            status = self.api_statuses.get(key, 'unknown')
            color = color_map.get(status, '#6c757d')
            canvas.delete('dot')
            canvas.create_oval(2, 2, 10, 10, fill=color, outline=color, tags='dot')

            if key in self.status_tooltips:
                label = api_labels.get(key, key)
                self.status_tooltips[key].text = tooltip_templates.get(status, '{}').format(label)

        if self.status_timestamp_label and self.status_timestamp_label.winfo_exists():
            self.status_timestamp_label.config(text=f"Checked: {self._get_time_since_check()}")

    def _get_time_since_check(self):
        """Return human-readable time since last API check."""
        if not self.api_status_timestamp:
            return "never"
        
        delta = datetime.now() - self.api_status_timestamp
        seconds = int(delta.total_seconds())
        
        if seconds < 60:
            return "just now"
        elif seconds < 3600:
            mins = seconds // 60
            return f"{mins} min{'s' if mins != 1 else ''} ago"
        else:
            hours = seconds // 3600
            return f"{hours} hour{'s' if hours != 1 else ''} ago"

    def refresh_api_status(self):
        """Force refresh of API status, updating dots and timestamp in place."""
        # Grey out all dots to signal that a check is in progress
        for canvas in self.status_canvases.values():
            if canvas.winfo_exists():
                canvas.delete('dot')
                canvas.create_oval(2, 2, 10, 10, fill='#6c757d', outline='#6c757d', tags='dot')

        if self.status_timestamp_label and self.status_timestamp_label.winfo_exists():
            self.status_timestamp_label.config(text="Checking APIs...")

        def run_check():
            self.check_api_status()
            if self.status_panel and self.status_panel.winfo_exists():
                self.after(0, self._update_api_status_display)

        threading.Thread(target=run_check, daemon=True).start()

    # ── Working Set → Network Analytics ─────────────────────────────

    def _send_working_set_to_network(self, tree=None) -> None:
        """Build graph data from working set entities and load into Network Analytics.

        Replicates the Bulk Entity Search → Export Graph Data flow:
        fetches officers/PSCs/trustees for each entity, writes a 7-column CSV,
        then navigates to Network Analytics with the file pre-loaded.

        If *tree* is provided and has a selection, only the selected entities
        are sent. Otherwise all entities in the working set are used.
        """
        entities = self._collect_working_set_entities()
        if not entities:
            messagebox.showinfo("Working Set", "No entities in working set.")
            return

        # Honour explicit tree selection
        if tree is not None:
            try:
                sel = tree.selection()
                if not sel:
                    messagebox.showinfo("Working Set", "No entities selected.")
                    return
                indices = [tree.index(item) for item in sel]
                entities = [entities[i] for i in indices if i < len(entities)]
            except tk.TclError:
                pass

        # Filter out inactive charities
        inactive = [e for e in entities if not e.get("active", True)]
        active_entities = [e for e in entities if e.get("active", True)]
        if inactive and not active_entities:
            messagebox.showwarning(
                "Working Set",
                "All entities in the working set are inactive charities "
                "and cannot be sent to Network Analytics."
            )
            return
        if inactive:
            names = ", ".join(e.get("name", "?") for e in inactive)
            messagebox.showinfo(
                "Working Set",
                f"Skipping inactive charities: {names}"
            )
        entities = active_entities

        # Show a simple progress dialog
        progress_win = tk.Toplevel(self)
        progress_win.title("Building Network Data")
        progress_win.transient(self)
        progress_win.geometry("360x100")
        progress_win.resizable(False, False)
        progress_lbl = ttk.Label(
            progress_win, text="Fetching entity data...",
            font=("Segoe UI", 10), padding=20
        )
        progress_lbl.pack(fill=tk.BOTH, expand=True)

        def _build():
            try:
                csv_path = self._build_ws_graph_csv(entities, progress_lbl)
            except Exception as e:
                log_message(f"Network preload failed: {e}")
                csv_path = None

            def _navigate():
                try:
                    progress_win.destroy()
                except tk.TclError:
                    pass
                if csv_path:
                    self._navigate_network_with_csv(csv_path)

            self.after(0, _navigate)

        threading.Thread(target=_build, daemon=True).start()

    def _build_ws_graph_csv(self, entities, progress_lbl) -> str:
        """Build a graph CSV from working set entities (runs in background thread).

        Returns the path to the temporary CSV file.
        """
        import csv as csv_mod
        import tempfile
        from .utils.helpers import (
            extract_address_string, clean_address_string,
            format_address_label, get_canonical_name_key,
        )

        rows = []  # list of 7-element tuples

        for i, ent in enumerate(entities):
            num = ent.get("company_number", ent.get("number", ""))
            name = ent.get("name", "Unknown")

            self.after(0, lambda n=name, idx=i, tot=len(entities):
                       progress_lbl.configure(
                           text=f"Processing {idx+1}/{tot}: {n}"))

            if not num:
                continue

            # Determine type — charity numbers are short pure digits
            is_charity = num.isdigit() and len(num) <= 7

            if is_charity:
                self._build_charity_graph_rows(
                    num, name, rows, extract_address_string,
                    clean_address_string, format_address_label,
                    get_canonical_name_key,
                )
            else:
                self._build_company_graph_rows(
                    num, name, rows, extract_address_string,
                    clean_address_string, format_address_label,
                    get_canonical_name_key,
                )

        if not rows:
            return None

        # Write CSV
        fd, csv_path = tempfile.mkstemp(prefix="Seed-ws-", suffix=".csv")
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            writer = csv_mod.writer(f)
            writer.writerow([
                "source_id", "source_label", "source_type",
                "target_id", "target_label", "target_type", "relationship"
            ])
            for row in rows:
                writer.writerow(row)

        log_message(f"Working set graph CSV: {len(rows)} edges written to {csv_path}")
        return csv_path

    def _build_company_graph_rows(self, company_number, company_name, rows,
                                   extract_addr, clean_addr, fmt_addr, canon_key):
        """Fetch company data and append graph edge rows."""
        profile, _ = ch_get_data(
            self.api_key, self.ch_token_bucket, f"/company/{company_number}"
        )
        officers, _ = ch_get_data(
            self.api_key, self.ch_token_bucket,
            f"/company/{company_number}/officers?items_per_page=100"
        )
        pscs, _ = ch_get_data(
            self.api_key, self.ch_token_bucket,
            f"/company/{company_number}/persons-with-significant-control?items_per_page=100"
        )

        if profile:
            company_name = profile.get("company_name", company_name)

        # Registered address
        if profile:
            addr_data = profile.get("registered_office_address", {})
            raw_addr = extract_addr(addr_data)
            if raw_addr:
                addr_id = clean_addr(raw_addr)
                if addr_id:
                    rows.append((
                        company_number, company_name, "company",
                        addr_id, fmt_addr(raw_addr).replace("\n", " "), "address",
                        "registered_at"
                    ))

        # Officers
        if officers:
            for officer in officers.get("items", []):
                oname = officer.get("name")
                if not oname:
                    continue
                dob = officer.get("date_of_birth")
                pkey = canon_key(oname, dob)
                role = officer.get("officer_role", "officer")
                rows.append((
                    company_number, company_name, "company",
                    pkey, oname, "person", role
                ))
                # Officer address
                oaddr_raw = extract_addr(officer.get("address"))
                if oaddr_raw:
                    oaddr_clean = clean_addr(oaddr_raw)
                    if oaddr_clean:
                        rows.append((
                            pkey, oname, "person",
                            oaddr_clean, fmt_addr(oaddr_raw).replace("\n", " "), "address",
                            "correspondence_at"
                        ))

        # PSCs
        if pscs:
            for psc in pscs.get("items", []):
                pname = psc.get("name")
                if not pname:
                    continue
                dob = psc.get("date_of_birth")
                pkey = canon_key(pname, dob)
                rows.append((
                    company_number, company_name, "company",
                    pkey, pname, "person", "psc"
                ))
                paddr_raw = extract_addr(psc.get("address"))
                if paddr_raw:
                    paddr_clean = clean_addr(paddr_raw)
                    if paddr_clean:
                        rows.append((
                            pkey, pname, "person",
                            paddr_clean, fmt_addr(paddr_raw).replace("\n", " "), "address",
                            "correspondence_at"
                        ))

    def _build_charity_graph_rows(self, charity_number, charity_name, rows,
                                   extract_addr, clean_addr, fmt_addr, canon_key):
        """Fetch charity data and append graph edge rows."""
        details, _ = _cc_get_data(
            self.charity_api_key, f"/charitydetails/{charity_number}/0"
        )
        trustees, _ = _cc_get_data(
            self.charity_api_key, f"/charitytrusteenamesV2/{charity_number}/0"
        )

        node_id = f"CC-{charity_number}"
        if details:
            charity_name = details.get("charity_name", charity_name)

        # Charity address
        if details:
            charity_addr = details.get("charity_contact_address")
            if charity_addr:
                addr_clean = clean_addr(charity_addr)
                if addr_clean:
                    rows.append((
                        node_id, charity_name, "charity",
                        addr_clean, fmt_addr(charity_addr).replace("\n", " "), "address",
                        "registered_at"
                    ))

        # Trustees
        if trustees and isinstance(trustees, list):
            for trustee in trustees:
                tname = trustee.get("trustee_name")
                if not tname:
                    continue
                pkey = canon_key(tname, None)
                rows.append((
                    node_id, charity_name, "charity",
                    pkey, tname, "person", "trustee"
                ))
                trustee_addr = trustee.get("trustee_address")
                if trustee_addr:
                    taddr_clean = clean_addr(trustee_addr)
                    if taddr_clean:
                        rows.append((
                            pkey, tname, "person",
                            taddr_clean, fmt_addr(trustee_addr).replace("\n", " "), "address",
                            "correspondence_at"
                        ))

    def _navigate_network_with_csv(self, csv_path: str, source_label: str = None) -> None:
        """Navigate to Network Analytics with the given CSV file pre-loaded."""
        self.clear_container()
        from .modules.network_analytics import NetworkAnalytics
        module = NetworkAnalytics(
            self,
            self.show_main_menu,
            self.ch_token_bucket,
            api_key=self.api_key,
            help_key="network_creator"
        )
        self._update_sidebar_active("network_workbench")
        self._refresh_working_set_indicator()

        # Pre-load the CSV file
        if csv_path not in module.source_files:
            module.source_files.append(csv_path)
            module.file_listbox.insert(tk.END, f"FILE: {os.path.basename(csv_path)}")
        if module.source_files:
            module.refine_section.set_enabled(True)
            module._mark_files_changed()

        # Show working set source label if provided
        if source_label and hasattr(module, "_ws_source_label"):
            module._ws_source_label.configure(text=source_label)
            module._ws_source_label.pack(fill=tk.X, pady=(5, 0))

    # --- Module Navigation Methods ---
    # These methods load the respective investigation modules.
    # Each module is imported and instantiated when needed.
    
    def show_director_investigation(self, prefill_name=None,
                                    prefill_year=None, prefill_month=None) -> None:
        """Show the Director Search module."""
        self.clear_container()
        # Import here to avoid circular imports and speed up startup
        from .modules.director_search import DirectorSearch
        DirectorSearch(self, self.api_key, self.show_main_menu, self.ch_token_bucket,
                       prefill_name=prefill_name, prefill_year=prefill_year,
                       prefill_month=prefill_month)
        self._update_sidebar_active("director_search")
        self._refresh_working_set_indicator()
    
    def show_ubo_investigation(self, prefill_company=None,
                               prefill_company_name=None,
                               prefill_entities=None) -> None:
        """Show the UBO Tracer module."""
        self.clear_container()
        from .modules.ubo_tracer import UltimateBeneficialOwnershipTracer
        try:
            UltimateBeneficialOwnershipTracer(self, self.api_key, self.show_main_menu,
                                               self.ch_token_bucket,
                                               prefill_company=prefill_company,
                                               prefill_company_name=prefill_company_name,
                                               prefill_entities=prefill_entities)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to open UBO Tracer:\n{e}")
            import traceback
            traceback.print_exc()
            self.show_main_menu()
            return
        self._update_sidebar_active("ubo_tracer")
        self._refresh_working_set_indicator()
    
    def show_grants_investigation(self, prefill_entities=None, prefill_source=None) -> None:
        """Show the Grants Search module."""
        self.clear_container()
        from .modules.grants_search import GrantsSearch
        GrantsSearch(self, self.api_key, self.show_main_menu,
                     prefill_entities=prefill_entities,
                     prefill_source=prefill_source)
        self._update_sidebar_active("grants_search")
        self._refresh_working_set_indicator()
    
    def show_data_match_investigation(self) -> None:
        """Show the Data Match module."""
        self.clear_container()
        from .modules.data_match import DataMatch
        DataMatch(self, self.show_main_menu, self.api_key)
        self._update_sidebar_active("data_match")
        self._refresh_working_set_indicator()
    
    def show_network_graph_creator(self) -> None:
        """Show the Network Analytics module."""
        self.clear_container()
        from .modules.network_analytics import NetworkAnalytics
        NetworkAnalytics(
            self,
            self.show_main_menu,
            self.ch_token_bucket,
            api_key=self.api_key,
            help_key="network_creator"
        )
        self._update_sidebar_active("network_workbench")
        self._refresh_working_set_indicator()

    def show_enhanced_dd(self, prefill_entity=None, prefill_entities=None) -> None:
        """Show the Enhanced Due Diligence module."""
        self.clear_container()
        from .modules.enhanced_dd import EnhancedDueDiligence
        EnhancedDueDiligence(
            self, self.api_key, self.show_main_menu, self.ch_token_bucket,
            charity_api_key=self.charity_api_key,
            prefill_entity=prefill_entity,
            prefill_entities=prefill_entities,
        )
        self._update_sidebar_active("edd")
        self._refresh_working_set_indicator()

    def show_unified_search(self, prefill_entities=None) -> None:
        """Show the Unified Search module."""
        self.clear_container()
        from .modules.unified_search import CompanyCharitySearch
        CompanyCharitySearch(
            self,
            self.show_main_menu,
            self.api_key,
            self.charity_api_key,
            self.ch_token_bucket,
            prefill_entities=prefill_entities
        )
        self._update_sidebar_active("bulk_entity_search")
        self._refresh_working_set_indicator()

    def show_contracts_finder(self):
        """Show the Contracts Finder module."""
        from .modules.contracts_finder import ContractsFinderInvestigation
        self.clear_container()
        ContractsFinderInvestigation(
            self,
            self.show_main_menu,
            self.ch_token_bucket,
            self.api_key
        )
        self._update_sidebar_active("contracts_finder")
        self._refresh_working_set_indicator()
