# multitool/app.py
"""
Main Application Class

This module contains the main App class that manages the application window,
navigation, API keys, and module loading.
"""

import os
import subprocess
import sys
import threading
import tkinter as tk
from tkinter import ttk, messagebox
import tkinter.font
import ttkbootstrap as tb
import keyring
import requests
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
from .utils.helpers import log_message
from .utils.token_bucket import TokenBucket
from .utils.settings import load_settings, save_settings, derive_initial_params
from .utils.app_state import AppState
from .ui.tooltip import Tooltip
from .ui.help_window import HelpWindow


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
        
        self.title("Multi-Tool")
        self.geometry("1320x700")
        self.minsize(1320, 700)
        
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

        # Working Set indicator (built into its own frame)
        self._build_working_set_indicator(sb)

        # Network Analytics Workbench — green, at bottom
        self._add_sidebar_button(sb, "Network Workbench",
                                 "network_workbench", "success",
                                 self.show_network_graph_creator)

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
            height=8, selectmode="none"
        )
        self._working_set_tree.heading("name", text="Name")
        self._working_set_tree.heading("number", text="Number")
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
        send_menu = tk.Menu(self._working_set_send_menu, tearoff=0)
        send_menu.add_command(label="Network Workbench",
                              command=self.show_network_graph_creator)
        send_menu.add_command(label="Enhanced Due Diligence",
                              command=self.show_enhanced_dd)
        self._working_set_send_menu.configure(menu=send_menu)
        self._working_set_send_menu.pack(side=tk.LEFT, padx=(0, 4))

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
        """Display the home screen landing view."""
        self.unbind("<Return>")
        self.clear_container()
        self.title("Multi-Tool - Dashboard")

        # Ensure sidebar is visible
        self._show_sidebar()

        # --- Header with API status ---
        header_frame = ttk.Frame(self.container)
        header_frame.pack(fill=tk.X, padx=20, pady=(15, 10))

        # Left side: Title and subtitle
        title_frame = ttk.Frame(header_frame)
        title_frame.pack(side=tk.LEFT, anchor="w")

        ttk.Label(
            title_frame,
            text="Multi-Tool Dashboard",
            font=("Helvetica", 20, "bold"),
            bootstyle="primary"
        ).pack(anchor="w")

        ttk.Label(
            title_frame,
            text="Select a module from the sidebar to begin your analysis.",
            font=("Helvetica", 11),
            foreground="gray"
        ).pack(anchor="w")

        # Right side: API Status Panel
        self.status_panel = ttk.LabelFrame(header_frame, text="API Status", padding=5)
        self.status_panel.pack(side=tk.RIGHT, anchor="ne")

        self._build_api_status_panel(self.status_panel)

        if self.api_statuses:
            self._update_api_status_display()
        else:
            def run_check():
                self.check_api_status()
                if self.status_panel and self.status_panel.winfo_exists():
                    self.after(0, self._update_api_status_display)

            threading.Thread(target=run_check, daemon=True).start()

        # --- Footer ---
        footer_frame = ttk.Frame(self.container)
        footer_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=20, pady=(0, 15))

        ttk.Button(
            footer_frame,
            text="\U0001f4d6 Open User Guide",
            command=self.show_main_guide,
            bootstyle="link",
        ).pack(side=tk.LEFT, anchor="sw", pady=(5, 0))

        # --- Sidebar state ---
        self._update_sidebar_active("home")
        self._refresh_working_set_indicator()
    
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

    # --- Module Navigation Methods ---
    # These methods load the respective investigation modules.
    # Each module is imported and instantiated when needed.
    
    def show_director_investigation(self) -> None:
        """Show the Director Search module."""
        self.clear_container()
        # Import here to avoid circular imports and speed up startup
        from .modules.director_search import DirectorSearch
        DirectorSearch(self, self.api_key, self.show_main_menu, self.ch_token_bucket)
        self._update_sidebar_active("director_search")
        self._refresh_working_set_indicator()
    
    def show_ubo_investigation(self) -> None:
        """Show the UBO Tracer module."""
        self.clear_container()
        from .modules.ubo_tracer import UltimateBeneficialOwnershipTracer
        UltimateBeneficialOwnershipTracer(self, self.api_key, self.show_main_menu, self.ch_token_bucket)
        self._update_sidebar_active("ubo_tracer")
        self._refresh_working_set_indicator()
    
    def show_grants_investigation(self) -> None:
        """Show the Grants Search module."""
        self.clear_container()
        from .modules.grants_search import GrantsSearch
        GrantsSearch(self, self.api_key, self.show_main_menu)
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

    def show_enhanced_dd(self) -> None:
        """Show the Enhanced Due Diligence module."""
        self.clear_container()
        from .modules.enhanced_dd import EnhancedDueDiligence
        EnhancedDueDiligence(
            self, self.api_key, self.show_main_menu, self.ch_token_bucket,
            charity_api_key=self.charity_api_key,
        )
        self._update_sidebar_active("edd")
        self._refresh_working_set_indicator()

    def show_unified_search(self) -> None:
        """Show the Unified Search module."""
        self.clear_container()
        from .modules.unified_search import CompanyCharitySearch
        CompanyCharitySearch(
            self,
            self.show_main_menu,
            self.api_key,
            self.charity_api_key,
            self.ch_token_bucket
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
