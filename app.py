# multitool/app.py
"""
Main Application Class

This module contains the main App class that manages the application window,
navigation, API keys, and module loading.
"""

import os
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
    CH_API_RATE_LIMIT,
    API_BASE_URL,
    CHARITY_API_BASE_URL,
    GRANTNAV_API_BASE_URL
)
from .help_content import HELP_CONTENT
from .utils.helpers import log_message
from .utils.token_bucket import TokenBucket
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
        self.geometry("750x875")
        
        # Initialize ttkbootstrap style
        self.style = tb.Style(theme="superhero")
        self.themed_window = self.style.master
        
        # Settings
        self.dark_theme_enabled = tk.BooleanVar(value=True)
        self.font_size = tk.IntVar(value=10)
        
        # Rate limiter for Companies House API
        # 600 requests per 5 mins = 2 requests/sec
        # Capacity of 50 allows for bursts
        self.ch_token_bucket = TokenBucket(capacity=50, refill_rate=2)
        
        # API keys
        self.api_key = ""
        self.charity_api_key = ""
        self.api_key_saved = False
        self.charity_api_key_saved = False
        
        # API status cache
        self.api_statuses = None
        self.api_status_timestamp = None
        
        # Main container
        self.container = ttk.Frame(self, padding=10)
        self.container.pack(fill=tk.BOTH, expand=True)
        
        # Bind Return key to invoke buttons
        self.bind_class("TButton", "<Return>", lambda e: e.widget.invoke())
        
        log_message("Application started.")
        
        # Create menu bar
        self._create_menu_bar()

        # Create API status variables
        self.api_statuses = {}
        self.api_status_timestamp = None
        self.status_panel = None
        
        # Load API keys and show appropriate screen
        self.load_api_keys()

    def _create_menu_group(self, parent, title, modules):
        """
        Helper to create a visually distinct group of menu buttons.
        
        Args:
            parent: The parent widget (Frame).
            title: The title of the group (e.g., "Discovery").
            modules: List of tuples (Button Text, Command, State, Description, Bootstyle).
        """
        # Create a labeled frame for the category
        frame = ttk.LabelFrame(parent, text=f" {title} ", padding=15, bootstyle="default")
        frame.pack(fill=tk.X, pady=10, anchor="n")
        
        for name, command, state, desc, style in modules:
            # Create a container for each row (Button + Description)
            row = ttk.Frame(frame)
            row.pack(fill=tk.X, pady=6)
            
            # Action Button
            btn = ttk.Button(
                row, 
                text=name, 
                command=command, 
                state=state, 
                bootstyle=style, 
                width=22  # Fixed width for alignment
            )
            btn.pack(side=tk.LEFT, padx=(0, 12))
            
            # Description Label (Next to the button)
            desc_lbl = ttk.Label(
                row, 
                text=desc, 
                font=("Segoe UI", 9), 
                foreground="gray"
            )
            desc_lbl.pack(side=tk.LEFT, fill=tk.X, expand=True)
            
            # Add tooltip for good measure
            Tooltip(btn, desc)
    
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
        
        # Theme toggle
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
        
        # Font size slider
        font_slider_frame = ttk.Frame(theme_frame)
        font_slider_frame.pack(side="left", padx=20)
        ttk.Label(font_slider_frame, text="Text Size:").pack(side="left", padx=(0, 5))
        font_slider = ttk.Scale(
            font_slider_frame,
            from_=8,
            to=16,
            variable=self.font_size,
            command=lambda val: self.font_size.set(round(float(val))),
        )
        font_slider.pack(side="left")
        
        # Buttons
        button_frame = ttk.Frame(settings_win)
        button_frame.pack(pady=20)
        
        ttk.Button(
            button_frame,
            text="Apply",
            command=self._update_font_size,
            bootstyle="success",
        ).pack(side="left", padx=5)
        
        ttk.Button(
            button_frame,
            text="Close",
            command=settings_win.destroy,
            bootstyle="primary",
        ).pack(side="left", padx=5)
    
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
        ch_entry = ttk.Entry(ch_frame, width=50)
        ch_entry.pack(pady=5)
        
        # Charity Commission
        cc_frame = ttk.LabelFrame(frame, text="Charity Commission API Key", padding=10)
        cc_frame.pack(fill=tk.X, pady=5)
        cc_entry = ttk.Entry(cc_frame, width=50)
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
        """Display the main menu with a categorized dashboard layout (Collision Fixed)."""
        self.unbind("<Return>")
        self.clear_container()
        self.title("Multi-Tool - Dashboard")
        self.geometry("1100x600")  # Height reduced slightly as requested

        # --- 1. Top Bar Container (Header Only) ---
        top_bar = ttk.Frame(self.container)
        top_bar.pack(fill=tk.X, padx=20, pady=(15, 10), side=tk.TOP)

        # Header Section
        header_frame = ttk.Frame(top_bar)
        header_frame.pack(side=tk.LEFT, fill=tk.Y)
        
        ttk.Label(
            header_frame, 
            text="Module Suite", 
            font=("Helvetica", 20, "bold"),
            bootstyle="primary"
        ).pack(anchor="w")
        
        ttk.Label(
            header_frame, 
            text="Select a module below to begin your analysis.", 
            font=("Helvetica", 11),
            foreground="gray"
        ).pack(anchor="w")

        # --- 2. Main Dashboard Area ---
        dashboard_frame = ttk.Frame(self.container)
        dashboard_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=5)

        # Define Button States
        ch_enabled = tk.NORMAL if self.api_key else tk.DISABLED
        unified_enabled = tk.NORMAL if self.api_key or self.charity_api_key else tk.DISABLED
        
        # Create Columns
        left_col = ttk.Frame(dashboard_frame)
        left_col.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))
        
        right_col = ttk.Frame(dashboard_frame)
        right_col.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(10, 0))

        # --- GROUP 1: Search & Discovery (Left Column) ---
        self._create_menu_group(
            parent=left_col, 
            title="Search & Discovery", 
            modules=[
                ("Bulk Entity Search", self.show_unified_search, unified_enabled, 
                 "Search companies & charities via mixed ID file", "primary"),
                ("Contracts Finder", self.show_contracts_finder, ch_enabled, 
                 "Find government contracts & enrich with Companies House data", "primary"),
                ("Grants Search", self.show_grants_investigation, tk.NORMAL, 
                 "Analyse funding data from 360Giving", "primary"),
            ]
        )

        # --- GROUP 2: Data Utilities (Left Column - Bottom) ---
        self._create_menu_group(
            parent=left_col,
            title="Data Utilities",
            modules=[
                ("Data Match", self.show_data_match_investigation, tk.NORMAL, 
                 "Fuzzy match two independent datasets", "secondary"),
                ("Network Analytics", self.show_network_graph_creator, tk.NORMAL, 
                 "Build and visualise relationship graphs", "secondary"),
            ]
        )

        # --- GROUP 3: Deep Dive & Investigation (Right Column) ---
        self._create_menu_group(
            parent=right_col, 
            title="Deep Dive Investigation", 
            modules=[
                ("Enhanced Due Diligence", self.show_enhanced_dd, ch_enabled, 
                 "Generate full financial & risk reports", "success"),
                ("UBO Tracer", self.show_ubo_investigation, ch_enabled, 
                 "Trace parent companies and ownership structures", "success"),
                ("Director Search", self.show_director_investigation, ch_enabled, 
                 "Locate all appointments for a specific director", "success"),
            ]
        )
        
        # --- 3. Footer Area (User Guide Left, Status Right) ---
        footer_frame = ttk.Frame(self.container)
        footer_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=20, pady=(0, 15))

        # User Guide (Left)
        footer_btn = ttk.Button(
            footer_frame,
            text="📖 Open User Guide",
            command=self.show_main_guide,
            bootstyle="link",
        )
        footer_btn.pack(side=tk.LEFT, anchor="sw", pady=(5, 0))

        # API Status Panel (Right) - Now packed into the footer frame
        self.status_panel = ttk.LabelFrame(footer_frame, text="API Status", padding=5)
        self.status_panel.pack(side=tk.RIGHT, anchor="se")
        
        # Load Status Logic (Same as before)
        if self.api_statuses:
            self._display_api_status(self.status_panel)
        else:
            checking_label = ttk.Label(
                self.status_panel,
                text="Checking APIs...",
                font=('Segoe UI', 7, 'italic'),
                foreground='gray'
            )
            checking_label.pack()
            
            def run_check():
                self.check_api_status()
                if checking_label.winfo_exists():
                    self.after(0, checking_label.destroy)
                if self.status_panel.winfo_exists():
                    self.after(0, lambda: self._display_api_status(self.status_panel))
            
            threading.Thread(target=run_check, daemon=True).start()
    
    def show_main_guide(self) -> None:
        """Show the main help window."""
        HelpWindow(self, "User Guide", HELP_CONTENT["main"])
    
    def clear_cache_and_logs(self) -> None:
        """Clear temporary files and logs."""
        if not os.path.exists(CONFIG_DIR):
            messagebox.showinfo("Info", "No cache directory found.")
            return
        
        if messagebox.askyesno("Confirm", "Delete all cached files and logs?"):
            import shutil
            try:
                shutil.rmtree(CONFIG_DIR)
                os.makedirs(CONFIG_DIR)
                messagebox.showinfo("Success", "Cache and logs cleared.")
            except Exception as e:
                messagebox.showerror("Error", f"Could not clear cache: {e}")
    
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
            except:
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
            except:
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
        except:
            statuses['grantnav'] = 'error'

        # --- NEW: Test Contracts Finder (no key required) ---
        try:
            # Import here to avoid top-level dependency
            from .api.contracts_finder import check_api_status as check_cf
            is_online = check_cf()
            statuses['contracts_finder'] = 'ok' if is_online else 'error'
        except Exception as e:
            # log_message(f"Contracts Finder check failed: {e}") 
            statuses['contracts_finder'] = 'error'
        
        # Cache results
        self.api_statuses = statuses
        self.api_status_timestamp = datetime.now()
        
        return statuses


    def _create_status_indicator(self, parent, label, status):
        """Create a single status indicator with colored circle."""
        frame = ttk.Frame(parent)
        frame.pack(anchor='w', pady=2)
        
        # Status circle (using Unicode circle characters)
        color_map = {
            'ok': ('●', '#28a745'),
            'error': ('●', '#dc3545'),
            'no_key': ('●', '#fd7e14'),
            'unknown': ('●', '#6c757d')
        }
        
        symbol, color = color_map.get(status, ('●', 'gray'))
        
        style = ttk.Style()
        bg_color = style.lookup('TFrame', 'background')

        # Create a small canvas to draw a colored circle
        canvas = tk.Canvas(frame, width=12, height=12, highlightthickness=0)

        canvas.configure(background=bg_color)
        canvas.pack(side=tk.LEFT, padx=(0, 3))

        # Draw a filled circle
        canvas.create_oval(2, 2, 10, 10, fill=color, outline=color)
        
        text_label = ttk.Label(frame, text=label, font=('Segoe UI', 7))
        text_label.pack(side=tk.LEFT)
        
        # Tooltip explaining status
        tooltip_text = {
            'ok': f'{label}: Connected ✓',
            'error': f'{label}: Connection failed',
            'no_key': f'{label}: No API key configured',
            'unknown': f'{label}: Status unknown'
        }
        Tooltip(frame, tooltip_text.get(status, label))
        
        return frame

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
        """Force refresh of API status."""
        # Clear the status panel and show checking message
        for widget in self.status_panel.winfo_children():
            widget.destroy()
        
        checking_label = ttk.Label(
            self.status_panel,
            text="Checking APIs...",
            font=('Segoe UI', 7, 'italic'),
            foreground='gray'
        )
        checking_label.pack()
        
        # Run check in background
        def run_check():
            statuses = self.check_api_status()
            
            # Update UI on main thread (only if widgets still exist)
            if checking_label.winfo_exists():
                self.after(0, checking_label.destroy)
            if self.status_panel.winfo_exists():
                self.after(0, lambda: self._display_api_status(self.status_panel))
        
        threading.Thread(target=run_check, daemon=True).start()

    def _display_api_status(self, status_panel):
        """Display API status indicators (used by both initial load and refresh)."""
        # Safety check - panel may have been destroyed if user navigated away
        if not status_panel.winfo_exists():
            return
        
        if not self.api_statuses:
            return
        
        self._create_status_indicator(
            status_panel, 
            "Companies House", 
            self.api_statuses['companies_house']
        )
        self._create_status_indicator(
            status_panel, 
            "Charity Commission", 
            self.api_statuses['charity_commission']
        )
        self._create_status_indicator(
            status_panel, 
            "GrantNav (360Giving)", 
            self.api_statuses['grantnav']
        )
        
        # --- NEW: Add Contracts Finder Indicator ---
        self._create_status_indicator(
            status_panel, 
            "Contracts Finder", 
            self.api_statuses.get('contracts_finder', 'unknown')
        )
        
        # Add timestamp and refresh button
        footer_frame = ttk.Frame(status_panel)
        footer_frame.pack(fill=tk.X, pady=(5, 0))
        
        timestamp_label = ttk.Label(
            footer_frame,
            text=f"Checked: {self._get_time_since_check()}",
            font=('Segoe UI', 6),
            foreground='gray'
        )
        timestamp_label.pack(side=tk.LEFT)
        
        refresh_btn = ttk.Button(
            footer_frame,
            text="↻",
            width=2,
            command=self.refresh_api_status
        )
        refresh_btn.pack(side=tk.RIGHT)
        Tooltip(refresh_btn, "Refresh API status")
    
    # --- Module Navigation Methods ---
    # These methods load the respective investigation modules.
    # Each module is imported and instantiated when needed.
    
    def show_director_investigation(self) -> None:
        """Show the Director Search module."""
        self.geometry("1200x600")
        self.clear_container()
        # Import here to avoid circular imports and speed up startup
        from .modules.director_search import DirectorSearch
        DirectorSearch(self, self.api_key, self.show_main_menu, self.ch_token_bucket)
    
    def show_ubo_investigation(self) -> None:
        """Show the UBO Tracer module."""
        self.geometry("800x600")
        self.clear_container()
        from .modules.ubo_tracer import UltimateBeneficialOwnershipTracer
        UltimateBeneficialOwnershipTracer(self, self.api_key, self.show_main_menu, self.ch_token_bucket)
    
    def show_grants_investigation(self) -> None:
        """Show the Grants Search module."""
        self.geometry("800x600")
        self.clear_container()
        from .modules.grants_search import GrantsSearch
        GrantsSearch(self, self.api_key, self.show_main_menu)
    
    def show_data_match_investigation(self) -> None:
        """Show the Data Match module."""
        self.geometry("800x600")
        self.clear_container()
        from .modules.data_match import DataMatch
        DataMatch(self, self.show_main_menu, self.api_key)
    
    def show_network_graph_creator(self) -> None:
        """Show the Network Analytics module."""
        self.geometry("900x800")
        self.clear_container()
        from .modules.network_analytics import NetworkAnalytics
        NetworkAnalytics(
            self,
            self.show_main_menu,
            self.ch_token_bucket,
            api_key=self.api_key,
            help_key="network_creator"
        )
    
    def show_enhanced_dd(self) -> None:
        """Show the Enhanced Due Diligence module."""
        self.geometry("900x850")
        self.clear_container()
        from .modules.enhanced_dd import EnhancedDueDiligence
        EnhancedDueDiligence(self, self.api_key, self.show_main_menu, self.ch_token_bucket)
    
    def show_unified_search(self) -> None:
        """Show the Unified Search module."""
        self.geometry("1100x700")
        self.clear_container()
        from .modules.unified_search import CompanyCharitySearch
        CompanyCharitySearch(
            self,
            self.show_main_menu,
            self.api_key,
            self.charity_api_key,
            self.ch_token_bucket
        )

    def show_contracts_finder(self):
        """Show the Contracts Finder module."""
        from .modules.contracts_finder import ContractsFinderInvestigation
        self.geometry("800x600")
        self.clear_container()
        ContractsFinderInvestigation(
            self,
            self.show_main_menu,
            self.ch_token_bucket,
            self.api_key
        )
