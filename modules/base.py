# multitool/modules/base.py
"""Base class for all investigation modules."""

import csv
import os
import re
import threading
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from typing import List, Dict, Any, Optional, Callable

from ..ui.scrollable_frame import ScrollableFrame
from ..ui.help_window import HelpWindow
from ..help_content import HELP_CONTENT
from ..utils.helpers import log_message


class InvestigationModuleBase(ttk.Frame):
    """
    Base class for all investigation modules.
    
    Provides common functionality including:
    - Standardized header with back button, title, and help button
    - Scrollable content area
    - File loading/parsing
    - CSV export
    - Thread-safe UI updates
    - Cancellation support
    
    Attributes:
        app: Reference to the main application
        api_key: API key for the module (if required)
        back_callback: Function to call when navigating back
        help_key: Key for looking up help content
        cancel_flag: Threading event for cancellation
        original_data: Raw data loaded from input file
        original_headers: Column headers from input file
        results_data: Processed results ready for export
        content_frame: Frame where module-specific UI should be placed
    """
    
    def __init__(
        self,
        parent_app,
        back_callback: Callable,
        api_key: Optional[str] = None,
        help_key: Optional[str] = None
    ):
        """
        Initialize the investigation module.
        
        Args:
            parent_app: Reference to the main App instance
            back_callback: Function to call when back button is clicked
            api_key: API key for the module (optional)
            help_key: Key for looking up help content (optional)
        """
        super().__init__(parent_app.container, padding=10)
        self.app = parent_app
        self.pack(fill=tk.BOTH, expand=True)
        
        self.api_key = api_key
        self.back_callback = back_callback
        self.help_key = help_key
        
        # Threading support
        self.cancel_flag = threading.Event()
        self._after_ids: List[str] = []
        
        # Data storage
        self.original_data: List[Dict[str, Any]] = []
        self.original_headers: List[str] = []
        self.results_data: List[Dict[str, Any]] = []
        
        # Create standardized header
        self._create_header()
        
        # Create scrollable content area
        self.scroller = ScrollableFrame(self)
        self.scroller.pack(fill="both", expand=True)
        self.content_frame = self.scroller.scrollable_frame
    
    def _create_header(self) -> None:
        """Create standardized, centered header for the module."""
        header_frame = ttk.Frame(self, padding=(0, 0, 0, 10))
        header_frame.pack(fill=tk.X, anchor="n")
        
        # Configure 3-column grid with equal side columns
        header_frame.columnconfigure(0, weight=1, uniform='sides')
        header_frame.columnconfigure(1, weight=0)
        header_frame.columnconfigure(2, weight=1, uniform='sides')
        
        # Back button (left)
        back_btn = ttk.Button(
            header_frame,
            text="← Back to Main Menu",
            command=self.safe_go_back,
            bootstyle="secondary"
        )
        back_btn.grid(row=0, column=0, sticky="w")
        
        # Module title (center) - derived from class name
        class_name = self.__class__.__name__
        title = re.sub(r"([A-Z])", r" \1", class_name).strip()
        
        title_label = ttk.Label(
            header_frame,
            text=title,
            font=("Helvetica", 14, "bold")
        )
        title_label.grid(row=0, column=1)
        
        # Help button (right)
        if self.help_key:
            help_btn = ttk.Button(
                header_frame,
                text="Help",
                command=self.show_module_help,
                bootstyle="info-outline"
            )
            help_btn.grid(row=0, column=2, sticky="e")
    
    def _schedule_update(self, func: Callable, *args) -> None:
        """
        Schedule a function to run and track its ID for cancellation.
        
        Args:
            func: Function to schedule
            *args: Arguments to pass to the function
        """
        if self.winfo_exists():
            after_id = self.after(0, func, *args)
            self._after_ids.append(after_id)
    
    def show_module_help(self) -> None:
        """Display the help window for this module."""
        if self.help_key and self.help_key in HELP_CONTENT:
            title = f"{self.help_key.replace('_', ' ').title()} Help"
            HelpWindow(self.app, title, HELP_CONTENT[self.help_key])
    
    def safe_update(self, func: Callable, *args) -> None:
        """
        Schedule a function to run via after() only if widget exists.

        Args:
            func: Function to schedule
            *args: Arguments to pass to the function
        """
        self._schedule_update(func, *args)

    def safe_ui_call(self, func: Callable, *args, **kwargs) -> None:
        """
        Thread-safe UI call dispatcher.

        Schedules a function to run on the main Tkinter thread via after().
        Use this for all UI updates from background threads including:
        - Widget method calls (.config, .insert, .set, etc.)
        - StringVar/IntVar/BooleanVar updates
        - messagebox calls
        - Progress bar updates

        Args:
            func: Function to schedule on the main thread
            *args: Positional arguments to pass to the function
            **kwargs: Keyword arguments to pass to the function
        """
        if self.winfo_exists():
            after_id = self.after(0, lambda: func(*args, **kwargs))
            self._after_ids.append(after_id)
    
    def safe_go_back(self) -> None:
        """Safely navigate back to main menu, cancelling pending operations."""
        self.cancel_flag.set()
        
        # Cancel any pending UI updates
        for after_id in self._after_ids:
            try:
                self.after_cancel(after_id)
            except tk.TclError:
                pass  # Already cancelled
        
        self.after(50, self.back_callback)
    
    def load_file_logic(self, path: str) -> bool:
        """
        Load and parse a CSV file.
        
        Attempts multiple encodings to handle different file formats.
        
        Args:
            path: Path to the CSV file
            
        Returns:
            True if file loaded successfully, False otherwise
        """
        self.original_data, self.original_headers = [], []
        
        try:
            encodings = ["utf-8-sig", "cp1252"]
            
            for enc in encodings:
                try:
                    with open(path, "r", encoding=enc, newline="") as f:
                        reader = csv.DictReader(f)
                        self.original_headers = reader.fieldnames
                        self.original_data = list(reader)
                    
                    if not self.original_headers or not self.original_data:
                        raise ValueError("CSV file is empty or invalid.")
                    
                    log_message(
                        f"Successfully loaded file '{os.path.basename(path)}' "
                        f"with encoding '{enc}'."
                    )
                    return True
                    
                except UnicodeDecodeError:
                    continue
            
            raise ValueError(
                f"Could not decode file with tried encodings: {', '.join(encodings)}"
            )
            
        except Exception as e:
            log_message(f"File loading failed: {e}")
            messagebox.showerror("File Error", f"Could not read file: {e}")
            return False
    
    def get_nested_value(
        self,
        data_dict: Dict[str, Any],
        key_path: str,
        default: Any = ""
    ) -> Any:
        """
        Get a value from a nested dictionary using underscore-separated path.
        
        Args:
            data_dict: Dictionary to search
            key_path: Underscore-separated path (e.g., "address_postal_code")
            default: Default value if path not found
            
        Returns:
            Value at path or default
        """
        keys = key_path.split("_")
        val = data_dict
        
        for key in keys:
            if isinstance(val, list) and val:
                val = val[0]
            if isinstance(val, dict):
                val = val.get(key)
            else:
                return default
        
        return val if val is not None else default
    
    def generic_export_csv(self, headers: List[str]) -> None:
        """
        Export results data to a CSV file.

        The file dialog runs on the main thread (Tk requirement). The actual
        CSV writing is offloaded to a background thread so the GUI stays
        responsive. A transient "Exporting..." label is shown while the
        thread is alive; ``after()`` polls for completion and displays the
        result messagebox back on the main thread (Tk is not thread-safe).

        Args:
            headers: List of column headers to include in export
        """
        if not self.results_data:
            messagebox.showinfo("No Data", "There is no data to export.")
            return

        filepath = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")]
        )

        if not filepath:
            return

        # Snapshot data so the background thread never touches Tk state
        rows_snapshot = list(self.results_data)
        row_count = len(rows_snapshot)
        basename = os.path.basename(filepath)

        # Mutable container for the worker to report its outcome
        result: Dict[str, Any] = {}

        def _write_csv():
            try:
                with open(filepath, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(
                        f,
                        fieldnames=headers,
                        extrasaction="ignore"
                    )
                    writer.writeheader()
                    writer.writerows(rows_snapshot)
                result["success"] = True
            except IOError as e:
                result["error"] = str(e)

        # Show a transient status label
        status_label = ttk.Label(
            self, text="Exporting\u2026", font=("Helvetica", 10, "italic")
        )
        status_label.pack(pady=2)

        worker = threading.Thread(target=_write_csv, daemon=True)
        worker.start()

        def _poll_worker():
            if worker.is_alive():
                self.after(100, _poll_worker)
                return
            # Worker finished — safe to touch Tk again
            status_label.destroy()
            if result.get("success"):
                log_message(
                    f"Successfully exported {row_count} rows to {basename}."
                )
                messagebox.showinfo(
                    "Success",
                    f"Data exported successfully to {basename}"
                )
            else:
                err = result.get("error", "Unknown error")
                log_message(f"Export failed: {err}")
                messagebox.showerror(
                    "Export Error", f"Could not write to file: {err}"
                )

        self.after(100, _poll_worker)
    
    def _update_scrollregion(self) -> None:
        def update():
            if not self.winfo_exists():
                return
            self.update_idletasks()
            self.scroller.scrollable_frame.update_idletasks()
            
            # Explicitly set the frame height to match content
            required_height = self.scroller.scrollable_frame.winfo_reqheight()
            canvas_height = self.scroller.canvas.winfo_height()
            self.scroller.canvas.itemconfig(
                self.scroller.frame_id, 
                height=max(required_height, canvas_height)
            )
            
            self.scroller.canvas.configure(
                scrollregion=self.scroller.canvas.bbox("all")
            )

        # 10ms delay instead of after_idle
        self.after(10, update)
