# multitool/ui/help_window.py
"""Help window dialog for displaying module documentation."""

import tkinter as tk
from tkinter import ttk


class HelpWindow(tk.Toplevel):
    """
    A modal, scrollable, read-only text window for displaying help content.
    
    The window is modal (blocks interaction with parent) and includes
    a close button.
    """
    
    def __init__(self, parent: tk.Widget, title: str, content: str):
        """
        Initialize and display the help window.
        
        Args:
            parent: Parent widget
            title: Window title
            content: Help text content to display
        """
        super().__init__(parent)
        self.title(title)
        self.geometry("600x500")
        self.transient(parent)
        self.grab_set()
        
        # Main container
        main_frame = ttk.Frame(self, padding=10)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Create scrollable text area
        txt_frame = ttk.Frame(main_frame, borderwidth=1, relief="sunken")
        txt_frame.pack(fill=tk.BOTH, expand=True)
        
        text_widget = tk.Text(
            txt_frame,
            wrap="word",
            font=("Segoe UI", 10),
            borderwidth=0,
            highlightthickness=0,
        )
        scrollbar = ttk.Scrollbar(
            txt_frame,
            orient="vertical",
            command=text_widget.yview
        )
        text_widget.configure(yscrollcommand=scrollbar.set)
        
        scrollbar.pack(side="right", fill="y")
        text_widget.pack(side="left", fill="both", expand=True, padx=5, pady=5)
        
        # Insert content and make read-only
        text_widget.insert(tk.END, content)
        text_widget.config(state="disabled")
        
        # Close button
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(10, 0))
        close_button = ttk.Button(
            button_frame,
            text="Close",
            command=self.destroy
        )
        close_button.pack()
        
        # Wait for window to close before returning
        self.wait_window()
