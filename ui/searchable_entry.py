# multitool/ui/searchable_entry.py
"""Searchable entry widget with autocomplete dropdown."""

import tkinter as tk
from tkinter import ttk
from typing import List, Optional


class SearchableEntry(ttk.Frame):
    """
    An entry widget with an autocomplete dropdown list.
    
    As the user types, a dropdown list appears showing matching values
    from a predefined list. Selecting an item populates the entry.
    
    Attributes:
        var: StringVar containing the current entry value
        all_values: List of values to search through
    """
    
    def __init__(self, parent, *args, **kwargs):
        """
        Initialize the searchable entry.
        
        Args:
            parent: Parent widget
            *args: Additional positional arguments for ttk.Frame
            **kwargs: Additional keyword arguments for ttk.Frame
        """
        super().__init__(parent, *args, **kwargs)
        
        self.parent = parent
        self.all_values: List[str] = []
        self.var = tk.StringVar()
        
        # Create entry widget
        self.entry = ttk.Entry(self, textvariable=self.var)
        self.entry.pack(fill=tk.X, expand=True)
        
        self.listbox_popup: Optional[tk.Toplevel] = None
        self.listbox: Optional[tk.Listbox] = None
        
        # Bind events
        self.entry.bind("<KeyRelease>", self._on_keyrelease)
        self.entry.bind("<FocusOut>", self._on_focus_out)
    
    def set_values(self, values: List[str]) -> None:
        """
        Set the list of values to search through.
        
        Args:
            values: List of strings to use for autocomplete
        """
        self.all_values = values
    
    def get(self) -> str:
        """Get the current entry value."""
        return self.var.get()
    
    def set(self, value: str) -> None:
        """Set the entry value."""
        self.var.set(value)
    
    def _on_keyrelease(self, event) -> None:
        """Handle key release events to update autocomplete list."""
        typed_text = self.var.get().lower()
        
        if not typed_text:
            self._hide_listbox()
            return
        
        # Filter values that contain the typed text
        filtered_list = [v for v in self.all_values if typed_text in v.lower()]
        
        if filtered_list:
            self._show_listbox(filtered_list)
        else:
            self._hide_listbox()
    
    def _show_listbox(self, values: List[str]) -> None:
        """
        Show the autocomplete dropdown with filtered values.
        
        Args:
            values: Filtered list of values to display
        """
        if not self.listbox_popup:
            # Create the popup window
            self.listbox_popup = tk.Toplevel(self.parent)
            self.listbox_popup.wm_overrideredirect(True)  # No title bar
            
            self.listbox = tk.Listbox(self.listbox_popup)
            self.listbox.pack(fill=tk.BOTH, expand=True)
            self.listbox.bind("<<ListboxSelect>>", self._on_listbox_select)
        
        # Position and size the popup
        x = self.entry.winfo_rootx()
        y = self.entry.winfo_rooty() + self.entry.winfo_height()
        width = self.entry.winfo_width()
        
        self.listbox_popup.wm_geometry(f"{width}x150+{x}+{y}")
        
        # Update listbox contents
        self.listbox.delete(0, tk.END)
        for v in values:
            self.listbox.insert(tk.END, v)
        
        self.listbox_popup.deiconify()
    
    def _hide_listbox(self, event=None) -> None:
        """Hide the autocomplete dropdown."""
        if self.listbox_popup:
            self.listbox_popup.withdraw()
    
    def _on_listbox_select(self, event) -> None:
        """Handle selection from the autocomplete list."""
        if not self.listbox:
            return
        
        current_selection_indices = self.listbox.curselection()
        if not current_selection_indices:
            return
        
        selection = self.listbox.get(current_selection_indices[0])
        self.var.set(selection)
        self._hide_listbox()
        self.entry.focus_set()
    
    def _on_focus_out(self, event) -> None:
        """Handle focus leaving the entry."""
        # Delay hiding to allow click on listbox
        self.after(200, self._hide_listbox)
    
    def config(self, *args, **kwargs) -> None:
        """Configure the internal entry widget."""
        if hasattr(self, 'entry'):
            self.entry.config(*args, **kwargs)
        else:
            super().config(*args, **kwargs)

    def configure(self, *args, **kwargs) -> None:
        """Configure the internal entry widget (alias for config)."""
        if hasattr(self, 'entry'):
            self.entry.configure(*args, **kwargs)
        else:
            super().configure(*args, **kwargs)
