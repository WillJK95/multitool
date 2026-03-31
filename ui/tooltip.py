# multitool/ui/tooltip.py
"""Tooltip widget for displaying hover help text."""

import tkinter as tk
from typing import Optional


class Tooltip:
    """
    Creates a Tooltip (pop-up) widget for a given Tkinter widget.
    
    Handles delays and prevents flickering when mouse moves between
    the widget and the tooltip itself.
    
    Attributes:
        widget: The widget to attach the tooltip to
        text: The tooltip text to display
        delay: Milliseconds to wait before showing tooltip
    """
    
    def __init__(self, widget: tk.Widget, text: str, delay: int = 500):
        """
        Initialize the tooltip.
        
        Args:
            widget: Tkinter widget to attach tooltip to
            text: Text to display in the tooltip
            delay: Delay in milliseconds before showing tooltip
        """
        self.widget = widget
        self.text = text
        self.delay = delay
        self.tooltip_window: Optional[tk.Toplevel] = None
        self.show_id: Optional[str] = None
        self.hide_id: Optional[str] = None
        
        self.widget.bind("<Enter>", self.enter)
        self.widget.bind("<Leave>", self.leave)
    
    def enter(self, event=None) -> None:
        """Handle mouse entering the widget."""
        self._cancel_hide()
        self.show_id = self.widget.after(self.delay, self.show_tooltip)
    
    def leave(self, event=None) -> None:
        """Handle mouse leaving the widget."""
        self._cancel_show()
        self.hide_id = self.widget.after(100, self.hide_tooltip)
    
    def show_tooltip(self) -> None:
        """Display the tooltip window."""
        if self.tooltip_window:
            return
        
        # Calculate position — bbox("insert") only works on Text widgets;
        # fall back to cursor position for Treeview and other widget types.
        try:
            x, y, _, _ = self.widget.bbox("insert")
            x += self.widget.winfo_rootx() + 25
            y += self.widget.winfo_rooty() + 25
        except Exception:
            x = self.widget.winfo_pointerx() + 10
            y = self.widget.winfo_pointery() + 10
        
        # Create tooltip window
        self.tooltip_window = tk.Toplevel(self.widget)
        self.tooltip_window.wm_overrideredirect(True)
        self.tooltip_window.wm_geometry(f"+{x}+{y}")
        
        label = tk.Label(
            self.tooltip_window,
            text=self.text,
            justify="left",
            background="#ffffe0",
            relief="solid",
            borderwidth=1,
            font=("tahoma", "8", "normal"),
        )
        label.pack(ipadx=1)
        
        # Bind events to the tooltip to prevent hiding when mouse moves over it
        label.bind("<Enter>", self.enter_tooltip)
        label.bind("<Leave>", self.leave_tooltip)
    
    def hide_tooltip(self) -> None:
        """Hide and destroy the tooltip window."""
        if self.tooltip_window:
            self.tooltip_window.destroy()
        self.tooltip_window = None
    
    def enter_tooltip(self, event=None) -> None:
        """Handle mouse entering the tooltip itself."""
        self._cancel_hide()
    
    def leave_tooltip(self, event=None) -> None:
        """Handle mouse leaving the tooltip."""
        self.hide_id = self.widget.after(100, self.hide_tooltip)
    
    def _cancel_show(self) -> None:
        """Cancel pending show operation."""
        if self.show_id:
            self.widget.after_cancel(self.show_id)
            self.show_id = None
    
    def _cancel_hide(self) -> None:
        """Cancel pending hide operation."""
        if self.hide_id:
            self.widget.after_cancel(self.hide_id)
            self.hide_id = None
