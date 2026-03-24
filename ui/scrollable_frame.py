# multitool/ui/scrollable_frame.py
"""Scrollable frame container widget."""

import tkinter as tk
from tkinter import ttk


class ScrollableFrame(ttk.Frame):
    """
    A frame container that provides vertical scrolling.
    
    The scrollable_frame attribute is where child widgets should be placed.
    Automatically handles mousewheel scrolling on all platforms.
    
    Attributes:
        canvas: The canvas widget that provides scrolling
        scrollbar: The vertical scrollbar
        scrollable_frame: The frame where content should be placed
    """
    
    def __init__(self, container, *args, **kwargs):
        """
        Initialize the scrollable frame.
        
        Args:
            container: Parent widget
            *args: Additional positional arguments for ttk.Frame
            **kwargs: Additional keyword arguments for ttk.Frame
        """
        super().__init__(container, *args, **kwargs)
        
        # Get background color from style
        style = ttk.Style()
        background_color = style.lookup("TFrame", "background")
        
        # Create canvas
        self.canvas = tk.Canvas(
            self,
            borderwidth=0,
            highlightthickness=0,
            background=background_color
        )
        
        # Create scrollbar
        self.scrollbar = ttk.Scrollbar(
            self,
            orient="vertical",
            command=self.canvas.yview
        )
        
        # Create inner frame
        self.scrollable_frame = ttk.Frame(self.canvas)
        
        # Configure scroll region when frame size changes
        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")),
        )
        
        # Create window in canvas
        self.frame_id = self.canvas.create_window(
            (0, 0),
            window=self.scrollable_frame,
            anchor="nw"
        )
        
        # Link scrollbar to canvas
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        
        # Pack widgets
        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")
        
        # Bind events
        self.canvas.bind("<Configure>", self._on_canvas_configure)
        self.scrollable_frame.bind("<Configure>", self._on_frame_configure, add="+")
        
        # Bind mousewheel to widget tree
        self._bind_tree(self)
    
    def _bind_tree(self, widget) -> None:
        """
        Recursively bind mousewheel events to widget and all descendants.
        
        Args:
            widget: Widget to bind events to
        """
        widget.bind("<MouseWheel>", self._on_mousewheel)
        widget.bind("<Button-4>", self._on_mousewheel)  # Linux scroll up
        widget.bind("<Button-5>", self._on_mousewheel)  # Linux scroll down
        
        for child in widget.winfo_children():
            self._bind_tree(child)
    
    def _on_canvas_configure(self, event) -> None:
        """Handle canvas resize events."""
        self.canvas.itemconfig(self.frame_id, width=event.width)
        wanted = self.scrollable_frame.winfo_reqheight()
        self.canvas.itemconfig(self.frame_id, height=max(wanted, event.height))
    
    def _on_mousewheel(self, event) -> None:
        """Handle mousewheel scroll events."""
        # Allow modules to temporarily disable outer scrolling
        if getattr(self, "_disabled", False):
            return
        # Only scroll if there's actually scrollable content
        if self.canvas.yview() == (0.0, 1.0):
            return
        
        if event.num == 4:  # Linux scroll up
            self.canvas.yview_scroll(-1, "units")
        elif event.num == 5:  # Linux scroll down
            self.canvas.yview_scroll(1, "units")
        else:  # Windows/Mac
            self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
    
    def _on_frame_configure(self, event) -> None:
        """Handle inner frame resize events."""
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        required_height = self.scrollable_frame.winfo_reqheight()
        self.canvas.itemconfig(self.frame_id, height=required_height)
        
        # Rebind to catch any newly added widgets
        self.after_idle(lambda: self._bind_tree(self.scrollable_frame))
    
    def destroy(self) -> None:
        """Clean up bindings before destroying the frame."""
        super().destroy()
