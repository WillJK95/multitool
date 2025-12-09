#!/usr/bin/env python3
# multitool/main.py
"""
Data Investigation Multi-Tool

Main entry point for the application.

Usage:
    python -m multitool.main
    
Or if installed:
    multitool
"""

import sys
import os

# Add parent directory to path for imports when running directly
if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from multitool.app import App
from multitool.utils.helpers import log_message


def main():
    """Main entry point for the application."""
    log_message("=" * 30)
    log_message("Application starting...")
    
    try:
        app = App()
        app.mainloop()
    except Exception as e:
        log_message(f"UNHANDLED FATAL ERROR: {e}")
        raise
    finally:
        log_message("Application closed.\n")


if __name__ == "__main__":
    main()
