#!/usr/bin/env python3
# multitool/tests/run_tests.py
"""
Simple test runner script.

Usage:
    python run_tests.py           # Run all tests
    python run_tests.py -v        # Run with verbose output
    python run_tests.py -k utils  # Run only tests matching 'utils'
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

if __name__ == "__main__":
    import pytest
    
    # Default arguments
    args = [
        os.path.dirname(os.path.abspath(__file__)),  # Test directory
        "-v",  # Verbose
        "--tb=short",  # Shorter tracebacks
    ]
    
    # Add any command line arguments
    args.extend(sys.argv[1:])
    
    # Run pytest
    exit_code = pytest.main(args)
    sys.exit(exit_code)
