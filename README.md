# Data Investigation Multi-Tool - Modular Structure

## Overview

This document explains the modular restructuring of the Data Investigation Multi-Tool from a single 10,000-line file into a maintainable package structure.

## Directory Structure

```
multitool/
├── __init__.py              # Package initialization, version info
├── main.py                  # Entry point: python -m multitool.main
├── app.py                   # Main App class (window, menus, navigation)
├── constants.py             # All shared constants (API URLs, field definitions)
├── help_content.py          # Help text for all modules
│
├── api/                     # API client modules
│   ├── __init__.py
│   ├── companies_house.py   # Companies House API functions
│   ├── charity_commission.py # Charity Commission API functions
│   └── grantnav.py          # 360Giving GrantNav API functions
│
├── utils/                   # Utility functions and classes
│   ├── __init__.py
│   ├── helpers.py           # Shared helpers (clean_company_number, log_message, etc.)
│   ├── token_bucket.py      # Rate limiting implementation
│   └── enrichment.py        # Data enrichment functions
│
├── ui/                      # Reusable UI components
│   ├── __init__.py
│   ├── tooltip.py           # Tooltip widget
│   ├── scrollable_frame.py  # Scrollable container
│   ├── searchable_entry.py  # Autocomplete entry widget
│   └── help_window.py       # Help dialog window
│
└── modules/                 # Investigation modules
    ├── __init__.py
    ├── base.py              # InvestigationModuleBase class
    ├── director_search.py   # Director Search module (TODO)
    ├── ubo_tracer.py        # UBO Tracer module (TODO)
    ├── grants_search.py     # Grants Search module (TODO)
    ├── data_match.py        # Data Match module (TODO)
    ├── network_analytics.py # Network Analytics module (TODO)
    ├── enhanced_dd.py       # Enhanced Due Diligence module (TODO)
    └── unified_search.py    # Unified Search module (TODO)
```

## What's Been Completed

### Core Infrastructure (100% Complete)

1. **`constants.py`** - All constants extracted:
   - API URLs
   - Configuration paths
   - Field definitions (COMPANY_DATA_FIELDS, GRANT_DATA_FIELDS, CHARITY_DATA_FIELDS)
   - Taxonomy mappings for iXBRL parsing

2. **`help_content.py`** - All help text extracted

3. **`api/companies_house.py`** - Full API client:
   - `ch_get_data()` - Core GET function with caching and retries
   - `ch_search_officers()`, `ch_search_companies()`
   - `ch_get_company()`, `ch_get_officers()`, `ch_get_pscs()`
   - `ch_get_filing_history()`
   - `check_api_status()`

4. **`api/charity_commission.py`** - Full API client:
   - `cc_get_data()` - Core GET function
   - `cc_get_charity_details()`, `cc_get_trustees()`
   - `cc_get_financial_history()`, `cc_search_charities()`

5. **`api/grantnav.py`** - Full API client:
   - `grantnav_get_data()` - Core GET function
   - `search_grants_by_org_id()`, `search_grants_by_org_name()`
   - `get_all_grants_for_org()` - Paginated fetch

6. **`utils/helpers.py`** - Shared utilities:
   - `log_message()` - Logging
   - `clean_company_number()` - Company number formatting
   - `clean_address_string()` - Address normalization
   - `get_canonical_name_key()` - Person matching key generation
   - `format_address_label()` - Graph label formatting
   - `get_nested_value()` - Nested dict access

7. **`utils/token_bucket.py`** - Rate limiter (complete rewrite with better API)

8. **`utils/enrichment.py`** - Data enrichment functions:
   - `enrich_with_company_data()`
   - `enrich_with_charity_data()`

9. **`ui/`** - All UI components extracted:
   - `Tooltip` - Hover help
   - `ScrollableFrame` - Scrollable container
   - `SearchableEntry` - Autocomplete widget
   - `HelpWindow` - Help dialog

10. **`modules/base.py`** - `InvestigationModuleBase` class (complete)

11. **`app.py`** - Main application class (complete)

## Modules Still To Extract

The following modules need to be extracted from the original `tool55.py`. Each is a self-contained class that extends `InvestigationModuleBase`:

### 1. DirectorSearch (Lines ~2175-3168)
```python
# modules/director_search.py
from .base import InvestigationModuleBase

class DirectorSearch(InvestigationModuleBase):
    # Copy class from original file
    # Update imports to use new module paths
```

### 2. CompanyCharitySearch (Lines ~3169-3975)
```python
# modules/unified_search.py
from .base import InvestigationModuleBase

class CompanyCharitySearch(InvestigationModuleBase):
    # Copy class from original file
```

### 3. UltimateBeneficialOwnershipTracer (Lines ~3976-5015)
```python
# modules/ubo_tracer.py
from .base import InvestigationModuleBase

class UltimateBeneficialOwnershipTracer(InvestigationModuleBase):
    # Copy class from original file
```

### 4. NetworkAnalytics (Lines ~5099-6494)
```python
# modules/network_analytics.py
from .base import InvestigationModuleBase

class NetworkAnalytics(InvestigationModuleBase):
    # Copy class from original file
```

### 5. GrantsSearch (Lines ~6495-6895)
```python
# modules/grants_search.py
from .base import InvestigationModuleBase

class GrantsSearch(InvestigationModuleBase):
    # Copy class from original file
```

### 6. DataMatch (Lines ~6896-7274)
```python
# modules/data_match.py
from .base import InvestigationModuleBase

class DataMatch(InvestigationModuleBase):
    # Copy class from original file
```

### 7. iXBRLParser & FinancialAnalyzer (Lines ~7275-7735)
```python
# utils/financial_analyzer.py
class iXBRLParser:
    # Copy class from original file

class FinancialAnalyzer:
    # Copy class from original file
```

### 8. EnhancedDueDiligence (Lines ~7736-9697)
```python
# modules/enhanced_dd.py
from .base import InvestigationModuleBase

class EnhancedDueDiligence(InvestigationModuleBase):
    # Copy class from original file
```

## Migration Checklist for Each Module

When extracting each module:

1. [ ] Create new file in `modules/` directory
2. [ ] Copy class definition from original file
3. [ ] Update imports at top of file:
   ```python
   from ..api.companies_house import ch_get_data
   from ..api.charity_commission import cc_get_data
   from ..utils.helpers import clean_company_number, log_message
   from ..ui.tooltip import Tooltip
   from ..constants import COMPANY_DATA_FIELDS
   from .base import InvestigationModuleBase
   ```
4. [ ] Replace any inline utility functions with imports from `utils/`
5. [ ] Remove duplicate method definitions (use shared utilities)
6. [ ] Test the module independently

## Import Pattern

```python
# Relative imports within the package
from ..api.companies_house import ch_get_data
from ..utils.helpers import clean_company_number
from ..constants import API_BASE_URL
from .base import InvestigationModuleBase

# Standard library
import threading
import csv

# Third-party
import networkx as nx
from pyvis.network import Network
```

## Running the Application

Once all modules are extracted:

```bash
# From the parent directory of multitool/
python -m multitool.main
```

Or create a simple runner script:

```python
#!/usr/bin/env python3
from multitool.main import main
main()
```

## Benefits of This Structure

1. **Maintainability**: Each module is self-contained and can be edited independently
2. **Testability**: Modules can be unit tested in isolation
3. **Readability**: Easier to understand the codebase structure
4. **Reusability**: API clients and utilities can be imported by external tools
5. **Collaboration**: Multiple developers can work on different modules simultaneously
6. **Lazy Loading**: Modules are only imported when needed, speeding up startup

## Next Steps

1. Extract remaining 7 investigation modules
2. Add type hints throughout
3. Write unit tests for API clients and utilities
4. Add integration tests for each module
5. Create setup.py/pyproject.toml for proper packaging
