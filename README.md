# MultiTool

A desktop application for conducting corporate due diligence investigations using UK public data sources. Built for fraud analysts, financial investigators, and compliance professionals.

## Features

### Director Search
Search for company directors by name and explore their network of appointments. Generate interactive network graphs showing connections between individuals and companies. Includes integration with 360Giving to find associated grant funding.

### Bulk Entity Search
Batch lookup companies and charities from a CSV file. Automatically enriches records with registration details, status, addresses, and financial information from Companies House and the Charity Commission. Supports exact and fuzzy matching, and exports graph data for use in Network Analytics.

### Ultimate Beneficial Ownership (UBO) Tracer
Trace ownership chains through corporate structures to identify ultimate beneficial owners. Handles complex multi-level ownership with configurable depth limits and generates hierarchical visualisation graphs.

### Network Analytics Workbench
Build and analyse corporate networks by combining graph exports from other modules. Identify key nodes, find paths between entities, discover hidden links via address proximity and surname matching, and generate interactive visualisations.

### Grants Search
Search the 360Giving GrantNav database for grants awarded to organisations. Supports lookup by company number or charity number, with fallback between sources.

### Enhanced Due Diligence
Comprehensive due diligence reports combining data from all sources. Automated risk detection including:
- Insolvency indicators
- Phoenix company patterns
- Director churn analysis
- Offshore PSC detection
- Filing compliance issues
- Financial health metrics

## Installation

### Prerequisites
- Python 3.9 or higher
- Windows 10/11 (primary platform)

### Setup

1. Clone the repository:
```bash
git clone https://github.com/WillJK95/multitool.git
cd multitool
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the application:
```bash
python -m multitool.main
```

### API Keys

The tool requires API keys for full functionality:

- **Companies House API**: Free registration at https://developer.company-information.service.gov.uk/
- **Charity Commission API**: Free registration at https://api-portal.charitycommission.gov.uk/

API keys can be configured in File → Manage API Keys within the application. Keys are stored securely using the system keychain (Windows Credential Manager / macOS Keychain / Linux Secret Service) and are never stored in plain text.

The 360Giving GrantNav API does not require authentication.

> **Note:** The postcode geolocation feature (used in Network Analytics) relies on the [pgeocode](https://github.com/symerio/pgeocode) library, which downloads approximately 2 MB of postal code reference data from [GeoNames](https://www.geonames.org/) on first use and caches it locally. An internet connection is required the first time this feature is used. Subsequent runs use the cached data.

## Usage

### Quick Start

1. Launch the application
2. Configure your API keys (File → Manage API Keys)
3. Select an investigation module from the sidebar
4. Load your data (CSV file or direct search entry, depending on the module)
5. Run the investigation
6. Export results as CSV, HTML report, or graph edge list

Each module has a Help button in its header with workflow guidance.

### Input File Format

Most modules accept CSV files with identifier columns. The tool automatically detects columns containing:
- Company numbers (8-digit format, with SC/NI/OC prefixes)
- Charity numbers (6-7 digit format)
- Organisation names

### Output Formats

- **CSV**: Enriched data with all retrieved fields
- **HTML**: Interactive network graphs and due diligence reports
- **Graph edge list**: CSV format for import into Network Analytics or other tools

## Data Sources

This tool aggregates data from the following public sources:

| Source | Data Provided | License |
|--------|---------------|---------|
| [Companies House](https://www.gov.uk/government/organisations/companies-house) | Company profiles, officers, PSCs, filing history | Open Government Licence |
| [Charity Commission](https://www.gov.uk/government/organisations/charity-commission) | Charity details, trustees, financial history | Open Government Licence v3.0 |
| [360Giving](https://www.threesixtygiving.org/) | Grant funding data | CC BY 4.0 |


## Project Structure

```
multitool/
├── main.py                        # Application entry point
├── app.py                         # Main window and navigation
├── constants.py                   # Configuration and field definitions
├── help_content.py                # In-app help text
├── api/                           # API client modules
│   ├── companies_house.py
│   ├── charity_commission.py
│   └── grantnav.py
├── modules/                       # Investigation modules
│   ├── base.py                    # Base class for all modules
│   ├── director_search.py
│   ├── unified_search.py          # Bulk Entity Search
│   ├── ubo_tracer.py
│   ├── network_analytics.py       # Network Analytics Workbench
│   ├── grants_search.py
│   └── enhanced_dd.py
├── ui/                            # Reusable UI components
│   ├── connection_matrix.py
│   ├── help_window.py
│   ├── licenses_window.py
│   ├── scrollable_frame.py
│   ├── searchable_entry.py
│   └── tooltip.py
├── utils/                         # Shared utilities
│   ├── app_state.py               # Cross-module working set state
│   ├── charity_financial_data.py
│   ├── edd_charity_checks.py
│   ├── edd_charity_visualizations.py
│   ├── edd_cross_analysis.py
│   ├── edd_visualizations.py
│   ├── enrichment.py
│   ├── financial_analyzer.py
│   ├── fuzzy_match.py
│   ├── helpers.py
│   ├── settings.py
│   └── token_bucket.py
└── lib/                           # Bundled front-end assets
    ├── bindings/
    └── tom-select/
```

## Building an Executable

To create a standalone Windows executable:

```bash
pip install pyinstaller
pyinstaller --onefile --noconsole --name "MultiTool" multitool/main.py
```

The executable will be created in the `dist/` folder.

## Contributing

Contributions are welcome. Please open an issue to discuss proposed changes before submitting a pull request.

## License

Copyright (c) 2025 Crown Copyright
Created by William Kenny

MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

## Acknowledgements

Created by William Kenny (2025).

Built with:
- [ttkbootstrap](https://github.com/israel-dryer/ttkbootstrap) - Modern UI themes for tkinter
- [NetworkX](https://networkx.org/) - Network analysis
- [gravis](https://github.com/robert-haas/gravis) - Interactive network visualisation
- [RapidFuzz](https://github.com/rapidfuzz/RapidFuzz) - Fuzzy string matching
- [pandas](https://pandas.pydata.org/) - Data manipulation
- [matplotlib](https://matplotlib.org/) - Data visualisation
- [lxml](https://lxml.de/) - XML/HTML parsing (iXBRL financial accounts)
- [keyring](https://github.com/jaraco/keyring) - Secure credential storage
- [pgeocode](https://github.com/symerio/pgeocode) - Postcode geolocation
- [Pillow](https://python-pillow.org/) - Image handling

## Disclaimer

This tool is provided for legitimate investigative and compliance purposes. Users are responsible for ensuring their use complies with applicable data protection laws and the terms of service of the underlying APIs. The author accepts no liability for misuse of this software.
