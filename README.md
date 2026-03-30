# Data Investigation Multi-Tool

A desktop application for conducting corporate due diligence investigations using UK public data sources. Built for fraud analysts, financial investigators, and compliance professionals.

## Features

### 🔍 Director Search
Search for company directors by name and explore their network of appointments. Generate interactive network graphs showing connections between individuals and companies. Includes integration with 360Giving to find associated grant funding.

### 🏢 Unified Company & Charity Search
Batch lookup companies and charities from a CSV file. Automatically enriches records with registration details, status, addresses, and financial information from Companies House and the Charity Commission.

### 👤 Ultimate Beneficial Ownership (UBO) Tracer
Trace ownership chains through corporate structures to identify ultimate beneficial owners. Handles complex multi-level ownership with configurable depth limits and generates hierarchical visualisation graphs.

### 📜 Contracts Finder
Search for contracts awarded to public bodies, enrich with Companies House data and export results for analysis.

### 🕸️ Network Analytics
Build and analyse corporate networks starting from seed companies or exported files from other modules. Combine multiple network files, identify key nodes, customise networks, and find paths between entities.

### 💰 Grants Search
Search the 360Giving database for grants awarded to organisations. Supports lookup by company number, charity number, or organisation name with full pagination support.

### 📊 Enhanced Due Diligence
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

API keys can be configured in File → Manage API Keys within the application.

The 360Giving GrantNav API does not require authentication.

> **Note:** The postcode geolocation feature (used in Network Analytics) relies on the [pgeocode](https://github.com/symerio/pgeocode) library, which downloads approximately 2 MB of postal code reference data from [GeoNames](https://www.geonames.org/) on first use and caches it locally. An internet connection is required the first time this feature is used. Subsequent runs use the cached data.

## Usage

### Quick Start

1. Launch the application
2. Configure your API keys (File → Manage API Keys)
3. Select an investigation module from the main menu
4. Load your data (CSV file or manual entry)
5. Run the investigation
6. Export results to CSV or generate network graphs

### Input File Format

Most modules accept CSV files with identifier columns. The tool automatically detects columns containing:
- Company numbers (8-digit format, with SC/NI/OC prefixes)
- Charity numbers (6-7 digit format)
- Organisation names

### Output Formats

- **CSV**: Enriched data with all retrieved fields
- **HTML**: Interactive network graphs (using vis.js)
- **Graph Data**: Edge lists for import into other network analysis tools

## Data Sources

This tool aggregates data from the following public sources:

| Source | Data Provided | License |
|--------|---------------|---------|
| [Companies House](https://www.gov.uk/government/organisations/companies-house) | Company profiles, officers, PSCs, filing history | Open Government Licence |
| [Charity Commission](https://www.gov.uk/government/organisations/charity-commission) | Charity details, trustees, financial history | Open Government Licence v3.0 |
| [360Giving](https://www.threesixtygiving.org/) | Grant funding data | CC BY 4.0 |
| [Contracts Finder](https://www.contractsfinder.service.gov.uk/Search) | Contracts data | Open Government Licence v3.0 |


## Project Structure

```
multitool/
├── main.py              # Application entry point
├── app.py               # Main window and navigation
├── constants.py         # Configuration and field definitions
├── api/                 # API client modules
│   ├── companies_house.py
│   ├── charity_commission.py
│   ├── contracts_finder.py
│   └── grantnav.py
├── modules/             # Investigation modules
│   ├── director_search.py
│   ├── unified_search.py
│   ├── ubo_tracer.py
│   ├── network_analytics.py
│   ├── contracts_finder.py
│   ├── grants_search.py
│   └── enhanced_dd.py
├── ui/                  # Reusable UI components
│   ├── help_window.py
│   ├── licenses_window.py
│   ├── scrollable_frame.py
│   ├── searchable_entry.py
│   └── tooltip.py
├── utils/		 # Shared utilities
│   ├── enrichment.py
│   ├── financial_analyzer.py 
│   ├── fuzzy_match.py
│   ├── helpers.py
│   └── token_bucket.py             
```

## Building an Executable

To create a standalone Windows executable:

```bash
pip install pyinstaller
pyinstaller --onefile --noconsole --name "DataInvestigationMultiTool" multitool/main.py
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
- [ttkbootstrap](https://github.com/israel-dryer/ttkbootstrap) - Modern UI themes
- [NetworkX](https://networkx.org/) - Network analysis
- [pyvis](https://github.com/WestHealth/pyvis) - Interactive network visualisation
- [RapidFuzz](https://github.com/rapidfuzz/RapidFuzz) - Fuzzy string matching

## Disclaimer

This tool is provided for legitimate investigative and compliance purposes. Users are responsible for ensuring their use complies with applicable data protection laws and the terms of service of the underlying APIs. The author accepts no liability for misuse of this software.
