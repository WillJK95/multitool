# Data Investigation Multi-Tool

A desktop application for conducting corporate due diligence investigations using UK public data sources. Built for fraud analysts, financial investigators, and compliance professionals.

## Features

### 🔍 Director Search
Search for company directors by name and explore their network of appointments. Generate interactive network graphs showing connections between individuals and companies. Includes integration with 360Giving to find associated grant funding.

### 🏢 Unified Company & Charity Search
Batch lookup companies and charities from a CSV file. Automatically enriches records with registration details, status, addresses, and financial information from Companies House and the Charity Commission.

### 👤 Ultimate Beneficial Ownership (UBO) Tracer
Trace ownership chains through corporate structures to identify ultimate beneficial owners. Handles complex multi-level ownership with configurable depth limits and generates hierarchical visualisation graphs.

### 🕸️ Network Analytics
Build and analyse corporate networks starting from seed companies. Combine multiple network files, identify key nodes, remove supernodes (e.g., formation agents), and find paths between entities.

### 💰 Grants Search
Search the 360Giving database for grants awarded to organisations. Supports lookup by company number, charity number, or organisation name with full pagination support.

### 🔗 Data Match
Match records between two datasets using exact or fuzzy matching. Configurable matching thresholds and multiple matching strategies for deduplication and record linkage.

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

## Project Structure

```
multitool/
├── main.py              # Application entry point
├── app.py               # Main window and navigation
├── constants.py         # Configuration and field definitions
├── api/                 # API client modules
│   ├── companies_house.py
│   ├── charity_commission.py
│   └── grantnav.py
├── modules/             # Investigation modules
│   ├── director_search.py
│   ├── unified_search.py
│   ├── ubo_tracer.py
│   ├── network_analytics.py
│   ├── grants_search.py
│   ├── data_match.py
│   └── enhanced_dd.py
├── ui/                  # Reusable UI components
└── utils/               # Shared utilities
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

Copyright 2015 William Kenny

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Acknowledgements

Created by William Kenny (2025).

Built with:
- [ttkbootstrap](https://github.com/israel-dryer/ttkbootstrap) - Modern UI themes
- [NetworkX](https://networkx.org/) - Network analysis
- [pyvis](https://github.com/WestHealth/pyvis) - Interactive network visualisation
- [RapidFuzz](https://github.com/rapidfuzz/RapidFuzz) - Fuzzy string matching

## Disclaimer

This tool is provided for legitimate investigative and compliance purposes. Users are responsible for ensuring their use complies with applicable data protection laws and the terms of service of the underlying APIs. The author accepts no liability for misuse of this software.
