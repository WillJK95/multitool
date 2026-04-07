# multitool/help_content.py
"""Help content for all modules.

These strings are displayed in the in-app Help panel. Keep them user-facing, task-focused,
and consistent with the current UI labels.
"""

HELP_CONTENT = {
    "main": """
--- Data Investigation Multi-Tool ---

This tool supports due diligence, counter-fraud, and conflict-of-interest investigations
by pulling data from multiple UK public data sources:

    - UK Companies House (company profiles, officers, PSCs, filings)
    - Charity Commission for England & Wales (charity details, trustees, finances)
    - 360Giving / GrantNav (grant funding records)

--- HOW TO USE ---

1. Select a module from the main menu
2. Follow the numbered steps inside that module (typically: upload CSV, select columns, run, export)
3. Use Export buttons to save outputs for casework, audit trails, or further analysis

--- APPLICATION SETTINGS ---

Theme Toggle:
    Switch between Dark Mode (Superhero theme) and Light Mode (Litera theme)
    using the toggle in the main menu.

Font Size:
    Adjust the application font size (8-16pt) using the slider in the main menu.
    Changes apply immediately across all modules.

API Status Monitor:
    The main menu displays real-time status indicators for all four data sources.
    - Green: API is responding normally
    - Red: API is unavailable or returning errors
    - Orange: API is responding with warnings
    - Grey: Status not yet checked
    Click "Refresh" to manually update the status of all APIs.

--- COMMON TIPS ---

API Keys:
    Companies House and Charity Commission modules require free API keys.
    Use "Manage API Keys" from the main menu to add, update, or delete stored keys.
    Keys are stored securely in your system's credential manager (keyring).

Cache & Logs:
    The tool saves temporary files (logs, graphs, reports) in a hidden folder
    in your user directory (~/.DataInvestigatorTool/).
    Use "Clear Cache & Logs" to remove these files.

CSV Encoding:
    The tool attempts to read CSVs as UTF-8 first, then falls back to CP1252.

Graph Workflow:
    Several modules can export graph data (edge lists). These can be combined in
    Network Analytics to build a larger graph and search for connections between entities.

Cancellation:
    Long-running operations can be cancelled using the Cancel button.
    The tool will stop at the next safe point and preserve any data already processed.
""",

    "api_keys": """
--- About API Keys ---

An API (Application Programming Interface) is a way for computer programs to talk to
each other. To use this tool, you need free API keys from Companies House and/or the
Charity Commission.

--- Security & Storage ---

Your API keys are stored securely using your operating system's credential manager:
    - Windows: Windows Credential Manager
    - macOS: Keychain
    - Linux: Secret Service (GNOME Keyring / KWallet)

Keys are never stored in plain text and are not shared with anyone.

--- How to Get Your Keys ---

1) Companies House

   - Register at: https://developer.company-information.service.gov.uk/
   - Go to "Your Applications" and create a new application
   - Choose "REST" as the API type
   - Copy the generated key into the tool

2) Charity Commission

   - Register at: https://api-portal.charitycommission.gov.uk/
   - In your "Profile" -> "Subscriptions", subscribe to the Charity Commission API
   - Copy your primary or secondary subscription key into the tool

--- Managing Keys ---

From the main menu:
    - "Manage API Keys" opens a dialog to view, update, or delete stored keys
    - "Reset API Keys" removes all stored keys (you will be prompted to re-enter them)

--- API Rate Limits ---

Companies House: 600 requests per 5 minutes (the tool manages this automatically)
Charity Commission: Approximately 2 requests per second
GrantNav: 2 requests per second (no key required)
""",

    "director": """
--- Director Investigation ---

This module finds company appointments for a given director or officer name using
Companies House data.

--- WORKFLOW ---

1) Enter Director Details
   - Full Name (required): Enter the director's full name
   - Year of Birth (optional but recommended): Helps narrow down results
   - Month of Birth (optional): Further refines the search

2) Search
   Click "Search" to query Companies House for matching officers.

3) View Results
   The results table shows all appointments found, including:
   - Company name and number
   - Appointment type (director, secretary, etc.)
   - Appointment dates
   - Resignation date (if applicable)
   - Occupation and nationality

--- EXPORT OPTIONS ---

Export Directorships:
    Saves the appointment list as a CSV file.

Obtain Grants Data & Export:
    For the companies found, fetches associated 360Giving grant records.
    Useful for spotting repeat funders, patterns, and related entities.

Generate Visual Graph:
    Opens an interactive network graph showing:
    - The searched director (central node)
    - All associated companies
    - Other officers and PSCs at those companies
    - Registered addresses
    Nodes are colour-coded by type and can be clicked for details.

Export Graph Data (CSV):
    Exports the graph as an edge list for use in Network Analytics.
    Allows combining with other modules' exports for broader analysis.

--- TIPS & LIMITATIONS ---

1000 Result Limit:
    Companies House officer search returns a maximum of 1000 results.
    For very common names, the correct person may not appear in the first 1000.
    Date-of-birth filtering happens after the initial search, so use extra
    caution with generic names like "John Smith".

Name Variations:
    Try different name formats if initial results are poor:
    - With/without middle names
    - Full middle name vs initials
    - Maiden names vs married names
""",

    "ubo": """
--- Ultimate Beneficial Ownership (UBO) Tracer ---

This module traces ownership chains by finding Persons with Significant Control (PSCs)
and recursively following corporate ownership structures.

--- WORKFLOW ---

1) Upload Input File
   Upload a CSV containing a column of company registration numbers.

2) Select Column
   Choose the column containing company numbers from the dropdown.

3) Configure Options

   Snapshot Date (optional):
       Approximate the ownership structure as it existed on a specific date.
       If left blank, the tool includes current and historical PSC data.

   Include Officers:
       Tick to also fetch director and secretary information alongside PSCs.

4) Run Investigation
   The tool recursively follows PSCs:
   - If a PSC is an individual, they are recorded as an ultimate beneficial owner
   - If a PSC is another company, the tool fetches that company's PSCs
   - This continues up to 20 levels deep to handle complex ownership structures

--- EXPORT OPTIONS ---

Export PSC List:
    Exports a flat CSV of all discovered PSCs with:
    - PSC name and type (individual or corporate)
    - Nature of control (shares, voting rights, etc.)
    - Notified date and ceased date
    - The chain of companies leading to each PSC

Visual Ownership Graph:
    Generates a hierarchical tree visualisation showing:
    - Root companies at the top
    - Ownership chains flowing downward
    - Ultimate beneficial owners at the bottom
    Shared PSCs (entities appearing in multiple chains) are highlighted.

Export Graph Data (CSV):
    Exports an edge list for use in Network Analytics.
    Includes all company-PSC relationships discovered.

--- TIPS ---

Shared PSCs:
    Entities appearing as PSCs for multiple companies are highlighted in the
    visual graph - these may indicate connected ownership structures.

Complex Structures:
    For very complex ownership (e.g., offshore holding companies), some chains
    may terminate at foreign entities not registered with Companies House.

Historical Data:
    Use the Snapshot Date to investigate ownership at a specific point in time,
    such as when a grant was awarded or a contract was signed.
""",

    "unified_search": """
--- Unified Bulk Search ---

This module checks a list of identifiers against multiple databases in one run,
enriching your data with company and charity information.

--- SETUP ---

1) Select Databases
   Tick which sources you want to query:
   - Companies House (UK company register)
   - Charity Commission (England & Wales charities)

2) Set Search Priority
   If your file is mostly companies or mostly charities, set the priority accordingly.
   Both sources can still be queried, but the priority setting speeds up matching.

3) Upload Your File
   Upload a CSV containing organisation identifiers (numbers or names).

4) Select Columns
   Map your columns to the appropriate identifier types:

   Distinct Columns:
       - Map company numbers to "Company Number"
       - Map charity numbers to "Charity Number"

   Single Mixed Column:
       - Map the same column to both dropdowns
       - The tool will try Companies House first, then Charity Commission if no match

5) Configure Matching

   Exact Matching:
       Used for registration numbers. Fast and precise.

   Fuzzy Matching (optional):
       Enable for name-based matching when you don't have registration numbers.
       - Set accuracy threshold (85-100%): higher = stricter matching
       - Outputs include match_score and matched_name for auditability

6) Select Enrichment Fields
   Choose which data fields to include in the output:

   Companies House fields:
       Company number, status, type, incorporation date, registered address,
       SIC codes, officers, PSCs, accounts info, jurisdiction

   Charity Commission fields:
       Charity number, registration date, trustees, financial history,
       income/expenditure, assets/liabilities, area of operation

--- OUTPUT ---

The enriched file includes:
    - match_status: "Match Found" or "No Match Found"
    - match_source: Which database matched (and whether exact or fuzzy)
    - All selected enrichment fields (blank for non-matched sources)
    - match_score and matched_name (when fuzzy matching was used)

--- GRAPH EXPORT ---

Export Graph Data (CSV):
    Build a combined relationship graph from your matched results, including:
    - Companies and their officers/PSCs/addresses
    - Charities and their trustees
    This edge list can be analysed in Network Analytics.

--- TIPS ---

Mixed Identifiers:
    If your data has a mix of company and charity numbers in one column,
    the tool will automatically determine which database to query based
    on the identifier format.

Fuzzy Matching Caution:
    Fuzzy matches are leads, not conclusions. Always verify high-importance
    matches manually, especially those with scores below 95%.
""",

    "grants_search": """
--- Grants Search (360Giving / GrantNav) ---

This module finds grant funding records linked to companies and charities using
the 360Giving GrantNav database.

--- WORKFLOW ---

1) Upload Input File
   Upload a CSV containing organisation identifiers.

2) Select Identifier Columns
   Choose one or both:
   - Company Number column (Companies House registration numbers)
   - Charity Number column (Charity Commission registration numbers)

3) Select Grant Fields
   Choose which grant data fields you want in the output:
   - Grant title and description
   - Award amount and currency
   - Award date
   - Funder name and identifier
   - Grant programme
   - Planned start/end dates and duration
   - Beneficiary location

4) Run Investigation
   For each row, the tool:
   - Tries Company Number first (converted to 360Giving organisation ID format)
   - Falls back to Charity Number if no grants found via company number
   - Records all matching grants

--- OUTPUT ---

Grants Found:
    One row per grant, with your input row data duplicated to preserve context.
    All selected grant fields are populated.

No Grants Found:
    You still get a row back with grant_search_status explaining what was attempted
    (e.g., "No grants found for company GB-COH-12345678").

--- TIPS ---

Company Number Format:
    Company numbers are normalised to 8 digits and converted to 360Giving
    organisation ID format (e.g., "123456" becomes "GB-COH-00123456").

Rate Limiting:
    GrantNav API is rate-limited to 2 requests per second.
    Large files will take time to process.

Grant Coverage:
    360Giving contains grants from participating funders only.
    Not all UK grant-makers publish to 360Giving, so absence of records
    does not necessarily mean no grants were received.
""",

    "data_match": """
--- Data Match ---

This module joins (matches) two CSV files together using either exact or fuzzy matching.

--- WORKFLOW ---

1) Upload Files

   Primary File (Left):
       Your main dataset - all rows will be preserved in the output.

   Matching File (Right):
       The lookup dataset - matching columns will be appended to your primary file.

2) Choose Matching Logic

   Exact Match on Identifier:
       Best for: company numbers, payroll IDs, invoice references, postcodes
       - Fast and precise
       - Optional: Pad numeric identifiers to 8 digits (useful for UK company numbers)

   Fuzzy Match on Text:
       Best for: organisation names, person names where spelling varies
       - Uses advanced text matching algorithms
       - Configurable accuracy threshold

3) Configure Fuzzy Matching (if selected)

   Accuracy Slider (0-100%):
       - Higher values = stricter matching (fewer false positives)
       - Lower values = looser matching (may catch more variations)
       - Recommended: Start at 85% and adjust based on results

   Algorithm Selection:
       - Weighted Ratio: Smart combination of methods (recommended default)
       - Token Sort: Good for names in different orders ("Smith John" vs "John Smith")
       - Token Set: Good for partial matches and extra words
       - Partial Ratio: Good for substring matching

4) Select Columns

   Join Column (Primary):
       The column in your primary file to match on.

   Join Column (Matching):
       The column in your matching file to match against.

   Columns to Append:
       Select which columns from the matching file to add to your output.

5) Run & Export
   The output contains:
   - All rows from your primary file
   - Appended columns from matching file (where matches found)
   - Match metadata: match_score, match_type, matched_value (for fuzzy matches)

--- TIPS ---

Prefer Numbers Over Names:
    If you have both an identifier and a name, prefer exact matching on the
    identifier first. Use fuzzy name matching as a fallback for unmatched records.

False Positives:
    Fuzzy matching can create false positives. Treat outputs as leads requiring
    verification, not definitive matches. Sort by match_score to review
    lower-confidence matches first.

Data Preparation:
    Clean your data before matching:
    - Remove extra whitespace
    - Standardise case (the tool handles this, but consistency helps)
    - Remove common prefixes/suffixes that might interfere with matching
""",

    "enhanced_dd": """
--- Enhanced Due Diligence (EDD) ---

This module generates comprehensive due diligence reports for a single company,
combining Companies House data with optional financial analysis from iXBRL accounts.

--- WORKFLOW ---

1) Enter Company Number
   Enter the 8-character Companies House registration number.
   Click "Fetch Company Data" to retrieve:
   - Company profile (status, type, incorporation date, address)
   - Officers (directors, secretaries)
   - Persons with Significant Control (PSCs)
   - Filing history (recent submissions)

2) Upload Accounts (Optional)
   Upload one or more iXBRL accounts files (.xhtml or .html format).
   These are the machine-readable accounts filed with Companies House.
   If accounts are loaded, additional financial analysis becomes available.

   Note: The tool validates that uploaded accounts match the company number.
   You will be warned if there appears to be a mismatch.

3) Select Checks
   Choose which due diligence checks to run. Checks are organised in three tiers:

   CORE CHECKS (fast, always available):
       - Company status warnings (dissolved, liquidation, etc.)
       - Filing compliance (overdue accounts/confirmation statements)
       - Late filing history analysis
       - Solvency position (if accounts loaded)
       - Liquidity ratios (if accounts loaded)

   ENHANCED CHECKS (moderate speed):
       - Director/PSC turnover analysis (unusual changes)
       - Revenue and profitability trends (if accounts loaded)
       - Predictive financial outlook
       - Companies House default address detection
       - Accounting reference date changes
       - Filing category changes (audit exemptions, etc.)
       - Offshore PSC analysis (non-UK controlling entities)

   DEEP INVESTIGATION (slower, more thorough):
       - Director insolvency history search
       - Phoenix company detection (similar names after dissolution)
       - Duplicate registered address analysis (shared addresses)

4) Generate Report
   Click "Generate Report" to produce an HTML report containing:
   - Executive summary with key findings
   - Company profile overview
   - Risk indicators grouped by severity (High/Medium/Low)
   - Officer and PSC listings
   - Financial analysis and charts (if accounts were uploaded)
   - Limitations and disclaimers section

   The report opens automatically in your default web browser.

--- FINANCIAL ANALYSIS ---

When iXBRL accounts are uploaded, the tool extracts and analyses:

Financial Position:
    - Total assets and liabilities
    - Net assets / shareholders' funds
    - Cash and cash equivalents
    - Trade debtors and creditors

Financial Ratios:
    - Current ratio (current assets / current liabilities)
    - Quick ratio (liquid assets / current liabilities)
    - Debt-to-equity ratio
    - Gross and net profit margins (if P&L data available)

Trend Analysis:
    - Year-on-year changes in key metrics
    - Revenue growth trends
    - Profitability trajectory
    - Employee count changes

Charts:
    - Asset/liability composition
    - Revenue and profit trends over time
    - Key ratio comparisons

--- TIPS ---

Multiple Years of Accounts:
    Upload accounts from multiple years to enable trend analysis.
    The tool will compare figures across periods.

Report Limitations:
    The report is point-in-time and relies on filed public data.
    It should not be the sole basis for decisions. Always consider
    information from other sources and professional judgement.

iXBRL Availability:
    iXBRL accounts are available from Companies House for most companies
    filing online since 2011. Download from the company's filing history
    on the Companies House website.
""",

    "network_analytics": """
--- Network Analytics ---

This module has been renamed to Network Analytics.
Please see the Network Analytics help section for the current workflow.
""",

    "network_creator": """
--- Network Analytics ---

This module lets you build, analyse, and visualise corporate networks by combining
exported graph data from multiple sources.

The module has two tabs:
    - Network Analytics: Build, refine, analyse, and visualise networks
    - Data Converter: Transform standard CSVs into edge list format

=== TAB 1: NETWORK ANALYTICS ===

--- DATA SOURCES ---

Add Graph Files:
    Import one or more CSV edge list files exported from other modules:
    - Director Investigation graph exports
    - UBO Tracer graph exports
    - Unified Search graph exports
    - Data Converter outputs

    Each file adds its relationships to the combined network.

Seed from Company (optional):
    Start the network from a specific company by entering its registration number.
    Options:
    - Expand via PSCs: Include all persons with significant control
    - Include Officers: Include all directors and secretaries
    - Fetch Associated Companies: Include companies linked to the same people

--- BUILD & REFINE ---

Build Graph:
    Click "Build Graph" to combine all loaded data into a single network.
    This must be done before analysis. The status bar shows:
    - Total nodes (entities)
    - Total edges (relationships)
    - Number of connected components (separate sub-networks)

Remove Entities:
    Temporarily remove specific entities to reduce noise:
    - Formation agents (e.g., "COMPANIES HOUSE DEFAULT ADDRESS")
    - Mailbox addresses used by many companies
    - Other "supernodes" that create false connections

    Removal is non-destructive - rebuild the graph to restore removed entities.

Graph Statistics:
    View metrics about your network:
    - Node count by type (companies, people, addresses)
    - Edge count by relationship type
    - Most connected nodes (potential hubs)
    - Isolated nodes (no connections)

--- ANALYSE ---

Shortest Path:
    Find the shortest connection between two specific entities.
    Enter entity names or IDs and click "Find Path".
    Results show each step in the connection chain.

List Connections:
    Find connections between groups of entities:

    List A vs List B:
        Upload two CSV files with entity identifiers.
        The tool finds all paths connecting entities in List A to entities in List B.

    Within One List:
        Upload a single CSV file.
        The tool finds all paths connecting any entity in the list to any other.

    Max Hops:
        Limit the maximum path length (default: 3).
        Higher values find more connections but take longer on dense graphs.

Connection Results:
    Results show:
    - Entity pairs with connections found
    - Path length (number of hops)
    - Full path details (each entity and relationship in the chain)
    - Common intermediaries (entities appearing in multiple paths)

--- HIDDEN LINKS DISCOVERY ---

Scan for Inferred Relationships:
    Discover potential connections not explicitly in your source data.
    These are investigative leads, not evidence.

    Address Proximity:
        Find companies with neighbouring registered addresses:
        - Same postcode
        - Adjacent building numbers
        - Configurable proximity radius

    Surname Matching:
        Find people with the same surname:
        - At the same registered address
        - At the same postcode
        - Within a configurable geographic radius

    Results are flagged as "inferred" relationships for careful review.

--- VISUALISE ---

Generate Interactive Graph:
    Create an HTML visualisation of your network with:

    Display Options:
        - Colour by source file (see which data came from where)
        - Colour by entity type (companies, people, addresses)
        - Hide isolated nodes (show only connected entities)
        - Highlight cohort (emphasise specific entities from a list)

    Interaction:
        - Pan and zoom to navigate
        - Click nodes to see full details
        - Drag nodes to rearrange layout
        - Physics-based layout (force-directed)

Export Options:
    - Save graph as HTML file for sharing
    - Export current edge list as CSV
    - Export node list with attributes

=== TAB 2: DATA CONVERTER ===

Convert standard CSV files into edge list format for import into Network Analytics.

--- WORKFLOW ---

1) Upload CSV
   Upload any CSV file you want to convert.

2) Select Conversion Type
   Choose the type of relationships to create:

   Entity to Identifier:
       Create links between names and their identifiers.
       e.g., "Supplier Name" -> "Company Number"

   Entity to Entity:
       Create links between two name columns.
       e.g., "Buyer" -> "Supplier"

   Entity to Attribute:
       Create links between entities and their properties.
       e.g., "Company" -> "Postcode"

3) Map Columns
   Select which columns contain your source and target entities.

4) Set Relationship Label
   Optionally specify a label for the relationship type
   (e.g., "SUPPLIES_TO", "LOCATED_AT", "EMPLOYED_BY").

5) Convert & Export
   The output is a standard edge list CSV with columns:
   - source: The source entity
   - target: The target entity
   - relationship: The relationship type
   - source_type: Entity type of source
   - target_type: Entity type of target

This output can be directly imported into Network Analytics.

--- TIPS ---

Graph Size:
    Very large graphs (>10,000 nodes) may be slow to visualise.
    Use the Remove Entities feature to focus on relevant portions.

Supernode Removal:
    Common addresses or formation agents can create misleading connections.
    Review the "most connected nodes" statistics and consider removing
    entities that are connected to many unrelated companies.

Combining Sources:
    The power of Network Analytics comes from combining multiple data sources.
    A connection visible when combining Director Search, UBO Tracer, and
    Unified Search exports may not be apparent in any single source alone.
""",
}
