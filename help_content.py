# multitool/help_content.py
"""Help content for all modules.

These strings are displayed in the in-app Help panel. Keep them user-facing, task-focused,
and consistent with the current UI labels.
"""

HELP_CONTENT = {
    "main": """
Welcome to the Data Investigation Multi-Tool.

This tool supports due diligence, counter-fraud, and conflict-of-interest investigations by pulling data from:
- UK Companies House
- Charity Commission (England & Wales)
- 360Giving (via GrantNav)
- Contracts Finder

--- HOW TO USE ---
1. Select a module from the main menu.
2. Follow the numbered steps inside that module (usually: upload a CSV, select columns, run, then export).
3. Use **Export** buttons to save outputs for casework, audit trails, or further analysis.

--- COMMON TIPS ---
- **API keys:** Companies House and Charity Commission modules require keys. You can reset stored keys from the main menu.
- **Cache & logs:** The tool saves temporary files (logs, graphs, reports) in a hidden folder in your user directory.
  You can clear these using the "Clear Cache & Logs" button.
- **CSV encoding:** The tool attempts to read CSVs as UTF‑8 first, then CP1252.
- **Graph workflow:** Several modules can export graph data (edge lists). These can be combined in **Network Analytics**
  to build a larger graph and search for connections.
""",

    "api_keys": """
--- About API Keys ---

An API (Application Programming Interface) is a way for computer programs to talk to each other. To use this tool,
you need a free API key from Companies House and/or the Charity Commission.

The keys you enter are stored securely in your system’s credential manager and are not shared.

--- How to get your keys ---

1) **Companies House**
- Register at: https://developer.company-information.service.gov.uk/
- Go to “Your Applications”, create an application, choose “REST”
- Copy the generated key into the tool

2) **Charity Commission**
- Register at: https://api-portal.charitycommission.gov.uk/
- In your “Profile” → “Subscriptions”, copy your primary or secondary key into the tool
""",

    "director": """
--- Director Investigation Help ---

This module finds company appointments for a given director/officer name (Companies House).

1) **Full Name**
Enter the director’s full name. For common names, include middle names/initials where possible.

2) **Year / Month of Birth (Optional, but recommended)**
Adding year and/or month of birth can significantly narrow down results.

--- QUIRKS & TIPS ---
- **1000 result limit:** Companies House officer search returns a maximum of 1000 results. For very common names,
  the correct person may not appear in the first 1000. Date-of-birth filtering happens *after* the initial search,
  so use extra caution with generic names.
- **Export Directorships:** Saves the appointment list you see in the table.
- **Obtain Grants Data & Export:** For the companies found, fetches associated 360Giving grant records and exports
  a separate file (useful for spotting repeat funders, patterns, and related entities).
- **Generate Visual Graph:** Opens an interactive network graph of companies, officers/PSCs, and addresses.
- **Export Graph Data (CSV):** Exports graph data for reuse in **Network Analytics** (e.g., to merge with other modules’ exports).
""",

    "ubo": """
--- UBO Investigation Help ---

This module traces Ultimate Beneficial Ownership (UBO) chains by finding Persons with Significant Control (PSCs).

1) **Input**
Upload a CSV containing a column of company registration numbers.

2) **Process**
The tool recursively follows PSCs. If a PSC is another company, it fetches that company’s PSCs, and so on (up to 20 levels).

--- QUIRKS & TIPS ---
- **Visual Graph vs Data Export:** The visual graph is a clean, hierarchical ownership chart. The CSV export is more detailed
  and is intended for deeper analysis and for combining in **Network Analytics**.
- **Snapshot Date:** Use this to approximate the ownership structure on a specific date. If left blank, the tool will include
  current and historical PSC data returned by Companies House.
- **Shared PSCs:** Entities that appear as PSCs for multiple companies in the chain are highlighted in the visual graph.
""",

    "unified_search": """
--- Unified Bulk Search Help ---

This module checks a list of identifiers against multiple databases in one run (Companies House + Charity Commission).

--- SETUP ---
1) **Select Databases**
Tick the sources you want to query.

2) **Select Priority**
If your file is mostly companies or mostly charities, set the priority accordingly. Both sources can still be queried,
but the priority setting can speed up matching (especially when fuzzy matching by name).

3) **Select Columns**
- **Distinct columns:** Map company numbers to “Company Number” and charity numbers to “Charity Number”.
- **Single mixed column:** Map the same column to both dropdowns. The tool will try Companies House first, then (if no match)
  try the Charity Commission.

4) **Fuzzy matching (if enabled)**
If identifiers are names rather than numbers, fuzzy matching will attempt to find the closest record above your threshold.
Outputs include **match_score** and **matched_name** for auditability.

--- OUTPUT ---
- **match_status:** “Match Found” or “No Match Found”
- **match_source:** Which database(s) matched, and whether the match was exact or fuzzy
- Enriched columns for the matched source(s); non-matched source columns remain blank

--- GRAPH EXPORT (Optional) ---
Use **Export Graph Data (CSV)** to build a combined graph from your matched results. This can include:
- Companies, officers, PSCs, and registered addresses (Companies House)
- Charities and trustees (Charity Commission)
You can then combine and analyse this in **Network Analytics**.
""",

    "grants_search": """
--- Grants Search Help (360Giving / GrantNav) ---

This module finds 360Giving grant records linked to companies and/or charities.

--- WORKFLOW ---
1) **Upload Input File**
Upload a CSV containing identifiers.

2) **Select Identifier Columns**
Choose either (or both):
- a **Company Number** column (Companies House numbers), and/or
- a **Charity Number** column (Charity Commission numbers)

3) **Select Grant Data Fields**
Choose which grant fields you want in the output (you can Select/Deselect All).

4) **Run Investigation**
For each row, the tool:
- tries Company Number first (converted to the standard 360Giving organisation ID format), then
- falls back to Charity Number if no grants were found via company number

--- OUTPUT ---
- If grants are found, the tool will output one row per grant (duplicating your input row to preserve your context).
- If no grants are found, you still get a row back with **grant_search_status** explaining what was attempted.

--- TIPS ---
- Company numbers are normalised to 8 digits (e.g., 123456 becomes 00123456) for 360Giving organisation IDs.
- The GrantNav API is rate-limited; large files will take time.
""",

    "contracts_finder": """
<h2>Contracts Finder</h2>

<p>This module allows for details of public sector contract awardees to be gathered based on the buyer organisation, using the UK Government Contracts Finder API.</p>

<h3>Workflow</h3>
<ol>
  <li><b>Search Contracts</b> – enter a buyer organisation and date range to find awarded contracts</li>
  <li><b>Enrich Suppliers</b> – fetch supplier director/PSC information from Companies House (where available)</li>
  <li><b>Export Results</b> – save outputs for analysis</li>
</ol>

<h3>Exports</h3>
<ul>
  <li><b>Supplier export</b> – enriched supplier details and contract summary fields</li>
  <li><b>Export Graph Data</b> – outputs an edge list (companies ↔ officers/PSCs ↔ addresses) compatible with <b>Network Analytics</b></li>
</ul>

<h3>Tips</h3>
<ul>
  <li>Use the buyer name as it appears in Contracts Finder and be aware that the name must appear exactly as it does in Contracts Finder (inclusive of commas etc.)</li>
</ul>
""",

    "data_match": """
--- Data Match Help ---

This module joins (matches) two CSV files together using either:
- an exact match on a unique identifier (e.g., company number), or
- a fuzzy match on a text field (e.g., organisation or person name).

--- WORKFLOW ---
1) **Upload Files**
- Upload a Primary (left) file
- Upload a Matching (right) file

2) **Choose Matching Logic**
- **Exact match on identifier:** best for company numbers, payroll IDs, invoice references, etc.
  - Optional: pad numeric identifiers to 8 digits (useful for UK company numbers).
- **Fuzzy match on text:** best for names where spelling varies.
  - Use the accuracy slider to trade recall vs precision.

3) **Select Columns**
Pick the join columns (and any additional columns from the matching file you want to append).

4) **Run & Export**
The export will contain the original primary file rows, plus the matched columns from the right-hand file (where found),
and match metadata (e.g., match score) when using fuzzy matching.

--- TIPS ---
- Fuzzy matching is powerful but can create false positives. Treat outputs as leads, not conclusions.
- If you have both a number and a name, prefer number matching first and use fuzzy matching as a fallback.
""",

    "enhanced_dd": """
--- Enhanced Due Diligence (EDD) Help ---

This module generates a structured due diligence report for a single company using Companies House data, with optional
financial analysis from iXBRL accounts files.

--- WORKFLOW ---
1) **Enter Company Number**
Click <b>Fetch Company Data</b> to retrieve:
- company profile
- officers
- PSCs
- filing history (recent items)

2) **Upload iXBRL Accounts (Optional)**
Upload one or more iXBRL accounts files (XHTML/HTML). If accounts are loaded, additional financial checks become available.

3) **Choose Checks**
Tick the checks you want to run. Some checks depend on accounts data and will be disabled until accounts are loaded.

4) **Generate Report**
The tool outputs an HTML report and opens it in your browser. It includes:
- executive summary
- company profile
- risk indicators grouped by severity
- (if accounts were uploaded) charts and financial commentary
- limitations & disclaimers section for safe use in government settings

--- NOTES ---
- The report is point-in-time and relies on filed/public data. It should not be the sole basis for decisions.
- iXBRL files are validated against the selected company number where possible; you will be warned if they appear to mismatch.
""",

    # Backwards-compatible key (older UI label)
    "network_creator": """
--- Network Analytics Help ---

This module has been renamed to <b>Network Analytics</b>. See the Network Analytics help section for the current workflow.
""",

    "network_analytics": """
--- Network Analytics Help ---

This module lets you combine multiple exported graph files into a single network graph, refine it, and then analyse
connections between entities (companies, people, charities, addresses).

It includes two tabs:
- <b>Network Analytics</b> (build, refine, analyse, visualise)
- <b>Data Converter</b> (turn non-graph CSVs into a basic edge list format)

--- TAB 1: NETWORK ANALYTICS ---

<b>DATA SOURCES</b>
- Add one or more exported graph CSV files (edge lists) from other modules.
- Optional: seed the network by fetching a single company (with options to expand via PSCs / associated companies).

<b>BUILD & REFINE</b>
- Build the combined graph (mandatory before analysis).
- Remove entities to reduce noise (temporary, non-destructive pruning).
  Use this to exclude “supernodes” such as formation agents or mailbox addresses.

<b>ANALYSE</b>
- Find the shortest connection between two entities.
- Find connections between two lists (List A vs List B), or within one list (all vs all).
  <i>Max hops</i> limits path length; large values on dense graphs can be slow.

<b>Hidden links discovery (optional)</b>
Scan the current (pruned) graph for inferred links not present in source data, such as:
- neighbouring registered addresses (postcode proximity)
- people with the same surname at the same postcode / within a proximity radius
These are investigative leads and should be treated as hypotheses, not evidence.

<b>VISUALISE</b>
Generate an interactive graph. Options typically include:
- distinguish nodes by source file
- hide isolated “islands”
- highlight a cohort/list of entity IDs

--- TAB 2: DATA CONVERTER ---
A wizard to convert a normal CSV into a simple edge list. Typical uses:
- linking “supplier → company number”
- linking “grant recipient → identifier”
- linking “staff member → organisation / address”
The output can then be imported into the Network Analytics graph alongside other exports.
""",
}
