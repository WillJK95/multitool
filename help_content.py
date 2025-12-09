# multitool/help_content.py
"""Help content for all modules."""

HELP_CONTENT = {
    "main": """
Welcome to the Data Investigation Multi-Tool.

This tool is designed to assist with due diligence and counter-fraud investigations by pulling data from UK Companies House, the Charity Commission, and 360Giving.

--- HOW TO USE ---

1.  Select an investigation type from the main menu. Most investigations require an API key, which will be saved securely in your system's credential manager after you enter it for the first time.

2.  Follow the steps within each module, which typically involve uploading a CSV file and selecting the relevant columns to analyze.

3.  Run the investigation and export the results. For Director, UBO, and Network Creator investigations, you can also generate interactive network graphs.

--- TIPS ---
-   **API Keys:** You can reset your stored API keys from the main menu at any time.
-   **Cache:** The tool saves temporary files (logs, graphs) in a hidden folder in your user directory (located at C:/Users/[YourName]/.DataInvestigatorTool. You can clear these files using the "Clear Cache & Logs" button.
-   **CSV Files:** All input files must be in .csv format. The tool will attempt to read files with UTF-8 and CP1252 encoding.
""",
    "api_keys": """
--- About API Keys ---

An API (Application Programming Interface) is a way for computer programs to talk to each other. To use this tool, you need to get a free "API Key" from Companies House and/or the Charity Commission. This key is like a password that identifies your tool when it requests data.

The keys you enter are stored securely in your system's credential manager and are not shared.

--- How to get your keys ---

1.  **Companies House:**
    -   Go to: https://developer.company-information.service.gov.uk/
    -   Register for an account.
    -   Once logged in, go to "Your Applications" and create a new application.
    -   Select "REST" as the application type.
    -   Your new key will be generated. Copy and paste it into the box in the tool.

2.  **Charity Commission:**
    -   Go to: https://api-portal.charitycommission.gov.uk/
    -   Register for an account.
    -   Once logged in, go to your "Profile" page.
    -   Under "Subscriptions", you will find your primary and secondary keys. You can use either one.
    -   Copy and paste the key into the box in the tool.
""",
    "director": """
--- Director Investigation Help ---

This module finds all company appointments for a given director's name.

1.  **Full Name:** Enter the director's full name. For common names, be as specific as possible (e.g., include middle names or initials).

2.  **Date of Birth:** Providing a year and/or month of birth is optional but HIGHLY RECOMMENDED. It significantly narrows down the search and helps find the correct person.

--- QUIRKS & TIPS ---
-   **1000 Result Limit:** The Companies House API will only return a maximum of 1000 initial search results. For very common names (e.g., "John Smith"), the person you are looking for may not appear. Please note date of birth filtering only occurs after the initial name search, so in the case of very generic names you are advised to use caution in interpreting results.
-   **Network Graph:** The graph shows all companies, their directors/PSCs, and registered addresses. This is useful for spotting shared addresses or co-directors.
-   **Export Graph Data:** This exports a list of connections (an "edge list") which can be used in the 'Network Graph Creator' to build a larger, combined graph.
""",
    "ubo": """
--- UBO Investigation Help ---

This module traces the ultimate beneficial ownership (UBO) chain for a list of companies by finding their Persons with Significant Control (PSCs).

1.  **Input:** The CSV file must contain a column with company registration numbers.

2.  **Process:** The tool will recursively search for PSCs. If a PSC is another company, it will then find the PSCs of that company, and so on, up to 20 levels deep.

--- QUIRKS & TIPS ---
-   **Visual Graph vs. Data Export:** The "Generate Visual Graph" button creates a clean, hierarchical chart showing ONLY the PSC ownership chain. The "Export Graph Data (CSV)" button exports a much more detailed file that includes ALL directors of ALL companies found in the chain, which is useful for deeper analysis.
-   **Snapshot Date:** Use this to see the ownership structure on a specific date in the past. If left blank, it will show all current and historic PSCs.
-   **Shared PSCs:** In the visual graph, any person or company that is a PSC for more than one entity in the chain will be highlighted in orange.
""",
    "network_creator": """
--- Network Graph Creator Help ---

This powerful module allows you to combine multiple data files into a single, large network graph to find hidden connections between entities.

The workflow is designed to be followed sequentially from top to bottom.

--- HOW TO USE ---

**Step 1: Seed Network with a Company (Optional)**
Enter a single company number to automatically fetch its directors and registered address. By default, this provides a simple network of the company, its officers, and address.

Optional checkboxes allow you to expand the scope:
- **Fetch PSCs:** Also retrieves Persons with Significant Control for each company.
- **Fetch all associated companies:** Retrieves all other companies where each director holds an appointment. Warning: this can result in a large number of API calls and a complex network.

**Step 2: Add Data Files**
- **Add Exported Graph Files:** Add the "edge list" CSV files you exported from the Director and/or UBO investigation modules. You can add multiple files.
- **Add Cohort File to Highlight (Optional):** Upload a simple, one-column CSV of entity IDs (e.g., company numbers, person IDs). Any of these entities that appear in the final graph will be visually highlighted with a dashed border.

**Step 3: Build Combined Network**
This is the main processing step. Clicking the **Build Combined Network** button reads all the source files you've added and constructs the master graph in memory.
- **This step is mandatory.** You must build the network before you can perform any of the analysis steps below.

**Step 4: Remove Entities (Optional)**
This is a powerful filtering tool used to remove noisy "supernodes" (like company formation agents or common administrative addresses) that create thousands of meaningless connections.
1. After building the network, type in the box to search for an entity.
2. Select it from the dropdown and click **"Add to Removal List"**.
3. Repeat for all entities you want to exclude.
- **Note:** This removal is temporary and non-destructive. It only applies to the analysis you run next (in steps 5, 6, and 7). The master graph remains untouched, so you can change your removal list and re-run the analysis without having to rebuild.

**Step 5: Generate Full Visual Graph**
This creates and opens an interactive graph of your network. The checkboxes allow you to customize the output:
- **Visually distinguish nodes:** Adds a colored border to nodes based on which source file they came from.
- **Eliminate unconnected companies:** Hides any "network islands" that do not contain at least two companies.
- **Show only networks connecting cohort members:** If you uploaded a cohort file in Step 2b, this will only display the sub-networks that contain two or more of your cohort members.

**Step 6: Find Connection Between Two Entities**
This finds the single shortest path between any two nodes in your graph. The search is performed on the *current* state of the graph, meaning it will respect any nodes you have removed in Step 4.

**Step 7: Find Connections Between Cohorts**
This is a bulk analysis tool to find all paths between two entire groups of entities.
1. Upload two separate single-column CSV files containing your entity IDs (Cohort A and Cohort B).
2. Select the **Max Hops** (maximum path length). Be warned: high numbers (> 6) on dense graphs can be extremely slow.
3. Choose whether to find the **Shortest Connection Only** (fast, one result per pair) or all connections (can be slow and produce huge files).
4. Click **Find Connections & Export...** to run the analysis and save the results to a CSV, where each row is a complete path.
""",
    "unified_search": """
--- Unified Bulk Search Help ---

This is a combined company and charity search tool. It allows you to check a list of identifiers against multiple databases (Companies House and Charity Commission) in a single run.

1.  **Select Databases:** Tick the boxes for the databases you want to search.

2. **Select Priority:** If your file contains mostly companies or mostly charities, set your preference for priority (both databases will be searched regardless, but choosing the correct one to prioritise will speed up the matching process, especially if fuzzy matching.

3.  **Select Columns:**
    -   **Distinct Columns:** If your file has separate columns for company numbers and charity numbers, map each one accordingly. The tool will check the relevant database for each column.
    -   **Single Column:** If your file has a single column of mixed or unknown identifiers, map that SAME column to BOTH the "Company Number" and "Charity Number" dropdowns.

4.  **Search Logic (Single Column):** When using a single column, the tool uses a default search order. For each row, it will:
    a) First, search Companies House with the identifier.
    b) If, and only if, no match is found, it will then search the Charity Commission with the same identifier.

--- OUTPUT ---
-   **match_source:** Shows which database(s) a match was found in.
-   **match_status:** Confirms if a match was found or not.
-   The file will contain columns for both company and charity data. For any given row, the columns for the database that was not matched will be blank.
""",
}
