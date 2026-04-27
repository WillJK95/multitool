# MultiTool: Technical and Cyber Security Information Note

*For IT Security, Architecture, and Compliance teams assessing MultiTool against organisational risk appetites. Describes MultiTool v0.1.0.*

## Summary

MultiTool is an open-source desktop application used for counter-fraud, grant due diligence, and procurement oversight work. It runs entirely on the end user's machine, with no server component, no cloud storage, and no telemetry. It queries a small, fixed set of public UK government and public-sector APIs over HTTPS. The full source is available on GitHub under an MIT licence with Crown Copyright, and the application runs in user space without administrator privileges.

## 1. Provenance and licence

- **Developer:** Will Kenny, DDAT, Department for Culture, Media and Sport (DCMS).
- **Licence:** MIT, with Crown Copyright.
- **Source:** Full source available on GitHub — no obfuscated or minified code, all dependencies declared in `requirements.txt`.
- **Authorisation:** Released under departmental consent, with senior DCMS technical leadership in the approval chain.
- **Status:** Not an official government product. Provided "as-is" under MIT.
- **Contact for IT queries:** Will.Kenny@dcms.gov.uk.

## 2. Architecture and data residency

- MultiTool is a standalone, locally executed desktop application built in Python.
- **No server component.** There is no cloud back-end, no developer-controlled infrastructure the tool communicates with, and no SaaS dependency.
- **Zero telemetry.** The application contains no tracking, no analytics, and no remote crash reporting.
- **Data at rest.** All data processed by the tool — CSVs loaded by the user, downloaded reports, and caches — remains on the local machine within the user's local directory (`~/.multitool`). No data is transmitted to the developer or any third party other than via the fixed API endpoints listed in section 5.
- **Data lifecycle.** The application does not infinitely retain data. Users can purge all downloaded iXBRL accounts, cached API responses, HTML reports, and application logs at any time via the 'Clear Cache & Logs' function in the application menu.
- **GDPR and compliance.** Because no data is processed on external servers, the tool does not introduce new third-party data processors to your supply chain.
- **Output rendering.** The tool generates standard HTML files (saved locally) and opens them in the user's default web browser via the `file://` protocol. Browsers must be permitted to execute standard JavaScript (e.g., D3.js) on local files to render the interactive graphs.

## 3. Installation and deployment

Two installation routes are supported. Both run entirely in user space; neither requires administrator or root privileges, and neither installs system services, scheduled tasks, or kernel-level components.

- **Packaged executable (recommended for most users).** A standalone executable built with PyInstaller is available from the GitHub Releases page (v0.1.0). No separate Python installation or dependency setup is required.
- **From source.** Users with Python already installed can clone the repository, install dependencies from `requirements.txt`, and run from the command line or PowerShell.

The application does not auto-update. Moving to a newer version requires the user (or their IT team) to download and install it deliberately, which allows organisations to pin to a reviewed version.

**Note for EDR/Antivirus:** As is common with PyInstaller bundles, enterprise EDR solutions (e.g., Defender, CrowdStrike) may initially flag the executable as an 'unrecognized binary' or false positive. Hash values of official releases are provided on GitHub to verify file integrity.

## 4. Credential management

- The tool requires API keys to interact with UK Government services (principally a free Companies House API key obtained by the user from Companies House directly).
- **No plaintext storage.** Keys are not stored in plaintext within the application or its configuration files.
- **Native OS credential stores.** The application uses the Python `keyring` library to hand off credential storage to the operating system's native, encrypted credential manager — Windows Credential Manager, macOS Keychain, or Linux Secret Service, depending on the platform.

## 5. Network traffic and allowlisting

Outbound network access is required for the tool to function. The network footprint is predictable and static, and limited to the endpoints listed below.

**Core public sector APIs (application traffic).** The application makes direct HTTPS REST calls to fetch live registry and grant data from the following endpoints:

- `https://api.company-information.service.gov.uk` — Companies House
- `https://document-api.companieshouse.gov.uk` — Companies House Documents
- `https://api.charitycommission.gov.uk/register/api` — Charity Commission
- `https://api.threesixtygiving.org/api/v1` — GrantNav / 360Giving

**Third-party geocoding and visualisation (application and browser traffic).** To generate network graphs and calculate geographic distances, the tool relies on standard open-source data repositories and Content Delivery Networks (CDNs):

- `https://download.geonames.org` — Contacted via HTTPS by the application's underlying geospatial library (`pgeocode`) to download and cache the UK postcode database locally. This is required for the "Inferred Links" proximity scanning feature.
- `https://unpkg.com` / `https://cdnjs.cloudflare.com` — Contacted by the user's default web browser when opening the locally generated HTML network graphs. The local HTML files pull standard open-source JavaScript rendering libraries (such as D3.js) from these public CDNs to display the interactive visualisation.

**Rate limiting.** The tool uses an internal `TokenBucket` algorithm (configured with both 'smooth' and 'burst' pacing modes) to strictly respect the external API rate limits set by Companies House and the Charity Commission, ensuring the host network does not trigger IP-level blocks.

## 6. Privilege and runtime footprint

- Runs in user space. No administrator or root rights required at install or runtime.
- No services, scheduled tasks, or kernel modules installed.
- No system-level configuration changes.
- Works offline for analysis of previously-saved data; internet access is only required when making live API calls.

## 7. Code auditability

The tool is fully open-source. There is no code obfuscation or compiled black-box logic, and all third-party dependencies are declared in `requirements.txt` in the repository. IT security teams are welcome to review the source directly on GitHub, run dependency analysis against `requirements.txt`, or observe network behaviour via a logging proxy to verify the claims above before approving the tool for local deployment.

## 8. Contact

For any questions, clarifications, or to request a walkthrough, please contact Will Kenny — Will.Kenny@dcms.gov.uk.
