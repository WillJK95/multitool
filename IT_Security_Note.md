# MultiTool: Technical and Cyber Security Information Note

*For IT Security, Architecture, and Compliance teams assessing MultiTool against organisational risk appetites. Describes MultiTool v0.1.0.*

## Summary

MultiTool is an open-source desktop application used for counter-fraud, grant due diligence, and procurement oversight work. It runs entirely on the end user's machine, with no server component, no cloud storage, and no telemetry. It queries a small, fixed set of public UK government and public-sector APIs over HTTPS. The full source is available on GitHub under an MIT licence with Crown Copyright, and the application runs in user space without administrator privileges.

MultiTool has not undergone a formal third-party penetration test. Section 2 sets out the security posture, attack surface, threat model, and the options available to teams with stricter risk appetites.

## 1. Provenance and licence

- **Developer:** Will Kenny, DDAT, Department for Culture, Media and Sport (DCMS).
- **Licence:** MIT, with Crown Copyright.
- **Source:** Full source available on GitHub — no obfuscated or minified code, all dependencies declared in `requirements.txt`.
- **Authorisation:** Released under departmental consent, with senior DCMS technical leadership in the approval chain.
- **Status:** Not an official government product. Provided "as-is" under MIT.
- **Contact for IT queries:** Will.Kenny@dcms.gov.uk.

## 2. Security validation and threat model

### Penetration testing status

MultiTool has not undergone a formal third-party penetration test. As a free, open-source project without enterprise funding, commercial security testing is not feasible. We state this plainly rather than imply assurance the project does not have.

What MultiTool offers instead is a constrained, inspectable attack surface. It is a local, client-side desktop application with no listening ports, no server component, and no inbound network connections. There is no service for an external attacker to reach. The full source is available for direct review, and the executable can be rebuilt from that source (section 8).

### Attack surface and mitigations

| Surface | Position and mitigation |
|---|---|
| The binary | The released executable is not cryptographically code-signed; code-signing certificates carry prohibitive ongoing costs for an unfunded project. A SHA-256 hash is published for each release on GitHub so IT teams can verify the executable has not been tampered with. Teams that would rather not trust a pre-built binary can compile it themselves from source. |
| Outbound network path | TLS certificate validation is enforced by default on all outbound calls. The set of endpoints contacted is fixed and listed in section 5. |
| HTML report and graph rendering | Reports are generated locally and opened via the `file://` protocol. HTML characters in fetched data are strictly escaped before rendering, mitigating cross-site scripting (XSS) and injection in the generated reports. |
| Credential storage | API keys are never stored in plaintext; they are held in the operating system's native encrypted credential store (section 4). |
| Third-party dependencies | The released executable is a frozen PyInstaller binary and does not pull code from PyPI at runtime. A subsequent upstream supply-chain compromise of a Python package therefore does not affect users already running the executable. All dependencies are declared in `requirements.txt` for independent supply-chain review (section 7). |
| Data | All processed data remains on the user's machine. Only the company name or number needed to fetch a record is transmitted outbound, and only to the public APIs in section 5 (section 3). |

### Realistic worst case

The realistic worst-case scenario for a tool of this kind is remote code execution arising from a vulnerability in a third-party dependency. Because MultiTool is a local application with no inbound connections, an attacker cannot push an exploit to the user. The user would have to pull a malicious payload in — for example, by loading a specially crafted input file (such as a malformed CSV) designed to exploit a specific parsing vulnerability — or an attacker would have to poison the upstream data returned by Companies House or the Charity Commission. Either route requires a highly targeted attack rather than opportunistic compromise.

### Options for stricter risk appetites

Adopting open-source software calls for a different risk assessment from procuring commercial SaaS. The trade-off is transparency, data sovereignty, and unrestricted use in exchange for the absence of a commercial assurance wrapper. Where MultiTool as distributed does not fit your risk appetite, the following options reduce or remove the relevant exposure:

- **Compile from source.** Build the executable yourself from the published source, giving full control over the build environment.
- **Run from source.** Run the application in a controlled Python environment rather than using the packaged executable.
- **Remote desktop.** Run the tool on a remote desktop or virtual environment to keep it off endpoints entirely, at the cost of some user friction.

Whether MultiTool aligns with your organisation's risk appetite remains a decision for your security team. We are happy to answer further technical questions during your validation process.

## 3. Architecture and data residency

- MultiTool is a standalone, locally executed desktop application built in Python.
- **No server component.** There is no cloud back-end, no developer-controlled infrastructure the tool communicates with, and no SaaS dependency.
- **Zero telemetry.** The application contains no tracking, no analytics, and no remote crash reporting.
- **Data at rest.** All data processed by the tool — CSVs loaded by the user, downloaded reports, and caches — remains on the local machine within the user's local directory (`~/.multitool`). No data is transmitted to the developer or any third party other than via the fixed API endpoints listed in section 5.
- **Data lifecycle.** The application does not infinitely retain data. Users can purge all downloaded iXBRL accounts, cached API responses, HTML reports, and application logs at any time via the 'Clear Cache & Logs' function in the application menu.
- **GDPR and compliance.** Because no data is processed on external servers, the tool does not introduce new third-party data processors to your supply chain.
- **Output rendering.** The tool generates standard HTML files (saved locally) and opens them in the user's default web browser via the `file://` protocol. Browsers must be permitted to execute standard JavaScript (e.g., D3.js) on local files to render the interactive graphs.

## 4. Installation and deployment

Two installation routes are supported. Both run entirely in user space; neither requires administrator or root privileges, and neither installs system services, scheduled tasks, or kernel-level components.

- **Packaged executable (recommended for most users).** A standalone executable built with PyInstaller is available from the GitHub Releases page (v0.1.0). No separate Python installation or dependency setup is required.
- **From source.** Users with Python already installed can clone the repository, install dependencies from `requirements.txt`, and run from the command line or PowerShell. Teams that prefer not to trust a pre-built binary can also compile their own executable this way (section 2).

The application does not auto-update. Moving to a newer version requires the user (or their IT team) to download and install it deliberately, which allows organisations to pin to a reviewed version.

**Note for EDR/Antivirus:** As is common with PyInstaller bundles, enterprise EDR solutions (e.g., Defender, CrowdStrike) may initially flag the executable as an 'unrecognized binary' or false positive. This is expected for an unsigned binary; the SHA-256 hash published with each release on GitHub can be used to verify file integrity.

## 5. Credential management

- The tool requires API keys to interact with UK Government services (principally a free Companies House API key obtained by the user from Companies House directly).
- **No plaintext storage.** Keys are not stored in plaintext within the application or its configuration files.
- **Native OS credential stores.** The application uses the Python `keyring` library to hand off credential storage to the operating system's native, encrypted credential manager — Windows Credential Manager, macOS Keychain, or Linux Secret Service, depending on the platform.

## 6. Network traffic and allowlisting

Outbound network access is required for the tool to function. The network footprint is predictable and static, and limited to the endpoints listed below. TLS certificate validation is enforced by default on all of these calls.

**Core public sector APIs (application traffic).** The application makes direct HTTPS REST calls to fetch live registry and grant data from the following endpoints:

- `https://api.company-information.service.gov.uk` — Companies House
- `https://document-api.companieshouse.gov.uk` — Companies House Documents
- `https://api.charitycommission.gov.uk/register/api` — Charity Commission
- `https://api.threesixtygiving.org/api/v1` — GrantNav / 360Giving

**Third-party geocoding and visualisation (application and browser traffic).** To generate network graphs and calculate geographic distances, the tool relies on standard open-source data repositories and Content Delivery Networks (CDNs):

- `https://download.geonames.org` — Contacted via HTTPS by the application's underlying geospatial library (`pgeocode`) to download and cache the UK postcode database locally. This is required for the "Inferred Links" proximity scanning feature.
- `https://unpkg.com` / `https://cdnjs.cloudflare.com` — Contacted by the user's default web browser when opening the locally generated HTML network graphs. The local HTML files pull standard open-source JavaScript rendering libraries (such as D3.js) from these public CDNs to display the interactive visualisation.

**Rate limiting.** The tool uses an internal `TokenBucket` algorithm (configured with both 'smooth' and 'burst' pacing modes) to strictly respect the external API rate limits set by Companies House and the Charity Commission, ensuring the host network does not trigger IP-level blocks.

## 7. Privilege and runtime footprint

- Runs in user space. No administrator or root rights required at install or runtime.
- No services, scheduled tasks, or kernel modules installed.
- No system-level configuration changes.
- Works offline for analysis of previously-saved data; internet access is only required when making live API calls.

## 8. Code auditability

The tool is fully open-source. There is no code obfuscation or compiled black-box logic, and all third-party dependencies are declared in `requirements.txt` in the repository. IT security teams are welcome to review the source directly on GitHub, run dependency analysis against `requirements.txt`, rebuild the executable from source to control the build environment, or observe network behaviour via a logging proxy to verify the claims above before approving the tool for local deployment.

## 9. Contact

For any questions, clarifications, or to request a walkthrough, please contact Will Kenny — Will.Kenny@dcms.gov.uk.
