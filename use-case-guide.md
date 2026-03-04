# MultiTool Use Case Guide: Investigation Workflows

MultiTool's real power isn't in any single module — it's in chaining them together. This guide walks through five realistic scenarios showing how modules feed into each other, with each workflow converging in the Network Analytics Workbench.

Every button name and field label in **bold** matches the actual interface.

---

## Use Case 1: Proactive Procurement Screening (Pocket NFI)

**The problem:** Westborough Metropolitan Borough Council wants to screen staff against supplier officers to catch undisclosed conflicts — the same logic as NFI, but on demand rather than every two years.

**The workflow has two entry points depending on what data you have:**

**Option A** — no supplier list to hand: Use **Contracts Finder** → search "Westborough Metropolitan Borough Council", enrich suppliers with **Fetch Officers/Directors**, **Fetch PSCs**, and **Fetch Registered Address**, then **Export Graph Data**.

**Option B** — Finance has provided a supplier list: Use **Bulk Entity Search** → upload the supplier CSV, enrich with the same officer/PSC/address fields, then **Export Graph Data (CSV)**.

**Both routes converge in the Network Analytics Workbench:**

1. In the **Data Converter** tab, load your HR staff list. Set Entity Type to **Person**, map the **Full Name** column, and add Home Postcode as a **Linked Attribute**. Generate as **Create Graph File (Nodes & Links)**.
2. In the **Network Analytics** tab, use **Add File(s)...** to load both the supplier graph and the staff graph.
3. **Scan for Duplicates** to merge name variants across sources (Companies House records "SMITH, John" while HR has "John Smith").
4. Exclude supernodes — formation agent addresses that connect to dozens of unrelated companies and drown out real signals.
5. **Scan for Inferred Links** — this is the key step. The Workbench finds people who share a surname and postcode, surfacing family connections a simple name match would miss entirely.
6. Under **ANALYSE**, select **Between two entity lists** (staff vs suppliers) and run **Find Connections & Export**.
7. **Generate Network Graph** with **Show inferred connections (dotted lines)** and **Colour-code nodes by source file** enabled.

**What this finds:** Rather than just catching "John Smith is a director of his own supplier" (which is rare and usually declared), the power is in inferred links. The Workbench might surface: *Sarah Okonkwo (Staff, Housing Dept) lives at postcode BD7 3PQ → David Okonkwo (Director, Okonkwo & Partners Consulting Ltd) is also at BD7 3PQ*. A likely spousal connection to a supplier holding a £420,000 housing maintenance contract — invisible to any name-matching exercise, but clear as day in the network graph.

---

## Use Case 2: Conflicts of Interest Detection

**The problem:** A councillor at Eastmoor City Council has raised concerns that companies giving gifts to council officers seem to keep winning contracts. The gifts & hospitality register records transactions, but says nothing about who actually owns the gift-giving companies.

**The workflow:**

1. **Bulk Entity Search** → upload the gifts register CSV (company names column), enrich donors with officers, PSCs, and addresses. **Export Graph Data (CSV)**.
2. **Director Search** → when a name catches your eye in the PSC data (e.g., "Margaret Hennessy" is PSC of a frequent gift-giver, and there's a Cllr Hennessy on the Planning Committee), deep-dive on that individual to map their full corporate footprint. Export graph data.
3. **Network Analytics Workbench** → load both graph files plus a staff/councillor graph from **Data Converter**. **Scan for Duplicates**, then **Scan for Inferred Links**. Run a **Between two entity lists** analysis (staff vs gift-giving companies).

**What this finds:** *Margaret Hennessy (PSC, 75% shareholder of Thornfield Construction plc) shares a postcode with Cllr James Hennessy (Planning Committee)*. A likely family connection between a major gift-giver and a planning decision-maker — a relationship that should have been declared and wasn't.

---

## Use Case 3: Grant Fraud Detection

**The problem:** The Midlands Regional Growth Fund has noticed several CICs receiving grants just below the £25,000 enhanced audit threshold. The fraud analyst suspects the same people control multiple entities and are structuring applications to avoid scrutiny.

**The workflow:**

1. **Grants Search** → find all grants to suspect entities and identify any additional organisations at the same addresses or with similar names.
2. **UBO Tracer** → trace each suspect CIC's ownership chain. Even short chains are revealing: "Midlands Youth Skills CIC" → PSC: Darren Fletcher. "Central Community Training CIC" → PSC: Darren Fletcher. "Heartlands Enterprise Development CIC" → PSC: Karen Fletcher.
3. **Bulk Entity Search** → enrich all suspect CICs with full officer/address data. **Export Graph Data (CSV)**.
4. **Network Analytics Workbench** → load all graph files. Use the **Data Converter** to add linked attributes from grant application data — particularly bank account details, which are evidence of common control. **Scan for Duplicates** and **Scan for Inferred Links** then run **Within a single entity list** to add all of the grant applicants.

**What this finds:** *Darren Fletcher is PSC of two CICs, Karen Fletcher is director of a third at the same residential address, and two of the three entities share a bank account.* Total grants controlled by a single household: £70,500, deliberately split to stay below audit thresholds.

---

## Use Case 4: Cartel and Bid-Rigging Indicators

**The problem:** Northern Transport Authority received three suspiciously close bids for a £15 million highway maintenance contract. The investigator needs to know whether the bidders are truly independent competitors.

**The workflow:**

1. **Contracts Finder** → search "Northern Transport Authority", enrich all suppliers, **Export Graph Data**. Also check historical contracts — bid rotation (where the same three companies take turns winning) leaves a trail.
2. **UBO Tracer** → trace each of the three suspect companies through their ownership layers. This is where cartel structures hide. Follow the PSC chain upward through any holding companies.
3. **Network Analytics Workbench** → load Contracts Finder and UBO Tracer graph data. **Scan for Duplicates**, **Scan for Inferred Links**. Run **Within a single entity list** on the three suspects with a maximum of 5 hops.

**What this finds:** *Pennine Highways Ltd and Northern Road Services Ltd both trace back through intermediate holding companies to the same ultimate parent: Baltic Ventures Holdings BV (Netherlands). CrossCountry Surfacing Ltd shares a registered address with Northern Road Services.* Three "competitors" — two with the same offshore parent, two sharing an address. The competition was illusory.

---

## Use Case 5: Lynchpin Analysis

**The problem:** 15 companies have been identified in a suspected carousel fraud. There is likely to be a ringleader, company or address that connects these shell companies — but that person may not be an obvious director of any single entity.

**The workflow:**

1. **UBO Tracer** + **Bulk Entity Search** → trace and enrich all 15 companies. Export all graph data. You want maximum data depth here: ownership chains, officers, PSCs, addresses.
2. **Network Analytics Workbench** → load everything. Use **Fetch & Add Network Data** with **Seed from Company** on key suspects to pull in associated entities beyond the original 15. **Scan for Duplicates** aggressively (essential for accurate connection counts). Remove supernode virtual offices. **Scan for Inferred Links**.
3. **Find Lynchpins** → add the 15 suspects. First run: **People only**, minimum 2 connections, max hop distance 1 (tight, conservative). Second run: **All entity types**, max hop distance 2 (wider, catches intermediary companies and controlling addresses).
4. **Generate Network Graph** with **Scale node size by connection count** and **Show only networks containing connections**.

**What this finds:** *Michael Reeves connects to 6 of the 15 suspect companies as director or PSC — 40% of the network, from a single individual. At hop distance 2, "14 Thornton Business Park" emerges as a shared registered address for 6 companies — a virtual office arranged specifically for the fraud infrastructure.* The lynchpin analysis surfaces the ringleader and the enabling address that a company-by-company investigation would have taken months to piece together.
