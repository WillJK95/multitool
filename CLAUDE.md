# CLAUDE.md — multitool

Desktop data-investigation tool (Python/tkinter) that queries UK public APIs
(Companies House, Charity Commission, 360Giving GrantNav, Contracts Finder)
for fraud analysis, due diligence, and compliance work.

---

## Directory layout

```
api/        Thin API clients — one file per external service.
modules/    Investigation modules (UI + business logic). One class per feature.
ui/         Reusable tkinter widgets (scroll frame, tooltips, help window, etc.).
utils/      Shared helpers: rate limiting, fuzzy matching, enrichment, EDD
            visualisations, financial analysis.
main.py     Entry point — creates App() and calls mainloop().
app.py      Main window, menu bar, API key management, TokenBucket init.
constants.py ALL base URLs, keyring names, rate-limit defaults, field defs.
            Always add new constants here, never inline them elsewhere.
```

---

## Architecture patterns

### Every investigation module follows the same shape
- Subclass `InvestigationModuleBase` (`modules/base.py`)
- `__init__` calls `super().__init__(parent_app, back_callback, api_key, help_key)`
- Heavy work runs in a `threading.Thread(daemon=True)`; all UI touches go
  through `self.safe_ui_call(fn, *args)` or `self._tracked_after(ms, fn)`
- Cancellation is signalled via `self.cancel_flag` (`threading.Event`)
- Register the new module in `app.py` (add a button in `_create_main_menu`)

### API clients all return `(data, error)` tuples
```python
data, error = ch_get_data(api_key, token_bucket, "/company/12345678")
if error:
    ...   # error is a human-readable string; data is None
```

### Caching — success-only in both CH and CC clients
`api/companies_house.py` and `api/charity_commission.py` each maintain a
module-level `_cache` dict (max 1024 entries, LRU-evicted). Errors are **never**
cached so transient failures (429, network blip) are retried on the next call.
Do **not** use `@lru_cache` on any function that takes an API key — it would
cache errors permanently for the session.

### Rate limiting
- **Companies House**: adaptive `TokenBucket` (`utils/token_bucket.py`) created
  in `app.py` and passed to every CH call. Capacity and refill rate are set from
  `X-Ratelimit-*` response headers automatically. Two pacing modes: `smooth`
  (default) and `burst`.
- **Charity Commission / GrantNav / Contracts Finder**: simple module-level
  `_last_request_time` + lock approach (0.5 s minimum gap). Correct — do not
  replace with TokenBucket without adding per-service bucket init.

### Pagination — already handled in wrappers
`ch_get_officers(api_key, bucket, company_number)` and
`ch_get_pscs(api_key, bucket, company_number)` automatically paginate via
`start_index` and return a single merged response dict. **Do not** add manual
pagination at call sites.

### Thread-safe UI rule
Tkinter is single-threaded. From any background thread:
```python
self.safe_ui_call(self.status_label.config, text="Done")  # correct
self.status_label.config(text="Done")                      # WRONG — crashes
```

### HTML report generation
`edd_visualizations.py` builds HTML strings. Always wrap external data with
`html.escape()` before embedding. matplotlib is used with the `Agg` backend —
**never call `plt.show()`** (it would block the event loop and the backend
doesn't support it).

---

## Key files to read for common tasks

| Task | Read first |
|------|-----------|
| Add a new investigation module | `modules/base.py`, `modules/grants_search.py` (simplest example) |
| Add a new CH API endpoint | `api/companies_house.py` |
| Change rate-limit behaviour | `utils/token_bucket.py`, `utils/settings.py` |
| Modify EDD report | `modules/enhanced_dd.py`, `utils/edd_visualizations.py`, `utils/edd_cross_analysis.py` |
| Change field definitions | `constants.py` (COMPANY_DATA_FIELDS, CHARITY_DATA_FIELDS, etc.) |
| Fuzzy matching logic | `utils/fuzzy_match.py` |
| iXBRL parsing | `utils/financial_analyzer.py` (`iXBRLParser`, `FinancialAnalyzer`) |
| Network graph building | `modules/network_analytics.py` (large — ~2000 lines) |

---

## Known issues / gotchas

- **`requirements.txt` is UTF-16 encoded** (characters display spaced out).
  Regenerate with `pip freeze > requirements.txt` if tooling misbehaves.
- **No test suite** exists. Test manually by running the app.
- `ipython`, `pytest`, `pefile`, `matplotlib-inline` are dev/build deps mixed
  into the main requirements.
- `openpyxl` is **not** in requirements — do not use `pd.ExcelWriter`.
- The `FinancialAnalyzer` class does not have `plot_trends`, `plot_ratios`, or
  `export_to_excel` methods (removed — they were dead code).
- The `iXBRLParser` class does not have a `debug_info` method (removed).
- CH filing/officer endpoints return max 100 items per page; the `ch_get_officers`
  and `ch_get_pscs` wrappers handle pagination transparently. Direct calls to
  `ch_get_data` with `/officers` paths will **not** paginate — always use the
  wrappers.

---

## Conventions

- Return convention for all API functions: `(data_or_None, error_str_or_None)`
- List fields serialised to CSV as semicolon-separated strings (`"; ".join(...)`)
- All base URLs in `constants.py`; all keyring service/account names there too
- Company numbers normalised to 8-digit zero-padded strings via
  `utils/helpers.clean_company_number()`
- Person names normalised via `utils/fuzzy_match.normalize_person_name()`
- Log via `utils/helpers.log_message(text)` — writes to `~/.multitool/app.log`
- Settings persisted to `~/.multitool/config.ini` via `utils/settings.py`
- API keys stored in system keyring (never written to disk/config)

---

## Running the app

```bash
python -m multitool.main          # from the parent directory of multitool/
```

Requires a Companies House API key (free at developer.company-information.service.gov.uk)
and optionally a Charity Commission API key. Configure via File → Manage API Keys.
