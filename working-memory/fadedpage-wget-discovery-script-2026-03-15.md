# Faded Page Wget Discovery Script

Date: 2026-03-15

Scope:
- Added a new standalone script:
  - `scripts/fadedpage_wget_discovery.py`
- Added targeted tests:
  - `tests/utils/test_fadedpage_wget_discovery_script.py`
  - `tests/utils/test_fadedpage_wget_discovery_real_export_fixture.py`
- Added a frozen real export fixture:
  - `tests/fixtures/fadedpage_wget_discovery/real_export_snapshot.json`

Intent:
- provide a minimal-dependency crawler that can be copied to another machine
- use the old `wget --spider --recursive` discovery style
- keep resumable state in SQLite
- continuously refresh a JSON export while crawling
- emit chatty runtime logs so long unattended runs are easier to trust

Dependencies:
- Python 3 stdlib
- `wget`

Behavior:
- defaults to `https://www.fadedpage.com/`
- uses a resumable SQLite DB:
  - observations table for all normalized URLs seen in wget output
  - candidates table for accepted ebook-shaped objects
- exports JSON with:
  - profile/root metadata
  - aggregate stats
  - full absolute candidate URLs plus derived fields
  - grouped book records keyed by host + logical book stem
  - rejection/acceptance reason counts from the observed URL stream

Candidate shaping:
- accepts Faded Page-style `link.php?file=...` ebook links
- accepts direct non-HTML ebook file paths like `.epub`, `.pdf`, `.mobi`, `.zip`, `.txt`
- does **not** treat generic path-based `.html` pages like `index.html` as ebook objects
  - HTML candidates must come from a query-file style URL
- explicitly excludes obvious site-control text files like `robots.txt`

Output object fields:
- `url`
- `host`
- `path`
- `filename`
- `stem`
- `extension`
- `object_kind`
- `source_kind`
- `query_filename`
- `discovered_at`

Grouped export fields:
- `groups`
  - one entry per `host + logical book stem`
  - known Faded Page suffix families like `-a5`, `-h`, and `-k` are collapsed into the same logical book
  - `variant_count`
  - `extensions`
  - `paths`
  - `primary_url`
  - `source_stems`
  - `variant_suffixes`
  - `variants`
- `books`
  - explicit likely-book records derived from the same grouping
  - `reader_pages`
  - `download_formats`
  - `likely_book`
  - `confidence`
  - `warnings`
  - `suspicious`

Stats fields now also include:
- `accepted_count`
- `rejected_count`
- `reason_counts`
- `rejection_reason_counts`

Terminal report mode:
- `--report text`
- `--report-limit N`
- intended for quick human inspection after or during a crawl
- currently includes:
  - crawl summary
  - rejection reasons
  - top format-coverage profiles
  - suspicious/incomplete books
  - likely-book sample list

Operational notes:
- crash tolerance comes from SQLite commits during streaming discovery
- on rerun, the script restarts the wget crawl but deduplicates candidates already found
- JSON export is refreshed during the run via:
  - `--export-every`
  - `--export-interval-s`
- runtime logging is intentionally chatty now:
  - startup config line
  - effective wget command line
  - raw wget output lines echoed with a `[wget]` prefix by default
  - a live TTY-only status footer, updated in place, with:
    - elapsed time
    - observed URL counts
    - accepted candidate counts
    - current per-minute observation rate
    - last observed URL
  - one line per newly accepted candidate
  - periodic progress summaries with reason counts
  - export refresh lines
  - explicit error summary before re-raising on wget failure
- quiet controls:
  - `--quiet-wget` adds `--no-verbose`
  - `--no-raw-wget-lines` suppresses echoed wget lines while keeping higher-level script logs
  - `--live-progress` forces the live footer on
  - `--no-live-progress` disables the live footer

Real-data refinement:
- the live SQLite crawl state showed that raw stem grouping was too literal
  - one logical book was often split across:
    - `<stem>`
    - `<stem>-a5`
    - `<stem>-h`
    - sometimes `<stem>-k`
- grouping now collapses those into one logical book
- stale candidates from older runs are revalidated during export
  - filtered rows are counted as `filtered_after_classification`
- one real Faded Page export snapshot is now frozen under `tests/fixtures`
  - higher-level regression tests assert:
    - exact snapshot stats
    - format coverage distribution
    - known suffix-family collapse for `-a5`, `-h`, `-k`
    - report cleanliness and usefulness on real data

Validation:
- `pytest -q tests/utils/test_fadedpage_wget_discovery_script.py`
  - `10 passed`
- `pytest -q tests/utils/test_fadedpage_wget_discovery_script.py tests/utils/test_fadedpage_wget_discovery_real_export_fixture.py`
  - `13 passed`
- `python -m py_compile scripts/fadedpage_wget_discovery.py tests/utils/test_fadedpage_wget_discovery_script.py`

Likely next steps:
- add richer page-level grouping heuristics if the remaining logical-book stem model proves too loose on broader coverage
