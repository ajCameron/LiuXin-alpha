# Web Read-Only Surface

Date: 2026-03-15

Scope:
- Added a new top-level surface package: `src/LiuXin_alpha/surfaces/web_readonly`
- Kept this slice in surface tests and working-memory only during the initial landing pass.

Implementation:
- stdlib WSGI app in `surfaces/web_readonly/app.py`
- entrypoint in `surfaces/web_readonly/__main__.py`
- package export wired in `surfaces/__init__.py`
- landing-page grouping now distinguishes:
  - `main`
  - `helper`
  - `interlink`
  - `intralink`

Routes:
- `/`
  - home page with table list and row counts, grouped into:
    - `main`
    - `helper`
    - `interlink`
    - `intralink`
- `/tables/<table>`
  - read-only browse view with offset/limit paging
- `/tables/<table>/<row_id>`
  - full row detail view
- `/search?table=<table>&column=<column>&q=<value>`
  - exact-column search using the database search primitive
- `/files/<file_id>/download`
  - serves a local file when the row can be resolved to one
  - otherwise redirects to an explicit HTTP(S) source/root when available
  - now also asks the database storage manager to retrieve bytes for store-backed files that are not directly addressable as local paths or simple redirects
  - unsupported backends now fail explicitly with `501 Not Implemented` instead of silently pretending the row is undownloadable

Public-safe defaults:
- no write routes
- no mutation commands
- database path hidden by default
- sensitive columns hidden by default:
  - `*credential*`
  - `*password*`
  - `*secret*`
  - `*token*`
  - `*policy_json*`
  - `*_scratch`

Display refinements:
- wide browse/search/detail tables now render inside horizontal scroll containers
- long cell/code values wrap instead of forcing the whole page off-screen
- mobile view reduces table font size and cell padding slightly
- landing page now splits self-link tables out of helper tables instead of lumping all relationship-ish tables together
- `transform_runs` is now classified as a helper table rather than a link table
- `works` row pages now use a specialized detail layout instead of the generic field dump
  - title-focused hero block
  - grouped metadata cards (`Titles`, `Record`, `Dates`, `Other metadata`)
  - top-level pills summarizing linked counts for high-value related tables
- `files` row pages now use a specialized detail layout instead of the generic field dump
  - file-focused hero block with download action
  - grouped metadata cards (`Identity`, `Location and access`, `Classification`, `Dates`, `Other metadata`)
  - top-level pills for role/media/store and high-value linked counts
- `stores` row pages now use a specialized detail layout instead of the generic field dump
  - store-focused hero block
  - grouped metadata cards (`Identity`, `Access`, `Capabilities`, `Dates`, `Other metadata`)
  - top-level pills for kind/protocol and high-value linked counts
  - existing hidden-column filtering still suppresses credentials and policy blobs
- row detail pages now render linked entities below the main row data
  - `labels`, `genres`, `subjects`, `languages`, `series`
    - pill-style linked navigation
  - `notes`, `comments`, `synopses`, `annotations`
    - excerpt/card style linked navigation
  - `agents`, `human_agents`, `org_agents`
    - compact card grid with role/type metadata
  - everything else
    - simple linked list fallback

Launch:
- `PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.web_readonly --database /path/to/library.sqlite`

Validation:
- `pytest -q tests/surfaces/test_web_readonly.py`
  - `7 passed`
- `pytest -q tests/surfaces/test_windowed_ui.py tests/surfaces/test_text_browser.py -k 'jobs_ or help_command'`
  - `10 passed`

Notes:
- The first version is deliberately server-rendered and stdlib-only.
- This is a read-only browse/search/download surface, not the future read/write web UI.

TODO:
- Add richer, domain-specific linked summaries for sections like `agents` on `works`, rather than only generic cards/counts now that the top-level pages are specialized.
- Extend file downloads further once more store backends expose reliable direct byte access; current web path now works for storage-manager-backed stores such as local managed/unmanaged stores and single-file blob stores, but discovery-only HTML stores still only support redirect/fallback behavior.
