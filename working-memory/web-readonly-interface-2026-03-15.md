# Web Read-Only Interface

Date: 2026-03-15

Scope:
- Added a new top-level interface package: `src/LiuXin_alpha/interfaces/web_readonly`
- Kept this slice in `interfaces`, interface tests, and working-memory only.

Implementation:
- stdlib WSGI app in `interfaces/web_readonly/app.py`
- entrypoint in `interfaces/web_readonly/__main__.py`
- package export wired in `interfaces/__init__.py`
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
  - paginated with `exact_limit` and `exact_offset`
- `/search?global_q=<value>[&search_table=<table>]`
  - public-facing grouped search across high-value tables using substring matching over summary fields
  - paginated with `global_limit` and `global_offset`
  - ranked so exact and primary-field matches rise above looser substring hits
  - result cards now expose matched-column hints and highlighted snippets
- `/files/<file_id>/download`
  - serves a local file when the row can be resolved to one
  - otherwise redirects to an explicit HTTP(S) source/root when available
  - now also asks the database storage manager to retrieve bytes for store-backed files that are not directly addressable as local paths or simple redirects
  - unsupported backends now fail explicitly with `501 Not Implemented` instead of silently pretending the row is undownloadable
- `/files/<file_id>/preview`
  - safe inline previews for text, HTML, and image files
  - store-backed preview uses the same storage-manager retrieval path as downloads
  - unsupported backends or unsafe types fail explicitly instead of trying to inline arbitrary content

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
- table browse pagination now uses neutral `Back` / `Forward` labels instead of `Newer` / `Older`
- landing page now splits self-link tables out of helper tables instead of lumping all relationship-ish tables together
- `transform_runs` is now classified as a helper table rather than a link table
- `works` row pages now use a specialized detail layout instead of the generic field dump
  - title-focused hero block
  - grouped metadata cards (`Titles`, `Record`, `Dates`, `Other metadata`)
  - top-level pills summarizing linked counts for high-value related tables
  - dedicated `Credits` section for linked agent rows, ordered by interlink priority when available
  - dedicated `Formats` section derived from the shared read model’s discovered file payloads
- `files` row pages now use a specialized detail layout instead of the generic field dump
  - file-focused hero block with download action
  - grouped metadata cards (`Identity`, `Location and access`, `Classification`, `Dates`, `Other metadata`)
  - top-level pills for role/media/store and high-value linked counts
  - capability pills for downloadability, delivery mode, and safe preview type
- both search forms now expose page-size selectors directly in the UI
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
- `web_readonly` now consumes the shared neutral `self.read_model` backend for:
  - global search result payloads
  - specialized `works` detail payloads
  - specialized `files` detail payloads

Launch:
- `PYTHONPATH=src python3 -m LiuXin_alpha.interfaces.web_readonly --database /path/to/library.sqlite`

Validation:
- `pytest -q tests/interfaces/test_web_readonly.py`
  - `11 passed`
- `pytest -q tests/interfaces/test_windowed_ui.py tests/interfaces/test_text_browser.py -k 'jobs_ or help_command'`
  - `10 passed`

Notes:
- The first version is deliberately server-rendered and stdlib-only.
- This is a read-only browse/search/download surface, not the future read/write web UI.

TODO:
- Refine work credits once the allowed role/type vocabulary for agent/work interlinks is stabilized; current web layer renders ordered contributors but does not invent schema-invalid role values.
- Extend file downloads further once more store backends expose reliable direct byte access; current web path now works for storage-manager-backed stores such as local managed/unmanaged stores and single-file blob stores, but discovery-only HTML stores still only support redirect/fallback behavior.
