# Web Calibre-Style Read-Only Surface

Date: 2026-03-15

Scope:
- Added a second top-level web surface package: `src/LiuXin_alpha/surfaces/web_calibre_readonly`
- Kept this slice in surface tests and working-memory only during the initial landing pass.
- Reused the existing `web_readonly` backend/query/download surface instead of forking it.

Reference:
- Extracted Calibre source archive to `/home/blackjane/calibre-master` from `/home/blackjane/calibre-master.zip`
- Used the content-server mobile/reset resources and route structure there as the visual/IA reference, not as a runtime dependency.

Implementation:
- app: `surfaces/web_calibre_readonly/app.py`
- entrypoint: `surfaces/web_calibre_readonly/__main__.py`
- package export wired in `surfaces/__init__.py`
- launchers:
  - `scripts/run_web_calibre_readonly.sh`
  - `scripts/run_web_calibre_readonly.py`

Routes:
- `/`
- `/robots.txt`
- `/ajax-setup`
- `/static/<what>`
  - currently supports:
    - `mobile.css`
    - `reset.css`
    - `empty.html`
    - `calibre.png`
- `/favicon.png`
- `/apple-touch-icon.png`
- `/icon/<which>`
- `/mobile`
  - Calibre-shaped home page with category buttons and recent titles
  - now also accepts Calibre-style mobile query params:
    - `search`
    - `num`
    - `start`
    - `sort`
    - `order`
- `/browse/titles`
- `/browse/authors`
- `/browse/tags`
- `/browse/series`
- `/browse/recent`
  - category listing pages using a mobile/content-server style listing table
- `/browse/book/<work_id>`
  - compatibility redirect to `/book/<work_id>`
- `/book/<work_id>`
  - book/detail page for `works`
- `/author/<table>/<row_id>`
  - linked-titles page for agent-like rows
- `/series/<row_id>`
  - linked-titles page for series rows
- `/tag/<row_id>`
  - linked-titles page for label rows
- `/get/<what>/<work_id>/<library_id?>`
  - partial Calibre-style content alias:
    - `thumb`
    - `cover`
    - format names such as `epub`
- `/legacy/get/<what>/<work_id>/<library_id>/<filename>`
  - partial legacy Calibre-compatible download alias for book formats
- `/stanza`
  - now redirected to `/opds`
- `/opds`
  - minimal Atom/OPDS root feed
- `/opds/search/{query?}`
  - minimal OPDS search feed over `works`
- `/opds/navcatalog/<which>`
  - minimal OPDS browse feeds for:
    - `titles`
    - `recent`
    - `authors`
    - `tags`
    - `series`
- `/opds/category/<category>/<item_id>`
  - minimal OPDS acquisition feed for linked works under author/tag/series categories
- `/ajax/library-info`
- `/ajax/categories/<library_id?>`
- `/ajax/category/<encoded_name>/<library_id?>`
- `/ajax/books_in/<encoded_category>/<encoded_item>/<library_id?>`
- `/ajax/books/<library_id?>`
- `/ajax/book/<book_id>/<library_id?>`
- `/ajax/search/<library_id?>`
- `/interface-data/init`
- `/interface-data/books-init`
- `/interface-data/get-books`
- `/interface-data/book-metadata/<book_id>`
- `/interface-data/tag-browser`
- `/interface-data/update/<translations_hash?>`
- inherited from `web_readonly`:
  - `/search`
  - `/tables/<table>`
  - `/tables/<table>/<row_id>`
  - `/files/<file_id>/download`
  - `/files/<file_id>/preview`

Behavior:
- `CalibreReadOnlyWebApplication` now defaults correctly to `CalibreReadOnlyWebConfig` instead of inheriting the base `ReadOnlyWebConfig` defaults.
- overrides row links so public browse/search now points to:
  - works -> `/book/...`
  - agents -> `/author/<table>/...`
  - series -> `/series/...`
  - labels -> `/tag/...`
- book pages now resolve downloadable files through the real WEMI/storage path:
  - `works -> expressions -> manifestations -> items -> files`
  - plus direct related files if present
- cover/thumb routes now resolve images through the parallel image path:
  - direct `works -> images`
  - plus `works -> expressions -> manifestations -> items -> images`
  - local/store-backed image retrieval when possible
  - SVG placeholder fallback when no cover image exists
- `/ajax/*` and `/interface-data/*` now expose a small compatibility JSON surface:
  - library info
  - categories
  - category members
  - linked book ids
  - book metadata
  - book lists
- `/opds*` now exposes a small compatibility Atom feed surface instead of redirecting away from OPDS entirely.
- payloads are now materially closer to Calibre conventions:
  - `/ajax/categories` returns a list, including non-category entries like `All books` and `Newest`
  - `/ajax/category/...` returns category metadata plus paginated `items`
  - `/ajax/books_in/...` returns Calibre-shaped search/result metadata plus `book_ids`
  - `/ajax/books` returns an id-keyed metadata mapping
  - `/interface-data/init`, `/books-init`, and `/get-books` now return:
    - `search_result`
    - `metadata`
    - library/session-ish metadata closer to Calibre’s frontend expectations
  - book metadata now includes more Calibre-like keys:
    - `id`
    - `title`
    - `authors`
    - `author_sort`
    - `series`
    - `tags`
    - `comments`
    - `formats`
    - `format_metadata`
    - `cover`
    - `thumbnail`
    - `uuid`
- category payloads now use encoded compatibility tokens rather than only plain names:
  - category URLs now use encoded names
  - item URLs now use encoded item ids
  - the server still accepts plain tokens for compatibility, but emits encoded ones by default
- category/icon polish:
  - `/ajax/categories` now emits Calibre-style icon paths for:
    - `All books`
    - `Newest`
    - `Authors`
    - `Tags`
    - `Series`
  - category payloads now carry:
    - `encoded_name`
    - canonical display names
    - icon URLs
  - `allbooks` and `newest` category endpoints now behave more like Calibre by resolving into book-result payloads instead of empty category lists
- book metadata `category_urls` now point at encoded `/ajax/books_in/...` endpoints instead of only direct HTML detail pages
- `/interface-data/tag-browser` now returns a Calibre-style tree payload:
  - `root`
  - `item_map`
  - top-level categories currently include:
    - `Authors`
    - `Tags`
    - `Series`
  - child items include encoded `/ajax/books_in/...` URLs and direct HTML item URLs
  - this replaces the previous flat category-list placeholder
- OPDS feed navigation semantics are now materially closer to Calibre:
  - feed-level `self` links
  - `start` links
  - `up` links on nav/category/search feeds
  - `first`, `last`, `next`, and `previous` links when paging applies
  - offset-based paging on:
    - `/opds/navcatalog/...`
    - `/opds/category/...`
    - `/opds/search/...`
  - category and book feeds now page according to the interface page-size config instead of dumping a fixed first slice only
- OPDS token semantics are now closer as well:
  - root OPDS nav links now emit encoded `O...` / `N...` tokens instead of only plain names
  - category acquisition feeds now emit encoded `I<id>:<category>` item tokens
  - the server still accepts the older plain forms as fallbacks, but the generated OPDS surface now prefers the Calibre-shaped token style
- downloads and previews still reuse the safe `web_readonly` file-serving behavior

Display:
- Calibre mobile/content-server inspired shell and listing layout
- reset/mobile CSS adapted from the Calibre content-server resources
- category-button home page
- listing-table browse pages with cover thumbnails when available and one-line action buttons
- book pages with:
  - title/byline hero
  - cover image pulled from `/get/cover/...`
  - series/tag pills
  - available-format table with download/preview buttons
  - compact record metadata
  - contributor credits and remaining linked entities below

Launch:
- `PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.web_calibre_readonly --database /path/to/library.sqlite`
- `./scripts/run_web_calibre_readonly.sh --database /path/to/library.sqlite --port 8081`
- `python3 scripts/run_web_calibre_readonly.py --database /path/to/library.sqlite --port 8081`

Validation:
- `pytest -q tests/surfaces/test_web_calibre_readonly.py`
  - `10 passed`
- `pytest -q tests/surfaces/test_web_readonly.py tests/surfaces/test_web_calibre_readonly.py`
  - `21 passed`
- `python -m LiuXin_alpha.surfaces.web_calibre_readonly --help`
  - passed
- `./scripts/run_web_calibre_readonly.sh --help`
  - passed
- `python3 scripts/run_web_calibre_readonly.py --help`
  - passed

Notes:
- The goal of this slice was to get the browsing model and visual feel close while keeping the backend read-only and shared.
- Current compatibility is still intentionally partial, but the route surface is now materially closer:
  - static/icon routes exist
  - mobile and legacy content routes exist
  - OPDS exists
  - AJAX/interface-data routes exist
- Payload compatibility is now much closer structurally, but still approximate rather than byte-for-byte or field-for-field Calibre parity.

TODO:
- add more category pages (`formats`, `languages`, maybe `publishers` depending on actual table surface)
- tighten payload compatibility further for edge fields Calibre exposes but LiuXin does not yet model directly
- expand the tag-browser tree beyond `Authors` / `Tags` / `Series` as more category surfaces become worth mirroring
- add more Calibre route aliases only where they map cleanly onto the current read-only data model

Update:
- OPDS category browsing now supports Calibre-style grouped category feeds via `/opds/categorygroup/<category>/<group>`.
- Large OPDS category feeds now collapse to first-letter group entries once they exceed `opds_max_ungrouped_items`.
- Group feeds use encoded plain category tokens like Calibre for `/opds/categorygroup/...`, while still accepting the existing encoded nav-token forms on input.
- Added CLI/config support for `opds_max_ungrouped_items`.
