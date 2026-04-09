# Working Memory Index

Updated: 2026-03-19

Start here for active handoff notes. This index should stay short.

## Current Notes

- [torrent-discovery-2026-03-15.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/torrent-discovery-2026-03-15.md)
  Standalone `.torrent` inventory now exists, producing torrent metadata, ebook-shaped file lists, stem-based logical-book groups, and alternate directory-based groups, with a later TODO for a torrent-backed store and on-demand client-driven downloads.

- [fadedpage-wget-discovery-script-2026-03-15.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/fadedpage-wget-discovery-script-2026-03-15.md)
  Added a standalone stdlib-plus-`wget` Faded Page discovery script that streams resumable state into SQLite, refreshes a JSON export of ebook-shaped objects, emits explicit likely-book group records, and now includes rejection-reason counts in JSON.

- [web-readonly-interface-2026-03-15.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/web-readonly-interface-2026-03-15.md)
  Added a new top-level `interfaces/web_readonly` package: stdlib WSGI browse/search/detail pages plus conservative file download handling for public-facing read-only use.

- [interface-job-view-seam-2026-03-15.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/interface-job-view-seam-2026-03-15.md)
  Added a shared terminal job snapshot/log seam plus `jobs tail`, so the interface now has one job-output model for both textual commands and the windowed job pane.

- [windowed-job-pane-scrollback-2026-03-15.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/windowed-job-pane-scrollback-2026-03-15.md)
  Windowed terminal job output pane now has scrollback/focus parity with the console pane, including `F6` focus switching and status-board hints.

- [core-api-surface-2026-03-15.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/core-api-surface-2026-03-15.md)
  First explicit core API slice: descriptor models, `api.describe`, HTTP/proxy introspection, and a note that the next step is replacing generic `invoke` with named RPCs.

- [full-suite-green-2026-03-15.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/full-suite-green-2026-03-15.md)
  Full suite is green again in the main repo, with the current pass/fail counts, latest passing report path, and a note that `LiuXin_alpha_data` has separate local changes not captured by the main repo commit.

- [test-env-rerun-2026-03-13.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/test-env-rerun-2026-03-13.md)
  Cleaned up the missing test dependency surface, added a minimal `past.builtins` shim, and reran the full suite for a cleaner post-env failure signal.

- [crawler-default-preference-2026-03-13.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/crawler-default-preference-2026-03-13.md)
  Remote HTML crawl rate defaults now use the shared `crawler_http_max_requests_per_hour_default` preference, with old backend-specific keys kept as fallback-only compatibility reads.

- [ingest-consolidation-2026-03-13.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/ingest-consolidation-2026-03-13.md)
  Remote HTML ingest now uses neutral `--crawler-*` terminal flags and a dedicated `RemoteHtmlRegistrationReport` under `ingest`.

- [ingest-store-bootstrap-2026-03-13.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/ingest-store-bootstrap-2026-03-13.md)
  HTML store bootstrap helpers now live in `ingest/remote_html`, and `storage/reconcile/store_db_sync.py` no longer owns the HTML backend setup path.

- [ingest-public-api-2026-03-12.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/ingest-public-api-2026-03-12.md)
  Public remote-HTML registration now lives under top-level `ingest`, with eager package imports removed from `ingest` and `storage` to avoid import cycles.

- [discovery-sources-refactor-2026-03-12.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/discovery-sources-refactor-2026-03-12.md)
  Remote HTML crawling now lives under top-level `ingest/sources`, with the shared remote-HTML DB ingest loop moved into `ingest/pipelines` and the HTML store backends reduced to wrapper facades.

- [native-html-readonly-2026-03-12.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/native-html-readonly-2026-03-12.md)
  Added a lightweight `native_html_readonly` backend and wired it through storage bootstrap, reconcile, library, `sync store`, and `new store`.

- [wget-crawl-telemetry-2026-03-12.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/wget-crawl-telemetry-2026-03-12.md)
  `wget_html_readonly` sync now reports crawler-observation counters such as HTML seen, book-like URLs found, HTML rejected, and rejection reasons.

- [telemetry-panel-2026-03-12.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/telemetry-panel-2026-03-12.md)
  Optional DB-write telemetry is now available in the terminal, including a windowed auxiliary panel and `telemetry panel` command for tracking dirty-record and trigger activity.

- [terminal-formatting-2026-03-12.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/terminal-formatting-2026-03-12.md)
  Terminal detail/report formatting has been standardized onto shared section/table renderers, including `row`, `store show`, `jobs show`, `sync`, `ingest`, `summary`, and the main `new_*` wizard summaries.

- [terminal-mutations-2026-03-11.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/terminal-mutations-2026-03-11.md)
  Terminal `set` / `edit` / `delete` work, including core-routed row updates and delete impact previews.

- [interface-findings-2026-03-11.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/interface-findings-2026-03-11.md)
  Terminal/interface review focused on windowed UI behavior, job/proxy issues, and the RPC transition boundary.

- [optimization-pass-driver-wrapper-opds-2026-03-19.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/optimization-pass-driver-wrapper-opds-2026-03-19.md)
  First targeted performance pass added wrapper-level derived-schema caches,
  narrowed the OPDS metadata path, fixed benchmark setup overhead, and dropped
  the measured hot paths from multi-second to sub-second / low-second ranges.

## Usage

- Prefer one note per topic per day.
- Add the newest relevant note here when creating it.
- Leave older notes in place unless they are clearly obsolete.
