# Working Memory Index

Updated: 2026-03-12

Start here for active handoff notes. This index should stay short.

## Current Notes

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

## Usage

- Prefer one note per topic per day.
- Add the newest relevant note here when creating it.
- Leave older notes in place unless they are clearly obsolete.
