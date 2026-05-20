# Working Memory Index

Updated: 2026-05-20

Start here for active handoff notes. This index should stay short.

## Current Notes

- [file-formats-pml-unicode-2026-05-20.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/file-formats-pml-unicode-2026-05-20.md)
  Current branch note for the PML pass after PR #69: shared OEB fixture
  coverage for `PMLMLizer`/`PMLOutput`, deterministic `.pmlz` output, and the
  explicit supported-versus-lossy PML unicode boundary.

- [file-formats-text-unicode-2026-05-20.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/file-formats-text-unicode-2026-05-20.md)
  Current branch note for the text/unicode file-format pass: reusable
  multiscript corpus, encoded payload cases, deterministic fuzz helpers,
  output encoding/newline matrix helpers, and the first TXT conversion/input
  tests built on the shared framework.

- [file-formats-cover-utils-2026-05-20.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/file-formats-cover-utils-2026-05-20.md)
  Current branch note for the lateral file-format pass: deterministic
  cover-extraction utility coverage, Python 3 byte-HTML parsing fixes,
  top-level/package utility alignment, and the environment-stable PML Pillow
  fallback test.

- [metadata-web-sources-hardening-2026-05-18.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-web-sources-hardening-2026-05-18.md)
  Current web-source hardening and expansion note: provider parser/backoff
  coverage, Google Images rendered-browser fallback, the new Library of
  Congress source slice, live-probe behavior, and next source candidates.
  Durable doc: `docs/development/metadata-web-sources.md`.

- [metadata-wemi-container-hardening-plan-2026-05-17.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-wemi-container-hardening-plan-2026-05-17.md)
  Current branch note for the planned WEMI container/conversion hardening pass:
  conversion contracts, identity-spine versus full-graph invariants,
  lazy/eager projection parity, writer safety, and relation-container torture
  coverage. Durable doc:
  `docs/development/metadata-wemi-container-hardening.md`.

- [metadata-utils-coverage-2026-05-17.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-utils-coverage-2026-05-17.md)
  Current branch note for the metadata helper coverage pass:
  `metadata.utils` / `metadata.ebook_metadata_tools` coverage, unicode
  torture, OPF/resource/language/timestamp fixes, and focused coverage at
  `93%` across the two helper modules.

- [metadata-standardize-coverage-2026-05-17.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-standardize-coverage-2026-05-17.md)
  Current branch note for the metadata standardization coverage pass:
  title separator/strip fixes, Python 3-safe tag cleanup, creator-name
  punctuation preservation, shared-module parity checks, and focused coverage
  at `97%` across `metadata.standardize` / `metadata.standardization`.

- [metadata-book-coverage-2026-05-17.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-book-coverage-2026-05-17.md)
  Current branch note for the metadata.book coverage pass: core
  `calibreMetadata`, JSON/dict serialization, unicode torture, renderer
  wrappers, and narrow Python 3 compatibility fixes now have focused coverage.

- [metadata-coverage-fallback-alignment-2026-05-17.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-coverage-fallback-alignment-2026-05-17.md)
  Current branch note for the post-coverage stale-test alignment: strict
  reader defaults stay strict, explicit `fallback_on_parse_error=True` owns
  conservative shell metadata, and the latest external coverage run is green.

- [metadata-text-reader-fuzzing-2026-05-17.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-text-reader-fuzzing-2026-05-17.md)
  Current branch note for the permissive text-reader malformed-input pass:
  TXT/HTML/plain-PML safe fallbacks, direct HTML byte payload handling,
  binary-signature guards, and text-like safety corpus coverage.

- [metadata-comic-reader-fuzzing-2026-05-17.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-comic-reader-fuzzing-2026-05-17.md)
  Current branch note for the post-PR #48 CBR/CBZ malformed-input pass:
  dedicated comic reader wrapper, strict unreadable/non-comic archive failures,
  and preserved shell metadata for valid image archives without comments.

- [metadata-container-reader-fuzzing-2026-05-17.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-container-reader-fuzzing-2026-05-17.md)
  Follow-up on PR #48 for the next metadata reader pass: ODT/ODT-beta named
  strict failures, a new LRF file-source wrapper, and shared malformed-input
  coverage for ODT/RAR/LRF.

- [metadata-legacy-format-fuzzing-2026-05-17.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-legacy-format-fuzzing-2026-05-17.md)
  Active branch note for the next malformed-input lane after PR #47:
  legacy/specialty metadata readers, starting with RTF/SNB/LRX strict wrapper
  checks while preserving valid-container fallbacks.

- [metadata-malformed-input-fuzzing-2026-05-16.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-malformed-input-fuzzing-2026-05-16.md)
  Active branch note for deterministic malformed-input and wrong-format
  metadata extractor coverage. The first slice adds
  `metadata.file_sources.registry` so tests and future plugin loading can
  enumerate/register readers without putting registry state on the base class.

- [metadata-writer-coverage-plan-2026-05-16.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-writer-coverage-plan-2026-05-16.md)
  Next high-value lane after extractor coverage: metadata writer tests as
  corruption-prevention checks, with round-trip readability, container
  validity, payload preservation, hostile unicode/escaping, and clean failure
  behavior. Durable doc: `docs/development/metadata-writer-coverage-contract.md`.

- [metadata-ingest-source-coverage-plan-2026-05-16.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-ingest-source-coverage-plan-2026-05-16.md)
  Coverage plan for production-heavy metadata file-source ingest readers, with
  a unicode/malformed-input testing strategy. The pass now covers the newer
  ingest readers plus archive/text/ODT, dispatcher/worker, PDB sub-readers, and
  older adapters; latest private-corpus validation is `457 passed`, `4 skipped`,
  with `metadata.file_sources` coverage at 91%.

- [data-artifacts-plan-2026-05-16.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/data-artifacts-plan-2026-05-16.md)
  Optional artifacts now use `scripts/build_artifacts.py`, track small fixtures
  directly, and keep multi-GB ISFDB DB payloads manifest-only with deterministic
  child build environment settings.

- [metadata-coverage-lift-2026-05-16.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-coverage-lift-2026-05-16.md)
  Focused coverage lift for the recent metadata API/container work, including
  the new WEMI projection/relation/lazy-value tests, 100% focused coverage
  across the six metadata hydrator modules, and the post-merge bzzdec malformed
  payload fix. Durable malformed-input testing docs:
  `docs/development/malformed-input-fuzzing.md`.

- [coverage-syntax-warning-cleanup-2026-05-15.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/coverage-syntax-warning-cleanup-2026-05-15.md)
  Coverage-runner and SyntaxWarning cleanup note: adds the dedicated coverage
  wrapper, redirects coverage data out of the repo root, records the 17-file
  warning cleanup, focused validation, and the remaining full coverage rerun.

- [type-checking-and-coverage-scope-2026-05-15.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/type-checking-and-coverage-scope-2026-05-15.md)
  Adds the static typing toolchain/config/runner scope and records the latest
  full coverage result: suite green, project-wide coverage dominated by
  legacy/vendor areas, and `surfaces.renderers.calibre_metadata` as the next
  targeted test gap.

- [metadata-api-todo-followups-2026-05-15.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-api-todo-followups-2026-05-15.md)
  Numbered list of the still-relevant TODOs in the metadata API/container slice
  after the relation-key/link/projection cleanup, so we can work down them in
  order.

- [metadata-projection-views-2026-05-15.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-projection-views-2026-05-15.md)
  Decision note for the next metadata API slice: add read-only embedded
  `values` and `text` projection views for relation-target values while keeping
  relation links as the authoritative graph/provenance surface.

- [tkinter-gui-architecture-2026-05-11.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/tkinter-gui-architecture-2026-05-11.md)
  Tkinter GUI spike/design handoff: current `tkinter-gui-foundation` branch/base,
  validation status, the new canonical dev doc paths, the intended split into
  backend/state/tasks/views, and a detailed core-backed implementation plan for
  read, write, job, cache, OPF, and storage slices.

- [metadata-terminal-write-report-parity-2026-05-10.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-terminal-write-report-parity-2026-05-10.md)
  Current branch note for terminal parity with metadata write reports: terminal
  metadata-specific `on` flows now use the WEMI metadata writer/report bridge,
  while generic links such as languages stay on the direct database path.

- [metadata-surface-write-report-integration-2026-05-10.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-surface-write-report-integration-2026-05-10.md)
  Web read-write metadata relation add/create routes now use the WEMI metadata
  writer/report path where supported, with direct database writes kept for
  generic links and CRUD.

- [metadata-surface-cache-read-path-2026-05-08.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-surface-cache-read-path-2026-05-08.md)
  The read model now accepts an explicit metadata read source/cache snapshot
  while keeping direct database reads as the default; this note captures the
  cache-source adapter additions, constructor wiring, read-only surface CLI/config
  wiring, route/direct-read audit, wrapper-script help/examples, validation, and
  remaining file/storage caveats.

- [metadata-interaction-surfaces-review-2026-05-08.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-interaction-surfaces-review-2026-05-08.md)
  Fresh review of the actual interaction surfaces under `surfaces`, covering
  the row/dict read model, cache gap, tags/labels transition, local host
  contracts, write-surface bypasses, and a staged plan to bridge web/API/OPDS
  and terminal flows onto the current metadata objects.

- [metadata-interface-review-plan-2026-05-08.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/metadata-interface-review-plan-2026-05-08.md)
  Fresh metadata interface review covering the public facade, API contracts,
  lazy/cache read paths, write-back reports, stale namespaces, and a staged plan
  for bringing the interface layer up to the current metadata implementation;
  test/source hygiene and public import-root cleanup are now implemented.

- [cache-performance-exploration-2026-04-21.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-cache-impl/working-memory/cache-performance-exploration-2026-04-21.md)
  Cache backend review concluded that the current `numpy_vectorized` layer is
  only a helper veneer over `schema_backed`, and that an optional
  `pyarrow_columnar` backend is the best fit for a genuinely faster immutable
  snapshot cache.

- [pr-surfaces-normalization-2026-04-16.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/pr-surfaces-normalization-2026-04-16.md)
  Draft PR title/body for the `interfaces` to `surfaces` normalization pass,
  including the moved-surface validation command and the two follow-up seams
  that remain outside this rename work.

- [surfaces-normalization-followups-2026-04-16.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/surfaces-normalization-followups-2026-04-16.md)
  `interfaces` has been normalized to `surfaces` across the live tree; the
  remaining code-level follow-up is that `file_formats` still expects a
  `surfaces.gui2` package that does not exist in this checkout.

- [test-alignment-source-questions-2026-04-16.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/test-alignment-source-questions-2026-04-16.md)
  Pre-normalization follow-up questions from the post-merge test tidy; the
  main `interfaces` to `surfaces` import/package issues recorded there have now
  largely been resolved by the current normalization pass.

- [mainline-storage-merge-2026-04-16.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/mainline-storage-merge-2026-04-16.md)
  `main` now includes the storage review merge plus the earlier test-port merge; this note captures the new storage/jobs/metadata architecture, the expanded test surface, and which older test-port assumptions are now stale.

- [torrent-discovery-2026-03-15.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/torrent-discovery-2026-03-15.md)
  Standalone `.torrent` inventory now exists, producing torrent metadata, ebook-shaped file lists, stem-based logical-book groups, and alternate directory-based groups, with a later TODO for a torrent-backed store and on-demand client-driven downloads.

- [fadedpage-wget-discovery-script-2026-03-15.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/fadedpage-wget-discovery-script-2026-03-15.md)
  Added a standalone stdlib-plus-`wget` Faded Page discovery script that streams resumable state into SQLite, refreshes a JSON export of ebook-shaped objects, emits explicit likely-book group records, and now includes rejection-reason counts in JSON.

- [web-readonly-surface-2026-03-15.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/web-readonly-surface-2026-03-15.md)
  Added a new top-level `surfaces/web_readonly` package: stdlib WSGI browse/search/detail pages plus conservative file download handling for public-facing read-only use.

- [surface-job-view-seam-2026-03-15.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/surface-job-view-seam-2026-03-15.md)
  Added a shared terminal job snapshot/log seam plus `jobs tail`, so the surface now has one job-output model for both textual commands and the windowed job pane.

- [windowed-job-pane-scrollback-2026-03-15.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/windowed-job-pane-scrollback-2026-03-15.md)
  Windowed terminal job output pane now has scrollback/focus parity with the console pane, including `F6` focus switching and status-board hints.

- [core-api-surface-2026-03-15.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/core-api-surface-2026-03-15.md)
  First explicit core API slice: descriptor models, `api.describe`, HTTP/proxy introspection, and a note that the next step is replacing generic `invoke` with named RPCs.

- [full-suite-green-2026-03-15.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/full-suite-green-2026-03-15.md)
  Full suite is green again in the main repo, with the current pass/fail counts, latest passing report path, and a note that `LiuXin_alpha_data` has separate local changes not captured by the main repo commit.

- [test-env-rerun-2026-03-13.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/test-env-rerun-2026-03-13.md)
  Cleaned up the missing test dependency surface, added a minimal `past.builtins` shim, and reran the full suite for a cleaner post-env failure signal.

- [crawler-default-preference-2026-03-13.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/crawler-default-preference-2026-03-13.md)
  Remote HTML crawl rate defaults now use the shared `crawler_http_max_requests_per_hour_default` preference, with old backend-specific keys kept as fallback-only compatibility reads.

- [ingest-consolidation-2026-03-13.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/ingest-consolidation-2026-03-13.md)
  Remote HTML ingest now uses neutral `--crawler-*` terminal flags and a dedicated `RemoteHtmlRegistrationReport` under `ingest`.

- [ingest-store-bootstrap-2026-03-13.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/ingest-store-bootstrap-2026-03-13.md)
  HTML store bootstrap helpers now live in `ingest/remote_html`, and `storage/reconcile/store_db_sync.py` no longer owns the HTML backend setup path.

- [ingest-public-api-2026-03-12.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/ingest-public-api-2026-03-12.md)
  Public remote-HTML registration now lives under top-level `ingest`, with eager package imports removed from `ingest` and `storage` to avoid import cycles.

- [discovery-sources-refactor-2026-03-12.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/discovery-sources-refactor-2026-03-12.md)
  Remote HTML crawling now lives under top-level `ingest/sources`, with the shared remote-HTML DB ingest loop moved into `ingest/pipelines` and the HTML store backends reduced to wrapper facades.

- [native-html-readonly-2026-03-12.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/native-html-readonly-2026-03-12.md)
  Added a lightweight `native_html_readonly` backend and wired it through storage bootstrap, reconcile, library, `sync store`, and `new store`.

- [wget-crawl-telemetry-2026-03-12.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/wget-crawl-telemetry-2026-03-12.md)
  `wget_html_readonly` sync now reports crawler-observation counters such as HTML seen, book-like URLs found, HTML rejected, and rejection reasons.

- [telemetry-panel-2026-03-12.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/telemetry-panel-2026-03-12.md)
  Optional DB-write telemetry is now available in the terminal, including a windowed auxiliary panel and `telemetry panel` command for tracking dirty-record and trigger activity.

- [terminal-formatting-2026-03-12.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/terminal-formatting-2026-03-12.md)
  Terminal detail/report formatting has been standardized onto shared section/table renderers, including `row`, `store show`, `jobs show`, `sync`, `ingest`, `summary`, and the main `new_*` wizard summaries.

- [terminal-mutations-2026-03-11.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/terminal-mutations-2026-03-11.md)
  Terminal `set` / `edit` / `delete` work, including core-routed row updates and delete impact previews.

- [surface-findings-2026-03-11.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/surface-findings-2026-03-11.md)
  Terminal surface review focused on windowed UI behavior, job/proxy issues, and the RPC transition boundary.

- [optimization-pass-driver-wrapper-opds-2026-03-19.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/optimization-pass-driver-wrapper-opds-2026-03-19.md)
  First targeted performance pass added wrapper-level derived-schema caches,
  narrowed the OPDS metadata path, fixed benchmark setup overhead, and dropped
  the measured hot paths from multi-second to sub-second / low-second ranges.

## Usage

- Prefer one note per topic per day.
- Add the newest relevant note here when creating it.
- Leave older notes in place unless they are clearly obsolete.
