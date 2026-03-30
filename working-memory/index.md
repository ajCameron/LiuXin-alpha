# Working Memory Index

Updated: 2026-03-19

Start here for active handoff notes. This index should stay short.

## Current Notes

- [web-readwrite-interface-2026-03-19.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/web-readwrite-interface-2026-03-19.md)
  Added a first local-first HTML mutation surface under `interfaces/web_readwrite`, reusing the read-only browse/search/detail stack while adding generic create/edit/delete pages for real tables.

- [benchmark-harness-2026-03-19.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/benchmark-harness-2026-03-19.md)
  Added the first alpha-native benchmark script suite for backend and WSGI hot paths, plus a combined JSON baseline artifact under `working-memory/test-results`.

- [semantic-test-db-series-2026-03-18.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/semantic-test-db-series-2026-03-18.md)
  Alpha-native semantic fixture families are now live for metadata, stores/assets, images/covers, custom columns, and identifiers, including `_db_1` expansions plus `pathological_relations_db_0` and `weird_data_db_0`, with imported-module discovery tightened so only supported builder-entrypoint modules participate by default.

- [standard-test-db-gap-analysis-2026-03-18.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/standard-test-db-gap-analysis-2026-03-18.md)
  Gap analysis for the standard `test_db_*` series: most names currently collapse to generic profiled fixtures, the first semantic-family expansion wave is now live, and the remaining standard-series question is whether a real `compat_projection_db` contract should exist at all.

- [benchmark-test-database-plan-2026-03-18.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/benchmark-test-database-plan-2026-03-18.md)
  Adds explicit alpha-native benchmark DBs as opt-in resources, wires them into the shared test-resource manager, and provides a standalone builder script for medium/large/custom benchmark corpora.

- [legacy-duplicate-cleanup-wave-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/legacy-duplicate-cleanup-wave-2026-03-16.md)
  First real alpha-side duplicate deletion wave: covered DB-property duplicates were removed from `src/LiuXin_tests/...`, and the legacy package now delegates those rows to the authoritative support copies while leaving only the rewrite family local.

- [db-property-custom-column-profile-cluster-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/db-property-custom-column-profile-cluster-2026-03-16.md)
  Confirms that `test_db_6`, `22`, `23`, `24`, and `25` are no longer a salvage backlog: live alpha fixtures now expose an explicitly empty custom-column profile, and current custom-column semantics are covered on active alpha seams, so that family moved to `covered`.

- [db-property-rich-content-cluster-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/db-property-rich-content-cluster-2026-03-16.md)
  Confirms that `test_db_4` and `10` are no longer honest salvage rows: live alpha fixtures only expose generated `titles` / `books` compatibility views and no rich synthetic-content maps, so that family moved to `rewrite`.

- [db-property-compatibility-projection-cluster-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/db-property-compatibility-projection-cluster-2026-03-16.md)
  Confirms that `test_db_1`, `14`, `15`, `16`, and `17` are no longer honest salvage rows: the live alpha fixtures only expose a narrow `titles` / `books` compatibility-view contract, not the old author-rich compatibility-builder semantics, so that family now belongs in `rewrite`.

- [db-property-identifier-cluster-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/db-property-identifier-cluster-2026-03-16.md)
  Confirms that `test_db_20` is no longer an honest salvage row: the live alpha fixture has empty identifier tables, empty identifier views, and no `identifier_title_links` table, so that seam now belongs in `rewrite`.

- [db-property-secondary-uuid-cluster-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/db-property-secondary-uuid-cluster-2026-03-16.md)
  Confirms that `test_db_18`, `19`, and `21` are no longer honest salvage rows: live alpha fixtures for those names do not materialize the old `secondary_uuid / content_level / loc_shelf` tables, so that family now belongs in `rewrite`.

- [db-property-remaining-clusters-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/db-property-remaining-clusters-2026-03-16.md)
  Records the final seven-row DB-property salvage split that is now fully resolved into `covered` and `rewrite`.

- [db-property-blank-optional-metadata-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/db-property-blank-optional-metadata-2026-03-16.md)
  Added a live alpha-native contract for the current blank optional-metadata support DB profiles; this was one of the supporting seams that helped close the DB-property salvage bucket.

- [db-property-simple-seam-review-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/db-property-simple-seam-review-2026-03-16.md)
  Review of the smallest remaining DB-property salvage rows: only `test_db_13_properties.py` could be honestly promoted to `covered`; the others still carry stale or unreplaced legacy semantics.

- [db-property-salvage-split-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/db-property-salvage-split-2026-03-16.md)
  The DB-property corpus is now fully resolved: all 26 support classes have live alpha subset coverage, the salvage bucket is empty, and the remaining legacy rows are split honestly between `covered` and explicit `rewrite` families.

- [db-property-alpha-subset-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/db-property-alpha-subset-2026-03-16.md)
  The legacy DB-property support corpus now has a collected alpha-native live schema/count contract across all 26 support classes.

- [db-property-support-registry-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/db-property-support-registry-2026-03-16.md)
  First promotion slice for the `salvage_existing` DB-property corpus: the 26 support classes now live behind a registry and a collected structural/resource-manager contract, while the stale old value snapshots remain to be normalized.

- [remaining-rewrite-deferral-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/remaining-rewrite-deferral-2026-03-16.md)
  Records the current decision to park the remaining rewrite rows instead of forcing fake migrations: `core_xmlrpc_compat` is deferred pending an explicit compat goal, `folder_store_runtime` is blocked until a real replacement seam exists, and the removed DB-property builder families stay in `rewrite`.

- [legacy-support-harness-closure-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/legacy-support-harness-closure-2026-03-16.md)
  Closed the old `legacy_support_harness` rewrite seam: five rows are now covered at active alpha helper/resource/macro tests, and two rows are retired as dead unittest/FSM scaffolding.

- [remaining-rewrite-seams-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/remaining-rewrite-seams-2026-03-16.md)
  Remaining legacy rewrite work is now down to six real seams: `core_xmlrpc_compat`, `folder_store_runtime`, and the removed `secondary_uuid`, identifier, compatibility-projection, and rich-content DB-property families.

- [relation-field-matrix-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/relation-field-matrix-2026-03-16.md)
  Active Batch B replacement for the legacy cache relation-field self-tests: one live pytest matrix now pins one-to-many, many-to-one, and many-to-many adapter behavior across default, typed, and priority variants.

- [custom-column-cache-semantics-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/custom-column-cache-semantics-2026-03-16.md)
  Second active Batch A slice for the cache/emulation rewrite: a small non-gated cache test now pins direct custom/composite category visibility rules and one-to-one custom-column update validation.

- [custom-column-field-matrix-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/custom-column-field-matrix-2026-03-16.md)
  First implemented slice of the cache/emulation rewrite Batch A: the old datatype-specific custom-column field tests are now replaced by a compact live Calibre-emulation value matrix.

- [cache-emulation-rewrite-checklist-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/cache-emulation-rewrite-checklist-2026-03-16.md)
  Narrows the old `database_caches` / `databases_legacy` rewrite bucket into concrete batches, and reclassifies the obvious `covered` and `retire` rows before more test-port work starts.

- [folder-store-builder-prune-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/folder-store-builder-prune-2026-03-16.md)
  Pruned dead `FolderStore` / `FolderStoreManager` asset-generation branches from the legacy `test_db_4` and `test_db_11` support builders, and pinned the replacement contract at the resource-manager layer.

- [folder-store-path-rewrite-slice-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/folder-store-path-rewrite-slice-2026-03-16.md)
  Picks the next concrete rewrite slice after the first harness replacements: keep `folder_store_path` as a narrow builder/schema seam, separate it from DB-property salvage, and do that before jumping into the larger cache/emulation cluster.

- [test-harness-rewrite-checklist-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/test-harness-rewrite-checklist-2026-03-16.md)
  Splits the six legacy `test_harness` files into concrete outcomes and now records the first three direct replacements that have landed: collected tree-generator tests, a `TestObjectsHandler` smoke test, and a focused `replace_in_folder_store_path(...)` macro test.

- [folder-stores-cleanup-boundary-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/folder-stores-cleanup-boundary-2026-03-16.md)
  Records that there are no standalone legacy `folder_stores` test modules left in alpha to delete directly; only duplicate support artifacts remain, and they should be removed only during the broader duplicate-tree cleanup.

- [cover-cache-triage-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/cover-cache-triage-2026-03-16.md)
  Legacy cover-cache utility tests should not be ported yet: the old `LiuXin_alpha.folder_stores` implementation does not exist in this checkout, so any focused tests need to wait for a real replacement seam.

- [folder-stores-rewrite-checklist-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/folder-stores-rewrite-checklist-2026-03-16.md)
  Concrete replacement map for legacy `folder_stores` tests: identifies what is already covered in alpha, what should be retired, and the first real replacement slice to add next (`on_disk_existing_managed` write-contract tests).

- [legacy-test-source-of-truth-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/legacy-test-source-of-truth-2026-03-16.md)
  Records the decision that alpha is the only living home for tests we still care about: port or rewrite them into alpha, then delete the duplicate legacy copies from alpha once they are no longer needed.

- [folder-stores-rewrite-plan-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/folder-stores-rewrite-plan-2026-03-16.md)
  Splits legacy `folder_stores` out into its own rewrite stream, grouped by backend-contract, reconcile, and cover/cache behavior, so DB-property salvage does not get blocked on storage redesign.

- [legacy-test-divergent-files-review-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/legacy-test-divergent-files-review-2026-03-16.md)
  Reviewed the remaining `13` divergent DB-support file pairs: one is now identical again, most remaining drift is intentional alpha normalization, and three files preserve a real `SQLite_apsw` to `SQLite` adaptation that should be kept.

- [folder-stores-rewrite-boundary-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/folder-stores-rewrite-boundary-2026-03-16.md)
  Explicitly marks legacy `folder_stores` as a hard rewrite seam: after the utility shims, any remaining blockers there should be rewritten against current storage backends/API/reconcile flows, not shimmed.

- [liuxin-tqdm-shim-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/liuxin-tqdm-shim-2026-03-16.md)
  Added a thin `utils/libraries/liuxin_tqdm.py` wrapper so legacy DB builders can use `tqdm.tqdm(...)` semantics without requiring the real `tqdm` package.

- [liuxin-clint-shim-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/liuxin-clint-shim-2026-03-16.md)
  Added a thin `utils/libraries/liuxin_clint.py` wrapper so `puts`/`colored` fall back cleanly when `clint` is absent, and repointed the legacy DB-support imports at it.

- [legacy-test-salvage-import-map-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/legacy-test-salvage-import-map-2026-03-16.md)
  Import-rewrite queue for the legacy DB-support salvage batch is now complete: `tests/support/test_databases` no longer imports `LiuXin_tests`, package-root import works, and the next real blocker is the separate `folder_stores` rewrite boundary.

- [legacy-test-salvage-checklist-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/legacy-test-salvage-checklist-2026-03-16.md)
  Records the completed normalization pass that closed the `salvage_existing` bucket: `tests/support/test_databases` is authoritative, import decoupling is done, and remaining legacy work is explicit rewrite or cleanup.

- [legacy-test-migration-plan-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/legacy-test-migration-plan-2026-03-16.md)
  Inventoried the original LiuXin test suite into a first-pass migration manifest, with initial `covered` / `salvage_existing` / `rewrite` / `vendor_frozen` / `integration_frozen` / `retire` guesses for every original test file.

- [image-backend-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/image-backend-2026-03-16.md)
  Extracted a neutral `interfaces/images` backend so cover/image discovery, target resolution, and placeholder generation are shared directly across hosts.

- [read-model-backend-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/read-model-backend-2026-03-16.md)
  Extracted a neutral `interfaces/read_model` backend and wired the main read-only hosts to compose it.

- [api-readonly-interface-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/api-readonly-interface-2026-03-16.md)
  Added a new top-level `interfaces/api_readonly` package as the first standalone machine-facing JSON API over the shared read-only interface infrastructure.

- [catalog-backend-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/catalog-backend-2026-03-16.md)
  Extracted a shared `interfaces/catalog` backend so Calibre-shaped category/work/file/image discovery and payload shaping are no longer owned by `web_calibre_readonly`.

- [acquisition-shared-api-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/acquisition-shared-api-2026-03-16.md)
  Extracted the Calibre-compatible `/get/...` and `/legacy/get/...` acquisition path into a neutral `interfaces/acquisition` package with an explicit host API.

- [opds-shared-api-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/opds-shared-api-2026-03-16.md)
  Extracted the OPDS implementation into a neutral `interfaces/opds` package with an explicit host API so multiple interface modules can reuse the same route/feed logic.

- [opds-readonly-interface-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/opds-readonly-interface-2026-03-16.md)
  Added a new top-level `interfaces/opds_readonly` package as a narrow standalone OPDS/acquisition surface that reuses the Calibre-compatible OPDS implementation without carrying the HTML browse UI.

- [web-calibre-readonly-interface-2026-03-15.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/web-calibre-readonly-interface-2026-03-15.md)
  Added a second top-level web surface, `interfaces/web_calibre_readonly`, which reuses the existing read-only backend but presents a Calibre mobile/content-server shaped home page, browse pages, and book pages.

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
