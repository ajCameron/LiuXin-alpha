# Legacy Test Migration Manifest

Date: 2026-03-16

## Outputs

- CSV manifest: [legacy-test-migration-manifest-2026-03-16.csv](legacy-test-migration-manifest-2026-03-16.csv)
- salvage checklist: [legacy-test-salvage-checklist-2026-03-16.md](legacy-test-salvage-checklist-2026-03-16.md)
- import rewrite map: [legacy-test-salvage-import-map-2026-03-16.md](legacy-test-salvage-import-map-2026-03-16.md)
- import rewrite CSV: [legacy-test-salvage-import-rewrite-map-2026-03-16.csv](legacy-test-salvage-import-rewrite-map-2026-03-16.csv)
- divergent-file review: [legacy-test-divergent-files-review-2026-03-16.md](legacy-test-divergent-files-review-2026-03-16.md)
- `folder_stores` rewrite plan: [folder-stores-rewrite-plan-2026-03-16.md](folder-stores-rewrite-plan-2026-03-16.md)
- `folder_stores` rewrite checklist: [folder-stores-rewrite-checklist-2026-03-16.md](folder-stores-rewrite-checklist-2026-03-16.md)
- cover-cache triage: [cover-cache-triage-2026-03-16.md](cover-cache-triage-2026-03-16.md)
- `folder_stores` cleanup boundary: [folder-stores-cleanup-boundary-2026-03-16.md](folder-stores-cleanup-boundary-2026-03-16.md)
- test-harness rewrite checklist: [test-harness-rewrite-checklist-2026-03-16.md](test-harness-rewrite-checklist-2026-03-16.md)
- alpha source-of-truth note: [legacy-test-source-of-truth-2026-03-16.md](legacy-test-source-of-truth-2026-03-16.md)

## Scope

- Source suite inventoried from the original `LiuXin-master` tree:
  - `src/LiuXin_tests`
  - embedded project tests under `src/LiuXin`
  - vendored tests under `original_library_code`
- This is an initial-status audit, not a final migration decision log.

## Summary

- total inventoried files: `124`
- `covered`: `63`
- `integration_frozen`: `9`
- `retire`: `24`
- `rewrite`: `5`
- `salvage_existing`: `16`
- `vendor_frozen`: `7`

## Largest Subsystems

- `database_caches`: `33`
- `database_properties`: `26`
- `legacy_test_infra`: `15`
- `vendored_libraries`: `8`
- `file_formats`: `6`
- `folder_stores`: `6`
- `test_harness`: `6`
- `pydrive`: `6`
- `embedded_utils`: `4`
- `legacy_suite_misc`: `3`
- `databases_legacy`: `2`
- `embedded_devices`: `2`

## Key Findings

- The original suite is mostly legacy `unittest` plus ramdisk/scratch-fixture infrastructure, not a modern `pytest` tree.
- The highest-value migration target remains the database-property and cache corpus.
- Alpha already contains partial carry-over of the old fixture/support world:
  - `src/LiuXin_tests`
  - `tests/support/test_databases`
- The DB-property corpus started as one `salvage_existing` bucket, but it is now split more honestly: minimal rows can move to `covered`, while the large semantic rows remain `salvage_existing`.
- Externally pinned surfaces are now split into vendor dependencies versus product integrations.
- `folder_stores` is now explicitly treated as a hard rewrite boundary, not a compatibility/shim target.
- The legacy `folder_stores` manifest rows are now more specific:
  - generic driver smoke is effectively covered
  - the on-disk-flat write matrix now has a concrete alpha replacement test
  - marker/seek logic is explicitly retired
  - cover-cache utility semantics are deferred until there is a live implementation seam again
- There are no standalone legacy `folder_stores` test modules left in alpha to delete directly; only duplicate support artifacts remain, and those should be removed only during the broader duplicate-tree cleanup.
- The old `legacy_support_harness` cluster is now fully resolved:
  - surviving tree-generation, scratch-asset, and path-rewrite behaviors are covered in active alpha tests
  - only the genuinely dead unittest base/FSM property helpers moved to `retire`
- The two old unittest-loader/helper rows are now retired with confidence, not just by intuition:
  - their live helper seams have explicit replacements
  - no current alpha behavior depends on the old unittest loader path
- The next rewrite slice is now explicitly narrowed to the remaining `folder_store_path` builder seam before taking on the larger cache/emulation corpus:
  - [folder-store-path-rewrite-slice-2026-03-16.md](folder-store-path-rewrite-slice-2026-03-16.md)
- That builder seam has now moved forward:
  - the dead `folder_stores` asset branches in `test_db_4` and `test_db_11` are pruned
  - resource-manager regression coverage now pins the replacement contract
  - remaining `folder_store_path` work is in schema/property salvage, not builder shims
- The duplicate `folder_stores` fixture baggage under `src/LiuXin_tests` has now started to be deleted for real:
  - `test_resources_manager` points at `tests/support/test_databases/test_db_1`
  - the duplicate `src/LiuXin_tests/.../folder_stores.csv` files are gone
  - the old `build_test_fsms.py` entrypoint is gone
- The next manifest cluster after `folder_stores` is now narrowed too:
  - [cache-emulation-rewrite-checklist-2026-03-16.md](cache-emulation-rewrite-checklist-2026-03-16.md)
  - cache/emulation rows are no longer all generic `rewrite`
  - obvious `covered` and `retire` rows have been split out first
- Batch A now has two active replacement slices in the default suite:
  - [custom-column-field-matrix-2026-03-16.md](custom-column-field-matrix-2026-03-16.md)
  - [custom-column-cache-semantics-2026-03-16.md](custom-column-cache-semantics-2026-03-16.md)
- That means custom-column metadata/value/category/precheck behavior is no longer only pinned in the legacy-gated cache modules.
- Manifest effect from that work:
  - the `15` legacy custom-column cache field/table rows are now `covered`
  - DB-writing cache-update behavior remains a deferred future seam, not a false `covered` claim
- Batch B has now landed as an active relation-field matrix:
  - [relation-field-matrix-2026-03-16.md](relation-field-matrix-2026-03-16.md)
  - the `16` legacy relation-field cache rows are now `covered`
  - that leaves the cache/emulation manifest cluster fully split into `33 covered / 2 retire / 0 rewrite`
- Two more low-risk rewrite rows have now been collapsed:
  - `src/LiuXin_tests/customize/test__init.py` is covered by a direct alpha customize-base smoke test
  - `src/LiuXin/file_formats/epub/cfi/tests.py` is covered by the active EPUB CFI parser/sort-key tests
- The old `legacy_support_harness` seam is now closed:
  - [legacy-support-harness-closure-2026-03-16.md](legacy-support-harness-closure-2026-03-16.md)
- The remaining rewrite backlog is now explicitly narrowed to:
  - [remaining-rewrite-seams-2026-03-16.md](remaining-rewrite-seams-2026-03-16.md)
  - only `core_xmlrpc_compat` and `folder_store_runtime` remain
- Alpha is now the only intended living home for tests we still care about; once a legacy test is ported or rewritten into alpha, the duplicate legacy copy in alpha should be deleted rather than retained indefinitely.
- First promotion work on the `salvage_existing` DB-property corpus has landed:
  - [db-property-support-registry-2026-03-16.md](db-property-support-registry-2026-03-16.md)
  - the 26 support classes are now discoverable through one registry and have a collected structural/resource-manager contract
  - the rows remain `salvage_existing` because the old per-DB value snapshots still need selective normalization onto the current schema
- The first selective alpha-schema normalization slice inside that salvage bucket has now landed too:
  - [db-property-alpha-subset-2026-03-16.md](db-property-alpha-subset-2026-03-16.md)
  - all `26` support classes now declare a live alpha `database_version/works/series/expressions/manifestations/items/files/agents/labels` subset
  - a collected test verifies those declarations against provisioned DBs
  - the follow-on split is recorded in:
    - [db-property-salvage-split-2026-03-16.md](db-property-salvage-split-2026-03-16.md)
  - `10` minimal legacy property rows are now `covered`
  - `16` larger semantic rows remain `salvage_existing`
- The first follow-on seam review is now recorded in:
  - [db-property-simple-seam-review-2026-03-16.md](db-property-simple-seam-review-2026-03-16.md)
  - that pass only promoted `test_db_13_properties.py`
- A real replacement seam for the current blank optional-metadata fixture profile is now covered too:
  - [db-property-blank-optional-metadata-2026-03-16.md](db-property-blank-optional-metadata-2026-03-16.md)
  - this pins the current alpha shape of `test_db_1`, `4`, `6`, `10`, `14`, `15`, `16`, `20`, `21`, `22`, `23`, `24`, and `25`
  - it does not justify further row promotion by itself, because the original legacy rows still carry stale unreplaced maps
- The remaining `16` salvage rows are now split into real semantic clusters instead of one flat backlog:
  - [db-property-remaining-clusters-2026-03-16.md](db-property-remaining-clusters-2026-03-16.md)
  - next best salvage target is the `secondary_uuid / content_level / loc_shelf` family (`test_db_18`, `19`, `21`)

## Status Definitions

- `covered`: current alpha suite already appears to cover the behavior at a higher-quality seam
- `salvage_existing`: the file or fixture corpus already exists in alpha and should be normalized/wired in, not recopied
- `rewrite`: behavior still matters but the old test shape does not fit alpha architecture
- `vendor_frozen`: vendored or patched dependency test; track separately from the main migration stream
- `integration_frozen`: externally pinned product integration or hardware/service surface; not dead, but outside the main migration stream for now
- `retire`: internal surface is actually gone, or the file is only legacy test infrastructure rather than product behavior

## First Batches

1. Normalize `tests/support/test_databases` and `src/LiuXin_tests` in alpha.
2. Promote the old DB property corpus into collected database/driver contracts.
3. Rewrite the old calibre cache tests against `tests/databases/caches` and `tests/databases/database_calibre_emultation`.
4. Audit file-format and metadata suites for the handful of old behaviors not already covered.
5. Keep `vendor_frozen` and `integration_frozen` as separate backlogs from true retirements.
6. Treat legacy `folder_stores` tests/fixtures as a separate rewrite stream against current storage seams.
7. Delete duplicated in-repo legacy copies once their behavior has landed in the authoritative alpha test tree.

## Salvage Normalization Findings

- `salvage_existing` is currently only the remaining DB property corpus:
  - `16` files under `src/LiuXin_tests/test_databases/test_db_properties`
- Alpha already has a duplicate support copy under:
  - `tests/support/test_databases/test_db_properties`
- The broader duplicated DB-support trees currently compare as:
  - `203` mapped file pairs
  - `190` identical
  - `13` divergent
- The supposed authoritative support tree is not self-contained yet:
  - import coupling to `LiuXin_tests` has now been removed
  - utility-dependency blockers such as `clint` and `tqdm` have been routed through alpha-side wrappers
  - the next blocker is `LiuXin_alpha.folder_stores`, which is a rewrite boundary rather than another shim candidate
- The `13` divergent support-tree files have now been reviewed:
  - one root-file divergence has disappeared entirely
  - most remaining drift is intentional alpha normalization
  - three property files preserve a real alpha-side driver adaptation from `SQLite_apsw` to `SQLite`
- Concrete next actions for this batch are captured in:
  - [legacy-test-salvage-checklist-2026-03-16.md](legacy-test-salvage-checklist-2026-03-16.md)
  - [legacy-test-salvage-import-map-2026-03-16.md](legacy-test-salvage-import-map-2026-03-16.md)
  - [legacy-test-divergent-files-review-2026-03-16.md](legacy-test-divergent-files-review-2026-03-16.md)
  - [folder-stores-rewrite-boundary-2026-03-16.md](folder-stores-rewrite-boundary-2026-03-16.md)
  - [folder-stores-rewrite-plan-2026-03-16.md](folder-stores-rewrite-plan-2026-03-16.md)
  - [folder-stores-rewrite-checklist-2026-03-16.md](folder-stores-rewrite-checklist-2026-03-16.md)
  - [folder-stores-cleanup-boundary-2026-03-16.md](folder-stores-cleanup-boundary-2026-03-16.md)
  - [test-harness-rewrite-checklist-2026-03-16.md](test-harness-rewrite-checklist-2026-03-16.md)
  - [custom-column-field-matrix-2026-03-16.md](custom-column-field-matrix-2026-03-16.md)
  - [custom-column-cache-semantics-2026-03-16.md](custom-column-cache-semantics-2026-03-16.md)

## Status Samples

### `salvage_existing`

- `src/LiuXin_tests/test_databases/test_db_properties/test_db_0_properties.py`
  target: `tests/support/test_databases/test_db_properties/test_db_0_properties.py`
  note: Exact or near-exact support copy already exists in alpha support tree; normalize and wire into collected pytest contracts instead of re-porting.
- `src/LiuXin_tests/test_databases/test_db_properties/test_db_10_properties.py`
  target: `tests/support/test_databases/test_db_properties/test_db_10_properties.py`
  note: Exact or near-exact support copy already exists in alpha support tree; normalize and wire into collected pytest contracts instead of re-porting.
- `src/LiuXin_tests/test_databases/test_db_properties/test_db_11_properties.py`
  target: `tests/support/test_databases/test_db_properties/test_db_11_properties.py`
  note: Exact or near-exact support copy already exists in alpha support tree; normalize and wire into collected pytest contracts instead of re-porting.
- `src/LiuXin_tests/test_databases/test_db_properties/test_db_12_properties.py`
  target: `tests/support/test_databases/test_db_properties/test_db_12_properties.py`
  note: Exact or near-exact support copy already exists in alpha support tree; normalize and wire into collected pytest contracts instead of re-porting.
- `src/LiuXin_tests/test_databases/test_db_properties/test_db_13_properties.py`
  target: `tests/support/test_databases/test_db_properties/test_db_13_properties.py`
  note: Exact or near-exact support copy already exists in alpha support tree; normalize and wire into collected pytest contracts instead of re-porting.
- `src/LiuXin_tests/test_databases/test_db_properties/test_db_14_properties.py`
  target: `tests/support/test_databases/test_db_properties/test_db_14_properties.py`
  note: Exact or near-exact support copy already exists in alpha support tree; normalize and wire into collected pytest contracts instead of re-porting.
- `src/LiuXin_tests/test_databases/test_db_properties/test_db_15_properties.py`
  target: `tests/support/test_databases/test_db_properties/test_db_15_properties.py`
  note: Exact or near-exact support copy already exists in alpha support tree; normalize and wire into collected pytest contracts instead of re-porting.
- `src/LiuXin_tests/test_databases/test_db_properties/test_db_16_properties.py`
  target: `tests/support/test_databases/test_db_properties/test_db_16_properties.py`
  note: Exact or near-exact support copy already exists in alpha support tree; normalize and wire into collected pytest contracts instead of re-porting.

### `rewrite`

- `src/LiuXin_tests/core/self_test.py`
  target: `tests/core`
  note: Legacy core self-tests need reinterpretation against the new runtime/proxy/http core, not direct porting.
- `src/LiuXin_tests/customize/test__init.py`
  target: `tests/file_formats/conversion/plugins + tests/metadata/file_sources`
  note: Old customize smoke should be rewritten against the current plugin surfaces rather than ported as a legacy init test.
- `src/LiuXin_tests/databases/caches/calibre/fields/custom_columns/is_multiple_false/cc_bool_is_multiple_false/cc_bool_imf_full_test.py`
  target: `tests/databases/caches + tests/databases/database_calibre_emultation`
  note: Map old calibre-cache field/table tests onto the modern cache and calibre-emulation suites; do not copy unittest structure.
- `src/LiuXin_tests/databases/caches/calibre/fields/custom_columns/is_multiple_false/cc_bool_is_multiple_false/cc_int_imf_self_test.py`
  target: `tests/databases/caches + tests/databases/database_calibre_emultation`
  note: Map old calibre-cache field/table tests onto the modern cache and calibre-emulation suites; do not copy unittest structure.
- `src/LiuXin_tests/databases/caches/calibre/fields/custom_columns/is_multiple_false/cc_comments_is_multiple_false/cc_comments_imf_full_test.py`
  target: `tests/databases/caches + tests/databases/database_calibre_emultation`
  note: Map old calibre-cache field/table tests onto the modern cache and calibre-emulation suites; do not copy unittest structure.
- `src/LiuXin_tests/databases/caches/calibre/fields/custom_columns/is_multiple_false/cc_comments_is_multiple_false/cc_comments_imf_self_test.py`
  target: `tests/databases/caches + tests/databases/database_calibre_emultation`
  note: Map old calibre-cache field/table tests onto the modern cache and calibre-emulation suites; do not copy unittest structure.
- `src/LiuXin_tests/databases/caches/calibre/fields/custom_columns/is_multiple_false/cc_datetime_is_multiple_false/cc_datetime_imf_self_test.py`
  target: `tests/databases/caches + tests/databases/database_calibre_emultation`
  note: Map old calibre-cache field/table tests onto the modern cache and calibre-emulation suites; do not copy unittest structure.
- `src/LiuXin_tests/databases/caches/calibre/fields/custom_columns/is_multiple_false/cc_datetime_is_multiple_false/cc_float_imf_full_test.py`
  target: `tests/databases/caches + tests/databases/database_calibre_emultation`
  note: Map old calibre-cache field/table tests onto the modern cache and calibre-emulation suites; do not copy unittest structure.

### `covered`

- `src/LiuXin_tests/file_formats/lit/test_all.py`
  target: `tests/file_formats`
  note: Original aggregate file-format loaders are superseded by the modern per-format pytest suites; audit for missing edge cases only.
- `src/LiuXin_tests/file_formats/lrf/test_all.py`
  target: `tests/file_formats`
  note: Original aggregate file-format loaders are superseded by the modern per-format pytest suites; audit for missing edge cases only.
- `src/LiuXin_tests/file_formats/odt/test_all.py`
  target: `tests/file_formats`
  note: Original aggregate file-format loaders are superseded by the modern per-format pytest suites; audit for missing edge cases only.
- `src/LiuXin_tests/file_formats/pdf/test_all.py`
  target: `tests/file_formats`
  note: Original aggregate file-format loaders are superseded by the modern per-format pytest suites; audit for missing edge cases only.
- `src/LiuXin_tests/file_formats/test_all.py`
  target: `tests/file_formats`
  note: Original aggregate file-format loaders are superseded by the modern per-format pytest suites; audit for missing edge cases only.
- `src/LiuXin_tests/file_formats/txt/test_all.py`
  target: `tests/file_formats`
  note: Original aggregate file-format loaders are superseded by the modern per-format pytest suites; audit for missing edge cases only.
- `src/LiuXin_tests/metadata/file_sources/test_all.py`
  target: `tests/metadata/file_sources`
  note: Alpha has a much larger metadata-file-source suite; verify old behaviors are represented and add only missing cases.
- `src/LiuXin/metadata/web_sources/test.py`
  target: `tests/metadata/web_sources`
  note: Alpha already has a broad metadata web-source suite; compare behavior and add only missing cases.

### `vendor_frozen`

- `src/LiuXin/utils/libraries/clint/liscence/test_clint.py`
  note: Vendored or pinned dependency test. Track separately from true retirements; replace with narrow compatibility smoke only if needed.
- `src/LiuXin/utils/libraries/dateutil/test/test.py`
  note: Vendored or pinned dependency test. Track separately from true retirements; replace with narrow compatibility smoke only if needed.
- `src/LiuXin/utils/libraries/pyTree/tests/test_treelib.py`
  note: Vendored or pinned dependency test. Track separately from true retirements; replace with narrow compatibility smoke only if needed.
- `src/LiuXin/utils/lx_libraries/liuxin_dateutil/test.py`
  note: Vendored or pinned dependency test. Track separately from true retirements; replace with narrow compatibility smoke only if needed.
- `original_library_code/MySQL_Python/tests/test_MySQLdb_capabilities.py`
  note: Vendored or pinned dependency test. Track separately from true retirements; replace with narrow compatibility smoke only if needed.
- `original_library_code/MySQL_Python/tests/test_MySQLdb_dbapi20.py`
  note: Vendored or pinned dependency test. Track separately from true retirements; replace with narrow compatibility smoke only if needed.
- `original_library_code/MySQL_Python/tests/test_MySQLdb_nonstandard.py`
  note: Vendored or pinned dependency test. Track separately from true retirements; replace with narrow compatibility smoke only if needed.

### `integration_frozen`

- `src/LiuXin_tests/utils/liuxin_pydrive/test_apiattr.py`
  note: Externally pinned product integration. Keep tracked separately from the main alpha migration stream until that integration is intentionally revived.
- `src/LiuXin_tests/utils/liuxin_pydrive/test_drive.py`
  note: Externally pinned product integration. Keep tracked separately from the main alpha migration stream until that integration is intentionally revived.
- `src/LiuXin_tests/utils/liuxin_pydrive/test_file.py`
  note: Externally pinned product integration. Keep tracked separately from the main alpha migration stream until that integration is intentionally revived.
- `src/LiuXin_tests/utils/liuxin_pydrive/test_filelist.py`
  note: Externally pinned product integration. Keep tracked separately from the main alpha migration stream until that integration is intentionally revived.
- `src/LiuXin_tests/utils/liuxin_pydrive/test_oauth.py`
  note: Externally pinned product integration. Keep tracked separately from the main alpha migration stream until that integration is intentionally revived.
- `src/LiuXin_tests/utils/liuxin_pydrive/test_util.py`
  note: Externally pinned product integration. Keep tracked separately from the main alpha migration stream until that integration is intentionally revived.
- `src/LiuXin/devices/mtp/test.py`
  note: Hardware or external-device integration surface. Not dead, but intentionally outside the main alpha migration stream.
- `src/LiuXin/devices/usbobserver/test.py`
  note: Hardware or external-device integration surface. Not dead, but intentionally outside the main alpha migration stream.

### `retire`

- `src/LiuXin_tests/test_constants.py`
  note: Legacy test-data plumbing, not a product behavior test.
- `src/LiuXin_tests/utils/liuxin_unittest_spam/test_client.py`
  note: Old unittest spam/client-server test infrastructure is internal legacy test plumbing, not product behavior.
- `src/LiuXin_tests/utils/liuxin_unittest_spam/test_dummy_receiver.py`
  note: Old unittest spam/client-server test infrastructure is internal legacy test plumbing, not product behavior.
- `src/LiuXin_tests/utils/liuxin_unittest_spam/test_dummy_worker.py`
  note: Old unittest spam/client-server test infrastructure is internal legacy test plumbing, not product behavior.
- `src/LiuXin_tests/utils/liuxin_unittest_spam/test_server.py`
  note: Old unittest spam/client-server test infrastructure is internal legacy test plumbing, not product behavior.
- `src/LiuXin_tests/utils/liuxin_unittest_spam/test_test_suits/test_suite_1.py`
  note: Old unittest spam/client-server test infrastructure is internal legacy test plumbing, not product behavior.
- `src/LiuXin_tests/utils/liuxin_unittest_spam/test_test_suits/test_suite_10.py`
  note: Old unittest spam/client-server test infrastructure is internal legacy test plumbing, not product behavior.
- `src/LiuXin_tests/utils/liuxin_unittest_spam/test_test_suits/test_suite_11.py`
  note: Old unittest spam/client-server test infrastructure is internal legacy test plumbing, not product behavior.
