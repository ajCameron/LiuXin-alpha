# Whole-project descriptive reST docstrings — 2026-09-08

Current status: **paused at D095 by user request**. [Shutdown checkpoint](project-docstrings-hundred-paused-2026-09-15.md): 15/100 units verified in this chunk, 408 declarations; 853/2,732 complete project files. **D096 is next only on a new user request.** Earlier authorization and pause notes below are historical.

## Historical handoff before this chunk

## Request and scope

Active, unfinished goal: give every project function, class, and module a
descriptive docstring and standardize it to reStructuredText. This is explicitly
broader than the earlier maintained-public-boundary ratchet. Do not declare it
complete after a modern-only pass or replace missing prose with generated symbol
name restatements.

Work is on `codex/project-docstrings`, originally based on `edf6bf05`. The branch
advanced to `9790d020` (pre-alpha documentation checkpoint) during the latest
continuation's HTTP-test permission wait; that commit includes the 606-file
reviewed checkpoint. The continuation itself did not commit or push, and its final
working-memory updates remain local. PR #115 historically contains the earlier
maintainability programme; no fresh remote publication status is claimed here.

Latest checkpoint: [user-requested pause — 2026-09-15](project-docstrings-d-paused-2026-09-15.md).
The user authorized all 232 D units after the completed C03–C032 batch. Through
D080, 80 D units and 1937 declarations are verified; 107 files promoted.
Current complete-file coverage is 829/2,732: 1,136 classes and
9,248 functions, or 11,213 declarations. Partially reviewed files
retain 43 verified declarations. Remaining: 1,903 files,
31,012 declarations and 1,260 documentation units.
Individual static/regression/example results are linked from the D handoff and
inventory. Earlier results retain their historical scope; no new full subsystem
signoff is claimed before its milestone gates run.

Execution follows the [bounded completion plan](../dev-docs/project-docstrings-completion-plan.md).
The user paused the D range on 2026-09-15 because token cost was too high.
D081 remains queued; resume only on a new user request.
No commit or publication is requested. Previous source/test modifications and the
three runtime-doc exceptions remain preserved.

Ownership-guard caveat resolved by G02: the baseline reproduced seven failures
from counting docstrings toward file/function spans and wrapper statements.
All seven now pass through the G01 helper with the original numeric limits and
comparison operators. The final selection passed 48 tests/doctests with seven
explicit doctest skips. Dependency, composition, delegation, and other ownership
contracts remain enforced. G02 intentionally changes three test files; production
source and the three previously verified runtime-documentation exceptions remain
unchanged. Earlier notes reporting raw-line guard failures retain historical scope.

The inventory includes private/nested/async functions, classes, modules, tests,
examples, scripts, inherited/vendored Python, preference modules, and tracked
Python in the initialized data submodule. That submodule has one generator and
must not silently disappear from the final audit. Virtual environments and
ignored build artifacts are not project-owned source; lambdas have no named
definition/docstring statement. Embedded source strings in tests are fixtures,
not declarations to rewrite mechanically.

The only tracked native implementation is
`src/LiuXin_alpha/file_formats/djvu/bzzdecoder.c`. Its 13 functions, two state/table
records, file introduction, and Python-exposed function/module documentation now
have a separate source-reviewed reST pass. The Python AST totals do not cover it.
This is retained Python-2 extension source, not a newly ported or built native
extension; see the verification boundary below.

## Baseline

The initial main-repository tracked-source scan parsed all 2,718 Python modules,
4,398 classes, and 34,964 functions without a syntax failure. Missing docstrings:
1,130 modules, 2,079 classes, and 24,632 functions. A stricter subsequent scan
also counts whitespace-only docstrings and the submodule/new auditing tool.

The generated full audit at `/tmp/liuxin-docstring-baseline-2026-09-08.json`
contains 2,720 modules, 4,399 classes, and 34,975 functions; 27,846 declarations
have missing/blank docstrings. Existing descriptions additionally have reST
field, example, and delimiter gaps. Those counts precede the first production
documentation edits and new test suite; compare like-for-like scope when
reporting progress. Missing declarations are concentrated in tests, file formats,
metadata, utilities, surfaces, and caches, but no package is exempt.

## Approach and current changes

- `scripts/audit_project_docstrings.py` discovers source through Git, including
  tracked submodule Python and new non-ignored main-checkout Python. It reports
  named-definition findings without importing modules. `--check` fails for
  gaps; explicit file arguments are visibly labelled as a batch, not full scope.
  Structural checks do not prove descriptive accuracy: every batch still needs
  source review, meaningful parameter/return prose, and appropriate examples.
- Repair `scripts/normalize_docstrings.py` before broad mechanical use. Missing
  modules now count; variadic/keyword-only order and bound/static/free/nested
  receivers are distinguished; typed parameter prose is retained; unknown or
  duplicate parameter/return fields and literals sharing code/comments remain unchanged
  for manual review. A docstring-stripped AST comparison guards executable syntax.
  Examples and field descriptions in this tool have also been reviewed.
- The normalizer still only formats existing descriptions. It must not invent
  summaries or claim that empty parameter fields satisfy this goal. Escaped or
  otherwise unsafe literals require manual handling rather than dropped content.
- Initial reviewed production files: terminal `browser_components/models.py`,
  `windowed_components/models.py`, and `browser_components/session.py`. Document
  row access, optional readline operations, completion/paging records, curses
  capabilities, browser queries, and actual interactive/scripted lifecycle and
  error behavior. Protocol ellipses and implementation statements are retained.
- `tests/scripts/test_docstring_migration.py` exercises inventory scope, implicit
  receiver handling, field preservation, unsafe literals, AST comparisons,
  parse failures, and actual temporary-Git source discovery. Its own named
  definitions are also documented rather than excluded from the request.
- Both documentation tools and the migration suite enter the formatter/lint
  scope (159 files), and CI runs the suite. The unfinished whole-project audit
  is not installed as a permanently failing default CI gate.

## Verification and next actions

The first six-file batch passes the structural audit: 6 modules, 11 classes,
and 69 functions have no reported docstring, reST-field, example, or delimiter
gaps. Reviewed source files are the three terminal modules above, both tools,
and the migration suite. Its initial verification is complete:

- Final migration/terminal/public-documentation selection: **107 passed**.
- Doctest and migration-test selection over all six files: **48 passed, 46
  skipped**. Skips are explicit integration-only examples requiring an existing
  browser/window or temporary workspace; they are not executed integration proof.
- Standalone normalizer/audit/test doctests: passed. Normalizer `--check` passes
  on all six files; every production executable AST matches `edf6bf05`.
- Full existing quality gate: passed with 159 formatted files, 456 annotated
  modules, 221 protected dependency modules, 188 strict-mypy files, and both sets
  of 37 negative examples. Lint, complexity, and production type checks are clean.
- Historical report immediately after the first batch included the new test module:
  2,721 modules, 4,399 classes, 34,985 functions; missing/blank docstrings remain
  on 1,130 modules, 2,082 classes, and 24,608 functions (**27,820 total**).
  There are no parse failures. Existing-but-incomplete descriptions remain
  separate findings; the project-wide goal is emphatically not complete.

## Subsequent source-reviewed batches

The reviewed Python set now contains **668 modules, 874 classes, and 7,211 functions**
(8,753 declarations), all passing both the strict structural audit and normalizer
`--check`. The exact paths are saved in
[project-docstrings-reviewed-files.txt](project-docstrings-reviewed-files.txt).
The reviewed file set is:

- C02 curated identifier repository/API: two full files, two classes, 22 functions.
  Normalized identity, owner copying, primary flags/raw projection, and transactional
  replacement are documented, with executable ASTs unchanged and focused checks passing.
- C01 Agent repository and API: two full files, two classes, 27 functions. Canonical
  resolution, aggregate/type/alias preparation, matching reuse, and credit ordering
  descriptions now agree with implementation; executable ASTs are unchanged.
- G01 docstring-aware ownership measurement helper and focused tests: two new
  files, one class, sixteen functions; strict audit and normalizer pass. Existing
  reviewed files retain their prior verification with unchanged source hashes.
- Catalog exact-entity mechanics, configured repositories, note operations, and
  repository exports/composition; their six remaining exact/note API modules;
  complete convenience and semantic Catalog regression modules (12 files).
- Catalog base/WEMI repositories: complete generic CRUD/link helpers, Work,
  Expression, Manifestation, and Item repositories and their five API modules,
  plus the full repository-invariant test module and nested failure seams.
  Documents shallow row copies, validation/alias order, full-table paging,
  priority/type identity, scoped matching, direct Item ownership, and creation/link
  transaction boundaries. Corrects unsupported atomicity/link-metadata promises
  while preserving all runtime behavior. Remaining repositories stay in scope.
- Catalog specialized matchers: complete Work, Agent, curated Identifier, and
  observed Item-Identifier implementations and their four API modules. Documents
  identifier-first terminal decisions, acceptance bypass, aliases and evidence,
  duplicate-row selection, and differing limit/ID validation. All matching source
  and API modules are complete. Existing Work exact keyword mismatch is retained:
  API `cand_str` versus concrete `candidate_str`; reconcile in a separate API change.
- Catalog matching policy: complete policy/normalization/scoring helpers, exact
  matcher and identity specification, all eleven specification declarations,
  group composition, both corresponding API modules, and both matching regression
  suites. Documents terminal decisions, scope/identity distinctions, candidate
  display limits, floating-point boundaries, and repository-owned policies without
  changing behavior. Specialized matchers and their contracts are completed above.
- Catalog foundation: package/API exports, shared candidates/results/errors,
  facade protocols, concrete composition and all nineteen repository shortcuts,
  plus the complete import and API-documentation test modules. Documents exact
  schema writer selection, delegated transaction/return behavior, borrowed database
  lifetime, shallow frozen records, and matching validation limits. Full Catalog
  regressions pass; the remaining Catalog owners and tests are still in scope.
- Example completion: all five conversion modules, Library facade example, three
  metadata examples, and comments-to-HTML example. Includes logger/metadata-stub
  classes, nested shim functions, private helpers, and all CLI functions. All 28
  tracked example/helper modules are now documented, with process-state, partial
  effect, cleanup, result-cap, and reporting limitations made explicit.
- Storage/catalog examples: all 11 storage scripts, six catalog example/helper
  modules, and the shared JSON/import helper. Descriptions cover persistence,
  partial effects, resource lifetime, exit/report distinctions, and actual sanitizer
  limits. Existing script headers, CLI help text, and executable behavior are retained.
  The live-backend autouse fixture uses a plain Python illustration to avoid the
  installed pytest's missing-doctest-location skip-reporting error; fixture/test
  behavior is unchanged. See the latest note for full regression recovery evidence.
- Storage test completion: all 22 Location test/helper modules, the workflow API2
  doubles/contracts, example subprocess regressions, live backend read contracts,
  and PostgreSQL manager concurrency/recovery test with nested callbacks. All 85
  storage test/helper modules are now reviewed, alongside all 206 storage source
  modules. Descriptions distinguish passive values, backend behavior, simulated
  workflow evidence, actual concurrency, local examples, and opt-in live execution.
- Application storage manager: complete constructor, persistence hooks, journal
  recovery/retry, Store-row reconciliation, backing resolution, and private row/
  dependency helpers. The complete MiniDB and database reload regression modules,
  including nested doubles and callbacks, are also reviewed. Completes all 206
  storage source modules; the remaining 26 storage test/helper modules are completed
  by the subsequent batch above.
  Documents partial effects, database ownership, recovery evidence, and exact
  local/double verification boundaries without changing executable behavior.
- Backend registry: complete runtime context, descriptor fields, registration/
  iteration/build logic, all 25 backend builders, and the full default profile
  declaration block. Shared Unicode contract helpers/package, the fixture module
  and its value class/properties, and the four-backend matrix tests are also reviewed.
  Covers field projection, validation/effect ordering, exact path bytes, and the
  distinction between declared coverage and executed behavior. The application
  store_manager.py is completed by the subsequent batch above.
- Storage configuration/compatibility: all seven remaining small modules directly
  under storage (location, storage_types, store_factory, store_container, migrations,
  single_file, and store_spec_utils), plus the full backend-registry regression
  module. Covers row/codecs, null omission, identity/reload partial writes, migration
  adoption/retry, status callback/cache boundaries, and exact real/fake test evidence.
  The backend registry and application manager are completed by subsequent batches
  above.
- Backup persistence and operator composition: complete database repository/codecs,
  artifact Store registry, prototype/indexing/reporting pipeline, and their three
  full regression modules. Completes all six backup source and five backup test
  modules. Includes partial writes/reuse, exact legacy digest behavior, callback
  failure boundaries, and genuine SquashFS readback after catalogue reopen.
- Concrete backup execution: the complete inventory planner, SquashFS workflow,
  sealed-image provenance workflow, both package initializers, and all three
  adjacent regression modules. Covers nested grouping, designation/refresh,
  staging/retry evidence, partial image publication, recipe validation ordering,
  and explicit fake-tool versus real-filesystem/database verification boundaries.
- Workflow APIs: all twelve generic, backup, and sealed-artifact API modules,
  completing all 65 storage API source modules. Includes lifecycle/retry boundaries,
  selected value validation, estimate limits, checkpoint and registration partial
  writes, and the separation of archive construction, Store registration, and
  whole-image Asset provenance. Existing examples remain verified.
- Public manager conveniences: the complete 20-method facade mixin and all
  24 private identifier/normalization/delivery helpers. Completes all 29 public
  storage_manager_api modules, with input, ownership, selection, partial export,
  policy, and provenance contracts checked against actual delegation behavior.
- Database persistence adapters: both full repository/unit-of-work modules,
  completing all 22 storage_manager implementation modules. Includes compatibility
  mappings, scalar/envelope codecs, cache integration/invalidation, journal states,
  legacy row reconstruction, all domain ports, and explicit transaction lifecycle.
- Manager composition and persistence ports: remaining router/composition/package
  exports, both public facade context methods and compatibility repository exports,
  and all six persistence protocols with 29 methods. Covers direct routing versus
  metadata mutation, lazy inventory, reader lifetime, runtime structural limits,
  repository evidence/removal, and deferred database transaction outcomes.
- Private manager support: complete request/result/branch values and hash/ID/UUID
  helpers, shared-state initializer, all three internal protocols and 39 methods,
  and all shared support/journal helpers. Covers legacy journal naming, constructor
  partial state, locking, transient transaction limits, digest evidence, publication
  cleanup, observation/claim mutation, and destination allocation boundaries.
- Manager ingest/reconciliation: complete API contracts and conveniences, both
  implementation mixins with nested publication/fallback callbacks, and the public
  model initializer, completing all 11 manager-model source modules. Covers inventory
  completeness, late revision capture, ordered/partial observation updates, stream
  ownership and retry consumption, identified/native transfer, and adoption boundaries.
- Derivation provenance: complete recipe/source/graph/plan values and private
  validators, resolver/registry API and concrete traversal conveniences, and the
  full implementation with private breadth-first state. Covers validation limits,
  registration identity checks, eager filtering/indexing, direction/depth semantics,
  replay proposals, and revision/removal boundaries without policy revalidation.
- Storage policy: complete policy models and API, policy CRUD/assignment mixin,
  shared placement/recreation/reference support (including nested cycle traversal),
  and the complete policy API regression module. Covers validation limits, evidence
  predicates, candidate restoration, field clearing, capacity calculation, destination
  ranking, recursive feasibility, and resolver failure boundaries. The unchanged
  manager-composition size guard now fails for the fully documented support module.
- Composite/Item/retrieval: complete Composite and resolution models, all three
  public API contracts and implementation mixins, plus the full manager-composition
  and StoreContainer SQLite regression modules. Covers membership/order, optional
  members, exact role keys, selection evidence/ranking, cache materialization,
  source-mode evaluation, result validation, and unchanged structural guard limits.
- Asset identity and Replica lifecycle: identifiers, identity/metadata values,
  compatibility exports, Replica/result models, catalogue/lifecycle API contracts,
  and both implementation mixins. The complete older manager-documentation test
  module now accurately describes its bounded checks without weakening assertions.
  Covers identity matching, reference constraints, selection, revision updates,
  physical/metadata failure boundaries, and limits of supplied report evidence.
- Manager Store administration and operational status: complete models, API
  contracts, and both implementation mixins, with the full application-manager
  registration/bootstrap regression module. Documents normalization limits,
  health/reconciliation predicates, registry/lifecycle failure boundaries, status
  aggregation, transient recovery, and mutable database-double behavior.
- Manager routing: complete bound Location and factory wrappers/private protocols,
  router primitives/default conveniences, manager failure categories, and the full
  storage-manager API regression module. Covers stream ownership, conditional reads,
  post-publication failures, dynamic delegation, and all memory/nested test fixtures.
- Storage common values: all models, characteristics, shared/legacy errors,
  Location re-exports, placement-hint records/projections, and storage/API package
  exports, plus complete placement-hint and bound-Location regressions. Documents
  actual validation limits, provider/lookup error boundaries, copying, selection
  priorities, repeated metadata reads, and advisory names/keys without runtime changes.
- Local Store plugins: all twenty remaining backend-plugin source modules and six
  full adjacent regression modules, covering flat digest filenames, Calibre-like
  hint placement and post-write database updates, managed/unmanaged roots, single-file
  compatibility, and aliases. All 65 backend-plugin source modules are now reviewed.
  The previously reviewed SquashFS builder's two collision-mode descriptions also
  clarify that supplying both mode arguments rejects even when they agree.
- SquashFS: the complete raw driver, all seven read-only/build plugin modules,
  all three adjacent regression modules, and the standalone manifest launcher.
  Covers escaped inventory parsing, spooled ranges, subprocess/resource ownership,
  configuration distinctions, staging leases/deduplication, manifest direct writes,
  candidate validation, and publication failure boundaries. All seventeen raw-driver
  modules are now documented and verified.
- ISO writer: the complete raw session/mutation/layout builder, all three writable
  adapter modules, and the complete regression module and package initializer.
  Contracts cover accepted-write expectations, layout/copy limits, loss policy,
  candidate key/size validation, publication, and post-publication failures.
- ISO reader: the complete raw parser/reader, all three read-only adapter modules,
  and the full regression module, including real image helpers and injected parser
  callbacks. Contracts cover namespace selection, extent/UDF reads, bounds, names,
  signatures, cleanup, and runtime/configuration distinctions.
- 7z: the complete raw driver and regression module, including its lazy single-member
  spool/factory, metadata limits, extraction/CRC/range ownership, and nested import
  substitute. The ISO reader is now complete in the subsequent batch above.
- TAR/RAR drivers and builder: both raw drivers in full, the two-module
  `rar_build` plugin, and its complete regression module. Includes bounded TAR
  parser requests/positions, declared-size copying, RAR parser/extractor/checksum
  paths, nested output drainers, staging deduplication/leases, candidate evidence,
  and publication/synchronization failure boundaries.
- Shared archive mechanics and ZIP: all of `storage/drivers/archive_common.py`,
  `storage/drivers/zip.py`, `store_backend_plugins/archive_backends.py`, the six
  ZIP/TAR/RAR/7z plugin initializers, and the complete local-archive regression
  module, including nested process and replacement fakes. Covers actual metadata
  version evidence, range/resource ownership, staged expectations, candidate
  validation, publication failure boundaries, and runtime/configuration policy.
- Rclone driver and adapters: all of `storage/drivers/rclone.py`, the five
  `rclone_http_readonly` compatibility modules, the two `rclone_writable` modules,
  and both complete backend regression modules. This includes process lifecycle,
  incremental parser limitations, staged publication and metadata evidence, mutable
  invocation settings, per-instance pacing, native-import heuristics, and every
  nested fixture/process/corruption helper.
- FTP driver and compatibility Store: all of `storage/drivers/ftp.py`, the three
  `store_backend_plugins/ftp_readonly` modules, and their full regression module.
  This includes mutable option/configuration boundaries, URI ownership, completed
  spool reads, TypeError retry behavior, connection cleanup, bounded listings, and
  every nested malformed-transfer/listing fake. Verification distinguishes memory
  transport from real local destination bytes and in-memory manager metadata.
- S3 driver: all of `storage/drivers/s3.py` and `test_s3_storage.py`, including
  the injected-client protocol, staged sessions, publication/read/inventory helpers,
  memory client, and nested malformed-response doubles. Documentation distinguishes
  local expectation accounting, remote publication, and later metadata observation,
  and describes the actual limits of response evidence and bounded enumeration.
- HTTP driver: all of `storage/drivers/http.py` and `test_http_storage.py`,
  including the response protocol/reader, private URL/header helpers, and nested
  regression fakes. The reviewed HTTP Store facade now correctly documents
  absolute inventory URLs. Contracts distinguish endpoint health from inventory,
  ownership from text validation, and response evidence from body verification.
- Local drivers: complete `storage/drivers/__init__.py`, `_errors.py`,
  `_validation.py`, `filesystem.py`, and `sqlite.py`; all four
  `store_backend_plugins/single_file_sqlite` compatibility modules; full
  `test_filesystem_storage.py` and `test_single_file_sqlite_storage_backend.py`
  regressions. This includes private readers/sessions and failure, key, path,
  publication, and connection helpers, with actual cleanup and version limits.
- Concrete Stores: all six `storage/stores` modules (exports, filesystem, HTTP,
  SQLite, S3, encryption), plus complete `test_encrypted_storage.py` and
  `test_backed_store_recursion.py` regression modules. This includes all key
  providers, private sessions/readers, header/metadata helpers, and test doubles.
  Documentation distinguishes header inspection from tag authentication, direct
  ciphertext staging from local plaintext staging, and construction from startup.
- Raw storage-driver APIs: all eight `storage/api/store_driver_api` modules;
  `storage/utils/__init__.py`, `constants.py`, `driver.py`, and `workflow.py`;
  full `test_store_driver_api2.py`, `test_storage_api_doc_examples.py`, and
  `test_storage_api_redraft.py` regression modules.
- Configured Store foundations: `storage/api/store_api/__init__.py`,
  `identity_api.py`, `lifecycle_api.py`, `facade_api.py`, `ingest_source_api.py`;
  `storage/utils/store.py`; and the full `test_ingest_source_api.py` regression
  module. Also reviewed: all of `file_api.py`, `convenience_api.py`, and
  `driver_backed_api.py`, plus `test_store_configuration_convenience.py` and the
  complete `test_driver_error_messages.py`, including nested transport/fault doubles.
  All eight Store API modules and all five storage utilities are now reviewed.
- Browser components: `__init__.py`, `models.py`, `session.py`, `host.py`, `legacy.py`,
  `completion_sources.py`, `completion.py`, `browsing.py`, `help.py`, `registry.py`,
  `catalog.py`, `rows.py`, and `contracts.py` (all thirteen modules).
- Windowed components: `__init__.py`, `models.py`, `completion.py`, `console.py`,
  `presentation.py`, `status.py`, `telemetry.py`, `input.py`, `layout.py`, `job_output.py`,
  and `contracts.py` (all eleven modules).
- Shared/resource helpers: `surfaces/write_refresh.py`, `resources/__init__.py`, and
  `surfaces/terminal/job_view.py`, plus `surfaces/metadata_facets.py` and the full
  `surfaces/core.py` session/model/compatibility adapter, including its overloads.
  Also reviewed: `surfaces/__init__.py`, `surfaces/api.py`,
  `surfaces/acquisition_types.py`, `surfaces/presentation.py`, and the complete
  `surfaces/system_profile.py` selection/loading/persistence module.
  Legacy shared helpers `surfaces/categories.py`, `tags_icons.py`, and
  `thumbnail_cache.py`, plus both `surfaces/images` modules, are now reviewed too.
  Both `surfaces/read_model` modules are also fully reviewed, including all 54
  functions in its complete API implementation.
  All six shared `surfaces/catalog`, `surfaces/acquisition`, and `surfaces/opds`
  modules (api.py plus initializer in each package) are now reviewed too.
  All three `surfaces/opds_readonly` modules (app.py, initializer, entrypoint)
  and the standalone Python launcher `scripts/run_opds_readonly.py` are reviewed.
  The same complete three-module scope in `surfaces/web_calibre_readonly` and
  its launcher `scripts/run_web_calibre_readonly.py` are now reviewed too.
  All three generic `surfaces/web_readonly` modules and its launcher
  `scripts/run_web_readonly.py` are also reviewed, including nested cleanup
  methods and both source definitions of the store-detail renderer.
  The three `surfaces/api_readonly` modules and launcher
  `scripts/run_api_readonly.py` are now reviewed too.
  All three `surfaces/web_readwrite` modules and launcher
  `scripts/run_web_readwrite.py` are also reviewed, including form coercion,
  synchronous mutation, multipart upload, and partial-write failure contracts.
- CLI foundations: `surfaces/cli/__init__.py`, `__main__.py`, `app.py`,
  `parser_types.py`, `parsers.py`, `completion.py`, and `common.py`. Source review
  includes lazy dispatch, selector/shortcut rewriting, complete grammar assembly,
  shell-specific completion depth, atomic file publication, and managed-job policy.
  The full `squashfs.py` compatibility facade, `squashfs_commands.py`, and
  `squashfs_parsers.py` are now reviewed, alongside `core_cli.py` and `jobs.py`.
  This includes all provenance TypedDicts, daemon signal handling, and log following.
  Also reviewed throughout: `serve.py`, `capabilities.py`, `catalogue.py`,
  `workflows.py`, and `diagnostics.py`, including local backup/restore and
  diagnostic redaction/probe failure boundaries.
  Also reviewed: `config_cli.py`, `initialize.py`, `ingest_runs.py`, and
  `storage_audit.py`, including selector persistence, wizard helpers, local
  initialization partial effects, and log-based history/resume reconstruction.
  Metadata catalogue/file/online commands and PostgreSQL diagnostic/SQL/grant/export
  commands are now fully reviewed too: `metadata.py` and `postgres.py`, including
  separate publication/polling policies, weak verification receipts, and credential
  handling boundaries. CLI grammar/help/completion fingerprints remain unchanged.
  The storage compatibility facade and all 23 `storage_commands` modules are also
  reviewed: `__init__.py`, `constants.py`, `core_access.py`, `filesystem.py`,
  `signals.py`, `prompts.py`, `administration.py`, `integrity.py`, `resources.py`,
  `store_options.py`, and `store_add.py`. These cover request/status differences,
  bounded file input, cooperative cancellation, policy construction, and the
  nontransactional Store save/refresh/probe/default sequence. The remaining modules
  are now reviewed too: `ingest.py`, `ingest_config.py`, `ingest_options.py`,
  `ingest_paths.py`, `ingest_preflight.py`, `ingest_reporting.py`, `ingest_run.py`,
  `parser_files.py`, `parser_integrity.py`, `parser_stores.py`, `parsers.py`, and
  `store_wizard.py`. All **47 CLI source modules, 10 classes, and 394 functions**
  pass the strict audit, with parser/help/completion behavior unchanged.
- Ingest application: `ingest/mixed_application.py`, including both request/result
  records, TextOutput protocol, event helper, coordinator construction, and the
  full discovery/durable execution and context-lifecycle boundary.
  The package initializer, `models.py`, `stores.py`, and legacy `adding.py` are now
  fully reviewed too: lazy exports, report/checkpoint records, all Store copy/adopt
  and retention helpers, and legacy filesystem grouping/import contracts. Missing
  legacy metadata imports, annotation mismatches, and partial effects are documented,
  not silently repaired. The complete `remote_html.py`, both `pipelines` modules,
  and all seven `sources` modules are now reviewed too: URL scope/normalization,
  shared rate preferences, native/wget discovery, subprocess capture/cleanup,
  Store upsert, registration timing, and partial-write/failure contracts. This
  completes all **15 ingest source modules**.
- Storage-side HTML adapters: all four `native_html_readonly` modules and all
  five `wget_html_readonly` modules, including the compatibility utility aliases.
  Source review covers HTTP/discovery composition, construction/startup/probe
  boundaries, option snapshots, mutable discovery policy, and inherited behavior.
  All three `storage/ingest` modules are now reviewed too: initializer,
  `squashfs_drive.py`, and the complete `mixed_format.py` coordinator. Their
  documentation explains adoption versus classification, incremental persistence,
  member/cache accounting, cooperative limits, callback failures, and reuse policy.
  All four `storage/reconcile` modules are now reviewed too: initializer, report
  models, local/rclone registration, and complete SquashFS designation/publication.
  Their documentation distinguishes incremental writes, digest claims versus byte
  verification, the legacy-row transaction, and later manager/replica registration.
  `library/unmanaged_disk_ingest.py` is reviewed as an exact compatibility export.
- Terminal application/presentation: `__init__.py`, `app.py`, `database_creation.py`,
  `presentation.py`, `text_browser.py`, `browser.py`, `windowed_ui.py`, and `__main__.py`.
- Terminal commands: `__init__.py`, `base.py`, `core.py`, `clear.py`, `quit.py`,
  `search.py`, `top.py`, `summary.py`, `telemetry.py`, `db.py`, `jobs.py`, `ingest.py`,
  `link.py`, `note_on.py`, `on.py`, `off.py`, `show.py`, `mutate.py`, `store_view.py`,
  `sync.py`, and all fourteen `new_*.py` entity/store wizards. This now covers all
  34 modules in the command package.
- Both terminal plugin modules: `plugins/__init__.py` and `plugins/base.py`.
- All four `startup_scripts` modules: `__init__.py`, `functional_declarations.py`,
  `preferences.py`, and `prefs_folder_manager.py`.
- The two documentation scripts, `tests/scripts/test_docstring_migration.py`, and
  `tests/surfaces/test_windowed_ui.py`, `tests/surfaces/test_metadata_facets.py`, and
  `tests/core/test_core_wire.py`, `tests/core/test_proxy_docstrings.py`, and
  `tests/core/test_core_http_daemon_phase2.py`, `tests/core/test_core_runtime_jobs_phase2.py`,
  `tests/core/test_core_runtime_metadata_commands.py`, `tests/core/test_core_proxy_jobs_phase3.py`,
  `tests/core/test_program_workflow_facade.py`, `tests/core/test_evacuation_workflow.py`,
  and `tests/core/test_semantic_api_contracts.py`, `tests/core/test_core_application_api.py`,
  `tests/core/test_program_service_contracts.py`, and all of
  `tests/surfaces/test_terminal_component_behavior.py`,
  `tests/surfaces/test_terminal_component_ownership.py`, and
  `tests/surfaces/test_terminal_documentation_contracts.py`, plus
  `tests/surfaces/test_surface_core_documentation_contracts.py`,
  `tests/surfaces/test_shared_surface_dependencies.py`,
  `tests/surfaces/test_surface_package_api.py`, and
  `tests/surfaces/test_shared_surface_documentation_contracts.py`,
  `tests/surfaces/test_images_api.py`, `tests/surfaces/test_images_api_contracts.py`,
  and `tests/surfaces/test_category_thumbnail_documentation_contracts.py`, plus
  all five read-model suites: `tests/surfaces/test_read_model_api.py`,
  `test_read_model_metadata_parity.py`, `test_read_model_failure_contracts.py`,
  `test_read_model_transport_errors.py`, and `test_read_model_documentation_contracts.py`
  in that same directory. Also reviewed: `tests/surfaces/test_acquisition_api.py`,
  `tests/surfaces/test_opds_api.py`, and
  `tests/surfaces/test_compatibility_surface_documentation_contracts.py`.
  Also reviewed: `tests/surfaces/test_catalog_api.py`,
  `tests/surfaces/test_opds_readonly.py`,
  `tests/surfaces/test_readonly_surface_cli_help.py`, and
  `tests/support/_surface_storage_tables.py`.
  All helpers and tests in `tests/surfaces/test_web_calibre_readonly.py` are
  now reviewed as well, together with `tests/surfaces/test_web_readonly.py`,
  `tests/surfaces/test_api_readonly.py`, and the shared direct/RPC acceptance
  harness in `tests/surfaces/test_core_surface_acceptance.py`.
  All 27 functions in `tests/surfaces/test_web_readwrite.py`, including both
  nested WSGI callbacks, are now reviewed and documented too.
  Also reviewed: all functions/classes in `tests/surfaces/test_cli_dependency_contracts.py`
  and `tests/surfaces/test_cli_operational_families.py`, including their nested
  callbacks and fake-Core/context fixtures. The full
  `tests/surfaces/test_cli_operator_hardening.py` suite is now also source-reviewed
  and documented, including all recording/SQLite/profile helpers and nested resume
  capture. Real local integration is distinguished from canned Core receipts.
  All ten functions and the module introduction in `tests/surfaces/test_cli_squashfs.py`
  are now reviewed too, including real publication and replica/provenance cases.
  Both `tests/surfaces/test_storage_audit_cli.py` and
  `tests/surfaces/test_cli_init_ingest.py` are now reviewed throughout, including
  nested callbacks and the latter's runtime-created Args fixture documentation.
  Both `tests/surfaces/test_cli_metadata.py` and `tests/surfaces/test_cli_postgres.py`
  are now documented throughout, including recording clients, sessions, lifecycle
  fakes, and nested callbacks. Mocked readiness/metadata receipts are distinguished
  from the real local SQLite catalogue and EPUB round-trip test.
  `tests/surfaces/test_cli_storage.py` is now fully documented too, including its
  helper functions, real lock fixture, checkout subprocess, and direct signal-state
  test. It exercises discovery/preflight/refusal/reporting, not successful managed ingest.
  All declarations in `tests/ingest/test_store_ingest.py` and
  `tests/databases/adding/test_adding_api.py` are now reviewed too, including nested
  HTTP doubles and directory-import recorders. Their live local/in-memory/injected
  evidence is distinguished from external services and legacy metadata integration.
  The complete `tests/ingest/test_remote_discovery_pathologies.py`, both
  `tests/storage/reconcile/test_{native,wget}_html_store_db_sync.py`, both
  `tests/library/test_{native,wget}_html_ingest_library.py`, and both native/wget
  `tests/storage/store_backend_plugins/*html_readonly/test_*html_readonly_storage_backend.py`
  modules are now documented too, including all nested HTTP/process doubles.
  Their assertions remain unchanged; native registration/Library cases retain
  their separate robots lookup rather than proving successful live crawling.
  Both `tests/storage/ingest/test_squashfs_drive_ingest.py` and
  `test_mixed_format_ingest.py` are now fully documented, including archive/database
  helpers and the nested metadata callback. Test docs distinguish real extractor
  use, SQLite reload/reopen boundaries, signature-only classification, and logs.
  Also fully reviewed: `tests/library/test_adding_unmanaged_disk_ingest.py`,
  `tests/library/test_rclone_http_ingest_library.py`, and reconciliation suites
  `test_rclone_http_store_db_sync.py`, `test_squashfs_db_sync.py`, and the full
  `test_squashfs_db_sync_contracts.py`, including static/nested doubles. They
  distinguish real archive tools, real catalogue rows, canned JSON, and transaction
  recorder evidence; historical test names do not overstate their assertion scope.
- Core: `__init__.py`, `api.py`, `factory.py`, `workflow_jobs.py`, `wire.py`,
  `errors.py`, `events.py`, `commands.py`, `queries.py`, `description.py`,
  `dispatch.py`, `registry.py`, `services.py`, and `runtime.py`; all four `proxies` modules
  (`__init__`, `jobs`, `local`, `remote`) and both `transport` modules (`__init__`,
  `http`) are also fully reviewed. Also reviewed: `program_endpoints/__init__.py`
  and `common.py`; `program_services/__init__.py`, `preferences.py`,
  `evacuation_models.py`, `evacuation_planning.py`, `evacuation_execution.py`,
  `storage_evacuation.py`, `storage_placement.py`, and `store_resolution.py`.
  Also fully reviewed: `program_services/payloads.py`, `stores.py`, `conversion.py`,
  `ingest.py`, `maintenance.py`, `storage_recovery.py`, `backup.py`, and `catalog.py`.
  The full `application_api.py`, `database_semantics_api.py`, `storage_graph_api.py`, and `browse_api.py`
  modules are also reviewed, including their nested helpers.
  All remaining Core modules are now reviewed: `program_services/database.py`,
  `schema.py`, `metadata.py`, `discovery.py`, `storage_integrity.py`, `storage_repair.py`,
  and `storage_status.py`; endpoint providers `backup_maintenance.py`, `catalog_search.py`,
  `content_workflows.py`, `database_schema.py`, `storage.py`, and `system_jobs.py`;
  `program_endpoints/handlers.py` and `program_api.py`. This completes all **57 Core
  source modules**, with 58 named classes and 635 named functions; the known
  runtime-created vacuum adapter class has an additional explicit docstring.
- Data-submodule `scripts/generate_calibre_fixture_libraries.py`.

The terminal set now covers all **69 source modules, 114 classes, and 536 named
functions**, including both shared ABCs and every remaining composition/helper owner.

This is a reviewed-file ledger, not a restriction of the whole-project objective.
Subsequent work documents actual behavior rather than simply filling fields:

- Browser host methods distinguish unsupported panes, capability declarations,
  prompting defaults, output flushing, and best-effort clearing. Legacy syntax
  rewrites preserve lookup/validation precedence. Completion sources explain
  visible-row priority, fallback scans, partial results, and column naming.
- Shared refresh documents method priority, false results, fallback objects, and
  which exceptions remain visible. Resource helpers distinguish Traversable
  handles from unpacked filesystem paths, without promising archive extraction.
- Windowed helpers document buffer-sensitive completion cycling, character-based
  wrapping (not terminal display cells), bottom-relative scroll offsets, separate
  output fragments, telemetry baseline resets, unavailable counts, and visible
  job/snapshot errors. Pure helper examples run; external driver examples are
  explicitly skipped integration illustrations.
- Startup documentation corrects copied folder descriptions and inaccurate
  guarantees. Existing files can count as present in some helpers; manifest paths
  are not validated/confined; creation errors propagate without rollback.
  Historical opener/import-hook and undefined-name limitations are explicit,
  not silently repaired as part of this documentation task.
- The data wrapper documents checkout discovery and its missing optional companion
  generator dependency. Its submodule was clean before this edit. It is now dirty
  and requires a separate submodule commit if publication is later requested;
  a main-repository commit alone would not capture the source edit. No fixture
  generation has been run.
- The style-guide example now supplies meaningful parameter/return descriptions
  and explicitly labels empty fields as unfinished documentation.

Verification after the windowed batch:

- Host/shared-refresh doctests and terminal regressions: **91 passed, 20 skipped**.
- Legacy/completion-source doctests: **5 passed, 20 skipped**.
- Migration, CI/link/tooling, terminal ownership/behavior, and windowed regressions:
  **191 passed** after the duplicate-return and field-continuation safety repairs.
- Five newly reviewed windowed modules, resource helpers, and adjacent
  terminal/migration/resource contracts: **127 passed, 33 skipped**.
- Data-wrapper doctests and existing DjVu integration selection: **6 passed,
  2 skipped**. This does not build or execute the legacy C extension.
- Full quality runner passed again: 159 formatted files, 456 annotated modules,
  221 protected dependency modules, 188 strict-mypy files, and all 37 negative
  examples rejected by each checker. Lint, complexity, and typing remained green.
  Startup and C documentation followed that gate; their executable-code comparisons
  are recorded separately rather than claiming they belong to its strict scope.
- All **17 edited production Python files** retain exactly the executable AST of
  `edf6bf05` after stripping literal docstrings. The data generator separately
  matches its submodule HEAD in the same comparison.
- Native C lexical comparison, excluding comments and combining adjacent string
  literals, finds exactly two changed tokens: the existing function and module
  documentation strings. No C implementation tokens changed. Mixed checkout line
  endings were normalized to LF as required by `.gitattributes`.

The first startup doctest collection failed because `functional_declarations.py`
imports the absent `LiuXin_alpha.utils.general_ops` package. Source/AST inspection
confirms this dependency predates the edits. Its module introduction now states
the limitation. The no-op print hook's single doctest passes when the unchanged
function is compiled directly from its AST; this is not module-import evidence.
Do not restore unrelated legacy imports merely to make this documentation run green.
The next explicit-file pytest run also collected the pre-existing manual
`startup_scripts.__init__.test_lopen` as a test function, independently of its
skipped doctest example. It failed at another removed legacy import,
`utils.calibre.ptempfile`: **1 failed, 43 passed, 19 skipped**. That routine's
docstring now states this dependency as well. The final doctest-focused rerun
deselects that specific manual test, not the documentation example or a default
CI test. Both pre-existing failures remain visible here; no implementation,
test-discovery defaults, or module-wide suppression was added to hide them.

Final startup doctests, documentation-link contracts, and Python BZZ fallback
selection: **43 passed, 19 explicitly skipped integration examples, 1 deselected
legacy manual test**. The extracted no-op-hook doctest also passed separately.
Final public-documentation, migration, CI-workflow, and terminal-ownership
contracts: **71 passed**. Rechecked the 17 production Python executable ASTs
and C documentation-only lexical comparison after the final prose changes.
Both root and data-submodule diffs are whitespace-clean. No new commit or push
has been made on this documentation branch.

Latest generated whole-project audit:
`/tmp/liuxin-docstring-catalog-exact-repositories-2026-09-12.json` covers 2,730 Python modules,
4,400 classes, and 35,119 functions. Missing/blank docstrings remain on **1,065
modules, 1,894 classes, and 20,044 functions (23,003 total)**, with no parse
failures. Existing incomplete descriptions are additional findings: 3,288
delimiter-layout, 9,492 missing-example, 2,735 parameter-field, 3,590 return-field,
3,151 empty-parameter-description, 4,119 empty-return-description, and 28
missing-summary findings. These categories overlap and are not additive totals
of undocumented declarations. The full project goal remains unfinished.

## Command, pane, job-view, and windowed-test continuation

The next eleven files extend the reviewed set from 21 to 32 modules:
browser help/registry/browsing/completion, windowed job-output/input/layout,
both component initializers, shared `job_view.py`, and the windowed test module.
Important source-derived contracts now documented include:

- Help reads registration metadata, not Python docstrings. Group aliases take
  lookup precedence; registrations can remain partially applied after a collision.
  Re-registering the same lifecycle plugin appends another entry.
- Paging limits can change the next/previous step distance. Search display limits
  do not bound contains-search scanning. Completion uses whitespace rather than
  shell quoting, preserves candidate order, and recognizes command-specific slots.
- Input accepts/edit/history behavior, best-effort persistence, setup-before-finally
  boundaries, pane-space allocation, status/telemetry cadence, and drawing error
  boundaries are explicit. The new owners remain within the original physical
  450/160-line limits; no ownership or complexity gate was changed.
- Shared job views normalize records shallowly without validating timing/state
  fields. Log helpers read the entire advertised file, do not confine paths or
  sanitize control characters, and distinguish absent/missing/empty/read failures.
  Removed the inaccurate existing "bounded log-tail" claim from these docstrings;
  no log-reading implementation or security policy was changed.
- Windowed test fakes and each test explain their asserted behavior. Their
  executable AST is identical to `edf6bf05`, including all original assertions.
  Pure test examples execute; examples needing pytest temporary paths are skipped.

Verification for this continuation (selections overlap):

- Help/registry/browsing doctests plus behavior/ownership contracts:
  **74 passed, 32 skipped**.
- Windowed input/layout/job-output doctests plus behavior/ownership contracts:
  **74 passed, 38 skipped**.
- Shared job-view doctests: **8 passed, 1 skipped**.
- Documented windowed tests plus their doctests: **50 passed, 4 skipped**.
- Full quality runner passed again with unchanged 159-file formatter scope,
  456 annotated modules, 221 dependency-protected modules, 188 strict-mypy files,
  and 37 negative examples rejected by each checker.
- All 27 edited production Python executable ASTs match `edf6bf05`; the newly
  documented windowed test module is compared separately. All reviewed files pass
  the documentation audit and normalizer. The job-view and windowed-test files are
  formatter-clean but remain outside the default lint/type scope.
- An additional Ruff inspection of those two outside-scope files reports existing
  Optional/f-string/import findings: eight in `job_view.py` and three in the test
  module. A before/after comparison against `edf6bf05` proves the same rule codes
  and messages remain, ignoring moved source locations. This inspection is not
  clean, but the documentation edits introduced no new lint findings there.

An asynchronous question asks whether physical owner-size limits may distinguish
docstring-only lines from implementation lines, with numeric limits preserved and
tests for the counting logic. No answer has arrived; do not assume approval or
change the guards. Catalog/row owners and large shared contracts still need their
documentation pass. Other project work remains available regardless of that choice.

The earlier broad test handle **5380** and focused handles **41523/26664** are now
authoritatively absent: polling reports unknown process IDs, and a process listing
showed no surviving test process. Their unobserved final results are not claimed
as passes. Fresh runs and the currently live replacement are recorded below.

## Application, extension, inspection, recovery, and job continuation

Nineteen more modules extend the reviewed set from 32 to 51. This includes the five
application/presentation modules, both plugin modules, and twelve command modules
listed above. Every named declaration in those files was reviewed against its
implementation and now has descriptive reST documentation.

- Presentation contracts distinguish character widths from display-cell widths,
  in-place fitting from rendering, and limited line-break escaping from general
  sanitization. Header suffixing does not guarantee uniqueness against names that
  already contain such suffixes; the prefix-strip helper requires nonempty prefixes.
- Application docs explain mode precedence, parser versus operational exceptions,
  lazy curses startup, wizard cancellation, and creation effects without rollback.
  The wizard rejects an explicit `--core-endpoint`; documentation does not extend
  that check to every indirectly resolved profile endpoint.
- Command metadata and factory docs separate construction, registration, execution,
  and post-return refresh. Abstract command bodies remain docstring-only. Optional
  lifecycle hooks do not promise one invocation per plugin object. Pure mocked-host
  examples test forwarding/continuation and do not start browsers or mutate databases.
- Table summaries inspect all counts even with a short display limit. Top/head/list
  preserve backend order and do not update paging state. Telemetry query failures
  become zero-valued displayed activity, while missing counts become question marks.
- SQLite unlock docs expose best-effort holder detection, potentially unbounded
  direct lsof waits, opt-in signalling, non-atomic sidecar renames, and checkpoint
  limitations. Dry-run still opens SQLite and probes a write transaction. Executable
  examples use pure parsing or empty targets; no real signalling/recovery was run.
  Existing unlock regressions mock process signalling and recovery helpers.
- Jobs docs distinguish requesting cancellation from waiting for termination, Core
  capability selection from fallback after errors, and log display limits from
  complete-file reads. Wait aliases such as `off` still enable waiting with no
  timeout. Disk ingestion submits a Core job and waits without a timeout; report-level
  file errors are displayed rather than automatically turning execution into failure.

Verification for this continuation (selections overlap):

- Application/presentation doctests plus terminal dependency contracts: **35 passed,
  4 skipped**.
- Command/base/plugin doctests plus component behavior/ownership and migration tests:
  **125 passed, 4 skipped**.
- Top/summary/telemetry doctests: **8 passed**.
- SQLite unlock doctests: **10 passed, 5 skipped**; skipped examples require external
  processes, paths, or a connected browser and are not executed integration evidence.
- Jobs/ingestion doctests: **22 passed, 2 skipped**.
- All **46 modified production Python files** retain the executable AST of
  `edf6bf05`. The windowed test module and data-submodule generator are separately
  unchanged under the same docstring-stripped comparison. All 51 reviewed files
  pass the strict audit and normalizer `--check`.
- A full quality run passed after the first command batch, and the final all-current
  rerun also passed: unchanged 159/456/221/188 formatter/annotation/dependency/mypy
  counts and 37 negative examples rejected by each checker. Lint, complexity, and
  production type checks are green within their existing scopes.
- Additional Ruff checks outside the default scope retain existing findings only:
  command initializer 1, clear 1, quit 1, top 3, summary 1, telemetry 3, db 22,
  jobs 35, ingest 16; core/search/plugin initializer have zero. Before/after rule
  codes and messages match `edf6bf05`, ignoring moved locations. This is not a
  claim that legacy command implementations are lint-clean.

The replacement broad text-browser/windowed run **84685** completed successfully:
**225 passed, 16 skipped doctests, 12 multiprocessing/fork warnings** in 1,482.08
seconds. It ran:
`.venv/bin/python -m pytest -q --doctest-modules
src/LiuXin_alpha/surfaces/terminal/browser_components/completion.py
tests/surfaces/test_text_browser.py tests/surfaces/test_windowed_ui.py`.
It started before the latest command prose, whose executable ASTs are unchanged;
the new command doctests ran separately afterward. This handle is now closed:
do not restart it or describe it as pending. No broad browser run remains live.

## Relations, row mutation, and shared-facet continuation

Eight more reviewed modules extend the ledger from 51 to 59: six command modules
(`link`, `note_on`, `on`, `off`, `show`, `mutate`), the shared metadata-facet helpers,
and their existing test module. Source-derived boundaries now documented include:

- Compact/split references, optional separators, selector limits and ordering,
  scalar parsing, permitted relation fields, and existing-link behavior. Generic
  `link`/`unlink` use the browser database facade; note/bulk commands dispatch Core
  operations. Multi-row deletion has no command-level rollback.
- Bulk attachment/detachment performs compensating recovery, not an atomic
  transaction. Source-resolution errors bypass the per-link handler. Effects can
  remain untracked after a write followed by an output/read error. Cleanup can
  interpret failed relationship reads as empty; semantic restoration does not
  preserve exact raw link IDs, priorities, or arbitrary extra fields.
- Language matching never creates language rows and suppresses individual search
  failures. Series retries multiple matching columns and can create after read
  failures. Genre/subject matching selects one schema-dependent column. The single
  `note-on` command checks all matching notes for existing links, unlike bulk note
  resolution's first-match reuse.
- `links` and `show all` skip failed relation reads and same-table links; no-results
  output does not prove absence. Page limits restrict display, not retrieval.
  Show-tag aliases all use the tags token even if the user typed a label alias.
- Row-edit coercion follows current Python values, not schema validation. Null
  aliases override strings, EOF keeps a field without canceling earlier edits,
  and there is no final concurrency recheck. Forced deletion still queries and
  prints impact; it skips only confirmation.
- Shared tag/label helpers now describe count-failure policy, exact field priority,
  whitespace/case normalization, display fallbacks, and payload-only construction.
  Their test fixtures and assertions are documented with executable examples.

Verification for this continuation (selections overlap):

- Link/single-note doctests: **14 passed, 5 skipped**.
- Bulk attachment doctests: **13 passed, 11 skipped**.
- Detachment/linked-display doctests: **22 passed, 6 skipped**.
- Shared-facet doctests plus original tests: **22 passed**.
- Row-mutation doctests: **17 passed, 5 skipped**.
- Newly documented facet tests plus their doctests: **28 passed**.
- Strict audit and normalizer `--check`: all **59 modules, 97 classes, and 463
  functions** pass. All **53 modified production Python files** and both documented
  existing test modules have executable ASTs identical to `edf6bf05`. The modified
  data-submodule generator also matches its own HEAD after docstring stripping.
- Final quality rerun passed: 159 formatted files, 456 annotated modules, 221
  protected dependency modules, 188 strict-mypy files, clean selected lint/complexity
  checks, and all 37 negative examples rejected by each type checker.
- Root and data-submodule diffs remain whitespace-clean. Outside-scope Ruff
  before/after checking initially required exact finding equality and caught a
  removed import-layout finding in `show.py` from mechanical formatting; this is
  an improvement, not a new lint failure. The completed comparison confirms no
  new findings across all eight files: link 26, note-on 5, on 32, off 11, show 16
  (previously 17), mutate 21, metadata facets 7, and the facet test module zero.
  These files remain outside the default lint scope; do not call the production
  command files lint-clean or silently fix their unrelated existing findings.

No implementation behavior, runtime help strings, or assertion bodies were changed
in this continuation. Remaining work is still whole-project, not limited to this
command-family milestone.

## Store inventory, synchronization, and creation-wizard continuation

Sixteen command modules extend the reviewed set from 59 to 75 modules
(118 classes and 552 functions). This completes the documentation review of all
34 command-package modules, not the entire terminal surface or project.

- Store inventory options, nested filter/sort records, display flags, and row
  lookup helpers now explain complete scans before pagination, numeric-reference
  precedence, tie ordering, and permissive coercions. Display and filter boolean
  parsing differ: a nonempty string such as `false` can display as `yes` while
  filtering as false. Counts describe database inventory, not live-file probes.
- Sync docs identify local/rclone/wget/native dispatch precedence, mode-specific
  flags, timeout/rate aliases, whole-token `to-db` removal (including values),
  incremental crawler writes, and progress/log behavior. Foreground waiting has
  no timeout or missing-job escape; log replay requests chunks without a total cap
  and strips trailing whitespace. Background submission does not imply success.
- All fourteen `new_*` wizards document actual prompts, defaults, duplicate
  checks, schema-dependent optional fields, payload construction, and cancellation
  boundaries. Standalone work/expression/manifestation/item creation is distinct
  from `new_title`'s Core WEMI stack; the latter does not create a legacy title row
  or inspect source files. Date/year coercions are intentionally described as
  permissive rather than validation. Agent parent checks do not necessarily
  enforce agent type. A created series can remain after its creator-link fails.
- Store setup can create local directories before saving or later cancellation.
  New configurations have no extra final confirmation; existing root/name matches
  prompt before Core's own upsert selection. Read-only/online flags are declared
  policy, not probes. Refresh clears existing manager state after persistence and
  is not part of an atomic save/refresh operation.

Verification (selections overlap):

- Store-view doctests: **15 passed, 5 skipped**. Sync doctests: **11 passed,
  3 skipped**. Focused store/parser regressions: **5 passed, 170 deselected**,
  with three existing multiprocessing/fork warnings.
- First five note/tag/genre/subject/series wizard doctests: **12 passed, 4 skipped**.
  Creator/organisation/publisher wizard doctests: **10 passed, 3 skipped**.
- The previous six-wizard test results were not recoverable: handle **43123** was
  authoritatively missing, and the process listing showed no surviving pytest
  process from the two launches whose handles had been truncated. No unobserved
  pass was claimed. Fresh expression/manifestation/item/work/title/store doctests:
  **34 passed, 9 skipped**. Fresh `test_text_browser.py -k 'new and wizard'`:
  **20 passed, 155 deselected**. Both replacements finished.
- All 75 reviewed modules passed strict audit and normalizer checks. All 69 edited
  production Python files, both existing surface test modules, and the separately
  tracked data generator retained their executable ASTs.
- Extra Ruff checking confirms no new rule/message findings in these sixteen
  outside-scope files. Existing counts: store-view 36, sync 24, creator 6,
  expression 6, genre 5, item 7, manifestation 3, note 1, organisation 8,
  publisher 7, series 6, store 11, subject 4, tag 4, title 13, work 6.

## Core worker, wire, envelope, and public-contract continuation

Twelve production Core modules and the existing wire-test module extend the
reviewed ledger from 75 to 88. Source review followed referenced library/backup
models, selected runtime/remote-client methods, and relevant tests without
changing those dependencies merely to improve documentation results.

- Worker reports are recursively unpacked, not guaranteed JSON-safe: bytes and
  unknown leaves remain, mapping keys can collide, and cycles are not guarded.
  Reconciliation workers forward backend-specific options and do not turn every
  report-level error into a raised job failure. Conversion reports post-run output
  observations, not validated ebook contents or an atomic replacement guarantee.
- Backup mapping decoding documents legacy aliases, truthiness/numeric coercion,
  constructor validation, and lack of backing-file/store checks. Direct execution
  defaults override conflicting mapped verification/cleanup flags. Persisted
  execution checkpoints each returned step but has no worker-level no-progress
  guard, timeout, or transaction coupling filesystem effects with checkpoint saves.
  Only complete results enter artifact registration; other terminal outcomes are
  recorded, not automatically raised here.
- Wire encoding documents tagged values, dictionary collision checks, row-like
  fallback collisions that can already collapse, repr-based set ordering, and
  missing cycle guards. Error descriptions preserve the important distinction
  between a committed canonical write and a failed cache refresh.
- Event/request/result envelopes are frozen only at the field level. Creation
  does not validate nested values or deliver events. Introspection records use
  lossy default rendering, not the strict wire encoder. Dynamic write classification
  is a naming heuristic, not an authorization or side-effect boundary.
- `core/registry.py` contained no implementation. Replaced its inaccurate claim
  to discover/load plugins with an explicit historical-placeholder description.
- The public protocol/ABC now documents identity reads, envelope execution,
  introspection, subscription, and shutdown. Kept all original protocol ellipses;
  abstract methods originally containing only docstrings remain docstring-only.
  Base `command`/`query` adapters unwrap `.result` without inspecting `.ok`; mocked
  executable examples demonstrate that policy instead of promising automatic
  error conversion. Remote shutdown targets the hosted runtime, not just a client.
- Factory docs distinguish borrowed libraries/databases/caches, owned wrappers
  and constructed caches, and the process-global job manager. Remote selection
  does not probe a server. Initialization failures have no factory-level cleanup
  compensation. Public exports, call signatures, handler metadata, and code bodies
  remain unchanged.

Verification (selections overlap):

- Worker doctests plus migration/ownership suites: **30 passed, 12 skipped**.
  The ten remote-sync routing/forwarding regressions passed, **185 deselected**;
  they use mocked remote backends, not live web crawling.
- Wire/error/event/envelope/dispatch doctests and documented wire tests:
  **28 passed**. Core factory/API doctests: **6 passed, 20 skipped**.
- Description doctests, the runtime phase-1 suite, and named-job/backup persistence
  tests initially yielded **24 passed, 1 failed**. The failure was sandbox denial
  of socket creation when the backup test started its local HTTP daemon, after
  its direct persistence checks succeeded. Permission-approved rerun of that
  single direct/RPC backup test: **1 passed**. Do not label the sandbox run green
  or treat the environment restriction as a source regression.
- All **88 modules, 137 classes, and 607 functions** pass strict structural audit
  and normalizer `--check`. All **81 edited production Python files** and all three
  existing edited test modules retain the executable AST of `edf6bf05`; the data
  generator separately retains that of its own HEAD. The Core/command selection
  of 47 files is formatter-clean.
- Extra Ruff baseline comparison adds no new findings: worker 5, wire 8, errors 0,
  events 3, commands 0, queries 0, registry 0, dispatch 1, descriptions 3,
  Core initializer 1, factory 1, API 2, wire tests 1. Legacy findings outside
  the default scope were not repaired or suppressed. Default quality gates passed
  after the first Core batch; final all-current verification is recorded below.

Final all-current verification also passed:

- Permission-approved Core factory/client ownership, direct/RPC envelope parity,
  typed-event subscription, reconciliation receipts, and non-wire-error regression
  selection: **6 passed, 10 deselected**.
- Full quality runner: **159 formatted files**, annotation coverage across **456
  modules**, **221** dependency-protected modules, clean selected lint/complexity,
  **188** strict-mypy files, no production typing errors, and all **37** negative
  examples rejected by each checker. No default scope or owner-size guard changed.
- Whole-project report after the API/factory edits contained 27,391 missing
  declarations. Later continuation counts supersede that snapshot above.
- Final developer-link/public-documentation contract selection: **23 passed**.
  Root and data-submodule diffs remain whitespace-clean.
- All test/audit/quality handles from these batches have finished. No run is
  pending, and no missing result is counted as success. The whole-project goal is
  still active; the separate documentation branch remains uncommitted/unpushed.

## Service composition, local/remote proxy, and HTTP continuation

Nine more modules extend the reviewed set from 88 to 97: Core services, all four
proxy modules, both HTTP transport modules, the existing HTTP test module, and a
new focused proxy-docstring/behavior test module. New source-derived documentation
records the following contracts without changing their implementation:

- Service construction can prepare a cache before later dependency validation
  fails; there is no compensating cleanup. Bindings check database object identity,
  with different fallback rules for Catalog/cache and read sources. Lazy access is
  not prevented by the close flag. Field metadata refresh discards even an injected
  object, and failed custom-field population can leave partial metadata cached.
- Reconciliation wraps cache-reload errors after a canonical write, not every
  later status/generation read. Close marks the service closed before cleanup;
  one failure prevents later cleanup and future calls do not retry. Borrowed caches
  kept by policy are not unbound from storage. Description may resolve application
  preferences even while leaving other service facades lazy.
- Jobs state normalization preserves scalar strings unchanged but strips/lowercases,
  deduplicates, and sorts collections. Local job proxies require mapping results;
  remote counterparts flatten successful non-dictionary results to empty dictionaries.
  Neither shape handling proves a missing job, successful cancellation, or termination.
- Local dynamic calls consume an optional `write` routing override. Remote dynamic
  calls use the naming heuristic and forward a `write` keyword as target data.
  Generated callables are not method-existence probes and are not cached on proxies.
  Subclass command/query overrides remain active in remote dynamic dispatch.
- Remote request docs distinguish pre-request JSON/construction failures from
  wrapped HTTP/read failures and decoded unsuccessful envelopes. Requests add no
  response-size cap, total wall-clock deadline, or general retry policy. Identity
  caches are not automatically invalidated when a server changes. Subscription
  threads retry HTTP errors, drop malformed events/callback failures, can advance
  cursors before successful delivery, and are not automatically stopped by shutdown.
- HTTP namespaces are routing prefixes, not access control; the adapter provides
  no TLS/authentication layer. Request limits validate declared length, not actual
  bytes read or elapsed time. A partial startup can leave a server behind before
  running is marked true, after which the current stop guard does nothing. These
  limitations are documented, not silently repaired during the docstring migration.
- Event history retains only 1,000 records and no explicit gap marker. Polling
  scans without consuming records, waits at most once per call, and can return
  early on notifications; direct calls do not apply the HTTP timeout clamp.
  History/cursors survive transport restart. HTTP docs distinguish GET 500 error
  handling from RPC 400 handling and identify uncaught response-serialization errors.
- Existing HTTP tests now describe their exact assertions. The post-stop health
  test can raise while evaluating `daemon.health_url`, before any HTTP request;
  it does not independently prove that a previously captured URL is unreachable.
  No assertion was weakened, replaced, or removed.

### Generated runtime docstring exception to plain AST stripping

Local dynamic dispatch previously overwrote `_caller.__doc__` with a one-line
description, so documenting the nested source function alone would not satisfy
runtime consumers. The string template assigned in
`_LocalTargetProxy.__getattr__` now includes descriptive reST and an explicit
integration example. Its `.format(self._target, method_name)`, generated name,
and dispatch code are unchanged. Remote dispatch does not replace its nested
docstring, so its new source docstring already reaches the generated callable.

The normalizer's generic `executable_structure` guard has **not** been weakened.
For the full comparison against `edf6bf05`, the local proxy requires one narrow,
explicitly inspected exception: locate exactly one assignment to `_caller.__doc__`
directly inside `_LocalTargetProxy.__getattr__`, require a `.format(...)` call whose
receiver is a string literal, and normalize only that receiver literal in both
ASTs. The original literal is `Local proxy dispatcher for {}.{}(...)`; the new one
starts with a newline and `Dispatch the local target method {}.{} through the Core
invoke routes.` and contains Example/args/kwargs/return fields. Compare the complete
remaining AST after stripping literal docstring statements. Do not strip arbitrary
assignments, formatting arguments, or generated-name code to make this comparison pass.

`tests/core/test_proxy_docstrings.py` checks both actual generated callable docstrings,
parameter order against live signatures, nonempty return fields, and explicit
doctest skips. Its mocked behavior tests cover local write-override consumption,
remote keyword forwarding/name-based routing, unchanged generated names, and
the existing local/remote malformed-job-result difference. It is discovered by
the normal full test suite; no default quality scope was expanded for this file.

Verification (selections overlap):

- Service doctests: **17 passed, 2 skipped**. Shared job-contract doctests:
  **4 passed, 8 skipped**. Local proxy doctests: **26 passed, 8 skipped**.
  Remote proxy doctests: **17 passed, 22 skipped**.
- New proxy documentation/behavior tests plus their doctests: **8 passed**.
  The new test module is independently formatter/lint-clean.
- Permission-approved existing Core application, runtime phase-1, and HTTP phase-2
  suites: **31 passed**. This finished before the final HTTP prose, whose executable
  AST remains unchanged. HTTP transport doctests afterward: **9 passed, 10 skipped**.
  Named nested handler examples remain source-audited integration illustrations;
  module doctest discovery does not execute hidden nested definitions.
- Final documented HTTP tests, proxy tests, and their doctests with local test
  sockets enabled: **12 passed, 4 skipped**.
- All **97 modules, 156 classes, and 735 functions** pass strict audit and normalizer
  checks. All **88 edited production Python files** and **four edited existing test
  files** preserve implementation ASTs, with only the specifically verified runtime
  docstring-template normalization above. The data generator is separately identical
  to its submodule HEAD after ordinary docstring stripping.
- All nine newly reviewed files are formatter-clean. Additional Ruff comparison
  introduces no new findings: services 1, proxy initializer 1, jobs 2, local 5,
  remote 9, transport initializer 0, HTTP daemon 12, existing HTTP tests 3, and new
  proxy tests 0. Legacy outside-scope findings were not fixed or suppressed.
- Root and data-submodule diffs are whitespace-clean. Whole-project audit was
  refreshed to the current 2,722-module scope including the four-function new test
  module; its updated missing/incomplete counts are recorded above.
- Final full quality runner passed after the HTTP edits: 159 formatted files,
  456 annotation-covered modules, 221 dependency-protected modules, clean selected
  lint/complexity, 188 strict-mypy files, no production type errors, and all 37
  negative examples rejected by each checker. No numeric guard or quality scope
  changed. Final migration/public-documentation/developer-link tests: **38 passed**.
- Every test/audit/quality handle from this continuation has completed. No run is
  pending and no unobserved result is claimed. Documentation remains local and
  uncommitted; the whole-project goal is still active with substantial work left.

## Runtime, preferences, registration, and evacuation continuation

Sixteen more source-reviewed modules extend the ledger from 97 to 113. Eleven
are production modules; five are existing test modules. The runtime and its two
job/metadata test modules were edited in the preceding interrupted batch, but
their missing verification output was not counted as success. Both old handles
were missing and no pytest process remained; fresh runs now cover those edits.

Important source-derived contracts:

- Runtime: one handler lock serializes reads and writes, but event delivery and
  wire conversion occur outside it. Registration is incremental, introspection
  executes attribute access, generic invoke is not a read-only/security boundary,
  and shutdown/cleanup can fail after state has changed without retry. Metadata
  receipts distinguish authoritative writes from later reconciliation; job
  descriptions cover argument normalization, result/log previews, cancellation
  races, retry linkage, timeout sentinels, and job-manager ownership. All 70
  functions, including the nested unsubscribe closure, were read and documented.
- Preference handlers distinguish Mapping keys from stringified items-only keys,
  membership checks from get/default results, and successful setter calls from
  durable updates. Membership exceptions suppress deletion; scope aliases are
  not canonicalized in receipts. Explicit null keys become the text `None`.
- Endpoint initialization describes query-before-command provider order and
  partial registration on failure. Registrar protocols retain their ellipses;
  payload declarations remain descriptive metadata, not executable validators.
- Evacuation models distinguish selected Asset counts, source claim counts,
  policy-aware capacity, estimates, shortfalls, and attempt receipts. Planning
  trusts observed VERIFIED claims and probes availability without reserving
  capacity. Greedy selection checks its count after appending, so a zero request
  can select one destination; an executable doctest records this actual behavior.
- Execution rechecks current eligibility/topology, but compares capacity against
  the plan's earlier target count. It checks once per entry before removal, not
  atomically per deletion. Copy/removal receipts, uncaught lookup/projection errors,
  partial work, source-byte retention, and pre-entry budgets are distinguished.
  Envelope adapters document null-bound assertions, count caps, before/after
  replanning, and the fact that actions_applied includes failed receipts.
- Placement capacity uses per-dimension bucket totals, not a joint feasible-subset
  search. No-policy eligibility and policy-aware eligibility differ. Store
  resolution documents exact fallback exceptions: durable lookup is not attempted
  for every unavailable live facade, and ambiguous live names report Unknown Store.
- The five test modules document actual assertions, fake behavior, and nested
  helpers. In particular, mocked removal does not delete fixture claims, remote
  proxy request tests do not use a server, and cancellation tests allow execution
  races. Embedded worker-source strings and every existing assertion are unchanged.

Verification (selections overlap):

- Final current-batch production/test doctests, runtime job/metadata/proxy tests,
  evacuation/facade/ownership tests, and migration/public-documentation/link
  contracts: **158 passed, 85 explicitly skipped integration examples**. Hidden
  nested examples remain source-reviewed, not automatically executed by module
  doctest discovery. An initial Store lookup example omitted the runtime's database
  attribute and failed; the fake now includes it, and the final selection passes.
- All **113 modules, 171 classes, and 882 functions** pass strict audit and
  normalizer checks. All **99 edited production Python modules** and **nine edited
  existing test modules** retain baseline executable ASTs, with only the previously
  verified local-proxy runtime docstring-template exception. Data-generator AST
  still matches its own submodule HEAD.
- Full fake-runtime API introspection remains identical before/after the runtime
  and service edits: 83 commands, 93 queries, three targets; SHA-256 of sorted JSON
  with core_uuid omitted is
  `6c4dfd704980bd0e971291338880ad747a8ba4f7750db5fa4fcbcc07f168fa26`.
  This checks the advertised fake-runtime description, not every real subsystem.
- Full quality runner passes after all current source edits: 159 formatter-clean
  files, 456 annotated modules, 221 dependency-protected modules, clean selected
  lint/complexity, 188 strict-mypy files, no production type errors, and all 37
  negative examples rejected by each checker. No gate limit or scope changed.
- All sixteen newly reviewed files are independently formatter-clean with no new
  Ruff findings against the baseline. Existing outside-scope findings remain:
  runtime 41, job tests 1, proxy-job tests 2; the other thirteen files have none.
- Permission-approved runtime phase-1, application API, HTTP phase-2, program
  discovery/preferences, and storage integrity/evacuation/migration real-database
  selection: **33 passed**. These exercise maintained direct/RPC paths, not a live
  PostgreSQL server or every project test.
- Whole-project inventory is refreshed above; root and data-submodule diffs are
  whitespace-clean. Final documentation-link/public-boundary tests after updating
  the handoff: **23 passed**. All current test/audit/quality handles have completed; no
  unobserved result is counted as a pass and no verification job remains pending.

At that earlier checkpoint, the program facade was read but not added to the ledger. Its
ownership test requires each delegate body to contain exactly one Return, imposes
an end-minus-start span below ten per method, and limits the file to 250 lines; literal
docstrings violate that contract. The general owner tests likewise count physical
docstring lines toward 450/160 limits. A nonblocking question asks whether those
checks may ignore docstrings while retaining executable-code limits. It remains
unanswered; no guard changes were made. Other whole-project work remains available,
so this is not an overall blocker. The documentation branch is still local and
uncommitted/unpushed. The subsequent Core milestone documented the facade fully
and explicitly records the resulting unchanged-guard failures; this paragraph
retains the earlier checkpoint's decision, not the current documentation scope.

## Remaining work and next actions

Use the [bounded plan](../dev-docs/project-docstrings-completion-plan.md) and exact
work inventory for current scheduling; C033 is next after the completed thirty-module
batch. Stop for token-spend review, then resume only explicitly requested modules. The historical backlog below describes remaining subject areas,
not authorization to resume an unbounded pass:

1. Keep auditing each subsequent edited file with `--check`, comparing its
   executable AST against `edf6bf05`, and running the affected tests. Do not
   weaken owner-size or typing gates.
2. Save updated whole-project reports and record exact reviewed file sets here.
3. Continue through all remaining declarations. All sixty-nine terminal and
   fifty-seven Core source modules are now reviewed; do not repeat those batches.
   The terminal milestone includes all commands, row/catalog helpers, both ABCs,
   both composition roots, and the package entrypoint. Core's source milestone includes
   all services/providers/protocols and its compatibility facade, plus the separate
   application, database-semantics, storage-graph, and browse/acquisition APIs.
   The full application API regression module and its nested helpers are also reviewed.
   The full `surfaces/core.py` adapter and its focused documentation-contract
   tests are now reviewed too: session lifecycle, schema/page conversion, driver
   heuristics, legacy reads/writes, acquisition decoding, and all three overloads.
   The shared package root, host protocols, acquisition values, presentation
   primitives, and complete system-profile module are now reviewed too, alongside
   their import/compatibility suites and new isolated profile-boundary tests.
   Category generation, tag icons, disk thumbnails, and both image modules are
   now reviewed with edge contracts and documented image integration tests.
   The full formerly 793-line `read_model/api.py` and its initializer are now
   documented, together with all four existing read-model test modules and new
   focused documentation contracts. Do not repeat that completed source review.
   Shared catalogue/acquisition/OPDS implementations and their package initializers
   are now documented too, plus the acquisition/OPDS API tests and new compatibility
   edge contracts. The catalogue API and standalone-OPDS test modules are now
   documented, as are the full standalone OPDS, Calibre-style web, and generic
   read-only web packages, their launchers, and their main regression modules.
   The full `surfaces/api_readonly` package, its launcher/main regression module,
   and the shared direct/RPC surface acceptance harness are now reviewed too.
   The full read/write web package, launcher, and main regression module are now
   reviewed too. CLI composition, parser contracts, completion, and shared helpers
   are now reviewed alongside dependency and operational-family test modules.
   The SquashFS CLI compatibility facade, command/parser owners, main regression
   module, and Core/job commands are now reviewed too. Serving/capability commands,
   catalogue operations, workflow commands, and diagnostics are now reviewed in
   full as well. Configuration/initialization, ingest-run and storage-audit owners
   and the complete initialization/storage-audit regression modules are now reviewed
   too. Metadata/PostgreSQL CLI owners and their two complete regression modules
   are now reviewed as well. The storage compatibility facade, all 23 storage_commands
   modules, and the entire test_cli_storage.py module are now reviewed too. This
   completes all 47 CLI source modules, including ingest, all parser builders,
   and the Store wizard. The complete operator-hardening suite and
   ingest/mixed_application.py are now reviewed and documented as well.
   The ingest initializer, models.py, stores.py, and adding.py are now reviewed too,
   with complete test_store_ingest.py and legacy test_adding_api.py documentation.
   Remote HTML wrappers, both pipelines modules, and all seven sources modules
   are now reviewed, completing the ingest source package. The discovery pathology,
   native/wget registration, Library, and backend regression modules are documented
   in full too. All nine storage-side native/wget adapter/utility modules and all
   three storage ingest modules are now reviewed, with both complete local-ingest
   regression modules. All four storage reconciliation modules, their legacy
   Library export, and five additional registration/publication regression modules
   are now reviewed too. All eight raw driver API modules, storage utils exports,
   constants, driver operations, and workflow normalization are now reviewed too,
   together with the complete memory-driver, Store-redraft, and example-marker
   regression modules. The Store package exports, identity/configuration, lifecycle,
   facade, and ingest-source contracts are now documented too, along with all of
   utils/store.py and the full advanced-ingest API regression module. The remaining
   Store file primitives, conveniences, and complete driver-backed adapter are now
   reviewed too, completing all eight Store API modules. Configuration-factory and
   driver-error regression modules are documented in full. All six concrete
   storage/stores modules are now reviewed too, alongside complete encryption and
   nested backed-Store regression modules. Driver exports, shared error/validation
   helpers, complete filesystem/SQLite drivers, and all four single-file SQLite
   compatibility modules are now reviewed too, together with complete filesystem
   and SQLite compatibility regressions. The HTTP driver and its full regression
   module are now complete too, with the HTTP Store inventory parameter corrected.
   The S3 driver and its complete regression module are now documented too.
   The FTP driver, all three FTP compatibility modules, and their complete
   regression module are now documented and verified too. The rclone raw driver,
   all seven read-only/writable adapter modules, and both complete backend
   regressions are now documented and verified as well. Shared archive mechanics,
   the complete ZIP driver, all configured ZIP/TAR/RAR/7z adapters and their six
   plugin initializers, and the full local-archive regression module are now
   documented and verified too. Complete raw TAR and RAR drivers, the build-once
   RAR plugin and initializer, and its full regression module are now documented
   and verified as well. The raw 7z driver and its full regression module are now
   documented and verified too. The entire raw ISO reader, all three read-only ISO
   adapter modules, and its complete regression module are now documented and verified
   as well. The entire ISO writer, three writable adapter modules, and complete
   regression module/package initializer are now documented and verified too.
   The complete raw SquashFS driver, seven read-only/build plugin modules, all three
   adjacent regression modules, and manifest launcher are now documented and verified
   too. All seventeen raw-driver modules are complete. The twenty remaining local
   backend-plugin modules and six full adjacent regression modules are now reviewed
   too, completing all 65 backend-plugin source modules. Common storage API models,
   characteristics, shared/legacy errors, Location values, placement hints, and both
   package exports are now reviewed too, with complete hint and bound-Location tests.
   Manager routing, bound Location handles/factories/private protocols, and manager
   errors are now reviewed too, with the complete storage-manager API regression
   module and all nested fixtures. Store/operational models, API contracts, both
   implementation mixins, and the full registration/bootstrap regression module
   are now reviewed too. Asset identity/metadata, nominal IDs, compatibility exports,
   Replica/result models, catalogue/lifecycle API contracts, both implementation
   mixins, and the older manager-documentation test module are now complete too.
   Composite/resolution models, all three Composite/Item-link/retrieval contracts
   and implementation mixins, plus the full composition and StoreContainer SQLite
   regression modules are now complete too. Policy models/API, CRUD/assignment
   and shared policy support mixins, and the complete policy API regression module
   are now reviewed too. Derivation values/private validators, resolver/registry
   API and traversal conveniences, and the complete graph/registration/replay mixin
   are now reviewed too. Reconciliation/ingest APIs and complete implementation
   mixins with nested callbacks, plus the public model initializer, are now reviewed
   too. All 11 manager-model source modules are complete. Private request/state values,
   all internal contracts, and the complete shared support mixin are now reviewed too.
   Routing/composition/package exports, the public manager facade and historical
   repository exports, and all six persistence protocols are now reviewed too.
   Both database repository and unit-of-work adapters are now fully documented,
   completing all 22 storage_manager implementation modules. Public conveniences
   are now fully reviewed too, completing all 29 storage_manager_api modules.
   All twelve generic/backup/sealed-artifact workflow API modules are now complete,
   finishing all 65 storage API source modules. The concrete backup planner,
   SquashFS workflow, sealed-image provenance workflow, both package initializers,
   and all three adjacent regression modules are now fully documented too.
   The backup repository, artifact registry, prototype pipeline, and their three
   full regression modules are now complete too, finishing all six backup source
   and five backup test modules. Storage location/types compatibility, configuration
   translation, factory, container, file-status callbacks, and schema migrations are
   now complete too, together with the full backend-registry test module. The full
   backend registry, shared Unicode fixture/contract helpers, and four-backend
   matrix test module are now complete too. The application-facing store_manager,
   MiniDB helper, and full database reload regression module are now reviewed and
   documented too, completing all 206 storage source modules. All 22 Location
   tests/helpers, the workflow API2 test, manager example regressions, live-backend
   contracts, and live PostgreSQL manager tests are now documented too, completing
   all 85 storage test/helper modules. All 11 examples/storage scripts,
   examples/_example_utils.py, and all six catalog example/helper modules are now
   reviewed and documented too. The remaining conversion (five), Library (one),
   metadata (three), and utility (one) examples are now complete too, finishing all
   28 tracked example/helper modules. Catalog exports, common/API values/errors,
   facade protocols, and the concrete Catalog are now reviewed, with complete import
   and API-documentation test modules. Shared matching policy, exact-entity matching,
   all identity specifications, matcher composition, and both corresponding API
   modules are now reviewed too, with both matching regression suites. This covers
   29 of 113 Catalog source modules and 5 of 19 Catalog test modules, including all
   matching implementation/API files, base and WEMI repositories/contracts, and the
   complete repository-invariant regression module. Continue the remaining exact,
   Agent, identifier, title/note repositories and contracts, then remaining Catalog
   owners and tests.
   Example execution does not
   imply documentation of dependencies.
   Preserve headers and inspect runtime doc consumers in every subsequent batch.
   Preserve the documented FRBR legacy fingerprint/identity behavior as an explicit
   follow-up issue rather than silently changing execution during documentation.
   Prior Composite/resolution read-ahead
   is now completed work, as are the private helper/state/contract files. The
   application manager, MiniDB, and database reload tests are now complete manifest
   entries rather than earlier read-ahead excerpts.
   The Core HTTP transport was already completed in the Core batch; do not treat
   the earlier generic CLI/HTTP reminder as an instruction to repeat it.
   The other referenced dependency excerpts are not completed-file claims.
   Then continue through the rest of the source, scripts, examples, and tests;
   preserve the full project backlog rather than treating this order as a scope limit.
4. Preference/configuration modules remain in scope. `utils/config/config_base.py`
   executes `default_tweaks.py` and custom tweaks directly into dictionaries. Adding
   a module docstring creates an observable `__doc__` entry unless the consumer
   handles metadata. Inspect all consumers and add appropriate compatibility
   checks before documenting those data-like modules; AST equality alone is
   insufficient there.

The initializer regression's runtime-created `Args` fixture has an explicit
descriptive `__doc__` entry outside ordinary leading-docstring AST stripping.
Its narrowly checked exception removes only that literal entry from the exact
`type('Args', ...)` mapping before comparing the remaining AST with `edf6bf05`.
Runtime inspection and the original default-application assertions passed. This
joins the previously documented local-proxy template and vacuum-adapter metadata
exceptions; it does not authorize ignoring arbitrary executable dictionary fields.

Some code consumes docstrings at runtime (Core introspection, CLI descriptions,
translated formatter descriptions). AST equality does not prove those text
consumers unchanged; inspect them and test relevant output when editing their
sources. Do not rewrite parser/data literals as prose merely to satisfy an audit.
