# Whole-repository architecture review

Date: 2026-09-26

Status: Historical review with source-checked implementation follow-ups dated
2026-10-18 through 2026-10-20. The original findings and line references are retained below; the
dated status tables describe what is implemented and what remains.

This review assesses the whole repository against the architecture described in
[top-level structure](<../02 - Top Level Structure.md>) and
[responsibility boundaries](<../04 - Seperation of Concerns.md>). It follows up the
[2026-09-17 top-level architecture review](top-level-architecture-review.md).
Findings are review evidence, not confirmed regressions or proof that a
particular refactor is safe. The dated handoff is in the
[working-memory note](../../working-memory/code-review-2026-09-26.md).

**Scope:** architecture and boundaries only: layering, ownership, lifecycle, and
enforcement. This was not a bug hunt, but the real defects it turned up are
listed.

**Original evidence base (September review)**
- Source: the pushed `codex/project-docstrings` branch at `01674d15`.
- Method: eight parallel reviewers, one per subsystem. They used static
  analysis, AST scans and reading the code.
- Limits: no test suite was run, and pip installs were blocked for some
  reviewers.
- The headline claims were re-checked by hand against the source. The key files
  were also compared with the unpushed working copy of 2026-09-26 (see
  *Evidence and limits*). Line numbers refer to `01674d15`.

## Documentation and source follow-up — 2026-10-20

Reviewed this record and the architecture folder against the working tree based
on `fc5de7af`. This is a documentation/ownership review, not a repeat of the
original eight-subsystem defect audit. The 68 finding IDs and original evidence
remain intact; the October 18 status tables and October 19 amendments retain
their checkpoints. No current GitHub protection, PR or merge status was queried.

Concrete changes now reflected in the guides:

- Lifecycle ownership is implemented in `core.library_services.LibraryServices`.
  Startup unwind covers registered owned resources; constructor-internal cleanup
  and exception-sensitive transaction rollback are separate contracts. The old
  memory-SQLite Library cache factory is gone, while lower-layer path wrappers
  and Database opt-ins remain. See the updated [opening-path review](lifecycle-opening-path-review-2026-10-09.md).
- The supported write/normalization boundary, Catalog hydration, Core category
  policy and application settings consolidation remain implemented. This does
  not close every validator or read projection: the default SQL ray and Catalog
  bundles still select WEMI paths differently, as the
  [worked example](../databases/wemi-link-examples.md#default-sql-view-versus-catalog-bundles)
  shows. Raw writes and historical-data repair retain separate contracts.
- Plugin composition now lives in `plugins`, after the merger and rename.
  `customize` and `plugin_system` are absent. Shared loading and priority policy
  do not unify the durable jobs, utility workers, ingest coordinators or every
  domain registry. UT-3/4/5's remaining provider/interface work stays visible.
- The [quality guide](maintainability-quality-gates.md) now describes current
  scopes and runner order. The broad docstring campaign completed on October 5;
  the September pause is historical. Top-level developer guides and this folder
  distinguish current contracts, decisions, proposals and dated findings.

Fresh checks pass: **243 protected modules**; whole-package **1,792 modules,
15 reviewed backward edges and 28 unresolved dynamic imports**; all **25
cold-import budgets**. The largest domain/runtime/combined components remain
**9/217/217**, with Catalog/cache cross-package components **0/0**. This supersedes
October 18's module inventory, not its dated external observations. The quality
runner was inspected with `--dry-run`; no complete typing/lint or application
suite pass is claimed. See the [layer](layer-direction-ratchet.md) and
[import](import-footprint-ratchet.md) guides for measurement limits.

Remaining priorities still include CO-4/SU-6 transport capabilities, ST-1/2/6
storage safety, DB-1/2 transaction/creation semantics, CO-8 job/ingest ownership,
and the retained format/compatibility defects. Implementation work belongs in the
[cross-project queue](../global_todo.md) and linked subsystem plans. RP-2/RP-7's
artifact and remote protection/CI observations need a fresh dedicated check before
acting on their dated counts or publication instructions.

## Current assessment — 2026-10-18

This section is the **October 18 checkpoint**, followed by explicitly dated
amendments. It is not a current publication instruction.


Reviewed source: `ce239fd4` on `configuration-ownership`, including implementation
commit `d59407f3`. This documentation branch, `architecture-review-followup`, starts
there. **Implemented did not mean merged into main:** [#122][pr122] and
[#123][pr123] were still open at this checkpoint. #119–#121 had merged into
intermediate branches after #118 reached main; #122 carried those changes and
#123 added configuration ownership. The recommendation then was to merge #122
first and retarget #123. Check current branch/PR state before any publication action.

The original diagnosis was useful, but its present-tense headline is now too broad:

| September theme | Position at the reviewed checkpoint |
|---|---|
| Nobody owns lifecycle | `core.library_services.LibraryServices` now owns startup, rollback and teardown; `Library` borrows its collaborators and Database defaults to persistence-only construction. Legacy opt-ins, path wrappers and transaction-on-close behavior remain. |
| Layer direction is aspirational | Whole-package checks now include deferred and literal dynamic imports, alongside cold-import budgets. Policy, hydration and conversion contracts have moved to their owners. Fifteen reviewed backward edges and a 217-module runtime component remain; passing a ratchet does not mean acyclic architecture. |
| Several owners for one concept | Supported bibliographic writes and stored identities now converge on Catalog/schema-owned boundaries. Core owns category semantics, and #123 supplies one application settings store. Jobs, ingest coordination and overlapping registries remain open; unrelated component settings and per-library DBPrefs are intentionally distinct. |
| Safety by convention | **Still a priority.** HTTP dispatch, physical deletion policy and compatibility-backend `read_only` forwarding retain the concrete gaps described below. A01's 2026-10-20 follow-up implements private backup staging and ownership-checked cleanup for PR review; see ST-6. |
| Legacy code looks live | Proven dead definitions, copy artifacts, the unused Qt blob and broken Library facades were removed; the fixture caller migrated. Other compatibility code is live or directly tested. Import-graph unreachability alone is not evidence for deletion. |

Status vocabulary in the subsystem updates: **Resolved** closes the named
boundary/defect; **Partial** identifies completed work and an explicit remainder;
**Open** means the cited issue is still present or its proposed change is not
implemented. These are architectural assessments, not a claim of complete test
coverage. Old counts, percentages and source line numbers below remain historical
unless explicitly remeasured here.

### Concrete change trail

| Change | Commits and evidence |
|---|---|
| Library lifecycle | `472d71d0`; [opening-path review](lifecycle-opening-path-review-2026-10-09.md), [LibraryServices](../../src/LiuXin_alpha/core/library_services.py), [failure/teardown regressions](../../tests/core/test_library_services_lifecycle.py). |
| Pure metadata policy and Catalog boundaries | `2709f504`, `862576cb`, `9ff593a2`; [layer migration record](../../working-memory/layer-direction-ratchet-plan-2026-10-09.md), [Catalog write contract](../catalog/catalog-write-validation-contract.md). Hydration is now in `catalog.retrieval`, and container vocabularies are metadata-owned. |
| Canonical writes, identities and rollback | `5f085a13`, `18040e46`, `3a4934cc`, `f40881b6`; [write consolidation](../../working-memory/catalog-write-consolidation-2026-10-15.md), [ownership follow-up](../../working-memory/ownership-consolidation-2026-10-18.md), [real-backend parity](../../tests/catalog/test_write_boundary_parity.py). The later follow-up supersedes the earlier PostgreSQL-pending note: SQLite, APSW and live PostgreSQL were exercised. |
| Conversion/plugin containment | `f8723b96`, `f1bb7622`; [containment record](../../working-memory/file-formats-containment-2026-10-13.md), [native runtime regressions](../../tests/file_formats/test_plugin_runtime.py). |
| Dead code, broken facades and cache defects | `06bf503d`, `af2cebd0`; [reachability disposition](../../working-memory/unreachable-code-review-2026-10-16.md), [legacy fixes](../../working-memory/legacy-review-fixes-2026-10-17.md). The later retirement of `library.legacy`/`backend` supersedes their earlier retained disposition. |
| Connection cleanup and test storage pressure | `152991fa`, `5cc3d5f5`, `d70cabb8`, `819b0a67`; [telemetry and cleanup record](../../working-memory/test-vm-telemetry-2026-10-17.md). Auxiliary handles close, completed scratch is reclaimed, and durable telemetry survives cleanup. This supports the storage-pressure diagnosis without proving the original VM crash cause. |
| Category read semantics | `daa6a0ed`, [#121][pr121]; [read ownership record](../../working-memory/catalogue-read-ownership-2026-10-18.md), [direct/HTTP parity tests](../../tests/surfaces/test_category_read_ownership.py). |
| Application configuration | `d59407f3`, [#123][pr123]; [configuration contract](../core/configuration-ownership.md), [handoff](../../working-memory/configuration-ownership-2026-10-18.md), [profile/worker regressions](../../tests/preferences/test_configuration_ownership.py). |

### Plugin follow-up — 2026-10-19

Commits `6b97be2f` and `78a01b61` in
[PR #125](https://github.com/ajCameron/LiuXin-alpha/pull/125) make application startup
explicit/lazy, share provider membership and loading/priority policy, then
consolidate `customize` into `plugin_system`. The old namespace and forwarding
modules are removed. Neutral mechanics and standalone formats retain enforced
internal dependency boundaries. The
[plugin system guide](../plugins/README.md) and
[migration table](../plugins/plugin-composition.md#import-migration)
describe the resulting API.

This closes the package split and startup ownership work in UT-3/4/5. It does
not close unsupported device providers, retained cache/driver interfaces,
provider availability reporting or separate jobs/ingest registry work. The
September findings and October 13 compatibility-shim checkpoint below remain
historical evidence; those shims no longer exist.

The subsequent 2026-10-19 naming follow-up renames the consolidated package to
`plugins`, including its tests and documentation. Imports now use
`LiuXin_alpha.plugins`; `plugin_system` references in the dated findings below
identify the earlier checkpoint, not a supported compatibility namespace.

### Remaining work in priority order

1. Close CO-4/SU-6's transport and capability boundaries; implement ST-1/ST-2's
   deletion/read-only enforcement and ST-6's staging ownership. Test refusals at
   the actual public boundaries, not only higher-level evacuation workflows.
2. Finish DB-1/DB-2's low-level transaction/creation semantics and ST-3's storage
   mutation ownership. A resource context and a transaction context need explicit,
   different contracts. Remove the remaining lower-layer opening wrappers.
3. Continue the focused ownership sequence with jobs, ingest and plugin
   composition (CO-8, ST-5, UT-3/4/5), including restart, cancellation, remote logs
   and unsupported providers. The 2026-10-19 plugin follow-up consolidates startup
   and priority order. Configuration propagation through workers is not durable
   job-state ownership.
4. Address the concrete remaining format failures and conversion reporting
   (FF-3/4/7/8), then simplify retained metadata/cache compatibility boundaries.
5. Remove tracked generated artifacts, fix CI environment/baseline failures and
   enable required checks (RP-2/5/7). Keep documentation tied to behavior; do not
   restart a bulk inventory campaign merely to refresh hashes.

### Verification and limits of this follow-up

The source and commit checks were read-only. A fresh whole-package inventory
matches the checked-in ratchet: **1,798 modules, 15 backward edges, 28 unresolved
dynamic imports**; largest domain/runtime/combined SCCs are **9/217/217**. No SCC
contains both Catalog and caches. The narrower protected-cycle scope contains
**243 modules**, so the old 12% figure must not be presented as the scope of the
whole-package direction check. That check applies named direction rules, not a
complete ban on every possible cross-package dependency.

Git still tracks **47 ignored files, 134,661,257 bytes (128.4 MiB)**. The classic
GitHub protection endpoint reports `Branch not protected`, and effective rules
for `main` are empty as checked on 2026-10-18. These are freshly checked RP-2/RP-7
facts, not inherited September assumptions.

Earlier validation is attributed to its change record: 2,597 passing containment
cases; 236 distinct passing write/identity cases; 331 passing read-model cases;
943 passing configuration cases after the corrected follow-up. These selections
overlap and must not be added together. The configuration quality gate passed.
The integration PR's full run had 7,176 passes and 40 failures (eight missing
repo-local Ruff, 22 missing encryption dependency, ten existing lazy-export/image
backend failures). This is not a green full-suite claim. This documentation update
does not rerun the application suite or certify native ICU, optional integrations,
all missing imports, or every historical reachability/similarity count.

For this documentation change, all **25 existing documentation link/boundary
tests pass**. A separate check confirms that all **68 finding IDs** have exactly
one dated status row and all **53 relative document/source links** resolve.
The fresh graph/ignored-file inventory and test reports are retained in the main
checkout's ignored `working-memory/test-results/architecture-review-20261018/`.
No production source, existing inventory baseline or runtime policy was changed.

## Original verdict (2026-09-26)

The **modern seams are good**: portable macros, the typed plugin registries,
the Catalog repository/matcher/writer split, the Core command envelope, the
storage driver → Store → registry layering, and the ratchet tooling itself.

The **documented architecture is not what the code does**. Five themes explain
most of the findings.

1. **Nobody owns the lifecycle.**
   - `Database` is the de facto composition root: it starts the maintenance
     thread and builds the StorageManager.
   - There are at least six other composition roots.
   - `StorageManager.close()` is never called.
   - Importing the foundation packages creates directories and `exec`s a
     tweaks file.
2. **The layer direction is aspirational.**
   - Counting import-time and deferred edges, one strongly connected import
     component of **121 modules** spans caches, catalog, databases, metadata
     and surfaces. It includes every protected `catalog.api`/`catalog.write`
     module.
   - `databases` holds bibliographic policy.
   - `metadata` containers are active records.
   - The dependency ratchet covers **12% of modules**. The only layer rule
     (surfaces → core) has already been bypassed with `importlib`.
3. **One concept, several owners, divergent behaviour.** This applies to:
   - Write paths into bibliographic records: five live ones.
   - Normalisation: three incompatible rules written to the same `*_norm`
     columns.
   - Catalogue read models: two, which disagree on tag counts.
   - Configuration: two worlds.
   - Jobs, ingest and plugin registries: several of each.
4. **Safety invariants hold by convention, not by structure.**
   - "Never delete originals": the default `remove_replica` deletes bytes with
     no policy check, and `read_only` is dropped for four store kinds.
   - "Read-only" web surfaces receive a full-power client.
   - The Core HTTP transport will run `invoke` on any hosted object, with no
     authentication.
5. **Legacy Calibre code is interleaved with live code and looks live.** Tens
   of thousands of lines are unreachable or target tables that no longer exist.
   60 imports in `file_formats` point at modules that don't exist. Registration
   and import failures are swallowed.

The team's own [2026-09-17 review](top-level-architecture-review.md) already
named lifecycle ownership, storage bootstrapping in `Database`,
metadata/catalog persistence, and configuration. This review confirms all four
with evidence and shows each is broader than stated.

## Original priority actions, with current disposition

The proposals below are retained for traceability. Their current status follows
the tables; they are not fourteen untouched tasks.

### Now: small, high value, low risk

| # | Action | Findings |
|---|---|---|
| 1 | Close the Core RPC hole: reject `transport_stable=False` operations (e.g. `invoke`) at the HTTP layer; stop accepting executable paths and `database_path` from the wire; require `application/json` plus a token from the ready file | CO-4 |
| 2 | Put a deletion guard inside `remove_replica`: refuse UNMANAGED/ARCHIVE/BACKUP bytes, and refuse deleting the last verified replica, without an override token. Forward `read_only` in every backend builder. Add refusal tests | ST-1, ST-2 |
| 3 | Fix the silent defects found: LRF input registers `LITInput`; the plugin priority sort discards its result; DOCX EMF import is outside its `try`; `Database.__exit__` commits after an exception; squashfs backup can `rmtree` a caller's directory | FF-2, UT-5, FF-3, DB-1, ST-6 |
| 4 | Add an import-everything smoke test (plus `create_core(...).shutdown()`) and an unresolved-`LiuXin_alpha`-import ratchet (baseline 60 in `file_formats`) | CO-1, FF-3 |
| 5 | `git rm --cached` the 48 tracked-but-ignored files (~129 MB) and the root scraper outputs; add a CI check on `git ls-files -ci --exclude-standard` | RP-2 |

### Next: structural

| # | Action | Findings |
|---|---|---|
| 6 | Core owns lifecycle: a core `LibraryServices.open(settings)` built on an `ExitStack` opens the Database (without storage or maintenance), then StorageManager, cache and Catalog, builds the `Library`, and closes in reverse. Route CLI ingest, reconcile and workflow jobs through it. Target and rationale: [target architecture](target-architecture.md#1-lifecycle-ownership) | DB-1, CO-2, CO-7, ST-3 |
| 7 | A whole-package layer-rank check with a checked-in allowlist of today's ~25 backward edges; count `importlib` strings as edges; report SCC size as a ratchet number | RP-1, SU-3 |
| 8 | One write path: re-implement `metadata.write` on `catalog.write`/`LinkUpdate`; put `admin.row.*` behind an admin mode; one normalisation owner with a per-column parity test, plus a backfill | CM-1, CM-2, SU-2 |
| 9 | One read model: `core.browse_api` owns catalogue projections; `surfaces/read_model` only maps view data; hydrators move into `catalog/retrieval` | SU-1, CM-3 |
| 10 | Configuration: a resolved `RuntimeSettings` injected downward; lazy preferences; no `exec` of tweaks; XDG defaults for installed packages | UT-1, UT-2, RP-6 |

### Later: containment and simplification

| # | Action | Findings |
|---|---|---|
| 11 | Quarantine legacy code into a `legacy`/`calibre_compat` package with an import ban, excluded from the wheel. Candidates: MetadataSQL, `library/caches`, `library/{legacy,backend,restore}`, `customize/cache`, `field_metadata`, OEB polish/iterator, and the device-driver builtins | DB-4, CO-3, FF-8, CM-4, CM-7, UT-5 |
| 12 | Move the conversion plugin base classes and registry into `file_formats/conversion`; rename `customize` to say it is the Calibre plugin host | FF-1, UT-4 |
| 13 | Split StorageManager into constructor-injected services; fold `DriverWrapper` into `Database`; move `metadata/web_sources` out of `metadata`; put the web apps on a shared WSGI base rather than inheritance | ST-4, DB-5, CM-8, SU-5 |
| 14 | Rescope the docstring campaign to modern packages, and derive staleness from git instead of committed per-file hashes | RP-3 |

### Disposition of the original action list — 2026-10-18

| Action | Current disposition |
|---|---|
| 1 | **Open:** HTTP transport still forwards generic commands/queries, including `invoke`; wire encoding is not authorization. |
| 2 | **Open:** manager deletion lacks the proposed loss-policy guard; four compatibility builders still drop `read_only`. |
| 3 | **Partial:** LRF registration and the discarded plugin priority sort are fixed (the latter in the 2026-10-19 plugin follow-up). EMF import, exceptional Database exit and staging deletion remain. |
| 4 | **Partial:** lifecycle, installed-wheel, focused import and cold-budget checks exist. There is no demonstrated replacement for the proposed comprehensive unresolved-static-import inventory. Do not blindly import modules that execute code on import. |
| 5 | **Open:** tracked-ignored cleanup is unfinished (fresh count above). Test scratch cleanup and removal of dead source assets solve different problems. |
| 6 | **Implemented core boundary, partial migration:** `LibraryServices` owns the graph and failure unwind; maintenance job scheduling and removal of lower-layer path wrappers remain. Storage table ownership is separate. |
| 7 | **Implemented ratchets, remaining debt:** all package modules are scanned; literal dynamic/deferred edges and SCC measurements are checked. Reviewed exceptions remain explicit. |
| 8 | **Implemented supported writes/identity boundary:** Core admin CRUD and compatibility adapters route through Catalog; persisted keys use declared policy. Admin authorization remains open. Audit/repair is collision-aware; the user explicitly excluded an old-schema migration/backfill project. No user database was migrated. |
| 9 | **Partial:** hydrators moved to Catalog and category semantics moved to Core. Work/search/file/acquisition projections, server-side listing costs and some host coupling remain. |
| 10 | **Implemented application settings boundary in #123:** `ConfigurationPaths` plus one Preferences store fulfill the role without a class named RuntimeSettings. Startup hooks and non-configuration scratch defaults remain. |
| 11 | **Partial, revised approach:** proven dead code and unusable facades were retired, with wheel guards. Tested compatibility features were retained explicitly; wholesale quarantine/exclusion was not implemented. |
| 12 | **Implemented package/contract ownership:** conversion contracts remain in `file_formats`; PR #125 consolidates the application host and remaining families under `plugin_system` (renamed `plugins` on October 19), removes `customize` and its shims, and enforces internal dependency direction. Other domain registry work remains. |
| 13 | **Open:** StorageManager service decomposition, DriverWrapper folding, web acquisition relocation and shared web-app composition are not delivered by the owner moves. |
| 14 | **Superseded as a scheduling proposal:** the broad docstring campaign completed in `8919fd57`; it was not rescoped as proposed. Hash/inventory maintenance remains a tooling decision, not a prerequisite to functional work. |

## Findings by subsystem

The finding tables retain the September evidence and earlier dated amendments.
The following status tables were assessed at `ce239fd4` on October 18, with
explicit October 19 plugin amendments. Those amendments do not describe code
already present in `ce239fd4`. Read the latest follow-up above before treating a
status, line reference, PR observation or recommendation as current.

Severity: **H** (undermines the stated architecture or makes cross-subsystem
change unsafe), **M**, **L**.

### databases and caches (DB)

| ID | Sev | Finding | Key evidence |
|---|---|---|---|
| DB-1 | H | `Database` is the composition root. It defaults `enable_storage_manager=True` and `enable_maintenance=True`, builds the StorageManager, and never closes it. `__exit__` may commit after an exception. `create=True` on an existing path calls `direct_self_delete()` and backup is optional | `databases/database/__init__.py:222-296,755,860,1075-1099`; `databases/runtime.py:65,93` |
| DB-2 | H | No single owner of transactions. Legacy `direct_*` methods commit per call, MetadataSQL commits on the shared connection, and macros use savepoints on fresh connections. `DriverWrapper.lock` is a connection, not a lock | `driver_wrapper/__init__.py:87`; `portable_macros_mixin.py:171-229` |
| DB-3 | H | Bibliographic meaning lives in `databases`: 181 column roles and merge/display policies, WEMI identifier vocabularies and relator roles that `metadata` imports, and identity policy in `normalized_identities` | `databases/column_metadata.py:216-520`; `databases/db_types.py:289-520` |
| DB-4 | H | About 35k lines of orphaned Calibre-shaped code are still wired in. MetadataSQL (constructed for every `Database`) queries `books`/`titles`/`creators`, which the FRBR schema no longer creates. The legacy cache island has no production importer. The maintenance thread only knows the `creators` table | `database/__init__.py:681`; `metadata_sql/*`; `maintenance/builtin_plugins.py:150` |
| DB-5 | M | Four facade layers of mostly pass-through mixins: `Database` has 46 of 138 methods as single delegations, `DriverWrapper` 57 of 127. The SQLite and APSW drivers are 74% identical | `databases/api/` (13.4k lines of mirror protocols) |
| DB-6 | M | "Macros" is two dialects. The portable gateway is good but is a 2,576-line god class; 74 of the 102 macro names in use are non-portable legacy | `portable_macros_mixin.py` |
| DB-7 | M | Cache direction is correct (cache → Catalog → db), but `StorageCacheAPI` still contracts a second write path. `numpy_vectorized` reimplements rather than layering, contrary to doc 08 | `many_many_tables_api.py:513-598` |
| DB-8 | M | `databases` reads global preferences (title-sort, series-index tweaks) to compute stored values, while core uses per-library `DBPrefs` | `SQL/databasedriver/utils.py:583-607` |
| DB-9 | L | The composite schema-version string is hand-bumped across three layers, with its format arguments in the wrong order | `SQLite/__init__.py:20-26` |

#### DB status update — 2026-10-18

| ID | Status | Concrete change and remaining action |
|---|---|---|
| DB-1 | Partial | [LibraryServices](../../src/LiuXin_alpha/core/library_services.py) owns open/close and failure unwind; Database storage/maintenance defaults are off (`472d71d0`). Auxiliary connection cleanup was fixed in `152991fa`. **Still open:** `Database.__exit__` calls close without exception-sensitive rollback, and `create_new_database(blank=True)` can delete existing data. Separate lifecycle success from transaction/create safety. |
| DB-2 | Partial | Catalog mutations, Row adapters and Agent aggregates now share macro transaction ownership; late-failure rollback is tested on SQLite/APSW/PostgreSQL (`18040e46`, `f40881b6`). `DriverWrapper.lock` is still a connection, legacy direct APIs remain, and close can commit. Audit/remove those escape routes rather than claiming one universal transaction API. |
| DB-3 | Partial | Pure author/title sorting, identifier/language rules and WEMI/container vocabularies moved to metadata; matching policy moved to Catalog (`2709f504`, `9ff593a2`). [normalized_identities](../../src/LiuXin_alpha/databases/normalized_identities.py) now selects schema declarations and delegates text normalization. Column-role/merge/display declarations remain in databases: distinguish schema metadata from residual bibliographic policy before moving more. |
| DB-4 | Partial | Dead Library cache prototypes and broken facades were removed (`06bf503d`, `af2cebd0`). MetadataSQL is still instantiated by Database, legacy Calibre caches remain, and maintenance plugins still target `creators`. Map supported callers and migrate/retire these individually; the old 35k-line estimate is not a fresh inventory. |
| DB-5 | Open | Database/DriverWrapper facade layers remain. Duplicate definitions were removed, but that does not collapse delegation or unify SQLite/APSW implementations. Consolidate only with backend contract/parity coverage. |
| DB-6 | Open | Modern writes use the portable gateway, but legacy macro dialects and the large implementation remain. Inventory live legacy calls and migrate by capability; the September 74/102 count has not been rerun. |
| DB-7 | Partial | Modern cache writes delegate through Catalog's write boundary; this removes competing preparation policy. The StorageCacheAPI link-table `update_db` contract and separate NumPy implementations remain. Document or reduce those roles; do not equate canonical persistence with a unified cache architecture. |
| DB-8 | Partial | Author/title policy moved out of database utilities; #123 binds application preferences before bootstrap and dispatch, with profile-specific sort caches. DBPrefs remains intentionally library-local. Audit direct SQL-UDF and out-of-dispatch consumers for settings scope before claiming every global read is gone. |
| DB-9 | Open | `get_SQLite_driver_master_version` still preserves the swapped/omitted version fields; documentation now describes it explicitly. Replace or retire this compatibility string with an actual schema-version contract. |

### core, library, ingest and jobs (CO)

| ID | Sev | Finding | Key evidence |
|---|---|---|---|
| CO-1 | H* | At `01674d15`, local Core composition fails on import: the `wget_html_readonly` plugin imports a deleted `.wget_utils`. **Already fixed in the unpushed working copy**, which imports `ingest.sources.wget_utils`. The lesson stands: `Library` imports every optional acquisition backend at import time, and no gate checks that imports resolve | `library/library.py:20-28` → `ingest` → `storage/.../wget_html_readonly/__init__.py:17` |
| CO-2 | H | No lifecycle owner. `CoreServices.close` has no try/finally. A failed `create_core` leaves the maintenance thread running. `shutdown` does not take the handler lock | `core/services.py:636-652`; `core/runtime.py:571-599`; `core/factory.py:62` |
| CO-3 | H | `library/` is vestigial: only 5 of its 51 modules are reachable from core or surfaces. `Library` never imports Catalog. Evacuation, placement and dedupe live in `core/program_services`, so core is becoming the domain layer | `library/backend.py:34-57` (imports modules that don't exist) |
| CO-4 | H | The RPC trust boundary is unsafe. HTTP dispatches any registered name, including `invoke`. `transport_stable` only affects wire encoding (`runtime.py:933,1079`), not access. `_invoke` calls any attribute on library, database or storage. Wire options can set `rclone_exe`/`wget_exe`. There is no auth, no Content-Type check and no Origin check | `core/runtime.py:162-179,1476-1502`; `core/transport/http.py:394-407,605-686` |
| CO-5 | M-H | One global lock for every handler. `jobs.wait` and long synchronous operations (audit, reconcile, evacuate, backup) run under it. The remote client's 10 s timeout leaves the server holding the lock | `core/runtime.py:912,1067,2182-2205` |
| CO-6 | M | Facade sprawl: 176 endpoints from 6 installers with two registration styles. `storage.*` has three owners. Each program operation is declared four times. `core_client(runtime=…)` returns the privileged runtime | `core/program_endpoints/handlers.py` (1,671 lines of stubs) |
| CO-7 | M | Many composition roots besides `SurfaceCoreSession`: `ingest/mixed_application.py`, `surfaces/cli/storage_audit.py` (via `importlib`), `core/workflow_jobs.py`, reconcile, `remote_html` and backup. Their maintenance defaults differ | `ingest/mixed_application.py:351-369`; `surfaces/core.py:203` |
| CO-8 | M | Fragmented jobs and ingest. The durable `jobs/` package has no importers; the live system is an in-memory singleton in `utils/jobs`. Run state is persisted in four places. There are at least six ingest entry paths, and no live ingest path touches Catalog | `utils/jobs/__init__.py:17-18`; `jobs/repository.py` |
| CO-9 | L | The terminal's local job-manager fallback is dead, and would show the wrong jobs against a remote Core | `surfaces/terminal/browser.py:151-152` |

#### CO status update — 2026-10-18

| ID | Status | Concrete change and remaining action |
|---|---|---|
| CO-1 | Partial | The missing wget utility import was corrected, Library is an injected facade, and lifecycle plus installed-wheel checks exercise real Core opening. Optional acquisition/format handling is still not a comprehensive import-everything guarantee. Retain explicit availability reporting and add a scoped missing-import inventory. |
| CO-2 | Resolved | `472d71d0` introduced ExitStack-based graph ownership, compensating teardown, independent closer attempts and handler-locked shutdown. [Lifecycle regressions](../../tests/core/test_library_services_lifecycle.py) cover failed opens, close failures, maintenance and borrowed databases. Job draining and transaction-on-close are separate CO-8/DB-1 follow-ups. |
| CO-3 | Partial | The supported [Library facade](../../src/LiuXin_alpha/library/library.py) now receives Catalog/storage/cache; eleven dead modules and `legacy`/`backend`/archive importer were retired, with fixture migration and wheel rejection. Retained metadata/cache helpers and Core's domain workflow responsibilities still need deliberate disposition. |
| CO-4 | Open | [HTTP dispatch](../../src/LiuXin_alpha/core/transport/http.py) still forwards names/payloads to runtime; [runtime](../../src/LiuXin_alpha/core/runtime.py) registers `invoke` with only `transport_stable=False`. No request authentication/Origin/Content-Type enforcement closes this boundary. Restrict remotely callable capabilities and server-owned path/executable options; test denied requests. |
| CO-5 | Open | Runtime still holds `_command_lock` around every handler, including waits and synchronous work. Move long operations/waits out of the shared critical section and define timeout/cancellation semantics; adding settings contexts did not change concurrency. |
| CO-6 | Open | Multiple endpoint installers, storage surfaces and the privileged local runtime remain. Reconcile endpoint ownership and expose constrained clients; historical endpoint/stub counts need a new inventory before setting deletion targets. |
| CO-7 | Partial | The [opening-path migration](lifecycle-opening-path-review-2026-10-09.md) routes jobs/audit through LibraryServices and makes mixed ingest consume an injected Library. Remote-HTML/reconcile path wrappers still open via a deferred Core dependency. Retire those wrappers and finish resolved-settings propagation through direct opening APIs. |
| CO-8 | Open | Core still uses the in-memory `utils.jobs` manager alongside durable `jobs` repository/worker/scheduler contracts and several ingest coordinators. #123 propagates settings through threads/fork/spawn; it does not unify persistence, restart, cancellation or run state. This remains the next focused ownership programme. |
| CO-9 | Open | The terminal still constructs a fallback `default_job_manager`, and job-view fallback code remains. Remove or explicitly restrict it to local operation; remote job status/logs must come from the selected Core. |

### storage (ST)

| ID | Sev | Finding | Key evidence |
|---|---|---|---|
| ST-1 | H | "Never delete originals" is a convention. `remove_replica(delete_bytes=True)` is the default and checks no mode, last-copy or backup policy. Public `router.delete` says it performs no loss-policy checks. CLI `storage file delete` needs only `--yes`. The only mode-aware guard lives in core evacuation. No test asserts that a delete is refused | `storage_manager/mixins/replicas.py:327-374`; `mixins/router.py:132-161`; `library/library.py:634-655` |
| ST-2 | H | `read_only` is dropped for managed, flat, calibre-like and sqlite stores: `_common()` forwards only name and UUID. An existing Calibre library marked read-only can still be deleted from | `storage/backend_registry.py:336-353,420-545` |
| ST-3 | M | StorageManager is not the sole writer of its tables. Reconcile writes the legacy `files`/`file_store_links`, and `core/storage_graph_api.py` does raw CRUD on asset and replica rows, bypassing the repository cache and revision counters | `reconcile/store_db_sync.py:332-376`; `core/storage_graph_api.py:1185-1215` |
| ST-4 | M | StorageManager is a god object: 38-class MRO, 14 bases, 130 public attributes, and 25 shared mutable fields. Durability works by swapping dicts for mapping adapters. `storage/utils/store.py` (498 lines) has no importers | `storage_manager/manager.py:47-62`; `_state.py:88-127` |
| ST-5 | M | Acquisition lives in storage. The HTML crawler backends subclass `ingest.sources` classes, which causes the storage↔ingest cycle (still present in the working copy). There are two adoption engines | `wget_html_storage_backend.py:16,30`; `ingest/stores.py` |
| ST-6 | M | Squashfs backup cleanup can `rmtree` a caller-supplied staging directory | `backup/squashfs_backup_workflow.py:840-843` |
| ST-7 | L | Leftover legacy and alias modules: `location.py`, `storage_types.py`, `single_file.py`, and 11 alias files | — |

Good here: the driver → Store → registry layering; a single mechanical delete
chokepoint in `driver_backed_api.py:1008-1027`; `CREATE_ONLY` defaults
everywhere; and `database_repository.py` using only `db.macros`, with no SQL.

#### ST status update — 2026-10-18

| ID | Status | Concrete change and remaining action |
|---|---|---|
| ST-1 | Open | [remove_replica](../../src/LiuXin_alpha/storage/storage_manager/mixins/replicas.py) still defaults to deleting bytes and reaches [router.delete](../../src/LiuXin_alpha/storage/storage_manager/mixins/router.py) without mode/last-copy policy. Add one enforced loss-policy boundary with refusal tests across direct, Core and CLI paths. |
| ST-2 | Open | Implementation moved to [utils/backend_registry](../../src/LiuXin_alpha/storage/utils/backend_registry.py), but `_common` still forwards only name/UUID; managed, flat, Calibre-like and SQLite builders omit `read_only`. Forward and enforce it through each resulting Store, including delete/write refusal tests. |
| ST-3 | Open | Reconcile still creates legacy `files`/`file_store_links`, and Core storage-graph CRUD still uses database macros. Catalog bibliographic consolidation does not make StorageManager the storage-table writer. Choose the storage mutation owner and reconcile cache/revision/transaction guarantees across all adapters. |
| ST-4 | Open | Storage helpers moved and documentation improved, but the manager's mixin/shared-state design and durable mapping adapters remain; `storage/utils/store.py` still exists. Introduce cohesive injected services after mapping callers; old MRO/attribute counts are historical. |
| ST-5 | Open | Crawler backends still depend on acquisition code in `ingest.sources`; registration/adoption paths remain split. Move acquisition orchestration above storage and retain backend adapters for byte access. Lifecycle delegation alone does not remove this cycle. |
| ST-6 | Implemented for PR review, 2026-10-20 | [Squashfs backup](../../src/LiuXin_alpha/storage/backup/squashfs_backup_workflow.py) now allocates private workspaces and validates saved ownership before cleanup/recovery. Legacy staging remains borrowed and retained. [Regressions](../../tests/storage/backup/test_backup_staging_ownership.py) cover caller sentinels, failure/cancellation, persisted retries and ownership changes. |
| ST-7 | Partial | Some storage helpers/aliases moved; `storage.backend_registry` is now an export surface over `storage.utils.backend_registry`, not another registry implementation. Remaining aliases and `storage_types.py` require caller-by-caller retirement decisions, not deletion based on the old filename list. |

### catalog and metadata (CM)

| ID | Sev | Finding | Key evidence |
|---|---|---|---|
| CM-1 | H | **Resolved 2026-10-11.** Core's `metadata.write` now enters `catalog.mutations.writer.write_values`; the writer/report/workflow are Catalog-owned and the operation uses an available macro transaction. | `core/runtime.py`; `catalog/mutations/metadata_writer.py`; `catalog/mutations/container_writer.py` |
| CM-2 | H | Three incompatible normalisations write to the same columns. For "Sci-Fi & Fantasy": the writer's tag rule gives `sci-fi&fantasy`, the writer's title rule gives `sci`, and Catalog gives `sci fi and fantasy`. Catalog never derives `tag_phash`, which facets sort on | `databases/column_metadata.py:628-644`; `catalog/matching/entity_specs.py:39-76,149` |
| CM-3 | M-H | **Write half resolved 2026-10-11.** Metadata containers are passive and no longer expose `write_to_database`; persistence enters through Catalog. Hydrator/database reads and the duplicate read models remain migration debt. | `catalog/mutations/metadata_writer.py`; `metadata/containers/` |
| CM-4 | M | The frozen legacy tools (Add/Ensure/Apply/Intralinker, 4.9k lines) are eagerly built on every Catalog and advertised on `CatalogAPI`. Their protocols anchor the 121-module cycle | `catalog/catalog.py:239-249`; `metadata_tools_api/common.py:17` |
| CM-5 | M | At least seven representations of "a book's metadata". `LiuXinWEMIMetadata` subclasses the Calibre shape. Nothing owns the conversion from containers to `MetadataCandidate` | `library/library_metadata.py`; `caches/write/*` |
| CM-6 | M | The same concepts are implemented several times: `standardize.py` vs `standardization.py` (17 shared names, some with diverging bodies); ISBN handling in 6 places; identifier-scheme aliases in 7; language normalisation in 4; author sort in 4 | `metadata/standardize.py`; `metadata/standardization.py` |
| CM-7 | M | `catalog/field_metadata.py` (3k lines) is Calibre cache compatibility with only legacy users. It creates upward edges into catalog | `databases/custom_columns/custom_columns.py:30` |
| CM-8 | L-M | `metadata/web_sources` (11k lines) is infrastructure: its own HTTP stack, threads, the Calibre plugin base and JSONConfig | `web_sources/base.py:25,222` |
| CM-9 | L | Catalog's coordinated `MetadataWriter` calls `db.macros` directly, not through `LinkWriter` | `catalog/mutations/metadata_writer.py:127-169` |

Good here: the protocol-only `catalog/api`; lazy, read-only matchers with an
injected `MatchingPolicy`; the 41-line `catalog/write/host_api.py` leaf; and
modern catalog using no raw SQL.

#### CM status update — 2026-10-18

| ID | Status | Concrete change and remaining action |
|---|---|---|
| CM-1 | Resolved | Core `metadata.write` enters Catalog's mutation writer; container writes and compatibility paths were subsequently routed through shared repositories/relationships (`862576cb`, `18040e46`, `f40881b6`). Real-backend rollback tests cover failures after earlier attachments. Keep this boundary protected. |
| CM-2 | Resolved | [metadata.text_identity](../../src/LiuXin_alpha/metadata/text_identity.py) owns pure profiles; database declarations select the stored policy, including `tag_phash`. Repository, driver, Row, macro and surface lookup paths now agree (`5f085a13`, `f40881b6`). Matching heuristics and sort presentation stay separate. Current-schema audit/repair remains; no old-schema import/backfill project is authorized or claimed. |
| CM-3 | Resolved | The earlier write-only update is stale: containers are passive and hydration/read-source ownership also moved to [catalog.retrieval](../../src/LiuXin_alpha/catalog/retrieval/metadata.py) in `9ff593a2`. Category read duplication was addressed in `daa6a0ed`; broader projection/performance work belongs to SU-1. |
| CM-4 | Partial | Add/Ensure/Apply/Intralinker are still eagerly attached, but modern methods now adapt to `catalog.row_mutations`, repositories and relationships instead of independent sidecar persistence. Catalog/caches no longer share an SCC. Frozen SQL reference helpers remain explicitly tested; separate their retirement from supported convenience adapters. |
| CM-5 | Open | Moving persistence and hydration clarifies ownership without eliminating the multiple metadata shapes. Define container/candidate/receipt conversion contracts and supported representations; the seven-shape count has not been re-inventoried. |
| CM-6 | Partial | Author sort, title sort, language normalization and curated identifiers now have metadata owners (`2709f504`, `862576cb`); callers and tests moved with them. `standardize.py`/`standardization.py` and distinct legacy ISBN contracts remain. Consolidate only after specifying intentional behavior differences. |
| CM-7 | Open | `catalog.field_metadata` still creates reviewed upward dependencies from custom columns and metadata book records/codecs. Its consumers are not all dead. Extract a lower-level descriptor contract or migrate live users before removing the compatibility implementation. |
| CM-8 | Open | Web acquisition, worker/HTTP behavior and configuration remain in `metadata.web_sources`. Define a provider contract and move orchestration to the acquisition owner; the original line count is historical. |
| CM-9 | Open / decision needed | `catalog.mutations.metadata_writer` still performs some macro link operations directly. Shared `CatalogRelationships` now owns other paths; decide which coordinated operations should delegate there and verify equivalent validation/transaction semantics. Do not force high-level aggregates through LinkWriter solely to reduce an owner count. |

### surfaces (SU)

| ID | Sev | Finding | Key evidence |
|---|---|---|---|
| SU-1 | H | Two catalogue query layers disagree. `core.browse_api` (used by the CLI) and `surfaces.read_model` (used by web, OPDS and API) have different tags/labels semantics. There is a third policy in `metadata_facets`. The read model calls back into private methods of the HTML host and does N+1 remote queries | `core/browse_api.py:33-37,1193-1240`; `read_model/api.py:498-742` |
| SU-2 | H | There are four ways to reach data. `surfaces/core.py` spends about 800 lines re-creating the legacy `Database` API over Core. Web and terminal write through raw `admin.row.*`, bypassing catalog invariants the CLI respects | `surfaces/core.py:1349-2156`; `web_readwrite/app.py:3334-3944` |
| SU-3 | H | Paths that bypass Core are invisible to the gates. `liuxin storage ingest` opens Database and StorageManager in-process. `storage_audit.py` reaches `Library` via `importlib`. The terminal opens raw `sqlite3` | `surfaces/cli/storage_audit.py:91`; `scripts/check_modern_import_cycles.py:226,236` |
| SU-4 | M | The upward imports into surfaces are misplaced code or stubs: `surfaces/categories.py` is used only by dead library code; the `gui2` stub is used only by `file_formats` and returns `b""` | `library/legacy.py:27`; `metadata/book/base.py:963`; `surfaces/gui2/__init__.py` |
| SU-5 | M | The web app family is built by implementation inheritance from a 3,728-line class. The OPDS and Calibre apps share 23 identical method bodies. The `main()`s are 91–96% similar | `web_readonly/app.py:256-3983` |
| SU-6 | M | "Read-only" is naming only. Read-only apps receive a full `CoreClientAPI`, the read-only base issues `storage.refresh`, and there is no auth or authorisation seam. The loopback guard exists only in `cli/serve.py` | `web_readonly/app.py:3766-3787,4178-4190` |
| SU-7 | M | The terminal duplicates CLI operator features and reads job logs from the local filesystem, which breaks against a remote Core. Neither the terminal nor Tk has an installed entry point | `terminal/job_view.py:264-320` |
| SU-8 | L | Test-only or dead code, and permanent alias modules | `thumbnail_cache.py`; `cli/storage.py` |

#### SU status update — 2026-10-18

| ID | Status | Concrete change and remaining action |
|---|---|---|
| SU-1 | Partial | `daa6a0ed` makes Core own category source/count/order/membership; surfaces adapt receipts, preserve errors and page remote results. [Parity tests](../../tests/surfaces/test_category_read_ownership.py) cover tags/labels variants over direct and HTTP Core. Generic Work/search/file/acquisition projections, host callbacks, server-side materialization and non-atomic multi-read behavior remain outside that slice. |
| SU-2 | Partial | Core `admin.row.*`, generic forms and compatibility writes now route known bibliographic mutations through Catalog, closing the semantic validation bypass. The large Core-to-legacy facade remains, and admin authorization is not implemented. Preserve validated adapters while narrowing privileges and reducing obsolete API emulation. |
| SU-3 | Partial | Literal `importlib` calls are visible to the gates and CLI audit explicitly uses LibraryServices; mixed ingest consumes borrowed services. Terminal database commands still use raw `sqlite3`, and direct surface composition remains. Decide which operator actions are legitimate bootstrap boundaries and migrate the others to Core. |
| SU-4 | Partial | Format rendering/comments moved below surfaces and now emit real image data (`f1bb7622`); broken `library.legacy` is gone. `surfaces.categories` still has a legacy cache caller, and metadata book rendering retains upward edges in the baseline. Remove those consumers/edges deliberately. |
| SU-5 | Open | The read-only web base still underlies the application family by implementation inheritance. Shared category behavior reduces semantic duplication but is not a WSGI composition refactor; old method-similarity counts were not rerun. |
| SU-6 | Open | Read-only applications still accept `CoreClientAPI` and issue `storage.refresh`. Introduce a read capability and enforce server-side authorization; loopback defaults are not that boundary. Coordinate with CO-4. |
| SU-7 | Open | [Terminal job logs](../../src/LiuXin_alpha/surfaces/terminal/job_view.py) still read a local Path. Add a Core log retrieval contract and decide the intended installed terminal/Tk launch experience; shared CLI operator code is a separate simplification. |
| SU-8 | Partial | The retired `surfaces/cli/storage.py` facade now has an absence/owner guard (`e1308b65`); duplicate web definitions were removed. Retained thumbnail helpers and aliases need actual caller/contract evidence before further retirement. |

### file_formats (FF)

| ID | Sev | Finding | Key evidence |
|---|---|---|---|
| FF-1 | H | The conversion plugin seam belongs to `customize`. All 41 plugin modules subclass `customize.conversion`, whose package init imports `databases`. That makes every converter depend on the DB stack; with deferred edges, one 879-module component spans file_formats, metadata, databases, catalog and caches | `customize/__init__.py:30`; `customize/ui.py:47,83` |
| FF-2 | H | Registration hides failures and has a live bug. The LRF branch appends `LITInput`, so LRF input is never registered. Every registration swallows exceptions at DEBUG level. 15 test files fake `customize.ui`, so the real registry is barely tested | `customize/builtins/conversion.py:114-122` |
| FF-3 | H | 60 imports of modules that don't exist, 30 of them unguarded. DOCX (signed off) crashes on any EMF image. `--embed-font-family`, SNB images and `OPF()` construction also crash. RTF WMF images are silently dropped. Tests fake the missing modules | `file_formats/docx/images.py:149`; `oeb/transforms/flatcss.py:241`; `opf/__init__.py:697` |
| FF-4 | M-H | The `surfaces.gui2` imports (17 sites) are a lossy shim. `HTML2ZIP` is a registered on-import plugin whose call always raises and is swallowed, so HTML imports silently lose their linked resources | `html/to_zip.py:48-66`; `customize/ui.py:302-310` |
| FF-5 | M | Duplicated ownership: `opf/__init__.py` duplicates `opf2.py` "to kill an importerror"; `file_formats/__init__.py` and `utils.py` define the same 13 helpers | `opf/__init__.py:3-15` |
| FF-6 | M | The metadata↔file_formats cycle hinges on `metadata/utils.py` importing the 2.5k-line `oeb/base.py` just for `OPF()`, plus format readers calling back into `metadata.file_sources` | `metadata/utils.py:23-24` |
| FF-7 | M | `ConversionReport` loss reports never reach jobs or the API: `run_conversion_job` returns only the output size. The edge registry has no production callers | `core/workflow_jobs.py:436-460` |
| FF-8 | L-M | 107 modules (about 22.6k lines) unreachable: `docx/writer`, OEB polish/iterator, readability, cli. `InputFormatPlugin.__call__` chdirs and empties the working directory, which blocks concurrent conversion | `customize/conversion.py:295-299` |

Good here: the shared `archive_preflight`, the frozen `ConversionReport` and
edge types, sign-off discipline (e.g. the LIT writer fails loudly), and CI lanes
with an installed-wheel conversion smoke test.

**2026-10-13 update:** FF-6's specific `metadata.utils` → OEB/OPF edge is
resolved. OPF helpers now belong to `file_formats.opf.utils`, while the metadata
record factory belongs beside the record class. Focused behavior was
transplanted with the code. The largest runtime SCC fell from 280 to 220 and the
combined SCC from 311 to 220; the wider conversion/plugin findings remain open.

**2026-10-13 FF-1 update:** the reusable plugin lifecycle now belongs to
`plugin_system.base`, and conversion contracts belong to
`file_formats.conversion.plugin`. All built-in converters import those owners;
the historical `customize` modules are exact-identity compatibility surfaces.
Cold-import budgets forbid the contracts from re-entering composition or higher
application domains. This resolves the ownership edge described by FF-1 and
reduces the largest runtime and combined SCCs from 220 to 218. Remaining
format-to-UI/customization edges are tracked separately rather than being
disguised with dynamic imports.

#### FF status update — 2026-10-18

| ID | Status | Concrete change and remaining action |
|---|---|---|
| FF-1 | Resolved | `f8723b96` moved generic lifecycle to `plugin_system.base` and conversion contracts to `file_formats.conversion.plugin`; `f1bb7622` added standalone native assembly. Internal format callers no longer depend on the application host for these contracts; cold budgets and direction rules enforce it. Wider metadata/format cycles remain separate. |
| FF-2 | Partial | LRF now registers `LRFInput`, with a regression against duplicate LIT entries, and real native HTML-to-EPUB/plugin runtime tests exercise assembly. The 2026-10-19 follow-up shares file membership and loading/priority rules between hosts. Optional-provider failures can still be logged without structured availability reporting; that remains open. |
| FF-3 | Open | The missing DOCX EMF import remains outside its `try` in [images.py](../../src/LiuXin_alpha/file_formats/docx/images.py); the reviewed missing module is still absent. Containment and passing selected conversions do not certify the old 60-import list. Refresh that inventory, then fix/retire handlers with real format inputs rather than mocked missing modules. |
| FF-4 | Partial | Headless rendering replaced format-to-surface shims and PNG serialization is real. [HTML2ZIP.run](../../src/LiuXin_alpha/file_formats/html/to_zip.py) still raises that GUI conversion is unavailable. Implement resource-preserving headless import or explicitly disable/report this provider; Qt bootstrap remains separate. |
| FF-5 | Open | OPF utilities moved to the format owner, but `opf/__init__.py` and `opf2.py`, plus duplicated top-level helpers, remain. Choose canonical implementations and migrate imports with parser/writer compatibility tests. |
| FF-6 | Partial | The specific generic `metadata.utils` → OEB/OPF edge is removed (`9ff593a2`); helpers live in `file_formats.opf.utils`. Format readers and metadata providers still form a larger component (fresh whole-runtime SCC: 217). Define provider/record contracts before claiming the whole cycle resolved. |
| FF-7 | Open | `run_conversion_job` still returns paths, format labels, existence and `size_bytes`, not the ConversionReport loss receipt. Carry structured loss/unsupported-feature results through job/Core/API contracts and establish production use for the edge registry. |
| FF-8 | Partial | The reachability audit retired only proven dead code and retained formats/tools with direct tests or standalone entrypoints. The old 107-module count is not a safe deletion list. Input plugin execution still uses temporary process-CWD changes and cleanup; isolate concurrent conversions and verify caller-directory safety. |

### utils, customize and configuration (UT)

| ID | Sev | Finding | Key evidence |
|---|---|---|---|
| UT-1 | H | Import-time side effects in the foundation layer. `constants/paths.py:93-94` creates directories (in the current working directory for an installed wheel). `constants/__init__` creates `~/.config/calibre` on Linux. `preferences` rewrites its ini. `config_base` `exec`s `tweaks.py`. `startup_scripts` patches `sys.meta_path` and builtins | `utils/config/config_base.py:772-815` |
| UT-2 | H | Two configuration worlds with no bridge. Manifest and profile selection is clean, but it never reaches the prefs, tweaks or `config_dir`. `config_dir` has two values. 47 of 64 tweak keys also exist in `preferences`, and the same key is read from different stores | `surfaces/system_profile.py`; `constants/__init__.py:400-438` |
| UT-3 | H | `customize.ui` is live (core imports it lazily), is cyclic with `library`, and loads 803 modules and 140 plugins at import. The circular import silently no-ops `run_plugins_on_import` | `customize/ui.py:47,83,1171`; `library/caches/utils.py:14-50` |
| UT-4 | M | `customize` holds six unrelated responsibilities, and it is one of about eight plugin mechanisms. The typed registries are the modern seams | — |
| UT-5 | M | About 30 device-driver builtins reference a deleted `devices` package, and an unguarded import kills the whole module. `sorted(_initialized_plugins, …)` discards its result, so priority ordering never applies (also true in the working copy) | `customize/ui.py:1165`; `customize/builtins/device_drivers.py:451` |
| UT-6 | M | `utils` is a junk drawer with upward edges into metadata, file_formats, databases and preferences, and domain code in `calibre_compat`. Author sort is implemented four times, one with an undefined `tweaks` | `utils/language_tools/lx_name_manip.py:501` |
| UT-7 | M | Vendored libraries: real forks (html5lib, calibre_zipfile) sit next to stale copies (dateutil 1.5 from 2010, BeautifulSoup 3.0.5) and unused ones. There are six `* (N).py` copy artifacts, 2.9k lines, shipped in the wheel | `utils/libraries/` |
| UT-8 | L | `utils/plugins/linux/images_rc.py` is a 7.5 MB, 114k-line PyQt5 resource blob with no importers | — |
| UT-9 | L | The error hierarchy is split. `errors.ImportError` shadows the builtin. There are two unrelated `StorageError`s. `CoreError` sits outside the root | `errors.py:58` |
| UT-10 | L | `startup_scripts` is dead but still has live side effects through `ptempfiles` | — |

#### UT status update — 2026-10-18

| ID | Status | Concrete change and remaining action |
|---|---|---|
| UT-1 | Partial | #123 makes application preferences lazy, path resolution non-writing and tweak inputs literal-only; imports no longer load/execute application settings. `startup_scripts.__init__` still modifies `sys.meta_path`/builtins through `utils.ptempfiles`. Remove that remaining startup dependency rather than claiming all foundation imports are inert. |
| UT-2 | Resolved | [configuration.py](../../src/LiuXin_alpha/configuration.py), Core settings composition and the shared Preferences view replace conflicting application stores/paths (`d59407f3`). Manifest selection reaches bootstrap, dispatch, workers and sort caches; INI owns ordinary writes. DBPrefs and component namespaces remain distinct by design. Process workers use copy semantics; singleton component lifetimes are still documented limitations. |
| UT-3 | Implemented startup boundary | PR #125 makes application startup explicit/lazy in `plugins.application` (originally `plugin_system.application`), removes `customize.ui`, and keeps standalone conversion independent. Shared initialization/priority rules, cold-import budgets and module-level dependency tests enforce the new boundary. The old 803-module/140-plugin figure is historical. |
| UT-4 | Partial | PR #125 consolidates both packages under what is now `plugins`, migrates repository callers and removes forwarding shims. Generic mechanics and application assembly retain enforced internal layers. Retained cache/folder-driver modules and unsupported providers still need cleanup; database, storage and terminal registries have distinct domain ownership. |
| UT-5 | Partial | The 2026-10-19 follow-up applies stable descending priority through one shared loader, preserves installed-first ties and removes import-triggered startup. Conflict handling no longer recursively initializes or deletes installed archives. Tests cover ordering, both startup orders, interruption and concurrent/reentrant lookups. Device-driver builtins still reference the removed package and registry assembly catches their failure; retire or explicitly report unsupported devices. |
| UT-6 | Partial | Author/title/identifier/language policy moved into metadata, including the broken author-sort utility; file-hook and rendering responsibilities moved to their owners. Other upward utility and calibre_compat dependencies remain. Continue by responsibility, with caller migration and import-budget checks. |
| UT-7 | Partial | All six numbered copy artifacts were removed and wheel verification rejects them (`06bf503d`). Vendored forks and stale third-party copies remain; assess upstream replacement, local patches and licensing per library instead of treating them as one deletion batch. |
| UT-8 | Resolved | Unused 7,497,537-byte `images_rc.py` was deleted in `06bf503d`; installed-wheel verification rejects its return. This does not resolve the separate Qt/Pillow compatibility failures. |
| UT-9 | Open | The project error hierarchy and shadowing names were not consolidated. Define public exception families and compatibility/transport mapping before changing callers. |
| UT-10 | Open | `ptempfiles` still imports `startup_scripts.prefs_folder_manager`, executing the package initializer. It is not entirely dead while that edge remains; move the needed folder helper and then retire the hook-bearing package. |

### repository, packaging, tests and enforcement (RP)

| ID | Sev | Finding | Key evidence |
|---|---|---|---|
| RP-1 | H | **Mitigated 2026-10-12.** The original review found only surface enforcement and a 121-module domain SCC. The whole-package direction ratchet now records every known backward/dynamic edge; package-root isolation reduced the measured domain SCC to 48. Fresh-process budgets additionally catch implicit parent-package execution. Existing reviewed debt still requires migration. | `scripts/check_layer_direction.py`; `scripts/check_import_footprint.py`; `scripts/layer_direction_baseline.json` |
| RP-2 | H | 48 files that `.gitignore` excludes are tracked, about 129 MB: `.tmp/` benchmark DBs (99 MB) and full-suite JSON reports in `working-memory/test-results` (31 MB). Also tracked: the root scraper SQLite with its `-wal` and `-shm` files, `extensions_source.zip`, `src/wget-log`, and prefs with instance UUIDs | `git ls-files -ci --exclude-standard` |
| RP-3 | M-H | The docstring campaign dominates churn: a 4.7 MB inventory keyed to per-file hashes that go stale on every functional edit. In the last two commits, 809 files changed only docstrings and 31 changed code. 68% of the queued declarations are in inherited, test or dead code, even though the quality-gates doc calls raw docstring totals "not a sensible pass/fail target" | `dev-docs/hacking/project-docstrings-work-units.json` |
| RP-4 | M | Two test trees. `src/LiuXin_tests` (20k lines) is a dead near-duplicate of `tests/support/test_databases` and imports `tests.support` backwards | — |
| RP-5 | M | Wheel contents: a top-level `past` package (collides with PyPI `future`), `images_rc.py`, the `(N).py` copies, vendored `setup.py`/doc/test files. PyQt5, apsw, wand and others are imported but not declared | `pyproject.toml` |
| RP-6 | M | Runtime state defaults to the checkout or the current working directory. Directories are created at import. Committed prefs carry per-instance UUIDs | `constants/paths.py:57-94` |
| RP-7 | M | CI gates are advisory (no branch protection as of 2026-09-07). The full job reruns the file-format lanes. `global_todo.md` and the CI doc disagree on what is done | `.github/workflows/tests-on-push.yml:186` |
| RP-8 | L-M | The test tree only partly mirrors `src`: cache tests under `tests/databases`; `library` has 5 test files for 27k lines; 1,194 private-attribute accesses in `tests/metadata` | — |

Good here: the ratchet design (named prefix scopes, three import contexts,
direction rules, self-tests, an honest scope statement), fresh-process import
tests, an explicitly scoped wheel with an install-and-convert gate, and the
repo-root hygiene guard in `tests/conftest.py`.

#### RP status update — 2026-10-18

| ID | Status | Concrete change and remaining action |
|---|---|---|
| RP-1 | Partial | The whole-package direction ratchet, literal dynamic-import analysis, SCC ceilings and 23 cold-import budgets are implemented; fresh metrics are above. The 243-module protected acyclic scope is a different measure. Fifteen backward edges and 28 unresolved dynamic imports remain, and cache source-direction policy is still deliberately undecided. |
| RP-2 | Open | Fresh `git ls-files -ci --exclude-standard` plus object-size checks find 47 tracked ignored files (128.4 MiB). Telemetry cleanup reclaims runtime scratch but does not untrack these objects, scraper output or committed instance preferences. Review fixture/artifact ownership, remove generated tracked files and add the promised gate. History reclamation would be a separate decision. |
| RP-3 | Partial / superseded | `8919fd57` completed the broad campaign; the completion record reports 1,372 units, 2,671 files and 42,166 declarations. Functional PRs now update affected docs. The per-file hash inventory still exists; completion is not adoption of the proposed git-derived staleness scheme. Avoid a new blanket hash-refresh exercise. |
| RP-4 | Partial | The authoritative `test_db_6` fixture migrated off broken Library APIs, and its `src/LiuXin_tests` duplicate was deleted (`af2cebd0`). The rest of the legacy test tree still exists and remains wheel-excluded. Retire duplicates through the existing test-source-of-truth policy. |
| RP-5 | Partial | Wheel guards now reject numbered copies, the Qt blob and retired Library paths; fresh installed-wheel checks previously passed. `past*` is still packaged, optional dependency expectations remain uneven, and current CI lacks encryption/Ruff in the full-suite environment. Audit packaging/dependency contracts independently of successful smoke conversion. |
| RP-6 | Partial | Application settings now default to XDG/home locations with no import-time mkdir (#123). `constants.paths` still derives other scratch/debug folders from the base directory, which can fall back to CWD, and committed instance preferences remain. Finish operational-directory ownership without conflating it with application configuration. |
| RP-7 | Open at October 18 checkpoint | GitHub checks on October 18 confirmed no classic main protection and no effective branch rules. CI includes architecture, wheel, format and full-suite lanes, but #122's full run is not green. Correct dependency setup and known failures, reconcile gate documentation, then require appropriate checks; duplicate lane coverage is still a scheduling decision. |
| RP-8 | Open | Focused behavior coverage improved for lifecycle, caches, Catalog, read models and profiles. No tree-wide mirroring/private-access migration was done, and retired Library code makes the old coverage ratio obsolete. Prioritize public-boundary tests for live behavior rather than a cosmetic test-tree reshuffle. |

## Reference tables (September snapshot)

These tables describe `01674d15`, not the current tree. Fresh enforcement metrics
are in the current assessment above; the configuration table is superseded by
[the configuration ownership contract](../core/configuration-ownership.md).

### Architectural enforcement coverage

| Package | Modules | Cycle ratchet | Strict mypy |
|---|---:|---:|---:|
| file_formats | 456 | 0 | 0 |
| utils | 246 | 0 | 0 |
| metadata | 206 | 5 | 0 |
| storage | 199 | 0 | 81 |
| surfaces | 175 | 148 | 67 |
| databases | 174 | 0 | 0 |
| catalog | 113 | 53 | 0 |
| caches | 75 | 15 | 0 |
| core | 57 | 0 | 36 |
| library | 51 | 0 | 0 |
| customize | 23 | 0 | 0 |
| ingest, jobs, constants, other | 38 | 0 | 1 |
| **Total** | **1,813** | **221 (12%)** | **185 (10%)** |

The core program trees are covered separately by the workflow-ownership test.

### Configuration sources (summarised from UT-2)

| Source | Reaches |
|---|---|
| `--database` / `--core-endpoint` / `--system-root` / `--profile`, `LIUXIN_SYSTEM_ROOT` / `LIUXIN_PROFILE`, connect pointer, `liuxin-system.json` | Deployment selection only (`surfaces/system_profile.py`); clean precedence |
| `LIUXIN_BASE_DIR`, `LIUXIN_PREFS_DIR` | `constants/paths.py`, frozen at import; falls back to the current working directory |
| `LIUXIN_CONFIG_DIR` / `CALIBRE_CONFIG_DIRECTORY` | Two modules with conflicting defaults |
| `LiuXin_prefs_file.ini` (`preferences`) | databases, catalog, metadata, library, customize, utils; core as fallback |
| `calibre_config/*.json`, `tweaks.py` (exec'd) | Plugin enablement, conversion config, cache views; 47 keys overlap with the ini |
| `DBPrefs` | Per-library preferences via `core/services.py`; undocumented |
| About 35 other `LIUXIN_*` / `CALIBRE_*` env vars | Scattered, often read at import time; the README lists 4 |

## Original evidence and limits

These claims were re-checked by hand in `01674d15`:

- **CO-1:** the wget `__init__` imports the deleted `.wget_utils`.
- **CO-4:** `invoke` is registered with only `transport_stable=False`, which
  gates wire encoding, not access.
- **FF-2:** the LRF branch appends `LITInput`.
- **UT-5:** the result of `sorted(...)` is discarded.
- **RP-2:** 48 tracked, ignored files totalling 129 MB.
- **DB-1:** the `__exit__` docstring says pending work may commit; `create=True`
  calls `direct_self_delete`.
- **ST-1:** `delete_bytes=True` is the default, and `router.delete` performs no
  loss-policy check.
- **ST-2:** `_common()` forwards only name and UUID.
- **FF-3:** the EMF import sits outside the `try`.
- **UT-1:** `paths.py` runs `_makedirs` at import.
- **CM-1:** Resolved 2026-10-11: `metadata.write` is routed to the Catalog mutation writer.
- **RP-1:** sample cycle edges confirmed.

Comparison with the unpushed working copy (2026-09-26):

- `core/runtime.py`, `storage_manager/mixins/replicas.py`,
  `customize/builtins/conversion.py` and `file_formats/docx/images.py` are
  identical to `01674d15`.
- `customize/ui.py` differs only in import formatting; the sort bug remains.
- The wget plugin now imports `ingest.sources.wget_utils`, so **CO-1 is fixed
  there**. The storage→ingest edge remains.

Not verified:

- Nothing was executed end to end against a real library.
- Line numbers may have drifted in the working copy.
- Reviewers' measured counts (SCC sizes, similarity ratios, unreachable-module
  totals) come from their own scripts and were not re-run.
- Branch protection was not checked.

## Follow-up

Turn accepted priority actions into their own dated working-memory notes as
they are picked up, and update the owning subsystem guides when a boundary
actually changes. This review does not change any source file, subsystem guide
or the docstring inventory.

## Related documentation

- [Top-level structure](<../02 - Top Level Structure.md>)
- [Responsibility boundaries](<../04 - Seperation of Concerns.md>)
- [Top-level architecture review, 2026-09-17](top-level-architecture-review.md)
- [Target architecture](target-architecture.md)
- [Maintainability quality gates](maintainability-quality-gates.md)
- [Core workflow ownership](../core/core-program-workflows.md)
- [Storage component status](../storage_and_backup/storage_component_status.md)
- [Catalog legacy mutation migration](../catalog/catalog-legacy-mutation-migration.md)
- [Conversion pipeline sign-off](../conversion_pipeline/conversion_pipeline_signoff.md)
- [Working-memory handoff](../../working-memory/code-review-2026-09-26.md)

[pr121]: https://github.com/ajCameron/LiuXin-alpha/pull/121
[pr122]: https://github.com/ajCameron/LiuXin-alpha/pull/122
[pr123]: https://github.com/ajCameron/LiuXin-alpha/pull/123
