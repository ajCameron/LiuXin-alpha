# Whole-repository architecture review — 2026-09-26

Maintained copy: [dev-docs/architecture-review-2026-09-26.md](../dev-docs/architecture-review-2026-09-26.md).
This note is the dated handoff record.

**Scope:** architecture and boundaries only: layering, ownership, lifecycle, and
enforcement. This was not a bug hunt, but the real defects it turned up are
listed.

**Evidence base**
- Source: the pushed `codex/project-docstrings` branch at `01674d15`.
- Method: eight parallel reviewers, one per subsystem. They used static
  analysis, AST scans and reading the code.
- Limits: no test suite was run. The local shell was unavailable, and pip
  installs were blocked for some reviewers.
- The headline claims were re-checked by hand against the source. The key files
  were also compared with the local working copy (see *Verification*). Line
  numbers refer to `01674d15`.

## Verdict

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

The team's own 2026-09-17 review
([top-level-architecture-review](../dev-docs/top-level-architecture-review.md))
already named lifecycle ownership, storage bootstrapping in `Database`,
metadata/catalog persistence, and configuration. This review confirms all four
with evidence and shows each is broader than stated.

## Priority actions

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
| 6 | One lifecycle owner (for example `open_library_services(config) -> ExitStack`): Database opened with `enable_storage_manager=False`, then StorageManager, cache and Catalog; close in reverse. Route CLI ingest, reconcile and workflow jobs through it | DB-1, CO-2, CO-7, ST-3 |
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

## Findings by subsystem

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

### core, library, ingest and jobs (CO)

| ID | Sev | Finding | Key evidence |
|---|---|---|---|
| CO-1 | H* | At `01674d15`, local Core composition fails on import: the `wget_html_readonly` plugin imports a deleted `.wget_utils`. **Already fixed in the local working copy**, which imports `ingest.sources.wget_utils`. The lesson stands: `Library` imports every optional acquisition backend at import time, and no gate checks that imports resolve | `library/library.py:20-28` → `ingest` → `storage/.../wget_html_readonly/__init__.py:17` |
| CO-2 | H | No lifecycle owner. `CoreServices.close` has no try/finally. A failed `create_core` leaves the maintenance thread running. `shutdown` does not take the handler lock | `core/services.py:636-652`; `core/runtime.py:571-599`; `core/factory.py:62` |
| CO-3 | H | `library/` is vestigial: only 5 of its 51 modules are reachable from core or surfaces. `Library` never imports Catalog. Evacuation, placement and dedupe live in `core/program_services`, so core is becoming the domain layer | `library/backend.py:34-57` (imports modules that don't exist) |
| CO-4 | H | The RPC trust boundary is unsafe. HTTP dispatches any registered name, including `invoke`. `transport_stable` only affects wire encoding (`runtime.py:933,1079`), not access. `_invoke` calls any attribute on library, database or storage. Wire options can set `rclone_exe`/`wget_exe`. There is no auth, no Content-Type check and no Origin check | `core/runtime.py:162-179,1476-1502`; `core/transport/http.py:394-407,605-686` |
| CO-5 | M-H | One global lock for every handler. `jobs.wait` and long synchronous operations (audit, reconcile, evacuate, backup) run under it. The remote client's 10 s timeout leaves the server holding the lock | `core/runtime.py:912,1067,2182-2205` |
| CO-6 | M | Facade sprawl: 176 endpoints from 6 installers with two registration styles. `storage.*` has three owners. Each program operation is declared four times. `core_client(runtime=…)` returns the privileged runtime | `core/program_endpoints/handlers.py` (1,671 lines of stubs) |
| CO-7 | M | Many composition roots besides `SurfaceCoreSession`: `ingest/mixed_application.py`, `surfaces/cli/storage_audit.py` (via `importlib`), `core/workflow_jobs.py`, reconcile, `remote_html` and backup. Their maintenance defaults differ | `ingest/mixed_application.py:351-369`; `surfaces/core.py:203` |
| CO-8 | M | Fragmented jobs and ingest. The durable `jobs/` package has no importers; the live system is an in-memory singleton in `utils/jobs`. Run state is persisted in four places. There are at least six ingest entry paths, and no live ingest path touches Catalog | `utils/jobs/__init__.py:17-18`; `jobs/repository.py` |
| CO-9 | L | The terminal's local job-manager fallback is dead, and would show the wrong jobs against a remote Core | `surfaces/terminal/browser.py:151-152` |

### storage (ST)

| ID | Sev | Finding | Key evidence |
|---|---|---|---|
| ST-1 | H | "Never delete originals" is a convention. `remove_replica(delete_bytes=True)` is the default and checks no mode, last-copy or backup policy. Public `router.delete` says it performs no loss-policy checks. CLI `storage file delete` needs only `--yes`. The only mode-aware guard lives in core evacuation. No test asserts that a delete is refused | `storage_manager/mixins/replicas.py:327-374`; `mixins/router.py:132-161`; `library/library.py:634-655` |
| ST-2 | H | `read_only` is dropped for managed, flat, calibre-like and sqlite stores: `_common()` forwards only name and UUID. An existing Calibre library marked read-only can still be deleted from | `storage/backend_registry.py:336-353,420-545` |
| ST-3 | M | StorageManager is not the sole writer of its tables. Reconcile writes the legacy `files`/`file_store_links`, and `core/storage_graph_api.py` does raw CRUD on asset and replica rows, bypassing the repository cache and revision counters | `reconcile/store_db_sync.py:332-376`; `core/storage_graph_api.py:1185-1215` |
| ST-4 | M | StorageManager is a god object: 38-class MRO, 14 bases, 130 public attributes, and 25 shared mutable fields. Durability works by swapping dicts for mapping adapters. `storage/utils/store.py` (498 lines) has no importers | `storage_manager/manager.py:47-62`; `_state.py:88-127` |
| ST-5 | M | Acquisition lives in storage. The HTML crawler backends subclass `ingest.sources` classes, which causes the storage↔ingest cycle (still present locally). There are two adoption engines | `wget_html_storage_backend.py:16,30`; `ingest/stores.py` |
| ST-6 | M | Squashfs backup cleanup can `rmtree` a caller-supplied staging directory | `backup/squashfs_backup_workflow.py:840-843` |
| ST-7 | L | Leftover legacy and alias modules: `location.py`, `storage_types.py`, `single_file.py`, and 11 alias files | — |

Good here: the driver → Store → registry layering; a single mechanical delete
chokepoint in `driver_backed_api.py:1008-1027`; `CREATE_ONLY` defaults
everywhere; and `database_repository.py` using only `db.macros`, with no SQL.

### catalog and metadata (CM)

| ID | Sev | Finding | Key evidence |
|---|---|---|---|
| CM-1 | H | A live mutation path bypasses Catalog. Core's `metadata.write` and the tag/series replace commands go to `metadata/write_workflows.py` and on to the 1,569-line `LiuXinWEMIMetadataWriter`. That writer does its own find-or-create, uses no transaction, and swallows exceptions. The legacy-mutation boundary test cannot see this path | `core/runtime.py:239-304,1669-1710`; `liuxin_wemi_metadata_writer.py:840-929` |
| CM-2 | H | Three incompatible normalisations write to the same columns. For "Sci-Fi & Fantasy": the writer's tag rule gives `sci-fi&fantasy`, the writer's title rule gives `sci`, and Catalog gives `sci fi and fantasy`. Catalog never derives `tag_phash`, which facets sort on | `databases/column_metadata.py:628-644`; `catalog/matching/entity_specs.py:39-76,149` |
| CM-3 | M-H | WEMI containers are active records: the hydrators (2,260 lines) call `db.*` directly, and the containers expose `write_to_database`. There are two read models for "metadata of item N": the hydrators and `catalog.retrieval` | `work_metadata_hydrator.py:40-578` |
| CM-4 | M | The frozen legacy tools (Add/Ensure/Apply/Intralinker, 4.9k lines) are eagerly built on every Catalog and advertised on `CatalogAPI`. Their protocols anchor the 121-module cycle | `catalog/catalog.py:239-249`; `metadata_tools_api/common.py:17` |
| CM-5 | M | At least seven representations of "a book's metadata". `LiuXinWEMIMetadata` subclasses the Calibre shape. Nothing owns the conversion from containers to `MetadataCandidate` | `library/library_metadata.py`; `caches/write/*` |
| CM-6 | M | The same concepts are implemented several times: `standardize.py` vs `standardization.py` (17 shared names, some with diverging bodies); ISBN handling in 6 places; identifier-scheme aliases in 7; language normalisation in 4; author sort in 4 | `metadata/standardize.py`; `metadata/standardization.py` |
| CM-7 | M | `catalog/field_metadata.py` (3k lines) is Calibre cache compatibility with only legacy users. It creates upward edges into catalog | `databases/custom_columns/custom_columns.py:30` |
| CM-8 | L-M | `metadata/web_sources` (11k lines) is infrastructure: its own HTTP stack, threads, the Calibre plugin base and JSONConfig | `web_sources/base.py:25,222` |
| CM-9 | L | Catalog's coordinated `MetadataWriter` calls `db.macros` directly, not through `LinkWriter` | `catalog/mutations/metadata_writer.py:127-169` |

Good here: the protocol-only `catalog/api`; lazy, read-only matchers with an
injected `MatchingPolicy`; the 41-line `catalog/write/host_api.py` leaf; and
modern catalog using no raw SQL.

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

### utils, customize and configuration (UT)

| ID | Sev | Finding | Key evidence |
|---|---|---|---|
| UT-1 | H | Import-time side effects in the foundation layer. `constants/paths.py:93-94` creates directories (in the current working directory for an installed wheel). `constants/__init__` creates `~/.config/calibre` on Linux. `preferences` rewrites its ini. `config_base` `exec`s `tweaks.py`. `startup_scripts` patches `sys.meta_path` and builtins | `utils/config/config_base.py:772-815` |
| UT-2 | H | Two configuration worlds with no bridge. Manifest and profile selection is clean, but it never reaches the prefs, tweaks or `config_dir`. `config_dir` has two values. 47 of 64 tweak keys also exist in `preferences`, and the same key is read from different stores | `surfaces/system_profile.py`; `constants/__init__.py:400-438` |
| UT-3 | H | `customize.ui` is live (core imports it lazily), is cyclic with `library`, and loads 803 modules and 140 plugins at import. The circular import silently no-ops `run_plugins_on_import` | `customize/ui.py:47,83,1171`; `library/caches/utils.py:14-50` |
| UT-4 | M | `customize` holds six unrelated responsibilities, and it is one of about eight plugin mechanisms. The typed registries are the modern seams | — |
| UT-5 | M | About 30 device-driver builtins reference a deleted `devices` package, and an unguarded import kills the whole module. `sorted(_initialized_plugins, …)` discards its result, so priority ordering never applies (also true in the local copy) | `customize/ui.py:1165`; `customize/builtins/device_drivers.py:451` |
| UT-6 | M | `utils` is a junk drawer with upward edges into metadata, file_formats, databases and preferences, and domain code in `calibre_compat`. Author sort is implemented four times, one with an undefined `tweaks` | `utils/language_tools/lx_name_manip.py:501` |
| UT-7 | M | Vendored libraries: real forks (html5lib, calibre_zipfile) sit next to stale copies (dateutil 1.5 from 2010, BeautifulSoup 3.0.5) and unused ones. There are six `* (N).py` copy artifacts, 2.9k lines, shipped in the wheel | `utils/libraries/` |
| UT-8 | L | `utils/plugins/linux/images_rc.py` is a 7.5 MB, 114k-line PyQt5 resource blob with no importers | — |
| UT-9 | L | The error hierarchy is split. `errors.ImportError` shadows the builtin. There are two unrelated `StorageError`s. `CoreError` sits outside the root | `errors.py:58` |
| UT-10 | L | `startup_scripts` is dead but still has live side effects through `ptempfiles` | — |

### repository, packaging, tests and enforcement (RP)

| ID | Sev | Finding | Key evidence |
|---|---|---|---|
| RP-1 | H | The layer direction is enforced only for surfaces. The cycle checker keeps an edge only if its target is protected, so it cannot see cycles through unprotected modules. Widened to all modern packages, it finds a 121-module runtime SCC. Commit `3ac8fde5` got past the boundary test with `importlib` | `scripts/check_modern_import_cycles.py:207-216` |
| RP-2 | H | 48 files that `.gitignore` excludes are tracked, about 129 MB: `.tmp/` benchmark DBs (99 MB) and full-suite JSON reports in `working-memory/test-results` (31 MB). Also tracked: the root scraper SQLite with its `-wal` and `-shm` files, `extensions_source.zip`, `src/wget-log`, and prefs with instance UUIDs | `git ls-files -ci --exclude-standard` |
| RP-3 | M-H | The docstring campaign dominates churn: a 4.7 MB inventory keyed to per-file hashes that go stale on every functional edit. In the last two commits, 809 files changed only docstrings and 31 changed code. 68% of the queued declarations are in inherited, test or dead code, even though the quality-gates doc calls raw docstring totals "not a sensible pass/fail target" | `dev-docs/project-docstrings-work-units.json` |
| RP-4 | M | Two test trees. `src/LiuXin_tests` (20k lines) is a dead near-duplicate of `tests/support/test_databases` and imports `tests.support` backwards | — |
| RP-5 | M | Wheel contents: a top-level `past` package (collides with PyPI `future`), `images_rc.py`, the `(N).py` copies, vendored `setup.py`/doc/test files. PyQt5, apsw, wand and others are imported but not declared | `pyproject.toml` |
| RP-6 | M | Runtime state defaults to the checkout or the current working directory. Directories are created at import. Committed prefs carry per-instance UUIDs | `constants/paths.py:57-94` |
| RP-7 | M | CI gates are advisory (no branch protection as of 2026-09-07). The full job reruns the file-format lanes. `global_todo.md` and the CI doc disagree on what is done | `.github/workflows/tests-on-push.yml:186` |
| RP-8 | L-M | The test tree only partly mirrors `src`: cache tests under `tests/databases`; `library` has 5 test files for 27k lines; 1,194 private-attribute accesses in `tests/metadata` | — |

Good here: the ratchet design (named prefix scopes, three import contexts,
direction rules, self-tests, an honest scope statement), fresh-process import
tests, an explicitly scoped wheel with an install-and-convert gate, and the
repo-root hygiene guard in `tests/conftest.py`.

## Reference tables

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

## Verification

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
- **CM-1:** `metadata.write` is routed to `metadata.write_workflows`.
- **RP-1:** sample cycle edges confirmed.

Comparison with the local working copy (2026-09-26):

- `core/runtime.py`, `storage_manager/mixins/replicas.py`,
  `customize/builtins/conversion.py` and `file_formats/docx/images.py` are
  identical to `01674d15`.
- `customize/ui.py` differs only in import formatting; the sort bug remains.
- The wget plugin now imports `ingest.sources.wget_utils`, so **CO-1 is fixed
  locally**. The storage→ingest edge remains.

Not verified:

- Nothing was executed end to end against a real library.
- Line numbers may have drifted in the local copy.
- Reviewers' measured counts (SCC sizes, similarity ratios, unreachable-module
  totals) come from their own scripts and were not re-run.
- Branch protection was not checked.

## Next steps

- If the priority list is accepted, turn items 1–5 into their own dated notes as
  they are picked up.
- This review does not change any source file, documentation guide, or the
  docstring inventory.
