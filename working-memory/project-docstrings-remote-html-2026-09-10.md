# Remote-HTML discovery and registration docstrings — 2026-09-10

Continuation of the [whole-project descriptive reST pass](project-docstrings-2026-09-08.md),
following [Store ingest and legacy discovery](project-docstrings-ingest-stores-2026-09-10.md).
The resumed checkout contained documentation edits in nine remote-HTML source
modules beyond the last recorded handoff. Reviewed their complete implementations,
finished the pipeline, and documented seven complete regression modules, including
all nested helpers and response/process doubles.

Branch remains `codex/project-docstrings`, at `edf6bf05`. This batch changes
documentation only; signatures, annotations, imports, executable statements,
test assertions, and gate policy retain their previous behavior. No commit,
push, or PR mutation was performed.

## Reviewed files

- [ingest/remote_html.py](../src/LiuXin_alpha/ingest/remote_html.py).
- Both [pipelines](../src/LiuXin_alpha/ingest/pipelines) modules: `__init__.py`
  and `remote_html.py`.
- All seven [sources](../src/LiuXin_alpha/ingest/sources) modules: `__init__.py`,
  `api.py`, `crawler_defaults.py`, `html_common.py`, `native_html.py`,
  `wget_html.py`, and `wget_utils.py`.
- [Discovery pathology tests](../tests/ingest/test_remote_discovery_pathologies.py).
- [Native registration tests](../tests/storage/reconcile/test_native_html_store_db_sync.py)
  and [wget registration tests](../tests/storage/reconcile/test_wget_html_store_db_sync.py).
- [Native Library tests](../tests/library/test_native_html_ingest_library.py)
  and [wget Library tests](../tests/library/test_wget_html_ingest_library.py).
- [Native backend tests](../tests/storage/store_backend_plugins/native_html_readonly/test_native_html_readonly_storage_backend.py)
  and [wget backend tests](../tests/storage/store_backend_plugins/wget_html_readonly/test_wget_html_readonly_storage_backend.py).

Batch: **17 modules, 20 classes, 189 functions = 226 AST declarations**.
Cumulative: **296 modules, 313 classes, 3,289 functions = 3,898 declarations**,
plus the previously documented native C and three runtime-doc metadata exceptions.
All **15 ingest source modules** are now reviewed. The exact cumulative file set
is saved in [project-docstrings-reviewed-files.txt](project-docstrings-reviewed-files.txt),
including the data-submodule generator. It matches the prior 279-file ledger plus
this batch and passes both documentation tools; it is not inferred completion
of untouched files elsewhere in the tree.

## Contracts made explicit

- Discovery returns candidate addresses, not verified ebooks, complete remote
  inventories, or authorization to fetch arbitrary network destinations. URL
  normalization is syntactic and component-specific; scope checks compare text.
  Other same-scheme authorities bypass the root-path restriction when enabled.
- Shared preference resolution distinguishes an absent modern key from explicit
  None, tries legacy keys in order, and falls back immediately after conversion
  errors. Options have limited construction validation and remain mutable.
- Native traversal is breadth-first. Page counts precede scope/robots checks;
  raw link occurrences count before deduplication. Fetch/parse errors can yield
  an empty cached result. Accepted file-like HTML links may also be traversed.
  Depth limits constrain descent, not already discovered links. Robots checks
  fail open on ordinary failures and their cached failures survive forced crawls.
- Native HEAD existence checks contact the supplied address before checking the
  final scope; GET fallback is limited to 405/501. Startup and existence checks
  have weaker body/type/truncation requirements than usable discovery pages.
- Wget caches successful filtered output, with duplicate occurrences across
  streamed lines counting toward the observation ceiling. User callback
  Exceptions are swallowed; internal limit failures abort. Prior callback
  effects and an old successful cache can survive an aborted forced crawl.
- Streamed wget capture checks character limits before retaining a chunk;
  nonstreamed capture checks after collecting both streams. Timer/cleanup policy
  targets the child, not its process group. Commands, environment, output, and
  error messages are not generally secret-scrubbed.
- Store setup persists a declaration before registration's file-schema/crawl
  checks. It uses a nonempty UUID only from the first matching row, not a search
  through later duplicates. Paths in database convenience helpers are lexical;
  constructor/body/context-exit errors propagate.
- Registration trusts discovery callbacks in incremental mode and ignores the
  returned URL list; deferred mode processes that list after discovery returns.
  Neither mode adds an all-run transaction or deletes absent files. Last matching
  duplicate keys win in the initial row map. Per-file failures are recorded,
  while setup/discovery failures escape without a completed report.
- Registration uses the raw derived key's POSIX suffix; observation counters use
  a separate decoded path/query heuristic. Lexical root stripping is not a scope
  check. Missing stat means zero size and unchecked integrity. The dormant hash
  helper can place a non-SHA-256 claim in the SHA-256 field; the live registration
  path disables that capture. No bytes are verified by these helpers.
- Volatile timestamps alone do not trigger a file update. Once another field
  changes, supported non-None timestamps, including acquired time, are assigned.
  None never clears old metadata. A link failure can follow an already counted
  file write. Manager bootstrap's return value is ignored; raised Exceptions
  enter the report after earlier writes. Callback consumers see the live report.
- Test docs identify which assertions use real catalogue rows, temporary bytes,
  in-memory manager metadata, or injected responses. Rate tests inspect arguments,
  timeout tests use a real timer against fake process state, and malformed-output
  tests can supply already-decoded surrogate text. Native database/Library tests
  inject page results but leave the separate robots lookup under its normal policy;
  they do not establish a successful live crawl or robots access.

## Verification

- Strict audit and normalizer pass for the 17-file batch and all **296 reviewed
  files**, with no structural findings or parse failures.
- All 17 executable ASTs match `HEAD` after stripping only leading literal
  docstrings. No runtime-doc exception was needed. Ruff code/message counts match
  baseline for every file; several remain outside the selected clean lint scope.
- Full `bash scripts/run_type_checks.sh`: **passed**. Scope remains 159 formatted
  files, 456 annotation-covered modules, 221 protected dependency modules, 188
  strict-mypy files, and 37 invalid examples rejected by each checker.
- Batch doctests and all seven regression modules, plus Store-ingest and storage
  CLI regressions: **139 passed, 106 explicitly skipped integration examples**,
  84.88 seconds. Nested local helper doctests are not automatically discovered;
  those examples were source-reviewed, and existing pytest cases exercise their
  helpers. No assertions or collection defaults were weakened.
- Migration/public-doc/link/ownership selection: **62 passed, 6 failed**,
  63.68 seconds. Failures remain the existing physical-line/statement guards for
  Core services, CLI storage owners, the Core facade, browser components, windowed
  components, and windowed_ui.py. This batch adds no owner-guard conflict and
  changes no counting logic or limit.
- Run logs and terminal `.done` metadata are in `working-memory/test-results/`
  under `docstrings-remote-html-2026-09-10-{regression,quality,contracts}`. The
  metadata records exact commands, start/finish times, exit codes, and log paths.
- Generated audits and AST/lint comparisons are under `/tmp/`; these are transient
  reports, not durable prerequisites for resuming. The reviewed-file manifest is
  durable so the explicit cumulative audit can be reconstructed after a restart.
- Markdown parsing resolved all **194 local link targets across 26 docstring
  notes**; all 296 manifest paths exist. Root and data-submodule diffs are
  whitespace-clean, staging is empty, and all verification processes completed.

No whole-project pytest, installed-wheel, remote CI, live PostgreSQL, live S3,
successful external crawl, or interactive UI result is claimed. The six ownership
failures remain visible alongside the passing behavioral and quality checks.

## Inventory and next work

Whole inventory: **2,730 modules, 4,400 classes, 35,119 functions**, no parse errors.
Missing/blank docs: **1,093 modules, 1,999 classes, 21,924 functions = 25,016
declarations**. Additional overlapping findings: 4,086 delimiter-layout,
10,349 missing-example, 3,006 parameter-field, 3,938 return-field, 4,056 empty-param,
5,484 empty-return, and 28 missing-summary. Reports:
`/tmp/liuxin-docstring-current-2026-09-10.json`,
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`,
`/tmp/liuxin-docstring-remote-html-batch-2026-09-10.json`, and
`/tmp/liuxin-remote-html-ast-lint-2026-09-10.json`.

Next bounded source batch: the storage-side native/wget adapter packages,
including the compatibility wget utility wrapper, followed by storage ingest
coordination and its remaining regression helpers. The backend implementations
were dependency-read here, not documented as completed source modules.
`storage/ingest/mixed_format.py` likewise remains unreviewed as a full module.
Continue through the remaining source/tests/scripts/examples/data-submodule
scope afterward. Preserve the configuration-exec and runtime-doc consumer
cautions in the main ledger. The whole-project task remains unfinished.
