# Concrete Store implementations — 2026-09-10

Continuation of the [whole-project descriptive reST pass](project-docstrings-2026-09-08.md),
following [configured Store API completion](project-docstrings-store-completion-2026-09-10.md).
The preceding orientation turn revalidated the checkpoint but made no documentation
changes. This continuation makes verified progress on eight complete files. The
whole-project goal remains active and unfinished, without an overall blocker.
Branch remains `codex/project-docstrings` at `edf6bf05`; no commit or push was made.

## Completed review

- All six [concrete Store modules](../src/LiuXin_alpha/storage/stores): exports,
  filesystem, HTTP, SQLite, S3, and encryption, including private readers/sessions,
  key providers, layout records, validators, and metadata helpers.
- Complete [encryption regressions](../tests/storage/test_encrypted_storage.py),
  including both filesystem observation doubles and fixture helpers.
- Complete [nested backed-Store regression](../tests/storage/test_backed_store_recursion.py),
  including ZIP construction and real-database setup helpers.

Batch: **8 modules, 13 classes, 120 functions = 141 declarations**. Cumulative:
**355 modules, 424 classes, 4,021 functions = 4,800 declarations**. Exact paths
are in the [reviewed manifest](project-docstrings-reviewed-files.txt).
All six concrete Store modules now join the completed eight Store API modules,
eight raw driver API modules, and five shared storage utilities. Concrete drivers
and other storage API/model/manager families remain unfinished.

## Contracts clarified

- Filesystem construction resolves a root and builds an unstarted driver; SQLite
  construction explicitly starts its driver and may open/create the container.
  HTTP construction supplies runtime callbacks/headers without performing a probe.
  S3 construction can create an SDK client and local staging without running Store
  startup. The package initializer constructs none of these Stores.
- Supplied filesystem/S3 configurations retain identity and policy while a separate
  URL still selects physical storage. Constructors do not compare that URL with
  the retained configuration URI. Filesystem restoration uses default driver options;
  S3 restoration explicitly reconstructs S3BackendOptions from backend_options.
- Filesystem file-URI parsing rejects nonlocal authorities. SQLite parsing instead
  uses the decoded path and ignores the authority. HTTP root_path retains original
  configured URL text, and its name helper is lexical rather than a URL parser or
  redactor. HTTP request budgets treat nonpositive/unparseable values as disabled.
- S3 options are a frozen, unvalidated record. Created SDK clients are owned;
  injected clients are borrowed. Placement metadata writes use a fixed allowlist,
  omit None, retain false/zero/empty values, and JSON-encode without a separate byte
  budget. Reading accepts any liuxin-* JSON field without the write allowlist or
  schema validation, with later normalized duplicate names winning. Raw metadata
  remains visible. The old comment calling the structured projection trusted was
  corrected without changing executable code.
- Encryption headers carry sizes, chunk layout, nonce prefix, and key identity.
  Chunk associated data binds the complete header, physical inner key, and chunk
  index, but not the Store UUID. Native ciphertext copy/move/digest and external
  URI rendering are not advertised. Inherited copying transfers plaintext.
- Known-size encrypted sessions select the key and stage a header immediately,
  then encrypt chunks into inner staging. Unknown-size sessions stage plaintext
  locally, select the key at commit, and produce a second local ciphertext file.
  Large direct-write inputs can temporarily occupy more than one chunk of memory.
- Size/digest expectations use accepted-write accounting, not a reread of the
  plaintext staging file. Commit returns a plaintext SHA-256 with inner ciphertext
  timestamps/version. Cleanup can fail after publication or replace an earlier
  error. A failed local encryption can leave a ciphertext file whose path was not
  returned to commit; owned directory cleanup can later remove it. Explicit staging
  roots remain caller-owned. Abort cleanup errors can prevent later cleanup/state
  updates; outstanding sessions are not tracked or drained by Store close.
- Nonempty reads authenticate only the chunks they consume, through one contiguous
  ciphertext stream. Earlier plaintext can already have been returned when a later
  chunk fails. Header reads are separate requests and receive no automatic version
  pin unless the caller supplies one. Read does not independently compare the full
  ciphertext length with layout or authenticate unrequested chunks.
- **Header inspection is not authentication.** Stat checks header structure and
  reported ciphertext length without key lookup/tag authentication. Inventory reads
  headers without comparing ciphertext size. Empty selected reads return BytesIO
  before historical-key lookup or tag authentication, including empty objects.
  These existing behaviors are documented, not changed or claimed fixed.
- Key providers control actual new-write/historical-read keys independently of the
  configuration snapshot, whose key_id can remain stale after rotation. Static
  providers validate byte lengths but defer header identifier checks. Generated
  configuration stores identity, not raw key bytes. Staging directories are prepared
  even for read-only/direct-write use; existing directory modes are not rewritten.
- Allocation forwards plaintext size/digest hints unchanged and adds the physical
  prefix only later during I/O. No caller enumeration prefix means inner enumeration
  can inspect its whole namespace before filtering. Pages preserve inner tokens;
  a filtered empty page can still have a continuation cursor, without refill.
- Capability/status/characteristic projections retain inner limits and observations
  where stated rather than checking health or keys. Plaintext maximum size remains
  unknown because ciphertext overhead consumes inner capacity. Status details append
  rather than replace existing names; duplicate names are retained, not rejected by
  StoreStatus. Capacity and counts still describe the inner namespace.
- Encryption tests use real local ciphertext and fixed test keys, with selected
  tampering, rotation, metadata, range, ingest, and Unicode assertions. Empty-file
  tests do not establish empty-tag authentication. The backed-Store regression
  persists ZIP topology through real SQLite close/reopen in one process and checks
  cache materialization, backing, replica modes, and readable nested content.

## Verification

- Strict audit and normalizer pass on all eight files and all **355 reviewed files**.
- All eight executable ASTs equal HEAD after stripping only leading literal
  docstrings. The earlier two multiline protocol-ellipsis preparations are now
  documented. No signatures, annotations, assertions, runtime-doc exceptions,
  gate scopes, ceilings, or suppressions changed.
- Ruff findings match baseline: exports 1, filesystem 1, HTTP 0, SQLite 1, S3 1,
  encryption 2, encryption tests 1, backed-Store tests 1. No new findings.
- Batch doctests and encryption/backed-Store, HTTP/S3, raw-driver API, configuration,
  driver-error, advanced-ingest, Store-redraft, example-marker, placement-hint, and
  Store-ingest regressions: **312 passed, 158 explicitly skipped integration examples**.
  HTTP/S3 regression modules were executed, not completed documentation reviews.
- Full quality runner passed: 159 formatted files, 456 annotation-covered modules,
  221 protected dependency modules, selected lint/complexity/production typing,
  188 strict-mypy files, and 37 invalid examples rejected by each checker.
- Final source review corrected a skipped S3 example that tried to instantiate a
  type union, clarified that status detail names can repeat, and removed the misleading
  trusted-projection comment. Executable code and runnable examples stayed unchanged;
  structural and AST/lint checks passed again afterward.
- Migration, public-documentation, and developer-link contracts: **38 passed**.
- All six local links in this handoff, three in the main ledger, and 113 in the
  index resolve. Changed Markdown and root/data-submodule diffs are whitespace-clean.
  All verification handles have completed; no run remains pending.
- Six previously recorded ownership-test failures still conflict with physical
  docstring lines/statements. They were not rerun for this unrelated scope, and
  their counting policy and limits remain unchanged.

Logs and exit metadata:
`working-memory/test-results/docstrings-concrete-stores-2026-09-10-*`.
Static reports: `/tmp/liuxin-docstring-concrete-stores-batch-2026-09-10.json`,
`/tmp/liuxin-concrete-stores-ast-lint-2026-09-10.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

## Remaining work

Fresh whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, with
no parse failures. Missing/blank docs: **1,087 modules, 1,964 classes, 21,435
functions = 24,486 declarations**, 117 fewer than the preceding checkpoint.
Overlapping findings: 3,960 delimiter-layout, 10,297 missing-example, 2,960 parameter-field,
3,883 return-field, 3,938 empty parameter descriptions, 5,313 empty return descriptions,
and 28 missing summaries.

Next review concrete driver implementations and shared error/validation mechanics:
`storage/drivers/__init__.py`, `_errors.py`, `_validation.py`, filesystem, HTTP,
SQLite, S3, and their remaining regression modules, then archive/remote drivers.
Driver excerpts, StoreStatus validation, and placement-hint model/alias excerpts
were dependency checks, not additional completed-file claims. Continue through the
remaining manager/model APIs, tests, source, scripts, examples, inherited Python,
and tracked data-submodule Python. Retain the runtime-doc consumer cautions and
three narrowly verified metadata exceptions documented in the main ledger.
