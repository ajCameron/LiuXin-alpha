# Configured Store foundations — 2026-09-10

Continuation of the [whole-project docstring pass](project-docstrings-2026-09-08.md),
following the now verified [raw driver batch](project-docstrings-driver-api-2026-09-10.md).
This goal turn made progress: it finished that existing 15-file batch and fully
reviewed/documented another seven files. The preceding orientation turn supplied
evidence identifying the unfinished batch and its next verification step. The
whole-project goal remains unfinished, without an overall blocker. Branch remains
`codex/project-docstrings` at `edf6bf05`; no commit or push was made.

## Completed review

- [Store package exports](../src/LiuXin_alpha/storage/api/store_api/__init__.py).
- [Identity and configuration contracts](../src/LiuXin_alpha/storage/api/store_api/identity_api.py).
- [Lifecycle contract](../src/LiuXin_alpha/storage/api/store_api/lifecycle_api.py).
- [Composed Store facade](../src/LiuXin_alpha/storage/api/store_api/facade_api.py).
- [Advanced ingest-source protocol and records](../src/LiuXin_alpha/storage/api/store_api/ingest_source_api.py).
- All [configured-Store utility operations](../src/LiuXin_alpha/storage/utils/store.py).
- Complete [ingest-source regression fixtures and cases](../tests/storage/api/test_ingest_source_api.py).

All private methods, constructors, enums, protocols, record fields, and module
introductions in these files are included. Batch: **7 modules, 17 classes,
63 functions = 87 declarations**. Cumulative: **342 modules, 396 classes,
3,791 functions = 4,529 declarations**, recorded in the
[exact reviewed manifest](project-docstrings-reviewed-files.txt).
All five storage utility modules are now reviewed. Five of eight configured
Store API modules are complete; file, convenience, and driver-backed APIs remain.

## Behavior clarified

- Store identity helpers compare configured UUIDs without independently parsing
  keys, checking existence, or type-checking the argument to owns_location.
  require_location returns the original owned object. locate handles a Location
  specially and otherwise delegates one token without coercion or URI detection.
- Default URI parsing and target allocation are unsupported; default URI
  rendering checks ownership and returns None. Allocation does not inherently
  reserve a name or publish bytes. Read-only configuration and backend capability
  are separate boundaries enforced by concrete Stores.
- available/writable return status fields without an extra probe or capability
  test. Store context entry returns self without startup; raw driver entry calls
  startup. Store exit always attempts close, returns None, and can replace a body
  exception if close fails.
- Ingest enum values describe consistency, delivery, resume, and metadata
  availability rather than implementing those guarantees. Profile construction
  normalizes enum values and stringified/trimmed/lowercased digest names, rejects
  empty/duplicate names, and rejects stable-range resume with unguarded reads.
  It does not establish algorithm support.
- Profile validation compares explicit consistency ranks and advertised digest
  algorithms only. It does not validate Store ownership, delivery, checkpoints,
  or bytes. Prepared values require a non-None version for version-pinned reads,
  distinct authoritative algorithms, and reject the exact empty provenance
  string. They do not validate URI syntax or redact credentials.
  Frozen records do not make nested values deeply validated or independent.
- Store utility reads omit the version keyword when None for compatible readers.
  read_bytes closes its stream and returns read() directly, without a separate
  bytes-type or memory-limit check. try_stat hides only StoreNotFound.
- Utility put validates chunk size/expected size before beginning a staged
  session, retries partial writes, and borrows the input stream. A falsey read
  result ends transfer before type checking. The session owns publication and
  verification; the helper returns commit metadata without checking its Location.
- Native digest capability without its protocol uses generic hashing; advertised
  native copy/move without their protocols raises instead. Invoked native
  failures do not trigger fallback. Generic hashing/copying opens unversioned
  streams and delegates size/digest verification to the destination session.
- Fallback move stats first and requires conditional deletion plus a non-None
  source version before copying. It uses streaming even if native copy exists,
  then deletes with the observed token. A later deletion failure leaves the
  published destination; no transaction spans the entire move.
- Test docs distinguish real filesystem/SQLite bytes, in-memory manager records,
  remote profile declarations, injected identity claims, and stream interruption.
  Resume tests verify four-byte staging, offset-four continuation and cleanup,
  rejection of staging/source changes before reopening, and strict error receipts.
  The interruption double is explicitly partial: short reads consume the actual
  count, and EOF before exhausting the prefix does not itself trigger its error.

## Verification

- Strict audit and normalizer pass on all seven files and all **342 reviewed
  files**. All seven executable ASTs match HEAD after stripping only leading
  literal docstrings; signatures, annotations, assertions, and behavior are
  unchanged. No additional runtime-doc exception was needed.
- Ruff code/message counts match baseline for all seven files: package exports,
  identity, facade, and the test module each retain one finding; utility operations
  retain two; lifecycle and ingest-source contracts have none. No gate or
  suppression changed.
- Batch doctests plus driver/manager API, Store-redraft, example-marker, and full
  Store-ingest regressions: **192 passed, 86 explicitly skipped examples**.
  Final review corrected prose about the private copy helper's required mode and
  caller-owned streams; no examples or executable statements changed afterward.
  The strict audit and AST/lint comparisons were repeated after those corrections.
- Full quality runner passed: 159 formatted files, 456 annotation-covered modules,
  221 protected dependency modules, production lint/complexity/types, 188 strict-mypy
  files, and 37 invalid examples rejected by each checker.
- Final migration, public-documentation, and developer-link contracts: **38
  passed**. All ten links in this handoff, eleven in the driver handoff, three
  in the main ledger, and 113 local index destinations resolve. Root and
  data-submodule diffs are whitespace-clean, as are these changed Markdown files.
  Every verification handle has completed; no run remains pending.
- The six prior physical-line/statement ownership-guard conflicts remain unchanged
  and were not rerun for this unrelated scope. No counting rule or ceiling changed.

Logs and exit metadata:
`working-memory/test-results/docstrings-store-foundation-2026-09-10-*`.
Static reports: `/tmp/liuxin-docstring-store-foundation-batch-2026-09-10.json`,
`/tmp/liuxin-store-foundation-ast-lint-2026-09-10.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

## Remaining work

The latest whole-project audit still covers **2,730 modules, 4,400 classes,
35,119 functions**, without parse failures. Missing/blank docs: **1,087 modules,
1,972 classes, 21,571 functions = 24,630 declarations**. Further overlapping
findings: 3,999 delimiter-layout, 10,310 missing-example, 2,967 parameter-field,
3,892 return-field, 3,996 empty parameter descriptions, 5,384 empty return
descriptions, and 28 missing summaries. The full goal is still substantially
unfinished; the completed modern areas do not define its scope.

Next fully read and document `storage/api/store_api/file_api.py`,
`convenience_api.py`, and `driver_backed_api.py`, then concrete Store/backend
implementations and remaining tests. Reads of the driver's ingest-preparation
methods, manager identified-stream signature, Digest model, and utility-related
manager regressions were dependency excerpts, not completed-file claims.
Continue through remaining source, tests, scripts, examples, inherited modules,
and tracked data-submodule Python. Preserve the three runtime-doc exceptions,
configuration exec-dictionary cautions, and six ownership conflicts in the main
ledger. The driver handoff retains the prior batch's independent verification.
