# Storage and catalog example documentation — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project goal after the
[storage test checkpoint](project-docstrings-storage-tests-completion-2026-09-11.md).
Branch remains `codex/project-docstrings` at `edf6bf05`, without commit or push.
Existing worktree/data-submodule changes are preserved.

Read and documented all 11 storage scripts, their shared examples/_example_utils.py
helper, and all six catalog example/helper modules. All 18 files matched HEAD
before editing. Only leading literal documentation changes; executable statements,
assertions, arguments, parser strings, and existing interpreter headers are retained.

Newly reviewed: **18 modules, 1 class, 40 functions = 59 declarations**.
Cumulative: **606 modules, 805 classes, 6,816 functions = 8,227 declarations**.
The [manifest](project-docstrings-reviewed-files.txt) includes all 206 storage source
modules, 85 storage test/helper modules, 11 storage examples, six catalog examples,
and the shared example helper. This is not completion of the whole-project goal.

## Contracts clarified

- Raw filesystem/SQLite examples use UPSERT write sessions with expected size and
  digest. Their reports are distinct from race/atomicity assertions or an Asset
  catalogue. HTTP reads use the observed version when available, check an optional
  digest before saving, and overwrite local output without an atomic-write wrapper.
- Assimilation publishes persistent destination files with transient manager
  metadata. Its retrievable flag compares read length with recorded size. The
  manual and workflow examples instead pass a database-backed manager and retain
  catalogue/files; they do not reopen the catalogue to prove restart persistence.
  Placeholder EPUB/JPEG-labelled bytes demonstrate storage rather than format parsing.
- SquashFS adoption, Library registration, direct reconciliation, and bootstrap
  descriptions explain flags, exit policies, partial effects, and cleanup ownership.
  The mixed-ingest example delegates directly to the packaged CLI. Script help
  uses explicit parser strings, not module __doc__.
- The shared JSON helper is a lossy diagnostic renderer: bytes become a short hex
  preview; mapping-key coercion can collide; collections can be materialized before
  truncation; dataclass traversal precedes its depth cutoff; custom _max_depth is
  not forwarded recursively. Non-finite floats and unbounded repr text remain
  possible. ASCII escaping handles Unicode/surrogateescaped filenames.
- Catalog contexts describe temporary versus retained output, the non-atomic
  initial existence check, optional template copying without clearing seed records,
  and cleanup gaps before the try block or when database close raises. The session
  record does not extend the live Catalog's lifetime.
- Catalog demonstrations distinguish selected report observations from assertions:
  false reuse/rollback/transfer flags do not themselves change exit status. The
  mutation example catches any Exception in its rejection block; the matching and
  writer examples catch their narrower exceptions. Match projection preserves
  selected values rather than rescoring, and link-column suffix lookup takes the
  first matching column without enforcing uniqueness.

## Storage doctest recovery

The previous full tests/storage run terminated with exit 1 after a pytest internal
error while formatting an autouse-fixture skip; its original log/done are retained.
The old note's claim that it was still active was stale.

Local installed-pytest inspection plus collected item metadata proved the cause:
the doctest for the wrapped _require_live_storage_flag fixture has a None source
line; fixture setup sets the skip to use the item location, which the report writer
asserts is non-None. Other collected live-backend items have real line numbers.
The fixture's already-skipped illustration is now a plain reST Python code block.
All four actual test functions, fixture/skip behavior, and runtime code remain.
No pytest implementation, selection filter, test assertion, or owner guard changes.

Focused recovery: **11 skipped**, 3.89s, exit 0. No external live endpoints were
configured or exercised. The rclone option prose now correctly states a
2,097,152-character JSON token cap. All 26 storage-test ASTs still match HEAD;
Ruff remains **7 to 7**, and their strict audit/normalizer pass.

## Verification checkpoint

- Storage examples/helper: all 12 executable ASTs match HEAD; Ruff **48 to 48**,
  with no introduced finding. Existing interpreter headers are byte-identical.
  All 11 before/after --help stdout/stderr/status results are identical.
- Catalog examples/helper: all six executable ASTs match HEAD; Ruff **4 to 4**,
  with no introduced finding; existing headers are retained. All five before/after
  --help stdout/stderr/status results are identical.
- Final strict normalizer/audit passed for all **606 reviewed files**, including
  the prose qualification and moving a docstring before an existing explanatory
  comment. Final AST/header/Ruff checks also pass after those edits.
- Storage-batch quality runner passed, and migration/public-documentation/link
  contracts passed **38 tests**, 44.26s. Final quality with catalog edits also passed:
  159 formatted files, 456 annotated modules, 221 protected dependency modules,
  lint/complexity, both production type checkers, 188 strict-mypy files, and both
  sets of 37 invalid-call examples.
- Catalog doctests/documentation contracts: corrected run reports **7 passed,
  13 skipped**, 16.79s. The first run found a trailing space in an expected string;
  that space and misindented :raises fields were corrected. Original failure logs
  remain. A temporary AST/header checker also needed its exec namespace isolated
  before its successful rerun; this was a checker error, not a repository defect.
- All five catalog scripts executed as real subprocesses, returned zero, and
  printed parseable final JSON reports. Each reported temporary storage, and its
  database path was absent after process exit. Per-script outputs are retained.
- Full storage regression and final whitespace checks remain in progress at this
  intermediate checkpoint. Use observed terminal exits and durable .done files,
  not elapsed time alone.

Durable runs use `working-memory/test-results/` prefixes
`docstrings-storage-examples-2026-09-11-{regression,quality,contracts}` and
`docstrings-catalog-examples-2026-09-11-{regression,regression-corrected,quality,cli}`.
The full storage run includes all tests/storage, storage/helper doctests, and
tests/library/test_adding_unmanaged_disk_ingest.py. Catalog CLI logs are saved per
script as well as by the enclosing runner. Static reports use
`/tmp/liuxin-{storage,catalog}-examples-ast-lint-2026-09-11.json`,
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`, and
`/tmp/liuxin-docstring-current-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, with no
parse failures. Missing/blank documentation remains on **1,067 modules, 1,897
classes, 20,185 functions = 23,149 declarations**, down 27 from the storage-test
checkpoint. Overlapping findings: 3,551 delimiter layout, 9,717 examples, 2,777
parameter fields, 3,657 return fields, 3,151 empty parameter descriptions, 4,119
empty return descriptions, and 28 missing summaries.

## Next

Ten tracked example modules remain unreviewed: five conversion modules, the Library
facade example, three metadata examples, and the comments-to-HTML utility example.
Their inventory totals two classes and 50 functions; these are not completed-file
claims. Continue there, then across the remaining source/tests/scripts/inherited
and data-submodule Python. The temporary documentation writer now preserves module
headers and indents multiline return/exception fields, but still requires review.

Keep the whole-project scope, three narrow runtime-doc exceptions, all seven
unresolved ownership-guard failures, and legacy FRBR fingerprint follow-up in the
[main ledger](project-docstrings-2026-09-08.md). Do not change limits to obtain a pass.
