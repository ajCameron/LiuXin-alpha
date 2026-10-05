# Storage test documentation completion — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project goal after the
[application manager checkpoint](project-docstrings-application-storage-manager-2026-09-11.md).
Branch remains `codex/project-docstrings` at `edf6bf05`, without commit or push.
Existing worktree and data-submodule changes are preserved.

Read and documented all 22 Location test/helper modules, the complete workflow
API2 double/contract module, example subprocess regressions, opt-in live backend
reads, and the live PostgreSQL manager test with both nested callbacks. All 26
files matched HEAD before editing. Only leading literal docstrings change.

Newly completed: **26 modules, 3 classes, 114 functions = 143 declarations**.
Cumulative: **588 modules, 804 classes, 6,776 functions = 8,168 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) now covers all
**206 storage source modules and 85 storage test/helper modules**. This does not
complete the wider source/test/script/example/inherited/data-submodule goal.

## Contracts clarified

- Location is a passive UUID/exact-key value. Filesystem Stores/drivers own path
  parsing, prefix inventory, byte operations, capabilities, and containment checks.
  BoundLocation retains the original address and routes through the current manager.
  Fixture descriptions record startup, repeated startup during attachment, and the
  absence of return-only fixture teardown hooks.
- Historical path/glob/async test names are not claims of pathlib or native async
  APIs. Coroutine I/O remains synchronous on the event-loop thread. Independent
  objects/range reads use workers; the replacement-reader case only reads before
  and after a joined worker, without observing publication concurrently.
- Ordinary local move assertions do not exercise a raced source-version change.
  The cross-Store move rejects before copying when protected source deletion is
  unavailable. The stat/try_stat case checks absence and presence only. Symlink
  containment coverage is conditional on actual link creation and has no race test.
- Memory workflow reports synthesize staging/verification evidence from declared
  expectations without building an archive. Checkpoint restoration retains status;
  terminal failures stay terminal, and cancel can relabel already terminal progress.
  The repository stores references in local dictionaries, applies its own ID/filter
  heuristics, ignores registration/protection flags for presence keys, and retains
  those keys after workflow deletion. Its historical durability test name does not
  imply persistence across processes.
- Example tests run actual subprocesses and inspect selected JSON reports, files,
  logs, local archives, or downloaded bytes. The fixed example inventory receives
  syntax checks; selected non-storage categories receive --help checks. These are
  separate from full execution or documentation of every example script.
- Live backend tests require an opt-in flag and root/key settings. They inspect
  stat and a short read, not full payload identity; helper cleanup begins after
  construction/settings evaluation. The PostgreSQL case creates/drops a generated
  schema, runs two real workers, then injects a metadata failure and reopens objects
  in one process. It does not simulate an operating-system crash.

## Verification

- All 26 edited executable ASTs match HEAD after removing only leading literal
  docstrings. Assertions, fixture behavior, runtime constants, marks, aliases, and
  guard limits are unchanged. No runtime-doc exception is added. Ruff remains
  **7 to 7**, with no introduced finding.
- Strict structural audit and normalizer pass for all 26 files and the complete
  **588-file reviewed set**; no findings or parse errors.
- Full quality runner passed: 159 formatted files, 456 annotated modules, 221
  protected dependency modules, lint/complexity, both production type checkers,
  188 strict-mypy files, and 37 invalid calls rejected by each checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 38.34s.
- The initial full tests/storage/doctest run terminated with exit 1 at
  04:52:36 UTC, after pytest's internal skip-reporting assertion. It has no complete
  regression summary; the original log and .done are retained. The subsequent
  [example checkpoint](project-docstrings-storage-catalog-examples-2026-09-11.md)
  diagnoses the wrapped fixture's missing doctest source line and records recovery.
  Its illustration is now a plain Python code block; fixture/test behavior is
  unchanged. Focused recovery: **11 skipped**, 3.89s, exit 0. The rclone JSON token
  cap wording is corrected to characters. Both corrections pass the 26-file
  AST/Ruff comparison and strict audit/normalizer. Full regression recovery in the
  subsequent checkpoint completed with **1,244 passed, 883 skipped, 2 failed**:
  the known manager size guard and sandbox-denied loopback socket creation. The
  permission-approved HTTP rerun then passed (**1 passed**, 3.38s). The original
  broad run's exit-1 evidence is retained; no whole-project green run is claimed.

Durable results use
`working-memory/test-results/docstrings-storage-tests-completion-2026-09-11-{regression,quality,contracts}.{log,done}`.
Static reports: `/tmp/liuxin-docstring-storage-tests-completion-batch-2026-09-11.json`,
`/tmp/liuxin-storage-tests-completion-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank docs remain **1,069 modules, 1,897 classes, 20,210 functions
= 23,176 declarations**, down 116. Overlapping findings: 3,579 delimiter layout,
9,733 examples, 2,780 parameter fields, 3,670 return fields, 3,152 empty parameter
descriptions, 4,120 empty return descriptions, and 28 missing summaries.

## Next

The subsequent [example checkpoint](project-docstrings-storage-catalog-examples-2026-09-11.md)
completes all 11 scripts in examples/storage, examples/_example_utils.py, and all
six catalog example/helper modules. It adapts the temporary writer to preserve
module interpreter headers and verifies unchanged --help output. Follow that
checkpoint for the remaining conversion/Library/metadata/utility examples.

Continue the rest of the source, tests, scripts, examples, inherited code, and
tracked data-submodule Python afterward. Preserve the three narrow runtime-doc
exceptions, unresolved ownership guards, and legacy FRBR fingerprint follow-up in
the [main ledger](project-docstrings-2026-09-08.md). The whole-project goal stays active.
