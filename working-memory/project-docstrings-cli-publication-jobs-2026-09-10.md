# CLI publication, daemon, and job docstrings — 2026-09-10

Continuation of the active [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following [CLI foundations](project-docstrings-cli-foundation-2026-09-10.md).
The preceding goal turn made verified progress. This turn checked clean target
paths and fully read the SquashFS facade (54 pre-migration lines), command owner
(317), parser owner (158), and regression module (397), followed by Core CLI
(212) and managed-job commands (253). All six files are documented throughout,
including four provenance types and nested cache/signal helpers.

The branch remains `codex/project-docstrings`, based on `edf6bf05`. Work is local
and uncommitted; no push, submodule publication, or maintainability PR change
accompanies this batch. Executable implementations and test assertions are unchanged.

## Reviewed contracts

- [squashfs.py](../src/LiuXin_alpha/surfaces/cli/squashfs.py): compatibility names
  remain direct aliases, including private helpers and the historical PostgreSQL
  parser attribute outside __all__. Main lazily forwards the complete CLI token
  list by identity; it does not prepend a squashfs command or build a separate CLI.
- [squashfs_parsers.py](../src/LiuXin_alpha/surfaces/cli/squashfs_parsers.py): three
  required subcommands, connection declarations, duplication/refresh defaults,
  and strict/report-error flags. Path/tool/codec/positive-ID validation, ID-list
  collection, and the provenance filter requirement are not parser guarantees.
- [squashfs_commands.py](../src/LiuXin_alpha/surfaces/cli/squashfs_commands.py):
  all nine functions and four TypedDicts now have complete contracts/examples.
  Types describe static shapes, not runtime validation or lineage guarantees.
  Provenance file projection requires all six column keys; explicit None values
  remain intact, while missing keys raise. Only file_id is integer-coerced.
- Publication uses named Core start commands, shallow-copied request data, and
  a dedicated runner rather than common.wait_for_job. Job IDs are stringified
  without stripping. Every state envelope is checked, but terminal-state spelling
  is exact, case-sensitive, and untrimmed. Nonterminal polls sleep 0.1 seconds
  without a local deadline or cancellation policy. Final result retrieval uses
  timeout_s=0.0, requires truthy execution.ok and a mapping result, and shallow-
  copies the result. Some malformed final envelopes raise ordinary attribute or
  conversion errors rather than the explicit RuntimeErrors for other shapes.
- Explicit IDs precede complete CLI-host UTF-8 list-file content; full-line
  comments/blanks are skipped, inline comments rejected by int conversion, and
  first-seen integer order retained without positivity/existence checks. Archive
  paths and publication flags are forwarded to Core. Reports print after session
  exit and before strict/report-error status projection. No adapter rollback is
  promised for accepted jobs, reports, or output failures. Text report counts
  omit detailed errors and may partly print before a malformed duration fails;
  JSON uses Unicode/sorted-key direct printing, not common atomic file output.
- Provenance requires the derivation table before checking selectors, reads all
  derivations, resolves both endpoints before filtering, and skips missing rows.
  Present file rows are cached; None entries cause repeated lookups. Store and
  file conditions are conjunctive but may match opposite endpoints. Original
  order and duplicate edges remain. Queries do not inspect archive bytes, infer
  derivations from replicas, validate cycles, or provide an atomic snapshot.
- [core_cli.py](../src/LiuXin_alpha/surfaces/cli/core_cli.py): health, capabilities,
  API projection, loopback classification, daemon lifecycle, parser declarations,
  and nested signal handling. Health defaults missing ok to success and projects
  status after output; capability/API commands do not interpret receipt status.
  Loopback validation strips/lowercases without DNS, while binding retains the
  original host string. The unsafe override adds neither authentication nor TLS.
- Daemon serving checks bind/positive size before composition, requires an owned
  local runtime, and redirects stdout only during factory construction. Daemon
  construction precedes the cleanup try block, so construction failures are not
  covered by its session cleanup. Readiness JSON and optional ready file are
  emitted after start, before signal installation. Main-thread signal callbacks
  only set an event. Normal shutdown restores signals, stops the daemon, then
  closes the session; an earlier cleanup error can prevent later cleanup. MiB
  conversion has no separate finite-number validation. Parser serving requires
  an explicit local database and offers no endpoint selector.
- [jobs.py](../src/LiuXin_alpha/surfaces/cli/jobs.py): all twelve functions, including
  the parser builder, are documented, distinguishing one Core wait query from shared
  watch polling and result retrieval. Storage composition is disabled. Wait
  inspects only dict-shaped result/job state, lowercases without stripping, and
  returns zero for nonterminal/unknown state; that is not proof of successful
  completion. Watch/result use the common narrower execution-status projection.
  Cancellation interprets its receipt without waiting for worker termination;
  retry neither validates the new ID nor waits for completion.
- Log following retains every chunk in memory even in raw mode. Per-query byte
  limits are not a whole-output cap, text is stringified, offsets are trusted,
  and raw chunks print immediately. Later errors cannot retract that output.
  Following stops for EOF plus recognized terminal state before checking its
  local deadline, with a 0.01-second poll floor and no cancellation. Absent EOF
  does not stop following but defaults true in the final projection; availability
  comes from the final page. Non-follow ignores timeout. Raw output rejects any
  destination except the exact '-' before opening Core and bypasses JSON controls.
- [test_cli_squashfs.py](../tests/surfaces/test_cli_squashfs.py): all ten functions
  and the module introduction are documented. Tool discovery is a skip gate,
  not version/execution validation. Digest calculation reads all bytes in memory.
  Terminal JSON extraction tolerates leading chatter but requires a valid object
  consuming the remaining tail. Fixture rows distinguish stored metadata from
  real source bytes/designation. Strict drift checks assert status, no duplicates,
  and failed scratch state, not rollback of every side effect. Replica/provenance
  cases publish through the underlying helper and use the CLI only for querying;
  no fictional lineage is inferred. The missing-filter test does not need tools.

Batch: **6 modules, 4 classes, 42 functions = 52 declarations**. Cumulative:
**231 modules, 266 classes, 2,542 functions = 3,039 reviewed Python declarations**,
plus the separately reviewed native C and runtime-created vacuum adapter.

## Verification

Overlapping selections must not be added into a distinct-test total.

- All **231 reviewed Python files**, including the data-submodule generator,
  pass the strict structural audit and normalizer --check. All six batch files
  pass individually. Final executable ASTs still match HEAD after stripping
  genuine leading docstrings; no imports, signatures, parser literals, conditions,
  fixture values, or assertions changed.
- No new Ruff findings relative to HEAD. Existing findings: Core CLI one I001;
  jobs one I001/one unused emit_bytes F401; SquashFS regression module two I001.
  The three SquashFS source owners are lint- and formatter-clean. They were
  rechecked after the final example correction; no warning was waived or fixed.
- Full `bash scripts/run_type_checks.sh`: **passed**, retaining 159 formatter
  files, 456 annotated modules, 221 protected dependency modules, 188 strict-mypy
  files, and 37 rejected invalid examples per checker. Selected formatting,
  lint, complexity, annotation, dependency direction, and both type checks pass.
- Initial combined doctest/regression run caught one new documentation-example
  error: a partial CoreRow was used where six projected keys are required. The
  source contract and example were corrected, not the implementation or assertion.
  The initial run had **84 passed, 1 failed, 48 explicitly skipped examples**.
  Corrected command-module doctests then passed: **6 passed, 6 explicitly skipped**.
- Final full batch doctest/regression rerun: **85 passed, 48 explicitly skipped
  examples** in 148.77 seconds. It includes the six batch files and the already
  documented CLI dependency/operational-family suites plus operator-hardening
  regressions. Both warnings are existing multiprocessing/fork deprecations
  from SQLite SquashFS publication cases. No tool/schema skip occurred here.
- Adjacent migration, public-doc/link, and ownership selection: **63 passed,
  5 failed** in 62.77 seconds. All failures are the same known physical-line/
  statement conflicts in Core services, program facade, browser components,
  windowed components, and windowed root. No guard logic or limits changed;
  this separate selection remains non-green.
- All **298 parser paths**, root help, and bash/zsh/fish script fingerprints
  match the values recorded in the previous CLI-foundation note. No parser
  literal or generated completion output changed.
- Isolated SquashFS checks passed for exact terminal states, whitespace job IDs,
  shallow request/result ownership, required envelope/result shapes, explicit
  versus file ID ordering, full-line versus inline comments, conjunction across
  opposite provenance endpoints, duplicate edge order, repeated negative lookups,
  required projected keys, table-before-filter errors, report-after-context order,
  forwarded publication options, and partial text output before duration failure.
  The scratch provenance fixture was corrected alongside the doctest to include
  actual required columns rather than assuming absent keys mean None.
- Mocked daemon checks passed for bind/size preflight, owned-runtime rejection,
  integer MiB conversion, actual-port readiness, two readiness destinations,
  signal installation/restoration and event notification, the constructor cleanup
  gap, and stopping/closing after output failure. Job checks passed for wait-state
  status distinctions, log text/EOF defaults, terminal-before-timeout order, raw
  accumulation and partial output on later error, output preflight, and poll floor.
- With explicitly permitted local sockets, a real temporary-SQLite Core CLI
  daemon started on loopback port zero, wrote matching JSON to output and ready
  files, reported its assigned port and 1.5-MiB limit, and returned normally after
  stop_after=0. A subsequent connection was refused, proving listener shutdown.
  All fixture files were confined to an automatically cleaned temporary directory.
- All **228 local link destinations across nineteen working-memory files**,
  including the index, resolve. Root and data-submodule diffs are whitespace-clean,
  the staging area is empty, and all verification processes have completed.

No fresh whole-project pytest, installed-wheel, remote-CI, or live PostgreSQL
verification is claimed by this batch.

## Inventory and next work

Full audit parses **2,730 modules, 4,400 classes, 35,119 functions** with no failures.
Missing/blank docs remain on **1,102 modules, 2,021 classes, and 22,567 functions =
25,690 declarations**. Additional overlapping findings: 4,202 delimiter-layout,
10,478 missing-example, 3,035 parameter-field, 3,975 return-field, 4,119 empty-parameter,
5,551 empty-return, and 28 missing-summary. Reports:
`/tmp/liuxin-docstring-current-2026-09-10.json` and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

Next: CLI serve.py (134 lines), capabilities.py (74), catalogue.py (755),
workflows.py (729), and diagnostics.py (440), then remaining configuration,
initialization, storage, ingest, metadata, and PostgreSQL command families and
their tests. These next owners have not been reviewed in full for this goal.
Do not repeat the completed CLI foundation, SquashFS/Core/job owners, or the
three completed CLI regression modules. The 1,205-line operator-hardening suite
remains only partly source-read despite repeated regression execution. Keep all
remaining project-owned source/tests/scripts/examples/native/submodule scope and
the configuration-exec caution in the main ledger. The whole-project goal stays active.
