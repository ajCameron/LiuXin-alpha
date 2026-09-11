# CLI serving, catalogue, workflows, and diagnostics docstrings — 2026-09-10

Continuation of the active [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following [CLI publication and jobs](project-docstrings-cli-publication-jobs-2026-09-10.md).
The intervening PR-status turn made no documentation progress; this turn revalidated
clean target paths, reread the style guide, and fully read all five source owners.
It documents every module and named function in those files, not only public exports.

Branch: `codex/project-docstrings`, based on `edf6bf05`. The documentation remains
local and uncommitted. No commit, push, submodule publication, PR modification,
production behavior change, or ownership-guard adjustment accompanies this batch.

## Reviewed contracts

- [serve.py](../src/LiuXin_alpha/surfaces/cli/serve.py): loopback classification
  strips/lowercases without DNS, but the original host is forwarded. Unsafe-bind
  refusal precedes import; the warning precedes lazy application import. The
  application owns lifecycle and validation. Explicit database wins over endpoint;
  profile/system-root selection is neither resolved nor forwarded here. A profile-
  only direct invocation can therefore pass None as the endpoint argument. This
  existing behavior is documented, not repaired. All leaves accept common flags,
  but database-path exposure applies only to web/web-write/calibre, cache controls
  exclude web-write, and the OPDS grouping limit applies only to opds/calibre.
- [capabilities.py](../src/LiuXin_alpha/surfaces/cli/capabilities.py): eight probes
  share one storage-enabled session. Individual Exception failures are recorded
  with unredacted text and do not stop later probes. Returned failure flags are
  not interpreted; payload dictionaries and receipts are not copied. Session
  and publication failures propagate. The plugins/capabilities and inspect/list
  aliases perform the same work.
- [catalogue.py](../src/LiuXin_alpha/surfaces/cli/catalogue.py): query/command
  adapters pass request identity through, exit Core before output, and return
  zero regardless of receipt status. Accepted mutations are not rolled back on
  publication failure. Specifications are read on the CLI host; repository,
  relationship, field, and resource semantics remain Core-owned. Integer casts
  do not impose positive ranges, and direct handlers do not necessarily enforce
  their parser's action choices.
- Catalogue confirmation policies are explicit: entity delete, WEMI unlink,
  and metadata merge refuse without yes, with no preview; custom-field deletion
  produces a local-only preview that proves neither existence nor feasibility.
  Match-or-create and other writes do not acquire an invented confirmation gate.
  Identifier replacement accepts dict/list containers without validating entries.
  Optional non-None zero/false/empty selectors differ from truthy-only selectors.
  WEMI linking retains false primary and zero priority; Work browse retains empty
  text and zero category ID. Custom-field typed changes override JSON, update
  uses is_editable while creation uses editable, and display JSON replaces display.
- Custom-field matching accepts only a list, skips non-dicts, trims only the
  reference, compares exact stringified num/label, returns the original dictionary,
  and counts duplicate entries separately. Missing num/label can match 'None'/''.
  Acquisition fetches complete content, closes Core, decodes, then publishes bytes
  to the CLI host. File-output diagnostics are emitted afterward; malformed resource
  metadata can fail serialization after the bytes have already been published.
- [workflows.py](../src/LiuXin_alpha/surfaces/cli/workflows.py): managed paths
  belong to the Core host, while specification/options files belong to the CLI
  host. Job submission inherits shared payload mutation, wait/detach, and narrow
  exit-status policy. Backup MiB conversion truncates after multiplying by
  1,048,576, without local positivity/finite-number checks. Direct database queries
  append the action suffix rather than implementing a second allowlist.
- SQLite verification uses a read-only URI and quick_check/integrity_check,
  requires exactly ['ok'] plus at least one table, and does not validate the
  LiuXin schema. SQLite failures return reports; filesystem/hash failures raise.
  Size/digest are read after connection closure, not in one atomic snapshot.
  Offline checks reject existing -wal/-shm paths and acquire/release a momentary
  exclusive transaction; callers must keep all users stopped through restoration.
- Restore requires confirmation before mutating profile selection, rejects remote
  and non-SQLite/APSW targets, verifies source, then creates and hashes a safety
  copy. Resolved-path inequality does not detect hard-link identity, and a prior
  safety existence check is not race-free exclusive creation. Staging is beside
  the target, flushed/fsynced, permission-bit-preserving, and compared against
  the source verification digest before atomic pathname replacement. Safety copies
  remain after failure; there is no automatic rollback after directory sync,
  final verification, or publication failure. Directory-open errors are suppressed
  by the parent-sync helper, while fsync/close errors propagate. Cleanup can also
  raise. These limitations were documented, not changed or presented as guarantees.
- Migration apply queries a plan even in preview. Preview returns zero regardless
  of plan.ok and emits inside the session; confirmed blocked plans return one.
  Dispatched apply emits applied=True without checking receipt.ok. Plan and apply
  are separate calls, not one adapter transaction. Maintenance run/clean/merge
  previews instead perform no Core lookup, and preserve selected row order/duplicates.
- [diagnostics.py](../src/LiuXin_alpha/surfaces/cli/diagnostics.py): corrected the
  misleading read-only module summary. Full collection inventories PATH executables,
  refreshes storage discovery, and commands Store probes; it can contact backends
  and refresh state. Missing tools are informational. Required query exceptions
  are errors; optional query exceptions are warnings. Returned health/ok flags
  and failed-job counts do not themselves fail checks. Empty Store probe lists
  succeed, and malformed Store entries are skipped or can yield a None reference.
- Configuration/tool-inventory failures are outside the Core catch. Exceptions
  within session/processing/exit can become a core_open error even when connection
  succeeded. Raw collection is unredacted; publication handlers apply heuristics.
  Redaction copies dict/list/tuple structures, stringifies keys, turns tuples into
  lists, preserves URL usernames, leaves unsupported values unchanged, and does
  not handle cycles or every possible secret. Stringified key collisions can lose
  entries. Status exit comes from check severity, not the storage healthy field.
- Support logs use a second storage-disabled session and only the first five
  job-list entries, skipping invalid ones without replacement from later entries.
  Each read requests offset zero and 16,384 bytes: these are initial chunks, despite
  the failed_job_log_tails output key. Log failures do not change doctor-based exit
  status. Environment selectors are represented only as booleans, but paths,
  usernames, arbitrary application data, and unrecognized secrets may remain.

Batch: **5 modules and 91 functions = 96 declarations**. Cumulative reviewed
Python: **236 modules, 266 classes, and 2,633 functions = 3,135 declarations**,
plus the separately reviewed native C and runtime-created vacuum adapter.

## Verification

- All five files pass strict audit and normalizer --check; all **236 reviewed
  Python files**, including the data-submodule generator, pass both checks too.
  Every executable AST in this batch matches HEAD after stripping only genuine
  leading docstrings. Signatures, parser literals, payloads, conditions, and
  operational behavior remain unchanged.
- Ruff introduced no findings relative to HEAD. Each file retains one I001;
  catalogue has one existing UP032, workflows seven UP032/one UP017, and diagnostics
  five UP032. None was waived or incidentally repaired.
- Full `bash scripts/run_type_checks.sh`: **passed**. Scope remains 159 formatted
  files, 456 annotation-covered modules, 221 dependency-protected modules, 188
  strict-mypy files, and 37 rejected invalid examples for each type checker.
- Combined five-module doctests plus CLI dependency, operational-family, and
  operator-hardening regressions: **93 passed, 82 explicitly skipped integration
  examples**, 129.24 seconds. The skips are documented integration examples, not
  executed integration evidence. No failed example or regression occurred.
- Adjacent migration/public-doc/link/ownership selection: **63 passed, 5 known
  ownership failures**, 56.76 seconds. These remain the Core service/facade and
  terminal browser/windowed physical-line conflicts recorded in the main ledger;
  none comes from these newly documented files. Guard code/thresholds are unchanged.
- All **298 parser paths**, root help, and bash/zsh/fish SHA-256 fingerprints match
  the previous CLI-foundation note. An initial standalone check omitted PYTHONPATH
  and failed before importing LiuXin; rerunning with PYTHONPATH=src passed. Calibre
  reported its existing temporary-config fallback outside writable user config.
- Isolated mocks passed serve guard-before-import, option forwarding by surface,
  unnormalized host forwarding, zero numeric options, and unresolved profile-only
  selection. Capability checks proved all-probe continuation, shared payload
  identity, raw errors, and successful-call treatment of failure receipts.
- Catalogue isolated checks passed exact/duplicate/missing-key custom-field
  matching, refusal-before-Core, local previews, typed JSON override precedence,
  zero/false/empty preservation, query identity/output ordering, and byte-output
  completion before later diagnostic serialization failure. Migration checks
  proved preview/blocked/apply status and output ordering; maintenance previews
  did not open Core.
- Diagnostic mocks passed required-versus-warning severity, receipt-status
  noninterpretation, full-mode refresh and Store commands, config-error boundaries,
  redaction shape/collision/nonsecret limits, five-entry log selection, offset-zero
  bounded reads, redacted log errors, and readiness-only exit status.
- Actual temporary SQLite fixtures passed quick/full verification with Unicode/#
  paths, empty/corrupt/missing-file handling, successful restore, safety collision,
  WAL refusal, staged-digest mismatch, permission preservation, and preservation
  of safety copies. Injected directory-sync and publication failures confirmed
  that replacement remains in place after those failures. Temporary staging was
  removed and the fixture directory automatically cleaned. No production database
  or live external service was accessed.
- All **235 local link destinations across twenty working-memory files** resolve.
  Root and data-submodule diffs are whitespace-clean, the staging area is empty,
  and every verification process started for this batch has completed.

No fresh whole-project pytest, installed-wheel, remote CI, live HTTP listener,
or PostgreSQL verification is claimed. The same known owner-limit conflicts
remain distinct from the passing quality gate and behavioral checks.

## Inventory and next work

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank docstrings: **1,102 modules, 2,021 classes, 22,491 functions
= 25,614 declarations**. Additional overlapping findings: 4,194 delimiter-layout,
10,463 missing-example, 3,032 parameter-field, 3,972 return-field, 4,107 empty-param,
5,539 empty-return, and 28 missing-summary findings. Reports:
`/tmp/liuxin-docstring-current-2026-09-10.json`,
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`, and
`/tmp/liuxin-docstring-cli-operations-2026-09-10.json`.

Next: config_cli.py (571 current lines), initialize.py (691), ingest_runs.py
(439), storage_audit.py (118), then metadata.py (1,248), postgres.py (479), and
the storage command owners. Their complete source review/documentation is still
pending. The 1,205-line operator-hardening regression module has been exercised
and selected sections read, but is not a completed whole-file documentation claim.
Continue through all remaining project-owned source/tests/scripts/examples/native
and submodule scope, retaining the configuration-exec warning in the main ledger.
The whole-project goal remains active; this batch is not a scope reduction.
