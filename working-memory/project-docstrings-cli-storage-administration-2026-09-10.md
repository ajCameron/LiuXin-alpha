# CLI storage administration docstrings — 2026-09-10

Continuation of the [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following [CLI metadata and PostgreSQL](project-docstrings-cli-metadata-postgres-2026-09-10.md).
The previous goal turn made verified progress. Rechecked clean target paths,
reread the style guide, then fully reviewed and documented thirteen files.

Branch remains `codex/project-docstrings`, based on `edf6bf05`. Work remains local,
uncommitted, and unpushed. No PR change, runtime fix, test assertion modification,
or ownership-guard relaxation accompanies this batch.

## Reviewed files and contracts

- [storage.py](../src/LiuXin_alpha/surfaces/cli/storage.py): compatibility names
  are object aliases, not wrappers. Replacing an alias does not patch references
  already held by implementation modules; __all__ exposes only the historical
  wildcard-import subset. The facade remains below its physical-line ceiling.
- [storage_commands/__init__.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/__init__.py)
  and [constants.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/constants.py):
  package ownership, error classification, and exit categories do not imply rollback
  of earlier effects. CLIUsageError retains ordinary ValueError construction.
- [core_access.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/core_access.py):
  storage-enabled sessions close before JSON publication; generic requests neither
  preflight output before execution nor interpret receipt health. A completed mutation
  can precede report failure. Store references strip then int-convert, retaining
  negative integers or empty/noninteger text. File input bounds a single read but
  follows symlinks, lacks regular-file/change checks, and can block opening special
  files. Tiny positive MiB ceilings truncate to zero; nonfinite conversion can fail.
- [filesystem.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/filesystem.py):
  containment is lexical, includes equality, and does not resolve '..' or symlinks.
  Nearest-existing lookup can return the input file itself or a nonexisting final
  anchor. Directory fsync ignores open/sync OSError but not close failures.
- [signals.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/signals.py): first
  signal latches an event and original number; any second signal raises KeyboardInterrupt.
  Main-thread entry saves/replaces handlers; worker entry is a no-op. Exit restores
  successful installations without clearing the request. Instances are not reentrant
  handler stacks; partial installation/restoration errors are not rolled back. The
  request itself does not terminate a subprocess or Core job.
- [prompts.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/prompts.py): input
  is visible and defaults/choices are printed verbatim. EOF/interrupt chain into the
  wizard cancellation exception. User text is stripped, selected defaults are not.
  Menu aliases are case-folded and later aliases win; numeric tokens use index
  handling only. Missing default values choose index one; empty menus keep retrying.
- [administration.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/administration.py):
  document all 24 Store/file/source/asset/replica commands and exact selector keys.
  Most return zero after receipt publication without checking health. Evacuation
  apply uses ok; verification uses healthy. Preview is a query, not a saved-plan
  authorization token. Update preserves unspecified values and allows empty changes.
- Downloads query and close Core before decoding/publishing bytes, have no local size
  cap, and print filesystem-output summaries on stderr afterward. Upload bounds local
  input and sends base64, not a server-side path. Copy's store key differs from the
  store_uuid key used by put/locate/read. Explicit deletion flags are checked before
  Core access. Generic Store save does not run typed-add checks or refresh/probe.
- Source-add lowercases/hyphen-normalizes without stripping; typed disk/HTTP/SquashFS
  defaults differ. Optional JSON overrides run last, including endpoints and typed
  policies. No secret filtering or positive-range checking is added in this path.
- [integrity.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/integrity.py):
  health uses healthy; audit/apply/recovery use ok after output. Reconcile refresh
  affects preview only. Repair converts binary GiB for apply without local positivity
  checks. Exact plan/action strings choose branches; direct callers bypass parser
  whitelists. Policy queries/violations return zero even for adverse receipts, and
  policy assignment allows zero IDs or no effective field change.
- [resources.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/resources.py):
  load bounded filter/values objects before Core; local validation is not a resource
  schema check. Exact update chooses update, otherwise direct calls choose create.
  Only deletion requires confirmation here. JSON publication can fail after mutation.
- [store_options.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/store_options.py):
  backend matching returns the first original descriptor, accepts only list-valued
  aliases, and normalizes both sides. File-location default role outranks read-only;
  requested read-only does not change a descriptor-derived default role.
- Option parsing accepts JSON scalars/string lists or fallback text, preserving empty
  values and nonfinite numbers. Sensitive-key heuristics inspect mapping/list keys,
  not secret values, tuples, or all possible spellings; no cycle/depth guard exists.
  File policy is checked before assignment overrides. backend_options precede option;
  later keys win. Only assignments stamp backend/section; file-only policy is not
  aligned against the descriptor. Store declarations mask write/delete capabilities
  for read-only selection, reject offline/read-only defaults, sort/deduplicate tags,
  and do not prove root existence or live backend capabilities.
- [store_add.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/store_add.py):
  lookup/build/save/refresh precede optional online probing and default selection.
  Probe Exceptions become raw structured errors; other stage errors propagate.
  Default selection can still run when refresh reports failures. Overall ok ignores
  saved/default receipt flags and uses a permissive failure-count coercion; malformed
  present fields prevent fallback but become zero, while negatives remain nonzero.
  A false ok exits one only when check is enabled. Earlier effects are not rolled back.
- [test_cli_storage.py](../tests/surfaces/test_cli_storage.py): all helpers and cases
  document actual evidence boundaries. Discovery uses loose/archive fixtures; preflight
  writes reports/logs despite not creating a database. The publisher collision fixture
  is occupied before the call, not an injected concurrent process. Exclusive locking
  is real; signal tests call the handler directly. The subprocess imports checkout src
  outside the repository, not an installed wheel. No successful managed ingest is claimed.

Batch: **13 modules, 3 classes, 78 functions = 94 AST declarations**. Cumulative:
**259 modules, 275 classes, 2,912 functions = 3,446 reviewed AST declarations**, plus
the previously documented native C and narrowly verified runtime doc metadata.

## Verification

- Strict audit and normalizer --check pass for all thirteen files and all **259
  reviewed Python files**, including the data-submodule generator.
- Every batch executable AST matches HEAD after stripping genuine leading docstrings.
  No new runtime-doc exception is needed. All twelve source files remain Ruff-clean;
  the storage test retains its one pre-existing I001, with no added findings.
- Full `bash scripts/run_type_checks.sh`: **passed**. Scope remains 159 formatted
  files, 456 annotation-covered modules, 221 protected dependency modules, 188 strict-
  mypy files, and 37 rejected invalid examples per checker.
- Thirteen-file doctest selection plus storage/operator-hardening/operational/dependency/
  initialization regressions: **142 passed, 129 explicitly skipped integration examples**,
  133.28 seconds. Counts include imported documentation where pytest discovers it;
  this is not a claim that every integration-only example ran.
- Adjacent migration/public-doc/link/ownership selection: **62 passed, 6 failed**,
  48.69 seconds. Five failures are the previously recorded Core/facade/terminal size
  conflicts. The sixth is storage_commands/administration.py: **623 physical lines
  versus the unchanged 450-line ceiling**, solely from documentation additions.
  Source AST checks pass; no guard logic or limit changed to hide the conflict.
- All **298 parser paths** and root-help/bash/zsh/fish fingerprints match the
  CLI-foundation baseline. Calibre used its known temporary-config fallback.
- Isolated temporary/mock checks passed generic zero-status receipts, mutation before
  output failure, confirmation refusal, preview/apply payload differences, unexpected
  direct resource-action fallback, symlink-following bounded reads, tiny limits,
  lexical containment, existing-file parent selection, fsync/close error distinctions,
  and last-wins typed-source JSON overrides.
- Further checks passed descriptor identity/list-only aliases, rejected option shapes,
  secret-value/tuple heuristic limits, policy override order, read-only capability
  masks versus default role, failure-count fallback quirks, default selection despite
  refresh failure, unchecked exit zero, and caught probe-error receipts.
- Mocked signal checks passed worker no-op, saved-handler restoration, retained first
  signal, second-delivery unwinding, and partial-install nonrollback. Prompt checks
  passed missing-default index one and chained EOF cancellation. No OS signal was sent,
  real backend configured, or external storage mutated.
- All **264 local link destinations across twenty-three working-memory files** resolve.
  Root and data-submodule diffs are whitespace-clean; staging remains empty.

No whole-project pytest, installed-wheel, live PostgreSQL, remote CI, or live HTTP
listener result is claimed. All verification processes have reached terminal states.

## Inventory and next work

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank docs: **1,100 modules, 2,016 classes, 22,258 functions =
25,374 declarations**. Additional overlapping findings: 4,157 delimiter-layout,
10,413 missing-example, 3,021 parameter-field, 3,960 return-field, 4,073 empty-param,
5,505 empty-return, and 28 missing-summary. Reports:
`/tmp/liuxin-docstring-current-2026-09-10.json`,
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`, and
`/tmp/liuxin-docstring-cli-storage-administration-2026-09-10.json`.

Next: the remaining **twelve** storage_commands modules: ingest.py, ingest_config.py,
ingest_options.py, ingest_paths.py, ingest_preflight.py, ingest_reporting.py,
ingest_run.py, parser_files.py, parser_integrity.py, parser_stores.py, parsers.py,
and store_wizard.py. ingest_paths.py was fully read as a dependency here but remains
undocumented; only the report-publisher excerpt of ingest_reporting.py was reread.
The complete operator-hardening module remains partly source-read despite repeated
execution. Continue through all remaining source/tests/scripts/examples/data-submodule
scope afterward; preserve the configuration-exec caution in the main ledger.
The whole-project goal remains active.
