# Shared surface/Core adapter docstrings — 2026-09-09

Continuation of the active [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
after the [terminal source milestone](project-docstrings-terminal-completion-2026-09-09.md).
The intervening PR-status turn did not advance documentation. Revalidation found
the adapter clean against HEAD; this turn then completed its source review, prose,
and focused contract tests. Work remains local on `codex/project-docstrings`, based
on `edf6bf05`, with no commit, push, PR change, or submodule publication. No agents
were delegated work. PR #115 remains separate from this unfinished documentation pass.

## Reviewed scope

- [surfaces/core.py](../src/LiuXin_alpha/surfaces/core.py): the complete module,
  six classes, and 82 functions, including three overload declarations, private
  helpers, mapping dunders, lifecycle hooks, and parser composition. The original
  1,098-line file was read completely; no declarations were omitted because of size.
- [New adapter contract tests](../tests/surfaces/test_surface_core_documentation_contracts.py):
  the complete module and nineteen functions, including the nested profile double.
  Its normal pytest discovery does not expand the maintained formatter/lint/type scopes.

Important source-linked contracts now made explicit:

- CoreRow freezes attributes, not the retained value mapping. Identity fields are
  not automatically mapping keys, nested objects remain shared, and a row with
  an empty value mapping is falsey even when it has an identifier. Page metadata
  is a passive backend receipt, not a recomputed consistency guarantee.
- Sessions distinguish borrowed clients from owned local composition; remote
  configuration performs no reachability probe. Local server connection strings
  bypass Path expansion, but the forwarded driver selector is not stripped.
  Close records its flag before shutdown, propagates failure, and never retries.
  Context entry does not reject/reopen a closed session. Legacy composition keeps
  the supplied database borrowed and explicitly disables job-manager shutdown.
- Schema loading commits the cache only after complete conversion, keeps later
  duplicate names, and retains empty successful inventories. Detail reuse requires
  a truthy relations_included flag. Returned copies are shallow; explicit source
  refresh and administrative writes do not invalidate the model's schema.
- Row conversion can query schema to derive relationships and ignores wire-supplied
  linkable-table declarations. Query inputs clamp offset/limit; response bounds,
  counts, and completeness are trusted after conversion. rows/get_all_rows perform
  one query, not exhaustive pagination. Both iterator-returning legacy views are
  eager, and their default iterator/list modes differ.
- Driver heuristics preserve first-ID-like column order, priority-suffix family
  precedence over type, exact/unique link-column selection, timestamp shortest-name
  ties, and explicit missing/ambiguous-table behavior.
- CoreDatabaseView is not read-only or an authorization boundary. Its supplied
  model need not use the info-query client. Metadata is cached while type is queried
  afresh. Legacy source selection uses truthy primary-row precedence; link readback
  zips/truncates entity and link results positionally without validating alignment.
  Priority sentinels omit the field, and mutation helpers retain their unverified
  success conventions. Failed readback does not undo a completed write.
- Acquisition reads fully materialize content, retain native bytes identity, and
  accept tagged base64 with non-strict decoding rather than adding validation or
  size limits. Parser selectors remain optional at parse time; profile resolution
  precedes session argument extraction and may mutate the namespace.

No implementation, signature, default, exception policy, guard limit, or parser
option was changed. Existing misleading read-only/deep-immutability implications
were removed rather than implemented as new restrictions.

## Verification

Selections overlap and must not be summed as distinct tests.

- Strict audit and normalizer checks pass for this two-file batch: **2 modules,
  6 classes, 101 functions**. The cumulative reviewed set is **155 modules,
  209 classes, 1,566 functions = 1,930 declarations**, all strictly clean.
- Initial adapter/new-test doctests and tests: **70 passed, 57 explicitly skipped
  integration examples**. No database or HTTP service is needed by those examples.
- The initial new-test lint run caught B010 in the intentional frozen-attribute
  assignment check. Replaced constant-name setattr with direct assignment; final
  new-test/doctest rerun: **42 passed**. New test module is formatter- and lint-clean.
- The production executable AST matches HEAD exactly after stripping genuine
  leading docstrings, including the retained ellipsis statements in all overloads.
  No exceptional runtime-doc normalization is needed for this module. Existing
  Core exceptions recorded in earlier notes remain separately scoped there.
- The adapter remains outside the incremental formatter/lint list. A before/after
  Ruff comparison shows no new findings: existing I001 (one), UP032 (five), UP037
  (four), and B905 (one) remain. They were neither waived nor fixed in a doc pass.
- Full `bash scripts/run_type_checks.sh`: **passed**. Its scopes remain 159 formatter
  files, 456 annotated modules, 221 protected dependencies, 188 strict-mypy files,
  and 37 rejected negative examples per checker, with no production typing errors.
  This runner does not execute physical-line ownership pytest guards.
- Broader surface/transport/ownership regression selection: **269 passed,
  eleven failed** in 168.87 seconds. Six failures were sandbox PermissionError
  while binding loopback sockets (the acceptance test plus five transport-error
  cases); the other five were the previously observed ownership guards.
- Approved outside-sandbox rerun of those two HTTP test modules: **six passed**
  in 26.12 seconds. This exercises direct/RPC clients across web read-only,
  read-write, API, OPDS, terminal, and headless Tk backends, plus error-code/detail
  preservation and cache capability-error classification. It does not claim a
  displayed GUI, interactive curses, live PostgreSQL, or a full-project test run.
- Public-documentation and developer-link handoff suites: **23 passed** in
  20.71 seconds. All **129 local link destinations across eight working-memory
  notes** resolve. Root and data-submodule diffs are whitespace-clean.

The prior five physical-line ownership failures remain documented in the terminal
and Core milestone notes. No permission to change their docstring-sensitive
counting has arrived, and their executable logic and numeric limits remain intact.
The broader selection is not all-green. The six environment failures are resolved
by the approved rerun; the five guard failures remain terminal failures, not pending
checks or hidden passes. All observed pytest, quality, audit, and AST/lint processes
have completed.

## Remaining scope

The regenerated whole-project audit inventories **2,726 modules, 4,399 classes,
35,057 functions**, with no parse failures. Missing/blank docs remain on **1,118
modules, 2,042 classes, 23,407 functions = 26,567 declarations**. Existing incomplete
prose has additional, overlapping findings recorded in the main ledger. This is
not completion of the full project objective.

Reports: `/tmp/liuxin-docstring-current-2026-09-09.json` and
`/tmp/liuxin-docstring-reviewed-2026-09-09.json`. Next source owners are the remaining
shared surface APIs/types/presentation/profile helpers, followed by CLI/HTTP/web
owners and the rest of the project. Profile application was inspected as a
dependency, not counted as a completed file. Keep configuration-exec dictionary
consumers and the dirty data-submodule generator in the project-wide inventory.
