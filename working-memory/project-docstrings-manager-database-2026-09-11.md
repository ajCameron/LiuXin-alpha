# Database storage metadata and unit-of-work adapters — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[composition and persistence-port checkpoint](project-docstrings-manager-composition-ports-2026-09-11.md).
Branch remains `codex/project-docstrings` at `edf6bf05`, without a commit or push.
Existing unrelated changes and data-submodule work are retained.

Source-reviewed and documented in full:

- `storage/storage_manager/database_repository.py`: both compatibility mappings,
  the entire durable metadata repository, all record/cache/journal/load helpers,
  and every scalar/codec helper. **3 classes, 108 functions**, 2,851 to 3,616 lines.
- `storage/storage_manager/database_unit_of_work.py`: all four domain repository
  adapters, transaction marker, unit of work/factory, revision helpers, and legacy
  mappings. **7 classes, 45 functions**, 858 to 1,194 lines.

Newly completed: **2 modules, 10 classes, 153 functions = 165 declarations**.
Cumulative: **519 modules, 751 classes, 6,240 functions = 7,510 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 519 unique
paths. All **22 tracked storage_manager implementation modules** are now complete.
Both targets matched HEAD before editing; prior repository excerpts and unit-of-work
read-ahead are now completed reviews, not additional files to count again.

## Contracts clarified

- Mapping facades retain callbacks/repositories, not a record catalogue. Collection
  views belong to the freshly loaded mapping. Only KeyError selects defaults;
  separate lookup/remove calls add no transaction or atomicity. Writes do not
  independently commit. The ingest-operation mapping ignores its assignment key,
  uses the result UUID, and has no-op removal; it cannot purge journal entries.
- Schema support checks table/callable shape rather than real operation semantics.
  Cache attachment requires the same database object and Asset/Replica coverage.
  A candidate can load before later rejection without replacing the old binding.
  Cache misses/errors do not fall back to macros. Invalidation can fail after a
  completed row mutation and does not itself guarantee rollback.
- Most single-row operations rely on caller transactions. Composite and Item-link
  replacement open macro contexts, while invalidation follows context exit.
  Envelope migration records its migration bookkeeping after the rewrite context,
  before invalidation. Identity reservation occurs before complete record construction;
  caller transaction/backend semantics govern failure and ID reuse.
- Store ensure/UUID lookup accepts the first duplicate row, while update/remove
  rejects duplicates. Generic upsert filters unsupported columns and uses separate
  existence/read and write calls. Record deletion adds no reference/loss-policy or
  physical-byte check. Item-role replacement preserves exact scratch text, projects
  unsupported scalar roles, and invalidates only the newly selected target relation.
- Typed record envelopes override scalar fields and supply their own identities;
  a different embedded identity can make a selected row appear absent under its
  scalar ID. Recognized malformed envelopes raise with their cause. Unrelated or
  invalid unmarked scratch JSON permits legacy fallback. Reservations can decode
  to None without being complete records.
- Legacy family loaders have distinct omission/error behavior: missing Asset evidence,
  invalid Replica state/mode, empty Composite membership, missing derivation endpoints,
  and invalid policy construction can skip rows. Other coercion/lookup failures remain
  visible. Legacy replication defaults can themselves violate policy validation;
  missing copy settings therefore do not guarantee a returned default policy.
- Item links load atomic rows before Composite rows; later IDs win within each
  table and Composite targets win across tables for duplicate Item/role keys.
  No role stripping occurs. Policy flags use scalar truthiness, and scalar
  reconstruction omits data available only in complete envelopes.
- Journal start resets an equal existing request to started even after committed
  or failed state, clears its error, and retains other payload/scalar values.
  State helpers add no transition/CAS check. Completed-result persistence replaces
  recovery-only payload fields and uses the result UUID without checking old request
  equality. Pending queries exclude exactly committed/failed states.
- Error text is truncated before surrogate escaping and may expand beyond 2,000
  stored characters. Scalar escaping and omission of request payloads are not secret
  redaction. Journal summaries retain their raw diagnostic/location scalars, and
  failure stringification/database errors can replace an earlier workflow error.
- The codec supports specified primitives, containers, UUIDs, datetimes, enums,
  and dataclass instances, with explicit constructor registration. Reserved marker
  keys can reinterpret ordinary dictionaries; encoded init=False fields need not
  reconstruct; nonfinite floats pass default JSON handling. There is no dynamic
  type import, arbitrary-value losslessness, or independent cycle detection.
- Unit-of-work begin returns inactive objects sharing repository ports. Entry opens
  the macro context; commit/rollback only mark intent. Exit ignores underlying
  suppression results and clears active state in finally but retains intent flags
  and the transaction reference. Re-entry is possible with old intent, so callers
  should use fresh units. Revision comparison is a Python read/check/write sequence,
  not an atomic database predicate introduced by these adapters.

## Verification

- Strict audit and normalizer passed for both targets and the **519-file reviewed
  set**. Final module/class/function totals and manifest uniqueness were independently
  recounted from source. The later policy-fallback prose clarification passed the
  same two-file structural checks without changing fields, examples, or declarations.
- Final executable ASTs match HEAD after stripping only leading literal docstrings.
  Signatures, annotations, executable statements, runtime strings, protocol behavior,
  compatibility exports, and guard limits are unchanged. No runtime-doc exception
  was added. Ruff findings remain **4 to 4** for the repository and **1 to 1** for
  the unit-of-work adapter, with no newly introduced finding.
- Selected doctests and Store/API/manager/ingest/filesystem/archive/database
  regressions: **454 passed, 396 explicitly skipped integration examples, 1 failed**,
  138.04s. The sole failure remains
  `test_storage_manager_composition.py::test_manager_module_stays_a_small_composition_root`:
  `_policy_support.py` **1,213**, `_support.py` **1,139**, and `_contracts.py` **902**
  exceed its unchanged **900-line** ceiling. This batch adds no offending file.
- Full quality runner passed: 159 formatted files, annotations across 456 modules,
  221 protected dependency modules, lint/complexity, both production type checkers,
  188 strict-mypy files, and 37 invalid examples rejected per checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 20.19s.
- **39 isolated observations** passed for mapping defaults/views/identity checks,
  codec limits, scalar coercion, envelope precedence and malformed input, legacy
  policy fallback, role merging, cache routing/rejection, post-write invalidation,
  duplicate Store lookup, journal reset/error/state/result behavior, and unit reuse.
  These use explicit row/cache/journal/transaction doubles and actual adapter code;
  they do not claim live database durability. The existing selected database suite
  supplies commit/rollback, cache integration, migration, reload, and restart evidence.
- The first observation run assumed a replication-policy row with no copy settings
  would load and stopped on KeyError. Source validation showed why it is skipped.
  The fixture now checks that omission explicitly and supplies valid copy settings
  for its separate truthiness observation. A short prose clarification was added;
  final AST/Ruff/structural checks passed again. No executable source or example
  changed after the broader passing checks, so those suites were not repeated.
- All processes completed with observed terminal exits. The **seven** known
  docstring-sensitive pytest guard failures remain unresolved: six earlier
  Core/CLI/terminal ownership failures plus the composition failure above.
  The earlier six were not rerun, and no guard was weakened or deselected.
- Final root and data-submodule whitespace checks passed. All 113 index links,
  three main-ledger links, and three links in this note resolve locally. Source
  census confirms 519 unique reviewed modules and no remaining modules in the
  22-file manager implementation package. Public conveniences remain byte-identical
  to HEAD and outside the manifest; existing data-generator edits and bytecode
  artifacts are retained.

Durable results:
`working-memory/test-results/docstrings-manager-database-2026-09-11-{regression,quality,contracts}.{log,done}`,
`docstrings-manager-database-2026-09-11-observations.json`, and the captured first
observation failure in `docstrings-manager-database-2026-09-11-observations-initial.log`.
Static reports: `/tmp/liuxin-docstring-manager-database-batch-2026-09-11.json`,
`/tmp/liuxin-manager-database-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank docs remain **1,076 modules, 1,908 classes, 20,579 functions
= 23,563 declarations**. This batch completed existing incomplete documentation.
Overlapping findings: 3,676 delimiter layout, 9,820 examples, 2,808 parameter fields,
3,700 return fields, 3,255 empty parameter descriptions, 4,257 empty return
descriptions, and 28 missing summaries.

## Next

The storage_manager implementation package is complete, but storage still has
**13 unreviewed API modules**: public manager conveniences and workflow APIs.
Continue with `storage/api/storage_manager_api/convenience_api.py` (2,002 lines),
then workflow contracts and application-manager/registry/workflow owners outside
the implementation package. The convenience file remains unedited and outside
the manifest; previous mentions were planning context, not a completed source read.

The broader metadata, catalogue/cache, formats, source/test/script/example,
inherited code, and tracked data-submodule Python remain in scope. Preserve the
runtime-doc consumer cautions and three narrow exceptions in the
[main ledger](project-docstrings-2026-09-08.md).
