# Public manager convenience operations — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[database adapter checkpoint](project-docstrings-manager-database-2026-09-11.md).
Branch remains `codex/project-docstrings` at `edf6bf05`, without a commit or push.
Existing unrelated changes and data-submodule work are retained.

Source-reviewed and documented in full:
`storage/api/storage_manager_api/convenience_api.py`, including the class, all
20 public methods, and all 24 private normalization/identifier/delivery helpers.
The file was read in full and matched HEAD before editing. It grew from 2,002
to 2,183 lines; no executable behavior was changed.

Newly completed: **1 module, 1 class, 44 functions = 46 declarations**.
Cumulative: **520 modules, 752 classes, 6,284 functions = 7,556 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 520 unique
paths. All **29 storage_manager_api modules** and all **22 storage_manager
implementation modules** are now reviewed. Dependency reads confirmed current
ingest, selection, replication, Composite resolution, and value-construction
contracts without counting those completed files again.

## Contracts clarified

- store dispatch checks bytes-like input first, converts it to bytes, and rejects
  a supplied size mismatch before delegation. Strings/PathLike values are local
  paths; other sources need only a read attribute at this dispatch boundary.
  Direct store_bytes forwards its data unchanged. Asset description and rich
  placement hints are separate; return shortcuts discard detailed ingest evidence.
- Stream ingest consumes the caller's current position without owning its reader.
  File ingest delegates stat/open/close and basename defaulting to ingest_file.
  Expected identity, retries, verification, and post-publication failure behavior
  belong to the underlying workflow. verify does not add a healthy-result assertion.
- Positive scalar IDs reject bool, strings, and fractional values without coercion,
  while IDs extracted from accepted records are not revalidated. Store references
  use structural UUID attributes, preferring store_uuid over store_ref.
- String open_file/get_file/read_file identifiers are digest values, never local
  paths or textual IDs. Existing Digest values retain their algorithm; direct
  Asset inputs ignore algorithm and size. Both non-None mode spellings reject
  even when equal, and omitted mode selects ACTIVE.
- open_asset extracts only an Asset ID, even from an existing resolution, then
  selects again. A Store argument is a preference, and verified requires recorded
  state rather than a new hash. No version precondition connects selection to
  opening. Reader ownership transfers to the caller; read helpers close it and
  can fail during cleanup after reading all bytes.
- Attribute conversion checks two-string unpacking and only tuple-collects the
  outer sequence: original lists or two-character strings can survive as entries.
  Mapping digests normalize through Digest without helper-level duplicate checks.
  Policy tag iterables become frozensets, so a bare string supplies characters.
- create_composite builds required ordered memberships and retains mapping keys
  as logical paths without the stricter export validation. store_composite validates
  each path just before that member's ingest; later member/path failures retain
  earlier Assets. Composite name/attributes and final Item linkage are validated
  after lower-level work, with no aggregate rollback.
- Directory export preflights names/current resolved containment and existing
  collisions, then creates parents and copies in order. It reselects Assets instead
  of pinning the initially resolved Replica. Distinct in-root aliases can refer to
  one file; ancestor conflicts or later filesystem changes are not fully preflighted.
  Failures can leave completed and partial/truncated targets. overwrite selects
  wb rather than temporary-file publication; ordinary creation uses xb.
- ZIP delivery rejects duplicate names before allocating an 8 MiB rollover spool,
  uses 1 MiB copy chunks, and transfers a completed rewound stream to the caller.
  The spool threshold is not an archive-size cap. UnicodeEncodeError from member
  creation is wrapped; assembly BaseException closes the output. The ZIP is not
  automatically ingested or registered with provenance.
- Delivery names prefer logical_path, logical_name, original_name, then sequence
  fallback. An invalid truthy preferred name rejects instead of trying later values.
  POSIX syntax validation does not establish Unicode encoding or host-specific
  filename portability, and containment preflight does not lock filesystem state.
- Policy conveniences normalize selected enums and collect caller values before
  policy validation/registration. They neither assign policies nor execute copies.
  Provenance conveniences materialize ordered roles/references without executing
  recipes; only Composite records distinguish Composite sources from atomic integers.

## Verification

- Strict structural audit and normalizer passed for the complete file and full
  **520-file reviewed set**. Manifest uniqueness and final declaration totals were
  independently recounted from source ASTs; remaining package counts agree.
- Executable AST matches HEAD after stripping only leading literal docstrings.
  Signatures, annotations, aliases, runtime strings, assertions, and guard limits
  are unchanged. No runtime-doc exception was added. Ruff findings remain
  **10 to 10**, with no newly introduced finding. Existing useful examples were
  retained and verified alongside the new prose and complete field descriptions.
- Selected doctests and Store/API/manager/ingest/filesystem/archive/database
  regressions: **454 passed, 278 explicitly skipped integration examples, 1 failed**,
  175.66s. The sole failure remains
  `test_storage_manager_composition.py::test_manager_module_stays_a_small_composition_root`:
  `_policy_support.py` **1,213**, `_support.py` **1,139**, and `_contracts.py` **902**
  exceed its unchanged **900-line** ceiling. This batch adds no offending file.
- Full quality runner passed: 159 formatted files, annotations across 456 modules,
  221 protected dependency modules, lint/complexity, both production type checkers,
  188 strict-mypy files, and 37 invalid examples rejected per checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 23.79s.
- **31 isolated observations** passed for normalization limits, dispatch/delegation,
  reader lifetime and cleanup failure, policy construction, Composite identity,
  partial member/Item-link effects, current symlink containment and in-root aliases,
  partial directory copies, ancestor conflicts, and ZIP completion/error cleanup.
  These combine explicit doubles with transient memory Store ingest and real
  temporary directories/symlinks/files/ZIPs. They do not claim live services,
  scheduled filesystem races, or durable database behavior. Readers/managers close
  in finally, temporary directories clean up, and the first observation run passed.
- All verification processes completed with observed terminal exits. The **seven**
  known docstring-sensitive pytest guard failures remain unresolved: six earlier
  Core/CLI/terminal ownership failures plus the composition failure above.
  The earlier six were not rerun; no guard was weakened or deselected.

Durable results:
`working-memory/test-results/docstrings-manager-convenience-2026-09-11-{regression,quality,contracts}.{log,done}`
and `docstrings-manager-convenience-2026-09-11-observations.json` in that directory.
Static reports: `/tmp/liuxin-docstring-manager-convenience-batch-2026-09-11.json`,
`/tmp/liuxin-manager-convenience-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank docs remain **1,076 modules, 1,908 classes, 20,579 functions
= 23,563 declarations**. This batch completed existing incomplete documentation.
Overlapping findings: 3,676 delimiter layout, 9,820 examples, 2,808 parameter fields,
3,700 return fields, 3,211 empty parameter descriptions, 4,213 empty return
descriptions, and 28 missing summaries.

## Next

Storage has **12 unreviewed API modules**, all under `storage/api/workflow_api`:
the package/base/shared models; six backup API modules; and three sealed-artifact
API modules. Continue their complete source reviews and then application-manager,
registry, and workflow implementation owners outside storage_manager. Workflow
files were inventoried here, not read ahead or claimed as completed work.

The broader metadata, catalogue/cache, formats, source/test/script/example,
inherited code, and tracked data-submodule Python remain in scope. Preserve the
runtime-doc consumer cautions and three narrow exceptions in the
[main ledger](project-docstrings-2026-09-08.md).
