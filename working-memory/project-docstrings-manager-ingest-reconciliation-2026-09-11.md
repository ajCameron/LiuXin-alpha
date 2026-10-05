# Manager ingest and reconciliation contracts — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[derivation checkpoint](project-docstrings-derivations-2026-09-11.md). Branch
remains `codex/project-docstrings` at `edf6bf05`, without a commit or push.

Source-reviewed and documented in full:

- `storage/api/storage_manager_api/models/__init__.py`, completing all **11**
  source modules in the public manager-model package.
- `storage/api/storage_manager_api/reconciliation_api.py` and `ingest_api.py`,
  including all concrete byte/file/identified/prepared-source conveniences.
- `storage/storage_manager/mixins/reconciliation.py` and `ingest.py`, including
  every nested publication and fallback callback.

Newly completed: **5 modules, 4 classes, 22 functions = 31 declarations**.
Cumulative: **505 modules, 719 classes, 5,976 functions = 7,200 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 505 unique
paths. All five files matched HEAD before editing. Existing model/regression docs
and shared `_support.py`/`_types.py` excerpts informed the contracts; these excerpts
are not new completed-file claims, and already reviewed test modules are not counted
again. No executable changes or runtime-doc exceptions were introduced.

## Contracts clarified

- Reconciliation planning captures nondeleted claims and advertised enumeration
  separately. COMPLETE enumeration can classify absence without inspecting a claim.
  An enumeration StorageError retains partial inventory and falls back to individual
  inspection. Unexpected exceptions and Asset lookup failures remain visible.
- Individual inspection compares size and optional digests, potentially using an
  authoritative SHA-256 stat digest instead of reading bytes. Planning does not update
  observations. Only unavailable inspection diagnostics enter plan errors; missing
  or corrupt classifications can coexist with a conclusive plan.
- The global Replica generation is captured after comparison. This does not establish
  a consistent snapshot of the original claims, inspected bytes, and final token.
  There is no separate Store-generation token in this implementation.
- Application processes missing, corrupt, then unavailable IDs, checking each record's
  Store membership. Lists are not deduplicated or required to be disjoint; later
  classifications win and updated-ID reports can repeat entries. Observations receive
  new timestamps/revisions and generic reasons rather than retaining detailed evidence.
- Application does not require a conclusive plan, reprobe bytes, promote matched
  claims, adopt unexpected objects, or repair physical content. Empty current plans
  report applied without advancing the generation. A later wrong-Store/lookup failure
  can leave earlier transient metadata writes before the final generation increment;
  rollback belongs to the supplied transaction adapter.
- Ordinary ingest consumes the caller's remaining stream in requested 1 MiB chunks,
  spilling its spool to disk above 8 MiB. False reads terminate before the bytes-type
  check; truthy non-bytes reads reject. Size and digest expectations are checked after
  consumption, before completed-operation lookup. The caller's reader stays open.
- Completed ordinary-stream retries still read/hash and compare normalized request
  expectations. Identified streams instead validate supplied identity structure, trust
  caller authority, and can return a completed result or reuse readable destination
  bytes without consuming the source. New publication passes the preferred digest and
  size to Store commit; extra declared digests are not independently hashed here.
- The API's default identified-stream method delegates to ordinary ingest and adds no
  independent SHA-256 requirement; the concrete mixin supplies that direct path.
  verify=False does not disable ordinary input identity checks or adoption hashing.
  Requested later verification can yield an unhealthy result without undoing publication.
- File ingest checks initial stat size and supplies it as the stream expectation.
  Same-size changes need not be detected without extra expected identity evidence.
  Missing original_name is filled without mutating the caller's metadata record.
- Prepared-source paths validate advertised authority/read consistency and retain the
  existing preparation. Native transfer uses authoritative SHA-256 and known size;
  the retained source version participates in retry identity but is not passed as a
  conditional token to import_from. StoreUnsupportedOperation around the entire
  completion attempt invokes fallback, potentially after earlier side effects.
- Adoption hashes existing bytes, finds or declares identity, and reuses a matching
  live Location claim without changing its mode or existing Asset metadata. A completed
  retry returns its prior result without stat/read. Asset/Replica writes precede final
  Item-link/completion metadata, so a late failure can retain partial registration.

## Verification

- Strict structural audit and normalizer passed for the five files and the full
  **505-file reviewed set**, with source-AST counts recorded above.
- Every executable AST matches HEAD after removing only leading literal docstrings.
  Signatures, annotations, assertions, runtime strings, and limits remain unchanged.
  Ruff result multisets match their baselines: one existing finding in the model
  initializer and ingest API, none in the other three files.
- Selected doctests and Store/API/manager/ingest/filesystem/archive/database regressions:
  **435 passed, 273 explicitly skipped integration examples, 1 failed**, 135.88s.
  The sole failure is the unchanged manager-composition size guard for
  `_policy_support.py`: **1,213 lines against 900**. Newly documented ingest and
  reconciliation mixins are **837** and **210** lines, within their unchanged ceilings.
- Full quality runner passed: 159 formatted files, annotations across 456 modules,
  221 protected dependency modules, lint/complexity, both production type checkers,
  188 strict-mypy files, and 37 rejected invalid examples per checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 19.39s.
- **29 isolated observations** passed over transient-manager memory Stores and one
  temporary local file, with explicit enumeration, reader, metadata, and native-transfer
  hooks. They exercise the snapshot/application boundaries, retry consumption, authority,
  local-file changes, adoption failures, and source-version/fallback distinctions above.
  They do not establish durable rollback, live remote behavior, or real backend-native
  transfer. Manager cleanup ran in finally; no harness corrections were needed.
- Every verification process completed with an observed terminal exit code. The seven
  known docstring-sensitive pytest guard failures remain unresolved: six earlier
  Core/CLI/terminal ownership failures plus the manager composition failure above.
  The earlier six were not rerun, and no guard was weakened or deselected.
- Root and data-submodule whitespace checks passed. All local links resolve in the
  index (113), main ledger (3), and this note (3). Final source recount confirms
  the reviewed and remaining-package totals. Existing data-generator edits and
  untracked bytecode remain retained.

Durable results:
`working-memory/test-results/docstrings-manager-ingest-reconciliation-2026-09-11-{regression,quality,contracts}.{log,done}`
and `docstrings-manager-ingest-reconciliation-2026-09-11-observations.json` in that
directory. Static reports:
`/tmp/liuxin-docstring-manager-ingest-reconciliation-batch-2026-09-11.json`,
`/tmp/liuxin-manager-ingest-reconciliation-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank docs remain **1,076 modules, 1,908 classes, 20,579 functions
= 23,563 declarations**. This batch completed existing incomplete descriptions.
Overlapping findings: 3,756 delimiter layout, 10,078 examples, 2,860 parameter fields,
3,767 return fields, 3,408 empty parameter descriptions, 4,453 empty return
descriptions, and 28 missing summaries.

## Next

Storage has **17 unreviewed API modules** and **10 unreviewed storage_manager
implementation modules**, plus application-manager, registry, repository, and workflow
owners elsewhere. Continue with private request/state/contracts and shared support,
remaining routing/composition exports and conveniences, and persistence adapters.
The remaining metadata, catalogue/cache, formats, tests/scripts/examples, inherited
code, and tracked data-submodule Python stay in scope. Retain the runtime-doc consumer
cautions and three narrow exceptions in the [main ledger](project-docstrings-2026-09-08.md).
