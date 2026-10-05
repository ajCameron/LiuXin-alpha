# Configured Store API completion — 2026-09-10

Continuation of the [whole-project descriptive reST pass](project-docstrings-2026-09-08.md),
following [Store foundations](project-docstrings-store-foundation-2026-09-10.md).
The preceding goal turn made verified progress; this turn completes the remaining
Store API source package and two adjacent regression modules. The full project
goal remains unfinished, without an overall blocker. Branch remains
`codex/project-docstrings` at `edf6bf05`; no commit or push was made.

## Completed review

- Complete [file primitives and optional protocols](../src/LiuXin_alpha/storage/api/store_api/file_api.py).
- Complete [Store convenience methods](../src/LiuXin_alpha/storage/api/store_api/convenience_api.py).
- Complete [driver-backed Store bridge](../src/LiuXin_alpha/storage/api/store_api/driver_backed_api.py),
  including its private write-session adapter and static mode checker.
- Complete [configuration-factory regressions](../tests/storage/api/test_store_configuration_convenience.py).
- Complete [driver-error regressions](../tests/storage/test_driver_error_messages.py),
  including nested transport clients, exception classes, and fault functions.

Batch: **5 modules, 15 classes, 110 functions = 130 declarations**. Cumulative:
**347 modules, 411 classes, 3,901 functions = 4,659 declarations**. Exact paths
are in the [reviewed manifest](project-docstrings-reviewed-files.txt).
All eight configured Store API modules, all eight raw driver API modules, and
all five shared storage utility modules are now reviewed. This does not complete
the other storage API families or concrete implementations.

## Contracts clarified

- Write-session publication and cleanup requirements remain explicit, including
  the distinction between complete publication and an advertised atomic step.
  The session example now handles partial acceptance rather than assuming one
  write consumes its entire input. Runtime protocol membership does not establish
  these operational guarantees.
- StoreFileAPI helpers rely on concrete primitives for routing and result checks.
  get/read_bytes omit a None version keyword; read_bytes returns the stream result
  directly and has no independent memory cap. put validates size/chunk bounds,
  handles partial writes, gates placement-hint forwarding, and owns the session
  while borrowing its input. Falsey chunks end transfer before type checking.
- **Copy behavior differs from the free utility:** StoreFileAPI.copy stats once
  and pins the source read when conditional_read and a non-None version are both
  available. Its move first observes a deletion version, calls self.copy (which
  can observe again or select an override), and conditionally deletes using the
  first token. Later deletion/cleanup failure can follow destination publication.
- Native protocols remain capability-gated; structural matches can include
  inherited streaming methods. Native import is destination-side cross-Store
  transfer with explicit source compatibility and authoritative size/digest input.
- Convenience FileInfo arguments supply only identity, not an implicit version
  condition or cached stat. Bytes-like input is normalized before dispatch, local
  files are statted before opening, and borrowed streams are not rewound/closed.
  Metadata projection precedes capability checks, destination allocation precedes
  mode validation, and unsupported allocation receives configured-name context.
  Advisory projection can return None; errors outside its own handling propagate.
- The driver session adapter permits zero acceptance, rejects negative/excessive
  counts after the raw write, and accumulates accepted bytes without a separate
  finished-state guard. Commit checks metadata after raw publication, supplies an
  unknown size from that count, and preserves a known size without recounting.
  It discards raw enter/exit return values, so a truthy raw exit cannot suppress a
  body exception. Abort idempotence belongs to the driver. Both constructor
  examples now include the required expected_address argument.
- Driver-backed routing requires canonical raw addresses whose address-space UUID
  equals the configured Store UUID; it does not remap a foreign scope. Existing
  Locations receive only UUID ownership checks in locate; actual operations parse
  their keys. Allocation checks protocol/capability but ignores placement hints
  and does not apply configured read-only policy by itself.
- Capability projection corroborates native copy/move/digest and paging protocols;
  other flags are forwarded directly. Read-only configuration clears four mutation
  flags while leaving underlying native mechanics visible; mutation methods still
  reject that policy. The default placement_hints claim remains false.
- Read-only characteristics remove STORE_COPY publication staging, retaining
  OBJECT_STAGE read spooling. Status translation masks writability without probing,
  recomputing availability, or redacting diagnostics. _require_writable checks
  configuration only, not endpoint health/capacity.
- Default ingest profiles derive guarantees from raw conditional/range/page flags
  and advertise no authoritative digest algorithms. Preparation refreshes only
  inventory entries, falls back only on StoreUnsupportedOperation, and can retain
  a supplied FileInfo even with inspect=True. Prepared opening validates claims
  and offset policy, then pins only VERSION_PINNED reads; it adds no byte hashing.
- Native copy/move validate metadata after the operation. Native move forwards
  the source stat version even when None. Native digest ignores chunk_size and
  trusts the delegated algorithm/value; missing advertised protocols raise rather
  than trigger fallback. Configured-policy and operation checks precede routing
  in the mutation paths.
- Inventory iterators validate canonical addresses and detect duplicates using a
  growing seen set. Known-size FileInfo entries reuse inventory metadata; unknown
  sizes trigger stat. Rich inventory preserves unknown sizes without stat. The
  known-size/inventory paths do not independently enforce the stat-digest flag.
  Pages are translated eagerly, keep raw tokens, and have no cross-page tracking.
- Tests distinguish real temporary paths/corrupt SQLite bytes from injected full-
  disk and remote faults. Configuration-factory tests do not create or probe Store
  roots. Error regressions check selected typed/contextual/redacted outcomes,
  without claiming live transports or exhaustive secret filtering.

## Verification

- Strict audit and normalizer pass on all five files and all **347 reviewed
  files**. Both eight-module API packages and all five utilities are confirmed
  present in the manifest.
- All five executable ASTs match HEAD after removing only leading literal
  docstrings. Signatures, annotations, assertions, and implementation statements
  are unchanged; no new runtime-doc exception was needed.
- Ruff code/message counts match baseline: file API 3, convenience 3, driver-backed
  bridge 4, configuration tests 0, driver-error tests 1. No gate, limit, or
  suppression changed.
- Batch doctests plus driver/manager API, advanced ingest, Store-redraft,
  example-marker, placement-hint, and full Store-ingest regressions:
  **210 passed, 144 explicitly skipped integration examples**.
- Full quality runner passed: 159 formatted files, 456 annotation-covered modules,
  221 protected dependency modules, selected lint/complexity/production typing,
  188 strict-mypy files, and 37 invalid examples rejected by each checker.
- Final review clarified two allocation-hint field descriptions after the
  behavioral run; examples and executable statements were unchanged. Batch
  structural and AST/lint checks were repeated afterward.
- Final migration, public-documentation, and developer-link contracts:
  **38 passed**. All eight local links in this handoff, three in the main ledger,
  and 113 in the index resolve. Root and data-submodule diffs are whitespace-clean,
  as are the changed Markdown files. Every verification handle has completed;
  no run remains pending.
- Six previously recorded ownership checks still conflict with physical docstring
  lines/statements; they were not rerun for this unrelated scope. No counting
  policy or ceiling was weakened.

Logs/exit metadata: `working-memory/test-results/docstrings-store-completion-2026-09-10-*`.
Static results: `/tmp/liuxin-docstring-store-completion-batch-2026-09-10.json`,
`/tmp/liuxin-store-completion-ast-lint-2026-09-10.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

## Remaining work

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, with no
parse failures. Missing/blank docs: **1,087 modules, 1,967 classes, 21,549
functions = 24,603 declarations**. Overlapping findings: 3,984 delimiter-layout,
10,310 missing-example, 2,961 parameter-field, 3,886 return-field, 3,938 empty
parameter descriptions, 5,313 empty return descriptions, and 28 missing summaries.

Next review the six `storage/stores` modules (exports, filesystem, HTTP, SQLite,
S3, and encryption), concrete drivers and their shared error/validation helpers,
and adjacent tests. Store model/capability definitions and the placement-hint
projector were read as dependencies, not completed files; the large manager API
regression module was executed but remains outside the reviewed documentation
manifest. Do not repeat the now-complete Store/driver API packages or utilities.
Continue through all remaining source, tests, scripts, examples, inherited Python,
and tracked data-submodule Python. Preserve runtime-doc consumer cautions and
the three narrowly checked metadata exceptions in the main ledger.
