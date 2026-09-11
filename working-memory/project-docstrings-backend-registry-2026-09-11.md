# Backend registry and Unicode contracts — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[storage configuration checkpoint](project-docstrings-storage-configuration-2026-09-11.md).
Branch remains `codex/project-docstrings` at `edf6bf05`, without commit or push.
Existing changes and data-submodule work are retained.

Read the complete 1,232-line backend_registry.py, including all 25 builders and
the full default descriptor/profile declarations. Documented its module, three
classes, all 41 named functions, and every field of both passive dataclasses.
Also read and documented the complete shared Unicode contract package initializer,
unicode_paths helper, four-backend matrix test module, and storage_unicode fixture
module. Runtime descriptor values, Unicode fixture literals, and assertions remain
unchanged. All five files matched HEAD before editing; combined length grew from
1,603 to 2,404 lines through docstrings only.

Newly completed: **5 modules, 5 classes, 47 functions = 57 declarations**.
Cumulative: **559 modules, 793 classes, 6,577 functions = 7,929 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 559 unique
paths. Only the application-facing storage/store_manager.py remains outside the
reviewed storage source set; its orchestration/mixin package is already complete.

## Contracts clarified

- Runtime construction context and backend descriptors are shallow frozen values,
  without interface/capability validation. Client/provider/resolver references are
  borrowed and can remain mutable. Descriptor profiles describe declared family
  properties, without probing a configured backend or certifying availability.
- Registry registration requires canonical kind spelling and checks collisions
  with already registered kind/alias names before updating its dictionaries.
  Duplicate normalized aliases within one descriptor are accepted. Access-protocol
  labels/aliases are separate metadata and are not automatically kind aliases.
- Normalization stringifies, strips, lowercases, and replaces hyphens; it does not
  validate registry membership or reject every unusual character. Enumeration
  snapshots/sorts on first generator advancement, includes earlier additions, and
  excludes later ones. The shared default registry remains mutable and unlocked.
- Build uses the original configuration and a truthy supplied context or a new
  context. Asset-backed intent requires read-only policy, a read-only file
  descriptor, and a non-None backing-path resolver. This is presence checking,
  not callability or Asset validation. Unbacked intent bypasses those checks.
  Custom builder results are not type/identity-checked; no lifecycle, rollback,
  persistence, or cleanup layer is added by the registry.
- Compatibility managed/unmanaged/flat/Calibre-like/SQLite builders project only
  root/name/UUID. HTTP additionally selects three request/inventory options,
  ignoring other options and preserving explicitly supplied None. Native HTML,
  wget, and FTP forward all options through their option constructors, which reject
  unknown names. These adapters do not pass full configuration to their backends.
- Filesystem and S3 delegate to their specialized configuration factories; S3
  receives the context client. Rclone preserves full configuration and writable
  construction removes/stringifies local_staging_directory before process-option
  construction. Configuration option pairs are not mutated.
- Six read-only image builders use the direct/Asset-resolved path, retain full
  configuration, and forward all option keywords. Resolver effects can precede
  constructor/keyword errors; even a configuration keyword collision occurs after
  path resolution. Five writable/build-once image builders use direct path
  projection and do not resolve backing themselves; the registry entry point owns
  their backed-view rejection. Constructors can create paths/staging/empty images
  before later validation failures.
- Local file URI handling accepts only empty or literal localhost authority and
  decodes percent-encoded bytes through os.fsdecode, retaining POSIX surrogate
  escapes. Query/fragment are ignored. Non-file URI strings and plain/relative
  paths pass through unchanged, without general containment/existence validation.
  Backing resolver results receive no further path decoding or type checks.
- Encryption requires resolver/provider presence before option parsing. A
  non-None explicit inner UUID wins over URI fallback, even when invalid. Unknown
  options reject before inner-Store resolution. Persisted key_id is removed from
  runtime constructor arguments; the active runtime provider supplies the key,
  while the original configuration can retain a different key_id. The wrapper
  borrows the inner Store by default and owns its normal staging behavior.
- The Unicode family-set test compares declarations with registry membership;
  it does not discover or execute behavioral coverage. The separate matrix runs
  eight cases against four real filesystem-derived plugin families.
- Single-case Unicode checking optionally compares a seeder-return location,
  checks exactly one inventory key, stat/filename, two full reads and one selected
  byte slice, and always requests a URI even when round-trip checking is off.
  Filename comparison uses the original case despite a key override. Bulk checks
  seed every case first, discard seeder results, then recompute keys during checks.
  Failures can leave partial seeding and no partial result; key mapping must stay
  consistent across both phases. Helpers add no version snapshot or cleanup.
- Fixture filename projection preserves exact text, while URL quoting uses strict
  UTF-8 and can reject lone surrogates. Raw POSIX filename bytes use a separate
  os.fsdecode fixture path; fixture constants and expected payloads are unchanged.

## Verification

- Strict structural audit and normalizer passed for all five new files and the
  complete **559-file reviewed set**. No reviewed audit findings or parse errors.
- Every edited executable AST matches HEAD after removing only leading literal
  docstrings. Signatures, annotations, aliases, descriptor/profile values, fixture
  bytes, assertions, and guard limits remain unchanged. No runtime-doc exception
  was added. Ruff remains **5 to 5**, with no newly introduced finding.
- Selected production/test doctests and backend registry, Unicode/local plugins,
  filesystem, encryption, S3 doubles, SQLite, backed Stores, local archives,
  ISO/7z, and manager regressions: **369 passed, 261 skipped, 1 failed**, 69.76s.
  All skips were explicit +SKIP integration examples, including previously
  documented adjacent modules. The 32-case matrix and actual local backend
  regressions passed. This does not claim live S3 or remote-service verification.
- The sole failure remains
  `test_storage_manager_composition.py::test_manager_module_stays_a_small_composition_root`:
  _policy_support.py 1,213, _support.py 1,139, and _contracts.py 902 exceed the
  unchanged 900-line ceiling. These are one failing pytest test. No edited file
  is an offender. All seven previously recorded docstring-sensitive pytest guard
  failures remain unresolved; the six earlier Core/CLI/terminal failures were not
  rerun. No guard was weakened or deselected.
- Full quality runner passed: 159 formatted files, 456 annotated modules,
  221 protected dependency modules, lint/complexity, both production type checkers,
  188 strict-mypy files, and 37 invalid calls rejected by each checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 30.03s.
- **22 targeted observations** passed on their first execution. Pure/double-based
  checks cover all builder families, validation ordering, alias collisions,
  iteration timing, URI/coercion behavior, and Unicode assertion boundaries.
  Additional real local checks verify encrypted write/readback with differing
  persisted/runtime key IDs, ZIP construction/readback through a file URI containing
  undecodable POSIX bytes, and all eight Unicode cases together with URI round trips.
  Stores and temporary files were closed/cleaned up; no remote service was called.
- All verification processes completed with observed terminal exits. Source was
  unchanged after validation began.
- Final independent recount confirms 559 unique reviewed modules, 793 classes,
  6,577 functions, and only store_manager.py remaining in the storage source tree.
  Root/data-submodule whitespace checks pass, and all 119 local link targets across
  the index, main ledger, and this note resolve. The existing data-generator edit
  and untracked data-submodule bytecode cache remain untouched.

Durable results:
`working-memory/test-results/docstrings-backend-registry-2026-09-11-{regression,quality,contracts}.{log,done}`
and `docstrings-backend-registry-2026-09-11-observations.json` in that directory.
Static reports: `/tmp/liuxin-docstring-backend-registry-batch-2026-09-11.json`,
`/tmp/liuxin-backend-registry-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank docs remain **1,070 modules, 1,907 classes, 20,370 functions
= 23,347 declarations**, down 38. Overlapping findings: 3,608 delimiter layout,
9,769 examples, 2,784 parameter fields, 3,673 return fields, 3,182 empty parameter
descriptions, 4,156 empty return descriptions, and 28 missing summaries.

## Next

Read and document the complete application-facing storage/store_manager.py
(1,499 lines). Constructor excerpts and previously selected database reload tests
are not completed-source claims. The database reload regression module and
tests/storage/_mini_db.py still need full review/documentation; the registry test
module was completed in the preceding batch. Other remaining storage tests are
also still in scope after the storage source milestone.

Continue the wider source/test/script/example/inherited/data-submodule backlog.
Preserve runtime-doc consumer checks and the three narrow exceptions recorded in
the [main ledger](project-docstrings-2026-09-08.md); the whole-project goal remains
active and unfinished.
