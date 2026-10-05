# Storage API values and placement hints — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[local-plugin checkpoint](project-docstrings-local-plugins-2026-09-10.md).
Branch remains `codex/project-docstrings`, based on `edf6bf05`, without a commit
or push. Existing unrelated changes and data-submodule work are retained.

Source-reviewed and documented in full:

- `storage/api/models.py`, `characteristics_api.py`, `errors.py`, `location_api.py`,
  `placement_hints_api.py`, and the API package initializer.
- The storage package initializer and separate legacy `storage/errors.py`.
- `tests/storage/api/test_placement_hints_api.py` and
  `tests/storage/api2/test_location_api2.py`, including nested providers and the
  recording router's entire implementation.

Newly completed: **10 modules, 47 classes, 72 functions = 129 declarations**.
Cumulative: **461 modules, 612 classes, 5,475 functions = 6,548 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 461 unique
paths. Seven source files already had unverified documentation edits at orientation;
this checkpoint completes and verifies that batch, including placement hints/tests.

## Contracts clarified

- Common values distinguish reported facts and intended typed contracts from the
  limited checks actually performed. Location validates UUID identity and key
  truth/NUL membership without a runtime string check. Digest normalizes text
  without validating algorithms, encoding, or digest length. Numeric comparisons
  generally do not enforce integer types or finiteness. Timestamp checks require
  awareness without converting zones.
- Inventory pages reject duplicate Locations within that page and empty token
  strings. They do not enforce one Store owner, validate a snapshot, or check
  cross-page duplication. FileInfo-to-inventory conversion shares nested values.
- Capability validation checks only its selected dependencies and retains the
  enumeration argument without enum coercion. Characteristics instead coerce their
  three enums and check positive declared bounds and unique limitation codes.
  Unknown limits do not establish support; status claims are not live probes.
- Four hint dataclasses add no validation or recursive freezing. to_mapping uses
  dataclasses.asdict, recursively copying rather than exposing a shallow view.
- Existing hints/mappings pass through by identity. Provider-call Exceptions are
  suppressed, but provider lookup and unguarded projection errors can propagate.
  Invalid provider results fall through to item/work/manifestation/expression
  structural dispatch. Relation materialization catches KeyError/ValueError/TypeError,
  including iteration failures; reads can recur and do not form a snapshot.
- Work and Item projections normalize format candidates and derive folder/name
  suggestions. Expression language_code can contain a display name; Manifestation
  file_formats retains link display text. Agent-selection/uniqueness rules differ
  between projections. Primary None targets stop selection, and Item manifestation
  ID fallback uses truthiness before integer conversion.
- Display/key helpers preserve their exact priorities and whitespace behavior.
  Format normalization can produce empty tokens or identical uppercase output from
  distinct lowercase Unicode inputs. Optional integer conversion accepts booleans
  and truncatable floats but lets OverflowError escape. Suggested keys are neither
  validated, reserved, nor checked for existence.
- API Store error names are aliases of the shared Storage-prefixed classes.
  Package-level StorageError belongs to the separate legacy LiuXinException hierarchy.
  Lazy package exports cache successful resolutions and retain import/lookup failures.
- Regression docs distinguish in-memory metadata round trips and forwarding checks
  from database persistence, digest verification, and complete backend guarantees.

## Verification

- Strict audit and normalizer pass on all ten files and the full 461-file reviewed
  set. Final batch totals and the cumulative manifest were recounted from ASTs.
- All ten executable ASTs match HEAD after removing only leading literal docstrings.
  Signatures, annotations, assertions, runtime strings, and limits are unchanged.
  No runtime-doc exception was added. Final Ruff result multisets match baseline.
- Batch/filesystem doctests plus manager, Store, API, ingest, archive, and filesystem
  regressions: **413 passed, 151 explicitly skipped integration examples**, 31.69s.
  Skips are not executed integration proof. The first run caught an incorrect
  exception class spelling in one new doctest; its expected text was corrected and
  the complete selection rerun. The initial failure log/marker is retained.
- Full quality runner passed: 159 formatter files, annotation coverage across 456
  modules, 221 protected dependency modules, selected lint/complexity, both production
  type checkers, 188 strict-mypy files, and 37 rejected invalid examples per checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 18.42s.
- **32 isolated observations** passed for copying/mutability, provider and relation
  failure boundaries, dispatch/selection/repeated reads, format/display conversions,
  actual value validation, timestamp awareness, and exception hierarchy identity.
  The initial observation script omitted the required inventory next_cursor argument;
  the temporary harness was corrected before the successful run. No production
  behavior or regression assertion was changed to accommodate either failure.
- The six known docstring-sensitive ownership-test failures remain outstanding.
  These unrelated guards were neither rerun nor weakened by this batch.
- Root and data-submodule whitespace checks passed. Regression, quality, contracts,
  cumulative audit, and whole-project audit handles all completed with observed
  exit-zero results. The data generator and existing bytecode artifacts remain intact.
- Final local-link checks resolve all 113 index links, three main-ledger links,
  and three links in this note. The three read-ahead modules remain byte-identical
  to HEAD and absent from the completed manifest.

Durable logs/exit metadata:
`working-memory/test-results/docstrings-storage-values-2026-09-11-{regression,quality,contracts}.{log,done}`.
The first regression result is retained with the `regression-initial` suffix.
Observations: `working-memory/test-results/docstrings-storage-values-2026-09-11-observations.json`.
Static reports: `/tmp/liuxin-docstring-storage-values-batch-2026-09-11.json`,
`/tmp/liuxin-storage-values-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, with no
parse failures. Missing/blank docs: **1,078 modules, 1,922 classes, 20,749 functions
= 23,749 declarations**. Overlapping findings: 3,785 delimiter layout, 10,217
missing examples, 2,884 parameter fields, 3,793 return fields, 3,612 empty parameter
descriptions, 4,752 empty return descriptions, and 28 missing summaries. Structural
counts alone do not establish descriptive completeness.

## Next: manager routing and location handles

Storage has **43 unreviewed API modules** and all 22 storage_manager implementation
modules still to cover, plus its remaining registries, repositories, and workflows.
Continue with manager `location_api.py` (431 lines), `router_api.py` (412), and
`location_factory.py` (163), then adjacent manager contracts/models and persistence.
Those three complete source files were read ahead, but remain unedited, unchanged
from HEAD, and outside the reviewed manifest. Relevant findings for that next pass:

- BoundLocation and LocationFactory are frozen but eq=False, retaining identity
  equality and references to live managers. Construction adds no shape validation.
  BoundLocation caches no metadata; None read versions omit the keyword when
  delegating, preserving compatibility with narrower test routers.
- Router read_bytes closes its returned reader and reads the full selected stream.
  write_bytes wraps data in BytesIO, passes len(data), and adds no explicit close.
  try_stat catches only StoreNotFound; default characteristics is an unknown profile.
- Router copy stats the source, optionally guards the read when conditional_read
  and a version are available, then forwards observed size/digest to put. The
  destination implementation owns validation. move separately stats/checks conditional
  deletion before calling copy, which stats again; deletion uses the first version.
  A later failure may leave the destination published, with no rollback here.
- LocationFactory delegates all selection/verification to its supplied manager.
  Its from_digital_asset_id method forwards through from_id; Replica resolution
  makes no selection among alternative Replica IDs in this wrapper.

Do not reduce the goal to storage: remaining metadata, catalogue/cache, formats,
source/test/script/example/inherited code and tracked data-submodule Python remain
included. Preserve the runtime-doc consumer cautions and three narrow exceptions in
the [main ledger](project-docstrings-2026-09-08.md).
