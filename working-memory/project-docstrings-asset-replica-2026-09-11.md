# Asset identity and Replica lifecycle — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[Store/operational checkpoint](project-docstrings-manager-stores-2026-09-11.md).
Branch remains `codex/project-docstrings`, based on `edf6bf05`, without a commit
or push. Existing unrelated changes and data-submodule work are retained.

Source-reviewed and documented in full:

- `storage/api/storage_manager_api/models/identifiers.py`, `asset_identity.py`,
  `assets.py`, and `replicas.py`.
- `storage/api/storage_manager_api/catalog_api.py` and `replicas_api.py`.
- `storage/storage_manager/mixins/catalog.py` and `mixins/replicas.py`.
- All of `tests/storage/api/test_storage_manager_docstrings.py`.

Newly completed: **9 modules, 16 classes, 47 functions = 72 declarations**.
Cumulative: **482 modules, 673 classes, 5,786 functions = 6,941 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 482 unique
paths. Shared-helper excerpts and the two read-ahead models below remain outside
that manifest.

## Contracts clarified

- NewType aliases return supplied inputs unchanged at runtime; they do not enforce
  integer types, positivity, or repository existence. Their existing attribute
  documentation literals remain unchanged.
- Asset models check selected size/ID comparisons and digest algorithm uniqueness,
  without reading bytes, enforcing integer/finiteness constraints, or generally
  validating metadata and policy references. Labels and nested containers remain
  retained. Digest normalization belongs to Digest, not the uniqueness helper.
- Replica values retain reported evidence without checking every state/flag/ID
  relationship. Placement hints affect equality but are excluded from hashing.
  Timestamp awareness does not convert to UTC. Verification health checks only
  VERIFIED enum identity and empty errors; aggregate readability trusts contained
  reports without validating their Asset ownership.
- Declaration validates policy references before deduplication. The shared lookup
  chooses the first ID with matching size and a non-conflicting digest overlap;
  digest sets need not be equal. Reuse retains the original record without merging
  new metadata, extra digests, or policy choices.
- Metadata replacement advances revision while retaining byte identity. Catalogue
  and Replica iterators capture ordered tuples of record references. Replica mode
  filtering uses identity, and tombstones remain visible unless callers filter them.
- Asset forgetting counts all Replica claims by default, including tombstones.
  Waiving that check does not waive composite, derivation, or Item references and
  does not cascade-delete claims or physical bytes.
- Replication closes the source reader after publication and before registration.
  Cleanup or registration errors can leave published bytes without the new claim.
  Optional verification records the outcome without automatically raising on an
  unhealthy report; a CORRUPT or UNAVAILABLE record may be returned.
- Verification selection resolves all explicit IDs and checks ownership/deleted
  state before inspecting any. Explicit subsets retain caller order; implicit
  scans default to the first healthy result. Earlier observations can persist when
  later verification fails. Argument conflicts are checked after Asset lookup.
- Removal deletes bytes before the separate metadata transaction. Its deletion
  flag records completion of the delete call and ignores that call's return value.
  Tombstones can coexist with intentionally preserved bytes. Forgetting checks an
  optional revision twice and accepts only StoreNotFound as absence evidence;
  storage observation and record mutation are not one atomic operation.
- The older documentation tests now describe their real limits: literal presence
  rather than nonblank quality, two known placeholder substrings, and parameter-name
  sets/return-field presence on shallow nonprivate functions. Empty prose, order,
  duplicate fields, and private/deeper fields are outside that bounded guard.
  Assertions and scope were preserved; broader source review/auditing still applies.

## Verification

- Strict structural audit and normalizer pass for all nine files and the full
  482-file reviewed set. Counts were recounted from source ASTs.
- Every executable AST matches HEAD after removing only leading literal
  docstrings. Signatures, annotations, assertions, runtime strings, NewType
  attribute literals, and limits are unchanged. No runtime-doc exception was
  added. Per-file Ruff result multisets match the baseline.
- Selected source/test doctests and Store/API/manager/ingest/filesystem/archive
  regressions, including the database reload/recovery suite: **450 passed,
  280 explicitly skipped integration examples**, 197.57s. Skips are not executed
  integration proof. The process was polled to observed exit zero; it was not
  restarted when a progress-log snapshot appeared unchanged.
- Full quality runner passed: 159 formatted files, annotation coverage across
  456 modules, 221 protected dependency modules, selected lint/complexity, both
  production type checkers, 188 strict-mypy files, and 37 rejected invalid examples
  per checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 26.55s.
- **27 isolated observations** passed for value validation limits, report
  predicates, digest-overlap reuse, metadata revisions/snapshots, tombstones,
  byte-absence checks, post-deletion metadata failure, post-publication reader-close
  failure, unhealthy replication returns, and eager explicit-selection validation.
  These used real transient-manager operations over memory Store bytes with
  isolated injected failures; no durable database was claimed. Managers and retained
  readers were closed in finally cleanup.
- The six known docstring-sensitive ownership-test failures remain outstanding;
  those guards were neither rerun nor weakened. Root and data-submodule whitespace
  checks passed. Every verification process completed with an observed exit code.

Durable logs/exit metadata:
`working-memory/test-results/docstrings-asset-replica-2026-09-11-{regression,quality,contracts}.{log,done}`.
Observations:
`working-memory/test-results/docstrings-asset-replica-2026-09-11-observations.json`.
Static reports: `/tmp/liuxin-docstring-asset-replica-batch-2026-09-11.json`,
`/tmp/liuxin-asset-replica-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, with no
parse failures. Missing/blank docs remain **1,078 modules, 1,908 classes,
20,590 functions = 23,576 declarations**; this batch replaces incomplete existing
docs. Overlapping findings: 3,783 delimiter layout, 10,169 missing examples,
2,884 parameter fields, 3,793 return fields, 3,513 empty parameter descriptions,
4,603 empty return descriptions, and 28 missing summaries. Structural counts
alone do not establish descriptive completeness.

## Next and read-ahead

Storage still has **29 unreviewed API modules** and **18 unreviewed
storage_manager implementation modules**, plus the application manager, registries,
repositories, and workflows outside that package. Continue with composite,
resolution, Item-link, and retrieval contracts/implementations, then remaining
policy, reconciliation, ingest, derivation, and persistence families.

Both `models/composites.py` and `models/resolutions.py` were read in full and
confirmed byte-identical to HEAD; they remain unedited and uncounted. Findings to
check against their implementations when continuing:

- Membership validates ID/sequence comparisons, optional nonblank labels, and NUL
  exclusion, without safe-path normalization or checking the required flag.
  Composite positions must compare equal to contiguous 0..n-1 after sorting;
  the supplied member sequence is not reordered, and repeated Asset IDs are allowed.
- Composite declarations validate an optional nonblank name; records do not repeat
  that name check. Attributes are unchecked in both. The availability assessment
  adds no constructor validation: readability is count equality plus no missing
  IDs/errors, not an independent probe or required-member check.
- Atomic/member resolutions validate only related Asset-ID equality and project
  retained Locations; constructing them does not check current physical health.
- Item resolution requires exactly one atomic/composite target, a nonblank retained
  role, and an ID not comparing at/below zero. Atomic selections reject composite
  members. Composite selections compare sets of whole membership values, requiring
  all required members and rejecting undeclared ones; duplicate resolutions can
  collapse in these set checks. Locations preserve the supplied resolution order
  and duplicates rather than sorting by sequence.

The remaining metadata, catalogue/cache, formats, source/test/script/example,
inherited code, and tracked data-submodule Python stay in scope. Preserve the
runtime-doc consumer cautions and three narrow exceptions in the
[main ledger](project-docstrings-2026-09-08.md).
