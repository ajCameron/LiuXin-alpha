# Legacy Test Salvage Checklist

Date: 2026-03-16

## Scope

- This checklist covers the `salvage_existing` bucket from [legacy-test-migration-manifest-2026-03-16.csv](legacy-test-migration-manifest-2026-03-16.csv).
- Current scope is narrow and concrete:
  - `16` remaining legacy DB property files
  - original source: `src/LiuXin_tests/test_databases/test_db_properties`
  - current alpha copy: `tests/support/test_databases/test_db_properties`

## Main Finding

- `salvage_existing` is not a porting problem yet.
- It is a normalization problem inside alpha.
- The support tree under `tests/support/test_databases` is the right authoritative target, but it still needs final cleanup and promotion work before the duplicate legacy tree can be deleted.

## Facts Established

- `salvage_existing` count: `16`
- The old `26`-row DB-property bucket has now split:
  - `10` minimal rows are `covered`
  - `16` larger semantic rows remain `salvage_existing`
- Duplicate alpha support trees compared:
  - `src/LiuXin_tests/test_databases`
  - `tests/support/test_databases`
- Mapped file pairs across those trees:
  - `203` total
  - `190` identical
  - `13` divergent
- Files under `tests/support/test_databases` still importing `LiuXin_tests`: `0`

## Recommended Authoritative Tree

- Treat [tests/support/test_databases](../tests/support/test_databases) as authoritative.

Reason:
- it already sits under the modern test-support area
- it is where the alpha migration should converge anyway
- the divergent property files there are already closer to current alpha driver assumptions than the older `src/LiuXin_tests` copies
- once the salvage batch is stabilized, the duplicate `src/LiuXin_tests/test_databases` copy should be removed from alpha rather than kept as a shadow tree

## Divergent Files Reviewed

- `__init__.py`
- `test_db_4/__init__.py`
- `test_db_properties/__init__.py`
- `test_db_properties/test_db_0_properties.py`
- `test_db_properties/test_db_11_properties.py`
- `test_db_properties/test_db_12_properties.py`
- `test_db_properties/test_db_17_properties.py`
- `test_db_properties/test_db_19_properties.py`
- `test_db_properties/test_db_1_properties.py`
- `test_db_properties/test_db_20_properties.py`
- `test_db_properties/test_db_2_properties.py`
- `test_db_properties/test_db_3_properties.py`
- `test_db_properties/test_db_4_properties.py`

Representative divergence:
- `src/LiuXin_tests/test_databases/test_db_properties/test_db_0_properties.py`
  still imports `SQLite_apsw`
- [tests/support/test_databases/test_db_properties/test_db_0_properties.py](../tests/support/test_databases/test_db_properties/test_db_0_properties.py) imports `SQLite`

That is exactly the kind of drift this normalization pass needs to preserve intentionally, not erase by accident.

Review outcome:
- [src/LiuXin_tests/test_databases/__init__.py](../src/LiuXin_tests/test_databases/__init__.py) and [tests/support/test_databases/__init__.py](../tests/support/test_databases/__init__.py) are now identical again.
- Most of the remaining divergence is intentional alpha normalization:
  - relative-import cleanup
  - `_legacy` helper usage
  - wrapper-based utility imports
  - docstring cleanup
- Three files carry a real alpha-side driver adaptation and should keep the support-tree version:
  - `test_db_properties/test_db_0_properties.py`
  - `test_db_properties/test_db_11_properties.py`
  - `test_db_properties/test_db_12_properties.py`
- detailed review note:
  - [legacy-test-divergent-files-review-2026-03-16.md](legacy-test-divergent-files-review-2026-03-16.md)

## Normalization Sequence

1. Freeze the decision that `tests/support/test_databases` is the authoritative alpha copy.
2. Review the `13` divergent files explicitly and keep the alpha-oriented variants where that divergence is intentional.
3. Rewire imports inside [tests/support/test_databases](../tests/support/test_databases) so they stop importing from `LiuXin_tests...`.
4. Move any still-useful helper modules from `src/LiuXin_tests/test_databases` into `tests/support/test_databases` or another neutral support location.
5. Only after the support tree is self-contained:
   - promote the remaining high-signal property files into collected contract coverage, or
   - keep them as support fixtures with explicit adapters
6. After import cleanup and promotion decisions, archive or remove the duplicate `src/LiuXin_tests/test_databases` tree.
7. Keep the original LiuXin repo as the historical archive, not the alpha repo itself.

## Import Rewire Targets

The import cleanup was mandatory and is now complete. The support tree no longer reaches back into the duplicate legacy tree.

High-priority import surfaces:
- `tests/support/test_databases/test_db_properties/__init__.py`
- all `tests/support/test_databases/test_db_properties/test_db_*_properties.py`
- builder modules like:
  - `test_db_4/__init__.py`
  - `test_db_5/__init__.py`
  - `test_db_10/__init__.py`
  - `test_db_11/__init__.py`

The first target is not “make the old tests pass”.
The first target is “make the alpha support tree stand on its own”.

Progress:
- the property package has already been decoupled from `LiuXin_tests.test_databases.test_db_properties...`
- the full support tree is now decoupled from `LiuXin_tests`
- `_legacy` helper shims now live under:
  - `tests/support/test_databases/_legacy`
- the legacy `clint` dependency is now routed through:
  - [liuxin_clint.py](../src/LiuXin_alpha/utils/libraries/liuxin_clint.py)
- the legacy `tqdm` dependency is now routed through:
  - [liuxin_tqdm.py](../src/LiuXin_alpha/utils/libraries/liuxin_tqdm.py)
- validation completed:
  - no remaining `from LiuXin_tests` or `import LiuXin_tests` under `tests/support/test_databases`
  - full `py_compile` over `tests/support/test_databases` passed
- package root import now succeeds:
  - `import tests.support.test_databases`
- remaining blocker is now different:
  - deeper builder imports still depend on missing/renamed project surfaces, with `LiuXin_alpha.folder_stores` currently the first visible one
  - this is now treated as a hard rewrite boundary, not a shim candidate
- detailed current state:
  - [legacy-test-salvage-import-map-2026-03-16.md](legacy-test-salvage-import-map-2026-03-16.md)
  - [liuxin-clint-shim-2026-03-16.md](liuxin-clint-shim-2026-03-16.md)
  - [liuxin-tqdm-shim-2026-03-16.md](liuxin-tqdm-shim-2026-03-16.md)
  - [folder-stores-rewrite-boundary-2026-03-16.md](folder-stores-rewrite-boundary-2026-03-16.md)

## Do Not Do First

- Do not port new property tests until the authoritative support tree decision is locked.
- Do not keep both trees live indefinitely.
- Do not delete `src/LiuXin_tests/test_databases` before the support-tree promotion decision is made.
- Do not normalize by blindly copying one tree over the other; the divergences include alpha-specific adaptations.

## Practical Next Step

The divergent-file review is now done.

Next work is split cleanly into:
- continued DB-property/support-tree cleanup
- separate storage rewrite planning for the legacy store-backed builders that cross into `folder_stores`

Reference:
- [folder-stores-rewrite-plan-2026-03-16.md](folder-stores-rewrite-plan-2026-03-16.md)


## Promotion Progress

The first collected promotion slice is now in place:
- [db-property-support-registry-2026-03-16.md](db-property-support-registry-2026-03-16.md)
- the support corpus is no longer inert
- but the old per-DB table/value declarations are still stale relative to the current alpha schema

So the next work here is not “import the files”; it is selective normalization of still-meaningful expectations.

First live slice of that normalization is now in place:
- [db-property-alpha-subset-2026-03-16.md](db-property-alpha-subset-2026-03-16.md)
- shared alpha-schema subset declared in [common_db_properties.py](../tests/support/test_databases/test_db_properties/common_db_properties.py)
- per-DB alpha row counts are now declared for all `26` support classes
- collected validation lives in [test_property_alpha_schema_subset.py](../tests/support/test_databases/test_db_properties/test_property_alpha_schema_subset.py)

That work is deliberately additive:
- it does not claim the old `theo_*` declarations are migrated
- it pins a narrow alpha-native contract next to them

The next honest split has now happened too:
- [db-property-salvage-split-2026-03-16.md](db-property-salvage-split-2026-03-16.md)
- `10` minimal rows are now `covered`
- `16` larger semantic rows remain here in `salvage_existing`

And the next seam review is now recorded in:
- [db-property-simple-seam-review-2026-03-16.md](db-property-simple-seam-review-2026-03-16.md)
- only `test_db_13_properties.py` moved in that pass

The next live replacement seam has also landed:
- [db-property-blank-optional-metadata-2026-03-16.md](db-property-blank-optional-metadata-2026-03-16.md)
- it pins the current blank optional-metadata profile for `13` of the remaining DBs
- but it does not justify additional `covered` moves, because the legacy rows still carry stale semantic maps beyond that profile

The remaining backlog is now split by actual semantics:
- [db-property-remaining-clusters-2026-03-16.md](db-property-remaining-clusters-2026-03-16.md)
- that replaces the old flat `16`-row salvage bucket with five explicit clusters
