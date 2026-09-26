# Ten-unit metadata documentation batch: M027–M036 complete

The explicit request for **ten more units** authorized **M027–M036** in inventory
order. All **265 newly selected declarations across 11 files** are verified.
**Ten files are now complete**, containing 265 declarations: 225 newly reviewed
here plus the 40 title API declarations already reviewed in M026. The new
**40-declaration concrete WEMI slice remains partial at 40/92 declarations**.
**M037 is next**, with 40 declarations in
`src/LiuXin_alpha/metadata/containers/metadata_containers/liuxin_wemi_metadata.py`.
All 52 later declarations (M037's 40 and M038's 12) retain identical source
segments and docstrings. The requested ten are complete; no further units are
authorized yet.

| Unit | Declarations |
|---|---:|
| M027 | 17 |
| M028 | 40 |
| M029 | 24 |
| M030 | 24 |
| M031 | 23 |
| M032 | 17 |
| M033 | 32 |
| M034 | 15 |
| M035 | 33 |
| M036 | 40 |

The batch completes the title API, eager/lazy WEMI protocols, non-WEMI row and
same-table relation contracts, the public concrete facade, compact formatting,
lazy field mappings, the lazy WEMI container and lazy database hydrator. It also
reviews construction, assignment, properties, title selection and diagnostic
rendering in the first concrete WEMI slice.

Source-review findings documented without changing executable behavior:

- Successful LazyValueToID loads are cached; failed loads remain retryable.
  Reading, mutation, iteration, length, truth testing and deepcopy can load the
  mapping. An unloaded repr does not. Materialize returns live storage; deepcopy
  returns a plain OrderedDict. There is no synchronization lock.
- Lazy relation loaders are removed before invocation; failures do not reinstall
  them. Identifier synchronization marks its flag before syncing and leaves it
  set after a failure. A wrapper can remain in lazy_fields after internal loading
  until hydrate_field replaces it with the materialized mapping.
- force_hydrate handles legacy fields and identifiers, while load without fields
  also drains other pending relation loaders. Deepcopy hydrates source legacy
  fields and can retain deferred callables with their original captured resources.
- The lazy hydrator resolves the selected identity chain eagerly. Preferred links
  override source-row parent-id hints. Other relation buckets remain deferred and
  retain the caller-owned database. Schema and query fallbacks do not suppress
  every malformed payload error.
- Row conversion ignores unknown columns; primary_id requires an exact int.
  Inline tree validation checks row types, parent consistency, position and
  duplicate child ids, without promising detection of longer graph cycles.
- Bundle/identity properties expose live objects, while stack/id dictionaries and
  projection wrappers are newly constructed. None bundle assignments create empty
  bundles. Non-None bundle coercion does not enforce runtime types. Title choices
  have explicit precedence; diagnostic accessors may invoke deferred behavior.
- The legacy null-field selection inversion, cleanup behavior and lossy Calibre
  conversion remain as implemented. Protocol docs describe the concrete behavior
  where needed without executing interface stubs.

Verification:

- All **265 new declarations** pass the docstring audit and normalization checks.
  Full-file checks also cover all **265 declarations in the ten completed files**,
  including the prior title API slice. Executable AST, signatures, decorators,
  annotations, comments, data literals and historical baseline hashes are
  unchanged. Unselected docs are identical. Ruff adds no findings and the diff
  has no whitespace errors.
- **207 regressions passed**, with no failures, skips or expected failures:
  API/export contracts **35**; legacy/WEMI/mapping/row/conversion/string consumers
  **92**; item hydration, edge cases and OPF adapters **80**. The existing utcnow
  deprecation warning remains. Lazy database behavior is exercised through existing
  fake/read-source fixtures, and row schema checks use local SQLite; this does not
  establish live PostgreSQL coverage.
- All **181 runnable example statements passed**, with no doctest skips. Modules
  are imported only when their selected docs have runnable examples, using isolated
  preference/config directories. **177 pytest command examples** have valid test
  paths/selectors and are counted separately from executed statements.
- Original scope has no source-level runtime doc dispatch. Two WEMI API tests use
  inspect.getdoc assertions: the required `normalized relation bucket key` wording
  and `RELATION_KEYS` reference are retained and their tests pass.
- Full configured quality runner passed: **156** formatted files, annotation
  coverage across **454** files, dependency checks over **215** protected modules,
  zero basedpyright errors, mypy over **182** files, and both type checkers rejecting
  all **37** invalid contract examples. Final source hashes match static, example,
  regression and quality evidence.
- Discovery remains **2,671 files**, with no new or missing paths. **34 out-of-scope
  storage files** differ from historical reviewed hashes. The previous 33 records
  are unchanged; storage_manager_api/router_api.py is newly flagged and was already
  dirty at this turn's preflight. This batch leaves those sources and review
  records untouched.

M progress: **36/274 units**, **800/7,198 declarations**, **62/344 complete files**.
D remains archived complete at **232/232 units**, **5,685 declarations** and
**374 files**; its archive and milestones are unchanged. Project coverage is
**1,122/2,671 complete-file review records**, containing **15,673 declarations**,
plus **77 partial-file declarations** (catalog 37 and concrete WEMI 40).
Remaining: **1,549 files and 26,411 declarations**. Across tracks, **300 documentation
units are verified** and **1,072 remain**. No commit/publication was requested,
no dependencies were installed, and no network service was used. Token usage is
unavailable.

- [Observations and exact commands](test-results/docstrings-m027-m036-2026-09-26/observations.json)
- [Static proof](test-results/docstrings-m027-m036-2026-09-26/static.json)
- [Runnable examples](test-results/docstrings-m027-m036-2026-09-26/examples-all.json)
- [Runtime-doc and command review](test-results/docstrings-m027-m036-2026-09-26/runtime-doc-review.json)
- [M037 unchanged declarations](test-results/docstrings-m027-m036-2026-09-26/M037-boundary.json)
- [M038 unchanged declarations](test-results/docstrings-m027-m036-2026-09-26/M038-boundary.json)
- [API contracts](test-results/docstrings-m027-m036-2026-09-26/contracts.log)
- [Container regressions](test-results/docstrings-m027-m036-2026-09-26/containers.log)
- [Hydration and OPF regressions](test-results/docstrings-m027-m036-2026-09-26/hydration.log)
- [Quality checks](test-results/docstrings-m027-m036-2026-09-26/quality.log)
- [Discovery and hash drift](test-results/docstrings-m027-m036-2026-09-26/discovery.json)
- [Inventory reconciliation](test-results/docstrings-m027-m036-2026-09-26/reconciliation.json)
- [Documentation diff](test-results/docstrings-m027-m036-2026-09-26/docstrings.diff)
- [Previous checkpoint](project-docstrings-m017-m026-2026-09-26.md)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
