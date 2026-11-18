# Cache review handoff — 2026-11-16

Current status: the compatibility migration is implemented and verified; see the
implementation follow-up below. Earlier sections preserve review checkpoints.

The user asked to finish the previous database batch with a commit/PR if needed,
then move to caches. Database implementation PR #161 and docstring PR #165 were
already merged. The remaining publication-status correction is commit `b19c1d16`,
branch `databases-signoff-record-2026-11-16`, in
[PR #170](https://github.com/ajCameron/LiuXin-alpha/pull/170). Its 22 documentation
checks passed locally; all executed CI checks, including full suite and database
regression, passed when checked on November 16. The PR is open, not merged.

## Initial cache review checkpoint

Reviewed main at `2d2a9cc304b5394ab90003b0567c4d06130337f1`. Canonical findings:
`dev-docs/architectural_reviews/caches-review-2026-11-16.md`. Manual executable
probes: `dev-docs/architectural_reviews/reproductions/caches_2026_11_16.py`.

- CR01: a reentrant receipt listener commits a newer custom-field projection,
  then an older completion overwrites it. Reproduced with SQLite and SQLite_apsw.
- CR02: schema-backed `reload_data()` partially refreshes tables, raises, and
  leaves READY state with unchanged generation and old query indexes.
- CR03: partial invalidation leaves exact lookups and text queries inconsistent
  on schema_backed and numpy_vectorized.

All three are open P2 findings. No production cache code changed. Existing focused
suite: 112 passed, six capability-specific skips. Manual defect probes: five
failures demonstrate the findings, three skips delimit backend scope. The review
contains commands and observable results. Cache review docs/probes and this
handoff are a separate, uncommitted batch, not part of PR #170.

Repair CR01 first, then CR02/CR03 under CACHE01. Preserve native persistence versus
cache publication ownership and the ReadCache/Cache split. Broader lifecycle,
concurrency, real outer-transaction rollback and legacy caller review remain.
No full cache suite, live PostgreSQL, Windows or performance run for this pass.

## Compatibility boundary review checkpoint

The user next asked to review what should move into `calibre_compat` or similar
under utils. Review complete; no production relocation. See
`dev-docs/caches/calibre-compatibility-review-2026-11-16.md` for the caller map,
destinations, migration sequence and exact validation commands.

- First slice: all 15 `caches.write` modules to
  `utils.calibre_compat.caches.write`; two external production callers, both legacy.
- Move the `plugins.cache` base family, `library.caches` implementations, old
  category builder (`surfaces.categories`) and `library.lazy` with that family.
- `metadata.field_adaptors` has three production callers, all compatibility
  consumers; move its legacy coercion while retaining native normalization owners.
- Split `catalog.search`: only the package-initializer legacy engine and typed
  operators move. Native `database_search` and `physical_plan` stay. A fresh
  native-search import currently initializes legacy Search via the parent package.
- Split shared field metadata before moving compatibility behavior. Core/native
  publication still uses it. Review `library.library_metadata` separately because
  it coordinates transactions and Catalog writes, not just cached projections.
- Keep native cache facades, all three backends, generic APIs/update payloads,
  canonical normalization and shared grammar in their present native owners.

Resolved absolute/relative imports across production Python with no skipped files.
Focused compatibility suite: 233 passed; native search/field-metadata suite: 59
passed. Total 292 baseline passes, no implementation changes. Follow the existing
explicit-break preference: update callers and guards, remove old paths, add no
forwarding facades. CR01–CR03 remain open. Review/docs are still uncommitted.

## Workspace preservation

The main checkout already had a dirty data submodule, a local architectural-review
index row, an untracked file-formats review and `Claude outputs/`. Those are not
part of either batch. PR #170 was built in an isolated worktree. Keep the existing
`Pre-pull review drafts 2026-11-16` stash; do not pop or drop it automatically.

## Implementation follow-up — completed and verified

The approved relocation is on `caches-calibre-compat-boundary`. All 61 source
moves are present, native Catalog search has a passive initializer, and native
`caches.field_metadata` replaces Core/custom-field dependence on the legacy
container. Registry-wide publication tickets repair CR01; CR02/CR03 remain open.
Old paths and the empty BaseTagClass are removed without forwarding modules.
Current layout and API changes: `dev-docs/caches/structure.md`.

Final evidence is recorded in `dev-docs/caches/calibre-compatibility-review-2026-11-16.md`.
The full cache directory passed 220 tests/20 expected skips. Compatibility/search
passed 291; final guards 159; layer/import tests 66; docs 22. The broader selection
passed 803 with two stale native-test failures and a build/pytest cleanup collision;
all affected cases passed after correction (four native/Core tests plus clean cache
and boundary reruns). CR01 manual probes now pass on both SQLite drivers; CR02/CR03
still produce three expected failures. Full quality runner passed, including both
type checkers and internal contracts. Mandatory regression IDs increased from
2,462 to 2,473; all 11 additions were verified against executed collection IDs.

The initial repository-root wheel build collided with tests/conftest.py's root
artifact cleanup, which partially removed build/. Do not run those concurrently.
Final build used `/tmp/liuxin-cache-wheel-source-_y_fidl_`; its wheel is
`/tmp/liuxin-cache-migration-wheel-final/liuxin_alpha-0.0.0-py3-none-any.whl`, SHA256
`c4984423635704f63f7cdcf46cef681944a4bda10f93fd562085dd951ada0ec7`.
Both wheel content checks and installed import/origin, retirement, native publication
and default Core-registry checks passed. EBNF/kakasi resource declarations were
added after clean packaging exposed omissions. There are no source forwarding shims.

No full project/live PostgreSQL/native Windows/performance run. CR02/CR03, broader
cache lifecycle/concurrency, legacy generated-schema capability migration and
library.library_metadata/book/conversion-policy review remain separate work.

Existing build output was preserved at
`/tmp/liuxin-build-before-cache-migration-20261116` before the clean wheel build.
Documentation membership checks use a temporary index and object directory under
`/tmp`; the real index and pre-existing user drafts are unchanged. The temporary
candidate includes the user's existing file-formats review solely for link checks.
No commit or PR for the cache migration yet.

The partial repository build was moved to `/tmp/liuxin-cache-incomplete-build-20261116`;
no generated build directory remains in the checkout. Both the original build
backup and final verified wheel are retained outside the repository.
