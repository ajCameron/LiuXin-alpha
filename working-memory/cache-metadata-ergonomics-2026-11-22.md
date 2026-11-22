# Metadata cache ergonomics implementation

User approved implementation of the metadata ergonomics plan on 2026-11-22.
Worktree: `/tmp/liuxin-cache-metadata-ergonomics-20261122`; branch:
`cache-metadata-ergonomics`, based on `510c0a82dc18634abcd6b53ba1ac0b667e2fa734`.
Foundation PR #213 is open from `cache-review` to `main`; deliver this work as a
separate follow-up PR targeting `cache-review` while that foundation is open.

## Decisions and implemented scope

- Literal exact/prefix/contains suggestions, no fuzzy identity selection.
- Shared `get_many`, public-field lookup with totals/provenance, and suggestions
  only on appropriate text services. Languages expose no mutations at runtime
  or through typing; Genres/Subjects have no Work membership methods.
- Hydrated search pages on all four WEMI services; each new composed read uses
  one capture and discards the whole cache attempt on explicit fallback.
- Work Tag/Series membership conveniences; `add_tag(text=...)` atomically ensures
  and attaches. Existing typed links retain their metadata. Series priority is
  not a volume number or a preferred-Series ranking.
- Explicit Agent-ID/role credits on every WEMI level. Role removal preserves
  other roles. Catalog now allocates safe priorities across owners: the longer
  test found the old per-owner allocator colliding with physical Agent scope.
- Shared full-row edit snapshots/revisions, conditional update/delete/rename,
  and mixed shared/WEMI batch preconditions. No whole-tree revision claim.
- PostgreSQL fresh-schema parity fixes: Series declares the native
  `series_parent_position` column; typed many-to-many links identify the pair
  plus role, matching SQLite. Agent insertion/replacement refuse incompatible
  pair-only link schemas before writing. Existing schemas are not migrated.

Contracts remain in `catalog.api`; metadata coordination remains in
`caches.metadata`; generic caches remain metadata-independent. The passive
shared values have a separate contract file to avoid a type-only import cycle.
See `dev-docs/caches/metadata-api.md` for the public workflow and semantics.

## Validation and delivery

Validated with Python 3.14.10, `PYTHONPATH=src`, and the worktree's `.venv`
symlink to `/tmp/liuxin-pr-192-venv`. Preserve the original checkout's Python 3.12
venv, dirty `LiuXin_alpha_data` submodule and six unrelated stashes.

Acceptance evidence under `/tmp/liuxin-ergonomics-*`:

- All **39 new mandatory cases** passed across the main run and focused reruns;
  `mandatory-audit.json` maps every node ID to its passing JUnit record. Earlier
  test-fixture failures were corrected and superseded. The manifest preserves
  all 3,030 prior requirements and now contains **3,069** distinct cases.
- Existing cache/metadata/tree/membership suite: **274 passed** in
  `regression-parallel.xml`. This broader run began before the final Agent guard
  and PostgreSQL corrections; focused credit and live PostgreSQL checks cover
  those changes.
- Existing Catalog credit tests: **6 passed**, SQLite and APSW,
  `catalog-credit.xml`. The final cross-owner/role-schema guard regressions also
  passed on both drivers (`credit-role-guard.xml`, included in the 39 above).
- Gated long workload: **4 smoke cases passed**, all cache modes, `long.xml`;
  timings in `long-artifacts`. The full 10,000-record/1,000-Work soak was not run.
- Documentation/tooling: **65 passed**, `docs-tools.xml`; the final documentation
  link/boundary follow-up passed **22 cases**, `docs-release.xml`.
- Final quality runner passed (`quality-release.log`): 462 format files,
  482 annotation files, 267 import-cycle modules, 2,086 layer modules,
  31 cold-import budgets; lint and both type checkers clean. Mypy checked 569
  source files; invalid typing examples rejected: 127 basedpyright / 123 mypy.
- PostgreSQL 16: **all 5 cases passed** across `postgres-schema-fixed.xml`
  and `postgres-cached-fixed.xml`, covering uncached and all three cached
  workflows plus an independent shared-token writer. The cluster is stopped.

The disposable PostgreSQL cluster is owned by the test runner under
`/tmp/liuxin-ergonomics-postgres-20261122`; its EXIT trap stopped it after successful completion. Exploratory
failed/interrupted PostgreSQL runs do not count as acceptance evidence. They
exposed real fresh-schema parity defects, now fixed. The reported role identity
failure was not an update-trigger/no-op problem.

The final `validation.json` audit records **393 distinct passing tests**, all
39 mandatory new cases, and 27 matching final implementation/test fingerprints.
These totals combine the broad run and focused follow-ups; they do not claim a
whole-repository test run or a full-size performance soak.

Delivery branch: `cache-metadata-ergonomics`, as a separate follow-up PR targeting
`cache-review` while foundation PR #213 is open. Retarget the follow-up to `main`
after the foundation merges. Neither PR is to be merged as part of this task.
