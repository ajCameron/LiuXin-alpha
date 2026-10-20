# Store read-only intent, 2026-10-20

Branch: `store-read-only-intent`, based on A01 commit `75b266ed`.
This implements architecture A02 / storage STOR02 / review ST-2. The focused PR
targets `backup-staging-ownership` while PR #128 remains open; that PR in turn
depends on documentation PR #127. Retarget after the dependencies merge.

## Behaviour

- Managed, flat, Calibre-like and SQLite registry builders pass explicit
  `read_only` into both Store configuration and the raw driver. Writable defaults
  and intrinsic unmanaged-directory restrictions remain intact.
- Read-only filesystem construction/startup cannot create missing roots. SQLite
  requires an existing object schema, opens an escaped URI with `mode=ro`, and
  skips directory/schema/index creation and journal configuration.
- SQLite advertises the configured mutation policy and rejects raw mutation
  methods before staging/publication. Its connections now close explicitly on
  success and failure; the earlier transaction context alone retained locks.
- Store/manager mutation refusals survive catalogue close/reopen and Store reload.
  Tests use the real registry and both catalogue drivers, plus writable controls.
- SQLite retains ordinary WAL coordination and sees live committed changes.
  Sidecars may still be touched; immutable mode would be incorrect for that use.
  Manager ingest can consume input and journal failure before destination refusal,
  while cleanup leaves no phantom Asset or Replica claim.

The storage contract, backend guide, task lists and annotated architecture review
now distinguish these fixes from the remaining retention/last-copy policy gap.

## Verification

- 139 targeted tests passed in 92.23 seconds: the new policy regressions, registry,
  all four backend families, driver API/error contracts and selected real database
  bootstrap/reload tests.
- The new regressions cover unchanged bytes/file metadata, writable controls,
  missing roots/containers/schema, optional-index preservation, engine-level
  SQLite write refusal, escaped filenames and live WAL visibility.
- The final run passed all 46 tests in 93.90 seconds: the 24 new regressions with
  Asset/Replica preservation assertions, plus 22 documentation navigation and
  public-boundary checks after staging the new files.
- Ruff lint, formatting of the new test module and staged whitespace checks passed.

Tests run serially, with immediate scratch cleanup and persistent telemetry under
ignored `working-memory/test-results/store-read-only-20261020/`. The full application
suite is left to CI.

## Boundaries and next work

This closes the four audited builder defects. It does not establish a universal
no-side-effect contract for remote providers or enforce retention/last-copy policy.
The six pre-existing staged storage edits and dirty data submodule remain separate.
Their saved worktree hashes and index blobs matched before committing; preserve
those edits in subsequent work.

Next: A03, nondestructive database creation. A05 can use this read-only boundary;
A06's transport authorization is independent.
