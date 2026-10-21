# Backup staging ownership, 2026-10-20

Branch: `backup-staging-ownership`, based on documentation commit `a5406cf8`.
This implements architecture A01 / storage STOR01 / review ST-6. The focused PR
targets `docs-ownership-contracts` while documentation PR #127 remains open.
Retarget to `configuration-ownership` after that dependency merges.

## Behaviour

- A new SquashFS backup treats `staging_target` as a parent and exclusively
  allocates a private workspace beneath it. Only `members/` is archived.
- The marker outside `members/` binds workspace ownership to the builder UUID.
  Effective declarations persist the actual members path, workspace and identity.
  Recovery and execution validate them; cleanup validates them again immediately
  before deletion. Missing/replaced/symlinked workspace evidence is not adopted.
- Successful requested cleanup removes only that workspace, including routed
  output's local intermediate image. Local output must be outside the workspace.
  `CLEANUP` means deletion succeeded; errors preserve publication evidence in a
  failed checkpoint rather than claiming success.
- Failed/cancelled runs and successful runs with cleanup disabled retain scratch.
  Default system scratch now follows the same explicit retention contract.
- Legacy captured declarations lack workspace evidence. They reopen existing
  flat staging as borrowed and never recursively clean it.
- Constructor failure releases newly allocated scratch. No shared staging parent
  or other run's workspace is claimed.

The prototype already saves the effective runtime declaration before checkpoints,
so ownership persists through its existing repository path without a schema change.
The storage contract and architecture task/review status now describe the fix.

## Verification

- 60 tests passed in 71.84 seconds: all storage backup tests, SquashFS build backend
  tests and workflow API tests. Includes real SquashFS creation/readback, database
  restart/source-loss readback, persisted failed-run retry, shared-parent sentinels,
  cancellation, verification/publication failure, cleanup errors and legacy recovery.
- The new ownership regressions substitute external archive execution/validation
  where needed; staging, filesystem cleanup and repository round trips remain real.
- Ruff lint passed for both implementation files and both affected test files;
  formatting passed for the two new modules.
- 22 documentation navigation/public-boundary tests passed in 49.48 seconds,
  including tracked-content links after staging the new test module.
  The staged diff passed its whitespace check.

Tests ran serially, with immediate pytest scratch cleanup and persistent telemetry.
Ignored local evidence lives under
`working-memory/test-results/backup-staging-20261020/`. The full application suite
was not run locally for this fix.

## Boundaries and next work

Concurrent executions of the same checkpoint are not synchronized. A partial
cleanup that removes ownership evidence requires inspection; recovery does not
invent evidence to retry deletion. These limits are documented.

Six pre-existing staged storage source edits retain their exact contents and index
blobs; the data submodule remains untouched. They are excluded from this commit.

A02 (Store read-only intent) and A03 (nondestructive database creation) remain the
next independent implementation slices.
