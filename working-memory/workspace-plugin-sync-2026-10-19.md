# Bring the merged plugin package into the working checkout

Date: 2026-10-19. Branch: `plugin-system-workspace`, based on merge `a5f77cfd`.

The user still saw `customize` because the main checkout remained on
`catalog-write-consolidation` at `af2cebd0`. Pulling the merged PR and updating
guide 02 had only advanced the isolated review worktree. The implementation was
already merged in PR #125; this follow-up synchronizes the working checkout.

## Preserved work

Reviewed all 203 changed paths before switching. Most documentation moves and
edits were already included in the merged commits; later navigation and ownership
updates supersede their earlier copies. Six storage-file comment/docstring edits
remain outstanding and were restored byte-for-byte. The plugin-root TODO asking
for the merger is fulfilled. The latest uncommitted guide 02 edit was copied from
the review worktree.

The original branch remains available. Recovery stash
`84ceb0646e5314aaac53a7ed8291f4ad47c92ec7` retains the original staged and unstaged
work and untracked review document; it has not been dropped. A separate local
snapshot also records original bytes, both patches and per-file dispositions.
The data submodule and other ignored local artifacts were not replaced.

## Current package boundary

The source and test `customize` directories are removed. Forty source and five
test bytecode files left by Git's tracked-file removal were archived outside the
checkout, so the retired directory cannot survive as an importable namespace.
The ignored build tree also contained 23 old `customize` modules; these were
archived to prevent a later incremental wheel build from packaging that stale copy.
Production callers and ZIP-plugin examples use the merged `plugin_system` owners.
Existing `customize` settings keys and customization verbs are distinct from the
removed Python package and retain their persisted configuration meaning.

Validation from the main checkout: 57 plugin/runtime, package-structure and
documentation tests pass with telemetry and immediate test-scratch cleanup.
The layer-direction check passes across 1,792 modules with the same 15 reviewed
edges and 28 dynamic imports; all 25 cold-import budgets pass. A fresh interpreter
finds no `LiuXin_alpha.customize` and resolves application/base contracts from this
checkout. The six original storage edits were checked byte-for-byte after testing.
No full application suite was repeated for this synchronization.
