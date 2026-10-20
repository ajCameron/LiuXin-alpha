# Subsystem documentation review, 2026-10-20

Branch: `docs-ownership-contracts`, continuing from `fc5de7af`.
This continues the top-level and architecture reviews for
[PR #127](https://github.com/ajCameron/LiuXin-alpha/pull/127), targeting
`configuration-ownership`. The user requested publication on 2026-10-20.
The documentation moves are included; six unrelated staged storage source edits
and the dirty data submodule remain outside this documentation commit.

## Scope and outcome

Reviewed the 107 existing files in the thirteen remaining top-level documentation
folders, including the completion inventory and three historical SQL sketches.
Each folder now has `next_tasks.md`. Added documentation folders for jobs, ingest
and caches because their current owners and contracts were scattered. Together
with the architecture folder, all seventeen folders have an index and task list.
The developer index routes all of them; architecture A14 records this documentation
pass as complete for PR review, without closing runtime findings.

Substantive corrections:

- Database field guide reconciled to all 67 explicit tables in a freshly generated
  SQLite schema, with physical columns/types and DDL links. Replaced obsolete
  composition-table descriptions, added missing current tables/fields, and corrected
  PostgreSQL's implemented status. Conceptual WEMI SQL is labelled and executable;
  its single-parent recommendation is distinguished from the actual graph.
- Core/Catalog guides now distinguish encoding from authorization, local runtime
  access from a nonexistent LocalCoreClient wrapper, partial preflight from
  transactional guarantees, and unresolved writer references from row creation.
- Metadata container APIs use the current `containers_api` roots. Catalog owns
  hydration; the dirtied queue is explicitly best-effort rather than a durable
  outbox. Corrected an invalid ISBN and inconsistent JSON example.
- Conversion placeholder replaced with current execution, reporting, registry and
  scratch contracts. DOCX EMF, HTML2ZIP availability, loss receipts and process-CWD
  isolation remain implementation tasks. Historical sign-offs retain their scope.
- Storage terminology no longer inserts a File layer between Asset and Replica.
  Staging ownership, read-only construction and deletion-policy gaps are prominent.
  Added missing path budgets; recovery-pack ideas remain proposals.
- GUI plan reflects existing metadata editing and shared local/remote sessions.
  Historical branch instructions and obsolete raw invocation examples were replaced.
- Contributor conventions, CI/extras, test streams, artifact ownership and historical
  docstring campaign records have explicit authority. Only the completed inventory's
  stale active-batch status changed; its historical hashes/counts were not regenerated.

Two existing tests still pointed to pre-move documentation paths. Updated only
those paths in `tests/catalog/test_catalog_api_documentation.py` and
`tests/storage/test_backend_registry.py`; no runtime implementation changed.

## Verification

- Publication verification: 30 focused tests passed in 54.72 seconds, with no
  deselections. New documents were staged first, so the tracked-content link
  check ran against the actual Git index. Selection: documentation navigation
  and public boundaries, Catalog guide syntax, generated WEMI schema, and backend
  registry/reference parity. This supersedes the earlier 29-pass run that
  deliberately excluded the tracked-content check before staging.
- Separate publication-set audit checked 149 Markdown documents, 1,092 local links,
  88 heading fragments and full index reachability. The final audit found no
  untracked developer documents or missing targets after staging the twenty new
  developer documents. The audit itself does not modify the Git index.
- Field-guide column/type comparison matches all 67 explicit generated tables.
  The conceptual SQL sketch executes independently. All three historical SQL
  sketch bodies remain unchanged beneath their new status headers.
- Annotation coverage check passed across 463 file-format Python files.
- Diff whitespace check passed. Hash checks confirm the six pre-existing storage
  source changes are untouched.

Tests ran serially with immediate temporary-directory cleanup and telemetry outside
scratch. No full suite, live provider probes, listeners, database provisioning,
installed-wheel run or visual desktop check was performed for this documentation pass.

Local detailed evidence is in
`working-memory/test-results/documentation-subsystems-20261020/`: review manifest,
generated schema audit, documentation audit, review patch, pytest output and telemetry.
These are ignored local artifacts; this note retains the portable result summary.

## Next work

Review the documentation through PR #127. Keep implementation PRs focused:
architecture A01 backup staging, A02 Store read-only intent and A03 safe
database creation remain the suggested first slices. A06 transport capabilities
can proceed independently. The per-folder task lists state owners, dependencies
and completion criteria; runtime changes were not implemented by this review.

The previous PR head (`fc5de7af`) had failed full-suite and architecture CI checks.
The focused local result above does not clear those checks; CI must assess the
updated PR head separately.
