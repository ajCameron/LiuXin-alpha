# FRBR generator local signoff — 2026-11-11

Publication authorized after signoff: commit and open a PR from
`database-generator-frbr-review-2026-11-11` to `database-review` in
`ajCameron/LiuXin-alpha`. Driver PR #156 is merged as `4c9b71ca`; its target tree
matches the tested driver checkpoint. The isolated clone now has a `github`
remote for publication; `origin` still points to the original local checkout.
Publication evidence, including the resulting commit and PR URL, belongs in
`/mnt/c/dev/LiuXin-alpha/.tmp/frbr-generator-publication-2026-11-11/`.
The earlier uncommitted/publication-not-requested entries below are historical.

The user requested a second review, then repairs for its two findings. FRBR06
(primary-flag domain) and FRBR07 (stale nullability API documentation) are fixed.
The generated schema constrains primary flags to NULL/0/1; Catalog create/update
accept None, booleans and integer 0/1 and reject other values before persistence.
Both public API documents now match physical nullability and placeholder policy.

All 14 new cases reproduced the prior failure. Final focused validation passes
174 cases; identifier import/write/hydration/merge coverage passes another 10;
documentation and regression-runner checks pass 48. Total: 232 passed, no skips.
All 117 FRBR mandatory cases pass and the manifest now contains 1,444 cases,
preserving the earlier 1,430. Full required quality gates and both type checkers
pass. Offline PostgreSQL rendering retains the new CHECK in 3,019 statements;
no live PostgreSQL or full mandatory-gate run was performed for this repair.
Direct Ruff checks of Catalog code and regressions pass. An extra lint check of
the two API documentation files found 20 existing style diagnostics: baseline
comparison proves identical findings and unchanged code ASTs outside docstrings.

Local correctness signoff is appropriate for this new-database SQLite generator
tranche. Existing-database migration, pure TOML parsing and PostgreSQL lifecycle
parity remain separate follow-ups. The signed-off changes are on the same clone
and branch described below. Commit and PR publication are now authorized; merging
the generator PR is not requested.
Evidence: `/mnt/c/dev/LiuXin-alpha/.tmp/frbr-generator-signoff-2026-11-11/`;
read validation-summary.json, green.log, integration.log, docs.log, quality.log,
postgres-render.json and api-docs-baseline.json. The preceding review is in
`/mnt/c/dev/LiuXin-alpha/.tmp/frbr-generator-second-review-2026-11-11/`.

## Earlier five-finding repair handoff (historical validation counts)

The user requested repairs for all five review findings and architectural
improvements. Implementation and required validation are complete. Use the
persistent isolated clone `/mnt/c/dev/LiuXin-alpha/.tmp/database-driver-review-2026-11-10`,
branch `database-generator-frbr-review-2026-11-11`, based on
`5304f34f55d7fb73d9921d72a2248818e4aa22eb`. Origin is the original local checkout,
not GitHub. Changes are staged and uncommitted; no FRBR publication was requested.
Driver PR #156 and its published branch are unchanged. No agents or skills used.

Read the [repair record](../dev-docs/databases/database-generator-frbr-review-2026-11-11.md)
and [build contract](../dev-docs/databases/database-generator-frbr-build-contract.md).

- Shared immutable SQL resource loading replaces paired BREAK parsing. Complete
  trigger bodies/trailing statements are retained; incomplete resources fail with
  path/ordinal context before stage execution. Aggregates share this loader.
- Frozen LinkBuildStep plans compile both TOML documents before any link DDL.
  Vocabulary rendering and standard link columns have dedicated modules.
- allow_placeholders is explicit and defaults true for Catalog compatibility;
  standalone strict mode honors nullable. Shared interlink emission honors the
  resolved physical nullability instead of ignoring it.
- Identifier guards allow blank placeholders and complete unassigned candidates
  (both owner fields NULL), reject half-references, and validate assigned owners.
  Owner deletion cascades across all five types. Referenced-key changes and owner
  replacement are rejected, including rowid/_rowid_/oid aliases, UPDATE OR REPLACE
  destination collisions and recursive_triggers=OFF. Unreferenced key moves work.
- Catalog complete identifier replacement demotes original primaries inside the
  existing transaction before promotion. Late failures restore original rows/flags.
- Optional intralink columns are emitted once with correct types; custom columns
  cannot redeclare endpoints. Redundant links are excluded with their metadata.
  Folder INSERT/UPDATE guards reject single backslashes using char(92).

Validation:

- Final focused run: 160 passed / zero skipped, 380.63 s; every one of the 103
  added mandatory cases passes all phases. Manifest: 1,430 required, with all
  previous 1,327 cases and invariant assignments preserved.
- Broad run: 1,233 passed / 58 conditional skips, 1,612.52 s. Two HTTP cases failed
  only with sandbox PermissionError at socket creation; loopback-enabled retry:
  2 passed / 58.66 s. Broad + retry + final focused suite reconcile to 1,275
  distinct passing cases. Do not claim a single all-green broad invocation or
  the full PostgreSQL-aware mandatory gate was run.
- Required full quality gates and both type checkers pass (mypy 462 source files).
  PostgreSQL offline schema rendering: 3,019 statements; no live PostgreSQL.
- Docs/boundaries/CI checks: 79 passed, then all 16 doc-link checks passed after
  staging the new guide. Earlier failed/interrupted runs are diagnostic only.
- Extra direct Catalog typing check found a pre-existing optional-string error
  in IdentifierRepository.find(); unchanged baseline AST proved in
  catalog-types-baseline.json. Required scope was not weakened to hide it.
- Tested source hashes and six protected original source/preferences/notes match;
  original recovery worktree index hash is unchanged. No database server started.

Evidence: `/mnt/c/dev/LiuXin-alpha/.tmp/frbr-generator-fixes-2026-11-11/`.
Start with validation-summary.json, key.log, verified.log, http.log,
quality-verified.log, postgres-render.log, docs.log/doclinks.log and
final-source-hashes.json. All validation processes are finished; do not resume
historical process handles. Earlier red.log/plan.log/final.log document initial
findings, fixture corrections and the subsequently repaired Catalog integration.

Existing databases have not been migrated. Pure TOML parsing, a versioned upgrade
path and PostgreSQL identifier-lifecycle parity remain future work. Preserve the
original dirty checkout, preferences/data, original .git staging and WSL recovery
artifacts. Its working-memory index points here. No commit/push/merge.
