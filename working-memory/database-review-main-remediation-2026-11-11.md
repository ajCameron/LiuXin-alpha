# Database review/main reconciliation — 2026-11-11

The user requested stopping Copilot's stalled conflict-remediation task and
repairing the GitHub conflict. Work is in the persistent review clone at
`/mnt/c/dev/LiuXin-alpha/.tmp/database-driver-review-2026-11-10`, local branch
`database-review-main-remediation-2026-11-11`.

## GitHub state and cause

- Calibre PR #158 merged into `database-review` as `a689ab27983e7f7f93a1157a5babea4034eba0df`.
- PR #159 merged the older `maintenance-plugin-modes` checkout (`cdcaf3dd`) into
  `main`, producing `520fefc616bf9f10bbbf2036813fe5e34735427c`.
- PR #160 (`database-review` → `main`) consequently had 81 conflicted files.
  Its common ancestor is `b2343096`; the maintenance snapshot predates the later
  wrapper, typing, driver, FRBR and Calibre repairs already on `database-review`.
- Copilot run `43097447813` had already **completed with `startup_failure`** and
  no jobs. The dynamic-run list contained no second active Copilot run. Nothing
  remained running to cancel; no cancellation or restart was performed.

## Resolution policy

Merge `main` into the PR branch, preserving both histories. The 213 paths changed
on main were audited: 176 file versions already occur in review-branch history;
the remaining 37 include comment/docstring edits, preference/version changes,
historical handoffs and the already-deleted legacy module.

Conflicts retain the later reviewed implementation, tests, typing coverage,
regression manifest and CI configuration. Distinct review TODOs from main were
ported onto the current implementations. The SQL-classifier relocation note
follows `sqlite_row_operation` into `databases/sql_statements.py`.

Main's APSW component version `(0, 3, 0)` and removal of the retired
`run_ta_update_after_each_change` default/shipped INI entries are retained. The
retirement guide now describes that removal. These are the only runtime AST
differences from pre-merge `database-review`; all other source changes are
comments. Newer explanatory docstrings are retained over whitespace changes and
empty parameter placeholders in the old checkout.

The registry's preserved comments move its existing dynamic builder import from
line 227 to 229. The architecture baseline records that new location; no new
import allowance or dependency is introduced.

Three older typing TODOs are preserved here as already addressed by the later
typing work, rather than reintroduced as pending implementation tasks:

- `# Todo: We can type this` — `repair_json_scalar` now accepts `object` and
  returns `dict[str, str]`.
- `# Todo: We can type the database better with the API` —
  `MaintenanceRuntime.__init__` now accepts `DatabaseAPI`.
- `# Todo: We can surely type the connection better...` —
  `MaintenanceRuntime.before_statement` now accepts `SQLConnection`.

The main-only driver publication and WSL recovery handoffs are retained. The
wrapper-publication receipt was appended while preserving its portable local
documentation link. The full old checkout index is retained as
[a historical snapshot](maintenance-checkout-index-2026-11-11.md); use the active
[working-memory index](index.md) for current instructions.

## Verification and delivery

Local evidence is under
`/mnt/c/dev/LiuXin-alpha/.tmp/database-review-main-remediation-2026-11-11`:
branch inventories, source-history comparisons, original conflict paths,
comment-preservation inventory, semantic delta and validation logs.

All merge entries are resolved. Fresh validation completed:

- Full `scripts/run_type_checks.sh` passed: formatting, annotation coverage,
  lint, architecture/import budgets, complexity, basedpyright, mypy and both
  internal contract checks (95/93 deliberately invalid examples rejected).
- Focused preference, driver/wrapper, FRBR, Calibre, maintenance and documentation
  checks: **822 passed, 149 skipped** across the suite and one socket-enabled
  retry. The initial suite had 821 passes and one sandbox `PermissionError`
  creating the local HTTP socket in the preferences integration test. That
  exact test passed when rerun with local socket access; no source repair was
  needed. Skips cover unconfigured PostgreSQL, unavailable fixture data,
  optional Hypothesis and one backend-specific case.
- AST comparison confirms only the two intended runtime changes above. Reviewed
  tests, CI, generators and quality configuration are unchanged except the
  architecture baseline line number. All 28 main-only comments are retained in
  their source, relocated source or the resolved-TODO archive above.
- No unmerged entries or whitespace errors remain. Both GitHub heads still
  match the merge parents recorded above immediately before publication.

Delivery uses the existing `database-review` branch and
[PR #160](https://github.com/ajCameron/LiuXin-alpha/pull/160), with a normal merge
commit preserving both histories. No force-push or merge of PR #160 is part of
this remediation. Publication and fresh CI receipts belong in the evidence
directory; inspect GitHub for their latest status before further branch work.

The original checkout remains on `maintenance-plugin-modes` at `cdcaf3dd`, with
its data submodule changes preserved. No original-checkout stash, switch or
reset is part of this remediation.
