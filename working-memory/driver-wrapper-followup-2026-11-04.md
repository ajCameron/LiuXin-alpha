# Driver-wrapper follow-up fixes and typing

Completed **2026-11-04** on `maintenance-plugin-modes`. User authorized fixing
the five follow-up review findings, then improving typing. No commit, push or PR
was requested. Preserve the large pre-existing maintenance changes, protected
preferences, user TODOs and the dirty `LiuXin_alpha_data` submodule.

The [review and evidence](../dev-docs/databases/driver-wrapper-review-2026-11-04.md)
and [DB09](../dev-docs/databases/next_tasks.md#db09-correct-remaining-wrapper-metadata-boundaries-and-type-the-wrapper)
record the complete change. Metadata edits preserve WEMI attachment; edits,
deletion marking and deferred cleanup preserve caller-owned transactions and
join active macro units. Cleanup DDL and definition deletion are atomic. View
headings and SQLite declared types use the primary schema snapshot and close
owned resources. Link caches and table specs call current driver hooks.

The full wrapper/API are now included in both existing static-checker scopes,
lint and formatting. Query connection/cursor protocols describe portable
containers; concrete mixins share an abstract implementation base. Signatures
describe existing False/None results and dictionary shapes. Empty row/missing
parent guards and raw get/fetchone cursor cleanup follow from these contracts.
API declarations remain abstract and contain no implementation.

## Verification

- Final boundary/API/tooling: **119 passed**, `/tmp/liuxin-wrapper-signoff.log`.
- Integration: **137 passed, three skipped, four existing xfailed**,
  `/tmp/liuxin-wrapper-integration.log`.
- Live PostgreSQL wrapper/writers/transactions: **25 passed**,
  `/tmp/liuxin-wrapper-typing-pg.log`.
- Final API signatures: **six passed**, `/tmp/liuxin-wrapper-final-api.log`.
- View-heading doctests: **nine examples passed**.
- Complete scoped quality runner: **passed**, 290 formatting targets and 244
  mypy sources, zero basedpyright diagnostics; both negative contract probes
  reject all **44** mistakes. `/tmp/liuxin-wrapper-quality-complete.log`.
- **105** local links resolve across the five edited database guides, including
  files currently untracked and intended for the eventual commit.
- All three preference hashes match
  `/tmp/liuxin-maintenance-signoff-preferences.json`. `git diff --check` passes.

Selections overlap; no unique-test total or new full-repository run is claimed.
Scratch/config/cache/telemetry were kept under `/tmp`, with per-test scratch
discarded. The private PostgreSQL cluster is stopped and removed; its server log
is `/tmp/liuxin-wrapper-typing-pg-server.log`.

## Delivery and remaining work

The subsequent [feature handoff](driver-wrapper-features-2026-11-04.md) and
[guide](../dev-docs/databases/driver-wrapper-features-2026-11-04.md) supersede the
physical-schema, view-row cleanup and hierarchy-cycle follow-ups below. Counts
in this note remain the evidence for the earlier boundary/typing pass.

Changes are uncommitted. New untracked files include the review, this handoff,
`db_types/driver_types.py`, `driver_wrapper/_base.py`, and the three wrapper
regression files. Do not omit them when preparing a future commit. Existing
staged maintenance work has not been regrouped.

Remaining follow-ups are recorded in the review: physical constraint facts in
table specs, custom-column placeholders and SQLite-only DDL, legacy view-row
connection cleanup/visibility, other explicit SQL commit boundaries and hierarchy
cycle detection. Do not claim all wrapper calls join a caller's transaction or
that all database drivers are statically checked. Real SquashFS import validation
still awaits physical server access Friday.
