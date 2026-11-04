# Maintenance and driver-wrapper PR

The user requested a PR for the review/upgrades, then explicitly chose to include
the maintenance overhaul as well. The branch is
`maintenance-driver-wrapper-upgrades`, based on `database-batch-overheads`
(`b2343096`, PR #153; prerequisite PR #152). The dedicated checkout is
`/tmp/liuxin-driver-wrapper-pr`. The wrapper commit is `073c9214`; maintenance and
delivery documentation follow separately. No merge was requested.

The [delivery checkpoint](../dev-docs/databases/review-upgrade-delivery-2026-11-04.md)
records the scope, PR-branch checks and remaining limits. It supersedes prior
uncommitted-delivery instructions in the maintenance/wrapper handoffs.

The original `maintenance-plugin-modes` checkout is preserved. The dirty
`LiuXin_alpha_data` submodule, both preference INI files and
`src/LiuXin_alpha/preferences.py` are excluded from the PR. Its obsolete TA-update
key is harmless without callers; the retirement inventory records that accurately.
Original source hashes were captured in `/tmp/liuxin-wrapper-pr-source-hashes.json`
before packaging. Do not discard the original work to make that checkout clean.

The combined checkout passed **371 tests, 13 backend-specific skips**. All quality
gates passed (303 formatting targets, 254 mypy files, zero basedpyright diagnostics,
both 47-invalid-call probes and 25 cold-import budgets). Documentation/tooling
checks passed **44 tests**; man rendering and whitespace pass. Earlier wrapper-only
preparation checks passed 224 focused cases and 45 live PostgreSQL cases. These
selections overlap. No new full-repository run or real-file import is claimed.

Logs and telemetry use `/tmp/liuxin-review-upgrade-pr-*`; the resource summary
records 132.8 MiB peak test-process-tree RSS, 3.99 GiB minimum available memory and
zero OOM kills. The private PostgreSQL instance had no leftover test schemas and
is stopped and removed. Its path record is historical; do not try to reuse it.

Use `gh pr view maintenance-driver-wrapper-upgrades --repo ajCameron/LiuXin-alpha`
for the published PR URL and state. Continue on the dedicated PR checkout while
the original checkout retains its already-published implementation changes; avoid
recommitting them as new work there. The source-checkout handoff records the
returned PR URL after publication.
