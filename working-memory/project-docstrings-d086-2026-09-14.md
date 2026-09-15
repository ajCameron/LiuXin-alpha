# D086 — src/LiuXin_alpha/databases/database — intralink_mixin, linked_rows_mixin, 2026-09-14

All 15 self-link and linked-convenience declarations reviewed. Shared regressions, imports/examples, full quality and inventory reconciliation passed. Nine real SQLite smoke assertions cover same-table identity, cross-table ordering, first result, IDs, fingerprints, dictionary coercion, filtered absence and input rejection. The two self-link return/deletion defects remain documented with their existing xfails; no runtime fix was made.

Verified 15 selected declarations. Executable ASTs, comments,
headers, and lint baselines are preserved; selected docs pass audit/normalizer.
Partial files are promoted only after every assigned slice and full-file checks.

[Evidence](test-results/docstrings-d-2026-09-14/D086/observations.json) records scope IDs, hashes,
checks and current inventory. Check commands/terminal exits are in the referenced
completion records. Work remains within the authorized D range.
