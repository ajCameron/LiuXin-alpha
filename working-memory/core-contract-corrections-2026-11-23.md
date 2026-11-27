# Core contract corrections — 2026-11-23

Completed slice 1 of the whole-Core review (F1/F4/F5/F8), prepared for the
user-requested separate commit. Worktree `/tmp/liuxin-core-review-20261123`, branch
`core-review`, base `faab57ce`. Preserve the original `cache-review` checkout,
its dirty data submodule and stashes. No push/PR requested for this commit.

Implemented:
- Capture immutable command registrations just as queries do. Authorization,
  handler and result serialization retain one selected declaration.
- Shared strict primitives in `core.payloads`; validate declared fields before
  dispatch effects, with strict IDs/flags/finite numbers/text in named adapters.
  Nested identity arrays and Store/policy controls reject coercible values too.
- Build discovery from actual command/query registrations, including extensions.
  Separate implementation, prerequisites, remote grants and unprobed readiness.
  Passive maintenance checks report the built-in clean stub unavailable without
  creating the lazy service. Remove the old completeness manifest/flag.
- Recursively copy/redact configuration diagnostics, including nested/query URL
  credentials, malformed URLs, IPv6, cycles and opaque objects. Inputs unchanged.
- Migrate tests which preserved the defects, align protocol docstrings, and update
  Core API/configuration/review/next-task documentation.

Validation:
- Original focused reproductions: 66 failed before the corrections.
- Initial boundary selection: 174 passed in 2.14 s.
- Broad selection: 590 passed / 1 failed / 1 skipped in 1,189.14 s. Includes all
  Core, native cache metadata workflow, surface boundaries/shared contracts,
  configuration and developer-doc checks. The failure was the old assertion
  expecting missing metadata fields to enter a handler. It now asserts early
  invalid_payload, unchanged metadata and no events, and passes in the rerun.
- Final focused selection: 207 passed in 3.09 s. Includes that corrected test,
  nine extra Store/policy cases, adapter/diagnostic regressions and quality-runner
  configuration checks. Runs overlap; do not add counts as unique tests.
- Final full quality: formatting, lint, complexity, imports/direction/footprint,
  basedpyright (0 errors), mypy (574 files), negative contracts 127/123 all passed.
- Final documentation/navigation/boundary checks: 22 passed in 9.49 s.
- PostgreSQL maintenance skipped without LIUXIN_TEST_POSTGRES_URL. Existing iso639
  deprecation warning only. No production behavior changed after broad collection.

Evidence uses `/tmp/liuxin-core-contracts-`: `regressions.{log,xml}`,
`final.{log,xml}`, `quality-final.log`. All validation processes completed.
The final quality rerun overlapped the broad suite only after verifying that all
its tool-cache root entries already existed. The pytest artifact guard rejects
new top-level entries; initialize caches first or run quality and pytest serially.

The canonical review records scope and evidence. Configuration closure (slice 2)
is next; remaining native Core metadata coverage, recovery and workflow completion
stay separate. This is not whole-Core sign-off. Git mutations need escalation
because the worktree gitdir sits under the original checkout. Commit identity is
`ajCameron <alexanderjfcameron@gmail.com>`.
