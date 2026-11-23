# Whole-Core review and completion plan — 2026-11-23

Goal: detailed review of all Core responsibilities, strengths and weaknesses,
plus an ordered plan for full functionality. The earlier integration review was
explicitly narrower and did not satisfy that whole-module scope on its own.

Reviewed `04d6fd7d` on `core-review`, including lifecycle fixes in `eb096f3c`.
Canonical artifact: `dev-docs/core/review-and-completion-plan-2026-11-23.md`.
Core README/next tasks now route to it; the integration review links forward.
No production code changed for this review. Preserve original checkout/stashes.

Evidence:
- Source inventory: 71 Python modules, 32,611 physical lines including docs.
- Standard runtime: 97 command and 111 query registrations. Capability manifest
  omits 59 registered names yet reports complete; native metadata's four routes
  are among the omissions. maintenance.clean advertises available but its built-in
  implementation refuses with capability_unavailable.
- Disposable probes reproduce repeated command ID effects, ambiguous serialization
  failure after effects, in-flight command result-policy replacement, event-buffer
  overflow without gap metadata, permissive primitive coercion and incomplete
  diagnostic redaction. They use fake collaborators and synthetic values.
- Fresh import of utils.ptempfiles adds PyQt4Ban and builtins.lopen.
- New focused run: 107 passed / 1 skipped in 135.43 s. Covers authority, job waits,
  durable audit/maintenance, wire/request limits and configuration propagation.
  Skip: live PostgreSQL maintenance needs LIUXIN_TEST_POSTGRES_URL. One existing
  iso639 deprecation warning. Test process completed successfully.
- Rechecked existing 405-pass integration log (one PostgreSQL skip), 229-pass
  caller-migration selections and full quality success (573 mypy files,
  127/123 negative type examples). Those selections overlap; do not add their
  counts as unique tests. Source runtime code is unchanged by this review.
- Documentation/navigation/boundary checks: 22 passed in 8.87 s.
- Final diff whitespace check passed. Documentation-only follow-up; no production
  source changes or altered regression expectations.

Temporary evidence: /tmp/liuxin-core-full-review-probe.py,
/tmp/liuxin-core-full-review-evidence.json and
/tmp/liuxin-core-full-review-tests.log. These are disposable local artifacts,
not repository dependencies. The canonical review records outcomes and source
owners so its findings do not depend on retaining those files.

Assessment: preserve the architecture; do not claim whole-Core sign-off.
Plan: contract corrections first, then configuration, native metadata coverage,
recoverable writes, durable workflow migration, bounded execution, event recovery,
domain/API consolidation and full backend/operational acceptance. Completing this
review goal means delivering that evidenced assessment and plan, not claiming
that the proposed implementation programme is already finished.
