# Contributor-tooling next tasks

Reviewed **2026-10-20**, working tree based on `fc5de7af`.
Status: **proposed maintenance**. The docstring campaign is complete, the quality
ratchets exist, and CI already has one general full-suite owner.

## HACK-01 — Make test environments match their selected contracts

**First; architecture A15.** Inventory tests needing Ruff, encryption, optional
backends and external tools against the extras installed by each CI lane. The
full-suite lane currently installs `test,conversion`, while typing and encryption
are separate extras. Reproduce environment-specific failures in a clean target
before changing dependencies or skips.

Done when each required lane installs its actual requirements and a missing
optional integration produces an intentional, explained outcome. Report a fresh
full run separately from focused checks; historical failures do not establish
current remote CI state.

## HACK-02 — Guard artifact ownership and reproducibility

Classify tracked ignored outputs, true fixtures and instance configuration before
removal. Add a narrow recurrence check for confirmed generated artifacts. Keep
large external fixture payloads manifest-owned and verify their deterministic
builders, rather than checking in new copies.

Done when clean-checkout provisioning works, fixture digests/row summaries are
repeatable and incidental local files cannot enter a wheel or commit unnoticed.
History rewriting and old-object reclamation require a separate decision. Preserve
telemetry outside test scratch and retain concise portable validation summaries.

## HACK-03 — Clarify the installed optional-dependency boundary

Audit the retained top-level `past` package and capability-specific imports against
wheel contents and extras. Add one supported installed workflow at a time; the
HTML-to-EPUB gate is not evidence for every backend or format.

Done when the selected capability either works from the installed artifact or
reports its missing dependency clearly, without accidentally importing from the
checkout. Keep retired plugin/Library paths absent from distributions.

## HACK-04 — Keep conventions and historical inventories separate

The Coding and Style guides now separate implementation principles from naming
and docstring form. Maintain that division. Treat the completed inventory's hashes,
old execution policy and counts as dated evidence; do not bulk-refresh them for
every functional change.

Done when a changed public contract has meaningful prose/examples, normalized
fields and relevant behaviour checks. Source comments and executable AST checks
are appropriate for a docstring-only campaign; they are not a substitute for
functional tests when behaviour changes. Extend documentation checks to meaningful
navigation/contract failures rather than tautological wording requirements.

## HACK-05 — Improve targeted diagnostics and merge evidence

Use the existing stream selector, telemetry and immediate scratch cleanup. Add a
focused lane or corpus only for an uncovered behaviour, keeping the confidence
union and smoke sentinel contracts intact. For parser failures, preserve small,
deterministic reproductions and resource limits.

Done when a failing case identifies its owning test/input and retains useful logs
without accumulating large scratch trees. Inspect current branch rules and job
status before recommending required checks; old GitHub observations remain dated.

## Validation and close-out

Use the [CI/stream contracts](../../tests/scripts/test_ci_workflow_contracts.py),
[documentation checks](../../tests/scripts/test_developer_documentation_links.py),
[tooling regressions](../../tests/scripts/) and installed-wheel checks as appropriate.
Update the [testing index](testing/README.md), [packaging contract](packaging.md)
and [quality guide](../architectural_reviews/maintainability-quality-gates.md) with
actual results. Do not run a full suite solely for a prose correction.
