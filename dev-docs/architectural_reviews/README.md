# Architecture and reviews

Start with the [current overview](<../02 - Top Level Structure.md>) and
[ownership rules](<../04 - Seperation of Concerns.md>). This folder currently holds
several kinds of material; their authority differs.

## Decisions and active enforcement

- [Target architecture](target-architecture.md): decisions and rationale, with
  implementation status per area. Lifecycle and Catalog work are partly implemented;
  the document is not one wholly unimplemented proposal.
- [Quality gates](maintainability-quality-gates.md): current scopes and commands.
- [Layer direction](layer-direction-ratchet.md): allowed directions and reviewed debt.
- [Import footprint](import-footprint-ratchet.md): runtime import boundaries.
- [Plugin composition](../plugin_system/plugin-composition.md): unified package,
  passive startup, shared loading policy and retired-namespace migration.

## Dated findings

- [Whole-repository review, 2026-09-26](architecture-review-2026-09-26.md), with
  the source-checked 2026-10-18 implementation and 2026-10-19 plugin follow-ups.
- [Opening-path review, 2026-10-09](lifecycle-opening-path-review-2026-10-09.md).
- [Documentation and project structure, 2026-10-18](documentation-structural-review-2026-10-18.md).
- [Earlier top-level review](top-level-architecture-review.md).

Original measurements and source line numbers describe their recorded checkpoint.
Follow-up notes identify completed changes and remaining work separately.

## Completed campaigns

- [Import shim removal](shim-removal-plan.md): historical decisions and verification.

Return to the [developer index](../README.md).
