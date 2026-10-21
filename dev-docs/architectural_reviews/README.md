# Architecture and reviews

Reviewed **2026-10-20**, against the working tree based on `fc5de7af`.
This folder holds decisions, enforcement guides and dated findings. Their
statuses differ; the presence of a proposal here does not make it implemented.

Start with the [package overview](<../02 - Top Level Structure.md>) and
[ownership rules](<../04 - Seperation of Concerns.md>). The ownership guide and
linked subsystem contracts govern new code. Use the target record for rationale,
the gate guides for enforcement, and dated reviews for evidence and remaining
findings. [Next tasks](next_tasks.md) proposes a prioritized implementation
sequence with owners, dependencies and completion checks. The
[cross-project work list](../global_todo.md) routes work to its detailed owner;
historical action lists are not competing roadmaps.

## Decisions and active enforcement

| Document | Status and purpose |
| --- | --- |
| [Target architecture](target-architecture.md) | Accepted ownership decisions, actual implementation and explicit remaining migrations. Lifecycle is implemented; durable maintenance and multi-library runtime selection are not. |
| [Quality gates](maintainability-quality-gates.md) | Current runner order, configured scopes, CI relationship and limits. The broad docstring campaign is complete; current checks remain scoped. |
| [Layer direction](layer-direction-ratchet.md) | Protected acyclic graph plus exact whole-package debt. Passing the baseline does not mean all dependencies are correct or acyclic. |
| [Import footprint](import-footprint-ratchet.md) | Fresh-process module budgets and forbidden dependencies; complements static graphs. |

[Plugin composition](../plugins/plugin-composition.md) is the subsystem contract
for the unified `plugins` package, passive imports and loading policy. Both
`customize` and `plugin_system` are retired; their names in older evidence are
historical, not compatibility APIs.

## Dated findings

| Document | Latest interpretation |
| --- | --- |
| [Whole-repository review, 2026-09-26](architecture-review-2026-09-26.md) | Original 68 findings preserved, with October 18 status tables and October 19–20 follow-ups. Read the latest note before using the original action list. |
| [Opening-path review, 2026-10-09](lifecycle-opening-path-review-2026-10-09.md) | Source-checked October 20: graph ownership implemented; borrowed resources, partial construction, transaction semantics and remaining wrappers are explicit. |
| [Documentation and project structure, 2026-10-18](documentation-structural-review-2026-10-18.md) | Dated baseline plus implementation follow-ups. Numbered guides and this folder have been reviewed; the larger proposed directory reshuffle is not an adopted migration. |
| [Earlier top-level review](top-level-architecture-review.md) | September recommendations with an October 20 disposition for all five themes. Original counts and package names remain dated evidence. |

## Completed campaigns

| Document | Status and limits |
| --- | --- |
| [Import shim removal](shim-removal-plan.md) | Completed September campaign with an October 20 interpretation. Later intentional lazy loading and retained adapters mean this is not a claim that every shim in the repository is gone. |

The index, next-task list and nine decision/review guides make eleven Markdown
files in this folder.
Clear authority and links are sufficient for this review; no further path move is
needed to distinguish current contracts from history.

## Updating a review

Preserve finding IDs, original line references and dated measurements. Add a dated
follow-up with the source checkpoint, concrete owner/caller changes, relevant
regression evidence and explicit remaining work. Use **resolved** for the named
boundary or defect, **partial** for a completed part with a stated remainder, and
**open** when the change is absent or still needs verification. Implementation,
local validation, publication and merge status are separate facts.

Keep external PR/branch-protection observations tied to their observation date;
a source review does not refresh GitHub state. Distinguish executed gates from
dry runs, test collection from execution, and focused tests from a full suite.
Ignored evidence directories are local-only; retain enough results and commands
in tracked prose to make the conclusion useful in a clean checkout.

Return to the [developer index](../README.md).
