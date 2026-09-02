# Metadata container family completeness matrix

This note records the stage-8 audit of the metadata container families.

See also: `metadata_container_boundaries.md` for the semantic category split added in stage 9.

## Scope

The goal of this pass is to track both the earlier completeness audit and the stage-18 expansion of the remaining obvious metadata families. It is to make sure
the existing families look complete and intentional across the current package
surface:

- API module exists
- implementation module exists
- package exports exist
- naming matches the current architecture
- docstrings exist at a useful level
- convenience sugar exists only where it is helpful
- read-side helper objects exist where they are genuinely needed

## Matrix

| Family | Category | API | Impl | Exported | Naming | Docstrings | Convenience sugar | Read-side helper | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Work identity/metadata | Core WEMI entity | Yes | Yes | Yes | Yes | Yes | Minimal | Hydrator | Core WEMI entity pair. |
| Expression identity/metadata | Core WEMI entity | Yes | Yes | Yes | Yes | Yes | Minimal | Hydrator | Core WEMI entity pair. |
| Manifestation identity/metadata | Core WEMI entity | Yes | Yes | Yes | Yes | Yes | Minimal | Hydrator | Core WEMI entity pair. |
| Item identity/metadata | Core WEMI entity | Yes | Yes | Yes | Yes | Yes | Minimal | Hydrator | Core WEMI entity pair. |
| Agent identity/profile | Agent exception | Yes | Yes | Yes | Yes | Yes | Minimal | Yes | Deliberate exception to `XMetadataAPI`. |
| Agent credits | Additional metadata family | Yes | Yes | Yes | Yes | Yes | Role sugar | N/A | Ordered per-role containers. |
| Agent participation | Read-side view | Yes | Yes | Yes | Yes | Yes | N/A | Yes | Read-side snapshot/view layer. |
| Titles | Additional metadata family | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Includes `ItemWemiTitleSlice`. |
| Notes | Additional metadata family | Yes | Yes | Yes | Yes | Yes | Yes | No | Typed free-text metadata. |
| Labels | Additional metadata family | Yes | Yes | Yes | Yes | Yes | Yes | No | Still somewhat provisional. |
| Genres | Additional metadata family | Yes | Yes | Yes | Yes | Yes | Light | No | Narrower than labels. |
| Subjects | Additional metadata family | Yes | Yes | Yes | Yes | Yes | Yes | No | Thin skin over label-like semantics. |
| Identifiers | Additional metadata family | Yes | Yes | Yes | Yes | Yes | Yes | No | Database-constrained scheme family. |
| Languages | Additional metadata family | Yes | Yes | Yes | Yes | Yes | Yes | No | Language attachments with kind-based sugar. |
| Dates | Additional metadata family | Yes | Yes | Yes | Yes | Yes | Yes | No | Flexible date/range attachments. |
| Ratings | Additional metadata family | Yes | Yes | Yes | Yes | Yes | Yes | No | Numeric rating attachments with kind-based sugar. |
| Series | Additional metadata family | Yes | Yes | Yes | Yes | Yes | Yes | No | Series-entry attachments with numbering support. |
| Resources | Additional metadata family | Yes | Yes | Yes | Yes | Yes | Yes | No | External link/resource attachments. |

## Stage-8 fixes applied

This pass closes a few small but real consistency gaps:

- Added explicit `__all__` declarations to the remaining public identity/profile
  API modules that were still relying on implicit export.
- Added an explicit `__all__` declaration to the `work_container` implementation
  module so the implementation side follows the same explicit-export rule.
- Recorded the family-completeness state in this document so later cleanup stages
  can distinguish intentional asymmetry from accidental drift.

## Public documentation ratchet

The 2026-09-01 repository maintainability pass reviewed the metadata model at
its public boundaries. Every module and top-level public class is now
documented across:

- main-table row and inline self-relation APIs;
- concrete non-WEMI row and tree-relation containers;
- WEMI identity and metadata API families;
- WEMI identity, metadata bundle, typed-value, per-kind, and target-wide
  implementation containers.

`tests/scripts/test_public_documentation_boundaries.py` enforces that coverage
and rejects known placeholder prose. The gate is deliberately semantic: it
does not require generated docstrings on private helpers or invent example
blocks for classes whose examples have not yet received human review.

## Known remaining rough edges

These are not stage-8 blockers, but they are worth keeping visible:

1. `labels` still feels semantically unstable and may eventually be split or
   reduced further.
2. Core WEMI bundle implementations now have hydrators for work, expression, manifestation, and item.
3. The old quarantine stub files have been removed; stale imports should fail
   clearly instead of passing through quarantine modules.
4. The metadata DB source layer is still sparse and does not yet mirror the full
   W/E/M/I family pattern.

## Exit condition for this stage

Stage 8 is considered complete when the current metadata families are visibly
complete enough that future work can focus on consolidation and tests, rather
than on chasing missing counterpart files or forgotten exports.

## Related policy

Families that expose runtime-installed convenience properties are governed by
`metadata_container_dynamic_convenience_policy.md`.


## Source layer note

The `metadata_db_source` package is a real read-side infrastructure layer. It should remain symmetric across Work / Expression / Manifestation / Item, with the deliberate agent exception (`identity` + `profile` + participation snapshot).
