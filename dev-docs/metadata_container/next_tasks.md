# Next metadata container tasks

Date: **2026-10-20**. Proposed work; this list does not implement the changes.

Preserve the distinction between identity, editable values, relation edges and read projections. Persistence and hydration ownership is already in Catalog.

## MC01. Specify selection and completeness together

**Next; depends on CAT02.** Distinguish one selected WEMI identity path from the full relation graph. Extend eager/lazy hydration fixtures with multiple works per manifestation, unloaded relations and missing optional branches.

Done when each result identifies its selection/completeness behaviour and loading one projection neither silently loses other graph edges nor mutates them.

## MC02. Make exchange losses visible

**Next.** Document which container fields survive OPF/Calibre-shaped exchange and which require the structured WEMI representation. Connect loss reporting to the conversion outcome contract rather than adding persistence to containers.

Done when round trips assert preserved values and explicitly reported losses, including link roles, provenance, primary flags and multi-parent paths.

## MC03. Align vocabularies at actual stored columns

**Following.** Keep identifier vocabularies in metadata and generated constraints in databases. Review Agent role type tables and unused family placeholders when a real schema column or consumer needs them.

Done when one vocabulary change is reflected in containers, writer validation and generated constraints without inventing columns just to consume placeholders.

## MC04. Protect family APIs and retire only proven duplicates

**Following.** Maintain explicit methods alongside dynamic convenience properties. Check API/export parity and live callers before removing older container helpers.

Done when a representative family has validation, mapping, relation and payload coverage, and any retired path has no supported callers or hidden plugin-discovery use.

## Validation and routing

Use the [test surface](metadata_container_test_surface.md), [facade workflows](metadata_facade_workflows.md) and Catalog hydration tests. A successful value-object construction is not proof that a database write is valid.

Return to [metadata container](README.md).
