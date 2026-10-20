# Metadata container architecture

Reviewed **2026-10-20**. Value containers remain passive. Hydration belongs to Catalog; current API roots and the follow-up work are linked in [next tasks](next_tasks.md). Stage numbers and historical test totals below describe their original campaign.

The model has identities for core WEMI entities, editable metadata bundles,
attached value families and read-side projections. `LiuXinWEMIMetadata` already
provides the combined item-centred representation; a new generic Book container
is not an outstanding prerequisite.

Original design date: **2026-04-23**. The semantic distinctions below remain
useful. The physical package proposals are superseded by the canonical homes in
the placement section and the [system guide](metadata_container_system_guide.md).

See also: `metadata_container_boundaries.md` for the stage-9 semantic boundary pass.
See also: `metadata_container_dynamic_convenience_policy.md` for the stage-11 runtime-sugar policy.

## Core split

The metadata layer is split into three distinct families:

1. **Core WEMI containers**
   - These represent the primary bibliographic entities and their database-backed metadata bundles.
   - This is the load-bearing part of the metadata model.
   - Keep their identity and metadata contracts distinct from attachment families, even within a shared package root.

2. **Additional metadata containers**
   - These are attached metadata families such as titles, identifiers, notes, genres, subjects, labels, and similar record content.
   - They are metadata *about* a WEMI entity, not independent first-class entities by default.
   - Their value contracts differ from core entity identity; a separate top-level package is not required.

3. **Read-side views / snapshots / slices**
   - These are query-result containers used to present joined or layered metadata.
   - They are not the same thing as either identity objects or editable metadata bundles.
   - Examples include participation snapshots or an item WEMI title slice.

The important rule is that **core WEMI is its own thing** and **additional metadata is separate**.

## The two-object rule for independent entities

For any object that has its own durable database identity, the default pattern is:

- `XIdentityAPI`: the thing itself
- `XMetadataAPI`: the database-backed metadata bundle for that thing

This applies to:

- `Work`
- `Expression`
- `Manifestation`
- `Item`
- `Agent`, but with a deliberate naming exception described below

Storage entities such as `DigitalAsset` and `AssetReplica` follow the same
identity/metadata split when they need abstract contracts, but those contracts
belong to `LiuXin_alpha.storage.api`, not the metadata container API.

### `XIdentityAPI`

`XIdentityAPI` is for the object itself.

It should contain:

- the stable identity / primary key for the object
- object-defining fields that belong to the main row
- lightweight, object-level semantics
- no broad query-slice behavior
- no attached-metadata family management

This is the smallest stable surface for "what is this thing?".

It's a row from the database, in cached metadata form.

### `XMetadataAPI`

`XMetadataAPI` is for the object's metadata on the database.

It should contain:

- the object's core metadata bundle as stored on the database
- the attached metadata containers that belong to the object's editable metadata surface
- no hidden database query service behavior
- no responsibility for acting like a generic row proxy

This is the main editable metadata surface for the object.

## Agent as a deliberate exception

Agent needs a three-part split rather than the normal two-part WEMI pattern.
This is caused by the fact that we have two types of Agent - Org and Person.

That is:

- `AgentIdentityAPI`: the agent itself
- `AgentProfileAPI`: metadata intrinsic to the agent
- `AgentParticipationSnapshot` / related read-side views: where the agent appears in the graph

This is a deliberate naming exception.

We do **not** want `AgentMetadataAPI`, because that would read too much like the WEMI `XMetadataAPI` objects and would 
blur two different ideas:

- metadata *about the agent itself*
- joined/query-result metadata *involving the agent across the graph*

`AgentProfileAPI` is the intrinsic-agent surface.

It is the home for things such as:

- alternate names / aliases
- identifiers attached to the agent
- notes about the agent
- labels or similar attached metadata about the agent
- biographical / organisational descriptive fields

It is **not** the home for joined participation results.

Those belong in explicitly read-side objects such as:

- `AgentParticipationSnapshot`
- `AgentParticipationsByRole`
- future bibliography / involvement views

So the rule is:

- core WEMI uses `XIdentityAPI` + `XMetadataAPI`
- agent uses `AgentIdentityAPI` + `AgentProfileAPI`
- graph/join results use snapshot/view/slice objects

## What does *not* get identity + metadata by default

Most attachment families are **not** independent identities.

That means containers such as:

- titles
- identifiers
- notes
- genres
- subjects
- labels
- languages
- dates
- ratings
- series
- resources

should be treated as **additional metadata containers**, not as `XIdentityAPI` / `XMetadataAPI` pairs.

They are value-object metadata attached to a WEMI entity unless and until we deliberately promote them into first-class 
authority entities.

## Read-side objects are separate

Joined or derived read models should not be smuggled into identity or metadata APIs.

Examples:

- `AgentParticipationSnapshot`
- `ItemWemiTitleSlice`
- future browse/report/query result containers

These are allowed and useful, but they should be explicitly named as read-side views, snapshots, or slices.

## Relation edges

Metadata relation links are now treated as relation edges: durable link-table
rows with identity, local CRUD helpers, provenance, and semantic edge metadata.

The shared API vocabulary is:

- `RelationEdgeID` for the durable link-row identity
- `RelationCardinality` for one-to-one, one-to-many, many-to-one, and
  many-to-many validation
- `OneOneRelationEdgeAPI`, `OneManyRelationEdgeAPI`,
  `ManyOneRelationEdgeAPI`, and `ManyManyRelationEdgeAPI` as named structural
  APIs for those four edge shapes
- `RelationEdgeType` for the semantic role/type stored on the edge
- `RelationEdgeSource` / `source` for where the assertion came from

Each WEMI bundle still owns its own relation-edge class (`WorkRelationEdge`,
`ExpressionRelationEdge`, `ManifestationRelationEdge`, `ItemRelationEdge`) so
different relation families can validate different cardinalities.

This does **not** mean every link table becomes an independent metadata
container family. The edge is first-class inside the owning metadata bundle.
Only promote a link table into a separate container when it grows lifecycle or
attached metadata that cannot be handled as edge state.

When schema work touches link tables, prefer adding a `source` column alongside
the existing provenance-ish fields. `origin` can describe the internal creation
path; `source` should describe the external assertion source such as user input,
importer, OPF, web source, or reconciliation pass.

## Earlier package structure options

The April proposal considered the names below. They are not current import paths
or a pending renaming instruction. The canonical placement later in this guide
keeps these semantic categories within the established roots.

### Core WEMI

Core WEMI identity + metadata APIs and implementations should live together in clearly named package areas, for example:

- `metadata/api/.../core_wemi_api/...`
- `metadata/containers/.../core_wemi/...`

These example names were not adopted.

### Additional metadata

Additional metadata families should live in their own package areas, separate from core WEMI, for example:

- `metadata/api/.../additional_metadata_api/...`
- `metadata/containers/.../additional_metadata/...`

The current contract distinguishes these families without adopting these paths.

### Read-side models

Read-side snapshots / slices should either:

- live beside the family they summarize, or
- live in a dedicated read-model package

but they should be visibly marked as read-side objects.

## Naming rules

- Use `XIdentityAPI` for the thing itself.
- Use `XMetadataAPI` for the database-backed metadata bundle of core WEMI and other non-agent independent entities.
- Use plural `...Container` names for attached metadata families where that is clearer.
- Use `...Snapshot`, `...Slice`, or `...View` for read-side query results.
- Avoid using `type` when `kind` or `role` is more specific.
- Avoid mixing old `ContainerAPI` names with the new `IdentityAPI` / `MetadataAPI` pattern.

## Immediate follow-up implications

1. Core WEMI packages should be separated from additional metadata packages.
2. Legacy API package zones that predate this split should be deleted or quarantined.
3. The public export surface should reflect the split directly.
4. Agent should use `AgentIdentityAPI` + `AgentProfileAPI`, with participation and bibliography style results kept in explicit read-side snapshot/view objects.
5. Future container additions should declare up front whether they are:
   - core WEMI
   - additional metadata
   - read-side view/snapshot/slice

## Decision summary

- Core WEMI is its own thing.
- Additional metadata is separate.
- Independent core entities get `XIdentityAPI` + `XMetadataAPI`.
- Agent is the deliberate naming exception: `AgentIdentityAPI` + `AgentProfileAPI`.
- Attached metadata families stay as metadata containers.
- Read-side query results are explicit and separate.


## Placement and package boundaries

All WEMI-attached metadata APIs currently live under:
`LiuXin_alpha.metadata.api.containers_api.wemi_containers_api`

All WEMI-attached implementation containers currently live under:
`LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers`

This is the **current canonical home**, not a claim that all of those families are
semantically the same kind of object.

Within those canonical package roots we still distinguish between:
- core WEMI identity objects
- core WEMI metadata bundles
- additional metadata families
- the agent profile exception
- read-side snapshots/views/slices

There should not be a second parallel home for WEMI-attached metadata families.
If a family is attached to work / expression / manifestation / item, its API belongs in
`wemi_containers_api` and its implementation belongs in `wemi_containers`.

Legacy package trees that previously held parallel WEMI metadata APIs should be deleted
rather than kept as compatibility layers.



## 5A. Legacy zones

Location alone does not establish that a helper is dead. Inspect production imports, plugin discovery and supported callers before retiring a legacy container. Migrate those callers through the canonical value contract and remove the old path in the same change; do not create another permanent WEMI owner.

The canonical implementation home is:
- `LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers`

The canonical API home is:
- `LiuXin_alpha.metadata.api.containers_api.wemi_containers_api`

Historical helper/mixin/generic container packages should not receive new work.

See `metadata_container_naming_conventions.md` for the normalised naming rules used by the container families.


## Related notes
- `metadata_container_db_constraint_alignment.md` — how generator-enforced DB constraints line up with canonical metadata vocabularies.

- `metadata_container_vocabularies.md` - canonical homes for shared container vocabularies.


## Related notes

- `metadata_db_source_layer.md` — the read-side database source layer for metadata containers and views.


## Core WEMI hydrators

Core WEMI metadata bundles currently have concrete implementation-side hydrators
for all four levels:

- `WorkMetadataHydrator`
- `ExpressionMetadataHydrator`
- `ManifestationMetadataHydrator`
- `ItemMetadataHydrator`

These helpers live in `catalog.retrieval.hydration`, with read contracts under `catalog.api.retrieval.hydration`. They construct editable metadata bundles from database rows/views. They are not identity objects, metadata APIs, or read-side
snapshots.
