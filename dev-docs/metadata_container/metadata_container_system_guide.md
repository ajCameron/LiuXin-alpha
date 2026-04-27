# Metadata Container System Guide

Date: 2026-04-26
Status: working guide / current intended system shape

This note is the single guided walkthrough of the metadata container system as it exists after the container cleanup sweep.

It is not the place for every local naming rule or every narrow policy detail.
Those still live in the smaller focused notes.
This guide is the "what is this system, how is it meant to fit together, and where does a new thing go?" document.

See also:
- `metadata_container_architecture.md`
- `metadata_container_boundaries.md`
- `metadata_container_family_matrix.md`
- `metadata_container_naming_conventions.md`
- `metadata_container_dynamic_convenience_policy.md`
- `metadata_container_vocabularies.md`
- `metadata_container_db_constraint_alignment.md`
- `metadata_container_test_surface.md`
- `metadata_db_source_layer.md`

## 1. The core split

The metadata container system is built around a deliberate split between:

1. **Core WEMI entity surfaces**
2. **Additional attached metadata families**
3. **Read-side snapshots / views / slices**

That split matters more than the fact that many of these objects currently live under the same canonical package roots.

The important rule is:

- **Core WEMI is its own thing**
- **Additional metadata is separate**
- **Read-side models are explicit and separate again**

## 2. The major object categories

### 2.1 Identity APIs

An identity API is the smallest stable object-level surface for a durable entity.

Examples:
- `WorkIdentityAPI`
- `ExpressionIdentityAPI`
- `ManifestationIdentityAPI`
- `ItemIdentityAPI`
- `AgentIdentityAPI`
- storage-side identities such as `DigitalAssetIdentityAPI`

Identity APIs answer the question:

> What is this thing?

They should hold:
- the primary identity of the entity
- object-defining row-level fields
- lightweight object semantics

They should **not** become broad query facades.

### 2.2 Metadata APIs

A metadata API is the editable database-backed metadata bundle for a core entity.

Examples:
- `WorkMetadataAPI`
- `ExpressionMetadataAPI`
- `ManifestationMetadataAPI`
- `ItemMetadataAPI`
- storage-side metadata APIs for storage entities

Metadata APIs answer the question:

> What editable metadata bundle belongs to this entity on the database?

They may contain:
- attached metadata containers
- editable metadata fields
- bundle-level convenience access

They should **not** act like generic row proxies.
They should **not** hide joined graph traversal behind a metadata-shaped name.

### 2.3 Agent profile as the deliberate exception

Agent follows a slightly different rule.

We use:
- `AgentIdentityAPI`
- `AgentProfileAPI`
- explicit read-side participation objects such as `AgentParticipationSnapshot`

We do **not** use `AgentMetadataAPI`.

That is deliberate. The name `AgentMetadataAPI` would blur two different ideas:
- metadata intrinsic to the agent itself
- metadata involving that agent across the graph

`AgentProfileAPI` is the intrinsic-agent surface.
Participation and bibliography-style results stay in explicitly named read-side objects.

### 2.4 Additional metadata families

Additional metadata families are attached metadata containers.
They are not independent first-class identities by default.

Examples:
- titles
- identifiers
- notes
- genres
- subjects
- labels
- agent credit containers

These families answer questions like:
- what titles attach to this work?
- what identifiers attach to this manifestation?
- what notes attach to this item?

They are value-object-heavy container families, not entity identity pairs.

### 2.5 Read-side snapshots / views / slices

These are query-result or composition objects.
They exist to present joined or layered metadata.

Examples:
- `AgentParticipationSnapshot`
- `AgentParticipationsByRole`
- `ItemWemiTitleSlice`

These answer questions like:
- where does this agent appear?
- what is the layered title for this item across W/E/M/I?

They are **not** editable metadata bundles.
They are **not** identity objects.
They should be visibly named as snapshots, views, or slices.

## 3. Canonical package homes

The current canonical homes are:

### API side

`LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api`

### Implementation side

`LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers`

This is a placement decision, not a semantic collapse.
A family living under the canonical WEMI-attached package root is **not** automatically the same kind of object as every other family there.

Within those roots we still distinguish between:
- core identity APIs
- core metadata APIs
- the agent profile exception
- additional metadata families
- read-side snapshots / slices / views

## 4. The core WEMI pattern

For core W/E/M/I entities the intended shape is:

1. `XIdentityAPI`
2. `XMetadataAPI`
3. implementation objects for both surfaces
4. metadata hydrator
5. metadata DB-source getter surface

In practice that means:
- `WorkIdentityAPI` + `WorkMetadataAPI`
- `ExpressionIdentityAPI` + `ExpressionMetadataAPI`
- `ManifestationIdentityAPI` + `ManifestationMetadataAPI`
- `ItemIdentityAPI` + `ItemMetadataAPI`

And on the implementation side:
- `WorkIdentity` + `WorkMetadata`
- `ExpressionIdentity` + `ExpressionMetadata`
- `ManifestationIdentity` + `ManifestationMetadata`
- `ItemIdentity` + `ItemMetadata`

These are the load-bearing entity surfaces of the metadata system.

## 5. Hydrators and DB-source getters

The metadata bundle story is not just APIs and containers.
There are two additional pieces in the system.

### 5.1 Hydrators

Hydrators build implementation metadata bundles from the database-facing inputs.

Examples:
- `WorkMetadataHydrator`
- `ExpressionMetadataHydrator`
- `ManifestationMetadataHydrator`
- `ItemMetadataHydrator`

Hydrators are part of the construction path for the editable metadata bundles.

### 5.2 Metadata DB-source getters

The metadata DB-source layer defines the database-facing read surface for building those bundles.

Examples:
- `WorkMetadataGetterAPI`
- `ExpressionMetadataGetterAPI`
- `ManifestationMetadataGetterAPI`
- `ItemMetadataGetterAPI`
- `AgentProfileGetterAPI`

This layer is real infrastructure.
It is not dead scaffolding.

## 6. Additional metadata family pattern

Additional metadata families generally follow a repeating pattern:

1. a vocabulary enum or controlled kind/status set
2. a base value object
3. W/E/M/I-specialised value objects where needed
4. a container or grouped container API
5. an implementation container
6. optional convenience sugar
7. optional read-side helper if the family naturally wants one

Examples:
- titles
- notes
- identifiers
- subjects
- genres
- labels

These families do **not** get `XIdentityAPI` + `XMetadataAPI` by default.
They are attached metadata families, not independent entities.

## 7. Agent pattern

Agent is the one place where the system deliberately does not mirror W/E/M/I exactly.

The intended split is:

1. `AgentIdentityAPI`
2. `AgentProfileAPI`
3. agent credit containers
4. participation snapshots / views
5. `AgentProfileGetterAPI`

This gives us:
- a stable identity surface
- an intrinsic agent profile surface
- separate read-side graph participation objects

That separation is important because agent is both:
- an entity in its own right
- a graph endpoint attached all over W/E/M/I

## 8. Dynamic convenience sugar

Several additional metadata families expose runtime-installed convenience properties or methods.

Examples include things like:
- role-specific text helpers
- scheme-specific convenience access
- kind-specific convenience access

The current policy is:
- this sugar is allowed for now
- it is **not** the canonical load-bearing API
- the generic explicit container APIs remain canonical
- the sugar should be driven from canonical vocabularies
- later code generation or explicit static surfaces may replace this runtime installation approach

In other words: the sugar is real, useful, and acceptable, but it is not the architectural centre of gravity.

## 9. Canonical vocabularies

Controlled vocabularies now have canonical homes.

### Database-constrained vocabularies

These live in `databases`, especially `db_types.py`.

Examples:
- identifier schemes
- identifier entity-type allow-lists
- MARC relator controlled sets used by database-facing logic

### Shared additional-metadata vocabularies

These live in:

`LiuXin_alpha.metadata.constants.container_vocabularies`

Examples:
- `TitleKind`
- `NoteKind`
- `NoteFormat`
- `NoteVisibility`
- `LabelKind`
- `GenreKind`
- `SubjectKind`
- `IdentifierStatus`

The rule is simple:
- one canonical source of truth
- APIs and implementations import from that source
- DB constraints draw from the canonical source where the schema is meant to enforce that vocabulary

## 10. Naming rules in practice

The naming rules are intentionally boring.

Use:
- `XIdentityAPI` for the entity surface
- `XMetadataAPI` for the editable metadata bundle of core independent entities
- `AgentProfileAPI` for intrinsic agent metadata
- `...Snapshot`, `...View`, `...Slice` for read-side result models
- `kind`, `role`, or `scheme` instead of vague `type`
- `to_text(...)` / `*_text` for joined text rendering in metadata families

Avoid:
- vague `DataAPI` / `DetailsAPI` names
- calling a read-side graph result “metadata”
- treating convenience sugar as the canonical API
- mixing old `ContainerAPI` naming with the newer identity/metadata/profile naming model

## 11. What a new family should declare up front

When adding a new metadata-related family, decide explicitly which of these it is:

1. **Core entity identity**
2. **Core entity metadata bundle**
3. **Additional attached metadata family**
4. **Agent-profile-like intrinsic bundle**
5. **Read-side snapshot/view/slice**

If that is not declared up front, the family will drift.

Questions to answer first:
- Does this thing have its own durable database identity?
- Is this intrinsic to an existing entity, or attached metadata about it?
- Is this editable bundle data, or is it a read-side composition/query result?
- Does it need a hydrator?
- Does it need a metadata DB-source getter?
- Does it need controlled vocabularies?
- Does the database need to constrain those vocabularies?

## 12. What not to do

Do not:
- treat all metadata-adjacent objects as the same kind of surface just because they live under one package root
- smuggle joined query behaviour into identity APIs
- turn metadata APIs into generic row proxies
- create a second parallel package home for WEMI-attached metadata families
- duplicate controlled vocabularies locally in each family module
- let runtime sugar become the only usable surface

## 13. The current state of the system

At the end of the sweep so far, the system now has:
- explicit W/E/M/I identity + metadata pairs
- explicit agent identity + profile + participation split
- canonical homes for WEMI-attached APIs and implementations
- canonical homes for shared metadata-family vocabularies
- metadata DB-source API symmetry
- W/E/M/I hydrator symmetry
- smoke-test coverage for the metadata package surface
- focused dev notes for boundaries, naming, vocabularies, test surface, and DB alignment

That means the system is now in a state where new work should be additive and deliberate rather than archaeological.

## 14. Practical next-step rule

If you are touching the metadata system and are unsure where something belongs, ask these questions in order:

1. Is it a durable entity in its own right?
2. Is it intrinsic to that entity, or attached metadata about it?
3. Is it an editable bundle, or a read-only joined/query result?
4. Does it belong to core WEMI, the agent exception, or an additional metadata family?

If you answer those correctly, the package home, naming, and supporting infrastructure usually follow from that.
