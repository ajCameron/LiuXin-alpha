# Metadata DB source layer

The `metadata.api.metadata_db_source` package is **real infrastructure**.
It is not abandoned scaffolding.

## What it is for

This layer defines the read-side contracts that sit between:

- the raw database / row layer, and
- metadata identity containers, metadata bundles, and read-side snapshots.

In other words: this is where the project says **how metadata objects are read from the database**.

## What lives here

The layer is intentionally narrow.

- `WorkMetadataGetterAPI`
- `ExpressionMetadataGetterAPI`
- `ManifestationMetadataGetterAPI`
- `ItemMetadataGetterAPI`
- `AgentProfileGetterAPI`
- `DBMetadataSourceAPI`

## What does not live here

This is **not** the place for:

- editable metadata container implementations
- identity container implementations
- ad-hoc query helpers unrelated to metadata construction
- storage APIs
- library orchestration logic

## Symmetry rule

The source layer should mirror the real metadata architecture:

- core WEMI entity sources expose identity + metadata-bundle getters
- the agent source exposes identity + profile + participation snapshot getters
- read-side snapshots/views/slices are retrieved here, but remain distinct from editable metadata bundles

## Current state

The layer is currently **API-only**.
That is acceptable.

Concrete hydrators/factories may live elsewhere for now, but the source-layer
contracts should still be symmetric so the architecture remains clear.

## Concrete hydrator note

The current implementation side now exposes concrete metadata hydrators for all
core WEMI bundle containers:

- `WorkMetadataHydrator`
- `ExpressionMetadataHydrator`
- `ManifestationMetadataHydrator`
- `ItemMetadataHydrator`

Those hydrators are implementation helpers, not source-layer contracts. The
source layer remains API-first, but the bundle-construction path is now
concretely symmetric across core WEMI.
