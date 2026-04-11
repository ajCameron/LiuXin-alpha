# Digital assets, replicas and storage policy

## Core graph

The storage-facing graph is now:

- `items`
- `digital_assets`
- `asset_replicas`

An item is the library-facing exemplar.
A digital asset is the storage-managed byte-bearing payload.
An asset replica is one physical copy of that payload on one store.

In shorthand:

`items -> digital_assets -> asset_replicas`

## Why this split exists

We do not want one table to mean all of the following at once:

- the library-facing exemplar
- the storage-managed payload
- the concrete path on a specific store

Those are different concerns.

## Tables

### `digital_assets`

One row per managed digital payload.
Usually this is one exact byte sequence.
Composite assets are also allowed, but they are logical assemblies and do not have direct replicas.

### `asset_replicas`

One row per physical copy of one digital asset on one store.
This is where storage placement, presence, observed hashes, and verification state live.

### `digital_asset_item_links`

Semantic links between items and digital assets.
The link carries the role.
Examples:

- `primary_payload`
- `cover`
- `page_scan`
- `ocr_text`
- `metadata_sidecar`
- `preview`
- `thumbnail`
- `supplement`
- `source_archive`
- `derived_output`

The role is deliberately not stored on `digital_assets` itself because the same asset may play different roles in different contexts.

### `digital_asset_compositions`

Ordered membership links for multipart payloads.
This is for things like multi-file audiobooks.
It is not the same as derivation.

Composition means “these assets together form this whole”.
Derivation means “this asset was produced from that asset”.

## Policy tables

Storage policy is now first-class in the schema.

### `replication_policies`

Desired-state policies for live readable copies.
These describe floors, targets, spread rules, and store selection constraints.

### `backup_policies`

Desired-state policies for backup/archive copies.
These describe backup copy counts, spread rules, verification expectations, and retention flags.

`digital_assets` may point directly at one replication policy and one backup policy.
This is the first step, not the final word on inheritance/overrides.

## Store metadata needed for policy

`stores` now also carries:

- `store_failure_domain`
- `store_region`
- `store_tags_json`

These exist so policy resolution can express things like:

- keep copies in different failure domains
- prefer a certain region
- require or forbid tagged stores

## Invariants

Important invariants now enforced in SQL/triggers include:

- `asset_replicas` may only point at atomic digital assets
- `asset_replica_storage_key` must remain store-relative
- `asset_replica_folder_id`, when present, must match the replica store
- `digital_asset_compositions` parents must be composite assets
- `digital_asset_compositions` must not contain cycles

## Practical rule of thumb

- library reasons in items
- storage reasons in digital assets and replicas
- link tables carry semantic attachment roles
- composition is explicit
- derivation is separate
