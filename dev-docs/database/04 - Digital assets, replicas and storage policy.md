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

Like metadata relation edges, storage link tables should also reserve room for
provenance. Generated main-schema relation tables include a nullable `source`
column for where the link assertion came from; the role/type column should
remain the semantic attachment role.

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


## Replica modes and why they exist

Not every replica is meant to count the same way.
The schema therefore needs an explicit replica mode on each physical copy.
A replica should not just answer “where does this asset exist?”, but also “what kind of copy is this meant to be?”.

Typical modes are:

- `active` — a live readable copy that may satisfy normal reads
- `backup` — a recoverable copy that counts toward backup policy but is not preferred for normal reads
- `archive` — a cold or deep-archive copy, such as tape
- `cache` — a transient convenience copy that should not count toward durable policy
- `transient` — a short-lived work copy created by an operation
- `unmanaged` — an observed copy that exists but is not yet policy-managed

This matters because the same store may be suitable for some modes and not others.
For example:

- a local SSD cache store may support `active` and `cache`, but not `archive`
- a tape store may support `archive`, but not `active`
- a slow read-only mirror may support `backup` or `archive`, but not `active`

The database should therefore track both:

- the mode of each `asset_replica`
- which replica modes each `store` can support

A practical field name for the store side is something like `store_supported_replica_modes_json`.
That can later be normalized if it starts to hurt, but it is enough to express “this store can hold archive copies but should never be selected for live replicas”.

## Policy inheritance and defaults

Direct policy assignment on `digital_assets` is useful, but not sufficient on its own.
The database also needs a durable notion of default policy at the storage-location level.

The likely first step is explicit foreign keys such as:

- `stores.store_default_replication_policy_id`
- `stores.store_default_backup_policy_id`
- `folders.folder_default_replication_policy_id`
- `folders.folder_default_backup_policy_id`

This makes policy resolution legible and queryable.
It is better than scattering important defaults only through code or opaque JSON blobs.

A useful rule of thumb is:

- policy rows describe desired state
- default-policy foreign keys describe where that policy comes from when an asset has no explicit override
- replica rows describe what physical copies actually exist now

## Composite and atomic asset rules

Composite assets are logical assemblies, not directly replicated byte objects.
The database should therefore enforce a few hard rules:

- only `atomic` digital assets may have `asset_replicas`
- composition parents must be `composite`
- composition members must be `atomic` unless a later use-case genuinely needs nested composites
- composition graphs must not contain cycles

This keeps multipart payloads clean.
For example, a multi-file audiobook can be represented as one composite digital asset whose ordered members are atomic MP3 assets.
The tape copy, SSD copy, and NAS copy then live on the member assets, not on the composite row itself.

## Useful indexes

The new shape also implies a few practical indexes:

- index `asset_replicas` by `digital_asset_id`
- index `asset_replicas` by `store_id`
- index `digital_asset_compositions` by `member_asset_id` for reverse lookups
- keep `parent + sequence_number` indexed for ordered multipart traversal

These are not glamorous, but they matter once repair, reconciliation, or reverse-membership queries start happening at scale.
