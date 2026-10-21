# Storage record mutation ownership

Implemented for PR review **2026-10-20**, as the first A10/STOR04 family.

Asset identity and Replica observations belong to the database's composed
`StorageManager`. Its persistence adapters keep SQL projections, serialized
records and revision tokens consistent inside the metadata transaction.
Changing only a SQL column can leave reads returning the old serialized value
and make optimistic revision checks ineffective.

## Core callers

The `asset` and `replica` families in `storage.resource.describe/list/get` remain
inspectable. Generic `storage.resource.create/update/delete` now refuses those
families with `storage_resource_read_only`, including aliases. This is an
intentional removal of an unsafe mutation route.

| Intent | Owned operation |
| --- | --- |
| Ingest new bytes | Core `storage.file.put`, or manager ingest methods |
| Claim existing Store bytes | Manager `adopt_location(Location(...), verify=True)` |
| Declare identity without claiming bytes exist | Manager `declare_digital_asset(...)` |
| Change descriptive Asset metadata | Manager `set_digital_asset_name(...)` and metadata setters |
| Refresh a Replica observation | Core `storage.replica.verify`, or manager `verify_replica(...)` |
| Assign Asset policies | Core `storage.asset.policies.set` |
| Remove retained bytes | Manager deletion operations under the [deletion policy](deletion_policy.md) |
| Forget metadata after inspecting physical state | Manager `forget_replica` / `forget_digital_asset`, subject to their preconditions |

Manager operations in this table are in-process APIs. This change does not add
remote wrappers for every one of them. Callers needing such a wrapper should add
a named, authorized Core operation that delegates to the owner.

Policy assignment preserves omitted fields; explicit `null` clears one. An
optional `if_revision` checks the current Asset record. The result retains
`asset_id`, the supplied assignments and `updated`, and adds the committed
`revision`. The owner validates current policy references and recreation rules,
then persists both record forms together. A validation failure rolls back the
mutation. Core reconciles derived state after commit; a reconciliation error can
therefore follow a committed update. Inspect the record before retrying.

## Existing-drive backup indexing

The FRBR scanner in `backup/prototype_pipeline.py` now adopts source files through
the bound manager, calculating actual SHA-256 identity. A rescan verifies the
existing live claim and advances its observation/revision. It does not rewrite
Asset identity when the source file changes.

If current bytes fail verification, the unhealthy observation remains persisted
and the scan raises `StoragePreconditionFailed` before backup planning. Resolve
that source discrepancy explicitly; automatic rescanning is not permission to
rebind a Location, forget an old Asset or erase bytes. Multiple live claims for
one Location also require reconciliation.

The old scanner wrote a SHA-512-plus-size fingerprint into SHA-256-named columns.
This change does not silently reinterpret or migrate those historical identities.
Such claims can fail verification and require an explicit repair decision.
New scans deduplicate equal byte identities through the manager.

Scanning remains incremental. Earlier adoptions and observations survive a later
file, callback or persistence failure. Repeating a scan reuses committed Location
claims. Existing verified claims count as `updated_files`, even if their bytes
are unchanged; new claims count as `inserted_files`. A file that disappears from
directory enumeration is not automatically forgotten. Adoption and verification
do not pin a source against external filesystem changes.

## Scope and remaining work

This slice closes generic Asset/Replica writes, policy assignment and the direct
FRBR scanner. Existing SquashFS publication already adopts verified claims through
the manager. Raw database/Store access remains privileged.

Composite, Item-link and policy-definition admin mutations still use retained
row adapters. Legacy `files` reconciliation, consistent partial-effect receipts
across all writers, publication recovery and derived-state refresh remain A10
work. The entire storage consolidation is not complete.

Regression evidence lives in
[Core ownership tests](../../tests/core/test_storage_record_ownership.py),
[scanner ownership tests](../../tests/storage/backup/test_frbr_index_ownership.py)
and the [backup pipeline tests](../../tests/storage/backup/test_existing_drive_squashfs_prototype.py).

Return to [storage](README.md).
