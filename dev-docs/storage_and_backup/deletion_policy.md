# Managed deletion and recovery

Implementation review: **2026-10-20**, architecture **A05 / STOR03**.

[`StorageDeletionMixin`](../../src/LiuXin_alpha/storage/storage_manager/mixins/deletion.py)
owns both `manager.remove_replica(...)` and `manager.delete(location)`.
Bound-Location deletion reaches that same manager method. Core evacuation calls
`remove_replica`; the CLI uses the Core endpoint and preserves its failure and
recovery receipts.

## Permitted operations

Byte deletion requires a registered live claim. `UNMANAGED`, `ARCHIVE` and
`BACKUP` copies are protected, even when another copy exists or a backup policy
does not have its retention lock set. Read-only configuration and unsupported
Store operations reject before a reservation is written.

For `ACTIVE`, `CACHE` and `TRANSIENT` copies, the manager requires another
physically distinct Location with both a retained VERIFIED claim and a fresh
successful digest inspection. A stale claim, duplicate claim for the same
Location, pending deletion, unavailable object or corrupt object is insufficient.
The remaining copies must also satisfy the affected mode's configured minimum,
Store tags and joint separation rules. An unrelated pre-existing backup deficit
does not prevent deletion of a redundant active copy.

This is deliberately conservative: a zero-copy or recreate-on-loss policy is
**not permission to erase the last verified copy**. Recipe planning remains
available for actual loss, but automatic eviction and destructive overrides are
not implemented here. `retention_priority` ranks policy preferences; it is not
an override token.

`delete_bytes=False` changes only the claim. `forget_replica` also remains a
metadata operation. Neither can discard an active deletion reservation. Keeping
a tombstone while preserving bytes remains supported and returns a warning.
Deleting a Replica does not delete its Digital Asset or bibliographic entities.

```python
# manager already owns an ingested Asset and two configured writable Stores.
survivor = manager.replicate_digital_asset(
    asset_id, destination_store_ref=other_store_uuid, verify=True,
)
receipt = manager.remove_replica(old_replica_id)
# A fresh survivor check precedes deletion; the old claim is now DELETED.
```

Raw `Store.delete`, `Store.move`, driver deletion and direct persistence ports
are **privileged mechanisms outside manager loss policy**. Tests simulating
external loss use these mechanisms explicitly. Existing callers deleting
unregistered Locations through the manager must register appropriate claims or
use a deliberately scoped low-level operation. The manager has no `force` flag.

## Reservation and concurrency

The operation has three separate phases:

1. Reserve the metadata writer, inspect current claims and surviving bytes, and
   commit fresh-revision `DELETING` claims for the target Location.
2. Request physical deletion from the Store.
3. In a new metadata unit, check the exact reservation revisions and replace the
   claims with `DELETED` tombstones, or forget the explicitly requested claim.

Database-backed storage metadata units use the existing macro transaction owner:
SQLite `BEGIN IMMEDIATE` or a PostgreSQL advisory transaction lock shared by
serialized units in the same database/schema. Their reads bypass per-facade
caches while serialized. This matters when another manager's cache still shows
a previously verified copy.

Deletion phases require an outermost transaction. Calling deletion inside an
ordinary or serialized macro unit fails before effects: releasing a savepoint
would leave the reservation invisible until the caller commits. See the
[transaction contract](../databases/transactions.md).

A competing deletion sees the committed reservation and cannot count it as a
survivor. Losing the first SQL connection after reservation does not turn that
copy back into a usable candidate. Verification and reconciliation cannot clear
the pending state. Verification also checks its original revision before publishing
an observation, so a late result cannot revive a completed deletion's tombstone.
Transient managers coordinate threads sharing that manager;
their in-memory reservations do not survive process loss.

These locks coordinate participating metadata owners, not arbitrary raw SQL or
external filesystem/object-store actors. A Store's supported version precondition
is forwarded; a previously observed version is used automatically when the
Store advertises conditional deletion. Backends without that capability cannot
provide atomic protection against an external object replacement.

## Failures and recovery evidence

Preflight policy failures leave bytes and Replica records unchanged. After
reservation begins, ordinary errors raise `StorageDeletionError` with the
original exception chained and a `ReplicaDeletionRecovery` value:

| Field | Meaning |
| --- | --- |
| `location` | Concrete object addressed by the operation. |
| `reservations` | Replica IDs and revision tokens written or attempted. A failed reservation transaction may have rolled them back. |
| `phase` | `reservation`, `bytes`, or final `metadata`. |
| `bytes_deleted` | `False` before any byte call; `None` if the backend call failed with an unknown outcome; `True` after an acknowledged deletion of a previously observed object. |

An acknowledgement is the Store's reported result, not proof that this caller
alone caused physical absence. A lost acknowledgement may mean the bytes are
already gone. A metadata rollback cannot restore erased bytes, and a failed SQL
commit can itself have an unknown outcome. No automatic retry or compensating
rewrite claims otherwise.

Pending claims appear as `replica_deletion_pending` in operational status with
a manual inspection action. Recovery is deliberately operator-controlled:
stop or establish the termination of the original operation, inspect the
object and surviving copies, then compare the recorded IDs/revisions against
the recovery receipt. Only after that quiescence and inspection should privileged
repository repair finalize an absent object or restore freshly verified evidence
for retained bytes. Do not clear a live reservation merely because bytes remain
present during a slow deletion. Automated recovery/override APIs remain absent.

Core evacuation can have published replacement copies before a later removal
fails. Its action receipt retains the deletion recovery value; CLI JSON and exit
status reflect the failed attempt. This is not a transaction over an entire
evacuation.

## Evidence

[Policy and fault tests](../../tests/storage/test_deletion_policy.py) cover both
entry points, protected modes, last-copy and separation refusals, stale evidence,
metadata-only removal, backend/metadata failures and competing threads.
[Durable tests](../../tests/storage/test_deletion_policy_durable.py) use independent
SQLite and optional live PostgreSQL owners, a stale bound cache, reconstruction,
reservation rollback, outer-transaction refusal, and real Core/CLI handling.
These are controlled operation failures and reconstructed objects; they do not
claim an OS crash or native APSW connection test.

The final focused selection passed **110 tests**, including live PostgreSQL and
Core/CLI failure handling; the complete typing, formatting and architecture gates
passed. The earlier broader caller selection exposed expectations of privileged
raw deletion through the manager; those callers now use the appropriate explicit
Store operation or assert policy refusal.

The competing-cache test uses the real schema-backed cache over Asset/Replica
tables. Full PostgreSQL schema discovery independently fails on a generated link
column exceeding PostgreSQL's identifier limit. This was reproduced on the
unchanged A04 parent (`e9efedc4`); these deletion checks do not certify full-schema
PostgreSQL cache loading. Repair that schema/discovery contract with compatibility
tests before claiming that wider backend combination.
