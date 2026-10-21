# Next storage tasks

Date: **2026-10-20**. STOR01–STOR03 are implemented for PR review. Other entries remain proposed work.

Keep the existing manager/Store/driver structure. Close mutation and resource ownership gaps before adding more orchestration layers.

## STOR01. Preserve caller-owned staging

**Implemented for PR review, 2026-10-20; architecture A01.** New backup runs allocate
a private workspace beneath the supplied staging parent. Persisted ownership is
checked on recovery and before cleanup. Legacy staging remains borrowed and retained.
Requested cleanup also releases a routed output's local intermediate image.

The [staging contract](storage_api.md#backup-staging-ownership) describes retention
and recovery. [Regression tests](../../tests/storage/backup/test_backup_staging_ownership.py)
cover sentinel preservation, separate runs, failure/cancellation, persisted retries,
ownership tampering and cleanup errors.

## STOR02. Preserve read-only intent through construction

**Implemented for PR review, 2026-10-20; architecture A02.** Managed, flat,
Calibre-like and SQLite builders now preserve explicit read-only configuration
through their Stores and drivers. Catalogue reopen and Store reload retain it.
SQLite opens existing storage with `mode=ro`, validates rather than initializes
the schema, and closes each operation's connection.

The [construction contract](storage_api.md#read-only-store-construction) records
the scope and WAL limitation. [Regression tests](../../tests/storage/test_read_only_backend_construction.py)
cover actual write/replace/delete/copy/move refusals, unchanged stored bytes and
metadata, missing-target preservation and writable controls. Intrinsic read-only
restrictions remain intact. The four audited forwarding defects are closed;
provider-specific initialization is not a universal no-side-effect guarantee.

## STOR03. Enforce retention at byte deletion

**Implemented for PR review, 2026-10-20; architecture A05.**
Both manager entry points use the [deletion policy and recovery contract](deletion_policy.md).
Protected modes and last-copy/minimum/separation refusals precede committed
DELETING reservations. Database owners serialize their decisions and bypass
stale per-facade caches. Core/CLI retain partial-outcome recovery evidence.

Raw Store/driver access remains privileged. There is no force override, automatic
pending-deletion recovery, or permission to erase a last copy merely because its
policy permits recreation/loss. Capacity-cleanup callers must use this owner when
introduced; leases and storage writer consolidation remain STOR04/STOR05 work.

## STOR04. Converge storage-record mutations and receipts

**First family implemented for PR review; architecture A10.** Generic Asset/Replica
admin writes are refused; policy definitions, Asset policy assignment, Composite
CRUD and FRBR backup indexing use the storage owner. Composite member rows are
inspectable only; full membership replacements retain the owner revision.
The [mutation contract](record-mutations.md) records caller
migration, revisions, historical fingerprints and partial effects.

Continue with Item-link and workflow-definition/source adapters and retained `files`
reconciliation. Keep schema mechanics below the owner and preserve source
observations, revision preconditions and cache invalidation.
Also repair SQLite's Composite membership uniqueness: valid repeated same-role
Assets currently conflict despite distinct positions. Preserve existing libraries
and verify parity with the domain model and PostgreSQL. The Core owner adapter
currently retains the transaction's complete rollback on this constraint failure.

Done when the selected live callers share validation and receipts, failure injection covers publication versus commit, and out-of-band inventory is not treated as metadata authority.

## STOR05. Pin materialized inputs during active workflows

**Next; coordinate ingest/caches.** Define Store or Replica leases before automatic cache eviction can run alongside mixed-format ingest. Reserve capacity and make expiry, process death and resume explicit.

Done when a running parser cannot lose its backing bytes, abandoned leases can recover, and completed workflows release pins without deleting retained originals.

## STOR06. Prototype recovery packs with a restore test

**Following.** Take the [recovery proposal](<Archive Recoverability, Redundancy, and Self-Describing Packs.md>) as a design input. Produce one versioned manifest/metadata bundle and a small catalogue-independent restore path before promising broad recovery grades.

Done when a fresh process reconstructs a small multi-work/multipart example from the pack, verifies hashes and reports missing tools or metadata explicitly.

## Validation and routing

Use focused registry, backup cleanup, Replica/policy, durable restart and mixed-ingest tests under [tests/storage](../../tests/storage). Keep live remote contracts explicit and read-only; historical benchmark timings are not acceptance thresholds.

Return to [storage](README.md).
