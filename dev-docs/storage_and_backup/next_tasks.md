# Next storage tasks

Date: **2026-10-20**. Proposed work; this list does not implement the changes.

Keep the existing manager/Store/driver structure. Close mutation and resource ownership gaps before adding more orchestration layers.

## STOR01. Preserve caller-owned staging

**First; architecture A01.** Track a backup run's scratch ownership, preferably by allocating a unique child under a caller-supplied parent. Carry that identity through resume and cleanup.

Done when sentinel files in the parent survive success, verification failure, cancellation and restart; cleanup removes only entries owned by the run.

## STOR02. Preserve read-only intent through construction

**First; architecture A02.** Pass and enforce explicit read-only configuration through every backend builder and durable reload. A backend descriptor default is not an override for caller intent.

Done when write, replace, delete and publication fail before mutation for configured read-only Stores, including writable backend families and reload/restart paths.

## STOR03. Enforce retention at byte deletion

**First; architecture A05.** Give deletion, Replica removal and capacity cleanup one policy decision at the mutation boundary. Distinguish a raw physical primitive from an authorized domain operation and retain auditable override intent where supported.

Done when last-copy, retention-locked and exact-recreation cases agree across all public routes, stale versions are rejected, and failures after byte deletion have recovery evidence.

## STOR04. Converge storage-record mutations and receipts

**Next; architecture A10.** Move reconcile/admin compatibility mutations behind one storage-owned service. Keep schema mechanics below it and preserve source observations, revision preconditions and cache invalidation.

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
