# Next job tasks

Date: **2026-10-20**. Proposed work; this list does not implement the changes.

The package has durable machinery, but default Core execution still uses the utility manager. Architecture A09 is convergence work, not a package rename.

## JOB01. Release long waits from Core dispatch

**First.** Separate handler registration/state access from the lock held for database-bound mutations. Establish a safe waiting path without sharing an unsafe database connection.

Done when one client can wait while another reads health/progress and requests cancellation; shutdown and concurrent database mutations retain a documented serialization contract.

## JOB02. Move one production workflow onto durable job state

**Next; architecture A09.** Select a real storage or conversion workflow. Connect definition, run, lease, heartbeat, attempts and receipt to Core operations, preserving existing client IDs through an explicit mapping where needed.

Done when restart after submit, lease and side effect gives a deterministic recoverable state; duplicate submissions and retries do not silently repeat publication.

## JOB03. Unify observable cancellation and results

**With JOB02.** Define requested versus acknowledged cancellation, worker failure, timeout and retry ancestry. Preserve domain warnings/loss reports and log references in the serialized result instead of equating a return value with a complete operation.

Done when direct and remote callers receive the same versioned outcome and know whether work may still be running after their request times out.

## JOB04. Retire duplicate registries by live caller migration

**Following.** Map durable handlers, utility executors, Core wrappers and domain journals. Keep execution backends and domain recovery evidence where they have distinct roles.

Done when one migrated workflow has one authoritative run state, obsolete callers are gone, and compatibility removal does not discard useful domain checkpoints.

## Validation and routing

Use repository/worker tests plus Core concurrent-client tests. Record manager ownership explicitly and keep all test databases, logs and worker outputs disposable.

Return to [job](README.md).
