# Next cache tasks

Date: **2026-10-20**. Proposed work; this list does not implement the changes.

Preserve the ReadCache/Cache split and shared Catalog write ownership. Optimize measured workloads without inventing another catalogue authority.

## CACHE01. Make invalidation responsibility explicit for every write route

**Next.** Audit direct Catalog calls, cache writer bridges and storage repository writes. Document which owner publishes affected IDs/links and how a failed reconciliation leaves the cache visibly dirty after a successful database commit.

Done when each selected route has tests for commit success plus refresh failure, external writes and rollback; stale snapshots cannot present themselves as fresh.

## CACHE02. Define leases for physical cache replicas

**Next; STOR05 and ING02.** Work with storage to pin materialized inputs used by active workflows. Keep this byte-retention contract separate from catalogue snapshot invalidation.

Done when capacity cleanup respects active leases, crash recovery has a bounded expiry policy, and catalogue refresh never deletes physical inputs as an incidental effect.

## CACHE03. Make registry lifetime and collisions deliberate

**Following.** Specify registration-before-use, alias collision and concurrent lookup behaviour for the cache backend registry. Share only mechanics justified across registries.

Done when duplicate registration has a tested policy and a failed load cannot silently leave callers with an unrelated backend under an old alias.

## CACHE04. Reduce legacy overlap using workload evidence

**Following.** Choose one remaining Calibre cache/search adapter and map live consumers, missing capabilities and performance. Keep known-miss versus unsupported behaviour intact.

Done when the selected caller uses one read contract, old imports can be removed, and small correctness checks plus a measured benchmark support the change.

## Validation and routing

Use [cache tests](../../tests/databases/caches), Catalog writer/retrieval tests and surface read-failure tests. Run large cache benchmarks only for a specific performance question with resource telemetry.

Return to [cache](README.md).
