# Storage production-hardening checkpoint - 2026-08-21

The storage follow-up checklist recorded in
`dev-docs/storage/storage_component_status.md` has been worked through.

Implemented and covered:

- database-generated concurrent-safe metadata IDs and UUID revisions;
- versioned schema and scratch-envelope migration, including future-version
  rejection;
- transactional compound metadata mutations and durable Store reconfiguration;
- correct SquashFS semantics: archived identical bytes are Replicas, not a
  derivation;
- aggregate operational health and suggested recovery actions;
- live PostgreSQL concurrent ingest/interruption/restart coverage;
- protected, read-only HTTP/FTP/rclone/S3 CI contracts;
- concrete database Asset/Replica/Composite/derivation repositories and an
  explicit-commit Unit of Work; and
- ID-scoped shared-cache invalidation plus a reproducible large-catalogue
  benchmark.

The cache result closes the question raised by removing
`InMemoryStorageManager`: the durable manager does not need another catalogue
copy. It either shares Core's cache or reads its database repositories
directly. Store facade/lock registries remain process-local because they are
live resources rather than persisted domain records.

The 50,000-book/250,000-link benchmark measured a 1.164 ms median one-row
refresh, one database row read, and no table scan. Construction reached about
745 MiB RSS, so cache enablement remains an explicit application-level
performance/memory decision.

## Verification

- `tests/storage`: 778 passed, five skipped (four credential-gated remote
  contracts and the opt-in PostgreSQL case).
- `tests/ingest tests/core`: 107 passed.
- `tests/databases/caches`: 142 passed, twelve documented legacy/live-plugin
  skips.
- PostgreSQL adapter/schema unit suite: 30 passed.
- Live PostgreSQL concurrent ingest, interrupted publication, restart recovery,
  and idempotency: one passed.
- Storage/cache source compilation and `git diff --check`: clean. The latter
  reports pre-existing repository line-ending conversion warnings only.

The non-overlapping verification selections total 1,058 passing tests. The
protected credential-backed HTTP/FTP/rclone/S3 workflow is configured but was
not invoked locally because its read-only environment secrets are intentionally
available only in protected CI.
