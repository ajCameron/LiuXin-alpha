# Core

Updated **2026-11-23** with the whole-Core review and completion plan.

Core owns application composition, configuration, lifecycle and the command/query
boundary. LibraryServices supplies collaborators; domain rules stay with their
Catalog, metadata and storage owners.

- [Whole-Core review and completion plan](review-and-completion-plan-2026-11-23.md):
  strengths, functional coverage, evidenced weaknesses and ordered sign-off work.
- [Core API](core-api.md): named operations, clients, read sources and failure contracts.
- [Native metadata API](native-metadata.md): creation, shared records, relationships,
  vocabulary trees, bounded batches, search discovery and reconciliation receipts.
- [Transport permissions](transport_permissions.md): server roles, authentication,
  endpoint exposure and restricted read-only clients.
- [Workflow ownership](core-program-workflows.md): operation providers and service owners.
- [Configuration ownership](configuration-ownership.md): profile selection, fixed
  paths, preference precedence and worker propagation.
- [Lifecycle review](../architectural_reviews/lifecycle-opening-path-review-2026-10-09.md):
  the completed registration caller migration and remaining lifecycle work.
- [Metadata and lifecycle integration review](integration-review-2026-11-23.md):
  metadata route ownership, startup rollback, complete cleanup and public workflow verification.

For presentation and startup procedures, use the [surface guides](../surfaces/README.md).
For lifecycle regression tests, start in `tests/core/test_library_services_lifecycle.py`;
choose other checks through [test streams](../hacking/test-streams.md).

[Next tasks](next_tasks.md) records implemented contracts, remaining changes and
completion checks.

Return to the [developer index](../README.md).
