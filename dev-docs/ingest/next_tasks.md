# Next ingest tasks

Date: **2026-10-20**. Proposed work; this list does not implement the changes.

Consolidate receipts and semantic boundaries before moving modules. Keep nested-container traversal with storage and provider parsing with its source owner.

## ING01. Define candidate-to-write receipts

**Next.** Specify how provider/legacy metadata becomes a candidate with provenance, matching decisions, selected fields and a Catalog write report. Choose one live source as the first caller rather than creating a second normalization layer.

Done when ambiguous/conflicting matches require an explicit outcome, rejected values leave no partial bibliographic change, and the receipt identifies stored versus merely discovered data.

## ING02. Pin source materialization during work

**Next; STOR05.** Coordinate cache leases, source version checks and capacity reservation for nested-container work and resumed prefix downloads.

Done when active readers retain their bytes, changed sources invalidate unsafe resume, and restart recovers or expires leases without deleting retained data.

## ING03. Unify operation identity and restart reporting

**Next; JOB02.** Map crawl/Store/mixed-ingest reports to the durable job identity while retaining domain checkpoints. State which setup, enumeration and object failures are resumable.

Done when a restart after publication but before metadata finalization avoids duplicate registration and reports prior successes, failures and pending work accurately.

## ING04. Retire broken legacy import paths through callers

**Following.** Inventory `ingest.adding` integration paths and database-path wrappers. Migrate supported grouping/import callers to explicit services; preserve useful pure discovery helpers only where they remain used.

Done when one selected path no longer imports absent metadata modules, its replacement has a tested public entry point, and no new compatibility owner is introduced.

## Validation and routing

Use existing Store/mixed-ingest tests and source fixtures, plus Catalog parity tests for bibliographic changes. Validate scratch cleanup with sentinels and failure injection rather than rerunning a large real collection.

Return to [ingest](README.md).
