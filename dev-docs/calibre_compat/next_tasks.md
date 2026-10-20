# Calibre compatibility next tasks

Reviewed **2026-10-20**, working tree based on `fc5de7af`.
Status: **reader hardening plus a proposed canonical-import design**.
[Current mappings](<mapping calibre to LiuXin.md>) and the
[import-policy draft](<00 - Calibre Compatibility Policy.md>) have different
implementation status. A `full` import-job description does not execute an import.

## CC-01 — Harden source observation and resource lifetime

**First, independently useful.** Exercise database and OPF-sidecar readers on
missing/corrupt records, unsafe paths, filesystem drift and early iterator exit.
Document reader/connection ownership and preserve reasons for skipped or
metadata-only observations. Use real small fixtures, including Unicode paths.

Done when interrupted scans close owned resources, borrowed resources survive,
source files remain unchanged and diagnostics distinguish unsafe/missing content
from an empty valid result. Do not silently repair the source Calibre library.

## CC-02 — Decide durable source identity before persistence

Specify source-library identity, source book identity, provenance/profile version,
repeat-import behaviour, and mappings that can split or be reassigned. Choose an
enforceable reference design; a polymorphic type/id pair alone is not a foreign key.
The proposed Manifestation anchor must remain provisional for ambiguous records.

Done when examples distinguish equal numeric IDs in different source libraries,
repeat observations of the same source, a novel plus introduction in one volume,
and several editions/files grouped under one source row. Record a design decision
before adding schema or presenting proposed classes as an existing API.

## CC-03 — Implement one explicit import profile

**Depends on CC-02 and application/storage transaction and receipt contracts.**
Add a Catalog adapter using existing matching, preparation and relationships;
compose byte acquisition through storage and the application workflow. Preserve
raw source evidence and uncertainty rather than inferring WEMI identity from
extensions or filenames.

Done when one supported profile imports a representative fixture, repeated import
is deterministic, and late byte/metadata failures leave a resumable or explicitly
failed receipt. Test preserved source values as well as canonical rows. This is a
new-source importer proposal, not an old LiuXin-schema backfill programme or a
promise of lossless export to Calibre.

## CC-04 — Preserve identity boundaries in projections

Keep source `(library, books.id)`, SQL ray IDs and web Work tokens distinct.
Coordinate default-path changes with Catalog/database selection policy rather
than inventing a compatibility-only rule. Document where flattened field edits
need a chosen canonical target and therefore cannot be inferred from a view.

Done when shared fixtures cover several Works in one Manifestation, extra Items,
missing Items and competing edition paths. SQL/web/read adapters preserve their
stated identities and the worked mapping examples match their output.

## Validation and close-out

Use the [Calibre reader regressions](../../tests/databases/database_calibre_emultation/)
and generated-schema/view cases, adding workflow tests only when CC-03 exists.
Update the policy draft's implementation matrix and the mapping guide per landed
slice. Keep unsupported plugin/server/round-trip features explicitly outside the
implemented compatibility contract.
