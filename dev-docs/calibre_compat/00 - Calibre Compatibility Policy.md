# Calibre canonical-import policy

Status: **Decision draft; canonical import is not implemented.**
Source reviewed **2026-10-20** against the working tree based on `fc5de7af`.
The proposal remains unimplemented; [next tasks](next_tasks.md) separates reader
hardening from a future canonical-import design.

This document retains the proposed default interpretation of an imported Calibre
record: preserve its source identity in a separate compatibility record, initially
anchored to a Manifestation. It specifies work still to do. Use the
[current overview](<../05 - Calibre compat.md>) for callable adapters and the
[mapping guide](<mapping calibre to LiuXin.md>) for implemented SQL/web identities.

## What is implemented and what is proposed

| Concern | Current implementation | Proposed canonical-import work |
| --- | --- | --- |
| Source inspection | `CalibreDB`, `CalibreReader`, OPF recovery, scan reports and normalized payloads. | Consume those observations without silently discarding source data. |
| Import actions | `CalibreImportPolicy` classifies `full`, `metadata_only` or `skip`; `iter_import_jobs()` yields descriptions. | Execute a workflow with explicit identity, persistence and storage policy. |
| Persistent source identity | Payloads/jobs carry source IDs and library context. | Durable source-library identity, source-record identity, anchor and policy provenance. |
| Canonical projection | Generated SQLite `books_v` uses WEMI ray IDs. The web adapter resolves Work IDs. | A separate mapping from imported source records to canonical entities. |
| Compatibility records | No `compatibility_sources` or `calibre_book_records` tables, and no `CalibreBookCompatibilityRecord` class. | Design and migrate the durable record model. Names in this draft are illustrative. |
| Catalog adapter | No `catalog.compat.calibre` or `catalog.compatibility.calibre` package. | Place bibliographic persistence policy under Catalog and compose the workflow through the application owners. |

The existing `CalibreImportPolicy` is a classification policy. It does not
implement the proposed `manifestation_anchor` profile. A job labelled `full` is
not evidence that any LiuXin rows or stored files have been created.

## Proposed default: a Manifestation anchor

A source row can combine title, contributors, language, publication information,
identifiers, local user state and several files. Preserve that source grouping,
then make the canonical interpretation explicit:

```text
(source library identity, source book ID)
    → compatibility record with import-policy provenance
    → provisional Manifestation anchor
    → explicit WEMI and storage relationships
```

The compatibility record would remain separate from the canonical Manifestation.
An anchor identifies the initial point of attachment; it does not claim that a
source row and a Manifestation are the same entity or have the same lifetime.

The rationale for this default is that edition/product evidence such as publisher,
publication date or ISBN can make a Manifestation a useful starting point. A
source library may instead group by abstract work, text version or local files.
The importer must retain that uncertainty instead of inferring identity from a
source row number, filename or format extension.

WEMI is a graph. A Manifestation can embody Expressions of several Works, as in
[a novel with its introduction](../databases/wemi-link-examples.md#6-a-novel-and-its-introduction-share-one-manifestation).
The proposed anchor must preserve those distinct paths rather than collapsing
them into one Work or treating every path as another owned copy.

## Attached formats need their own evidence

Several files grouped under one source row might be alternative encodings,
a conversion and its original, a scan and OCR text, different editions,
translations, abridgements or supplementary material. The proposed importer
must not infer that they are bibliographically equivalent.

The earlier draft suggested an initial Item per attached format, grouped beneath
the provisional Manifestation. That remains a proposed import convention, not a
schema requirement or implemented behaviour. Decide explicitly when files share
an Item, when a composite is needed, and when the evidence requires different
Manifestations, Expressions or Works.

Use the actual storage model: atomic bytes are represented by `digital_assets`,
physical placements by `asset_replicas`, and multipart logical assets by separate
`composite_digital_assets` records and membership links. An extra physical replica
does not by itself imply another bibliographic Item. The
[schema overview](<../03 - Database Schema.md>) owns those current structures.

## Preserve source identity and ambiguity

A durable compatibility record should preserve at least:

| Information | Purpose |
| --- | --- |
| Stable source-library identity and source book ID | Distinguish row 42 in one library from row 42 in another; support repeat imports. |
| Import policy/profile and its revision | Explain how source facts were interpreted. |
| Anchor type and canonical target | Record the chosen attachment without reusing source IDs as canonical primary keys. |
| Raw values and relevant source locations | Retain ambiguous tags, comments, ratings, custom columns and paths for later classification. |
| Import outcome, warnings and provenance | Explain omissions, metadata-only decisions and later repairs. |

The earlier candidate DDL was a sketch, not an installed migration. A final schema
still needs enforceable references, repeat-import semantics, provenance/versioning,
and a way to represent splits and reassignment. A polymorphic `anchor_type` plus
`anchor_id` alone would not provide a normal foreign key to each possible target.
Choose the persistence design with those constraints in view.

A one-anchor record also needs an explicit policy for a source row that splits
across several canonical editions. Do not silently overwrite the source mapping
or imply that the first Manifestation remains a complete description.

## Write ownership

The proposed workflow must use existing ownership boundaries:

| Owner | Responsibility |
| --- | --- |
| Source compatibility readers | Observe source schema, values, file references and drift. |
| Metadata | Represent values and domain meaning without owning persistence. |
| Catalog | Resolve bibliographic identity and coordinate canonical record/link writes. |
| Storage | Acquire or register bytes and manage placements, replicas and storage policy. |
| Library/application workflows, composed by Core | Coordinate the operation, progress, failures and recovery across Catalog and storage. |
| Databases | Provide schema, transactions, constraints and generic persistence mechanics. |

A future Calibre-specific Catalog adapter should delegate to the
[canonical writers](../catalog/catalog-write-validation-contract.md). It must not
establish another independent normalization or relationship policy. Package names
such as `catalog.compat.calibre` remain placement proposals, not import paths.

The importer must decide the semantic target of a field before writing it. Title
and creator evidence may belong to a Work or Expression; publisher/date/ISBN
evidence may identify a Manifestation; ratings, comments and custom values may
need source-scoped preservation. These are decisions requiring evidence and a
profile, not universal field-to-column mappings.

Reads should return observations or projections without silently creating records.
File attachment and path changes require coordinated Catalog/storage operations.
Writing to a flattened SQL or web response shape cannot replace that workflow.

## Implementation work still required

1. Define durable source identity, compatibility records and schema migrations,
   including anchor integrity, splitting and repeat-import behaviour.
2. Specify the initial profile's field and relationship rules, with explicit
   treatment of ambiguous metadata and formats that embody different content.
3. Add a Catalog adapter using existing writers and matching policy, plus a
   coordinated application workflow for asset/replica acquisition.
4. Define transaction boundaries, partial-failure outcomes, progress and restart
   behaviour. SQL rollback cannot by itself undo external byte operations.
5. Add end-to-end cases for duplicate source IDs across libraries, repeat imports,
   missing/unsafe files, multiple Works per volume, mixed editions and source
   custom data. Verify the preserved source record as well as the resulting graph.

Reader, schema-view and classification tests cover existing primitives. They do
not certify this unimplemented pipeline or lossless export back to Calibre.

## Future profiles and scope

Retain room for future `item_anchor`, `work_anchor`, `expression_anchor` or
`forensic_preserve` profiles if concrete libraries require them. These are design
names, not current configuration options. The first implementation should make
one explicit profile reliable before adding others.

Perfect automatic WEMI inference, semantic round trips of every custom column,
complete plugin compatibility and a replacement Calibre server are outside this
initial importer proposal. Preserve uncertain evidence so later review can improve
the model without losing the source record.

Return to the [compatibility index](README.md).
