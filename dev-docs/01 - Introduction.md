# What is LiuXin-alpha?

Reviewed: **2026-10-20**. Status: **Project motivation and current orientation**.

LiuXin-alpha is an archival library project: a rich catalogue connected to storage,
conversion, acquisition and recovery workflows. It grew from LiuXin and shares
substantial inherited code with Calibre. The original shorthand, “archival-grade
Calibre”, describes the ambition; it is not a certification of preservation,
scale or compatibility.

The project aims to handle large collections, multiple storage media, arbitrary
digital assets and audiobooks while keeping bibliographic meaning separate from
file placement. Tens of terabytes, tape-oriented workflows and broad plugin
compatibility are goals to evaluate through concrete deployments and tests, not
blanket claims about the current alpha.

## Catalogue meaning and physical reality

The catalogue uses Work, Expression, Manifestation and Item (WEMI) identities.
A novel and an introduction can be separate Works embodied in one edition; one
owned volume need not become two Items merely because it contains both. A
translation is also distinguishable from a format conversion or another copy.
See the [schema overview](<03 - Database Schema.md>) and
[worked link examples](databases/wemi-link-examples.md).

Storage separately describes atomic assets, composite assemblies, configured
Stores and Replica claims. Copying an EPUB to another Store changes its physical
availability without deciding that it is a new Work. A catalogue record, a
reported digest and a successful backup job each establish different facts;
none alone proves that every desired copy is intact and recoverable.

## What contributors can work with now

The [package guide](<02 - Top Level Structure.md>) maps the implementation:

- Core composes resources, configuration and application commands/queries.
- Catalog owns semantic bibliographic writes, identity and persistent retrieval;
  metadata supplies values and pure rules, and databases supplies persistence.
- Storage provides manager, Store and driver contracts, durable metadata,
  acquisition/reconciliation and backend-specific backup/recovery operations.
- Caches provide derived reads and Catalog-delegating writes.
- Surfaces expose CLI, terminal, GUI and web operations with differing coverage.
- File formats and metadata providers supply parsing/conversion; `plugins` owns
  shared loading and application composition.

The [ownership guide](<04 - Seperation of Concerns.md>) distinguishes implemented
consolidation from remaining jobs, ingest, registry and compatibility work.
Installed backend support, optional tools and individual surface capabilities
must be checked against their guides rather than inferred from the package list.

## Preservation principles and present limits

### Retain original information

The preservation goal is to retain originals and the evidence needed to interpret
them. Derived data can be disposable only under an explicit retention/recreation
policy; “derived” by itself is not permission to destroy the only usable copy.

The code does not provide a universal “original files can never be deleted”
guarantee. Destructive storage paths and policy enforcement still need work.
New operations must establish their safety at the actual mutation boundary and
report partial effects. See [storage](<06 - Storage.md>) for the current contracts
and gaps, including the separation between physical deletion and database state.

### Preserve rich meaning across narrower interfaces

A book-shaped interface may expose only part of the WEMI graph. Keep the source
facts and canonical relationships explicit when applying an adapter. The
[Calibre compatibility guide](<05 - Calibre compat.md>) identifies lossy SQL/web
projections, supported readers and the still-proposed canonical import workflow.
Compatibility is a tested boundary, not a promise that every Calibre plugin or
field round-trips unchanged.

### Make operation and failure understandable

Prefer clear names, explicit ownership and observable results. Optional features
can degrade through documented fallbacks; failed required reads must remain
failures, not become empty catalogues or evidence of deletion. A backup receipt
should distinguish produced artifacts, verified content and unresolved work.

### Keep scale measurable

Bound scratch space, memory and nested extraction work. Use representative small
fixtures for correctness and explicit larger campaigns for performance. Measure
the backing volume as well as the test directory when investigating storage
pressure. See [test databases](<07 - Test Databases.md>) for provisioning and
telemetry, and [project goals](project-level-goals.md) for the review principles.

Return to the [developer index](README.md) to choose a subsystem or contribution.
