# Ideas and experiments

Updated: **2026-10-20**.

This is a collection of proposals to discuss and try. Some have existing building
blocks; inclusion here does not mean an entire feature is missing, implemented or
scheduled. The [project goals](project-level-goals.md) guide the choices, and the
[ownership guide](<04 - Seperation of Concerns.md>) remains the architectural
contract. Likely owners below describe responsibilities, not new package proposals.

Keep an idea here while its value or shape is uncertain. Once accepted, give it a
focused design or subsystem work-list entry, link that decision here, and record
what was actually implemented. Avoid maintaining two competing roadmaps.

## Promising first experiments

These are suggested starting points rather than an agreed implementation order.
Each should produce something useful before requiring a larger system.

| Idea | First useful result | Why start here |
| --- | --- | --- |
| I08: explain catalogue choices | A report showing all candidate paths and why a default was selected. | Makes the known SQL-view/Catalog selection disagreement inspectable. |
| I06: metadata review queue | Compare conflicting observations for one record before applying a choice. | Makes enrichment useful without silently replacing curated facts. |
| I15: resource budgets | Preflight one ingest/conversion with estimated scratch and free-space headroom. | Builds on the storage-pressure investigation and existing test telemetry. |
| I13: recovery rehearsal | Restore one representative archive into an empty catalogue and verify it. | Tests whether preserved bytes can become a usable library again. |
| I01: device preview | Show what would be backed up or copied to one mounted directory. | Develops the original device idea without first requiring hot-plug support. |

## Devices and reading

### I01. Device discovery, backup and curated reading libraries

When an enrolled device appears, LiuXin could discover relevant files, back up new
material, and maintain a selection of things the reader wants to read. Treat
backup from a device and delivery to a device as separate operations with their
own policies. A folder selected explicitly by the user is a sufficient first
device backend.

A device profile could record stable identity, supported formats, a space reserve,
reading-list selection and the last successfully managed inventory. Show a plan
with additions, replacements, skipped files and expected space use before applying
it. Files LiuXin did not place should retain their independent ownership; an
unplugged device or failed scan must not become evidence of deletion.

**First experiment:** compare a mounted folder with a small reading list and
produce a resumable copy/backup plan. Verify transferred bytes before marking an
operation complete. Later, add device-arrival events and explicit removal policy.

**Likely owners:** Core/Library coordinates the workflow, storage owns device byte
access, jobs owns execution, and surfaces present the plan. Decide whether a
device is a Store, a delivery target, or exposes both roles.

### I02. Reading lists that express intent

“Read this Work in French, preferably unabridged and available offline” is more
useful than pinning a file path. A list could express preferred language,
accessibility needs, acceptable formats and a device-space budget, then resolve
those preferences to suitable Expressions, Manifestations and available Items.
Missing or unsuitable copies should remain visible as unmet reading intentions.

**First experiment:** persist a small list of Work references and explicit
preferences, then show the chosen copy and alternatives. Keep manual overrides.
Personal lists, completion and ratings need an owner separate from bibliographic
series membership or shared catalogue facts.

**Likely owners:** a Library workflow uses Catalog retrieval and storage
availability; Core supplies the current reader/library context. A first version
can have one explicit local reader without inventing multi-user synchronization.

### I03. Annotations and reading progress with honest anchors

A note may concern a Work, a translation, or a passage in one acquired file.
Preserve that scope, the quoted text when appropriate, the asset identity and a
format-specific locator. A PDF page number alone cannot identify the equivalent
place in a reflowed EPUB or revised translation.

**First experiment:** attach a note and progress marker to one Item/asset and
reopen them reliably. A later cross-format lookup can propose an anchor match
with evidence and retain the original when uncertain. Unresolved annotations
remain accessible. Reader state should have an explicit export/privacy policy.

**Likely owners:** metadata defines anchor values; Catalog persists supported
relationships; reader-facing workflows interpret them. Decide the scope of
progress before equating “finished one translation” with “finished every version”.

### I04. Companion material and parallel reading

Make the introduction, notes, maps, translation and audiobook easy to find while
reading a novel. A volume can contain distinct Works, and useful companions can
live in other editions or assets. Present the relationship and availability
instead of requiring the reader to infer it from filenames.

**First experiment:** offer “other contents of this edition” and “other versions
of this Work”, using the [worked WEMI examples](databases/wemi-link-examples.md).
Synchronized text/audio or translation alignment is a later experiment: an
asserted association does not establish matching passages or timestamps.

**Likely owners:** Catalog supplies explicit relationships, storage supplies
access, and surfaces assemble the reading experience.

## Understanding and curating the catalogue

### I05. An edition's contents as a navigable graph

Show how a novel, introduction and commentary share an edition and an owned
volume. Extend this to anthologies, bound collections, multipart works and
supplementary material without duplicating the physical copy in the interface.
Every view should say whether it is counting Works, editions, Items or paths.

**First experiment:** a bounded read-only graph/table for one Manifestation,
including contributor roles and the owned Items. Then investigate explicit
content role, sequence and page/section extent within that edition. Current
WEMI link priority selects/orders alternatives at a different boundary; it must
not silently become a table-of-contents position. The same Expression can occupy
different positions in different Manifestations.

**Likely owners:** metadata/Catalog owns meaning and persistence; surfaces owns
presentation. Decide how arrangement and incomplete contents evidence are stored
before adding fields or another interpretation of existing link columns.

### I06. Provenance and a metadata review queue

Explain where a title, identifier, contributor credit or WEMI link came from:
embedded metadata, an external source, a source-library import, or a deliberate
human edit. Preserve conflicting observations and make the selected value
explainable. A confidence score should accompany evidence, not replace it.

**First experiment:** show two observations for one record, their source/time,
the current value and a proposed change. Applying a reviewed choice should use
the existing Catalog write boundary. Start with a limited field family rather
than making a universal assertion engine a prerequisite for enrichment.

**Likely owners:** source adapters acquire observations; metadata represents
them; Catalog owns their relationship to curated state. Decide which provenance
belongs on a field, link, source record or import run. Existing `source`/`origin`
columns are useful inputs, not proof of a complete history model.

### I07. Reviewable merges and splits

Support “these records describe the same Work” and “this imported record actually
contains two editions” as explicit operations. A preview should enumerate affected
Expressions, credits, identifiers, Items, annotations and compatibility mappings.
Byte deduplication and bibliographic identity must remain separate decisions.

**First experiment:** generate a read-only merge/split report for a small fixture,
using the [matching policy](catalog/catalog-matching-policy.md). Define collision
handling and stale-plan rejection before enabling execution. Retain the source
identity and a change receipt; promise reversal only where the retained evidence
and subsequent edits make it well-defined.

**Likely owners:** Catalog owns graph transformations; a Library workflow handles
any storage consequences. A database transaction cannot reverse external byte
deletion. Decide whether old IDs redirect, remain historical, or are retired.

### I08. Explain why a result or default was chosen

Let a developer or reader ask “why this edition?”, “why does this volume appear
twice?”, or “why did this not match?”. Return candidate paths, applied rules,
scope, primary markers, priorities, availability assumptions and any truncation.

**First experiment:** explain default selection for the existing
[SQL-view and Catalog-bundle discrepancy](databases/wemi-link-examples.md#default-sql-view-versus-catalog-bundles).
Use the evidence to choose a contract, then keep the explanation attached to the
same policy that selects the result. Do not implement a second selector solely
to explain the first one.

**Likely owners:** Catalog owns matching/path explanations; Core owns application
source selection. This complements the current [Catalog work list](catalog/catalog-roadmap.md).

### I09. Search that states its scope

Support questions such as “novels I can read offline in French”, “editions with
this introduction”, and “files I own that have not yet been identified”. Offer
explicit result kinds and distinguish bibliographic relevance from availability.
Show when a result is partial, stale or limited by a query budget.

**First experiment:** one structured query over Works, language and availability,
with stable paging and a documented counting rule. Benchmark the actual server
work; paginating an already materialized catalogue does not bound its cost.
Full-text/OCR search can later return passages with their source asset and
extraction version, keeping the index rebuildable.

**Likely owners:** Catalog defines persistent query semantics, Core composes
application projections, and surfaces renders them. Reuse a shared read model
across terminal, web and API consumers.

### I10. Progressive cataloguing and accountable imports

Allow useful acquisition before every bibliographic question is settled. Track
which bytes were acquired and verified, which source assertions were preserved,
and which identities still require review. “Acquired”, “catalogued” and “reviewed”
should communicate different facts, with queues for the remaining work.

**First experiment:** follow one source record from observation through a proposed
WEMI mapping to an import receipt, including a missing format and a conflicting
identifier. Repeat the import and explain what changed. Build on the
[Calibre import-policy draft](<calibre_compat/00 - Calibre Compatibility Policy.md>)
and [mixed-ingest workflow](storage_and_backup/mixed_ingest_operations.md).

**Likely owners:** ingest preserves source context, Catalog resolves bibliographic
identity, storage records acquired bytes, and the Library workflow coordinates
outcomes. Partial records should have explicit status rather than plausible but
invented titles, authors or editions.

## Preservation, access and recovery

### I11. Derivation history and visible conversion loss

For a converted, OCR-produced or otherwise derived asset, retain its source asset,
tool/plugin version, settings, output validation and loss report. The reader
should be able to distinguish the original scan from OCR text and see when an
EPUB omitted images or could not preserve structure.

**First experiment:** carry one existing conversion report through the job result
to an asset's derivation record and a readable completion receipt. Preserve the
original and label uncertain OCR. Re-running a recipe is useful even when the
tool cannot promise byte-for-byte reproducibility.

**Likely owners:** file formats produces the report, storage owns asset/derivation
evidence, and jobs/Core carries the result. Follow the
[conversion work list](conversion_pipeline/conversion_pipeline_todo.md); bibliographic
decisions about a new Expression remain Catalog policy.

### I12. A library that can explain itself after the main database is lost

Export a bounded collection with payload identities, WEMI relationships,
provenance, checksums and a versioned, documented manifest. Include enough schema
and reconstruction guidance to interpret it without the original live instance.
An entire-library export and a portable subset should have explicit completeness
rules, especially where an Item is reachable from several Works.

**First experiment:** export the novel/introduction fixture and reconstruct it in
an empty catalogue. Preserve source/export IDs and record their mapping to newly
allocated local IDs. Separate private annotations and credentials from generally
shareable bibliographic metadata; credentials should not enter the recovery pack.

**Likely owners:** a Library export workflow composes Catalog and storage. Extend
the existing [archive recoverability proposal](<storage_and_backup/Archive Recoverability, Redundancy, and Self-Describing Packs.md>)
instead of introducing another manifest format without a compatibility decision.

### I13. Recovery rehearsals and evidence of recoverability

Demonstrate recovery by restoring a selected pack into a clean environment,
validating payloads, rebuilding catalogue relationships and opening representative
content. Report what was verified, what was only inventoried and what could not
be tested. A successful backup write is only one part of that evidence.

**First experiment:** run an explicit recovery drill for a small multilingual,
multi-work collection containing a composite asset. Record the archive, manifest,
schema and tool versions used, elapsed time and required external dependencies.
Keep drill output separate from the live catalogue.

**Likely owners:** storage/workflows performs restore and verification; Core/jobs
coordinates the run. Use the recovery proposal's distinctions between payload,
metadata and interpretability. A sample-based drill should state its sample and
cannot certify every asset in a large collection.

### I14. Availability and health that reflect the evidence

Answer “can I read this now?” and “how well is it preserved?” separately. Show
online/offline/unknown state, last successful verification, known corruption,
retrieval cost and the policy shortfall. An unplugged drive is unavailable,
not evidence that its contents were deleted.

**First experiment:** an availability report for one Item with online, offline
and unverified replicas, followed by an explicit retrieval plan. Extend protection
reports to distinguish independent failure domains: two paths on the same disk
should not be presented as protection against losing that disk.

**Likely owners:** storage owns observations and placement policy; Core/Library
combines them with the requested bibliographic object. Decide how long evidence
remains useful and how device/host failure domains are declared.

## Execution, extensibility and development

### I15. Resource budgets, free-space headroom and cache leases

Plan for input staging, archive expansion, conversion products and retained output
before starting a large operation. Track high-water usage and preserve a reserve.
Estimates should identify uncertainty; recheck available space at meaningful
boundaries and stop resumably when the budget is no longer safe.

**First experiment:** expose a resource estimate and measured scratch high-water
mark for one ingest/conversion job. Correlate it with test/workflow telemetry,
including host-volume headroom where observable. Container or WSL free space
alone may not explain pressure on the backing Windows volume.

Extend the [existing ingest budgets](storage_and_backup/mixed_ingest_operations.md#safety-budgets)
and telemetry. Eviction needs explicit ownership and live pin/lease semantics
before reclaiming cache objects used by another job. Reclaim disposable scratch
and eligible cache entries without treating original assets as temporary files.

**Likely owners:** jobs/Core enforces execution budgets, storage owns cache and
publication lifetimes, and test tooling measures fixture/scratch use. Avoid a new
global cleanup daemon with a separate interpretation of ownership.

### I16. Common operation receipts and reliable resumption

Give ingest, conversion, backup, repair and device transfer a consistent way to
describe planned actions, completed actions, warnings, skipped work and remaining
work. Carry one operation/run identity through Core requests, workers, storage
journals, progress and logs.

**First experiment:** follow one interrupted ingest through restart and show which
steps were reconciled or retried. Define idempotency and cancellation at each
side-effect boundary; a resumption token alone does not guarantee exactly-once
execution. Any reusable plan should state its assumptions and be revalidated
against current records, policy and available space.

**Likely owners:** consolidate through the existing jobs and application workflow
owners described in the [ownership guide](<04 - Seperation of Concerns.md#ingest-jobs-and-execution-helpers>).
Keep subprocess execution mechanics below durable job policy.

### I17. Capability and configuration diagnostics

Show which providers are available, which were selected, why a provider could not
load, and which optional dependency or configuration is relevant. Include the
origin of resolved settings, with secret values redacted. A failure should be
distinguishable from an intentionally disabled or unsupported capability.

**First experiment:** add a structured availability report for one conversion
provider and one storage backend, using their existing registries. Expose the
same report to CLI and GUI consumers. Avoid performing a transfer or contacting
a remote service merely to list locally known capabilities.

**Likely owners:** [plugins](plugins/plugin-composition.md) owns shared loading
mechanics, domain registries interpret capabilities, and Core owns application
composition/configuration. This should reduce divergent registry behaviour while
preserving the differences between a format converter and a byte-store driver.

### I18. Small semantic fixtures and failure rehearsals

Keep a compact library of awkward but realistic cases: a novel and introduction,
a bilingual edition, an omnibus, duplicate identifiers, an offline Store,
a composite audiobook and an interrupted transfer. Use them to check public
behaviour, example documentation and the equivalence of application read paths.

**First experiment:** promote the most useful worked WEMI fixture into a shared
bounded fixture, then test one question through Catalog and the relevant Core
surface. Add deliberately injected failures at publication/commit boundaries.
Track completeness, counts and recovery outcomes rather than only execution time.

**Likely owners:** subsystem test owners and contributor tooling. Make larger
scale/resource campaigns explicit, with deterministic generation, telemetry and
scratch cleanup. Useful coverage should not require every ordinary test run to
rebuild a huge library.

### I19. Assisted cataloguing as reviewable proposals

Allow deterministic rules or an optional model to suggest contributor roles,
language, contents or candidate WEMI relationships. Each suggestion should carry
the evidence, source/tool version and proposed record/link changes so it can be
accepted, amended or rejected through the ordinary curation workflow.

**First experiment:** suggest an introduction Work from supplied contents metadata
in the novel fixture, with no writes until the proposal is applied. Preserve
uncertainty and conflicting evidence. An optional remote provider needs an
explicit choice about which content leaves the library; offline/manual operation
should remain useful.

**Likely owners:** metadata providers produce candidate values, Catalog matching
and validation constrain application, and surfaces expose review. Reuse I06's
proposal/evidence boundary so assistance does not acquire a privileged write path.

### I20. Portable collection exchange before live federation

Let two libraries exchange a selected collection and explain what is already
present, newly acquired, conflicting or incomplete. Preserve source-library
identity and provenance; integer row IDs from different catalogues are not a
global identity scheme. Shared bibliographic records and personal reading state
need separate exchange choices.

**First experiment:** compare two small exported manifests and produce an import
preview, reusing I12's export contract and I07's identity review. Keep the original
export IDs stable while recording local mappings. Deletions and concurrent edits
need explicit conflict rules before considering live synchronization.

**Likely owners:** Library workflows compose Catalog matching and storage transfer.
Read-only search across libraries or live federation can follow once library
selection, identity, permissions and failure behaviour have clear contracts.

## Turning an idea into work

Before promoting an entry, record the user problem, the smallest useful outcome,
existing implementation to reuse, owning subsystem, dependencies and open choices.
Specify observable success, resource/failure limits and how to test the result.
Keep speculative API/table names out of current contracts until a design is chosen.

Prefer a complete small workflow over a new abstraction without a caller. Update
the relevant subsystem work list when an experiment becomes an accepted change;
leave a short dated outcome here so a future review can distinguish an idea from
an implementation. Return to the [developer index](README.md).
