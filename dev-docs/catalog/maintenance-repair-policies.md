# Catalog repair policies

Started: **2026-11-02**. Verified: **2026-11-03**. These policies implement the
fourth part of the [maintenance completion work](../databases/maintenance-completion-plan-2026-11-02.md).
Catalog owns repair decisions and inherited metadata; database helpers own physical
inspection, locks and audit storage. Targeted SQLite/APSW/PostgreSQL checks cover
the backend and failure boundaries below. The stated scope limits still apply.

## Entry points and initial supported scope

`catalog.retrieval.inherited.tags(level=..., entity_id=..., max_rows=10000)` reads
all direct/ancestor Tag contributions in a read-only snapshot. Core exposes
`catalog.inherited.tags`; the CLI is `liuxin catalog inherited-tags item 42`.
Works, Expressions and Items have direct Tag links; Manifestations inherit along
their Expression links. Tags are ordered by casefolded text then ID; provenance
keeps each traversed physical link in ID order. The budget counts both fetched
rows and graph visits. Exceeding it rejects the entire result. There is no cache.

`catalog.mutations.repairs.merge(...)` and `.cleanup(...)` preview by default.
Core names are `catalog.repair.merge` and `catalog.repair.cleanup`. The existing
`writer.merge_entities` and `catalog.metadata.merge` now delegate to the same
policy and also default to preview. Apply requires `dry_run=False` and the
preview's `state` as `expected_state`. The fingerprint covers inspected rows and
reference declarations, including references which arrived after a prior preview.

The initial repositories are Works, Expressions, Manifestations, Items, Tags,
Genres, Subjects, Series, Labels and Notes. Other repositories need a separately
reviewed ownership policy. Supported merges preserve discovered interlinks and
intralinks, curated polymorphic identifiers, and Manifestation Item ownership.
Other direct owned records block merge. Hierarchical merges require the same
parent and no child-reference transfer; changing tree scope/cycles is unsupported.
Coalescing a relationship identity with its own incoming FK is also unsupported.
These cases are visible refusals; there is no cascade-based fallback.

An explicit apply creates `catalog_repair_receipts` if needed, within the same
transaction. Receipt version 1 stores inspected before-state, decisions and applied
operations as JSON. `catalog.repair.receipt`/`liuxin catalog repair receipt ID`
reads a receipt without installing anything. Queue pruning never removes receipts.
Receipt retention is deliberately separate from maintenance backlog retention.

For example, save this merge request to a JSON file:

```json
{"repository":"works","source_id":12,"target_id":18,
 "resolutions":{"work_title":{"take":"target"}}}
```

```bash
liuxin catalog repair merge request.json --database catalog.sqlite
liuxin catalog repair merge request.json --database catalog.sqlite \
  --expected-state HASH_FROM_PREVIEW --yes
```

Scalar decisions use exact physical column names and either `{"take":"source"}`,
`{"take":"target"}` or `{"value":...}`. Relationship decisions use `table:row_id`
keys under `relationship_resolutions`, with `{"values":{...}}` for explicit
priority/primary/payload changes and `{"take":"source"}` or `{"take":"target"}`
for duplicate-payload conflicts. Endpoint, identity and timestamp edits are refused.
Unresolved conflicts block the whole repair. Re-preview after changing decisions.

Cleanup requests use `repository` and `candidates`, for example:

```json
{"repository":"tags","candidates":[
 {"entity_id":23,"decision":"discard","reason":"Mistaken import"},
 {"entity_id":24,"decision":"preserve","reason":"Planned vocabulary"}]}
```

Use `catalog repair cleanup` with the same preview/apply flags. Candidates with
references, outgoing identity fields or no recognized display metadata are
ineligible. A blocked discard prevents all deletion; preserve candidates remain
untouched. The operation accepts at most 500 explicitly selected candidates.

## Existing owners and the problem to fix

The former `MetadataWriter.merge_entities` filled empty fields, transferred only
selected relationships and silently preferred target conflicts before deletion.
It now delegates to the shared repair owner, with its old private partial-transfer
helpers removed. Existing fixtures select their previous target preferences
explicitly. `Maintainer.merge` remains physical
interlink machinery and cannot decide semantic equivalence.

## Semantic merge

The operator selects both identities and asserts that they describe the same
entity. Matching display text is candidate evidence only. The retained identity
is explicit; a smaller ID has no semantic preference. Work, Expression,
Manifestation and Item identities must remain at their respective WEMI levels.

A complete preview must include source/target scalar values, every affected
relationship family, direct and polymorphic references, owned records and
conflicts. A scan budget is allowed, but exceeding it must refuse the plan rather
than omit relationships. The receipt identifies the inspected state so apply can
reject a stale preview after rechecking under the writer/row locks.

Scalar policy:

- Fill an empty target from a populated source; retain equal populated values.
- Differing populated values require a field-level source/target/explicit-value
  resolution. Do not silently discard source metadata through target precedence.
- Recompute derived normalized keys through their existing write owner. Do not
  copy identity columns, generated keys or operational timestamps as metadata.
- Preserve the pre-merge source and target values and explicit decisions in an
  audit receipt. An absent provenance model is not permission to invent one.

Relationship policy:

- Compare complete typed identities, including roles, direction, scope and
  priority groups. `inspired_by` and `fanfiction_of` remain different relations.
- Coalesce only proven identical relationships with equivalent semantic payloads.
  Different payloads require an explicit decision. Operational timestamps alone
  do not make two otherwise equivalent links semantically different.
- Scope is part of identity. Same-named Genres under different parents must not
  merge implicitly. Tree cycles, remapping into invalid self-links and uniqueness
  or priority collisions are preview conflicts, not exceptions to swallow.
- Repoint all supported direct/polymorphic references and preserve owned records.
  Unsupported reference kinds block apply before any write. A Manifestation merge
  may move Item ownership; it does not move, overwrite or delete book files.
- Apply scalar decisions, relationship changes, source deletion, audit receipt and
  prompt invalidation in one transaction. Any late failure rolls everything back.

The existing `merge_entities` entrypoint delegates to this policy. Callers relying
on target precedence must make that decision explicit; the old partial path is gone.

## Inherited metadata

Inheritance is a derived read model, not a write of copied tags into descendants.
Start with structured tag contributions along the actual WEMI graph:

- Work contributions belong to that Work.
- An Expression may expose the contributions of each linked Work as well as its
  own direct tags.
- A Manifestation may expose each embodied Expression/Work contribution, retaining
  all paths rather than selecting one primary Work.
- An Item may expose its Manifestation's contributions alongside direct metadata.
- Never propagate a descendant's tags upward automatically: a translation-specific
  tag does not necessarily describe every Expression of the Work.

An introduction and a novel in one Manifestation therefore remain two attributed
contributions. A presentation may show a distinct tag union, but the underlying
result retains source level, source ID and relationship path. Stable ordering and
deduplication must be specified; removing the final source link returns an empty
contribution rather than retaining stale text.

Agent/series/genre ancestry needs an explicit policy beyond WEMI containment.
Tagging a person must not automatically tag everything they wrote. Any supported
opt-in expansion must identify selected credit roles and ancestor relationships,
retain provenance, and detect cycles. Identifier schemes keep their existing
normalization owner. Localized separator strings belong to presentation.

The current implementation computes this view on demand. A future cache must use the
durable lazy runtime, revision/freshness receipts and a rebuild policy; reading a
view must never secretly schedule or perform repair.

## Selected unused-record cleanup

An unlinked Work, Agent or vocabulary entry may be intentional. A generic
"unlinked means garbage" rule is rejected. Cleanup requires explicit selected
identities, a discard decision and a recorded reason. Reserved placeholder rows
and deliberately preserved identities are ineligible.

The preview must inspect direct FKs, polymorphic references, interlinks,
intralinks, tree descendants and owned subtype/resource records. Apply must
recheck those references under the write lock and reject a stale plan or a new
reference. It may delete only the approved rows and explicitly included owned
records; unrelated vocabulary, linked resources and physical files are retained.
Unknown reference/ownership rules block deletion rather than being skipped.

Execution is bounded and atomic, including its durable receipt and prompt
invalidation. The receipt records which rows were removed, which were retained
and why. This is an operator-selected disposal operation, not an automatic task
run when opening a Library or after every import.

## Required evidence

Tests must cover scoped Genre names, typed Work relationships, multi-Work
Manifestations, retained custom scalar values, complete reference handling,
stale previews, concurrent new references, source-deletion failure and prompt
failure. Assert both final state and rollback of intermediate transfers. Test
the existing public merge caller after migration, not just a new isolated helper.

Related: [Catalog/database ownership](catalog-database-ownership.md),
[legacy capability inventory](../databases/legacy-maintenance-retirement-2026-11-02.md).
