# Native metadata operations

Implemented **2026-11-24** as completion-plan slice 3. These named operations
adapt `CoreServices.metadata`, which binds MetadataAPI/CatalogCache to the selected
Catalog and optional cache. Catalog retains domain validation, identity rules,
transactions and deletion policy; the metadata service owns reconciliation.
Core validates transport controls and projects the existing results. No new
metadata authority or top-level package is introduced.

The implementation is split by operation family under
[`program_services/native_metadata`](../../src/LiuXin_alpha/core/program_services/native_metadata/).
The same names and wire values work through direct and HTTP clients. Queries
require `read`; commands require `write`. Batch execution cannot elevate those
grants or invoke arbitrary commands. Legacy `metadata.get` and its item-centred
OPF projection retain their separate contract.

## Discovery and bounds

`api.describe` declares every route's fields and authority. `metadata.schema`
lists WEMI levels, shared kinds, immutable/suggestible/hierarchical families,
batch-eligible operations and transport budgets. `metadata.search.schema` accepts
an optional `level` and returns semantic filter names, aliases, operators and
scalar sort fields. `metadata.search.validate` checks a search without executing
it; invalid semantic searches preserve their parameter path in `invalid_payload`.

Native controls use strict JSON types: boolean values are not integer IDs,
strings are not coerced into IDs, and unknown top-level fields are rejected.
Direct calls accept tuples as arrays but reject opaque objects, cyclic input and
nonfinite numbers. Data objects retain Catalog's public-alias validation; the
transport does not invent another metadata schema.

| Input | Bound |
| --- | --- |
| Page `limit` and identity arrays | 0–1,000 records; default page size 100 |
| Suggestions | Default page size 20; maximum 1,000 |
| `max_nodes` for complete tree operations | 1–1,000; default 1,000 |
| Batch operations and initial revision tokens | At most 100 each |
| Transport input structure | Maximum depth 48 and 100,000 visited values |

`metadata.search` now requires a finite integer page size; its previous `limit=null`
spelling is intentionally rejected. Use `offset` pages instead. Pagination does
not retain a snapshot between requests. Bounds limit returned pages and supplied
work; they do not assert that every domain read scans only that many source rows.
Context-owned record queries currently collect their domain result before slicing.
General execution, byte and mixed-load budgets remain completion-plan slice 6.

## WEMI reads and writes

`level` is `work`, `expression`, `manifestation` or `item`, defaulting to `work`.
All IDs must be positive integers. A confirmed missing row differs from an
unavailable or corrupt read, which raises instead of returning empty metadata.

| Route | Inputs and result |
| --- | --- |
| `metadata.search` | `query`, optional `filters`, `sort`, `level`, `limit`, `offset`; references, exact total and provenance |
| `metadata.read_many` | `entity_ids`, optional `level`, `missing` (`none` or `raise`); ordered detached entities with duplicate/missing slots retained |
| `metadata.read_for_edit` | `entity_id`, optional `level`; native `metadata`, writable scalar `fields`, and `revision` |
| `metadata.create` | `data`, optional `level`; receipt containing the new ID, without implicit matching |
| `metadata.update` | `entity_id`, `data`, optional `level`, `expected_revision`; partial update receipt |
| `metadata.delete` | `entity_id`, optional `level`, `expected_revision`; deletion under Catalog constraints |
| `metadata.wemi.create_stack` | `work`, `expression`, `manifestation`, optional `items`, existing `work_id`, `origin`; one atomic connected path |
| `metadata.attach` / `metadata.replace` | `entity_id`, `data`, optional `level`; append/replace supplied groups, retaining omitted groups |
| `metadata.wemi.link` | `parent_level`, `parent_id`, `child_level`, `child_id`, optional `primary`, `priority`, `origin`; adjacent relationship receipt |
| `metadata.wemi.unlink` | Same four endpoint selectors; removes the relationship while retaining its entities |

Creation payloads use the existing public aliases, such as Work `title`,
Expression `label`, Manifestation `edition_statement` and Item `inventory_code`.
Deleting an Item and deleting its metadata are not authorization to destroy
managed content files. Domain owners retain their respective policies.

## Shared records and suggestions

`kind` selects `agents`, `tags`, `labels`, `genres`, `subjects`, `series`,
`languages`, `ratings`, `identifiers`, `item_identifiers`, `notes`, `comments`,
`synopses` or `annotations`. Languages are immutable constants and expose reads
only. No name selects a Python attribute or physical table dynamically.

Language reads expose stored constants; they do not seed or repair the database.
Fresh PostgreSQL schemas currently create an empty language table, unlike SQLite's
seeded table. This is an open database-bootstrap gap, recorded in the
[completion review](review-and-completion-plan-2026-11-23.md#native-metadata-completion-follow-up).
The PostgreSQL API tests explicitly load a language fixture after verifying the
empty result; they do not establish default language-data parity between backends.

Default database bootstrap can retain reserved Agent/Series records with ID zero.
Shared listings and `metadata.shared.read_many` may read these records; the
mutation boundary requires positive IDs. `metadata.schema.reserved_record_ids`
lets clients keep those rows read-only. The Series sentinel represents absence,
so Catalog and cached hierarchy sources exclude it before tree pagination and
sibling ordering. Tree operations preserve the stored sentinel.

| Route | Inputs and result |
| --- | --- |
| `metadata.shared.read_many` | `kind`, `entity_ids`, optional `missing`; ordered `records` |
| `metadata.shared.list` | `kind`, optional page controls; ID-ordered `records` |
| `metadata.shared.lookup` | `kind`, exact public-alias `fields`, optional page controls; all matching candidates with total/provenance |
| `metadata.shared.suggest` | `kind`, literal `text`, optional `mode` (`exact`, `prefix`, `contains`), `filters`, page controls; candidates without identity guessing |
| `metadata.shared.read_for_edit` | Mutable `kind`, `entity_id`; shared `metadata`, scalar `fields`, record revision |
| `metadata.shared.create` / `update` | Mutable `kind`, `data`; update also requires `entity_id` and accepts `expected_revision` |
| `metadata.shared.delete` | Mutable `kind`, `entity_id`, optional revision; hierarchical kinds also accept `detach` |
| `metadata.shared.ensure` | Exact vocabulary identity; returns reused/new ID with truthful `changed` |
| `metadata.agents.create_person` / `create_organisation` | `data`; creates the Agent and subtype atomically |
| `metadata.agents.rename` | `entity_id`, `name`, optional `derive_sort` and shared revision |
| `metadata.text.add` | `kind` (`notes`, `comments`, `synopses`), `entity_id`, `text`, optional WEMI `level`; creates fresh attached text |
| `metadata.shared.for_owner` | Contextual `kind`, `entity_id`, optional `level`, page controls; attached records and total |
| `metadata.identifiers.link` | `identifier_id`, `entity_id`, optional `level` (also `agent`) and `priority`; assigned/copied row ID |

Exact text ensures support tags, labels, genres, subjects and series through
`text`; hierarchical kinds additionally accept nullable `parent_id`. Ratings
require `value`, `out_of` and `source`. Ensures preserve existing attributes,
report known reuse as `changed=false`, and refuse ambiguous matches. Agent names
are not an identity ensure operation. Suggestions support Agents, text vocabularies
and Languages, not Ratings or contextual records.

Owner queries support Notes/Comments/Synopses on WEMI, curated Identifiers on WEMI
or Agents, and raw Item identifiers/Annotations on Items. Annotation queries
accept `user_id` and `annotation_kind`. Curated identifier assignment may copy a
row to preserve its original owner; scheme restrictions still apply to the new
owner family. Equal text on two owners is not implicitly the same record.

## Relationships

| Route family | Controls |
| --- | --- |
| `metadata.credits.list` | `entity_id`, optional WEMI `level`; Agent/role/priority records |
| `metadata.credits.add` | Owner, existing `agent_id`, `role`, optional `priority` and owner revision |
| `metadata.credits.remove` | Owner, `agent_id`, `role`, optional owner revision; retains other roles |
| `metadata.credits.set` | Owner, `role`, ordered `agent_ids`, optional owner revision; empty list clears only that role |
| `metadata.memberships.tags.list` / `series.list` | `work_id`; detached memberships and link attributes |
| `metadata.memberships.tags.add` | `work_id`, exactly one `tag_id` or literal `text`, optional Work revision |
| `metadata.memberships.series.add` | `work_id`, existing Series `entity_id`, optional Work revision |
| `metadata.memberships.tags.remove` / `series.remove` | `work_id`, member `entity_id`, optional Work revision |
| `metadata.memberships.tags.set` / `series.set` | `work_id`, `entity_ids`, optional Work revision; empty list clears membership |

All revision controls are named `expected_revision`. Removing a relationship
retains the referenced shared record. Membership replacement preserves retained
link metadata; Series membership does not imply volume-number or preferred-order
semantics. Invalid roles, duplicate set IDs and missing endpoints follow Catalog
validation rather than being silently dropped.

## Vocabulary trees

Tree routes require `kind` to be `genres`, `subjects` or `series`. `None`/JSON null
selects root scope. Structural writes use the existing hierarchy owner, including
cycle, missing-parent, sibling-completeness and attachment constraints.

- `metadata.tree.roots` and `metadata.tree.children` return sibling-ordered pages;
  children requires `entity_id`.
- `metadata.tree.ancestors` returns root-to-parent records; `metadata.tree.subtree`
  returns root-inclusive preorder. Both require `entity_id` and accept `max_nodes`.
  Exceeding the bound raises without returning a partial tree.
- `metadata.tree.ensure_path` takes text `names`, optional `parent_id`, `max_nodes`.
  It returns component IDs and preserves exact scoped matching and no-op receipts.
- `metadata.tree.create` takes `node={"data": {...}, "children": [...]}`, optional
  `parent_id`, `max_nodes`. All nodes are fresh and created atomically, with preorder
  IDs returned. Structural parent/position fields belong to the tree input shape.
- `metadata.tree.move` requires `entity_id` and explicit nullable `parent_id`,
  with optional `before_id`; omission of the sibling appends the branch.
- `metadata.tree.reorder` requires explicit nullable `parent_id` and the complete
  `entity_ids` sibling set, each exactly once.
- `metadata.shared.delete` deletes only a leaf. `metadata.tree.delete_subtree`
  deletes descendants before parents within `max_nodes`. Both require explicit
  `detach=true` when supported existing attachments must be removed. Unknown
  references still block deletion; detachment does not delete WEMI owners.

## Atomic client batches

`metadata.batch` accepts `operations` and optional `expected` revision tokens.
Each operation has `operation`, `payload` and an optional unique nonempty `key`;
the default key is its zero-based index as text. Only the write names returned by
`metadata.schema.batch_operations` are accepted. Queries, nested batches, arbitrary
Core commands, callbacks and generic invocation are excluded.

```python
receipt = client.command("metadata.batch", {
    "operations": [
        {"key": "work", "operation": "metadata.create",
         "payload": {"data": {"title": "A native library"}}},
        {"key": "tag", "operation": "metadata.shared.ensure",
         "payload": {"kind": "tags", "text": "Reviewed"}},
        {"key": "link", "operation": "metadata.memberships.tags.add",
         "payload": {"work_id": {"$ref": "work"},
                     "tag_id": {"$ref": "tag"}}}
    ]
})
```

Within a batch payload, `{"$ref": "key", "path": [...]}` selects an earlier
operation's result. Paths contain mapping keys or nonnegative array indexes;
for a stack result, `{"$ref": "stack", "path": ["item_ids", 0]}` selects its
first Item. Forward/self references, extra reference fields, invalid paths and
wrong resolved types fail. `$ref` is reserved in batch objects. References return
copied JSON values, never live handles or reflection targets.

Names, required primitive controls, keys and reference syntax are checked before
opening the batch. Resolved types and domain constraints are checked inside its
transaction. All initial `expected` tokens are checked before its callback under
the metadata writer reservation. A later operation failure rolls back prior
operations. Batch errors identify `operation_index`, `operation` and `key` where
available; this is context, not a durability guarantee independent of the owner.

The aggregate receipt contains ordered `{key, operation, result, changed}` entries.
Child pending receipts are deliberately not presented as separate committed writes.
The outer receipt owns final `persistence`, `cache` and `generation`. Results are
wire-projected before commit, so a bad child result cannot commit merely because
its serialization was deferred until response construction.

## Revisions, receipts and recovery

WEMI tokens identify `{level, entity_id}`; shared tokens identify `{kind, entity_id}`.
Both include a nonempty content digest. Shared tokens cover the record, not its
incoming relationships. Tokens are scoped to the originating catalogue and API
format. A stale token produces `metadata_conflict` before mutation; empty patches
and known relationship/ensure no-ops retain their existing `changed=false` rules.
Use a batch's initial `expected` list to guard compound/tree operations that do not
accept an individual revision argument.

Every write preserves the existing metadata receipt. `pending` is not durable
success when the caller owns an outer transaction. Cache publication failure after
commit raises `metadata_reconciliation_failed` with the receipt and
`canonical_write_committed=true`; it must not trigger an automatic repeat of the
canonical write. Subsequent supported reads recover dirty cache state through its
existing service contract. Idempotent retry after lost network responses is the
separate recoverable-command slice; these routes do not claim that guarantee.

Regression coverage lives in
[`test_native_metadata_completion.py`](../../tests/core/test_native_metadata_completion.py),
[`test_native_metadata_shared_boundary.py`](../../tests/core/test_native_metadata_shared_boundary.py),
[`test_native_metadata_validation.py`](../../tests/core/test_native_metadata_validation.py),
[`test_native_metadata_atomicity.py`](../../tests/core/test_native_metadata_atomicity.py),
and the [live PostgreSQL matrix](../../tests/core/test_native_metadata_postgres_live.py).
The latter requires `LIUXIN_TEST_POSTGRES_URL` and creates disposable schemas.
The full workflow HTTP fixtures allow 120 seconds per request because the suites
exercise many real writes and cache publications under concurrent test load;
these are correctness checks, not latency acceptance. Production client defaults
are unchanged.
Final run evidence is recorded in the [Core completion review](review-and-completion-plan-2026-11-23.md).

Return to [Core API](core-api.md), [Core](README.md), or [next tasks](next_tasks.md).
