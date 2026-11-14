# Native custom fields

The native API supports SQLite, SQLite_apsw and PostgreSQL. Construction only
reads configuration and never changes a catalogue. The portable `CustomColumns`
facade adapts Calibre-shaped callers onto the same storage; see the
[compatibility service contract](custom-columns.md).

See [custom-column ownership](custom-column-ownership.md) for the native/driver/cache
boundary and the removed compatibility entry points.

## Identities and values

Use `CustomFieldService(db)`, or `catalog.custom_fields`. An owner is an
`OwnerRef(table, id)`. A field reference is its integer definition ID or
`(attachment, label)`. Labels and names are unique within their attachment.
FieldMetadata keys are `#works.status`, `#manifestations.status`, and so on.
A short alias such as `#status` exists only when it is unambiguous. Attachment
tables are explicit. Series positions live in values; native metadata has no
synthetic `_index` companion, so that suffix remains available for real fields.

```python
from LiuXin_alpha.databases.custom_columns import CustomFieldService, OwnerRef

fields = CustomFieldService(db)
field = fields.create_field("works", "notes", datatype="comments", mode="list")
owner = OwnerRef("works", work_id)
fields.set_value(owner, field.id, ["First note", "First note", "Second note"])
notes = fields.get_value(owner, field.id)
fields.edit_entries(owner, field.id, remove=[notes[1].id])
```

Stored datatypes are text, comments, enumeration, Series, integer, float,
boolean, rating and datetime. Each supports these modes:

| Mode | Result | Membership |
| --- | --- | --- |
| `scalar` | Typed value, `SeriesValue`, or `None` | At most one entry |
| `set` | Tuple of `ValueEntry` objects | Ordered unique identities |
| `list` | Tuple of `ValueEntry` objects | Ordered entries; repetitions retained |

`ValueEntry` carries entry ID, typed value, ordinal, optional Series position,
and optional shared vocabulary ID. Positions belong to individual Series
entries. Use `SeriesValue("Saga", 2.5)` when supplying one. Set identity is the
value, so a repeated Series name does not create a second set member with a
different position. Use a list when those repetitions matter.

Text, enumeration and Series identities use NFC plus case folding. Comments
preserve case and delimiters. Shared vocabulary spelling is authoritative;
`rename_value` changes it for all referencing owners, merging identities when
needed. Enumeration renames also update the allowed choices. Integer values
must fit signed 64 bits; booleans are distinct from integers; ratings are 0–10.
Floats and Series positions must be finite. Datetimes retain naive values;
aware values normalize to UTC. Use consistent timezone awareness within a
field when making range queries. Empty text, false and zero are stored values.
`None` clears a field; collection entries themselves cannot be null.

A replacement creates new entry identities unless supplied `ValueEntry`
objects retain them. Deleted entry and vocabulary identities are not reused by
field migrations, including after a field becomes empty or changes between shared
and inline storage. Entry IDs and vocabulary IDs remain separate spaces; migration
allocation can introduce gaps and callers must not infer counts from IDs.
`edit_entries` preserves retained IDs and supports
addition, replacement, removal and complete reorder permutations. IDs supplied
for another owner or field are rejected. Foreign keys and persistent SQLite
triggers clean memberships when an owner is deleted, including through an
independent connection with SQLite foreign-key enforcement disabled.

## Transactions and publication

`set_value`, `apply_patch`, shared-value changes and entry edits join managed
macro transactions. A patch may span fields and attachment tables; all inputs
are validated before its first value write. An expected field revision can
reject stale edits. Revisions cover the whole field, not individual owners.
Standalone writes reserve the writer before inspecting the field. Nested
PostgreSQL writes acquire the same transaction advisory lock even inside an
ordinary macro transaction, before reading revisions or plan fingerprints.
Savepoint rollback may release its reservation, so each later operation acquires
it again. This coordinates native/cooperating writers; raw SQL outside that
protocol remains the caller's responsibility.

Ordinary SQLite composition retains SQLite's single-writer and snapshot rules.
If a deferred read transaction cannot promote to a writer, roll back and retry
the outer transaction. Do not retry an inner edit against the same stale snapshot.

```python
with db.macros.transaction(serialized=True):
    fields.set_value(owner, first_field, "ready")
    fields.set_value(owner, second_field, 3)
```

Nested failures roll back their savepoint and discard its pending publication.
Native receipt listeners run only after the owning transaction commits, under
the host mutex. The native service owns no metadata or result cache. A failed listener cannot undo committed SQL:
the raised exception group explicitly reports that publication failed after
commit. Do not retry that write; refresh the projection. Unmanaged borrowed
transactions cannot deliver native write receipts. Use the managed transaction
API for composition.

`subscribe(callback)` returns an unsubscribe function. Receipts report owners,
fields and resulting revisions. Migration receipts additionally map retained or
removed entry IDs and used vocabulary IDs; a removed identity maps to `None`.
Subscriptions belong to the service instance; they are not a cross-process
change feed. Callers writing through another service or connection must refresh
their own projections.

`caches.custom_fields.CustomFieldCache` owns the native FieldMetadata projection.
Construct it with a native service and a FieldMetadata object. Its `write()`
context prepares descriptors before commit, so preparation failures roll SQL
back, and publishes them after the outer transaction commits. Nested rollback
also discards pending descriptors. Core native commands use this coordinator.
Receipt listeners may run before the separate metadata publication callback.

```python
from LiuXin_alpha.caches.custom_fields import CustomFieldCache

projection = CustomFieldCache(fields, field_metadata)
with projection.write() as fields:
    fields.create_field("works", "status")
projection.refresh()  # Explicit refresh after writes through other services.
```

`refresh()` reads a snapshot and defers publication if called inside a managed
transaction. Direct native calls do not update this projection. Database table
registries belong to `Database.refresh_db_metadata()` and its normal driver
invalidation path; the native service does not maintain wrapper aliases.

## Reading, querying and categories

`get_values(owners, fields)` reads the applicable owner/field matrix within one
snapshot, batching owner validation and value bindings in groups of 400.
`get_owner_values` reads stored fields for one owner. `inventory` offers paged
values and distinct-owner counts, including unused vocabulary filtering.
`usage_count`, `is_shared`, and `next_series_position` support shared values.
The Series helper returns the maximum stored position plus one, or one when
none exists.

`find_owners` combines predicates with AND. Supported operators are equality,
membership, case-sensitive literal substring/prefix, numeric/datetime range,
and unset. Collection quantifiers are `any` and non-vacuous `all`: an empty
collection does not satisfy `all`. Equality uses the field's canonical identity.
The result uses an ascending owner-ID cursor. Page limits are 1–1000. Keep an
outer `fields.read()` context when several pages must share one snapshot.
Computed fields cannot be SQL-filtered through this API.

The cache-owned native FieldMetadata projection includes normalized categories for
every attachment. Series descriptors expose structured values without synthetic
`_index` keys. Explicit Calibre projections can expose their own scoped scalar
Series companions.
The legacy results-cache projection supports scalar values and delimiter-free
text sets. Lists, non-text collections and per-entry Series positions require
the native API and raise `utils.calibre_compat.custom_columns.CompatibilityError`
at the compatibility boundary. This exception and the facade are not exported by
the native package.
All compatibility text-set writes, including shared-value renames and bulk
additions, reject delimiter-bearing input before committing. Bulk removals
clean legacy input and compare NFC/case-folded identities; additions win when
the same identity appears in both inputs. Scalar values and native collection
values continue to support delimiters.
Native composites are evaluated through Catalog, including when a legacy
caller would otherwise be able to represent their rendered string.

## Explicit schema evolution

Fresh FRBR catalogues and all creation APIs use the sole native storage layout,
whose persisted marker is `2`. V1 support has been removed entirely: no reader,
writer, importer or upgrade tool remains. Unsupported definitions raise
`UnsupportedStorage` without mutation. The Calibre facade adapts supported API
shapes onto native operations; it never dispatches by storage version.

Schema plans fingerprint metadata, values, owner identities and schema definitions.
Applying a stale fingerprint fails. Plan intent is checked again under the native
mutation transaction. PostgreSQL uses the cooperating writer advisory lock.

`plan_field_migration(field, changes, ...)` covers datatype, mode, attachment,
label, display, editability and name changes. Populated attachment moves require
an explicit owner map. Multiple-to-scalar conversion requires `reduce="first"`
or `"last"`; conversions that collapse set identities require `collapse=True`.
Enumeration choice removals fail when existing values lack valid replacements;
provide a `value_mapping` to resolve them. Mapping a value to `None` explicitly
removes its memberships. There is no implicit lossy numeric
conversion. Dependency-changing edits are rejected until dependent composites
are updated. Plans may materialize the field's values in memory; schedule large
catalogue migrations accordingly.

Label, name and editability changes without value mappings update the definition
in place, preserving physical tables and indexes. Rebuilds capture allocator
history before dropping storage, including deleted identities, then reserve that
bound in every replacement table before generating new identities. Carrying a
conservative bound through both storage shapes preserves vocabulary history even
when an intermediate datatype has no vocabulary table. This needs no cache or
additional storage-version support; all changes remain in the migration unit.

The explicit compatibility adapter and existing Core `custom-fields.update` route use this
migration validation for every field, including enumeration choices,
composite dependencies and revision increments. Adapter edits can join managed
macro transactions; their caller owns any compatibility projection refresh.
Compatibility facade metadata edits and retirement require their own transaction
because they refresh maps immediately after commit. Native service operations support managed composition.

`retire_field` hides a field and rejects new values without deleting storage.
Retired fields survive compatibility facade loading. Only an explicit
`plan_purge` / `apply_purge` permanently removes them. Audit reports invalid
metadata/values, missing storage, orphan references, ordering and cleanup
problems. `plan_repair` accepts explicit actions: `cleanup_trigger`, `ordering`,
`orphan_owner`, `orphan_value`, `unused_values`, and `identity_key`. Unsupported integrity faults
remain blockers; repairs never infer replacement values. Ordinary native reads
do not run repairs.

On SQLite, audit compares every owned cleanup/reference trigger with the
canonical generated definition: owner deletion, shared-value deletion and
insert/update reference guards. Missing or modified definitions produce a
`cleanup_trigger` issue naming the trigger. Explicit repair reinstalls the
complete set for the field; audit itself leaves the schema unchanged. Even a
semantically equivalent manual rewrite is reported for reconciliation.

## Computed fields

Use `catalog.computed_fields`, or `CustomFieldEvaluator(fields)`. The evaluator
reuses the bundled Calibre template engine, reads within one snapshot and
reports owner/field context on failure. Composite fields are read-only.

```python
caption = fields.create_field(
    "works", "caption", datatype="composite",
    display={"composite_template": "{title}: {#works.status:uppercase()}"},
)
```

Local references resolve on the explicit owner. Cross-attachment reads need a
named path in `display.paths`, for example:

```python
{
    "composite_template": "{translations}",
    "paths": {
        "translations": {
            "tables": ["expressions"],
            "field": "label",
            "aggregate": "join",
            "separator": ", ",
        }
    },
}
```

Paths traverse each declared relationship and deduplicate owners in ID order.
Several related owners require explicit aggregation: join, list, count, sum,
min or max. No preferred Work/Expression/Manifestation/Item is inferred.
Definition references and explicit dependency IDs are checked for cycles;
runtime cycle detection also covers dynamically resolved template references.
Multiple composites split their formatted result using the configured separator,
preserving repetition for lists and removing it for sets.

## Core transport

The `custom-fields.native.*` family is separate from existing compatibility
routes. Queries expose `definitions`, `value`, `values`, `owner`, `inventory`,
`usage`, `find`, `next-series`, `audit` and `plan`. Commands expose `create`,
`patch`, `entries`, `rename-value`, `delete-value`, `retire` and `apply`.
Inspection/value reads require `read`; value mutations require `execute`;
creation, retirement, audit and schema/repair previews or application require
`admin`. These requirements use the existing Core authorization boundary.

Wire owners are `{ "table": "works", "id": 42 }`; field references are numeric
IDs or `["works", "status"]`. Entry objects retain IDs and positions. Datetimes
use `{ "$datetime": "2026-01-01T00:00:00+00:00" }`. `display` stays a JSON object.
Migration value mappings are arrays of `[old, new]` pairs. Plan application
accepts the original intent plus its preview fingerprint and rebuilds the plan
server-side; it does not trust client-supplied effects, issues or SQL.

Regression coverage lives in [the native contracts](../../tests/databases/test_native_custom_fields.py)
and [their PostgreSQL lane](../../tests/databases/test_native_custom_fields_postgres_live.py).
The [follow-up regressions](../../tests/databases/test_custom_column_followup_repairs.py)
and [portable PostgreSQL cases](../../tests/databases/test_custom_column_followup_postgres_live.py)
cover the six findings from the [follow-up review](custom-columns-followup-review-2026-11-12.md).
All native cases are mandatory in the database regression ratchet. Return to
the [database index](README.md).
