# Catalog writer architecture

Status: inheritance-oriented writer foundation implemented, 2026-07-21.

## Decision

A catalog writer is a thin metadata-aware coordinator. The common base owns
the sequences "build one normalized update, then apply it once" and "build
one source/value instruction, then apply it once". Concrete storage-shape
writers own the update type and catalog operation.

A catalog link writer specializes that lifecycle by translating a caller's
field update into a normalized `LinkUpdate`, asking `Catalog` to apply that
update, and returning the database result.

It does not implement link persistence itself.

The shared-destination write seam is:

```text
caller values
    -> field adaptation and validation
    -> operation-aware destination-value resolution
    -> LinkUpdate
    -> Catalog.write_link_update(...)
    -> portable database macros
    -> complete LinkRow results
```

Replacements and additions may ensure a shared destination row. Deletions use
the same matching policy in find-only mode: an unknown value is already absent
and must not be created merely to delete it. After resolution, a link writer
must hold relation IDs only. Raw metadata values must not cross the
`LinkUpdate` boundary.

Owned one-to-one destination rows use a separate seam:

```text
caller replacement values
    -> field adaptation and validation
    -> CatalogOwnedRowUpdate
    -> Catalog.write_owned_row_update(...)
    -> one portable database transaction
       -> update linked row in place
       -> or create destination row and link it
       -> or unlink on None without deleting the row
```

## Ownership

| Component | Owns | Does not own |
| --- | --- | --- |
| `BaseCatalogWriter` | The invariant bulk and single-pair build-then-apply lifecycles | Any storage shape, update representation, persistence method, or value policy |
| `CatalogValueWriter` | The invariant adapt-then-validate value path and default one-entry update construction | Scalar-column, destination-row, or link persistence |
| `CatalogColumnWriter` | Normalizing a same-table value map into `CatalogColumnUpdate` | Link discovery or destination-row matching |
| `CatalogOwnedRowOneToOneWriter` | Normalizing replacement/unlink intent for a destination row exclusively owned by one source | SQL, transactions, destination cleanup, or shared-value matching |
| Catalog link writer | Interpreting field update intent, invoking field-specific adaptation/validation/resolution, constructing `LinkUpdate`, and returning the write result | SQL, transactions, locks, cache mutation, schema discovery, matching algorithms, or cleanup |
| Writer factory | Resolving declared table/column/link specifications before construction | Guessing between ambiguous columns or rediscovering schema during a write |
| Destination resolver | Matching one metadata value to a shared destination row and creating that row for replacement/addition when policy permits | Link replacement/addition/deletion semantics |
| `CatalogOwnedRowUpdate` | Immutable owned-row replacement values and the portable atomic operation they require | Metadata adaptation, SQL, cleanup, or cache mutation |
| `LinkUpdate` | Immutable normalized link instructions, validation, duplicate handling, scopes, replacement/incremental composition, and inspection | Database selection, metadata matching policy, cache updates, or presentation |
| `Catalog.write_link_update` | Catalog-facing application of a normalized update through the catalog database | Reinterpreting caller input or field-specific matching |
| Portable database macros | Atomic reads and writes, backend-portable link identity, ordering, type scope, constraints, and transaction behavior | Bibliographic or field-specific metadata policy |
| Cache | Derived, rebuildable state and cache reconciliation | Canonical persistence or write authority |
| Cleanup service | Explicit removal of unreferenced destination rows under declared policy | Being an implicit side effect of every link write |

## Catalog link writer responsibilities

A writer may:

- accept replacement, addition, deletion, and clear instructions;
- adapt caller values into the field's domain representation;
- reject invalid values before the link mutation begins;
- choose an injected resolver configured for the field;
- invoke that resolver to obtain destination IDs;
- build a `LinkUpdate` using a declared `StorageLinkSpec`;
- call `catalog.write_link_update(update)` exactly once; and
- return the complete `LinkRow` mapping produced by the database.

For a shared-value writer, creation is operation-aware. Replacement and
addition resolution may call `ensure_table_value`; deletion resolution must
call `find_table_value` and omit a missing destination as a no-op.

Validation must be explicit. Silently dropping values is not the default. A
field may deliberately define an omission policy, but that policy must be named
and tested.

### Link-type validation boundary

Link types are validated before destination values are resolved or created.
The guard covers an update-wide `link_type` scope, nested typed-map keys, and
the explicit type on a rich `LinkValue`. Typed-map syntax and explicit named
types fail on an untyped link. Named values must be non-blank strings;
`None` remains the distinct, valid SQL-null type.

When `StorageLinkSpec.allowed_types` is non-empty, every named type must be in
that declared tuple. When `allowed_types_table` is present, the driver wrapper
reads that table for each named typed write and the value must also be present
there.
The writer deliberately does not cache registry rows, so extending the table
takes effect for an existing writer instance. Both restrictions apply when
both are declared.

The checks are layered:

1. `CatalogLinkWriter` rejects invalid caller intent before adaptation,
   destination resolution, or destination-row creation.
2. `LinkUpdate` enforces typed capability, value shape, and the static
   `StorageLinkSpec.allowed_types` declaration for direct construction.
3. Portable macros repeat the capability and allowed-value checks immediately
   before persistence, including a fresh registry read, so direct catalog or
   macro callers cannot bypass the policy.

`DatabaseDriverWrapperAPI.get_allowed_link_types(...)` is the registry-read
contract. It supports canonical `{link_table}__types` tables with a `type`
column and the legacy `allowed_types__{link_table}` shape.

## Catalog link writer anti-responsibilities

A writer must not:

- issue SQL or reconstruct link-table column names;
- acquire database locks or manage transactions;
- perform a sequence of partial per-link mutations;
- update cache-owned maps or report that a cache is authoritative;
- implement creator, series, language, or custom-column matching inline;
- branch on names such as `authors`, `series`, or `publisher` to select hidden
  persistence algorithms;
- introspect the database to invent its own `StorageLinkSpec` during a write;
- delete unused destination rows as an implicit post-write action;
- catch broad exceptions merely to log and re-raise them; or
- depend on legacy database methods being forwarded directly by `Catalog`.

## Implemented hierarchy

The implementation uses inheritance only where behavior is genuinely shared:

```text
BaseCatalogWriter          build update -> apply update
│                          build one update -> apply update
└── CatalogValueWriter     adapt value -> validate value
    ├── CatalogColumnWriter
    │                         -> CatalogColumnUpdate
    ├── CatalogOwnedRowOneToOneWriter
    │                         -> CatalogOwnedRowUpdate
    └── CatalogLinkWriter    resolve shared destination -> LinkUpdate
        └── CatalogTableValueLinkWriter
                              -> ensure for replacement/addition
                              -> find-only for deletion
```

`CatalogColumnWriter` is the concrete same-table leaf.
`CatalogOwnedRowOneToOneWriter` is the separate-table leaf when each source
owns the identity of its one-to-one destination row. It updates that row in
place so a value change does not replace the destination ID. A missing link is
created atomically; `None` unlinks and leaves cleanup explicit.

`CatalogTableValueLinkWriter` is the concrete shared-value leaf. It can support
any cardinality when constructed explicitly. It delegates replacement and
addition matching/creation to `ensure_table_value`, and deletion matching to
the non-creating `find_table_value` operation.

`BaseCatalogWriter` and `CatalogValueWriter` live in
`catalog.write.base_writer`. They are regular abstract classes with explicit
constructors and identity semantics. `CatalogLinkWriter` lives in
`catalog.write.link_writer`. It is an abstract field-writer foundation rather
than a frozen callback container. The concrete leaves and their normalized
update types are also exported from `catalog.write`.

Their essential contract is:

```python
class BaseCatalogWriter[UpdateT, ResultT](ABC):
    @abstractmethod
    def build_update(self, *args, **kwargs) -> UpdateT:
        ...

    @abstractmethod
    def build_one_update(self, src_id, dst_value, **kwargs) -> UpdateT:
        ...

    @abstractmethod
    def apply_update(self, update: UpdateT) -> ResultT:
        ...

    def write(
        self,
        *args,
        **kwargs,
    ) -> ResultT:
        return self.apply_update(self.build_update(*args, **kwargs))

    def write_one(self, src_id, dst_value, **kwargs) -> ResultT:
        return self.apply_update(
            self.build_one_update(src_id, dst_value, **kwargs)
        )


class CatalogLinkWriter[RawValueT, ValueT](CatalogValueWriter, ABC):
    @abstractmethod
    def adapt(self, raw_value: RawValueT) -> ValueT: ...

    def validate(self, value: ValueT) -> None: ...

    @abstractmethod
    def resolve_destination(self, value: ValueT) -> DstTableID: ...
```

`build_update(...)` performs metadata adaptation, validation, resolution, and
normalization without applying link changes. This makes the complete immutable
instruction available for inspection, policy checks, or testing.

`write(...)` builds the same update, calls `catalog.write_link_update(update)`
exactly once, and returns that call's result unchanged. It does not catch or
retry database failures.

`write_one(src_id, dst_value, **kwargs)` is the one-entry form of `write`.
It preserves adaptation, validation, normalized update construction, catalog
application, failures, and the normal source-keyed result mapping. It does not
unwrap the result. For link writers it is an authoritative one-destination
replacement, equivalent to `write({src_id: dst_value})`; a non-destructive
plural-link change remains explicit as `write(additions={src_id: dst_value})`.

The abstract link writer's operational core is:

```python
update = LinkUpdate.from_legacy(
    self.link_spec,
    replacements,
    additions=additions,
    deletions=deletions,
    secondary_id_for=self._destination_id_for,
    link_type=link_type,
)
return self.catalog.write_link_update(update)
```

For every distinct raw metadata value processed by `LinkUpdate`, overridable
field methods run in this order:

```text
adapt -> validate -> resolve_destination
```

Existing integer destination IDs and rich `LinkValue` instructions bypass all
three metadata methods. `LinkUpdate` retains structural checks, duplicate
handling, type inheritance, and operation ordering; the writer does not
duplicate those rules.

`LinkUpdate` retains its mapping-like iteration over affected source IDs.
Per-link iteration is explicit: `update.iter_links()` streams immutable
`LinkUpdateLink` views in source-ID and database-operation order, while
`update.links()` returns the same views as a tuple. `LinkUpdateEntry` provides
matching methods for one source ID.

A `LinkUpdateLink` is a read-only mapping over its extra link-table columns.
Core relation data remains unambiguous attributes (`src_id`, `dst_id`,
`operation`, `link_type`, and `priority`), while extras support normal mapping
access such as `link["credited_as"]`, `link.get(...)`, `items()`, and
`dict(link)`.

The column-oriented `CatalogTableValueLinkWriter` deliberately tightens that
legacy convention: a raw integer is a value in the named destination column.
An already-resolved destination ID must be explicit as `LinkValue(id)`. This
prevents numeric metadata columns from being silently interpreted as row IDs.

Subclass by field behavior or storage shape, not by metadata table name. A
concrete writer may still delegate matching to an injected resolver strategy;
inheritance does not require matching policy to be embedded in the writer.

`StorageLinkSpec.cardinality` controls shared-link request validation.
`ONE_TO_ONE` and `MANY_TO_ONE` permit at most one destination per source in
replacements and additions. `ONE_TO_MANY`, `MANY_TO_MANY`, and `UNKNOWN`
permit multiple destinations. Reverse-side uniqueness for one-to-one and
one-to-many links is left to the atomic database constraint because an
isolated request cannot see every existing owner safely.

## Schema-driven factory

The public factory requires only two schema-identifying strings:

```python
writer = create_catalog_writer(
    catalog,
    src_table="titles",
    dst_column="title_title",
)

writer.write_one(title_id, "Example value")
```

Resolution is deterministic:

1. Load the declared `StorageSchemaSpec`.
2. If `dst_column` belongs to `src_table`, return `CatalogColumnWriter`.
3. Otherwise require exactly one writable table containing `dst_column`.
4. Ask the driver wrapper for the directed `StorageLinkSpec` from source to
   destination.
5. For `ONE_TO_ONE`, return `CatalogOwnedRowOneToOneWriter` using that
   specification.
6. For every other cardinality, return `CatalogTableValueLinkWriter` using
   shared ensured-value policy.

An unknown table or column, an ambiguous destination column, a non-writable
source, or a missing link fails during factory construction. `force_refresh`
is available for callers which have changed the schema out of band.

## Catalog API

`Catalog` and `CatalogAPI` expose the schema-driven workflow directly:

```python
writer = catalog.create_writer("titles", "title_title")
update = writer.build_one_update(title_id, "Example value")
result = writer.apply_update(update)

catalog.write("titles", "title_title", {title_id: "Bulk value"})
catalog.write_one("titles", "title_title", title_id, "Single value")
```

`create_writer(...)` delegates to `create_catalog_writer(...)`. The `write(...)`
and `write_one(...)` conveniences construct that writer and forward the concrete
writer's arguments unchanged, so replacement, incremental, typed-map,
rich-link, extra-column, type-scope, and clear forms retain their normal
semantics and return mappings.

The normalized `write_column_update(...)`, `write_link_update(...)`, and
`write_owned_row_update(...)` methods remain the persistence seam used by
writers. The conveniences select and drive writers; they do not duplicate
validation, resolution, transaction, or database-write logic.

## Storage-cache API

`StorageCacheAPI` mirrors the catalog workflow without introducing a second
persistence implementation:

```python
writer = cache.create_writer("titles", "title_title")
update = writer.build_one_update(title_id, "Example value")
result = writer.apply_update(update)

cache.write("titles", "title_title", {title_id: "Bulk value"})
cache.write_one("titles", "title_title", title_id, "Single value")
```

`create_writer(...)` returns the concrete catalog writer selected by the
normal factory, including column, owned one-to-one, and shared-link writers.
The returned writer retains build/inspection, bulk, single-value, replacement,
incremental, typed-map, rich-link, and link-type-scope behavior. Consequently,
the same early type guards and live allowed-type registry checks apply through
the cache API.

The cache does not persist data itself. It supplies a small catalog facade to
the writer, lets `Catalog` and portable macros perform the canonical write,
then reconciles only affected cache objects after success. Snapshot backends
reload the changed main table or the destination and both directed link-table
views. The database-backed cache needs no explicit refresh because its reads
are live. Empty writes and failed validation/database writes do not refresh or
otherwise mutate cache state.

A writer created by a cache remains bound to that cache/database pair. If the
cache is detached or attached to a different database, the old writer rejects
application before persistence. `cache.catalog` is the public compatibility
alias for the attached `cache.db` handle.

## Scalar-field boundary

Same-table scalar column updates are not link updates. `CatalogColumnWriter`
therefore builds `CatalogColumnUpdate` and applies it through
`Catalog.write_column_update`; it does not inherit `CatalogLinkWriter` or
construct a synthetic `StorageLinkSpec`.

Likewise, an owned one-to-one value stored in another table is not represented
as a synthetic scalar-column update or as shared-value replacement. It uses
`CatalogOwnedRowUpdate`, while the portable macro layer keeps destination-row
creation and link creation in one transaction. `BaseCatalogWriter` remains
suitable precisely because it knows nothing about relation cardinality,
custom-column links, creator matching, or cleanup.

## Return contract

Link writers return the mapping produced by `Catalog.write_link_update`:

```python
Mapping[SrcTableID, tuple[LinkRow, ...]]
```

Owned one-to-one writers return the same result shape through
`Catalog.write_owned_row_update`, including an empty tuple for an explicit
unlink.

Column writers return the stable values accepted by
`Catalog.write_column_update`:

```python
Mapping[SrcTableID, ValueT]
```

Introduce a larger result dataclass only when a real caller needs additional
stable information such as created destination IDs or resolution provenance.
Do not predict that requirement in the base contract.

## Rebuild sequence

1. Retain and test `StorageLinkSpec`, `LinkUpdate`, and
   `Catalog.write_link_update` as the stable lower seam.
2. Build an inheritance-oriented lifecycle base and value-preparation base.
   **Complete.**
3. Test replacement, incremental, typed, ordered, empty, invalid, resolver
   failure, all link cardinalities, and real-database behavior. **Complete for
   the foundation.**
4. Define the same-table scalar update and catalog-application contract.
   **Complete.**
5. Add schema-backed concrete column/link writers and minimal-string factory.
   **Complete.**
6. Add the first policy specializations: operation-aware shared-value lookup
   and owned-row one-to-one mutation. **Complete.**
7. Add metadata-specific field writers and resolver policies where these
   generic policies are insufficient.
8. Migrate live cache-writer callers one field family at a time, keeping cache
   reconciliation separate from canonical database writes.
9. Add cleanup as an explicit service or workflow only where field policy
   requires it.

## Removed legacy design

The old catalog-only writer hierarchy was an incomplete migration of the cache
writers. It mixed cache-era calling conventions with catalog and database
responsibilities, used nullable table/column strings instead of a stable link
specification, and had no live consumers or direct tests.

It is intentionally removed rather than retained as a template. The live
`caches.write` hierarchy is outside this decision and remains in place until
its callers are migrated deliberately.
