# Link capabilities

Link tables may carry two optional, independent pieces of relation metadata:

- a type column, which assigns a role or kind to the relation;
- a priority column, which orders related rows.

Use the same capability query at every database layer:

```python
capabilities = database.get_link_capabilities("agents", "works")

capabilities.typed       # True
capabilities.priority    # True
capabilities.both        # True
capabilities.kind        # LinkKind.TYPED_PRIORITY
capabilities.type_column
capabilities.priority_column
capabilities.link_table
```

The low-level driver spelling is
`direct_get_link_capabilities(table1, table2)`; the driver wrapper and
`Database` expose `get_link_capabilities(table1, table2)`.

`LinkKind` is exhaustive:

| Kind | Type column | Priority column |
| --- | --- | --- |
| `PLAIN` | No | No |
| `TYPED` | Yes | No |
| `PRIORITY` | No | Yes |
| `TYPED_PRIORITY` | Yes | Yes |

`LinkCapabilities.ordered` is an alias for `priority`, matching the existing
`StorageLinkSpec.ordered` vocabulary. The capability result also exposes the
actual physical column names, so callers do not need to reconstruct them.

For an intralink, pass the same table as both endpoints:

```python
database.get_link_capabilities("works", "works")
```

If both endpoint tables exist but have no link table, the method returns
`None`. An unknown endpoint table raises `InputIntegrityError`. Use
`force_refresh=True` after out-of-band schema changes.
