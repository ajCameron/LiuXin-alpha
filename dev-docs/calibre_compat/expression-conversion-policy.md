# Expression-centred Calibre conversion

Decision and implementation: **2026-10-29**. Profile: **`expression-v1`**.

The logical book in this compatibility projection is an **Expression**. An EPUB
and print edition of the same realization can therefore share a book projection;
a translation is a separate Expression. This is a local conversion policy, not a
claim that an arbitrary external Calibre row already identifies one Expression.
Source rows may contain mixed editions or content and still require assessment.

The [older import draft](<00 - Calibre Compatibility Policy.md>) proposed a
provisional Manifestation anchor for preserved source records. That proposal
does not govern this implemented projection. Durable import identity, splitting
and reassignment remain separate work; no source-record schema was added here.

## Owners and public API

[`metadata.calibre_conversion`](../../src/LiuXin_alpha/metadata/calibre_conversion.py)
owns the immutable reference, policy revision and pure value rules.
[`catalog.compatibility.calibre`](../../src/LiuXin_alpha/catalog/compatibility/calibre.py)
implements graph resolution, projections and atomic writes, exposed as
`catalog.calibre`. It delegates to canonical repositories and portable database
macros. Databases does not import Catalog, and Catalog contains no executable SQL.

The first profile implements six writable fields and derived title sort. It is
not a complete Calibre Metadata object, a source importer or a lossless export.

## Identity is explicit

| Input | Meaning and handling |
| --- | --- |
| `CalibreBookRef(expression_id=13)` | Explicit canonical Expression identity. |
| `CalibreBookRef(13, work_id=3, manifestation_id=105)` | The same Expression with selected Work/edition context; every supplied edge is validated. |
| SQL ray string `"3:13:105"` | Parsed into the same reference and checked against current graph membership. |
| Bare integer `13` or numeric string `"13"` | Rejected: it could be a source Calibre ID, web Work token or another canonical ID. |
| `(source library, source books.id)` | Still requires a durable import mapping; this adapter does not invent one. |

Existing SQL `books.book_id` values remain W:E:M ray strings. Existing web book
tokens remain Work IDs. No existing ID space is silently redefined as Expression
IDs. A parsed ray that no longer exists is rejected even if its individual rows
still exist. The adapter does not create missing rows or relationships.

## Field policy

| Compatibility field | Read and write owner | Rule |
| --- | --- | --- |
| `title` | Expression title override | Display falls back through Expression subtitle, Work title and canonical title. `None` clears the override. A write never changes the shared Work title. |
| `title_sort` | Derived projection | Calculated from the displayed title using the shared title-sort helper. No separate write changes Work sort. |
| `language` | Expression primary Language FK | Accept an existing Language ID or `None`. Preserve the Work's original language and other language relationships. |
| `last_modified` | Expression modification timestamp | Read/write integer epoch milliseconds. ISO datetimes and datetime objects are accepted; naive datetimes mean UTC. Fractional milliseconds are floored. Numeric units are never guessed. |
| `comments` | Expression comment attachments | Read all attached comments in repository order, separated by blank lines. A write replaces that Expression's comment attachments; `None` clears them. Old rows are retained, and other WEMI levels are untouched. |
| `author_sort` | Selected Work's creator sort | Shared by that Work's Expressions. Select Work context explicitly when an Expression has several parents. |
| `rating` | Selected Work, policy-owned rating link | The current schema has no Expression-rating link. Use a ten-point score, retaining fractions; zero/`None` means unrated. Only links typed `calibre:expression-v1` are replaced. Other rating sources remain. |

Work author sort and ratings are deliberately **shared Work fields**, even though
the book projection is Expression-centred. They do not represent separate ratings
for translations or users. Adding Expression-specific or source/user-specific
ratings requires an explicit schema/profile decision. The rating value is never
interpreted as a row ID; the adapter allocates a validated rating row and reuses
its own unchanged rating. Replacements retain old unlinked rows, as comment
replacement already does; garbage collection is separate policy.

Cover flags are derived from asset attachments and cannot create or remove cover
bytes. Series position still lacks a dedicated field; link priority is not a
volume number. Publisher, ISBN, publication date, format inventory, sizes, paths
and conversion options need their own explicit edition/storage/source rules.
`write_field` rejects all fields without an implemented rule.

## Read selection does not authorize ambiguous writes

`project()` honors explicit context. Otherwise it reuses Catalog's
[preferred WEM path ranking](../catalog/catalog-wemi-selection.md) for display.
It retains all `work_ids` and `manifestation_ids`, including unselected editions.
The original `reference` remains separate from the selected display IDs. Items
do not establish book identity or choose an edition; an Expression without an
edition still has a projection. Reads are not an atomic snapshot.

An Expression-scoped write needs no preferred Work. A Work-scoped write requires
explicit Work context or exactly one distinct parent. It never takes the first
parent merely because display selected that path. An orphan Expression can
receive title/comment/language/timestamp changes but cannot receive a Work rating
or author sort until the caller supplies valid linked Work context.

## Worked examples

Assume Work 3 is the novel, Expression 13 its text, and Manifestations 105 and 106
two editions. Work 4/Expression 14 is an introduction sharing Manifestation 105.

```python
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.metadata.calibre_conversion import CalibreBookRef

catalog = Catalog(db)
novel = CalibreBookRef(expression_id=13)
book = catalog.calibre.project(novel)
assert book["expression_id"] == 13
assert book["manifestation_ids"] == (105, 106)

introduction = catalog.calibre.project("4:14:105")
assert introduction["expression_id"] == 14

# Both edition rays resolve to Expression 13. Identical updates coalesce.
catalog.calibre.write_field("title", {
    "3:13:105": "The revised novel",
    "3:13:106": "The revised novel",
})

# Inspect the shared owner before choosing a Work-scoped operation.
assert catalog.calibre.resolve_field_target("author_sort", "3:13:105") == (
    "work", 3,
)
catalog.calibre.write_field("author_sort", {"3:13:105": "Surname, Given"})
catalog.calibre.write_field("comments", {novel: "Notes about this realization"})
```

The title change leaves the introduction, another translation, Work title and
edition metadata intact. It is visible through both novel edition rays in this
new projection. An additional Item for Manifestation 105 does not add another
logical book. SQL compatibility views retain their older read policy: notably,
`titles` remains a Work projection and `books.book_last_modified` aggregates WEM
timestamps. They are not transparently rebound to this adapter's field rules.

## Transactions, aliases and cache boundaries

`write_field(field, mapping)` validates values, resolves all identities and
detects conflicting assignments before writing, inside one portable transaction.
Equivalent keys resolving to one owner are written once when their prepared
values agree. Different values for that owner reject the whole batch. This also
detects two different Expressions trying to write different author-sort/rating
values to their shared Work. An enclosing macro transaction can roll back the
operation, and late repository failures roll back earlier entries.

Results retain input keys. Scalar fields return canonical owner IDs;
comments/ratings return associated row IDs or `None` on clear. Compatibility
callers must not use either kind of result as a replacement book ID.

Migrated persistence hooks cover author sort, title, last-modified, comments,
primary-language clearing and ratings. The title writer no longer dispatches a
second write to the old sort projection, and rating value preparation uses the
shared policy instead of truncating fractions. Their keys must now be explicit
references or rays; inherited integer-keyed caches are not thereby converted.

The full legacy cache is still unsupported. Some surrounding cache routines
mutate memory before persistence, and refresh/invalidation of other rays sharing
an Expression or Work remains the cache owner's responsibility. These changes
establish the persistence boundary, not end-to-end cache atomicity. The frozen
`catalog_macros` facade and flagged `metadata_sql` entry points are not restored.
Three now-uncalled SQL entry points were removed from the implementation and
API: `update_title_creator_sort`, `set_title_rating` and
`break_lang_title_primary_link`.

## Validation and remaining work

[`test_calibre_conversion_policy.py`](../../tests/catalog/test_calibre_conversion_policy.py)
covers shared editions, introductions, translations, orphan/shared Expressions,
invalid identities and edges, alias conflicts, fractional ratings, unrelated
rating sources, comment replacement and rollback, and the migrated cache hooks.
Run alongside the Catalog ownership, SQL-boundary and generated-view tests.

Verification on **2026-10-29**: **165 distinct tests passed** across SQLite and
APSW. The initial focused run passed 62 tests. The broader run passed 164 and
found one exported-API documentation check requiring a fuller class docstring;
that check passed after the docstring was expanded. Coverage also includes
Catalog convenience/API/aggregate contracts, WEMI path selection, generated
compatibility views, then-current `metadata_sql` helpers and writer dependency/import
contracts. New policy, compatibility, API and regression modules pass Ruff, and
`git diff --check` passes. No full-suite or live PostgreSQL run was performed.

Next: convert remaining cache identity/read models explicitly; replace cover and
asset inventory methods through storage owners; decide Series position and
source-scoped user state. Compatibility projections were removed from
`metadata_sql` on **2026-10-30**, while concrete table operations remain there.
The unused `catalog_macros` facade stays retired. Source-library import continues
to require durable identity and provenance.
