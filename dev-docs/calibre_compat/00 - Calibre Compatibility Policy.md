# Calibre Compatibility Policy

## Status

Decision draft.

This document records the initial LiuXin policy for Calibre compatibility, especially the interpretation of Calibre's `books` table within the LiuXin WEMI/catalog architecture.

The key decision is:

> LiuXin treats each imported Calibre `books` row as a Manifestation-anchored compatibility record, 
> then projects upward to Expression and Work, and downward to Item and DigitalAsset.

This is the initial policy for the first compatibility implementation. 
It is not a claim that every Calibre library has the same semantics. 
Future import profiles may support other interpretations.

---

## Background

Calibre's `books` table is the centre of the Calibre data model. 
Most other Calibre metadata either hangs directly off `books.id`, links back to it, or assumes it as the main object of user interaction.

LiuXin does not have a direct canonical equivalent to `books`. LiuXin uses a WEMI-style model:

```text
Work
  Expression
    Manifestation
      Item
        DigitalAsset
          DigitalAssetReplica
```

Calibre's `books` row may contain or imply information that belongs at several LiuXin levels:

- Work-level information, such as title, creator relationships, abstract series membership, or conceptual grouping.
- Expression-level information, such as language, translation, version, abridgement, or text realisation.
- Manifestation-level information, such as publisher, publication date, ISBN, edition, product identity, or cover.
- Item-level information, such as library-held copy state, user tags, ratings, custom columns, and local record identity.
- DigitalAsset-level information, such as attached files and formats.
- Replica-level information, such as the actual stored copy path, backing store, backup state, or file location.

Because Calibre flattens these concerns into one central row, LiuXin must treat Calibre `books` as a compatibility/import shape rather than a canonical WEMI entity.

---

## Decision

For the initial Calibre compatibility layer:

```text
Calibre `books` row
    -> CalibreBookCompatibilityRecord
        -> anchor: Manifestation
        -> projection up: Expression, Work
        -> projection down: Item, DigitalAsset, DigitalAssetReplica
```

A Calibre `books` row is therefore interpreted as a Manifestation-centred imported record.

The row is not itself a canonical WEMI entity. It is a compatibility record that preserves the imported Calibre identity and maps it onto LiuXin's canonical WEMI/storage structure.

In short:

```text
Calibre book ~= Manifestation-anchored compatibility record
```

Not:

```text
Calibre book == Work
Calibre book == Expression
Calibre book == Manifestation
Calibre book == Item
```

The anchor is Manifestation by default, but the compatibility record remains separate from the canonical `manifestations` table.

---

## Rationale

Manifestation is the best default anchor for imported Calibre `books` rows because a Calibre row often behaves like an edition/product-like bibliographic bundle:

- it may have publisher information;
- it may have publication date information;
- it may have ISBN or other manifestation-like identifiers;
- it may have cover art;
- it may group multiple attached file formats under one bibliographic record;
- it often corresponds to "this edition or release of a book" more closely than to "this abstract Work" or "this individual local file".

This is not always true. Calibre libraries vary heavily by user habit. Some users use Calibre rows as Work-like groupings. Others use them as direct Item/file containers. Others use them as a messy mixture.

However, Manifestation is the best initial default because it is the least bad interpretation for a general Calibre import.

---

## Important caveat: formats are not assumed equivalent

Calibre groups multiple file formats beneath one `books` row. LiuXin must not assume that those formats are semantically identical.

For example, formats attached to one Calibre book row may be:

```text
same edition, different encodings
retail EPUB plus converted MOBI
PDF scan plus OCR EPUB
different editions wrongly grouped
abridged and unabridged versions grouped together
translation and original text grouped together
supplementary material grouped with the main book
```

Therefore, on import:

```text
Each attached Calibre format/file may become its own DigitalAsset.

Each DigitalAsset may be attached through its own Item.

Those Items may initially be grouped beneath the Manifestation anchored by the
Calibre book row, but LiuXin must not treat the files as guaranteed equivalent.
```

Default import shape:

```text
Work
  Expression
    Manifestation        <- anchor for imported Calibre `books` row
      Item               <- imported EPUB file/context
        DigitalAsset
          DigitalAssetReplica
      Item               <- imported PDF file/context
        DigitalAsset
          DigitalAssetReplica
      Item               <- imported AZW3 file/context
        DigitalAsset
          DigitalAssetReplica
```

Later cleanup, matching, or repair logic may split files into different Manifestations, Expressions, or Works if evidence shows that the Calibre grouping was wrong.

---

## Compatibility identity

Calibre `books.id` should be preserved as compatibility identity, not treated as canonical LiuXin bibliographic identity.

A compatibility record should preserve:

```text
source library
source Calibre book id
import policy
anchor type
anchor id
raw imported values where useful
```

Possible table shape:

```sql
CREATE TABLE `compatibility_sources` (
    `id` INTEGER PRIMARY KEY,
    `source_type` TEXT NOT NULL,
    `source_name` TEXT,
    `source_path` TEXT,
    `created_ep_k` INTEGER NOT NULL,
    `updated_ep_k` INTEGER NOT NULL
);

CREATE TABLE `calibre_book_records` (
    `id` INTEGER PRIMARY KEY,
    `compatibility_source_id` INTEGER NOT NULL,
    `source_book_id` INTEGER NOT NULL,

    `anchor_type` TEXT NOT NULL CHECK (`anchor_type` IN (
        'work',
        'expression',
        'manifestation',
        'item'
    )),
    `anchor_id` INTEGER NOT NULL,

    `import_policy` TEXT NOT NULL,

    `raw_title` TEXT,
    `raw_author_sort` TEXT,
    `raw_path` TEXT,

    `created_ep_k` INTEGER NOT NULL,
    `updated_ep_k` INTEGER NOT NULL,

    UNIQUE (`compatibility_source_id`, `source_book_id`)
);
```

For the initial implementation:

```text
anchor_type   = 'manifestation'
import_policy = 'manifestation_anchor'
```

The `anchor_type` field exists to keep the design open for future import profiles. The first implementation does not need to support all profiles.

---

## Projection policy

The compatibility layer may expose a flattened Calibre-like `books` view or API object, but that object is a projection.

It should be derived from canonical LiuXin records according to semantic ownership rules.

### Field ownership guide

| Calibre-like field | LiuXin semantic home | Initial projection policy |
| --- | --- | --- |
| `books.id` | Compatibility record | Preserve as imported compatibility identity |
| `title` | Usually Work, with Expression/Manifestation overrides | Project best display title from W/E/M path |
| `sort` | Derived/cache/projection | Derive from selected title |
| `authors` | Usually Work agents | Project creator agents by role policy |
| `author_sort` | Derived/cache/projection | Derive from selected creator roles |
| `languages` | Expression | Project expression language |
| `publisher` | Manifestation | Project manifestation publisher/agent |
| `pubdate` | Manifestation, with possible fallback | Manifestation first, then Expression/Work if needed |
| `identifiers` | Usually Manifestation; sometimes Work/Expression/Item | Merge using identifier priority policy |
| `series` | Usually Work relationship | Project series-like Work relationship |
| `tags` | Compatibility-scoped or Item/library labels | Preserve imported tags without overclaiming semantics |
| `rating` | Compatibility-scoped or Item/user state | Preserve as user/library state |
| `comments` | Ambiguous | Preserve raw; classify later if possible |
| `cover` | Manifestation or compatibility override | Project from compatibility/Manifestation, with fallback |
| `formats` | Item -> DigitalAsset | List attached payloads under the anchored Manifestation |
| `path` | Compatibility/storage projection | Preserve raw path where useful; otherwise derive |
| `size` | DigitalAsset/Replica aggregate | Project from attached assets |
| custom columns | Compatibility-scoped by default | Preserve under compatibility metadata until classified |

---

## Read policy

Compatibility reads may be generous.

A Calibre-like `books` API or view may flatten data from multiple LiuXin layers for convenience:

```text
Work title
Work agents
Expression language
Manifestation publisher
Manifestation identifiers
Item/user labels
DigitalAsset formats
DigitalAssetReplica paths
```

This is acceptable because compatibility reads are projections. The flattened shape is useful for Calibre compatibility, import diagnostics, migration tools, and legacy-style views.

However, the flattened view must not be treated as canonical truth. Canonical truth remains in the WEMI/storage tables.

---

## Write policy

Compatibility writes must be explicit and policy-driven.

A write to a flattened Calibre-style field should route to the correct semantic layer.

Examples:

```text
write title
    -> update/create title at Work level by default, unless an override policy says otherwise

write authors
    -> update Work-agent links by default

write language
    -> update Expression language

write publisher
    -> update Manifestation publisher/agent data

write pubdate
    -> update Manifestation date metadata by default

write ISBN
    -> update Manifestation identifier by default

write tags
    -> update compatibility-scoped tags or Item/library labels

write rating
    -> update compatibility-scoped rating or Item/user state

write comments
    -> preserve as imported/comment metadata until classified

write format/path
    -> storage/catalog operation, not a simple metadata field update
```

The compatibility layer should avoid silent, surprising mutation.

Method names should make write behaviour clear. For example:

```text
resolve_calibre_book()
resolve_or_create_calibre_book()
update_calibre_title()
attach_calibre_format_asset()
```

Not:

```text
get_book()
```

where `get_book()` might silently create records.

---

## API placement

Calibre compatibility belongs above raw database mechanics.

Suggested placement:

```text
catalog/
  compat/
    calibre/
      records.py
      import_policy.py
      projection.py
      writers.py
      views.py
```

or:

```text
catalog/
  compatibility/
    calibre/
      records.py
      import_policy.py
      projection.py
      writers.py
      views.py
```

Responsibilities:

```text
databases
    Generic SQL, drivers, schema, transactions, triggers, rows, views.

metadata
    WEMI concepts, value types, field semantics, metadata dataclasses.

catalog
    Metadata-aware persistence, repositories, matching, retrieval, projections.

catalog.compat.calibre
    Calibre-specific compatibility records, import policy, flattened projections,
    and write-routing semantics.

storage
    Digital assets, replicas, stores, packs, backup state, physical locations.

library
    Workflows that combine catalog and storage: import, ingest, repair, backup,
    enrichment, user-level operations.
```

The raw `databases` layer should not contain Calibre ontology decisions. It may expose compatibility SQL views if useful, but the meaning and policy should be owned by `catalog.compat.calibre`.

---

## Initial import algorithm

A first-pass Calibre import may follow this shape:

```text
for each Calibre books row:
    create/find Work
    create/find Expression
    create/find Manifestation

    create CalibreBookCompatibilityRecord
        source_book_id = Calibre books.id
        anchor_type = 'manifestation'
        anchor_id = manifestation_id
        import_policy = 'manifestation_anchor'

    project title/authors/series upward as needed
    project publisher/pubdate/identifiers to Manifestation
    preserve ambiguous metadata on the compatibility record

    for each attached format/file:
        create DigitalAsset
        create DigitalAssetReplica for the stored file
        create Item for the held digital object/context
        link Item to Manifestation
        link Item to DigitalAsset
```

This gives LiuXin a useful WEMI/storage structure without pretending the Calibre row was perfectly modelled.

---

## Future import profiles

The initial implementation uses the Manifestation-anchored import policy.

Future profiles should be considered, but not implemented until there is a concrete need.

Possible future profiles:

```text
manifestation_anchor
    Default. Treat each Calibre book row as an edition/product-like grouping.

item_anchor
    For libraries where each Calibre row is best understood as one local held
    copy or file-container record.

work_anchor
    For libraries where each Calibre row is best understood as an abstract book
    or story grouping, with formats and editions loosely attached.

expression_anchor
    For libraries organised primarily around language/version/translation.

forensic_preserve
    Preserve Calibre structure almost exactly, import raw records, and defer
    WEMI decisions until later review.
```

The schema should leave room for these profiles by storing `import_policy` and `anchor_type`, but the first implementation only needs to support:

```text
import_policy = 'manifestation_anchor'
anchor_type = 'manifestation'
```

---

## Non-goals for the first implementation

The first implementation does not need to:

```text
infer all possible WEMI distinctions perfectly
detect every wrongly grouped Calibre format
support all future import profiles
round-trip every Calibre custom column semantically
make Calibre `books` a canonical LiuXin table
make `books.id` the primary bibliographic identity
solve UI display formatting
solve backup/storage policy
```

The first implementation should:

```text
preserve source Calibre identity
anchor each Calibre row to a Manifestation
project metadata up and down consistently
preserve ambiguous metadata safely
import attached files as assets/items without assuming equivalence
leave room for future import profiles
```

---

## Summary

The working decision is:

```text
Calibre compatibility is Manifestation-anchored by default.

A Calibre `books` row is represented as a compatibility record anchored to a
LiuXin Manifestation.

The compatibility layer projects upward to Expression and Work for intellectual
metadata, and downward to Item, DigitalAsset, and DigitalAssetReplica for held
files and formats.

This is an import policy and projection strategy, not a universal truth about
all Calibre libraries.

Future import profiles may support Item-anchored, Work-anchored,
Expression-anchored, or forensic-preservation imports.
```

This gives LiuXin a firm implementation target while preserving the ability to handle the messy reality of user Calibre libraries later.
