# Mapping Calibre records to LiuXin

Checked: **2026-10-19**. The SQL and web mappings below describe implemented
behaviour; the source-import mapping remains a proposal.

## Three different meanings of “book ID”

| Context | Identity | What it identifies |
| --- | --- | --- |
| A source Calibre library | Source-library identity plus its integer `books.id`. | A row in that library's `metadata.db`. The reader returns the source ID; a future importer must preserve the library context too. |
| LiuXin SQLite `books` / `books_v` | Text ray ID such as `3:13:105`. | One complete Work–Expression–Manifestation path in the canonical catalogue. |
| LiuXin Calibre-style web book token | A Work ID, optionally with a parsed suffix. | A Work resolved by the surface catalogue adapter. It is not the SQL ray ID. |

The [current compatibility overview](<../05 - Calibre compat.md>) links the owners
of each interface. Similar field names do not make these IDs interchangeable.

## One volume, two Works, two SQL book rows

Use the [novel and introduction example](../databases/wemi-link-examples.md#6-a-novel-and-its-introduction-share-one-manifestation):

| Work | Expression | Manifestation | Owned Item | SQL `book_id` |
| --- | --- | --- | --- | --- |
| 3: *The Winter Orchard* | 13: novel text | 105: paperback | 1005 | `3:13:105` |
| 4: *Reading The Winter Orchard* | 14: introduction text | 105: same paperback | 1005 | `4:14:105` |

`books_v` has two rows because there are two complete rays. The owned volume is
still one Item. `books_v` itself stops at the ray level; the table above includes
Item context from `wemi_ray_items_v`. An additional copy would add an Item without
adding another `books_v` row. A complete ray with no Item still has a SQL book row.

The web adapter can resolve Work tokens `3` and `4`. Those Work lookups do not
select exactly one edition or assert separate physical copies. The
[WEMI examples](../databases/wemi-link-examples.md) explain the separate default-ray
and Catalog-bundle selection contracts.

## An external row is not already one of those identities

Suppose source library A has `books.id = 42`, with title *The Winter Orchard* and
an EPUB plus a PDF. The source reader emits a normalized payload with book ID 42
and the discovered file references. It does not allocate Work 42, assign a ray
ID, or create a LiuXin Manifestation.

Under the [proposed import policy](<00 - Calibre Compatibility Policy.md>), a future
importer would preserve `(source library A, 42)` and record its chosen anchor
separately. A row numbered 42 in source library B would remain a different source
record, even if matching later connected both to the same canonical entities.

The EPUB and PDF need examination before making bibliographic identity claims.
A PDF might contain an introduction absent from the EPUB, or a different
translation or edition. A source row's grouping is evidence to preserve; it does
not establish that every attached file realizes the same text or embodies the
same set of Works. The proposed Manifestation anchor is provisional and needs an
explicit way to record later splits and reassignment.

## Fields cross semantic boundaries

A flattened source record can combine several kinds of facts. This is a guide to
questions an importer must resolve, not an implemented field-to-column recipe:

| Source fact | Canonical question |
| --- | --- |
| Title and authors | Which Work or Expression does the text describe, and what roles do the contributors have? |
| Languages | Which Expressions realize which Works in those languages? |
| Publisher, date or ISBN | Which edition or product does the evidence identify? |
| Format files and cover | Which assets carry which content, and which Items or other records should reference them? |
| Local paths | Where do the bytes live, and how should storage record their placement? |
| Tags, ratings, comments and custom columns | Is this bibliographic description, local/user state, or ambiguous source data to preserve pending classification? |

A useful projection can flatten these facts for display. Its reverse mapping
requires a policy: writing a displayed field is not enough to identify which
canonical entity should change.

The [schema overview](<../03 - Database Schema.md>) describes actual stored
relations. The [import-policy draft](<00 - Calibre Compatibility Policy.md>) owns
the proposed decisions and remaining implementation work.
