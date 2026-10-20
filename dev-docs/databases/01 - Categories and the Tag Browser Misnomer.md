# Categories and the Tag Browser Misnomer

Reviewed **2026-10-20**. This describes the inherited Calibre-style facet helper, not the complete current Catalog category contract. See [Catalog read ownership](../catalog/catalog-cache-boundary.md).

Calibre’s UI has a panel called the **Tag Browser**. The name is misleading: it is not a browser of *tags*, it is a browser of **categories** (facets) such as authors, series, publishers, languages, formats, ratings, and selected custom columns.

In LiuXin, we use the word **category** for the facet itself (the thing being listed/expanded), and we treat “Tag Browser” as a legacy UI label / compatibility term.

## What a “category” means in code

The calibre-style category browser is driven by `FieldMetadata` and the category routines in `LiuXin_alpha.surfaces.categories`.

A field becomes a category if:

1. `FieldMetadata[<field>]["is_category"]` is true **and** the field is not of kind `user` or `search`, **and**
2. For descriptors of kind `field`, `in_table` is `books` (an absent value defaults to `books`).

The helper reads dictionary descriptors from [Catalog field metadata](../../src/LiuXin_alpha/catalog/field_metadata.py); this legacy `books` vocabulary does not change the WEMI schema.

There is one special case:

- **Composite** fields do not use `is_category`. In calibre, a composite field becomes a category when its display metadata contains `display["make_category"] = True`.

## Custom columns and categories

Custom columns can be attached to any table (`custom_column_in_table`), but the calibre-style category browser is (currently) a facet browser over the **books** table.

Therefore, LiuXin applies these rules:

- A **normalized** custom column attached to books appears as a category.
- A **composite** custom column attached to books appears as a category only if `display.make_category` is true.
- Custom columns attached to *non-books* tables do **not** appear in the calibre-style category browser (even if normalized), because they are not facets over the book set.

This keeps compatibility with calibre’s expectations while avoiding broken category views that would require “non-book” browsing semantics.

## Practical naming guidance

When writing docs / APIs / comments:

- Prefer **category** for the facet.
- Prefer **category browser** (or **categories panel**) for the UI concept.
- Treat “Tag Browser” as a legacy term used for calibre compatibility.

