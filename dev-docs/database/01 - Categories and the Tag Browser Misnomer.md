# Categories and the Tag Browser Misnomer

Calibre’s UI has a panel called the **Tag Browser**. The name is misleading: it is not a browser of *tags*, it is a browser of **categories** (facets) such as authors, series, publishers, languages, formats, ratings, and selected custom columns.

In LiuXin, we use the word **category** for the facet itself (the thing being listed/expanded), and we treat “Tag Browser” as a legacy UI label / compatibility term.

## What a “category” means in code

The calibre-style category browser is driven by `FieldMetadata` and the category routines in `LiuXin_alpha.databases.categories`.

A field becomes a category if:

1. `FieldMetadata[<field>]["is_category"]` is true **and** the field is not of kind `user` or `search`, **and**
2. (LiuXin rule) the field is a facet over **books** — i.e. `FieldMetadata[<field>].in_table == "books"`.

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

