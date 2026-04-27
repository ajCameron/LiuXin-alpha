# Metadata container DB-constraint alignment

This note freezes the stage-13 rule: where the database is meant to constrain an
allowed metadata vocabulary, the FRBR generator should draw that constraint from the
**same canonical source** as the Python container layer.

## Canonical homes

- Database-constrained identifier vocabularies live in `LiuXin_alpha.databases.db_types`.
- Shared additional-metadata-family vocabularies live in
  `LiuXin_alpha.metadata.constants.container_vocabularies`.
- The FRBR generator is responsible for turning those canonical vocabularies into SQL
  `CHECK` constraints when a schema table chooses to store the relevant column.

## Live today

The following are already generator-aligned and active in the schema:

- `entity_identifier_entity_type`
- `entity_identifier_scheme` (constrained by entity type)
- `item_identifier_scheme`

These constraints are generated from `db_types`, not duplicated as hand-written SQL
lists in the schema files.

## Prepared for future schema columns

The generator now also recognises placeholders for the canonical metadata-family
vocabularies below:

- `__TITLE_KIND_CHECK__`
- `__NOTE_KIND_CHECK__`
- `__NOTE_FORMAT_CHECK__`
- `__NOTE_VISIBILITY_CHECK__`
- `__LABEL_KIND_CHECK__`
- `__GENRE_KIND_CHECK__`
- `__SUBJECT_KIND_CHECK__`
- `__IDENTIFIER_STATUS_CHECK__`

At the time of writing, the FRBR SQL schema does **not** yet materially expose all of
those columns/tables, so most of these placeholders are currently unused. This is
intentional: the generator glue is ready, but the schema should only add constraints
when the corresponding columns are real.

## Not yet aligned

Agent-credit roles are still an intentional exception. Those constraints currently flow
through the interlink/intralink TOML and generated `__types` tables rather than a single
canonical role enum wired straight into SQL `CHECK` constraints. That may be worth
concertising later, but it is not part of this stage.

## Practical rule

When adding a new DB-constrained metadata vocabulary:

1. define the canonical values in the correct home;
2. teach the generator to substitute a placeholder from that canonical source;
3. use the placeholder in the SQL file once the column actually exists;
4. add a test proving the generated SQL contains the expected values.
