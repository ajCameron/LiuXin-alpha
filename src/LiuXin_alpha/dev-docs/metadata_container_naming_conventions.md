# Metadata container naming conventions

See also: `metadata_container_dynamic_convenience_policy.md` for the current policy on runtime-installed convenience properties.


This note records the naming rules used by the current metadata-container
architecture.

## Core rules

- Use `...Kind`, `...Role`, and `...Scheme` for controlled classification values.
- Use `target_id` and `target_kind` for the attached W/E/M/I target.
- Use `as_write_payload()` for database-write serialisation.
- Use `to_text(sep=...)` for joining a single container into display text.
- Use `kind_text(...)`, `role_text(...)`, or `scheme_text(...)` on top-level
  containers when selecting a classified subset.
- Use `*_text` for default joined text convenience properties.
- Use `*_to_text(sep=...)` for joined text methods with caller-supplied
  separators.
- Reserve `display_*` for singular chosen display values, such as
  `display_title` or `display_genre`.

## Current examples

- Titles: `main_titles_text`, `main_titles_to_text()`
- Notes: `descriptions_text`, `descriptions_to_text()`
- Labels: `tags_text`, `tags_to_text()`
- Subjects: `topics_text`, `topics_to_text()`
- Agent credits: `authors_text`, `authors_to_text()`
- Identifiers: `isbn_13_text`, `isbn_13_to_text()`

## Intentionally retained domain-specific names

Some families still expose domain-specific singular helpers where that improves
clarity:

- `display_title`
- `display_genre`
- `main_title`
- `sort_title`
- `primary_identifier_for_scheme()`

These are not exceptions to the joined-text rule. They represent chosen or
primary values rather than formatted aggregates.
