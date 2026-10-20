# Metadata container vocabularies

Reviewed **2026-10-20**. Value containers remain passive. Hydration belongs to Catalog; current API roots and the follow-up work are linked in [next tasks](next_tasks.md). Stage numbers and historical test totals below describe their original campaign.

This note freezes where controlled vocabularies should live.

## Rule of thumb

- **Bibliographic vocabularies** live in `LiuXin_alpha.metadata`, including when
  database schema generation consumes them to build constraints.
- **Core metadata typing** for W/E/M/I ids and agent-credit roles lives in
  `LiuXin_alpha.metadata.metadata_types`.
- **Shared metadata-family vocabularies** for additional metadata containers
  live in `LiuXin_alpha.metadata.constants.container_vocabularies`.

## Current canonical homes

### `LiuXin_alpha.metadata.identifiers`

- `IdentifierEntityType`
- `IdentifierScheme`
- per-entity identifier scheme sets

### `LiuXin_alpha.metadata.relators`

- `MarcRelatorRole`
- per-WEMI MARC relator role sets

### `LiuXin_alpha.metadata.metadata_types`

- `WorkID`, `ExpressionID`, `ManifestationID`, `ItemID`, `LanguageID`
- `CreditSource`
- `WorkAgentRole`, `ExpressionAgentRole`, `ManifestationAgentRole`, `ItemAgentRole`

### `LiuXin_alpha.metadata.constants.container_vocabularies`

- `TitleKind`
- `NoteKind`, `NoteFormat`, `NoteVisibility`
- `LabelKind`
- `GenreKind`
- `SubjectKind`
- `IdentifierStatus`

## Implication

Container API and implementation modules should import these shared enums rather
than redefining them locally. Dynamic convenience-property layers may still use
these enums, but they do not own them.


## Related notes

- `metadata_container_db_constraint_alignment.md` — how generator-enforced DB constraints line up with canonical metadata vocabularies.
