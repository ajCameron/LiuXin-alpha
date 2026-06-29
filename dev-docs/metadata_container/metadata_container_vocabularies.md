# Metadata container vocabularies

This note freezes where controlled vocabularies should live.

## Rule of thumb

- **Database-constrained vocabularies** live in `LiuXin_alpha.databases` because
  the database generator and constraint layer need them directly.
- **Core metadata typing** for W/E/M/I ids and agent-credit roles lives in
  `LiuXin_alpha.metadata.metadata_types`.
- **Shared metadata-family vocabularies** for additional metadata containers
  live in `LiuXin_alpha.metadata.constants.container_vocabularies`.

## Current canonical homes

### `LiuXin_alpha.databases.db_types`

- `IdentifierEntityType`
- `IdentifierScheme`
- MARC relator codes and their allowed sets

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
