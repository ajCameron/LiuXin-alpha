"""Render schema CHECK fragments from the canonical metadata vocabularies."""

from LiuXin_alpha.metadata.constants.container_vocabularies import (
    GenreKind,
    IdentifierStatus,
    LabelKind,
    NoteFormat,
    NoteKind,
    NoteVisibility,
    SubjectKind,
    TitleKind,
)
from LiuXin_alpha.metadata.identifiers import (
    ALL_IDENTIFIER_ENTITY_TYPES,
    ENTITY_IDENTIFIER_SCHEMES_BY_TYPE,
    OBSERVED_ITEM_IDENTIFIER_SCHEMES,
    IdentifierEntityType,
)


def _sql_quote_literal(value: str) -> str:
    """
    Surround text with apostrophes and double embedded apostrophes.

    Example:
        >>> _sql_quote_literal("author")
        "'author'"


    :param value: String to embed in controlled generated SQL.
    :return: Escaped SQL text literal.
    """
    return "'" + value.replace("'", "''") + "'"


def _sql_in_list(values: list[str] | tuple[str, ...] | set[str]) -> str:
    """
    Sort and quote strings for a comma-separated SQL IN-list body.

    Example:
        >>> _sql_in_list(["b", "a"])
        "'a', 'b'"


    :param values: String collection; duplicates are retained for list/tuple inputs.
    :return: Joined quoted values without surrounding parentheses; empty input gives
        empty text.
    """
    return ", ".join(_sql_quote_literal(value) for value in sorted(values))


def _build_entity_identifier_type_check_sql() -> str:
    """
    Build a nullable CHECK constraint from the canonical identifier entity types.

    Example:
        The generated constraint accepts NULL or a known entity-type spelling.


    :return: Constraint fragment ending in a comma for insertion into a CREATE TABLE
        body.
    """
    allowed_entity_types = _sql_in_list(list(ALL_IDENTIFIER_ENTITY_TYPES))
    return f"""CONSTRAINT `entity_identifier_entity_type_valid`
    CHECK (
      `entity_identifier_entity_type` IS NULL
      OR `entity_identifier_entity_type` IN ({allowed_entity_types})
    ),"""


def _build_entity_identifier_scheme_check_sql() -> str:
    """
    Build the entity-type-dependent identifier scheme CHECK constraint.

    A null entity type or null scheme passes; otherwise a matching canonical pair is
    required.

    Example:
        An item-specific scheme must be paired with an entity type whose allowed set
        includes it.


    :return: Constraint fragment without a trailing comma.
    """
    clauses: list[str] = []

    for entity_type in IdentifierEntityType:
        allowed_schemes = ENTITY_IDENTIFIER_SCHEMES_BY_TYPE[entity_type]
        allowed_schemes_sql = _sql_in_list([scheme.value for scheme in allowed_schemes])
        clauses.append(
            f"(`entity_identifier_entity_type` = {_sql_quote_literal(entity_type.value)} "
            f"AND `entity_identifier_scheme` IN ({allowed_schemes_sql}))"
        )

    joined_clauses = "\n        OR ".join(clauses)

    return f"""CONSTRAINT `entity_identifier_scheme_valid_for_entity_type`
    CHECK (
      `entity_identifier_entity_type` IS NULL
      OR `entity_identifier_scheme` IS NULL
      OR (
        {joined_clauses}
      )
    )"""


def _build_observed_item_identifier_scheme_check_sql() -> str:
    """
    Build a nullable observed-item identifier scheme constraint.

    Example:
        The emitted IN-list uses OBSERVED_ITEM_IDENTIFIER_SCHEMES enum values.


    :return: CHECK fragment ending in a comma.
    """
    allowed_schemes_sql = _sql_in_list(
        [scheme.value for scheme in OBSERVED_ITEM_IDENTIFIER_SCHEMES]
    )
    return f"""CONSTRAINT `item_identifier_scheme_valid`
    CHECK (
      `item_identifier_scheme` IS NULL
      OR `item_identifier_scheme` IN ({allowed_schemes_sql})
    ),"""


def _build_simple_text_check_sql(
    *,
    constraint_name: str,
    column_name: str,
    allowed_values: list[str] | tuple[str, ...] | set[str],
) -> str:
    """
    Build a named nullable text CHECK from a sorted canonical vocabulary.

    Example:
        A title-kind constraint accepts NULL or one of the supplied title-kind values.


    :param constraint_name: Trusted unescaped constraint identifier.
    :param column_name: Trusted unescaped column identifier.
    :param allowed_values: Canonical strings quoted into the IN-list.
    :return: CHECK fragment ending in a comma.
    """
    allowed_values_sql = _sql_in_list(allowed_values)
    return f"""CONSTRAINT `{constraint_name}`
    CHECK (
      `{column_name}` IS NULL
      OR `{column_name}` IN ({allowed_values_sql})
    ),"""


def _substitute_canonical_vocabulary_placeholders(sql_text: str) -> str:
    """
    Replace known identifier/metadata vocabulary markers throughout SQL text.

    Unknown markers are left untouched; replacements are textual and not SQL-aware.

    Example:
        SQL without any known marker passes through unchanged.


    :param sql_text: Packaged SQL containing supported placeholder tokens.
    :return: SQL text containing canonical CHECK fragments.
    """
    replacements = {
        "__ENTITY_IDENTIFIER_ENTITY_TYPE_CHECK__": _build_entity_identifier_type_check_sql(),
        "__ENTITY_IDENTIFIER_SCHEME_BY_TYPE_CHECK__": _build_entity_identifier_scheme_check_sql(),
        "__ITEM_IDENTIFIER_SCHEME_CHECK__": _build_observed_item_identifier_scheme_check_sql(),
        "__TITLE_KIND_CHECK__": _build_simple_text_check_sql(
            constraint_name="title_kind_valid",
            column_name="title_kind",
            allowed_values=tuple(member.value for member in TitleKind),
        ),
        "__NOTE_KIND_CHECK__": _build_simple_text_check_sql(
            constraint_name="note_kind_valid",
            column_name="note_kind",
            allowed_values=tuple(member.value for member in NoteKind),
        ),
        "__NOTE_FORMAT_CHECK__": _build_simple_text_check_sql(
            constraint_name="note_format_valid",
            column_name="note_format",
            allowed_values=tuple(member.value for member in NoteFormat),
        ),
        "__NOTE_VISIBILITY_CHECK__": _build_simple_text_check_sql(
            constraint_name="note_visibility_valid",
            column_name="note_visibility",
            allowed_values=tuple(member.value for member in NoteVisibility),
        ),
        "__LABEL_KIND_CHECK__": _build_simple_text_check_sql(
            constraint_name="label_kind_valid",
            column_name="label_kind",
            allowed_values=tuple(member.value for member in LabelKind),
        ),
        "__GENRE_KIND_CHECK__": _build_simple_text_check_sql(
            constraint_name="genre_kind_valid",
            column_name="genre_kind",
            allowed_values=tuple(member.value for member in GenreKind),
        ),
        "__SUBJECT_KIND_CHECK__": _build_simple_text_check_sql(
            constraint_name="subject_kind_valid",
            column_name="subject_kind",
            allowed_values=tuple(member.value for member in SubjectKind),
        ),
        "__IDENTIFIER_STATUS_CHECK__": _build_simple_text_check_sql(
            constraint_name="identifier_status_valid",
            column_name="identifier_status",
            allowed_values=tuple(member.value for member in IdentifierStatus),
        ),
    }

    for placeholder, replacement in replacements.items():
        sql_text = sql_text.replace(placeholder, replacement)

    return sql_text
