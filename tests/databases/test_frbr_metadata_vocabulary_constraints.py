"""
Check that SQL vocabulary placeholders expand from canonical identifier and metadata enums.

These are string-content tests; they do not execute the expanded SQL or prove
constraint enforcement.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_frbr_metadata_vocabulary_constraints.py
"""

from __future__ import annotations

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen
from LiuXin_alpha.databases.db_types import IdentifierEntityType, IdentifierScheme
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


def test_identifier_placeholders_are_generated_from_db_types() -> None:
    """
    Require all entity and scheme strings in expanded identifier SQL and remove all three markers.

    Example:
        >>> test_identifier_placeholders_are_generated_from_db_types()


    :return: None; failed expectations raise AssertionError.
    """
    sql = frbr_gen._substitute_canonical_vocabulary_placeholders(
        "\n".join(
            [
                "__ENTITY_IDENTIFIER_ENTITY_TYPE_CHECK__",
                "__ENTITY_IDENTIFIER_SCHEME_BY_TYPE_CHECK__",
                "__ITEM_IDENTIFIER_SCHEME_CHECK__",
            ]
        )
    )

    for entity_type in IdentifierEntityType:
        assert entity_type.value in sql

    for scheme in IdentifierScheme:
        assert scheme.value in sql

    assert "__ENTITY_IDENTIFIER_ENTITY_TYPE_CHECK__" not in sql
    assert "__ENTITY_IDENTIFIER_SCHEME_BY_TYPE_CHECK__" not in sql
    assert "__ITEM_IDENTIFIER_SCHEME_CHECK__" not in sql


def test_future_metadata_family_placeholders_are_ready_for_schema_use() -> None:
    """
    Expand eight metadata-family markers and require every corresponding enum value.

    Checks title, note, label, genre, subject and identifier-status vocabularies by
    substring presence.

    Example:
        >>> test_future_metadata_family_placeholders_are_ready_for_schema_use()


    :return: None; failed expectations raise AssertionError.
    """
    sql = frbr_gen._substitute_canonical_vocabulary_placeholders(
        "\n".join(
            [
                "__TITLE_KIND_CHECK__",
                "__NOTE_KIND_CHECK__",
                "__NOTE_FORMAT_CHECK__",
                "__NOTE_VISIBILITY_CHECK__",
                "__LABEL_KIND_CHECK__",
                "__GENRE_KIND_CHECK__",
                "__SUBJECT_KIND_CHECK__",
                "__IDENTIFIER_STATUS_CHECK__",
            ]
        )
    )

    for enum_cls in (
        TitleKind,
        NoteKind,
        NoteFormat,
        NoteVisibility,
        LabelKind,
        GenreKind,
        SubjectKind,
        IdentifierStatus,
    ):
        for member in enum_cls:
            assert member.value in sql

    for placeholder in (
        "__TITLE_KIND_CHECK__",
        "__NOTE_KIND_CHECK__",
        "__NOTE_FORMAT_CHECK__",
        "__NOTE_VISIBILITY_CHECK__",
        "__LABEL_KIND_CHECK__",
        "__GENRE_KIND_CHECK__",
        "__SUBJECT_KIND_CHECK__",
        "__IDENTIFIER_STATUS_CHECK__",
    ):
        assert placeholder not in sql
