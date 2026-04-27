"""Tests for FRBR generator alignment with canonical metadata vocabularies."""

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
