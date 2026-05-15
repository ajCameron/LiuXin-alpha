from __future__ import annotations

import importlib

from LiuXin_alpha.metadata.api import (
    GenreRowAPI,
    GenreTreeRelationAPI,
    GenreTreeRelationsContainerAPI,
    LanguageRowAPI,
    MetadataTableRowAPI,
)
from LiuXin_alpha.metadata.containers import (
    GenreRow,
    GenreTreeRelation,
    GenreTreeRelationsContainer,
    LanguageRow,
)


def test_non_wemi_api_exports_from_public_metadata_api() -> None:
    api_module = importlib.import_module("LiuXin_alpha.metadata.api")

    for expected_name in (
        "AnnotationRowAPI",
        "CommentRowAPI",
        "EntityIdentifierRowAPI",
        "GenreRowAPI",
        "GenreTreeRelationAPI",
        "GenreTreeRelationsContainerAPI",
        "HumanAgentRowAPI",
        "InlineSelfRelationAPI",
        "LabelRowAPI",
        "LanguageRowAPI",
        "MetadataRowMapping",
        "MetadataRowValue",
        "MetadataTableRowAPI",
        "NoteRowAPI",
        "ObservedItemIdentifierRowAPI",
        "OrgAgentRelationRowAPI",
        "OrgAgentRowAPI",
        "RatingRowAPI",
        "SelfRelationsContainerAPI",
        "SeriesRowAPI",
        "SeriesTreeRelationAPI",
        "SeriesTreeRelationsContainerAPI",
        "SubjectRowAPI",
        "SubjectTreeRelationAPI",
        "SubjectTreeRelationsContainerAPI",
        "SynopsisRowAPI",
        "TagRowAPI",
    ):
        assert hasattr(api_module, expected_name)


def test_non_wemi_api_does_not_export_concrete_container_names() -> None:
    api_module = importlib.import_module("LiuXin_alpha.metadata.api")

    for concrete_name in (
        "GenreRow",
        "GenreTreeRelation",
        "GenreTreeRelationsContainer",
        "LanguageRow",
        "MetadataTableRow",
    ):
        assert not hasattr(api_module, concrete_name)


def test_non_wemi_concrete_rows_satisfy_api_protocols() -> None:
    language = LanguageRow(language_id=1, language="English", language_code="eng")
    genre = GenreRow(genre_id=2, genre="Fiction")

    assert isinstance(language, MetadataTableRowAPI)
    assert isinstance(language, LanguageRowAPI)
    assert isinstance(genre, GenreRowAPI)


def test_non_wemi_concrete_self_relations_satisfy_api_protocols() -> None:
    parent = GenreRow(genre_id=1, genre="Fiction")
    child = GenreRow(genre_id=2, genre="Fantasy", genre_parent_id=1)
    relation_link = GenreTreeRelation.from_child_row(child, parent=parent)
    container = GenreTreeRelationsContainer()
    container.add_relation(relation_link)

    assert isinstance(relation_link, GenreTreeRelationAPI)
    assert isinstance(container, GenreTreeRelationsContainerAPI)
