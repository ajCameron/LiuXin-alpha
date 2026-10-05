"""
Verify non-WEMI containers hydrate from their main-table row shapes.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test non wemi main table rows through its owning regression module::

        python -m pytest -q tests/metadata/containers/test_non_wemi_main_table_rows.py
"""
from __future__ import annotations

import importlib
import sqlite3
from dataclasses import fields
from typing import Type

import pytest

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import (
    database_generator as frbr_gen,
)
from LiuXin_alpha.metadata.containers import (
    NON_WEMI_MAIN_TABLE_ROW_CONTAINERS,
    NON_WEMI_SELF_RELATION_CONTAINERS,
    EntityIdentifierRow,
    GenreRow,
    GenreTreeRelation,
    GenreTreeRelationsContainer,
    LanguageRow,
    MetadataTableRow,
    RatingRow,
    SeriesRow,
    SeriesTreeRelation,
    SubjectRow,
    SubjectTreeRelation,
)


def test_non_wemi_row_container_mapping_round_trip() -> None:
    """
    Verify non wemi row container mapping round trip.

    Example:
        Exercise test non wemi row container mapping round trip through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_non_wemi_main_table_rows.py


    :return: None; the function records state or raises through its assertions.
    """
    language = LanguageRow.from_mapping(
        {
            "language_id": 1,
            "language": "English",
            "language_code": "eng",
            "language_bcp47_primary": "en",
            "ignored": "not a modelled column",
        }
    )

    assert language.primary_id == 1
    assert language.display_name == "English"
    assert language.to_mapping()["language_code"] == "eng"
    assert "ignored" not in language.to_mapping()

    genre = GenreRow(genre_id=12, genre="Science Fiction", genre_full="Fiction/Science Fiction")
    assert genre.primary_id == 12
    assert genre.to_mapping()["genre_full"] == "Fiction/Science Fiction"

    rating = RatingRow(rating_id=3, rating=4.5, rating_out_of=5, rating_source="manual")
    assert rating.to_mapping()["rating"] == 4.5

    identifier = EntityIdentifierRow.from_mapping(
        {
            "entity_identifier_id": 7,
            "entity_identifier_entity_type": "work",
            "entity_identifier_entity_id": 42,
            "entity_identifier_scheme": "uuid",
            "entity_identifier_value": "abc",
        }
    )
    assert identifier.primary_id == 7
    assert identifier.to_mapping()["entity_identifier_entity_type"] == "work"


def test_non_wemi_row_container_accepts_sqlite_row() -> None:
    """
    Verify non wemi row container accepts sqlite row.

    Example:
        Exercise test non wemi row container accepts sqlite row through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_non_wemi_main_table_rows.py


    :return: None; the function records state or raises through its assertions.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT 9 AS language_id, 'French' AS language, 'fre' AS language_code;"
        ).fetchone()
        language = LanguageRow.from_mapping(row)
    finally:
        conn.close()

    assert language.primary_id == 9
    assert language.display_name == "French"
    assert language.to_mapping()["language_code"] == "fre"


def test_non_wemi_self_relation_container_models_inline_tree_link() -> None:
    """
    Verify non wemi self relation container models inline tree link.

    Example:
        Exercise test non wemi self relation container models inline tree link through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_non_wemi_main_table_rows.py


    :return: None; the function records state or raises through its assertions.
    """
    parent = GenreRow(genre_id=1, genre="Fiction")
    child = GenreRow(
        genre_id=2,
        genre="Science Fiction",
        genre_parent_id=1,
        genre_position=0,
        genre_tree_id=1,
    )

    relation_link = GenreTreeRelation.from_child_row(child, parent=parent, source="manual")

    assert relation_link.child_id == 2
    assert relation_link.resolved_parent_id == 1
    assert relation_link.as_child_update_payload() == {
        "genre_parent_id": 1,
        "genre_position": 0,
        "genre_tree_id": 1,
    }
    assert relation_link.as_relation_payload() == {
        "relation_name": "genre_tree_parent",
        "table_name": "genres",
        "child_id": 2,
        "parent_id": 1,
        "position": 0,
        "tree_id": 1,
        "source": "manual",
    }

    container = GenreTreeRelationsContainer()
    container.add_relation(relation_link)

    assert container.children_of(1) == (relation_link,)
    assert container.roots() == ()


def test_non_wemi_self_relation_container_validates_shape_and_duplicate_children() -> None:
    """
    Verify non wemi self relation container validates shape and duplicate children.

    Example:
        Exercise test non wemi self relation container validates shape and duplicate children through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_non_wemi_main_table_rows.py


    :return: None; the function records state or raises through its assertions.
    """
    parent = GenreRow(genre_id=1, genre="Fiction")
    child = GenreRow(genre_id=2, genre="Science Fiction", genre_parent_id=1)
    relation_link = GenreTreeRelation(child=child, parent=parent)

    container = GenreTreeRelationsContainer()
    container.add_relation(relation_link)

    with pytest.raises(ValueError, match="Duplicate self-relation"):
        container.add_relation(GenreTreeRelation(child=child, parent=parent))

    with pytest.raises(ValueError, match="cannot relate a row to itself"):
        GenreTreeRelation(child=parent, parent=parent).validate()

    with pytest.raises(TypeError, match="child must be GenreRow"):
        GenreTreeRelation(child=SubjectRow(subject_id=10), parent=parent).validate()

    with pytest.raises(ValueError, match="parent id 1 does not match"):
        GenreTreeRelation(child=GenreRow(genre_id=3, genre_parent_id=99), parent=parent).validate()

    with pytest.raises(ValueError, match="position cannot be negative"):
        GenreTreeRelation(child=GenreRow(genre_id=4), position=-1).validate()


def test_non_wemi_self_relation_payloads_for_subject_and_series() -> None:
    """
    Verify non wemi self relation payloads for subject and series.

    Example:
        Exercise test non wemi self relation payloads for subject and series through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_non_wemi_main_table_rows.py


    :return: None; the function records state or raises through its assertions.
    """
    subject_relation = SubjectTreeRelation.from_child_row(
        SubjectRow(
            subject_id=2,
            subject_parent_id=1,
            subject_parent_position=5,
            subject_tree_id="subjects",
        )
    )
    assert subject_relation.as_child_update_payload() == {
        "subject_parent_id": 1,
        "subject_parent_position": 5,
        "subject_tree_id": "subjects",
    }

    series_relation = SeriesTreeRelation.from_child_row(
        SeriesRow(
            series_id=2,
            series_parent_id=1,
            series_parent_position=3,
            series_tree_id="main",
        )
    )
    assert series_relation.as_child_update_payload() == {
        "series_parent_id": 1,
        "series_parent_position": 3,
        "series_tree_id": "main",
    }


@pytest.fixture(scope="module")
def frbr_schema_conn() -> sqlite3.Connection:
    """
    Perform the frbr schema conn test-helper operation with deterministic inputs.

    Example:
        Exercise frbr schema conn through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_non_wemi_main_table_rows.py


    :return: The deterministic value, row, identity or collection described above.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON;")
    frbr_gen.create_new_database(conn)
    return conn


@pytest.mark.parametrize("container_cls", NON_WEMI_MAIN_TABLE_ROW_CONTAINERS)
def test_non_wemi_row_container_fields_match_schema(
    frbr_schema_conn: sqlite3.Connection,
    container_cls: Type[MetadataTableRow],
) -> None:
    """
    Verify non wemi row container fields match schema.

    Example:
        Exercise test non wemi row container fields match schema through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_non_wemi_main_table_rows.py


    :param frbr_schema_conn: Value supplied for frbr schema conn in the focused test
        operation.
    :param container_cls: Value supplied for container cls in the focused test
        operation.
    :return: None; the function records state or raises through its assertions.
    """
    actual_cols = {
        row[1]
        for row in frbr_schema_conn.execute(
            f"PRAGMA table_info(`{container_cls.TABLE_NAME}`);"
        ).fetchall()
    }
    modelled_cols = {field.name for field in fields(container_cls)}

    assert actual_cols == modelled_cols


@pytest.mark.parametrize("relation_cls", NON_WEMI_SELF_RELATION_CONTAINERS)
def test_non_wemi_self_relation_columns_exist_in_schema(
    frbr_schema_conn: sqlite3.Connection,
    relation_cls,
) -> None:
    """
    Verify non wemi self relation columns exist in schema.

    Example:
        Exercise test non wemi self relation columns exist in schema through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_non_wemi_main_table_rows.py


    :param frbr_schema_conn: Value supplied for frbr schema conn in the focused test
        operation.
    :param relation_cls: Value supplied for relation cls in the focused test operation.
    :return: None; the function records state or raises through its assertions.
    """
    actual_cols = {
        row[1]
        for row in frbr_schema_conn.execute(
            f"PRAGMA table_info(`{relation_cls.TABLE_NAME}`);"
        ).fetchall()
    }
    expected_cols = {
        relation_cls.CHILD_ID_COLUMN,
        relation_cls.PARENT_ID_COLUMN,
    }
    if relation_cls.POSITION_COLUMN is not None:
        expected_cols.add(relation_cls.POSITION_COLUMN)
    if relation_cls.TREE_ID_COLUMN is not None:
        expected_cols.add(relation_cls.TREE_ID_COLUMN)

    assert expected_cols <= actual_cols


def test_non_wemi_rows_export_from_concrete_surface_not_api() -> None:
    """
    Verify non wemi rows export from concrete surface not api.

    Example:
        Exercise test non wemi rows export from concrete surface not api through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_non_wemi_main_table_rows.py


    :return: None; the function records state or raises through its assertions.
    """
    expected_names = [
        "AnnotationRow",
        "CommentRow",
        "EntityIdentifierRow",
        "GenreRow",
        "GenreTreeRelation",
        "GenreTreeRelationsContainer",
        "HumanAgentRow",
        "InlineSelfRelation",
        "LabelRow",
        "LanguageRow",
        "MetadataTableRow",
        "NON_WEMI_MAIN_TABLE_ROW_CONTAINERS",
        "NON_WEMI_SELF_RELATION_CONTAINERS",
        "NoteRow",
        "ObservedItemIdentifierRow",
        "OrgAgentRelationRow",
        "OrgAgentRow",
        "RatingRow",
        "SeriesRow",
        "SeriesTreeRelation",
        "SeriesTreeRelationsContainer",
        "SelfRelationsContainer",
        "SubjectRow",
        "SubjectTreeRelation",
        "SubjectTreeRelationsContainer",
        "SynopsisRow",
        "TagRow",
    ]

    for module_name in (
        "LiuXin_alpha.metadata.containers",
        "LiuXin_alpha.metadata.containers.metadata_containers",
        "LiuXin_alpha.metadata.containers.metadata_containers.non_wemi_containers",
    ):
        module = importlib.import_module(module_name)
        for expected_name in expected_names:
            assert hasattr(module, expected_name), f"{module_name} is missing {expected_name}"

    api_module = importlib.import_module("LiuXin_alpha.metadata.api")
    for expected_name in expected_names:
        assert not hasattr(api_module, expected_name), (
            f"metadata.api should not export concrete {expected_name}"
        )


def test_non_wemi_main_table_rows_are_split_by_table_module() -> None:
    """
    Verify non wemi main table rows remain split by table module.

    Example:
        Exercise test non wemi main table rows are split by table module through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_non_wemi_main_table_rows.py


    :return: None; the function records state or raises through its assertions.
    """
    expected_modules = {
        "AnnotationRow": "annotation_row",
        "CommentRow": "comment_row",
        "EntityIdentifierRow": "entity_identifier_row",
        "GenreRow": "genre_row",
        "HumanAgentRow": "human_agent_row",
        "LabelRow": "label_row",
        "LanguageRow": "language_row",
        "NoteRow": "note_row",
        "ObservedItemIdentifierRow": "observed_item_identifier_row",
        "OrgAgentRelationRow": "org_agent_relation_row",
        "OrgAgentRow": "org_agent_row",
        "RatingRow": "rating_row",
        "SeriesRow": "series_row",
        "SubjectRow": "subject_row",
        "SynopsisRow": "synopsis_row",
        "TagRow": "tag_row",
    }

    for class_name, module_leaf in expected_modules.items():
        module = importlib.import_module(
            "LiuXin_alpha.metadata.containers.metadata_containers."
            f"non_wemi_containers.{module_leaf}"
        )
        row_class = getattr(module, class_name)
        assert row_class.__module__.endswith(f".{module_leaf}")

    compatibility_module = importlib.import_module(
        "LiuXin_alpha.metadata.containers.metadata_containers."
        "non_wemi_containers.main_table_rows"
    )
    for class_name in expected_modules:
        assert hasattr(compatibility_module, class_name)
