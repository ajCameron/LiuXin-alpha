from __future__ import annotations

import pytest

from LiuXin_alpha.databases.db_types import IdentifierScheme
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api.containers_api import (
    ExpressionRelationLink,
    ManifestationRelationLink,
)
from LiuXin_alpha.metadata.constants.container_vocabularies import (
    IdentifierStatus,
    TitleKind,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers import (
    ExpressionAgentCredit,
    ExpressionAgentCreditsContainer,
    ExpressionIdentifier,
    ExpressionIdentifiersContainer,
    ExpressionIdentity,
    ExpressionMetadata,
    ExpressionTitle,
    ExpressionTitlesContainer,
    ManifestationIdentity,
    ManifestationMetadata,
    WorkTitle,
)
from LiuXin_alpha.metadata.metadata_types import ExpressionAgentRole


class _RowDriver:
    def identify_table_from_row_dict(self, row_dict: dict[str, object]) -> str:
        if "work_id" in row_dict:
            return "works"
        if "expression_id" in row_dict:
            return "expressions"
        if "manifestation_id" in row_dict:
            return "manifestations"
        if "item_id" in row_dict:
            return "items"
        raise AssertionError(f"unexpected row payload: {row_dict!r}")

    def get_allowed_tables_snapshot(self) -> tuple[str, ...]:
        return ("works", "expressions", "manifestations", "items")

    def get_id_column(self, table: str) -> str:
        return {
            "works": "work_id",
            "expressions": "expression_id",
            "manifestations": "manifestation_id",
            "items": "item_id",
        }[table]

    def check_for_intralink_table(self, _table: str) -> bool:
        return False

    def get_interlinked_tables(self, _table: str) -> tuple[str, ...]:
        return tuple()


class _RowDatabase:
    driver_wrapper = _RowDriver()

    def get_column_headings(self, table: str) -> set[str]:
        return {
            "works": {"work_id", "work_title"},
            "expressions": {"expression_id", "expression_label"},
            "manifestations": {"manifestation_id", "manifestation_format_detail"},
            "items": {"item_id", "item_source_name"},
        }[table]


def _row(row_dict: dict[str, object]) -> Row:
    return Row(database=_RowDatabase(), row_dict=row_dict)


def test_expression_and_manifestation_mapping_serializes_row_targets() -> None:
    work_row = _row(
        {
            "work_id": 7,
            "work_title": "\u4e09\u4f53",
        }
    )
    expression = ExpressionMetadata(
        expression=ExpressionIdentity(
            expression_id=42,
            expression_work_id=7,
            expression_label="\u4e2d\u6587\u7b80\u4f53",
        )
    )
    expression.add_relation_link(
        "work",
        ExpressionRelationLink(
            target=work_row,
            primary=True,
            priority=3,
            index=2,
            link_id="expression-work-7",
            type="translation-base",
            origin="fixture",
            data="\u539f\u6587",
            extra={"confidence": 0.95},
        ),
    )

    payload = expression.to_mapping()
    link_payload = payload["relations"]["works"][0]

    assert link_payload["target"] == {"work_id": 7, "work_title": "\u4e09\u4f53"}
    assert not isinstance(link_payload["target"], Row)
    assert link_payload["link_id"] == "expression-work-7"
    assert link_payload["extra"] == {"confidence": 0.95}

    hydrated = ExpressionMetadata.from_mapping(payload)
    link = hydrated.get_relation_links("works")[0]
    assert link.target == {"work_id": 7, "work_title": "\u4e09\u4f53"}
    assert link.primary is True
    assert link.priority == 3
    assert link.index == 2

    expression_row = _row(
        {
            "expression_id": 42,
            "expression_label": "\u4e2d\u6587\u7b80\u4f53",
        }
    )
    manifestation = ManifestationMetadata(
        manifestation=ManifestationIdentity(
            manifestation_id=77,
            manifestation_expression_id=42,
            manifestation_format_detail="EPUB",
        )
    )
    manifestation.add_relation_link(
        "expressions",
        ManifestationRelationLink(
            target=expression_row,
            primary=True,
            link_id="manifestation-expression-42",
        ),
    )

    manifestation_payload = manifestation.to_mapping()
    manifestation_link = manifestation_payload["relations"]["expressions"][0]
    assert manifestation_link["target"] == {
        "expression_id": 42,
        "expression_label": "\u4e2d\u6587\u7b80\u4f53",
    }
    assert not isinstance(manifestation_link["target"], Row)


def test_expression_relation_helpers_preserve_primary_and_alias_contracts() -> None:
    metadata = ExpressionMetadata()
    first = ExpressionRelationLink(
        target={"work_id": 1, "work_title": "Fallback"},
        priority=10,
        index=1,
    )
    selected = ExpressionRelationLink(
        target={"work_id": 2, "work_title": "\u4e3b\u8981\u4f5c\u54c1"},
        priority=1,
        index=9,
        link_id=200,
    )

    assert metadata.validate_relation_name("creator") == "agents"
    assert metadata.validate_relation_name(" work ") == "works"

    metadata.add_relation_link("work", first)
    metadata.add_relation_link("works", selected)

    assert metadata.primary_relation_link("works") is selected
    assert metadata.primary_related("works") == {
        "work_id": 2,
        "work_title": "\u4e3b\u8981\u4f5c\u54c1",
    }
    assert metadata.get_all_related()["works"] == [first.target, selected.target]
    assert metadata.get_relation_link_by_id("works", 200) is selected

    metadata.set_primary_relation_link("works", first)

    assert first.primary is True
    assert selected.primary is False
    assert metadata.primary_related("works") == first.target
    assert metadata.remove_relation_link_by_id("works", 200) is True
    assert metadata.get_related("works") == [first.target]

    with pytest.raises(KeyError, match="Unknown expression-metadata relation key"):
        metadata.validate_relation_name("not-a-real-relation")


def test_expression_title_container_handles_unicode_order_and_validation() -> None:
    titles = ExpressionTitlesContainer(expression_id=42)
    main = titles.ensure_kind(TitleKind.MAIN)
    first = ExpressionTitle(
        title_kind=TitleKind.MAIN,
        text="\u4e09\u4f53",
        normalized_text="\u4e09\u4f53",
        sort_text="San Ti",
        expression_id=42,
        language_id=1,
        script_code="Hans",
        source="fixture",
    )
    second = ExpressionTitle(
        title_kind=TitleKind.MAIN,
        text="\u306d\u3058\u307e\u304d\u9ce5\u30af\u30ed\u30cb\u30af\u30eb",
        normalized_text="Nejimakidori Kuronikuru",
        expression_id=42,
        language_id=2,
        script_code="Jpan",
    )
    translated = ExpressionTitle(
        title_kind=TitleKind.TRANSLATED,
        text="Les Mis\u00e9rables - \u60b2\u60e8\u4e16\u754c",
        expression_id=42,
        language_id=3,
        applies_to_language_id=1,
    )

    main.add_title(first)
    main.add_title(second)
    titles.add_title(translated)
    main.set_primary(1)
    main.move_title(1, 0)
    titles.validate()

    assert main.texts() == (
        "\u306d\u3058\u307e\u304d\u9ce5\u30af\u30ed\u30cb\u30af\u30eb",
        "\u4e09\u4f53",
    )
    assert [title.position for title in main] == [0, 1]
    assert titles.display_title == (
        "\u306d\u3058\u307e\u304d\u9ce5\u30af\u30ed\u30cb\u30af\u30eb"
    )
    assert titles.sort_title == "Nejimakidori Kuronikuru"
    assert titles.kind_text(TitleKind.TRANSLATED) == (
        "Les Mis\u00e9rables - \u60b2\u60e8\u4e16\u754c"
    )
    assert titles.as_write_payload()[0]["script_code"] == "Jpan"

    with pytest.raises(ValueError, match="target_id 99 does not match"):
        titles.add_title(
            ExpressionTitle(
                title_kind=TitleKind.MAIN,
                text="Wrong expression",
                expression_id=99,
            )
        )
    with pytest.raises(ValueError, match="Cannot add work title"):
        main.add_title(
            WorkTitle(
                title_kind=TitleKind.MAIN,
                text="Wrong target kind",
                work_id=42,
            )
        )
    with pytest.raises(ValueError, match="text cannot be blank"):
        ExpressionTitle(
            title_kind=TitleKind.MAIN,
            text=" ",
            expression_id=42,
        ).validate()

    first.is_primary = True
    second.is_primary = True
    with pytest.raises(ValueError, match="Only one primary title"):
        titles.validate()


def test_expression_identifier_container_enforces_scheme_and_shape() -> None:
    identifiers = ExpressionIdentifiersContainer(expression_id=42)
    primary = ExpressionIdentifier(
        scheme=IdentifierScheme.URI,
        value="https://example.invalid/\u66f8\u7c4d/\u4e09\u4f53",
        normalized_value="https://example.invalid/books/san-ti",
        expression_id=42,
        is_primary=True,
        status=IdentifierStatus.ACTIVE,
        source="fixture",
    )
    secondary = ExpressionIdentifier(
        scheme=IdentifierScheme.URI,
        value="urn:liuxin:\u4e09\u4f53",
        expression_id=42,
        status=IdentifierStatus.SUPERSEDED,
        notes="\u65e7\u8b58\u5225\u5b50",
    )

    identifiers.add_identifier(primary)
    identifiers.add_identifier(secondary)
    identifiers.ensure_scheme(IdentifierScheme.URI).move_identifier(1, 0)
    identifiers.validate()

    assert identifiers.schemes() == (IdentifierScheme.URI,)
    assert identifiers.scheme_values(IdentifierScheme.URI) == (
        "urn:liuxin:\u4e09\u4f53",
        "https://example.invalid/\u66f8\u7c4d/\u4e09\u4f53",
    )
    assert identifiers.scheme_normalized_values(IdentifierScheme.URI) == (
        "urn:liuxin:\u4e09\u4f53",
        "https://example.invalid/books/san-ti",
    )
    assert identifiers.primary_identifier_for_scheme(IdentifierScheme.URI) is primary
    assert identifiers.as_write_payload()[0]["status"] == IdentifierStatus.SUPERSEDED

    with pytest.raises(ValueError, match="not allowed for expression"):
        identifiers.ensure_scheme(IdentifierScheme.ISBN_13)
    with pytest.raises(ValueError, match="target_id 99 does not match"):
        identifiers.add_identifier(
            ExpressionIdentifier(
                scheme=IdentifierScheme.URI,
                value="urn:wrong-target",
                expression_id=99,
            )
        )
    with pytest.raises(ValueError, match="value cannot be blank"):
        ExpressionIdentifier(
            scheme=IdentifierScheme.URI,
            value=" ",
            expression_id=42,
        ).validate()

    primary.is_primary = True
    secondary.is_primary = True
    with pytest.raises(ValueError, match="Only one primary identifier"):
        identifiers.validate()


def test_expression_agent_credit_container_validates_roles_and_unicode() -> None:
    credits = ExpressionAgentCreditsContainer(expression_id=42)
    translator = ExpressionAgentCredit(
        agent_id=10,
        credited_as="\u5218\u6148\u6b23",
        sort_as="Liu, Cixin",
        expression_id=42,
        role=ExpressionAgentRole.TRANSLATOR,
        language_id=1,
        confidence=0.98,
        is_primary=True,
        notes="\u7b80\u4f53\u4e2d\u6587",
    )
    narrator = ExpressionAgentCredit(
        agent_id=11,
        credited_as="\u5c71\u7530\u592a\u90ce",
        expression_id=42,
        role=ExpressionAgentRole.NARRATOR,
        confidence=0.75,
    )

    credits.add_credit(translator)
    credits.add_credit(narrator)
    credits.validate()

    assert credits.roles() == (
        ExpressionAgentRole.TRANSLATOR,
        ExpressionAgentRole.NARRATOR,
    )
    assert credits.role_text(ExpressionAgentRole.TRANSLATOR) == "\u5218\u6148\u6b23"
    assert credits.all_agent_ids() == {10, 11}
    assert credits.translators_text == "\u5218\u6148\u6b23"
    assert credits.as_write_payload()[0]["language_id"] == 1
    assert credits.get_role(ExpressionAgentRole.NARRATOR).remove_agent(11) == 1
    assert credits.role_ids(ExpressionAgentRole.NARRATOR) == tuple()

    with pytest.raises(ValueError, match="translator credits should carry"):
        ExpressionAgentCredit(
            agent_id=12,
            credited_as="No language",
            expression_id=42,
            role=ExpressionAgentRole.TRANSLATOR,
        ).validate()
    with pytest.raises(ValueError, match="confidence must be between"):
        ExpressionAgentCredit(
            agent_id=12,
            credited_as="Bad confidence",
            expression_id=42,
            role=ExpressionAgentRole.EDITOR,
            confidence=1.5,
        ).validate()
    with pytest.raises(ValueError, match="target_id 99 does not match"):
        credits.add_credit(
            ExpressionAgentCredit(
                agent_id=12,
                credited_as="Wrong target",
                expression_id=99,
                role=ExpressionAgentRole.EDITOR,
            )
        )
