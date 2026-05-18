from __future__ import annotations

import pytest

from LiuXin_alpha.databases.db_types import IdentifierScheme
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api.containers_api import (
    ExpressionRelationLink,
    ManifestationRelationLink,
)
from LiuXin_alpha.metadata.constants.container_vocabularies import (
    DateKind,
    IdentifierStatus,
    LabelKind,
    LanguageKind,
    NoteFormat,
    NoteKind,
    NoteVisibility,
    RatingKind,
    ResourceKind,
    SeriesKind,
    SubjectKind,
    TitleKind,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers import (
    ExpressionAgentCredit,
    ExpressionAgentCreditsContainer,
    ExpressionDate,
    ExpressionDatesContainer,
    ExpressionIdentifier,
    ExpressionIdentifiersContainer,
    ExpressionIdentity,
    ExpressionLabel,
    ExpressionLabelsContainer,
    ExpressionLanguage,
    ExpressionLanguagesContainer,
    ExpressionMetadata,
    ExpressionNote,
    ExpressionNotesContainer,
    ExpressionRating,
    ExpressionRatingsContainer,
    ExpressionResource,
    ExpressionResourcesContainer,
    ExpressionSeriesEntriesContainer,
    ExpressionSeriesEntry,
    ExpressionSubject,
    ExpressionSubjectsContainer,
    ExpressionTitle,
    ExpressionTitlesContainer,
    ManifestationIdentity,
    ManifestationMetadata,
    WorkDate,
    WorkLabel,
    WorkLanguage,
    WorkNote,
    WorkRating,
    WorkResource,
    WorkSeriesEntry,
    WorkSubject,
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


def test_expression_note_and_label_containers_validate_unicode_shape() -> None:
    notes = ExpressionNotesContainer(expression_id=42)
    descriptions = notes.ensure_kind(NoteKind.DESCRIPTION)
    primary_note = ExpressionNote(
        note_kind=NoteKind.DESCRIPTION,
        body="\u7ffb\u8a33\u30ce\u30fc\u30c8: Les Mis\u00e9rables",
        body_format=NoteFormat.MARKDOWN,
        title="\u7ffb\u8a33\u8005\u6ce8",
        expression_id=42,
        language_id=2,
        visibility=NoteVisibility.PUBLIC,
        association_start_ep_k=10,
        association_end_ep_k=20,
    )
    secondary_note = ExpressionNote(
        note_kind=NoteKind.DESCRIPTION,
        body="\u7b80\u4f53\u4e2d\u6587 summary note",
        expression_id=42,
    )

    descriptions.add_note(secondary_note)
    descriptions.add_note(primary_note)
    descriptions.set_primary(1)
    descriptions.move_note(1, 0)
    notes.validate()

    assert descriptions.bodies() == (
        "\u7ffb\u8a33\u30ce\u30fc\u30c8: Les Mis\u00e9rables",
        "\u7b80\u4f53\u4e2d\u6587 summary note",
    )
    assert [note.position for note in descriptions] == [0, 1]
    assert notes.primary_notes()[NoteKind.DESCRIPTION] is primary_note
    assert notes.descriptions_text == (
        "\u7ffb\u8a33\u30ce\u30fc\u30c8: Les Mis\u00e9rables\n\n"
        "\u7b80\u4f53\u4e2d\u6587 summary note"
    )
    assert notes.as_write_payload()[0]["body_format"] == NoteFormat.MARKDOWN
    assert notes.as_write_payload()[0]["visibility"] == NoteVisibility.PUBLIC

    labels = ExpressionLabelsContainer(expression_id=42)
    tags = labels.ensure_kind(LabelKind.TAG)
    primary_label = ExpressionLabel(
        label_kind=LabelKind.TAG,
        text="\u5b87\u5b99\u6b4c\u5287",
        normalized_text="\u5b87\u5b99\u6b4c\u5287",
        sort_text="uchu-kageki",
        expression_id=42,
        language_id=2,
        authority_record_id=100,
        external_key="genre:space-opera",
    )
    secondary_label = ExpressionLabel(
        label_kind=LabelKind.TAG,
        text="space opera",
        expression_id=42,
    )

    labels.add_label(secondary_label)
    tags.add_label(primary_label)
    tags.set_primary(1)
    tags.move_label(1, 0)
    labels.validate()

    assert tags.texts() == ("\u5b87\u5b99\u6b4c\u5287", "space opera")
    assert labels.tags_text == "\u5b87\u5b99\u6b4c\u5287, space opera"
    assert labels.primary_label_for_kind(LabelKind.TAG) is primary_label
    assert labels.as_write_payload()[0]["external_key"] == "genre:space-opera"

    with pytest.raises(ValueError, match="Cannot add work note"):
        descriptions.add_note(
            WorkNote(
                note_kind=NoteKind.DESCRIPTION,
                body="Wrong target kind",
                work_id=42,
            )
        )
    with pytest.raises(ValueError, match="body cannot be blank"):
        ExpressionNote(
            note_kind=NoteKind.DESCRIPTION,
            body=" ",
            expression_id=42,
        ).validate()
    with pytest.raises(ValueError, match="association_end_ep_k cannot be earlier"):
        ExpressionNote(
            note_kind=NoteKind.DESCRIPTION,
            body="bad range",
            expression_id=42,
            association_start_ep_k=20,
            association_end_ep_k=10,
        ).validate()
    with pytest.raises(ValueError, match="Cannot add work label"):
        tags.add_label(
            WorkLabel(
                label_kind=LabelKind.TAG,
                text="Wrong target kind",
                work_id=42,
            )
        )
    with pytest.raises(ValueError, match="text cannot be blank"):
        ExpressionLabel(
            label_kind=LabelKind.TAG,
            text=" ",
            expression_id=42,
        ).validate()

    primary_label.is_primary = True
    secondary_label.is_primary = True
    with pytest.raises(ValueError, match="Only one primary label"):
        labels.validate()


def test_expression_subject_and_language_containers_validate_unicode_shape() -> None:
    subjects = ExpressionSubjectsContainer(expression_id=42)
    topics = subjects.ensure_kind(SubjectKind.TOPIC)
    primary_subject = ExpressionSubject(
        subject_kind=SubjectKind.TOPIC,
        text="\u6642\u9593\u65c5\u884c",
        normalized_text="\u6642\u9593\u65c5\u884c",
        sort_text="jikan-ryoko",
        expression_id=42,
        language_id=2,
        external_key="topic:time-travel",
    )
    secondary_subject = ExpressionSubject(
        subject_kind=SubjectKind.TOPIC,
        text="time travel",
        expression_id=42,
    )

    topics.add_subject(secondary_subject)
    topics.add_subject(primary_subject)
    topics.set_primary(1)
    topics.move_subject(1, 0)
    subjects.validate()

    assert topics.texts() == ("\u6642\u9593\u65c5\u884c", "time travel")
    assert subjects.topics_text == "\u6642\u9593\u65c5\u884c, time travel"
    assert subjects.primary_subject_for_kind(SubjectKind.TOPIC) is primary_subject
    assert subjects.as_write_payload()[0]["external_key"] == "topic:time-travel"

    languages = ExpressionLanguagesContainer(expression_id=42)
    content = languages.ensure_kind(LanguageKind.CONTENT)
    primary_language = ExpressionLanguage(
        language_kind=LanguageKind.CONTENT,
        language_id=2,
        language_code="zh-Hans",
        language_name="\u7b80\u4f53\u4e2d\u6587",
        expression_id=42,
        applies_to_language_id=1,
    )
    secondary_language = ExpressionLanguage(
        language_kind=LanguageKind.CONTENT,
        language_code="ja",
        language_name="\u65e5\u672c\u8a9e",
        expression_id=42,
    )

    languages.add_language(secondary_language)
    content.add_language(primary_language)
    content.set_primary(1)
    content.move_language(1, 0)
    languages.validate()

    assert content.texts() == ("\u7b80\u4f53\u4e2d\u6587", "\u65e5\u672c\u8a9e")
    assert languages.content_languages_text == "\u7b80\u4f53\u4e2d\u6587, \u65e5\u672c\u8a9e"
    assert languages.as_write_payload()[0]["language_code"] == "zh-Hans"

    with pytest.raises(ValueError, match="Cannot add work subject"):
        topics.add_subject(
            WorkSubject(
                subject_kind=SubjectKind.TOPIC,
                text="Wrong target kind",
                work_id=42,
            )
        )
    with pytest.raises(ValueError, match="text cannot be blank"):
        ExpressionSubject(
            subject_kind=SubjectKind.TOPIC,
            text=" ",
            expression_id=42,
        ).validate()
    with pytest.raises(ValueError, match="Cannot add work language"):
        content.add_language(
            WorkLanguage(
                language_kind=LanguageKind.CONTENT,
                language_code="en",
                work_id=42,
            )
        )
    with pytest.raises(ValueError, match="language record must provide"):
        ExpressionLanguage(
            language_kind=LanguageKind.CONTENT,
            expression_id=42,
        ).validate()

    primary_language.is_primary = True
    secondary_language.is_primary = True
    with pytest.raises(ValueError, match="Only one primary language"):
        languages.validate()


def test_expression_series_and_rating_containers_validate_value_contracts() -> None:
    series = ExpressionSeriesEntriesContainer(expression_id=42)
    main_series = series.ensure_kind(SeriesKind.SERIES)
    primary_entry = ExpressionSeriesEntry(
        series_kind=SeriesKind.SERIES,
        name="\u9280\u6cb3\u5e1d\u56fd",
        sort_name="ginga teikoku",
        numbering_text="\u7b2c\u4e8c\u90e8",
        position_in_series=2.0,
        language_id=2,
        authority_scheme="local",
        authority_identifier="series-2",
        expression_id=42,
    )
    secondary_entry = ExpressionSeriesEntry(
        series_kind=SeriesKind.SERIES,
        name="Foundation",
        position_in_series=1.5,
        expression_id=42,
    )

    series.add_entry(secondary_entry)
    main_series.add_entry(primary_entry)
    main_series.set_primary(1)
    main_series.move_entry(1, 0)
    series.validate()

    assert main_series.texts() == (
        "\u9280\u6cb3\u5e1d\u56fd #\u7b2c\u4e8c\u90e8",
        "Foundation #1.5",
    )
    assert series.series_entries_text == (
        "\u9280\u6cb3\u5e1d\u56fd #\u7b2c\u4e8c\u90e8; Foundation #1.5"
    )
    assert series.as_write_payload()[0]["authority_identifier"] == "series-2"

    ratings = ExpressionRatingsContainer(expression_id=42)
    overall = ratings.ensure_kind(RatingKind.OVERALL)
    primary_rating = ExpressionRating(
        rating_kind=RatingKind.OVERALL,
        value=4.5,
        scale_max=5.0,
        scale_min=0.0,
        normalized_value=0.9,
        agency="\u8a55\u8ad6\u793e",
        expression_id=42,
    )
    secondary_rating = ExpressionRating(
        rating_kind=RatingKind.OVERALL,
        value=8,
        scale_max=10,
        expression_id=42,
    )

    ratings.add_rating(secondary_rating)
    overall.add_rating(primary_rating)
    overall.set_primary(1)
    overall.move_rating(1, 0)
    ratings.validate()

    assert overall.texts() == ("4.5/5", "8/10")
    assert ratings.overall_ratings_text == "4.5/5, 8/10"
    assert ratings.as_write_payload()[0]["normalized_value"] == 0.9

    with pytest.raises(ValueError, match="Cannot add work series entry"):
        main_series.add_entry(
            WorkSeriesEntry(
                series_kind=SeriesKind.SERIES,
                name="Wrong target kind",
                work_id=42,
            )
        )
    with pytest.raises(ValueError, match="authority_scheme and authority_identifier"):
        ExpressionSeriesEntry(
            series_kind=SeriesKind.SERIES,
            name="Partial authority",
            expression_id=42,
            authority_scheme="local",
        ).validate()
    with pytest.raises(ValueError, match="Cannot add work rating"):
        overall.add_rating(
            WorkRating(
                rating_kind=RatingKind.OVERALL,
                value=4,
                work_id=42,
            )
        )
    with pytest.raises(ValueError, match="value must lie within"):
        ExpressionRating(
            rating_kind=RatingKind.OVERALL,
            value=6,
            scale_max=5,
            expression_id=42,
        ).validate()
    with pytest.raises(ValueError, match="normalized_value must be between"):
        ExpressionRating(
            rating_kind=RatingKind.OVERALL,
            value=4,
            normalized_value=1.2,
            expression_id=42,
        ).validate()

    primary_rating.is_primary = True
    secondary_rating.is_primary = True
    with pytest.raises(ValueError, match="Only one primary rating"):
        ratings.validate()


def test_expression_resource_and_date_containers_validate_value_contracts() -> None:
    resources = ExpressionResourcesContainer(expression_id=42)
    authority = resources.ensure_kind(ResourceKind.AUTHORITY)
    primary_resource = ExpressionResource(
        resource_kind=ResourceKind.AUTHORITY,
        uri="https://example.invalid/\u4f5c\u54c1/\u4e09\u4f53",
        label="\u6a29\u5a01\u30ec\u30b3\u30fc\u30c9",
        mime_type="text/html",
        access_note="\u516c\u958b",
        expression_id=42,
        is_public=True,
    )
    secondary_resource = ExpressionResource(
        resource_kind=ResourceKind.AUTHORITY,
        uri="urn:example:authority:san-ti",
        expression_id=42,
    )

    resources.add_resource(secondary_resource)
    authority.add_resource(primary_resource)
    authority.set_primary(1)
    authority.move_resource(1, 0)
    resources.validate()

    assert authority.texts() == (
        "\u6a29\u5a01\u30ec\u30b3\u30fc\u30c9",
        "urn:example:authority:san-ti",
    )
    assert resources.authority_resources_text == (
        "\u6a29\u5a01\u30ec\u30b3\u30fc\u30c9, urn:example:authority:san-ti"
    )
    assert resources.as_write_payload()[0]["mime_type"] == "text/html"

    dates = ExpressionDatesContainer(expression_id=42)
    published = dates.ensure_kind(DateKind.PUBLISHED)
    primary_date = ExpressionDate(
        date_kind=DateKind.PUBLISHED,
        start_ep_k=20200101,
        end_ep_k=20201231,
        display_text="\u4ee4\u548c2\u5e74",
        calendar="gregorian",
        expression_id=42,
        applies_to_language_id=2,
    )
    secondary_date = ExpressionDate(
        date_kind=DateKind.PUBLISHED,
        start_ep_k=20190101,
        end_ep_k=20191231,
        expression_id=42,
    )

    dates.add_date(secondary_date)
    published.add_date(primary_date)
    published.set_primary(1)
    published.move_date(1, 0)
    dates.validate()

    assert published.texts() == ("\u4ee4\u548c2\u5e74", "20190101\u201320191231")
    assert dates.published_dates_text == "\u4ee4\u548c2\u5e74; 20190101\u201320191231"
    assert dates.as_write_payload()[0]["calendar"] == "gregorian"

    with pytest.raises(ValueError, match="Cannot add work resource"):
        authority.add_resource(
            WorkResource(
                resource_kind=ResourceKind.AUTHORITY,
                uri="https://example.invalid/wrong-target",
                work_id=42,
            )
        )
    with pytest.raises(ValueError, match="uri cannot be blank"):
        ExpressionResource(
            resource_kind=ResourceKind.AUTHORITY,
            uri=" ",
            expression_id=42,
        ).validate()
    with pytest.raises(ValueError, match="Cannot add work date"):
        published.add_date(
            WorkDate(
                date_kind=DateKind.PUBLISHED,
                display_text="wrong target",
                work_id=42,
            )
        )
    with pytest.raises(ValueError, match="date record must provide"):
        ExpressionDate(
            date_kind=DateKind.PUBLISHED,
            expression_id=42,
        ).validate()
    with pytest.raises(ValueError, match="end_ep_k cannot be earlier"):
        ExpressionDate(
            date_kind=DateKind.PUBLISHED,
            start_ep_k=20200101,
            end_ep_k=20190101,
            expression_id=42,
        ).validate()

    primary_date.is_primary = True
    secondary_date.is_primary = True
    with pytest.raises(ValueError, match="Only one primary date"):
        dates.validate()
