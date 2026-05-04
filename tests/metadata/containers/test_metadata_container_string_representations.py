from __future__ import annotations

from LiuXin_alpha.databases.db_types import IdentifierScheme
from LiuXin_alpha.metadata.constants.container_vocabularies import (
    DateKind,
    GenreKind,
    LabelKind,
    LanguageKind,
    NoteKind,
    RatingKind,
    ResourceKind,
    SeriesKind,
    SubjectKind,
    TitleKind,
)
from LiuXin_alpha.metadata.containers.metadata_containers.non_wemi_containers import (
    GenreRow,
    GenreTreeRelation,
    GenreTreeRelationsContainer,
    LabelRow,
    TagRow,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers import (
    AgentIdentity,
    AgentProfile,
    WorkAgentCredit,
    WorkAgentCreditsContainer,
    WorkDate,
    WorkDatesContainer,
    WorkGenre,
    WorkGenresContainer,
    WorkIdentity,
    WorkIdentifier,
    WorkIdentifiersContainer,
    WorkLanguage,
    WorkLanguagesContainer,
    WorkLabel,
    WorkLabelsContainer,
    WorkMetadata,
    WorkNote,
    WorkNotesContainer,
    WorkRating,
    WorkRatingsContainer,
    WorkResource,
    WorkResourcesContainer,
    WorkSeriesEntriesContainer,
    WorkSeriesEntry,
    WorkSubject,
    WorkSubjectsContainer,
    WorkTitle,
    WorkTitlesContainer,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import (
    ExpressionIdentity,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import (
    ItemIdentity,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import (
    ManifestationIdentity,
)
from LiuXin_alpha.metadata.metadata_types import WorkAgentRole


def _assert_sane_string(value: object, *expected_parts: str) -> None:
    rendered = str(value)
    assert rendered
    assert " object at " not in rendered
    assert not rendered.startswith("<")
    for expected in expected_parts:
        assert expected in rendered


def test_row_and_relation_containers_have_sane_string_representations() -> None:
    label = LabelRow(label_id=7, label_text="Space Opera")
    tag = TagRow(tag_id=8, tag="Space Opera")
    parent = GenreRow(genre_id=1, genre="Speculative Fiction")
    child = GenreRow(genre_id=2, genre="Space Opera", genre_parent_id=1)
    relation = GenreTreeRelation(child=child, parent=parent)
    relations = GenreTreeRelationsContainer()
    relations.add_relation(relation)

    _assert_sane_string(label, "LabelRow", "label_id=7", "Space Opera")
    _assert_sane_string(tag, "TagRow", "tag_id=8", "Space Opera")
    _assert_sane_string(relation, "GenreTreeRelation", "child_id=2", "parent_id=1")
    _assert_sane_string(relations, "GenreTreeRelationsContainer", "1 relations")


def test_wemi_identity_and_bundle_containers_have_sane_string_representations() -> None:
    work = WorkTitle(title_kind=TitleKind.MAIN, text="The Book", work_id=1)
    identity = WorkMetadata(
        work=WorkIdentity(work_id=1, work_title="The Book", work_type="novel")
    )
    identity.add_related("titles", work)

    _assert_sane_string(WorkIdentity(work_id=1, work_title="The Book"), "WorkIdentity", "The Book")
    _assert_sane_string(
        ExpressionIdentity(expression_id=2, expression_work_id=1, expression_label="English text"),
        "ExpressionIdentity",
        "English text",
    )
    _assert_sane_string(
        ManifestationIdentity(manifestation_id=3, manifestation_format_detail="EPUB"),
        "ManifestationIdentity",
        "EPUB",
    )
    _assert_sane_string(
        ItemIdentity(item_id=4, item_manifestation_id=3, item_source_name="local.epub"),
        "ItemIdentity",
        "local.epub",
    )
    _assert_sane_string(
        AgentIdentity(agent_id=5, agent_display_name="Ada Example"),
        "AgentIdentity",
        "Ada Example",
    )
    _assert_sane_string(
        AgentProfile(agent=AgentIdentity(agent_id=5, agent_display_name="Ada Example")),
        "AgentProfile",
        "Ada Example",
    )
    _assert_sane_string(identity, "WorkMetadata", "The Book", "titles:1")


def test_additional_metadata_value_containers_have_sane_string_representations() -> None:
    examples = [
        WorkTitle(title_kind=TitleKind.MAIN, text="The Book", work_id=1),
        WorkLabel(label_kind=LabelKind.TAG, text="favourite", work_id=1),
        WorkGenre(text="Space opera", genre_kind=GenreKind.GENRE, work_id=1),
        WorkSubject(subject_kind=SubjectKind.TOPIC, text="Navigation", work_id=1),
        WorkIdentifier(scheme=IdentifierScheme.UUID, value="book-uuid", work_id=1),
        WorkLanguage(language_kind=LanguageKind.CONTENT, language_code="en", language_name="English", work_id=1),
        WorkDate(date_kind=DateKind.CREATED, display_text="1899", work_id=1),
        WorkRating(rating_kind=RatingKind.OVERALL, value=4.5, work_id=1),
        WorkSeriesEntry(series_kind=SeriesKind.SERIES, name="Chronicles", numbering_text="2", work_id=1),
        WorkResource(resource_kind=ResourceKind.FULL_TEXT, uri="https://example.invalid/book", label="Full text", work_id=1),
        WorkNote(note_kind=NoteKind.DESCRIPTION, body="A concise note.", work_id=1),
        WorkAgentCredit(agent_id=10, credited_as="Ada Example", work_id=1, role=WorkAgentRole.AUTHOR),
    ]

    for example in examples:
        _assert_sane_string(example, example.__class__.__name__)


def test_additional_metadata_group_containers_have_sane_string_representations() -> None:
    containers = []

    titles = WorkTitlesContainer(work_id=1)
    titles.add_title(WorkTitle(title_kind=TitleKind.MAIN, text="The Book", work_id=1))
    containers.append((titles, "1 titles", "The Book"))

    labels = WorkLabelsContainer(work_id=1)
    labels.add_label(WorkLabel(label_kind=LabelKind.TAG, text="favourite", work_id=1))
    containers.append((labels, "1 labels", "favourite"))

    genres = WorkGenresContainer(work_id=1)
    genres.add_genre(WorkGenre(text="Space opera", work_id=1))
    containers.append((genres, "1 genres", "Space opera"))

    subjects = WorkSubjectsContainer(work_id=1)
    subjects.add_subject(WorkSubject(subject_kind=SubjectKind.TOPIC, text="Navigation", work_id=1))
    containers.append((subjects, "1 subjects", "Navigation"))

    identifiers = WorkIdentifiersContainer(work_id=1)
    identifiers.add_identifier(WorkIdentifier(scheme=IdentifierScheme.UUID, value="book-uuid", work_id=1))
    containers.append((identifiers, "1 identifiers", "book-uuid"))

    languages = WorkLanguagesContainer(work_id=1)
    languages.add_language(
        WorkLanguage(language_kind=LanguageKind.CONTENT, language_code="en", language_name="English", work_id=1)
    )
    containers.append((languages, "1 languages", "English"))

    dates = WorkDatesContainer(work_id=1)
    dates.add_date(WorkDate(date_kind=DateKind.CREATED, display_text="1899", work_id=1))
    containers.append((dates, "1 dates", "1899"))

    ratings = WorkRatingsContainer(work_id=1)
    ratings.add_rating(WorkRating(rating_kind=RatingKind.OVERALL, value=4.5, work_id=1))
    containers.append((ratings, "1 ratings", "4.5"))

    series = WorkSeriesEntriesContainer(work_id=1)
    series.add_entry(WorkSeriesEntry(series_kind=SeriesKind.SERIES, name="Chronicles", numbering_text="2", work_id=1))
    containers.append((series, "1 series entries", "Chronicles #2"))

    resources = WorkResourcesContainer(work_id=1)
    resources.add_resource(
        WorkResource(resource_kind=ResourceKind.FULL_TEXT, uri="https://example.invalid/book", label="Full text", work_id=1)
    )
    containers.append((resources, "1 resources", "Full text"))

    notes = WorkNotesContainer(work_id=1)
    notes.add_note(WorkNote(note_kind=NoteKind.DESCRIPTION, body="A concise note.", work_id=1))
    containers.append((notes, "1 notes", "A concise note."))

    credits = WorkAgentCreditsContainer(work_id=1)
    credits.add_credit(WorkAgentCredit(agent_id=10, credited_as="Ada Example", work_id=1, role=WorkAgentRole.AUTHOR))
    containers.append((credits, "1 credits", "Ada Example"))

    for container, count_text, display_text in containers:
        _assert_sane_string(container, container.__class__.__name__, count_text, display_text)
