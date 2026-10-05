"""
Smoke-test construction and shared behavior across the metadata family.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test metadata family smoke through its owning regression module::

        python -m pytest -q tests/metadata/containers/test_metadata_family_smoke.py
"""
from __future__ import annotations

from LiuXin_alpha.databases.db_types import IdentifierScheme
from LiuXin_alpha.metadata.constants.container_vocabularies import (
    GenreKind,
    LabelKind,
    NoteKind,
    SubjectKind,
    TitleKind,
    LanguageKind,
    DateKind,
    RatingKind,
    SeriesKind,
    ResourceKind,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_credit_containers import (
    WorkAgentCredit,
    WorkAgentCreditsContainer,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_participation import (
    AgentParticipationEntry,
    AgentParticipationSnapshot,
    AgentProfileSummary,
    WorkSummary,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.genres_containers import (
    WorkGenre,
    WorkGenresContainer,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.identifier_containers import (
    WorkIdentifier,
    WorkIdentifiersContainer,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.languages_containers import (
    WorkLanguage,
    WorkLanguagesContainer,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.dates_containers import (
    WorkDate,
    WorkDatesContainer,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.ratings_containers import (
    WorkRating,
    WorkRatingsContainer,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.series_containers import (
    WorkSeriesEntry,
    WorkSeriesEntriesContainer,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.resources_containers import (
    WorkResource,
    WorkResourcesContainer,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.labels_containers import (
    WorkLabel,
    WorkLabelsContainer,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.notes_containers import (
    WorkNote,
    WorkNotesContainer,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.subjects_containers import (
    WorkSubject,
    WorkSubjectsContainer,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.titles_containers import (
    ItemTitlesContainer,
    ItemWemiTitleSlice,
    WorkTitle,
    WorkTitlesContainer,
)
from LiuXin_alpha.metadata.metadata_types import AgentTypes, CreditSource, WorkAgentRole


def test_titles_container_smoke_round_trip() -> None:
    """
    Verify titles container smoke round trip.

    Example:
        Exercise test titles container smoke round trip through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_family_smoke.py


    :return: None; the function records state or raises through its assertions.
    """
    title = WorkTitle(title_kind=TitleKind.MAIN, text="The Book", work_id=1)
    container = WorkTitlesContainer(work_id=1)
    container.add_title(title)
    container.validate()

    payload = container.as_write_payload()
    assert payload[0]["work_id"] == 1
    assert payload[0]["text"] == "The Book"
    assert container.kind_text(TitleKind.MAIN) == "The Book"


def test_notes_container_smoke_round_trip() -> None:
    """
    Verify notes container smoke round trip.

    Example:
        Exercise test notes container smoke round trip through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_family_smoke.py


    :return: None; the function records state or raises through its assertions.
    """
    note = WorkNote(note_kind=NoteKind.DESCRIPTION, body="A concise note.", work_id=1)
    container = WorkNotesContainer(work_id=1)
    container.add_note(note)
    container.validate()

    payload = container.as_write_payload()
    assert payload[0]["work_id"] == 1
    assert payload[0]["body"] == "A concise note."
    assert container.kind_text(NoteKind.DESCRIPTION) == "A concise note."


def test_labels_container_smoke_round_trip() -> None:
    """
    Verify labels container smoke round trip.

    Example:
        Exercise test labels container smoke round trip through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_family_smoke.py


    :return: None; the function records state or raises through its assertions.
    """
    label = WorkLabel(label_kind=LabelKind.TAG, text="favourite", work_id=1)
    container = WorkLabelsContainer(work_id=1)
    container.add_label(label)
    container.validate()

    payload = container.as_write_payload()
    assert payload[0]["work_id"] == 1
    assert payload[0]["text"] == "favourite"
    assert container.kind_text(LabelKind.TAG) == "favourite"


def test_genres_container_smoke_round_trip() -> None:
    """
    Verify genres container smoke round trip.

    Example:
        Exercise test genres container smoke round trip through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_family_smoke.py


    :return: None; the function records state or raises through its assertions.
    """
    genre = WorkGenre(text="Space opera", genre_kind=GenreKind.GENRE, work_id=1)
    container = WorkGenresContainer(work_id=1)
    container.add_genre(genre)
    container.validate()

    payload = container.as_write_payload()
    assert payload[0]["work_id"] == 1
    assert payload[0]["text"] == "Space opera"
    assert container.to_text() == "Space opera"


def test_subjects_container_smoke_round_trip() -> None:
    """
    Verify subjects container smoke round trip.

    Example:
        Exercise test subjects container smoke round trip through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_family_smoke.py


    :return: None; the function records state or raises through its assertions.
    """
    subject = WorkSubject(subject_kind=SubjectKind.TOPIC, text="Navigation", work_id=1)
    container = WorkSubjectsContainer(work_id=1)
    container.add_subject(subject)
    container.validate()

    payload = container.as_write_payload()
    assert payload[0]["work_id"] == 1
    assert payload[0]["text"] == "Navigation"
    assert container.kind_text(SubjectKind.TOPIC) == "Navigation"


def test_identifiers_container_smoke_round_trip() -> None:
    """
    Verify identifiers container smoke round trip.

    Example:
        Exercise test identifiers container smoke round trip through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_family_smoke.py


    :return: None; the function records state or raises through its assertions.
    """
    identifier = WorkIdentifier(
        scheme=IdentifierScheme.UUID,
        value="123e4567-e89b-12d3-a456-426614174000",
        work_id=1,
    )
    container = WorkIdentifiersContainer(work_id=1)
    container.add_identifier(identifier)
    container.validate()

    payload = container.as_write_payload()
    assert payload[0]["work_id"] == 1
    assert payload[0]["value"] == "123e4567-e89b-12d3-a456-426614174000"
    assert container.scheme_text(IdentifierScheme.UUID) == "123e4567-e89b-12d3-a456-426614174000"


def test_languages_container_smoke_round_trip() -> None:
    """
    Verify languages container smoke round trip.

    Example:
        Exercise test languages container smoke round trip through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_family_smoke.py


    :return: None; the function records state or raises through its assertions.
    """
    language = WorkLanguage(language_kind=LanguageKind.CONTENT, language_code="en", language_name="English", work_id=1)
    container = WorkLanguagesContainer(work_id=1)
    container.add_language(language)
    container.validate()

    payload = container.as_write_payload()
    assert payload[0]["work_id"] == 1
    assert payload[0]["language_code"] == "en"
    assert container.kind_text(LanguageKind.CONTENT) == "English"


def test_dates_container_smoke_round_trip() -> None:
    """
    Verify dates container smoke round trip.

    Example:
        Exercise test dates container smoke round trip through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_family_smoke.py


    :return: None; the function records state or raises through its assertions.
    """
    date = WorkDate(date_kind=DateKind.CREATED, display_text="1899", work_id=1)
    container = WorkDatesContainer(work_id=1)
    container.add_date(date)
    container.validate()

    payload = container.as_write_payload()
    assert payload[0]["work_id"] == 1
    assert payload[0]["display_text"] == "1899"
    assert container.kind_text(DateKind.CREATED) == "1899"


def test_ratings_container_smoke_round_trip() -> None:
    """
    Verify ratings container smoke round trip.

    Example:
        Exercise test ratings container smoke round trip through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_family_smoke.py


    :return: None; the function records state or raises through its assertions.
    """
    rating = WorkRating(rating_kind=RatingKind.OVERALL, value=4.5, scale_max=5.0, work_id=1)
    container = WorkRatingsContainer(work_id=1)
    container.add_rating(rating)
    container.validate()

    payload = container.as_write_payload()
    assert payload[0]["work_id"] == 1
    assert payload[0]["value"] == 4.5
    assert container.kind_text(RatingKind.OVERALL) == "4.5/5"


def test_series_entries_container_smoke_round_trip() -> None:
    """
    Verify series entries container smoke round trip.

    Example:
        Exercise test series entries container smoke round trip through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_family_smoke.py


    :return: None; the function records state or raises through its assertions.
    """
    entry = WorkSeriesEntry(series_kind=SeriesKind.SERIES, name="Chronicles", numbering_text="2", work_id=1)
    container = WorkSeriesEntriesContainer(work_id=1)
    container.add_entry(entry)
    container.validate()

    payload = container.as_write_payload()
    assert payload[0]["work_id"] == 1
    assert payload[0]["name"] == "Chronicles"
    assert container.kind_text(SeriesKind.SERIES) == "Chronicles #2"


def test_resources_container_smoke_round_trip() -> None:
    """
    Verify resources container smoke round trip.

    Example:
        Exercise test resources container smoke round trip through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_family_smoke.py


    :return: None; the function records state or raises through its assertions.
    """
    resource = WorkResource(resource_kind=ResourceKind.FULL_TEXT, uri="https://example.invalid/book", label="Full text", work_id=1)
    container = WorkResourcesContainer(work_id=1)
    container.add_resource(resource)
    container.validate()

    payload = container.as_write_payload()
    assert payload[0]["work_id"] == 1
    assert payload[0]["uri"] == "https://example.invalid/book"
    assert container.kind_text(ResourceKind.FULL_TEXT) == "Full text"


def test_agent_credit_container_smoke_round_trip() -> None:
    """
    Verify agent credit container smoke round trip.

    Example:
        Exercise test agent credit container smoke round trip through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_family_smoke.py


    :return: None; the function records state or raises through its assertions.
    """
    credit = WorkAgentCredit(
        agent_id=10,
        credited_as="Ada Example",
        work_id=1,
        role=WorkAgentRole.AUTHOR,
        source=CreditSource.USER_SET,
    )
    container = WorkAgentCreditsContainer(work_id=1)
    container.add_credit(credit)
    container.validate()

    payload = container.as_write_payload()
    assert payload[0]["work_id"] == 1
    assert payload[0]["credited_as"] == "Ada Example"
    assert container.role_text(WorkAgentRole.AUTHOR) == "Ada Example"


def test_item_wemi_title_slice_smoke() -> None:
    """
    Verify item wemi title slice smoke.

    Example:
        Exercise test item wemi title slice smoke through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_family_smoke.py


    :return: None; the function records state or raises through its assertions.
    """
    work_titles = WorkTitlesContainer(work_id=1)
    work_titles.add_title(WorkTitle(title_kind=TitleKind.MAIN, text="The Book", work_id=1))
    item_titles = ItemTitlesContainer(item_id=4)

    slice_ = ItemWemiTitleSlice(
        work_titles=work_titles,
        expression_titles=None,
        manifestation_titles=None,
        item_titles=item_titles,
    )
    assert slice_.title_parts() == ("The Book",)
    assert slice_.full_title() == "The Book"


def test_agent_participation_snapshot_smoke() -> None:
    """
    Verify agent participation snapshot smoke.

    Example:
        Exercise test agent participation snapshot smoke through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_family_smoke.py


    :return: None; the function records state or raises through its assertions.
    """
    snapshot = AgentParticipationSnapshot(
        agent=AgentProfileSummary(
            agent_id=10,
            agent_type="human",
            display_name="Ada Example",
        ),
        works=(
            AgentParticipationEntry(
                credit=WorkAgentCredit(
                    agent_id=10,
                    credited_as="Ada Example",
                    work_id=1,
                    role=WorkAgentRole.AUTHOR,
                ),
                target=WorkSummary(work_id=1, title="The Book"),
            ),
        ),
    )

    assert not snapshot.is_empty()
    assert snapshot.all_entries() == snapshot.works
    assert snapshot.counts_by_level()["works"] == 1
