"""
Verify projection views expose stable immutable metadata snapshots.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test metadata projection views through its owning regression module::

        python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py
"""
from __future__ import annotations

from collections.abc import Mapping

import pytest

from LiuXin_alpha.metadata.api import (
    MetadataTextViewAPI,
    MetadataValuesViewAPI,
    UnloadedMetadataProjectionError,
    WorkRelationLink,
)
from LiuXin_alpha.metadata.containers import WorkIdentity, WorkMetadata
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import (
    ItemIdentity,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import (
    ItemMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import (
    ManifestationMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.projection_views import (
    LiuXinWEMITextView,
    LiuXinWEMIValuesView,
    MetadataTextView,
    MetadataValuesView,
    _append_identifier,
    _identifier_pair,
    _iter_rating_values,
    _iter_text_values,
    _lazy_relation_loaders,
    _metadata_data,
    _target_mapping,
    _target_text,
)


class _RowLike:
    """
    Provide the RowLike test fixture or double with explicit deterministic behavior.

    Example:
        Exercise RowLike through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py
    """
    def __init__(self, row_dict: Mapping[str, object]) -> None:
        """
        Initialize the RowLike test double.

        Example:
            Exercise RowLike.init through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :param row_dict: Value supplied for row dict in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self._row_dict = dict(row_dict)

    @property
    def row_dict(self) -> Mapping[str, object]:
        """
        Perform the row dict test-helper operation with deterministic inputs.

        Example:
            Exercise RowLike.row dict through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self._row_dict


class _MappingTarget:
    """
    Provide the MappingTarget test fixture or double with explicit deterministic behavior.

    Example:
        Exercise MappingTarget through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py
    """
    def __init__(self, payload: Mapping[str, object]) -> None:
        """
        Initialize the MappingTarget test double.

        Example:
            Exercise MappingTarget.init through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :param payload: Value supplied for payload in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self._payload = dict(payload)

    def to_mapping(self) -> Mapping[str, object]:
        """
        Perform the to mapping test-helper operation with deterministic inputs.

        Example:
            Exercise MappingTarget.to mapping through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return dict(self._payload)


class _ProjectionMetadata:
    """
    Provide the ProjectionMetadata test fixture or double with explicit deterministic behavior.

    Example:
        Exercise ProjectionMetadata through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py
    """
    def __init__(
        self,
        relations: Mapping[str, list[object]] | None = None,
        *,
        known_relations: set[str] | None = None,
        primary_raises: bool = False,
    ) -> None:
        """
        Initialize the ProjectionMetadata test double.

        Example:
            Exercise ProjectionMetadata.init through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :param relations: Value supplied for relations in the focused test operation.
        :param known_relations: Value supplied for known relations in the focused test
            operation.
        :param primary_raises: Value supplied for primary raises in the focused test
            operation.
        :return: None; the function records state or raises through its assertions.
        """
        self._relations = {key: list(value) for key, value in (relations or {}).items()}
        self._known_relations = known_relations or set(self._relations)
        self._primary_raises = primary_raises

    def validate_relation_name(self, relation_key: str) -> str:
        """
        Perform the validate relation name test-helper operation with deterministic inputs.

        Example:
            Exercise ProjectionMetadata.validate relation name through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :param relation_key: Value supplied for relation key in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        if relation_key in self._known_relations:
            return relation_key
        raise KeyError(relation_key)

    def get_related(self, relation_key: str) -> list[object]:
        """
        Return related from deterministic test state.

        Example:
            Exercise ProjectionMetadata.get related through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :param relation_key: Value supplied for relation key in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return list(self._relations.get(relation_key, ()))

    def primary_related(self, relation_key: str) -> object | None:
        """
        Perform the primary related test-helper operation with deterministic inputs.

        Example:
            Exercise ProjectionMetadata.primary related through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :param relation_key: Value supplied for relation key in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        if self._primary_raises:
            raise KeyError(relation_key)
        return None


class _TextValues:
    """
    Provide the TextValues test fixture or double with explicit deterministic behavior.

    Example:
        Exercise TextValues through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py
    """
    tags = ("tag",)
    labels = ("label",)
    genres = ("genre",)
    subjects = ("subject",)
    series = ("series",)
    primary_title = "Primary"
    titles = ("Primary", "Secondary")
    languages = ("en",)
    ratings = ("4.5",)
    agents = ("Agent",)

    def relation_values(self, relation_key: str) -> tuple[str, ...]:
        """
        Perform the relation values test-helper operation with deterministic inputs.

        Example:
            Exercise TextValues.relation values through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :param relation_key: Value supplied for relation key in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return (relation_key, "value")


class _BundleValues:
    """
    Provide the BundleValues test fixture or double with explicit deterministic behavior.

    Example:
        Exercise BundleValues through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py
    """
    def __init__(
        self,
        relation_values: Mapping[str, tuple[str, ...]] | None = None,
        identifiers: Mapping[str, tuple[str, ...]] | None = None,
    ) -> None:
        """
        Initialize the BundleValues test double.

        Example:
            Exercise BundleValues.init through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :param relation_values: Value supplied for relation values in the focused test
            operation.
        :param identifiers: Value supplied for identifiers in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self._relation_values = dict(relation_values or {})
        self.identifiers = dict(identifiers or {})

    def relation_values(self, relation_key: str) -> tuple[str, ...]:
        """
        Perform the relation values test-helper operation with deterministic inputs.

        Example:
            Exercise BundleValues.relation values through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :param relation_key: Value supplied for relation key in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return self._relation_values.get(relation_key, ())


class _Bundle:
    """
    Provide the Bundle test fixture or double with explicit deterministic behavior.

    Example:
        Exercise Bundle through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py
    """
    def __init__(
        self,
        relation_values: Mapping[str, tuple[str, ...]] | None = None,
        identifiers: Mapping[str, tuple[str, ...]] | None = None,
    ) -> None:
        """
        Initialize the Bundle test double.

        Example:
            Exercise Bundle.init through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :param relation_values: Value supplied for relation values in the focused test
            operation.
        :param identifiers: Value supplied for identifiers in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self._known_relations = set(relation_values or ())
        if identifiers is not None:
            self._known_relations.add("identifiers")
        self.values = _BundleValues(relation_values, identifiers)

    def validate_relation_name(self, relation_key: str) -> str:
        """
        Perform the validate relation name test-helper operation with deterministic inputs.

        Example:
            Exercise Bundle.validate relation name through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :param relation_key: Value supplied for relation key in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        if relation_key in self._known_relations:
            return relation_key
        raise KeyError(relation_key)


class _WemiProjectionMetadata:
    """
    Provide the WemiProjectionMetadata test fixture or double with explicit deterministic behavior.

    Example:
        Exercise WemiProjectionMetadata through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py
    """
    def __init__(self) -> None:
        """
        Initialize the WemiProjectionMetadata test double.

        Example:
            Exercise WemiProjectionMetadata.init through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :return: None; the function records state or raises through its assertions.
        """
        self._data = {
            "labels": ["legacy-label"],
            "genre": "legacy-genre",
            "subject": "legacy-subject",
            "series": "legacy-series",
            "languages": ["und", "en"],
            "ratings": {"overall": 4.5},
            "authors": ["Legacy Author"],
        }
        self.titles = ("Legacy Title",)
        self.display_title = "Legacy Title"
        self._lazy_identifiers_loaded = True
        self._lazy_relation_loaders: dict[tuple[str, str], object] = {}
        self._bundles = {
            "item": _Bundle(
                {"tags": ("item-tag",), "custom": ("custom-item",)},
                {"doi": ("10.0000/example",)},
            ),
            "expression": None,
            "manifestation": None,
            "work": _Bundle({"tags": ("work-tag",)}),
        }

    def get_identifiers(self) -> Mapping[str, tuple[str, ...]]:
        """
        Return identifiers from deterministic test state.

        Example:
            Exercise WemiProjectionMetadata.get identifiers through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return {"isbn": ("9780306406157",)}

    def get_wemi_metadata(self, level: str) -> object | None:
        """
        Return wemi metadata from deterministic test state.

        Example:
            Exercise WemiProjectionMetadata.get wemi metadata through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :param level: Value supplied for level in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return self._bundles.get(level)


class _GetOnlyMetadata:
    """
    Provide the GetOnlyMetadata test fixture or double with explicit deterministic behavior.

    Example:
        Exercise GetOnlyMetadata through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py
    """
    def get(self, field: str, default: object | None = None) -> object | None:
        """
        Perform the get test-helper operation with deterministic inputs.

        Example:
            Exercise GetOnlyMetadata.get through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :param field: Value supplied for field in the focused test operation.
        :param default: Value supplied for default in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return {"tags": ["get-tag"]}.get(field, default)


class _BadMappingTarget:
    """
    Provide the BadMappingTarget test fixture or double with explicit deterministic behavior.

    Example:
        Exercise BadMappingTarget through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py
    """
    def to_mapping(self) -> object:
        """
        Perform the to mapping test-helper operation with deterministic inputs.

        Example:
            Exercise BadMappingTarget.to mapping through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return "not-a-mapping"


class _StringOnlyTarget:
    """
    Provide the StringOnlyTarget test fixture or double with explicit deterministic behavior.

    Example:
        Exercise StringOnlyTarget through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py
    """
    def __str__(self) -> str:
        """
        Perform the str test-helper operation with deterministic inputs.

        Example:
            Exercise StringOnlyTarget.str through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return "string-only"


class _NonMappingData:
    """
    Provide the NonMappingData test fixture or double with explicit deterministic behavior.

    Example:
        Exercise NonMappingData through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py
    """
    _data = []
    _lazy_relation_loaders = []


def test_work_metadata_projection_values_and_text() -> None:
    """
    Verify work metadata projection values and text.

    Example:
        Exercise test work metadata projection values and text through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


    :return: None; the function records state or raises through its assertions.
    """
    metadata = WorkMetadata(
        work=WorkIdentity(
            work_id=5,
            work_title="Permutation City",
            work_canonical_title="Permutation City",
        )
    )
    metadata.set_relation_links(
        "tags",
        [
            WorkRelationLink(target={"tag": "Space Opera"}),
            WorkRelationLink(target=_RowLike({"tag": "Imported"})),
            WorkRelationLink(target={"tag": "Space Opera"}),
        ],
    )
    metadata.set_relation_links(
        "labels",
        [WorkRelationLink(target={"label_text": "Award Winners"})],
    )
    metadata.set_relation_links(
        "genres",
        [WorkRelationLink(target={"genre_full": "Science Fiction"})],
    )
    metadata.set_relation_links(
        "subjects",
        [WorkRelationLink(target=_MappingTarget({"subject": "Far Future"}))],
    )
    metadata.set_relation_links(
        "titles",
        [
            WorkRelationLink(target={"text": "Secondary Title"}, index=1),
            WorkRelationLink(target={"text": "Primary Title"}, primary=True, index=0),
        ],
    )
    metadata.set_relation_links(
        "identifiers",
        [
            WorkRelationLink(target={"scheme": "isbn", "value": "978-0575082076"}),
            WorkRelationLink(
                target={
                    "entity_identifier_scheme": "isbn",
                    "entity_identifier_value": "978-0575082076",
                }
            ),
            WorkRelationLink(
                target={
                    "entity_identifier_scheme": "doi",
                    "entity_identifier_value": "10.0000/example",
                }
            ),
        ],
    )
    metadata.set_relation_links(
        "languages",
        [WorkRelationLink(target={"language_code": "en"})],
    )
    metadata.set_relation_links(
        "ratings",
        [WorkRelationLink(target={"rating": 4.5})],
    )
    metadata.set_relation_links(
        "agents",
        [WorkRelationLink(target={"agent_display_name": "Greg Egan"})],
    )

    assert isinstance(metadata.values, MetadataValuesViewAPI)
    assert isinstance(metadata.text, MetadataTextViewAPI)
    assert metadata.values.tags == ("Space Opera", "Imported")
    assert metadata.values.relation_values("tag") == ("Space Opera", "Imported")
    assert metadata.values.labels == ("Award Winners",)
    assert metadata.values.genres == ("Science Fiction",)
    assert metadata.values.subjects == ("Far Future",)
    assert metadata.values.titles == (
        "Secondary Title",
        "Primary Title",
        "Permutation City",
    )
    assert metadata.values.primary_title == "Primary Title"
    assert dict(metadata.values.identifiers) == {
        "isbn": ("978-0575082076",),
        "doi": ("10.0000/example",),
    }
    assert metadata.values.languages == ("en",)
    assert metadata.values.ratings == ("4.5",)
    assert metadata.values.agent_names == ("Greg Egan",)

    assert metadata.text.tags == "Space Opera, Imported"
    assert metadata.text.relation_text("tag", separator="|") == "Space Opera|Imported"
    assert metadata.text.title == "Primary Title"
    assert metadata.text.titles == "Secondary Title ; Primary Title ; Permutation City"
    assert metadata.text.agent_names == "Greg Egan"


def test_projection_views_are_read_only_and_do_not_mutate_links() -> None:
    """
    Verify projection views remain read only and do not mutate links.

    Example:
        Exercise test projection views are read only and do not mutate links through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


    :return: None; the function records state or raises through its assertions.
    """
    metadata = WorkMetadata()
    metadata.set_relation_links(
        "tags",
        [WorkRelationLink(target={"tag": "Space Opera"}, link_id=101)],
    )
    links_before = tuple(metadata.get_relation_links("tags"))
    values = metadata.values

    assert values.tags == ("Space Opera",)
    assert tuple(metadata.get_relation_links("tags")) == links_before

    with pytest.raises(AttributeError):
        values.tags = ("Changed",)

    metadata.set_relation_links(
        "identifiers",
        [WorkRelationLink(target={"scheme": "isbn", "value": "978-0575082076"})],
    )
    identifiers = metadata.values.identifiers
    with pytest.raises(TypeError):
        identifiers["isbn"] = ("changed",)


def test_unsupported_projection_properties_are_empty() -> None:
    """
    Verify unsupported projection properties remain empty.

    Example:
        Exercise test unsupported projection properties are empty through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


    :return: None; the function records state or raises through its assertions.
    """
    metadata = ManifestationMetadata()

    assert metadata.values.tags == ()
    assert metadata.text.tags == ""
    with pytest.raises(KeyError):
        metadata.values.relation_values("tags")


def test_title_projection_falls_back_to_identity_display_fields() -> None:
    """
    Verify title projection falls back to identity display fields.

    Example:
        Exercise test title projection falls back to identity display fields through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


    :return: None; the function records state or raises through its assertions.
    """
    metadata = ItemMetadata(item=ItemIdentity(item_source_name="source-file.epub"))

    assert metadata.values.titles == ("source-file.epub",)
    assert metadata.values.primary_title == "source-file.epub"
    assert metadata.text.title == "source-file.epub"


def test_metadata_values_view_covers_fallback_and_skip_paths() -> None:
    """
    Verify metadata values view covers fallback and skip paths.

    Example:
        Exercise test metadata values view covers fallback and skip paths through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


    :return: None; the function records state or raises through its assertions.
    """
    values = MetadataValuesView(
        _ProjectionMetadata(
            {
                "titles": [" Relation Title "],
                "identifiers": [
                    {"scheme": "isbn", "value": "9780306406157"},
                    {"scheme": "", "value": "ignored"},
                    {"value": "missing-scheme"},
                ],
            },
            primary_raises=True,
        )
    )

    assert values.series == ()
    assert values.titles == ("Relation Title",)
    assert values.primary_title == "Relation Title"
    assert dict(values.identifiers) == {"isbn": ("9780306406157",)}
    assert dict(MetadataValuesView(_ProjectionMetadata()).identifiers) == {}


def test_metadata_text_view_exposes_all_relation_text_properties() -> None:
    """
    Verify metadata text view exposes all relation text properties.

    Example:
        Exercise test metadata text view exposes all relation text properties through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


    :return: None; the function records state or raises through its assertions.
    """
    text = MetadataTextView(_TextValues())

    assert text.relation_text("custom", separator="|") == "custom|value"
    assert text.labels == "label"
    assert text.genres == "genre"
    assert text.subjects == "subject"
    assert text.series == "series"
    assert text.languages == "en"
    assert text.ratings == "4.5"
    assert text.agents == "Agent"


def test_liuxin_wemi_projection_view_edge_paths() -> None:
    """
    Verify liuxin wemi projection view edge paths.

    Example:
        Exercise test liuxin wemi projection view edge paths through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


    :return: None; the function records state or raises through its assertions.
    """
    metadata = _WemiProjectionMetadata()
    values = LiuXinWEMIValuesView(metadata)
    text = LiuXinWEMITextView(values)

    assert values.relation_values("title") == ("Legacy Title",)
    assert values.relation_values("identifier") == (
        "9780306406157",
        "10.0000/example",
    )
    assert values.relation_values("custom") == ("custom-item",)
    assert values.ratings == ("4.5",)
    assert values.tags == ("item-tag", "work-tag")
    assert values.primary_title == "Legacy Title"

    with pytest.raises(KeyError, match="Unknown WEMI stack relation key"):
        values.relation_values("unknown")

    assert text.labels == "legacy-label"
    assert text.genres == "legacy-genre"
    assert text.subjects == "legacy-subject"
    assert text.series == "legacy-series"
    assert text.titles == "Legacy Title"
    assert text.languages == "en"
    assert text.ratings == "4.5"
    assert text.agents == "Legacy Author"
    assert text.agent_names == "Legacy Author"


def test_liuxin_wemi_projection_reports_unloaded_identifier_and_bundle_dependencies() -> None:
    """
    Verify liuxin wemi projection reports unloaded identifier and bundle dependencies.

    Example:
        Exercise test liuxin wemi projection reports unloaded identifier and bundle dependencies through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


    :return: None; the function records state or raises through its assertions.
    """
    metadata = _WemiProjectionMetadata()
    metadata._lazy_identifiers_loaded = False
    with pytest.raises(UnloadedMetadataProjectionError) as identifier_error:
        LiuXinWEMIValuesView(metadata).identifiers
    assert identifier_error.value.unloaded_dependencies == ("legacy:identifiers",)

    metadata = _WemiProjectionMetadata()
    metadata._lazy_relation_loaders = {("work", "tags"): object()}
    with pytest.raises(UnloadedMetadataProjectionError) as relation_error:
        LiuXinWEMIValuesView(metadata).tags
    assert relation_error.value.unloaded_dependencies == ("work:tags",)


def test_liuxin_wemi_projection_reads_legacy_values_from_get_fallback() -> None:
    """
    Verify liuxin wemi projection reads legacy values from get fallback.

    Example:
        Exercise test liuxin wemi projection reads legacy values from get fallback through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


    :return: None; the function records state or raises through its assertions.
    """
    assert LiuXinWEMIValuesView(_GetOnlyMetadata()).tags == ("get-tag",)
    assert LiuXinWEMIValuesView(object()).tags == ()


def test_projection_helper_edge_cases() -> None:
    """
    Verify projection helper edge cases.

    Example:
        Exercise test projection helper edge cases through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_metadata_projection_views.py


    :return: None; the function records state or raises through its assertions.
    """
    assert _target_text(" text ", "tags") == "text"
    assert _target_text(4.5, "ratings") == "4.5"
    assert _target_text({"unknown": "value"}, "tags") is None
    assert _target_text(_StringOnlyTarget(), "tags") == "string-only"

    assert _identifier_pair("isbn: 9780306406157") == ("isbn", "9780306406157")
    assert _identifier_pair(":missing-scheme") is None
    assert _identifier_pair({}) is None
    assert _target_mapping(_BadMappingTarget()) == {}

    assert _iter_text_values(None) == ()
    assert _iter_text_values((None, "tag")) == ("tag",)
    assert _iter_text_values({"tag": 1}) == ("tag",)
    assert _iter_rating_values(None) == ()
    assert _iter_rating_values("5") == ("5",)
    assert _iter_rating_values({"unrated": ""}) == ("unrated",)
    assert _iter_rating_values(
        {"calibre": 4, "overall": 8},
        suppress_calibre=True,
    ) == ("8",)

    identifiers: dict[str, list[str]] = {}
    _append_identifier(identifiers, "", "value")
    assert identifiers == {}

    assert _metadata_data(object()) == {}
    assert _metadata_data(_NonMappingData()) == {}
    assert _lazy_relation_loaders(object()) == {}
    assert _lazy_relation_loaders(_NonMappingData()) == {}
