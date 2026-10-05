"""
Verify WEMI relation properties expose stable typed identities.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test wemi metadata relation properties api through its owning regression module::

        python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py
"""
from __future__ import annotations

from types import SimpleNamespace

from LiuXin_alpha.metadata.api import (
    ExpressionMetadataAPI,
    ExpressionRelationLink,
    ItemMetadataAPI,
    ItemRelationLink,
    ManifestationMetadataAPI,
    ManifestationRelationLink,
    MetadataRecord,
    MutableMetadataRecord,
    WorkMetadataAPI,
    WorkRelationLink,
)


class _RelationStoreMixin:
    """
    Provide the RelationStoreMixin test fixture or double with explicit deterministic behavior.

    Example:
        Exercise RelationStoreMixin through its owning regression module::

            python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py
    """
    RELATION_LINK_CLASS: type

    def _init_relation_store(self) -> None:
        """
        Perform the init relation store test-helper operation with deterministic inputs.

        Example:
            Exercise RelationStoreMixin.init relation store through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        self._links = {name: [] for name in self.relation_names()}

    def get_relation_links(self, relation_key):
        """
        Return relation links from deterministic test state.

        Example:
            Exercise RelationStoreMixin.get relation links through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :param relation_key: Value supplied for relation key in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        relation_key = self.validate_relation_name(relation_key)
        return self._links[relation_key]

    def set_relation_links(self, relation_key, links) -> None:
        """
        Perform the set relation links test-helper operation with deterministic inputs.

        Example:
            Exercise RelationStoreMixin.set relation links through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :param relation_key: Value supplied for relation key in the focused test operation.
        :param links: Value supplied for links in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        relation_key = self.validate_relation_name(relation_key)
        self._links[relation_key] = list(links)

    @property
    def values(self):
        """
        Perform the values test-helper operation with deterministic inputs.

        Example:
            Exercise RelationStoreMixin.values through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return object()

    @property
    def text(self):
        """
        Perform the text test-helper operation with deterministic inputs.

        Example:
            Exercise RelationStoreMixin.text through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return object()

    def write_to_database(self, *args, **kwargs):
        """
        Perform the write to database test-helper operation with deterministic inputs.

        Example:
            Exercise RelationStoreMixin.write to database through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :param args: Positional values forwarded by the test double.
        :param kwargs: Keyword values forwarded by the test double.
        :return: The deterministic value, row, identity or collection described above.
        """
        return None

    def to_mapping(self, include_related: bool = True) -> MutableMetadataRecord:
        """
        Perform the to mapping test-helper operation with deterministic inputs.

        Example:
            Exercise RelationStoreMixin.to mapping through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :param include_related: Value supplied for include related in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return {"relations": self.get_all_related()} if include_related else {}


class _ExpressionMetadata(_RelationStoreMixin, ExpressionMetadataAPI):
    """
    Provide the ExpressionMetadata test fixture or double with explicit deterministic behavior.

    Example:
        Exercise ExpressionMetadata through its owning regression module::

            python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py
    """
    def __init__(self) -> None:
        """
        Initialize the ExpressionMetadata test double.

        Example:
            Exercise ExpressionMetadata.init through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :return: None; the function records state or raises through its assertions.
        """
        self._expression = None
        self._expression_work_id = None
        self._work_ids = None
        self._init_relation_store()

    @property
    def expression(self):
        """
        Perform the expression test-helper operation with deterministic inputs.

        Example:
            Exercise ExpressionMetadata.expression through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self._expression

    @expression.setter
    def expression(self, value) -> None:
        """
        Perform the expression test-helper operation with deterministic inputs.

        Example:
            Exercise ExpressionMetadata.expression through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :param value: Value stored, compared or projected by the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self._expression = value

    @property
    def work_ids(self):
        """
        Perform the work ids test-helper operation with deterministic inputs.

        Example:
            Exercise ExpressionMetadata.work ids through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self._work_ids

    @work_ids.setter
    def work_ids(self, work_ids) -> None:
        """
        Perform the work ids test-helper operation with deterministic inputs.

        Example:
            Exercise ExpressionMetadata.work ids through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :param work_ids: Value supplied for work ids in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self._work_ids = work_ids

    @property
    def expression_work_id(self):
        """
        Perform the expression work id test-helper operation with deterministic inputs.

        Example:
            Exercise ExpressionMetadata.expression work id through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self._expression_work_id

    @expression_work_id.setter
    def expression_work_id(self, expression_work_id) -> None:
        """
        Perform the expression work id test-helper operation with deterministic inputs.

        Example:
            Exercise ExpressionMetadata.expression work id through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :param expression_work_id: Value supplied for expression work id in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self._expression_work_id = expression_work_id

    @classmethod
    def from_mapping(cls, payload: MetadataRecord):
        """
        Perform the from mapping test-helper operation with deterministic inputs.

        Example:
            Exercise ExpressionMetadata.from mapping through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :param payload: Value supplied for payload in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return cls()


class _ManifestationMetadata(_RelationStoreMixin, ManifestationMetadataAPI):
    """
    Provide the ManifestationMetadata test fixture or double with explicit deterministic behavior.

    Example:
        Exercise ManifestationMetadata through its owning regression module::

            python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py
    """
    def __init__(self) -> None:
        """
        Initialize the ManifestationMetadata test double.

        Example:
            Exercise ManifestationMetadata.init through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :return: None; the function records state or raises through its assertions.
        """
        self._manifestation = None
        self._init_relation_store()

    @property
    def manifestation(self):
        """
        Perform the manifestation test-helper operation with deterministic inputs.

        Example:
            Exercise ManifestationMetadata.manifestation through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self._manifestation

    @manifestation.setter
    def manifestation(self, value) -> None:
        """
        Perform the manifestation test-helper operation with deterministic inputs.

        Example:
            Exercise ManifestationMetadata.manifestation through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :param value: Value stored, compared or projected by the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self._manifestation = value

    @classmethod
    def from_mapping(cls, payload: MetadataRecord):
        """
        Perform the from mapping test-helper operation with deterministic inputs.

        Example:
            Exercise ManifestationMetadata.from mapping through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :param payload: Value supplied for payload in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return cls()


class _WorkMetadata(_RelationStoreMixin, WorkMetadataAPI):
    """
    Provide the WorkMetadata test fixture or double with explicit deterministic behavior.

    Example:
        Exercise WorkMetadata through its owning regression module::

            python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py
    """
    def __init__(self) -> None:
        """
        Initialize the WorkMetadata test double.

        Example:
            Exercise WorkMetadata.init through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :return: None; the function records state or raises through its assertions.
        """
        self._work = None
        self._init_relation_store()

    @property
    def work(self):
        """
        Perform the work test-helper operation with deterministic inputs.

        Example:
            Exercise WorkMetadata.work through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self._work

    @work.setter
    def work(self, value) -> None:
        """
        Perform the work test-helper operation with deterministic inputs.

        Example:
            Exercise WorkMetadata.work through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :param value: Value stored, compared or projected by the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self._work = value

    @classmethod
    def from_mapping(cls, payload: MetadataRecord):
        """
        Perform the from mapping test-helper operation with deterministic inputs.

        Example:
            Exercise WorkMetadata.from mapping through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :param payload: Value supplied for payload in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return cls()


class _ItemMetadata(_RelationStoreMixin, ItemMetadataAPI):
    """
    Provide the ItemMetadata test fixture or double with explicit deterministic behavior.

    Example:
        Exercise ItemMetadata through its owning regression module::

            python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py
    """
    def __init__(self) -> None:
        """
        Initialize the ItemMetadata test double.

        Example:
            Exercise ItemMetadata.init through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :return: None; the function records state or raises through its assertions.
        """
        self._item = None
        self._init_relation_store()

    @property
    def item(self):
        """
        Perform the item test-helper operation with deterministic inputs.

        Example:
            Exercise ItemMetadata.item through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self._item

    @item.setter
    def item(self, value) -> None:
        """
        Perform the item test-helper operation with deterministic inputs.

        Example:
            Exercise ItemMetadata.item through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :param value: Value stored, compared or projected by the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self._item = value

    @classmethod
    def from_mapping(cls, payload: MetadataRecord):
        """
        Perform the from mapping test-helper operation with deterministic inputs.

        Example:
            Exercise ItemMetadata.from mapping through its owning regression module::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :param payload: Value supplied for payload in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return cls()


def _exercise_relation_properties(container, relation_names: tuple[str, ...]) -> None:
    """
    Perform the exercise relation properties test-helper operation with deterministic inputs.

    Example:
        Exercise exercise relation properties through its owning regression module::

            python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


    :param container: Value supplied for container in the focused test operation.
    :param relation_names: Value supplied for relation names in the focused test
        operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    for relation_name in relation_names:
        values = [f"{relation_name}-one", f"{relation_name}-two"]
        setattr(container, relation_name, values)
        assert getattr(container, relation_name) == values


def test_expression_metadata_relation_properties_and_primary_ids() -> None:
    """
    Verify expression metadata relation properties and primary ids.

    Example:
        Exercise test expression metadata relation properties and primary ids through its owning regression module::

            python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


    :return: None; the function records state or raises through its assertions.
    """
    metadata = _ExpressionMetadata()
    metadata.set_relation_links("works", [ExpressionRelationLink(target={"work_id": "11"}, primary=True)])
    metadata.set_relation_links(
        "manifestations",
        [ExpressionRelationLink(target={"manifestation_id": "22"}, primary=True)],
    )
    metadata.set_relation_links("items", [ExpressionRelationLink(target={"item_id": "33"}, primary=True)])

    assert metadata.primary_work == {"work_id": "11"}
    assert metadata.primary_work_id == 11
    assert metadata.primary_manifestation == {"manifestation_id": "22"}
    assert metadata.primary_manifestation_id == 22
    assert metadata.primary_item == {"item_id": "33"}
    assert metadata.primary_item_id == 33

    metadata.set_relation_links("works", [])
    metadata.work_id = 44
    metadata.primary_work_id = 45
    metadata.work_ids = [45, 46]
    assert metadata.work_id == 45
    assert metadata.primary_work_id == 45
    assert metadata.work_ids == [45, 46]

    _exercise_relation_properties(metadata, ExpressionMetadataAPI.relation_names())
    assert metadata.to_mapping()["relations"]["works"] == ["works-one", "works-two"]
    assert isinstance(_ExpressionMetadata.from_mapping({}), _ExpressionMetadata)
    assert str(metadata) == "_ExpressionMetadata()"


def test_manifestation_metadata_relation_properties_and_primary_ids() -> None:
    """
    Verify manifestation metadata relation properties and primary ids.

    Example:
        Exercise test manifestation metadata relation properties and primary ids through its owning regression module::

            python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


    :return: None; the function records state or raises through its assertions.
    """
    metadata = _ManifestationMetadata()
    metadata.set_relation_links("works", [ManifestationRelationLink(target={"work_id": "101"}, primary=True)])
    metadata.set_relation_links(
        "expressions",
        [ManifestationRelationLink(target={"expression_id": "102"}, primary=True)],
    )
    metadata.set_relation_links("items", [ManifestationRelationLink(target={"item_id": "103"}, primary=True)])

    assert metadata.primary_work == {"work_id": "101"}
    assert metadata.primary_work_id == 101
    assert metadata.primary_expression == {"expression_id": "102"}
    assert metadata.primary_expression_id == 102
    assert metadata.primary_item == {"item_id": "103"}
    assert metadata.primary_item_id == 103

    metadata.set_relation_links("expressions", [])
    assert metadata.primary_expression_id is None
    metadata.manifestation = SimpleNamespace(manifestation_expression_id=104)
    assert metadata.primary_expression_id == 104

    _exercise_relation_properties(metadata, ManifestationMetadataAPI.relation_names())
    assert metadata.to_mapping()["relations"]["works"] == ["works-one", "works-two"]
    assert isinstance(_ManifestationMetadata.from_mapping({}), _ManifestationMetadata)
    assert str(metadata) == "_ManifestationMetadata()"


def test_work_metadata_primary_ids_and_string_path() -> None:
    """
    Verify work metadata primary ids and string path.

    Example:
        Exercise test work metadata primary ids and string path through its owning regression module::

            python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


    :return: None; the function records state or raises through its assertions.
    """
    metadata = _WorkMetadata()
    metadata.set_relation_links("expressions", [WorkRelationLink(target={"expression_id": "201"}, primary=True)])
    metadata.set_relation_links(
        "manifestations",
        [WorkRelationLink(target={"manifestation_id": "202"}, primary=True)],
    )
    metadata.set_relation_links("items", [WorkRelationLink(target={"item_id": "203"}, primary=True)])

    assert metadata.primary_expression == {"expression_id": "201"}
    assert metadata.primary_expression_id == 201
    assert metadata.primary_manifestation == {"manifestation_id": "202"}
    assert metadata.primary_manifestation_id == 202
    assert metadata.primary_item == {"item_id": "203"}
    assert metadata.primary_item_id == 203
    assert isinstance(_WorkMetadata.from_mapping({}), _WorkMetadata)
    assert str(metadata) == "_WorkMetadata()"


def test_item_metadata_primary_ids_fallbacks_and_string_path() -> None:
    """
    Verify item metadata primary ids fallbacks and string path.

    Example:
        Exercise test item metadata primary ids fallbacks and string path through its owning regression module::

            python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


    :return: None; the function records state or raises through its assertions.
    """
    metadata = _ItemMetadata()
    metadata.set_relation_links("works", [ItemRelationLink(target={"work_id": "301"}, primary=True)])
    metadata.set_relation_links(
        "expressions",
        [ItemRelationLink(target={"expression_id": "302"}, primary=True)],
    )
    metadata.set_relation_links(
        "manifestations",
        [ItemRelationLink(target={"manifestation_id": "303"}, primary=True)],
    )

    assert metadata.primary_work == {"work_id": "301"}
    assert metadata.primary_work_id == 301
    assert metadata.primary_expression == {"expression_id": "302"}
    assert metadata.primary_expression_id == 302
    assert metadata.primary_manifestation == {"manifestation_id": "303"}
    assert metadata.primary_manifestation_id == 303

    metadata.set_relation_links("manifestations", [])
    assert metadata.primary_manifestation_id is None
    metadata.item = SimpleNamespace(item_manifestation_id=304)
    assert metadata.primary_manifestation_id == 304

    assert isinstance(_ItemMetadata.from_mapping({}), _ItemMetadata)
    assert str(metadata) == "_ItemMetadata()"
