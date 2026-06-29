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
    RELATION_LINK_CLASS: type

    def _init_relation_store(self) -> None:
        self._links = {name: [] for name in self.relation_names()}

    def get_relation_links(self, relation_key):
        relation_key = self.validate_relation_name(relation_key)
        return self._links[relation_key]

    def set_relation_links(self, relation_key, links) -> None:
        relation_key = self.validate_relation_name(relation_key)
        self._links[relation_key] = list(links)

    @property
    def values(self):
        return object()

    @property
    def text(self):
        return object()

    def write_to_database(self, *args, **kwargs):
        return None

    def to_mapping(self, include_related: bool = True) -> MutableMetadataRecord:
        return {"relations": self.get_all_related()} if include_related else {}


class _ExpressionMetadata(_RelationStoreMixin, ExpressionMetadataAPI):
    def __init__(self) -> None:
        self._expression = None
        self._expression_work_id = None
        self._work_ids = None
        self._init_relation_store()

    @property
    def expression(self):
        return self._expression

    @expression.setter
    def expression(self, value) -> None:
        self._expression = value

    @property
    def work_ids(self):
        return self._work_ids

    @work_ids.setter
    def work_ids(self, work_ids) -> None:
        self._work_ids = work_ids

    @property
    def expression_work_id(self):
        return self._expression_work_id

    @expression_work_id.setter
    def expression_work_id(self, expression_work_id) -> None:
        self._expression_work_id = expression_work_id

    @classmethod
    def from_mapping(cls, payload: MetadataRecord):
        return cls()


class _ManifestationMetadata(_RelationStoreMixin, ManifestationMetadataAPI):
    def __init__(self) -> None:
        self._manifestation = None
        self._init_relation_store()

    @property
    def manifestation(self):
        return self._manifestation

    @manifestation.setter
    def manifestation(self, value) -> None:
        self._manifestation = value

    @classmethod
    def from_mapping(cls, payload: MetadataRecord):
        return cls()


class _WorkMetadata(_RelationStoreMixin, WorkMetadataAPI):
    def __init__(self) -> None:
        self._work = None
        self._init_relation_store()

    @property
    def work(self):
        return self._work

    @work.setter
    def work(self, value) -> None:
        self._work = value

    @classmethod
    def from_mapping(cls, payload: MetadataRecord):
        return cls()


class _ItemMetadata(_RelationStoreMixin, ItemMetadataAPI):
    def __init__(self) -> None:
        self._item = None
        self._init_relation_store()

    @property
    def item(self):
        return self._item

    @item.setter
    def item(self, value) -> None:
        self._item = value

    @classmethod
    def from_mapping(cls, payload: MetadataRecord):
        return cls()


def _exercise_relation_properties(container, relation_names: tuple[str, ...]) -> None:
    for relation_name in relation_names:
        values = [f"{relation_name}-one", f"{relation_name}-two"]
        setattr(container, relation_name, values)
        assert getattr(container, relation_name) == values


def test_expression_metadata_relation_properties_and_primary_ids() -> None:
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
