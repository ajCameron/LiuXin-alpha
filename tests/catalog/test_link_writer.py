"""Tests for the inheritance-oriented catalog writer foundation."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import is_dataclass, replace
from types import SimpleNamespace
from typing import Any

import pytest

from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.write import (
    BaseCatalogWriter,
    CatalogLinkWriter,
    CatalogValueWriter,
    LinkUpdate,
)
from LiuXin_alpha.databases.macro_types import LinkRow, LinkValue
from LiuXin_alpha.databases.schema_specs import (
    LinkCardinality,
    StorageLinkSpec,
)


def _link_spec(
    cardinality: LinkCardinality = LinkCardinality.UNKNOWN,
) -> StorageLinkSpec:
    return StorageLinkSpec(
        primary_table="titles",
        secondary_table="creators",
        link_table="creator_title_links",
        cardinality=cardinality,
        primary_link_col="creator_title_link_title_id",
        secondary_link_col="creator_title_link_creator_id",
        priority_link_col="creator_title_link_priority",
        type_link_col="creator_title_link_type",
        ordered=True,
        typed=True,
        type_part_of_identity=True,
    )


class _RecordingCatalog:
    def __init__(
        self,
        result: Mapping[int, tuple[LinkRow, ...]] | None = None,
        failure: Exception | None = None,
    ) -> None:
        self.result = {} if result is None else result
        self.failure = failure
        self.updates: list[LinkUpdate] = []

    def write_link_update(
        self,
        update: LinkUpdate,
    ) -> Mapping[int, tuple[LinkRow, ...]]:
        self.updates.append(update)
        if self.failure is not None:
            raise self.failure
        return self.result


class _ConfiguredLinkWriter(CatalogLinkWriter[str, str]):
    def __init__(
        self,
        catalog: Any,
        link_spec: StorageLinkSpec,
        *,
        resolve_destination: Callable[[str], int],
        adapt: Callable[[str], str] = lambda value: value,
        validate: Callable[[str], None] = lambda _value: None,
    ) -> None:
        self._adapt_value = adapt
        self._validate_value = validate
        self._resolve_value = resolve_destination
        super().__init__(catalog, link_spec)

    def adapt(self, raw_value: str) -> str:
        return self._adapt_value(raw_value)

    def validate(self, value: str) -> None:
        self._validate_value(value)

    def resolve_destination(self, value: str) -> int:
        return self._resolve_value(value)


def _identity_writer(
    catalog: Any,
    link_spec: StorageLinkSpec | None = None,
) -> CatalogLinkWriter[str, str]:
    return _ConfiguredLinkWriter(
        catalog,
        _link_spec() if link_spec is None else link_spec,
        resolve_destination=lambda value: {"ada": 20, "grace": 21}[value],
    )


def test_link_writer_builds_an_inspectable_update_without_writing() -> None:
    catalog = _RecordingCatalog()
    events: list[tuple[str, str]] = []

    def adapt(value: str) -> str:
        events.append(("adapt", value))
        return value.strip().casefold()

    def validate(value: str) -> None:
        events.append(("validate", value))
        if not value:
            raise ValueError("destination value cannot be empty")

    def resolve(value: str) -> int:
        events.append(("resolve", value))
        return {"ada": 20, "grace": 21}[value]

    writer = _ConfiguredLinkWriter(
        catalog,
        _link_spec(),
        resolve_destination=resolve,
        adapt=adapt,
        validate=validate,
    )

    update = writer.build_update(
        replacements={
            10: [
                " Ada ",
                " Ada ",
                30,
                LinkValue(
                    31,
                    link_type="author",
                    priority=2,
                    extra={"credited_as": "A. Writer"},
                ),
            ]
        },
        additions={11: {"editor": " Grace "}},
        deletions={12: None},
    )

    assert catalog.updates == []
    assert update.replacements[10] == (
        LinkValue(20),
        LinkValue(30),
        LinkValue(
            31,
            link_type="author",
            priority=2,
            extra={"credited_as": "A. Writer"},
        ),
    )
    assert update.additions[11] == (LinkValue(21, link_type="editor"),)
    assert update.deletions[12] == ()
    assert events == [
        ("adapt", " Ada "),
        ("validate", "ada"),
        ("resolve", "ada"),
        ("adapt", " Grace "),
        ("validate", "grace"),
        ("resolve", "grace"),
    ]


def test_link_writer_writes_once_and_returns_the_catalog_result_unchanged() -> None:
    expected = {10: (LinkRow(10, 20, link_type="author"),)}
    catalog = _RecordingCatalog(expected)
    writer = _identity_writer(catalog)

    result = writer.write(
        {10: {"author": "ada"}},
        link_type="author",
    )

    assert result is expected
    assert len(catalog.updates) == 1
    assert catalog.updates[0].replacements == {
        10: (LinkValue(20, link_type="author"),)
    }


def test_link_writer_write_one_builds_one_authoritative_link_update() -> None:
    expected = {10: (LinkRow(10, 20, link_type="author"),)}
    catalog = _RecordingCatalog(expected)
    writer = _identity_writer(catalog)

    result = writer.write_one(10, "ada", link_type="author")

    assert result is expected
    assert len(catalog.updates) == 1
    assert catalog.updates[0].replacements == {
        10: (LinkValue(20, link_type="author"),)
    }
    assert catalog.updates[0].additions == {}


@pytest.mark.parametrize(
    "build",
    (
        lambda writer: writer.build_update(
            {10: LinkValue(20, link_type="reviewer")}
        ),
        lambda writer: writer.build_update(
            {10: {"reviewer": "ada"}}
        ),
        lambda writer: writer.build_one_update(
            10,
            "ada",
            link_type="reviewer",
        ),
    ),
    ids=("rich-link", "typed-map", "type-scope"),
)
def test_link_writer_rejects_disallowed_types_before_resolution(
    build: Callable[[CatalogLinkWriter[str, str]], LinkUpdate],
) -> None:
    catalog = _RecordingCatalog()
    resolved: list[str] = []
    spec = replace(_link_spec(), allowed_types=("author", "editor"))
    writer = _ConfiguredLinkWriter(
        catalog,
        spec,
        resolve_destination=lambda value: resolved.append(value) or 20,
    )

    with pytest.raises(ValueError, match="not allowed by the link spec"):
        build(writer)

    assert resolved == []
    assert catalog.updates == []


def test_link_writer_rejects_a_type_scope_when_the_link_is_untyped() -> None:
    catalog = _RecordingCatalog()
    resolved: list[str] = []
    spec = replace(
        _link_spec(),
        type_link_col=None,
        typed=False,
        type_part_of_identity=False,
    )
    writer = _ConfiguredLinkWriter(
        catalog,
        spec,
        resolve_destination=lambda value: resolved.append(value) or 20,
    )

    with pytest.raises(ValueError, match="requires a typed link spec"):
        writer.write_one(10, "ada", link_type="author")

    assert resolved == []
    assert catalog.updates == []


@pytest.mark.parametrize(
    ("link_type", "error", "message"),
    (
        ("  ", ValueError, "cannot be blank"),
        (3, TypeError, "must be a string or None"),
    ),
)
def test_link_writer_rejects_invalid_named_type_values(
    link_type: object,
    error: type[Exception],
    message: str,
) -> None:
    writer = _identity_writer(_RecordingCatalog())

    with pytest.raises(error, match=message):
        writer.build_one_update(10, "ada", link_type=link_type)  # type: ignore[arg-type]


def test_link_writer_allows_the_null_type_with_an_allowed_type_list() -> None:
    spec = replace(_link_spec(), allowed_types=("author", "editor"))
    writer = _identity_writer(_RecordingCatalog(), spec)

    update = writer.build_one_update(10, "ada", link_type=None)

    assert update.replacements == {10: (LinkValue(20),)}


def test_link_writer_reads_allowed_types_live_for_every_typed_write() -> None:
    class _AllowedTypesWrapper:
        def __init__(self) -> None:
            self.values = ["author"]
            self.calls: list[StorageLinkSpec] = []

        def get_allowed_link_types(
            self,
            link_spec: StorageLinkSpec,
        ) -> tuple[str, ...]:
            self.calls.append(link_spec)
            return tuple(self.values)

    catalog = _RecordingCatalog()
    wrapper = _AllowedTypesWrapper()
    catalog.db = SimpleNamespace(driver_wrapper=wrapper)
    resolved: list[str] = []
    spec = replace(
        _link_spec(),
        allowed_types_table="creator_title_links__types",
    )
    writer = _ConfiguredLinkWriter(
        catalog,
        spec,
        resolve_destination=lambda value: resolved.append(value) or 20,
    )

    with pytest.raises(ValueError, match="does not exist in allowed-types"):
        writer.build_one_update(10, "ada", link_type="reviewer")
    assert resolved == []

    wrapper.values.append("reviewer")
    update = writer.build_one_update(10, "ada", link_type="reviewer")

    assert update.replacements == {
        10: (LinkValue(20, link_type="reviewer"),)
    }
    assert wrapper.calls == [spec, spec]
    assert resolved == ["ada"]

    wrapper.values.clear()
    with pytest.raises(ValueError, match="allowed types: <none>"):
        writer.build_one_update(10, "ada", link_type="author")
    assert resolved == ["ada"]


@pytest.mark.parametrize(
    ("wrapper", "error", "message"),
    (
        (
            SimpleNamespace(),
            TypeError,
            "must provide get_allowed_link_types",
        ),
        (
            SimpleNamespace(get_allowed_link_types=lambda _spec: None),
            ValueError,
            "returned no registry",
        ),
        (
            SimpleNamespace(get_allowed_link_types=lambda _spec: ("",)),
            ValueError,
            "blank value",
        ),
    ),
    ids=("missing-reader", "missing-registry", "malformed-registry"),
)
def test_link_writer_rejects_an_unreadable_allowed_type_registry(
    wrapper: object,
    error: type[Exception],
    message: str,
) -> None:
    catalog = _RecordingCatalog()
    catalog.db = SimpleNamespace(driver_wrapper=wrapper)
    spec = replace(
        _link_spec(),
        allowed_types_table="creator_title_links__types",
    )
    writer = _ConfiguredLinkWriter(
        catalog,
        spec,
        resolve_destination=lambda _value: pytest.fail(
            "invalid registry must fail before destination resolution"
        ),
    )

    with pytest.raises(error, match=message):
        writer.build_one_update(10, "ada", link_type="author")


def test_link_type_validation_preserves_one_shot_link_iterables() -> None:
    spec = replace(_link_spec(), allowed_types=("author",))
    writer = _identity_writer(_RecordingCatalog(), spec)
    links = (
        LinkValue(destination_id, link_type="author")
        for destination_id in (20, 21)
    )

    update = writer.build_update({10: links})

    assert update.replacements == {
        10: (
            LinkValue(20, link_type="author"),
            LinkValue(21, link_type="author"),
        )
    }


def test_link_writer_sends_an_empty_update_through_the_catalog_once() -> None:
    expected: dict[int, tuple[LinkRow, ...]] = {}
    catalog = _RecordingCatalog(expected)
    writer = _identity_writer(catalog)

    assert writer.write() is expected
    assert len(catalog.updates) == 1
    assert not catalog.updates[0]


@pytest.mark.parametrize("stage", ("adapt", "validate", "resolve"))
def test_link_writer_does_not_write_when_value_processing_fails(stage: str) -> None:
    catalog = _RecordingCatalog()
    failure = LookupError(stage)

    def adapt(value: str) -> str:
        if stage == "adapt":
            raise failure
        return value

    def validate(_value: str) -> None:
        if stage == "validate":
            raise failure

    def resolve(_value: str) -> int:
        if stage == "resolve":
            raise failure
        return 20

    writer = _ConfiguredLinkWriter(
        catalog,
        _link_spec(),
        resolve_destination=resolve,
        adapt=adapt,
        validate=validate,
    )

    with pytest.raises(LookupError) as caught:
        writer.write({10: "ada"})

    assert caught.value is failure
    assert catalog.updates == []


def test_link_writer_propagates_catalog_failure_without_retrying() -> None:
    failure = RuntimeError("atomic write failed")
    catalog = _RecordingCatalog(failure=failure)
    writer = _identity_writer(catalog)

    with pytest.raises(RuntimeError) as caught:
        writer.write({10: "ada"})

    assert caught.value is failure
    assert len(catalog.updates) == 1


def test_existing_ids_and_rich_links_bypass_metadata_callbacks() -> None:
    catalog = _RecordingCatalog()
    writer = _ConfiguredLinkWriter(
        catalog,
        _link_spec(),
        resolve_destination=lambda value: pytest.fail(
            f"unexpected resolution: {value!r}"
        ),
        adapt=lambda value: pytest.fail(f"unexpected adaptation: {value!r}"),
        validate=lambda value: pytest.fail(
            f"unexpected validation: {value!r}"
        ),
    )

    update = writer.build_update(
        {10: [20, LinkValue(21, link_type="editor", priority=3)]}
    )

    assert update.replacements[10] == (
        LinkValue(20),
        LinkValue(21, link_type="editor", priority=3),
    )


def test_link_writer_is_an_abstract_field_specific_base() -> None:
    with pytest.raises(TypeError, match="abstract"):
        CatalogLinkWriter(_RecordingCatalog(), _link_spec())


def test_link_writer_rejects_invalid_catalog_and_link_spec() -> None:
    with pytest.raises(TypeError, match="catalog must provide write_link_update"):
        _identity_writer(object())

    with pytest.raises(TypeError, match="link_spec must be a StorageLinkSpec"):
        _ConfiguredLinkWriter(
            _RecordingCatalog(),
            object(),  # type: ignore[arg-type]
            resolve_destination=lambda value: 20,
            adapt=lambda value: value,
        )


def test_link_writer_is_a_regular_extensible_class_with_read_only_config() -> None:
    writer = _identity_writer(_RecordingCatalog())

    assert not is_dataclass(writer)
    assert hasattr(writer, "__dict__")
    assert writer != _identity_writer(_RecordingCatalog())
    with pytest.raises(AttributeError):
        writer.link_spec = _link_spec()  # type: ignore[misc]


def test_base_value_writer_supports_a_same_table_scalar_specialization() -> None:
    class SameTableWriter(
        CatalogValueWriter[str, str, dict[int, str], dict[int, str]]
    ):
        def __init__(self) -> None:
            super().__init__(object())  # type: ignore[arg-type]
            self.applied: list[dict[int, str]] = []

        def adapt(self, raw_value: str) -> str:
            return raw_value.strip()

        def validate(self, value: str) -> None:
            if not value:
                raise ValueError("value cannot be empty")

        def build_update(self, values: Mapping[int, str]) -> dict[int, str]:
            return {
                source_id: self.prepare_value(value)
                for source_id, value in values.items()
            }

        def apply_update(self, update: dict[int, str]) -> dict[int, str]:
            self.applied.append(update)
            return update

    writer = SameTableWriter()

    assert writer.write({1: " title "}) == {1: "title"}
    assert writer.write_one(2, " subtitle ") == {2: "subtitle"}
    assert writer.applied == [{1: "title"}, {2: "subtitle"}]
    assert writer.write.__func__ is BaseCatalogWriter.write
    assert writer.write_one.__func__ is BaseCatalogWriter.write_one


def test_link_writer_supports_one_to_one_values_in_another_table() -> None:
    writer = _identity_writer(
        _RecordingCatalog(),
        _link_spec(LinkCardinality.ONE_TO_ONE),
    )

    update = writer.build_update({10: "ada"})

    assert update.replacements[10] == (LinkValue(20),)


@pytest.mark.parametrize(
    "cardinality",
    (LinkCardinality.ONE_TO_ONE, LinkCardinality.MANY_TO_ONE),
)
def test_singular_source_cardinalities_reject_multiple_destinations(
    cardinality: LinkCardinality,
) -> None:
    writer = _identity_writer(
        _RecordingCatalog(),
        _link_spec(cardinality),
    )

    with pytest.raises(ValueError, match="at most one destination"):
        writer.build_update({10: ["ada", "grace"]})

    with pytest.raises(ValueError, match="at most one destination"):
        writer.build_update(additions={10: ["ada", "grace"]})


@pytest.mark.parametrize(
    "cardinality",
    (LinkCardinality.ONE_TO_MANY, LinkCardinality.MANY_TO_MANY),
)
def test_plural_source_cardinalities_accept_multiple_destinations(
    cardinality: LinkCardinality,
) -> None:
    writer = _identity_writer(
        _RecordingCatalog(),
        _link_spec(cardinality),
    )

    update = writer.build_update({10: ["ada", "grace"]})

    assert update.replacements[10] == (LinkValue(20), LinkValue(21))


def test_link_writer_only_applies_updates_for_its_configured_link() -> None:
    writer = _identity_writer(_RecordingCatalog())
    other_spec = StorageLinkSpec(
        primary_table="works",
        secondary_table="subjects",
        link_table="subject_work_links",
        primary_link_col="work_id",
        secondary_link_col="subject_id",
    )

    with pytest.raises(ValueError, match="does not match writer"):
        writer.apply_update(LinkUpdate.from_ids(other_spec, {1: 2}))


def test_link_writer_applies_replacement_and_incremental_updates_to_real_db(db) -> None:
    db.driver_wrapper.executescript(
        """
        CREATE TABLE link_writer_sources (
            link_writer_source_id INTEGER PRIMARY KEY,
            link_writer_source_name TEXT NOT NULL
        );
        CREATE TABLE link_writer_values (
            link_writer_value_id INTEGER PRIMARY KEY,
            link_writer_value_name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE link_writer_links (
            link_writer_source_id INTEGER NOT NULL,
            link_writer_value_id INTEGER NOT NULL,
            UNIQUE(link_writer_source_id, link_writer_value_id),
            FOREIGN KEY(link_writer_source_id)
                REFERENCES link_writer_sources(link_writer_source_id),
            FOREIGN KEY(link_writer_value_id)
                REFERENCES link_writer_values(link_writer_value_id)
        );
        INSERT INTO link_writer_sources VALUES (1, 'source');
        INSERT INTO link_writer_values VALUES (10, 'existing');
        """
    )
    spec = StorageLinkSpec(
        primary_table="link_writer_sources",
        secondary_table="link_writer_values",
        link_table="link_writer_links",
        primary_id_col="link_writer_source_id",
        secondary_id_col="link_writer_value_id",
        primary_link_col="link_writer_source_id",
        secondary_link_col="link_writer_value_id",
    )

    def validate(value: str) -> None:
        if not value:
            raise ValueError("destination value cannot be empty")

    writer = _ConfiguredLinkWriter(
        Catalog(db),
        spec,
        adapt=lambda value: value.strip(),
        validate=validate,
        resolve_destination=lambda value: db.macros.ensure_table_value(
            spec.secondary_table,
            "link_writer_value_name",
            value,
            id_column=spec.secondary_id_col,
        ),
    )

    initial_rows = writer.write({1: [10, " matched "]})
    matched_id = next(
        row.secondary_id for row in initial_rows[1] if row.secondary_id != 10
    )
    final_rows = writer.write(
        additions={1: "added"},
        deletions={1: 10},
    )

    assert {row.secondary_id for row in final_rows[1]} == {
        matched_id,
        next(
            row[0]
            for row in db.driver_wrapper.execute(
                "SELECT link_writer_value_id FROM link_writer_values "
                "WHERE link_writer_value_name = 'added'"
            )
        ),
    }
    assert {
        row[0]
        for row in db.driver_wrapper.execute(
            "SELECT link_writer_value_name FROM link_writer_values"
        )
    } == {"existing", "matched", "added"}
