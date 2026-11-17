"""Native schema and custom-field descriptors, published as complete snapshots.

Standard keys are physical ``table.column`` names. Custom keys are native
``#attachment.label`` identities; an unambiguous ``#label`` is a lookup alias.
There are no book layouts, result slots, synthetic Series fields or Calibre
category conventions. Catalog owns values and policy; this registry is derived.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import (
    Callable,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    Mapping,
    ValuesView,
)
from dataclasses import dataclass
from threading import RLock
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from LiuXin_alpha.databases.custom_columns.native_types import FieldDefinition
    from LiuXin_alpha.databases.schema_specs import StorageSchemaSpec

type FieldDescriptor = Mapping[str, Any]


def _freeze(value: Any) -> Any:
    """Detach JSON-shaped metadata into recursively immutable containers."""
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


@dataclass(frozen=True)
class FieldMetadataSnapshot:
    """One consistent generation of descriptors and their lookup aliases."""

    generation: int
    fields: Mapping[str, FieldDescriptor]
    aliases: Mapping[str, str]


class FieldMetadata(Mapping[str, FieldDescriptor]):
    """Hold native descriptors independently of any database or legacy container.

    Preparation returns a publication callback for the transaction owner. Tickets
    belong to this registry, so an older completion cannot overwrite a newer
    publication even when different cache coordinators share the registry.
    """

    def __init__(self, schema: StorageSchemaSpec | None = None) -> None:
        self._lock = RLock()
        self._issued = 0
        self._published = 0
        standard: dict[str, FieldDescriptor] = {}
        if schema is not None:
            for table in schema.tables.values():
                if not table.is_main_table:
                    continue
                for column in table.columns:
                    key = f"{table.name}.{column.name}"
                    standard[key] = _freeze(
                        {
                            "key": key,
                            "name": column.name,
                            "table": table.name,
                            "column": column.name,
                            "datatype": column.declared_type or column.affinity,
                            "collection_mode": "scalar",
                            "is_custom": False,
                            "nullable": column.nullable,
                            "hidden": column.hidden,
                            "generated": column.generated,
                            "primary_key": column.is_primary_key,
                        }
                    )
        self._standard = MappingProxyType(standard)
        self._snapshot = FieldMetadataSnapshot(0, self._standard, MappingProxyType({}))

    @property
    def generation(self) -> int:
        """Return the currently published generation; preparation alone does not advance it."""
        return self._snapshot.generation

    def snapshot(self) -> FieldMetadataSnapshot:
        """Capture descriptors, aliases and generation together for a multi-step read."""
        return self._snapshot

    def __getitem__(self, key: str) -> FieldDescriptor:
        """Read one canonical key; alias resolution is explicit."""
        return self._snapshot.fields[key]

    def __iter__(self) -> Iterator[str]:
        """Retain one snapshot's key order for this iterator."""
        return iter(self._snapshot.fields)

    def __len__(self) -> int:
        """Count descriptors in the current snapshot."""
        return len(self._snapshot.fields)

    def keys(self) -> KeysView[str]:
        """Iterate keys from one retained snapshot."""
        return self._snapshot.fields.keys()

    def items(self) -> ItemsView[str, FieldDescriptor]:
        """Iterate complete descriptors from one retained snapshot."""
        return self._snapshot.fields.items()

    def values(self) -> ValuesView[FieldDescriptor]:
        """Iterate values from one retained snapshot."""
        return self._snapshot.fields.values()

    def all_field_keys(self) -> tuple[str, ...]:
        """Return canonical schema and custom-field keys in publication order."""
        return tuple(self._snapshot.fields)

    def standard_field_keys(self) -> tuple[str, ...]:
        """Return the physical schema keys supplied at construction."""
        return tuple(self._standard)

    def custom_field_keys(self, include_composites: bool = True) -> tuple[str, ...]:
        """Select active custom definitions, optionally omitting computed fields."""
        return tuple(
            key
            for key, field in self._snapshot.fields.items()
            if field["is_custom"]
            and (include_composites or field["datatype"] != "composite")
        )

    def sortable_field_keys(self) -> tuple[str, ...]:
        """Select scalar descriptors; query execution still validates supported types."""
        return tuple(
            key
            for key, field in self._snapshot.fields.items()
            if field["collection_mode"] == "scalar" and field["datatype"] != "composite"
        )

    def displayable_field_keys(self) -> tuple[str, ...]:
        """Select descriptors that are not declared hidden by the schema."""
        return tuple(
            key
            for key, field in self._snapshot.fields.items()
            if not field.get("hidden", False)
        )

    def searchable_fields(self) -> tuple[str, ...]:
        """Return canonical keys; aliases resolve explicitly through the lookup method."""
        return self.all_field_keys()

    def search_term_to_field_key(self, term: str) -> str | None:
        """Resolve a canonical key or unique short alias; unknown/ambiguous terms miss."""
        snapshot = self._snapshot
        return term if term in snapshot.fields else snapshot.aliases.get(term)

    def prepare_custom_fields(
        self, definitions: Iterable[FieldDefinition]
    ) -> Callable[[], None]:
        """Prepare a complete custom-field replacement without publishing it.

        Call the returned operation only after commit. Discard it on rollback.
        Standard schema descriptors survive replacement, and alias ambiguity is
        recomputed from the complete active definition set.
        """
        definitions = tuple(
            definition for definition in definitions if not definition.retired
        )
        counts = Counter(definition.label for definition in definitions)
        fields = dict(self._standard)
        aliases = {}
        for definition in definitions:
            key = definition.key
            if key in fields:
                raise ValueError(f"Duplicate native field descriptor: {key}")
            fields[key] = _freeze(
                {
                    "key": key,
                    "id": definition.id,
                    "label": definition.label,
                    "name": definition.name,
                    "attachment": definition.attachment,
                    "datatype": definition.datatype,
                    "collection_mode": definition.mode,
                    "editable": definition.editable,
                    "display": definition.display,
                    "revision": definition.revision,
                    "is_custom": True,
                }
            )
            if counts[definition.label] == 1:
                aliases["#" + definition.label] = key
        frozen_fields, frozen_aliases = (
            MappingProxyType(fields),
            MappingProxyType(aliases),
        )
        with self._lock:
            self._issued += 1
            ticket = self._issued

        def publish() -> None:
            with self._lock:
                if ticket <= self._published:
                    return
                self._snapshot = FieldMetadataSnapshot(
                    self._snapshot.generation + 1, frozen_fields, frozen_aliases
                )
                self._published = ticket

        return publish
