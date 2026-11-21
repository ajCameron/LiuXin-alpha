"""
Native schema and custom-field descriptors, published as complete snapshots.

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
    """
    Detach JSON-shaped metadata into recursively immutable containers.

    This does not clone arbitrary plugin objects or recursively freeze other
    container types such as sets.

    Example:
        >>> frozen = _freeze({"choices": ["A", "B"]})
        >>> frozen["choices"]
        ('A', 'B')


    :param value: JSON-shaped value; mappings and sequences are recursively copied.
    :return: Read-only mapping, tuple, or unchanged non-container leaf.
    """
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


@dataclass(frozen=True)
class FieldMetadataSnapshot:
    """
    One consistent generation of descriptors and their lookup aliases.

    Example:
        Keep ``registry.snapshot()`` to read descriptors and aliases from one generation.
    """

    generation: int
    fields: Mapping[str, FieldDescriptor]
    aliases: Mapping[str, str]


class FieldMetadata(Mapping[str, FieldDescriptor]):
    """
    Hold native descriptors independently of any database or legacy container.

    Preparation returns a publication callback for the transaction owner. Tickets
    belong to this registry, so an older completion cannot overwrite a newer
    publication even when different cache coordinators share the registry.

    Example:
        >>> registry = FieldMetadata()
        >>> registry.generation, len(registry)
        (0, 0)
    """

    def __init__(self, schema: StorageSchemaSpec | None = None) -> None:
        """
        Build immutable standard descriptors and an empty custom-field generation.

        Example:
            ``FieldMetadata()`` starts without physical or custom descriptors.


        :param schema: Native schema whose main-table columns become descriptors, or
            None.
        :return: None; creates generation zero and publication-ticket state.
        """
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
        """
        Return the currently published generation; preparation alone does not advance it.

        Example:
            Preparing a snapshot alone leaves registry.generation unchanged.


        :return: Current published generation number.
        """
        return self._snapshot.generation

    def snapshot(self) -> FieldMetadataSnapshot:
        """
        Capture descriptors, aliases and generation together for a multi-step read.

        Example:
            Read snapshot.fields and snapshot.aliases together to avoid mixed generations.


        :return: Current immutable snapshot object, safe to retain across later
            publications.
        """
        return self._snapshot

    def __getitem__(self, key: str) -> FieldDescriptor:
        """
        Read one canonical key; alias resolution is explicit.

        Example:
            Resolve #shelf with search_term_to_field_key before indexing the registry.


        :param key: Canonical physical or attachment-qualified custom key.
        :return: Immutable descriptor; aliases and unknown keys raise KeyError here.
        """
        return self._snapshot.fields[key]

    def __iter__(self) -> Iterator[str]:
        """
        Retain one snapshot's key order for this iterator.

        Example:
            A later publication does not change an iterator already created.


        :return: Iterator retaining the selected snapshot's canonical key order.
        """
        return iter(self._snapshot.fields)

    def __len__(self) -> int:
        """
        Count descriptors in the current snapshot.

        Example:
            Retired custom definitions do not contribute to this count.


        :return: Number of descriptors in the selected current snapshot.
        """
        return len(self._snapshot.fields)

    def keys(self) -> KeysView[str]:
        """
        Iterate keys from one retained snapshot.

        Example:
            ``tuple(registry.keys())`` lists canonical keys.


        :return: Key view from one retained snapshot.
        """
        return self._snapshot.fields.keys()

    def items(self) -> ItemsView[str, FieldDescriptor]:
        """
        Iterate complete descriptors from one retained snapshot.

        Example:
            Iterating items cannot mix keys from separate publications.


        :return: Item view retaining one snapshot's keys and descriptors.
        """
        return self._snapshot.fields.items()

    def values(self) -> ValuesView[FieldDescriptor]:
        """
        Iterate values from one retained snapshot.

        Example:
            ``tuple(registry.values())`` captures the current descriptor sequence.


        :return: Descriptor view from one retained snapshot.
        """
        return self._snapshot.fields.values()

    def all_field_keys(self) -> tuple[str, ...]:
        """
        Return canonical schema and custom-field keys in publication order.

        Example:
            This excludes short lookup aliases such as #shelf.


        :return: Physical and active custom keys in publication order.
        """
        return tuple(self._snapshot.fields)

    def standard_field_keys(self) -> tuple[str, ...]:
        """
        Return the physical schema keys supplied at construction.

        Example:
            Custom-field publication preserves the standard key sequence.


        :return: Physical schema keys in their construction order.
        """
        return tuple(self._standard)

    def custom_field_keys(self, include_composites: bool = True) -> tuple[str, ...]:
        """
        Select active custom definitions, optionally omitting computed fields.

        Example:
            Use include_composites=False when only stored-value fields are wanted.


        :param include_composites: Include composite definitions when true; otherwise
            omit them.
        :return: Active custom canonical keys in publication order.
        """
        return tuple(
            key
            for key, field in self._snapshot.fields.items()
            if field["is_custom"]
            and (include_composites or field["datatype"] != "composite")
        )

    def sortable_field_keys(self) -> tuple[str, ...]:
        """
        Select scalar descriptors; query execution still validates supported types.

        Example:
            The query layer still validates whether a selected scalar can be ordered.


        :return: Canonical keys for scalar, non-composite descriptors.
        """
        return tuple(
            key
            for key, field in self._snapshot.fields.items()
            if field["collection_mode"] == "scalar" and field["datatype"] != "composite"
        )

    def displayable_field_keys(self) -> tuple[str, ...]:
        """
        Select descriptors that are not declared hidden by the schema.

        Example:
            A schema-hidden physical column is omitted from this list.


        :return: Canonical keys whose descriptors do not set hidden=True.
        """
        return tuple(
            key
            for key, field in self._snapshot.fields.items()
            if not field.get("hidden", False)
        )

    def searchable_fields(self) -> tuple[str, ...]:
        """
        Return canonical keys; aliases resolve explicitly through the lookup method.

        Example:
            Search capability discovery belongs to the query service, not this registry.


        :return: All canonical field keys; this does not certify query execution
            support.
        """
        return self.all_field_keys()

    def search_term_to_field_key(self, term: str) -> str | None:
        """
        Resolve a canonical key or unique short alias; unknown/ambiguous terms miss.

        Example:
            Two attachments defining shelf make the short alias #shelf ambiguous.


        :param term: Canonical key or candidate short custom-field alias.
        :return: Canonical key when resolved, otherwise None for unknown or ambiguous
            terms.
        """
        snapshot = self._snapshot
        return term if term in snapshot.fields else snapshot.aliases.get(term)

    def prepare_custom_fields(
        self, definitions: Iterable[FieldDefinition]
    ) -> Callable[[], None]:
        """
        Prepare a complete custom-field replacement without publishing it.

        Call the returned operation only after commit. Discard it on rollback.
        Standard schema descriptors survive replacement, and alias ambiguity is
        recomputed from the complete active definition set.

        Example:
            A late callback for an older preparation cannot overwrite a newer publication.


        :param definitions: Complete custom-field population; retired definitions are
            filtered out.
        :return: Publication callback with a monotonically ordered registry ticket.
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
            """
            Publish this prepared snapshot only if its ticket is newer.

            Example:
                Publishing the same prepared callback twice advances generation only once.


            :return: None; atomically publishes the prepared snapshot or ignores a stale
                ticket.
            """
            with self._lock:
                if ticket <= self._published:
                    return
                self._snapshot = FieldMetadataSnapshot(
                    self._snapshot.generation + 1, frozen_fields, frozen_aliases
                )
                self._published = ticket

        return publish
