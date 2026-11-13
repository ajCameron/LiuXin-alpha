"""Own the in-memory projection of native custom-field definitions.

Use ``write()`` to prepare metadata in the SQL transaction and publish it after
commit. Direct native service writes deliberately do not refresh this projection.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from LiuXin_alpha.databases.custom_columns.native_repository import names
from LiuXin_alpha.databases.custom_columns.native_service import CustomFieldService
from LiuXin_alpha.databases.transaction_participants import after_commit

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.field_metadata import FieldMetadata


class CustomFieldCache:
    """Coordinate native writes with a caller-owned field metadata projection."""

    def __init__(self, service: CustomFieldService, field_metadata: FieldMetadata):
        self.service = service
        self.field_metadata = field_metadata

    @contextmanager
    def write(self) -> Iterator[CustomFieldService]:
        """Roll back on preparation failure; publish only after the outer commit.

        Nested scopes follow savepoint rollback and publish in completion order.
        Receipt listeners describe committed SQL and may run before this projection.
        """
        with self.service.write():
            yield self.service
            after_commit(self.service.db.macros, self.prepare())

    def refresh(self) -> None:
        """Refresh from a snapshot, deferring publication when already in a transaction."""
        with self.service.read():
            after_commit(self.service.db.macros, self.prepare())

    def prepare(self) -> Callable[[], None]:
        """Validate a descriptor snapshot without changing the visible metadata."""
        definitions = self.service.list_fields()
        fields = [
            dict(
                label=f"{d.attachment}.{d.label}",
                table=names(d)[0],
                column="value",
                datatype=d.datatype,
                colnum=d.id,
                name=d.name,
                display=d.display,
                is_multiple={}
                if d.mode == "scalar"
                else {"cache_to_list": "|", "ui_to_list": ",", "list_to_ui": ", "},
                is_category=d.normalized,
                is_editable=d.editable,
                is_csp=False,
                in_table=d.attachment,
            )
            for d in definitions
        ]
        publish = self.field_metadata.prepare_custom_fields(
            "*", fields, series_companions=False
        )

        def complete() -> None:
            publish()
            aliases = self.field_metadata._search_term_map
            for d in definitions:
                descriptor = self.field_metadata._tb_cats[d.key]
                descriptor["collection_mode"] = d.mode
                descriptor["revision"] = d.revision
            for d in definitions:
                alias = "#" + d.label
                aliases.pop(alias, None)
                if sum(other.label == d.label for other in definitions) == 1:
                    aliases[alias] = d.key

        return complete
