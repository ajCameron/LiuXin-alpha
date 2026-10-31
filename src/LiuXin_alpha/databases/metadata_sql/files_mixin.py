"""Update stored file rows through the portable SQL transaction owner.

The physical ``files`` table has ``file_id`` and ``file_size_bytes`` columns.
Its row IDs are distinct from WEMI entities, projection rays, assets and replicas.
These operations change database records only; storage owns filesystem effects.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING

from LiuXin_alpha.databases.db_types.macro_types import LINK_TYPE_UNSET, LinkedTableRow

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class FilesSQLMixin:
    """Provide table-scoped file writes, joining any outer macro transaction."""

    db: DatabaseAPI

    def set_file_name(self, file_id: int, new_fname: str) -> None:
        """Update ``files.file_name`` for a file row; do not rename any bytes."""
        self.db.macros.update_row("files", file_id, {"file_name": new_fname})

    def set_file_size(self, file_id: int, size: int) -> None:
        """Record ``file_size_bytes`` without measuring files or updating assets."""
        self.db.macros.update_row("files", file_id, {"file_size_bytes": size})

    def set_file_size_and_name(self, file_id: int, size: int, fname: str) -> None:
        """Update a file row's recorded name and byte count in one statement."""
        self.db.macros.update_row(
            "files", file_id, {"file_name": fname, "file_size_bytes": size}
        )

    def delete_file_by_id(self, file_id: int) -> None:
        """Delete a file row by its ID; schema cascades apply, bytes are untouched."""
        self.db.macros.delete_row("files", file_id)

    def delete_files_by_id(
        self, file_ids: Iterable[int | tuple[int] | list[int]]
    ) -> None:
        """Atomically delete file rows from IDs or one-cell bindings.

        Materialize and validate the entire batch before deletion. Missing IDs
        are no-ops. Schema cascades apply; physical files are never removed.
        """
        ids = []
        for value in file_ids:
            if isinstance(value, int):
                ids.append(value)
            else:
                if isinstance(value, (str, bytes)):
                    raise TypeError("deletion bindings must be parameter sequences")
                binding = tuple(value)
                if len(binding) != 1:
                    raise ValueError("deletion binding width does not match columns")
                ids.append(binding[0])
        self.db.macros.delete_rows_by_id("files", ids)

    def update_files_bulk(
        self, updates: Mapping[int, Mapping[str, str | int | None]]
    ) -> None:
        """Atomically update recorded file names/sizes by physical file ID.

        Payloads may contain file_name and/or file_size_bytes. Empty payloads
        and missing IDs are no-ops. Schema constraints apply; bytes are untouched.
        """
        materialized = {row_id: dict(values) for row_id, values in updates.items()}
        for values in materialized.values():
            if set(values) - {"file_name", "file_size_bytes"}:
                raise ValueError(
                    "File batches accept file_name and file_size_bytes only"
                )
        self.db.macros.update_rows("files", materialized)

    def get_files_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical files destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """
        spec = self.db.driver_wrapper.get_link_spec(source_table, "files")
        return self.db.macros.get_linked_rows(spec, source_id, link_type=link_type)
