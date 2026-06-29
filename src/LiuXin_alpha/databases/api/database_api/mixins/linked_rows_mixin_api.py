from __future__ import annotations

import abc
from typing import Any, Iterable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import RowAPI


class DatabaseLinkedRowsMixinAPI(abc.ABC):
    """Typed API for helper methods that fetch rows linked to a seed row."""

    @abc.abstractmethod
    def get_linked_rows(
        self,
        seed_row: "RowAPI | dict[str, Any]",
        target_table: str,
        *,
        type_filter: Optional[str] = None,
    ) -> list["RowAPI"]:
        """Return rows in ``target_table`` linked to ``seed_row``.

        Implementations should accept either a live :class:`RowAPI` or a plain row dict.
        If ``target_table`` is the seed row's own table, the seed row should be returned in a single-item list.
        """

    @abc.abstractmethod
    def get_first_linked_row(
        self,
        seed_row: "RowAPI | dict[str, Any]",
        target_table: str,
        *,
        type_filter: Optional[str] = None,
    ) -> Optional["RowAPI"]:
        """Return the first linked row in ``target_table`` or ``None`` if there is none."""

    @abc.abstractmethod
    def get_linked_ids_set(
        self,
        seed_row: "RowAPI | dict[str, Any]",
        target_table: str,
        *,
        type_filter: Optional[str] = None,
    ) -> set[Any]:
        """Return the ids for rows in ``target_table`` linked to ``seed_row``."""

    @abc.abstractmethod
    def get_linked_fingerprint(
        self,
        seed_row: "RowAPI | dict[str, Any]",
        *,
        target_tables: Optional[Iterable[str]] = None,
        type_filter: Optional[str] = None,
    ) -> set[str]:
        """Return a ``{"table_id"}`` style fingerprint for linked rows."""
