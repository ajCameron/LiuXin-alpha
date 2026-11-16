"""Own custom value writes and restore optional cache projections on rollback.

A cache cannot follow an arbitrary caller's later rollback. Facade writers
therefore require an idle connection and a complete commit. Native operations remain available for managed transaction composition. Hosts must serialize these writes
with cache readers; their dirty hooks must honor commit=False and avoid external
side effects. Listener delivery happens after commit and is not transactional.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, MutableSequence
from copy import deepcopy
from functools import wraps
from typing import TYPE_CHECKING, Any, cast

from LiuXin_alpha.databases.custom_columns.native_service import CustomFieldService
from LiuXin_alpha.databases.transaction_participants import after_commit
from LiuXin_alpha.databases.transactions import transaction_active
from LiuXin_alpha.utils.calibre_compat.custom_columns.api import (
    CustomColumnCache,
    CustomColumnRows,
)

if TYPE_CHECKING:
    from LiuXin_alpha.utils.calibre_compat.custom_columns.facade import CustomColumns


class _CacheJournal:
    """Remember original row values before either direct or set-based mutation."""

    def __init__(self, cache: CustomColumnCache) -> None:
        self.cache = cache
        self.originals: dict[int, list[Any]] = {}

    def __getitem__(self, owner_id: int, /) -> MutableSequence[Any]:
        row = self.cache._data[owner_id]
        if owner_id not in self.originals:
            self.originals[owner_id] = deepcopy(list(row))
        return row

    def restore(self) -> None:
        """Restore row objects in place, including values modified by a failed set."""
        for owner_id, values in self.originals.items():
            self.cache._data[owner_id][:] = values


class _JournaledCache:
    def __init__(self, cache: CustomColumnCache) -> None:
        self.cache = cache
        self.journal = _CacheJournal(cache)

    @property
    def _data(self) -> CustomColumnRows:
        return self.journal

    def __getitem__(self, position: int, /) -> MutableSequence[Any]:
        return self.cache[position]

    def set(self, row: int, col: int, val: Any, row_is_id: bool = False) -> None:
        if not row_is_id:
            raise ValueError("Custom value writes require persistent owner IDs")
        self.journal[row]
        self.cache.set(row, col, val, row_is_id=True)


def atomic_custom_write[**P, R](
    operation: Callable[P, R],
) -> Callable[P, R]:
    """Wrap a writer in one SQL transaction with optional cache rollback."""

    @wraps(operation)
    def write(*args: P.args, **kwargs: P.kwargs) -> R:
        host = cast("CustomColumns", args[0])
        connection = host._custom_connection()
        if (
            transaction_active(connection)
            or host.db.macros.current_transaction_connection() is not None
        ):
            raise RuntimeError("Custom-column writes require their own transaction")
        cache = host.value_cache
        journaled = _JournaledCache(cache) if cache is not None else None
        notify = host.notify
        notices: list[tuple[str, tuple[int, ...]]] = []

        def defer(event: str, ids: Iterable[int]) -> None:
            notices.append((event, tuple(ids)))

        # Enumeration renaming also mutates a shared allowed-values list.
        enums = []
        for data in host.custom_column_num_map.values():
            values = data.get("display", {}).get("enum_values")
            if isinstance(values, list):
                enums.append((values, values[:]))
        host.data = journaled
        host.notify = defer
        committed = False

        def mark_committed() -> None:
            nonlocal committed
            committed = True

        try:
            with CustomFieldService(host.db).write():
                after_commit(host.db.macros, mark_committed)
                result = operation(*args, **kwargs)
        except BaseException as error:
            if committed:
                error.add_note(
                    "Custom-column SQL committed; projection publication failed."
                )
            else:
                if journaled is not None:
                    journaled.journal.restore()
                for values, previous in enums:
                    values[:] = previous
            raise
        finally:
            host.data = cache
            host.notify = notify
        try:
            for event, ids in notices:
                notify(event, ids)
        except BaseException as error:
            error.add_note(
                "Custom-column SQL committed and cache updated; notification delivery failed. Do not retry the write blindly."
            )
            raise
        return result

    return write
