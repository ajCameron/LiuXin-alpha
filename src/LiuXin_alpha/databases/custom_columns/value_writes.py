"""Own legacy cached value writes and restore touched cache rows on rollback.

A cache cannot follow an arbitrary caller's later rollback. Cached facade writers
therefore require an idle connection and a complete commit. Lower-level SQL macros
remain available for caller-owned transactions. Hosts must serialize these writes
with cache readers; their dirty hooks must honor commit=False and avoid external
side effects. Listener delivery happens after commit and is not transactional.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, MutableSequence
from copy import deepcopy
from functools import wraps
from typing import Any, Concatenate

from LiuXin_alpha.databases.api.custom_columns_api import (
    CustomColumnCache,
    CustomColumnRows,
    CustomColumnsAPI,
)
from LiuXin_alpha.databases.transactions import transaction_active, transaction_scope


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


def atomic_custom_write[HostT: CustomColumnsAPI, **P, R](
    operation: Callable[Concatenate[HostT, P], R],
) -> Callable[Concatenate[HostT, P], R]:
    """Wrap a cached writer in one SQL transaction and buffered notification unit."""

    @wraps(operation)
    def write(host: HostT, /, *args: P.args, **kwargs: P.kwargs) -> R:
        connection = host._custom_connection()
        if transaction_active(connection):
            raise RuntimeError(
                "Cached custom-column writes require their own transaction"
            )
        cache = host.data
        journaled = _JournaledCache(cache)
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
        try:
            with transaction_scope(connection, owns_transaction=True):
                result = operation(host, *args, **kwargs)
        except BaseException:
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
