"""Small lazy mapping used by database-backed metadata containers."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Iterator, Mapping, MutableMapping
from copy import deepcopy
from typing import Any


class LazyValueToID(MutableMapping[str, Any]):
    """
    Mapping-compatible lazy ``value -> row_id`` container.

    The loader is called at most once. Copying or iterating the mapping
    materializes it, which keeps the existing metadata attribute surface plain
    for callers while letting lazy metadata objects avoid relation queries until
    a field is read.
    """

    def __init__(self, loader: Callable[[], Mapping[str, Any]], *, label: str) -> None:
        self._loader = loader
        self._label = str(label)
        self._loaded = False
        self._values: OrderedDict[str, Any] = OrderedDict()

    @property
    def label(self) -> str:
        return self._label

    @property
    def loaded(self) -> bool:
        return self._loaded

    def materialize(self) -> OrderedDict[str, Any]:
        if not self._loaded:
            self._values = OrderedDict(self._loader())
            self._loaded = True
        return self._values

    def __getitem__(self, key: str) -> Any:
        return self.materialize()[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.materialize()[key] = value

    def __delitem__(self, key: str) -> None:
        del self.materialize()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.materialize())

    def __len__(self) -> int:
        return len(self.materialize())

    def __bool__(self) -> bool:
        return bool(self.materialize())

    def __deepcopy__(self, memo: dict[int, Any]) -> OrderedDict[str, Any]:
        return deepcopy(self.materialize(), memo)

    def __repr__(self) -> str:
        if self._loaded:
            return repr(self._values)
        return f"<lazy {self._label}>"

    def __str__(self) -> str:
        return repr(self)


__all__ = ["LazyValueToID"]
