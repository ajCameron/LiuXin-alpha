"""
Provide a mutable value-to-id mapping that loads its contents on first use.

Successful materialization is cached. Read, mutation, iteration, length, truth
testing and deepcopy may invoke the loader; label, loaded and an unloaded repr do
not.

Example:
    >>> values = LazyValueToID(lambda: {"tag": 7}, label="tags")
    >>> values.loaded
    False
    >>> values["tag"]
    7
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Iterator, Mapping, MutableMapping
from copy import deepcopy
from typing import Any


class LazyValueToID(MutableMapping[str, Any]):
    """
    Defer an ordered mapping until the first operation that needs its contents.

    A successful loader call is cached; a failure leaves the wrapper unloaded so a later
    access retries. This class provides no synchronization for concurrent
    materialization. Deepcopy returns a plain OrderedDict.

    Example:
        >>> values = LazyValueToID(lambda: {"tag": 7}, label="tags")
        >>> repr(values)
        '<lazy tags>'
        >>> list(values)
        ['tag']
    """

    def __init__(self, loader: Callable[[], Mapping[str, Any]], *, label: str) -> None:
        """
        Retain the loader and display label without reading any values.

        Example:
            >>> values = LazyValueToID(lambda: {"tag": 7}, label="tags")
            >>> values.loaded
            False


        :param loader: Zero-argument callable returning mapping entries when materialized.
        :param label: Display label converted to str for the unloaded placeholder.
        :return: None.
        """
        self._loader = loader
        self._label = str(label)
        self._loaded = False
        self._values: OrderedDict[str, Any] = OrderedDict()

    @property
    def label(self) -> str:
        """
        Read the stored display label without triggering the loader.

        Example:
            >>> values = LazyValueToID(lambda: {"tag": 7}, label="tags")
            >>> values.label
            'tags'


        :return: String label.
        """
        return self._label

    @property
    def loaded(self) -> bool:
        """
        Report whether materialization has completed successfully.

        Example:
            >>> values = LazyValueToID(lambda: {"tag": 7}, label="tags")
            >>> values.loaded
            False


        :return: True after a successful load; False before loading or after a failed
            attempt.
        """
        return self._loaded

    def materialize(self) -> OrderedDict[str, Any]:
        """
        Load and cache an OrderedDict, or return the existing live mapping.

        The loaded flag is set only after mapping construction succeeds; exceptions
        propagate and leave loading retryable.

        Example:
            >>> values = LazyValueToID(lambda: {"tag": 7}, label="tags")
            >>> data = values.materialize()
            >>> data is values.materialize()
            True


        :return: Live cached OrderedDict.
        """
        if not self._loaded:
            self._values = OrderedDict(self._loader())
            self._loaded = True
        return self._values

    def __getitem__(self, key: str) -> Any:
        """
        Materialize the mapping and look up an exact key.

        Example:
            >>> values = LazyValueToID(lambda: {"tag": 7}, label="tags")
            >>> values["tag"]
            7


        :param key: Exact key to retrieve.
        :return: Stored value; missing keys raise KeyError.
        """
        return self.materialize()[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """
        Materialize the mapping before inserting or replacing one entry.

        Example:
            >>> values = LazyValueToID(lambda: {"tag": 7}, label="tags")
            >>> values["other"] = 8
            >>> list(values.items())
            [('tag', 7), ('other', 8)]


        :param key: Key to insert or replace.
        :param value: Value retained without copying.
        :return: None.
        """
        self.materialize()[key] = value

    def __delitem__(self, key: str) -> None:
        """
        Materialize the mapping before removing one exact key.

        A missing key raises KeyError.

        Example:
            >>> values = LazyValueToID(lambda: {"tag": 7}, label="tags")
            >>> del values["tag"]
            >>> len(values)
            0


        :param key: Exact key to remove.
        :return: None.
        """
        del self.materialize()[key]

    def __iter__(self) -> Iterator[str]:
        """
        Materialize the mapping and iterate over keys in insertion order.

        Example:
            >>> values = LazyValueToID(lambda: {"tag": 7}, label="tags")
            >>> list(values)
            ['tag']


        :return: Iterator over the live mapping keys.
        """
        return iter(self.materialize())

    def __len__(self) -> int:
        """
        Materialize the mapping and count its entries.

        Example:
            >>> values = LazyValueToID(lambda: {"tag": 7}, label="tags")
            >>> len(values)
            1


        :return: Number of stored entries.
        """
        return len(self.materialize())

    def __bool__(self) -> bool:
        """
        Materialize the mapping and test whether it contains any entries.

        Example:
            >>> values = LazyValueToID(lambda: {"tag": 7}, label="tags")
            >>> bool(values)
            True


        :return: True for a nonempty loaded mapping.
        """
        return bool(self.materialize())

    def __deepcopy__(self, memo: dict[int, Any]) -> OrderedDict[str, Any]:
        """
        Materialize and deep-copy the underlying mapping rather than the wrapper.

        Example:
            >>> values = LazyValueToID(lambda: {"tag": 7}, label="tags")
            >>> from copy import deepcopy
            >>> copied = deepcopy(values)
            >>> isinstance(copied, OrderedDict), values.loaded
            (True, True)


        :param memo: Identity memo forwarded to deepcopy for nested keys and values.
        :return: Independent OrderedDict using the supplied deepcopy memo.
        """
        return deepcopy(self.materialize(), memo)

    def __repr__(self) -> str:
        """
        Render the loaded mapping or an unloaded label placeholder.

        An unloaded representation does not invoke the loader.

        Example:
            >>> values = LazyValueToID(lambda: {"tag": 7}, label="tags")
            >>> repr(values), values.loaded
            ('<lazy tags>', False)


        :return: OrderedDict representation or a lazy placeholder string.
        """
        if self._loaded:
            return repr(self._values)
        return f"<lazy {self._label}>"

    def __str__(self) -> str:
        """
        Use the same representation as repr without forcing an unloaded mapping.

        Example:
            >>> values = LazyValueToID(lambda: {"tag": 7}, label="tags")
            >>> str(values)
            '<lazy tags>'


        :return: Loaded mapping representation or lazy placeholder.
        """
        return repr(self)


__all__ = ["LazyValueToID"]
