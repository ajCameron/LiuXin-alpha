"""Mapping facades over durable storage repository operations.

The facades retain callbacks or a narrow Item-target repository protocol rather
than a second catalogue. Collection operations deliberately reload repository
state and add no transaction or cache of their own.
"""

from __future__ import annotations

import dataclasses
from collections.abc import (
    Callable,
    ItemsView,
    Iterator,
    Mapping,
    MutableMapping,
    ValuesView,
)
from typing import Any, Generic, Protocol, TypeVar, overload

from LiuXin_alpha.storage.api import storage_manager_api as manager_api
from LiuXin_alpha.storage.storage_manager.mixins._types import _ItemTarget

_K = TypeVar("_K")
_V = TypeVar("_V")
_DefaultT = TypeVar("_DefaultT")


class _ItemTargetRepository(Protocol):
    """Declare only the repository operations required by the Item-link mapping.

    Example:
        >>> isinstance(repository.item_targets(), MutableMapping)  # doctest: +SKIP
    """

    def get_item_target(self, key: tuple[manager_api.ItemID, str]) -> _ItemTarget:
        """Return the exact Item-role target or raise KeyError.

        :param key: Exact Item identity and role pair.
        :return: Registered target kind and identity.
        """
        ...

    def upsert_item_target(
        self, value: tuple[tuple[manager_api.ItemID, str], _ItemTarget]
    ) -> None:
        """Insert or replace one Item-role target.

        :param value: Item-role key paired with its target kind and identity.
        :return: None after persistence.
        """
        ...

    def remove_item_target(self, key: tuple[manager_api.ItemID, str]) -> None:
        """Remove every link matching one Item-role key.

        :param key: Exact Item identity and role pair.
        :return: None after removal.
        """
        ...

    def load_item_targets(
        self,
    ) -> dict[tuple[manager_api.ItemID, str], _ItemTarget]:
        """Load a fresh mapping of all Item-role targets.

        :return: Newly loaded Item-role mapping.
        """
        ...


class RepositoryRecordMapping(MutableMapping[_K, _V], Generic[_K, _V]):
    """
    Adapt repository callables to a mutable mapping without retaining records.

    Each lookup or collection operation calls its supplied provider afresh. A write invokes upsert,
    and deletion first requires the value to exist. This facade adds no transaction, lock, commit,
    revision check, or rollback: those guarantees belong to the bound callables and their enclosing
    context. Its collection views belong to the mapping returned by load_all, not this facade.

    Example:
        >>> records = {}
        >>> mapping = RepositoryRecordMapping(
        ...     get_one=records.__getitem__, load_all=lambda: dict(records),
        ...     upsert=lambda value: records.update({value[0]: value}),
        ...     remove=records.__delitem__, key_of=lambda value: value[0],
        ... )
        >>> mapping[7] = (7, "book")
        >>> mapping[7]
        (7, 'book')
    """

    def __init__(
        self,
        *,
        get_one: Callable[[_K], _V],
        load_all: Callable[[], Mapping[_K, _V]],
        upsert: Callable[[_V], None],
        remove: Callable[[_K], None],
        key_of: Callable[[_V], _K] | None = None,
    ) -> None:
        """
        Retain repository callbacks without calling or validating them. key_of optionally protects
        assignment identity; it is not used for reads or deletions.

        Example:
            >>> mapping = RepositoryRecordMapping(get_one=get_one, load_all=load_all, upsert=save, remove=remove)  # doctest: +SKIP


        :param get_one: Callable loading one key and raising KeyError when absent.
        :param load_all: Callable returning the mapping used for each fresh collection view or size query.
        :param upsert: Callable persisting the supplied value; the assignment key is not passed separately.
        :param remove: Callable removing one key after a successful lookup.
        :param key_of: Optional value-to-key extractor whose result must equal the assignment key.
        :return: None after retaining the callbacks.
        """

        self._get_one = get_one
        self._load_all = load_all
        self._upsert = upsert
        self._remove = remove
        self._key_of = key_of

    def __getitem__(self, key: _K) -> _V:
        """
        Load one record through the bound provider. Propagate KeyError and all other provider
        failures without substituting defaults or caching the result.

        Example:
            >>> value = mapping[key]  # doctest: +SKIP


        :param key: Repository key forwarded without coercion.
        :return: The provider-supplied record value.
        """

        return self._get_one(key)

    def __setitem__(self, key: _K, value: _V) -> None:
        """
        Check key_of(value) against key when configured, then call upsert(value). A mismatch raises
        ValueError before writing. Without key_of, the key is ignored by the write callback;
        persistence and commit timing belong to the provider.

        Example:
            >>> mapping[key] = value  # doctest: +SKIP


        :param key: Repository key forwarded without coercion.
        :param value: Complete value passed to the upsert callback.
        :return: None after the provider write returns; this wrapper does not commit a surrounding transaction.
        """

        if self._key_of is not None and self._key_of(value) != key:
            raise ValueError("repository mapping key does not match record identity.")
        self._upsert(value)

    def __delitem__(self, key: _K) -> None:
        """
        Load the record to require its existence, then invoke removal. The read and remove calls are
        separate and not protected by this wrapper; later failures propagate after any provider side
        effects.

        Example:
            >>> del mapping[key]  # doctest: +SKIP


        :param key: Repository key forwarded without coercion.
        :return: None after successful lookup and removal.
        """

        self._get_one(key)
        self._remove(key)

    def __iter__(self) -> Iterator[_K]:
        """
        Call the load_all callback immediately and return an iterator over that mapping's keys.
        Ordering and snapshot stability belong to the returned mapping; later facade calls load
        independently.

        Example:
            >>> keys = tuple(mapping)  # doctest: +SKIP


        :return: An iterator over keys in the freshly obtained provider mapping.
        """

        return iter(self._load_all())

    def __len__(self) -> int:
        """
        Call the load_all callback and return its size. This can load every value and does not use a
        cached count or a dedicated database COUNT query.

        Example:
            >>> count = len(mapping)  # doctest: +SKIP


        :return: The number of entries in the newly obtained provider mapping.
        """

        return len(self._load_all())

    def __contains__(self, key: object) -> bool:
        """
        Attempt get_one(key), returning False only for KeyError. No key coercion or type validation
        is added; provider errors of other types propagate, and an existing value of None still
        counts as present.

        Example:
            >>> present = key in mapping  # doctest: +SKIP


        :param key: Candidate lookup key, forwarded even when its runtime type differs from the annotation.
        :return: True when lookup returns successfully, otherwise False for KeyError.
        """

        try:
            self._get_one(key)  # type: ignore[arg-type]
        except KeyError:
            return False
        return True

    def get(self, key: _K, default: Any = None) -> _V | Any:
        """
        Load the requested key and substitute default only for KeyError. Other decoding, database,
        or provider failures remain visible, and no result is cached by the facade.

        Example:
            >>> value = mapping.get(key, None)  # doctest: +SKIP


        :param key: Repository key forwarded without coercion.
        :param default: Value returned unchanged when the lookup raises KeyError; defaults to None.
        :return: The loaded value, or the exact supplied default for a missing key.
        """

        try:
            return self._get_one(key)
        except KeyError:
            return default

    def values(self) -> ValuesView[_V]:
        """
        Call the load_all callback and return its values view. The view belongs to that returned
        mapping; this method does not retain it or promise live updates from subsequent repository
        writes.

        Example:
            >>> snapshot = tuple(mapping.values())  # doctest: +SKIP


        :return: The provider mapping view containing its values.
        """

        return self._load_all().values()

    def items(self) -> ItemsView[_K, _V]:
        """
        Call the load_all callback and return its items view. The view belongs to that returned
        mapping; this method does not retain it or promise live updates from subsequent repository
        writes.

        Example:
            >>> snapshot = tuple(mapping.items())  # doctest: +SKIP


        :return: The provider mapping view containing its key/value pairs.
        """

        return self._load_all().items()

    def pop(self, key: _K, default: Any = dataclasses.MISSING) -> _V | Any:
        """
        Load and remove one value, substituting a default only when lookup raises KeyError.

        The dataclasses.MISSING sentinel means no default, even if passed explicitly; that path
        raises a new KeyError(key). A successful lookup is followed by a separate remove call.
        Removal errors propagate rather than returning default, and a provider whose remove callback
        is a no-op retains the entry.

        Example:
            >>> previous = mapping.pop(key, None)  # doctest: +SKIP


        :param key: Repository key forwarded without coercion.
        :param default: Missing-key result, or dataclasses.MISSING to require the key.
        :return: The value loaded before removal, or the supplied default when lookup reports absence.
        """

        try:
            value = self._get_one(key)
        except KeyError:
            if default is dataclasses.MISSING:
                raise KeyError(key) from None
            return default
        self._remove(key)
        return value


class RepositoryItemTargetMapping(
    MutableMapping[tuple[manager_api.ItemID, str], _ItemTarget]
):
    """
    Present role-keyed Item links as a mapping over the database repository.

    Keys are (ItemID, role) pairs and values identify an Asset or Composite. The facade retains only
    its repository; collection access reloads both link tables. It adds no transaction or
    normalization, and failed writes/deletions retain the repository's partial-failure and
    backend-constraint behavior.

    Example:
        >>> target = repository.item_targets().get((manager_api.ItemID(7), "cover"))  # doctest: +SKIP
    """

    def __init__(
        self,
        *,
        repository: _ItemTargetRepository,
    ) -> None:
        """
        Retain the metadata repository without loading links or opening a transaction.

        Example:
            >>> mapping = RepositoryItemTargetMapping(repository=repository)  # doctest: +SKIP


        :param repository: Authoritative metadata adapter supplying Item-link reads and writes.
        :return: None after retaining the repository.
        """

        self._repository = repository

    def __getitem__(self, key: tuple[manager_api.ItemID, str]) -> _ItemTarget:
        """
        Load one Item target through the bound provider. Propagate KeyError and all other provider
        failures without substituting defaults or caching the result.

        Example:
            >>> value = mapping[key]  # doctest: +SKIP


        :param key: Pair of Item identity and exact role text.
        :return: The provider-supplied Item target value.
        """

        return self._repository.get_item_target(key)

    def __setitem__(
        self,
        key: tuple[manager_api.ItemID, str],
        value: _ItemTarget,
    ) -> None:
        """
        Pass the key/value pair to upsert_item_target. The repository replaces matching role links
        in its transaction; this wrapper does not validate the kind, normalize the role, or resolve
        referenced objects.

        Example:
            >>> mapping[key] = value  # doctest: +SKIP


        :param key: Pair of Item identity and exact role text.
        :param value: Pair of target kind and Asset or Composite identity.
        :return: None after the provider write returns; this wrapper does not commit a surrounding transaction.
        """

        self._repository.upsert_item_target((key, value))

    def __delitem__(self, key: tuple[manager_api.ItemID, str]) -> None:
        """
        Load the Item target to require its existence, then invoke removal. The read and remove
        calls are separate and not protected by this wrapper; later failures propagate after any
        provider side effects.

        Example:
            >>> del mapping[key]  # doctest: +SKIP


        :param key: Pair of Item identity and exact role text.
        :return: None after successful lookup and removal.
        """

        self._repository.get_item_target(key)
        self._repository.remove_item_target(key)

    def __iter__(self) -> Iterator[tuple[manager_api.ItemID, str]]:
        """
        Call the repository link loader immediately and return an iterator over that mapping's keys.
        Ordering and snapshot stability belong to the returned mapping; later facade calls load
        independently.

        Example:
            >>> keys = tuple(mapping)  # doctest: +SKIP


        :return: An iterator over keys in the freshly obtained provider mapping.
        """

        return iter(self._repository.load_item_targets())

    def __len__(self) -> int:
        """
        Call the repository link loader and return its size. This can load every value and does not
        use a cached count or a dedicated database COUNT query.

        Example:
            >>> count = len(mapping)  # doctest: +SKIP


        :return: The number of entries in the newly obtained provider mapping.
        """

        return len(self._repository.load_item_targets())

    @overload
    def get(
        self,
        key: tuple[manager_api.ItemID, str],
    ) -> _ItemTarget | None:
        """Describe lookup without an explicit default for static type checkers.

        Example:
            >>> mapping.get(key)  # doctest: +SKIP


        :param key: Exact Item identity and role pair to retrieve.
        :return: Registered target, or None when the key is absent.
        """
        ...

    @overload
    def get(
        self,
        key: tuple[manager_api.ItemID, str],
        default: _ItemTarget,
    ) -> _ItemTarget:
        """Describe lookup with an Item-target default for static type checkers.

        Example:
            >>> mapping.get(key, other_target)  # doctest: +SKIP


        :param key: Exact Item identity and role pair to retrieve.
        :param default: Item target returned when the key is absent.
        :return: Registered target or the supplied Item-target default.
        """
        ...

    @overload
    def get(
        self,
        key: tuple[manager_api.ItemID, str],
        default: _DefaultT,
    ) -> _ItemTarget | _DefaultT:
        """Describe lookup with a caller-selected default for static type checkers.

        Example:
            >>> mapping.get(key, fallback)  # doctest: +SKIP


        :param key: Exact Item identity and role pair to retrieve.
        :param default: Caller-selected value returned when the key is absent.
        :return: Registered target or the supplied default.
        """
        ...

    def get(
        self,
        key: tuple[manager_api.ItemID, str],
        default: _DefaultT | None = None,
    ) -> _ItemTarget | _DefaultT | None:
        """
        Load the requested key and substitute default only for KeyError. Other decoding, database,
        or provider failures remain visible, and no result is cached by the facade.

        Example:
            >>> value = mapping.get(key, None)  # doctest: +SKIP


        :param key: Pair of Item identity and exact role text.
        :param default: Value returned unchanged when the lookup raises KeyError; defaults to None.
        :return: The loaded value, or the exact supplied default for a missing key.
        """

        try:
            return self._repository.get_item_target(key)
        except KeyError:
            return default

    def values(self) -> ValuesView[_ItemTarget]:
        """
        Call the repository link loader and return its values view. The view belongs to that
        returned mapping; this method does not retain it or promise live updates from subsequent
        repository writes.

        Example:
            >>> snapshot = tuple(mapping.values())  # doctest: +SKIP


        :return: The provider mapping view containing its values.
        """

        return self._repository.load_item_targets().values()

    def items(self) -> ItemsView[tuple[manager_api.ItemID, str], _ItemTarget]:
        """
        Call the repository link loader and return its items view. The view belongs to that returned
        mapping; this method does not retain it or promise live updates from subsequent repository
        writes.

        Example:
            >>> snapshot = tuple(mapping.items())  # doctest: +SKIP


        :return: The provider mapping view containing its key/value pairs.
        """

        return self._repository.load_item_targets().items()

    def pop(
        self,
        key: tuple[manager_api.ItemID, str],
        default: Any = dataclasses.MISSING,
    ) -> _ItemTarget | Any:
        """
        Load and remove one value, substituting a default only when lookup raises KeyError.

        The dataclasses.MISSING sentinel means no default, even if passed explicitly; that path
        raises a new KeyError(key). A successful lookup is followed by a separate remove call.
        Removal errors propagate rather than returning default, and a provider whose remove callback
        is a no-op retains the entry.

        Example:
            >>> previous = mapping.pop(key, None)  # doctest: +SKIP


        :param key: Pair of Item identity and exact role text.
        :param default: Missing-key result, or dataclasses.MISSING to require the key.
        :return: The value loaded before removal, or the supplied default when lookup reports absence.
        """

        try:
            value = self._repository.get_item_target(key)
        except KeyError:
            if default is dataclasses.MISSING:
                raise KeyError(key) from None
            return default
        self._repository.remove_item_target(key)
        return value


__all__ = ["RepositoryItemTargetMapping", "RepositoryRecordMapping"]
