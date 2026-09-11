"""
Retain legacy per-file status and backend callbacks without metadata ownership.

SingleFileStatus caches supplied/probed facts independently of catalogue Asset
identity. Its completion flag, private existence cache, and construction-time
last_checked value must not be mistaken for current integrity evidence.
"""

from __future__ import annotations

from typing import Callable, Optional

import time


class SingleFileStatus:
    """
    Cache backend-supplied file status behind read-only public properties. Initialization obtains
    missing existence, size, and hash values through borrowed callbacks in that order, without
    skipping later checks when the file is absent. It does not validate returned values, UUID
    syntax, URL reachability, or a digest algorithm. The UUID can remain None despite its property's
    str annotation. last_checked is assigned once at construction unless the caller changes it
    directly; rechecks never advance it.

    Callback replacement and rechecks mutate this object without synchronization or rollback.
    Existence is held in _exists without a public property; recheck_self reports normal completion
    rather than existence or change.

    Example:
        >>> state = SingleFileStatus(
        ...     "memory:item", exists=True, size=4, file_hash="backend-hash",
        ...     check_exists_function=lambda url: True,
        ...     check_size_function=lambda url: 4,
        ...     check_hash_function=lambda url: "backend-hash",
        ... )
        >>> state.size, state.uuid is None
        (4, True)
    """
    _exists: bool    # - Does the file, you know, exist?

    _uuid: str      # - If you want to do anything with a folder store, you need the file uuid
    _size: int      # - File size in bytes
    _hash: str      # - Hash of the file itself
    _url: str       # - Some form of resource ULR to get at the file

    last_checked: Optional[float]   # - When did we last KNOW we had the file?

    # These are all STORAGE BACKEND functions which are bound into this class to permit object checks
    _check_exists_function: Callable[[str], bool]
    _check_size_function: Callable[[str], int]
    _check_hash_function: Callable[[str], str]

    def __init__(self,
                 url: str,
                 exists: Optional[bool] = None,
                 size: Optional[int] = None,
                 file_hash: Optional[str] = None,
                 uuid: Optional[str] = None,
                 last_checked: Optional[float] = None,
                 check_exists_function: Optional[Callable[[str], bool]] = None,
                 check_size_function: Optional[Callable[[str], int]] = None,
                 check_hash_function: Optional[Callable[[str], str]] = None,
                 ) -> None:
        """
        Retain supplied file facts and fetch only values that are None through callbacks. Assert all
        three callbacks are non-None even when every fact is supplied; these assertions do not check
        callability and can be disabled by optimized Python. Missing facts are fetched existence,
        size, then hash, independently of an absent-file result. Supplied false/zero/empty-string
        facts are retained. Callback errors propagate after any earlier assignments. Finally retain
        the callbacks and supplied UUID, then use last_checked or the current epoch time in seconds.
        No supplied value is otherwise validated or normalized.

        Example:
            >>> state = SingleFileStatus(url, check_exists_function=exists, check_size_function=size, check_hash_function=digest)  # doctest: +SKIP


        :param url: Opaque resource identifier retained and passed unchanged to each invoked backend callback.
        :param exists: Optional cached existence value; only None triggers the existence callback.
        :param size: Optional cached byte count; only None triggers the size callback.
        :param file_hash: Optional backend-defined digest text; only None triggers the hash callback.
        :param uuid: Optional backend file identifier retained verbatim, with None permitted at runtime.
        :param last_checked: Optional observation time retained verbatim; None captures float(time.time()) in epoch seconds.
        :param check_exists_function: Non-None backend callback receiving url to obtain existence; retained for later rechecks.
        :param check_size_function: Non-None backend callback receiving url to obtain byte size, even when existence is false.
        :param check_hash_function: Non-None backend callback receiving url to obtain a backend-defined hash.
        :return: None after initialization; failed assertions or callbacks can interrupt construction after partial assignment.
        """
        assert check_exists_function is not None, "check_exists_function is not defined"
        assert check_size_function is not None, "check_size_function is not defined"
        assert check_hash_function is not None, "check_hash_function is not defined"

        self._url = url

        if exists is not None:
            self._exists = exists
        else:
            self._exists = check_exists_function(url)

        if size is not None:
            self._size = size
        else:
            self._size = check_size_function(url)

        if file_hash is not None:
            self._hash = file_hash
        else:
            self._hash = check_hash_function(url)

        self._uuid = uuid

        self._check_exists_function = check_exists_function
        self._check_size_function = check_size_function
        self._check_hash_function = check_hash_function

        if last_checked is not None:
            self.last_checked = last_checked
        else:
            self.last_checked = float(time.time())


    @property
    def uuid(self) -> str:
        """
        Return the retained backend identifier without lookup or UUID validation. The value may be
        None despite the str return annotation.

        Example:
            >>> value = state.uuid  # doctest: +SKIP


        :return: The originally supplied identifier, without conversion or copying.
        """
        return self._uuid

    @uuid.setter
    def uuid(self, value: str) -> None:
        """
        Reject assignment to the public uuid property without mutating cached state. Private
        attributes and callback-driven updates remain separate mechanisms.

        Example:
            >>> state.uuid = replacement  # doctest: +SKIP


        :param value: Proposed replacement, ignored because public assignment is rejected.
        :return: Never returns normally; always raises AttributeError with the existing cannot-set-uuid message.
        """
        raise AttributeError("Cannot set the uuid manually.")

    @property
    def size(self) -> int:
        """
        Return the cached byte count without invoking the size callback. It reflects initialization
        or the last selected size recheck, not necessarily current bytes.

        Example:
            >>> value = state.size  # doctest: +SKIP


        :return: The retained backend/supplied size value, without validation or refresh.
        """
        return self._size

    @size.setter
    def size(self, size: int) -> None:
        """
        Reject assignment to the public size property without mutating cached state. Private
        attributes and callback-driven updates remain separate mechanisms.

        Example:
            >>> state.size = replacement  # doctest: +SKIP


        :param size: Proposed replacement, ignored because public assignment is rejected.
        :return: Never returns normally; always raises AttributeError with the existing cannot-set-size message.
        """
        raise AttributeError("Cannot set the size manually.")

    @property
    def hash(self) -> str:
        """
        Return the cached backend-defined hash without reading bytes or checking an algorithm. No
        new integrity evidence is acquired.

        Example:
            >>> value = state.hash  # doctest: +SKIP


        :return: The retained hash text, without verification or refresh.
        """
        return self._hash

    @hash.setter
    def hash(self, value: str) -> None:
        """
        Reject assignment to the public hash property without mutating cached state. Private
        attributes and callback-driven updates remain separate mechanisms.

        Example:
            >>> state.hash = replacement  # doctest: +SKIP


        :param value: Proposed replacement, ignored because public assignment is rejected.
        :return: Never returns normally; always raises AttributeError with the existing cannot-set-hash message.
        """
        raise AttributeError("Cannot set the hash manually.")

    @property
    def url(self) -> str:
        """
        Return the opaque resource identifier retained at initialization. No path parsing,
        resolution, or backend lookup occurs.

        Example:
            >>> value = state.url  # doctest: +SKIP


        :return: The retained URL value, without conversion or refresh.
        """
        return self._url

    @url.setter
    def url(self, value: str) -> None:
        """
        Reject assignment to the public url property without mutating cached state. Private
        attributes and callback-driven updates remain separate mechanisms.

        Example:
            >>> state.url = replacement  # doctest: +SKIP


        :param value: Proposed replacement, ignored because public assignment is rejected.
        :return: Never returns normally; always raises AttributeError with the existing cannot-set-url message.
        """
        raise AttributeError("Cannot set the url manually.")

    def update_check_exists_function(self, check_exists_function: Callable[[str], bool]) -> None:
        """
        Replace the stored exists callback without invoking it, checking callability, or changing
        cached facts or last_checked. The next selected recheck uses this reference and propagates
        its errors.

        Example:
            >>> state.update_check_exists_function(replacement)  # doctest: +SKIP


        :param check_exists_function: Replacement backend callback expected to accept the retained URL; assigned without runtime validation.
        :return: None after retaining the replacement callback.
        """
        self._check_exists_function = check_exists_function

    def update_check_size_function(self, check_size_function: Callable[[str], int]) -> None:
        """
        Replace the stored size callback without invoking it, checking callability, or changing
        cached facts or last_checked. The next selected recheck uses this reference and propagates
        its errors.

        Example:
            >>> state.update_check_size_function(replacement)  # doctest: +SKIP


        :param check_size_function: Replacement backend callback expected to accept the retained URL; assigned without runtime validation.
        :return: None after retaining the replacement callback.
        """
        self._check_size_function = check_size_function

    def update_check_hash_function(self, check_hash_function: Callable[[str], str]) -> None:
        """
        Replace the stored hash callback without invoking it, checking callability, or changing
        cached facts or last_checked. The next selected recheck uses this reference and propagates
        its errors.

        Example:
            >>> state.update_check_hash_function(replacement)  # doctest: +SKIP


        :param check_hash_function: Replacement backend callback expected to accept the retained URL; assigned without runtime validation.
        :return: None after retaining the replacement callback.
        """
        self._check_hash_function = check_hash_function

    def recheck_self(self, all: bool = False, exists: bool = False, size: bool = False, hash: bool = False) -> bool:
        """
        Refresh selected cached facts in existence, size, hash order without changing last_checked.
        A truthy all flag invokes all three callbacks and ignores individual flags. Otherwise invoke
        only selected checks; no flags performs no I/O. An absent existence result does not suppress
        size/hash calls. Assign each successful result immediately, so a later failure retains
        earlier updates and propagates. Normal completion always returns True, including no-op or
        absent-file cases; it is neither existence evidence nor a changed-state indicator.

        Example:
            >>> completed = state.recheck_self(exists=True)  # doctest: +SKIP


        :param all: Whether to refresh every fact, overriding individual selection flags.
        :param exists: Whether to refresh _exists when all is falsey.
        :param size: Whether to refresh _size when all is falsey.
        :param hash: Whether to refresh _hash when all is falsey.
        :return: True after normal completion; callback errors propagate and can leave a partially refreshed cache.
        """
        # Check to see if everything is negative

        if all:
            self._exists = self._check_exists_function(self._url)
            self._size = self._check_size_function(self._url)
            self._hash = self._check_hash_function(self._url)
            return True

        if exists:
            self._exists = self._check_exists_function(self._url)

        if size:
            self._size = self._check_size_function(self._url)

        if hash:
            self._hash = self._check_hash_function(self._url)

        return True
