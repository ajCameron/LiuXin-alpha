from __future__ import annotations

from typing import Callable


class SingleFileStatus:
    """
    Contains the status of a file.

    This contains no metadata at all.
    Only information on the file itself is stored and presented.
    What this information means can be backend dependent.
    """
    _exists: bool    # - Does the file, you know, exist?

    _uuid: str      # - If you want to do anything with a folder store, you need the file uuid
    _size: int      # - File size in bytes
    _hash: str      # - Hash of the file itself
    _url: str       # - Some form of resource ULR to get at the file

    last_checked: str   # - When did we last KNOW we had the file?

    # These are all STORAGE BACKEND functions which are bound into this class to permit object checks
    _check_exists_function: Callable[[str], bool]
    _check_size_function: Callable[[str], int]
    _check_hash_function: Callable[[str], str]

    @property
    def uuid(self) -> str:
        """
        Return the uuid of the file.

        :return:
        """
        return self._uuid

    @uuid.setter
    def uuid(self, value: str) -> None:
        """
        Cannot set the uuid manually.

        :param value:
        :return:
        """
        raise AttributeError("Cannot set the uuid manually.")

    @property
    def size(self) -> int:
        """
        Return the size of the file.

        :return:
        """
        return self._size

    @size.setter
    def size(self, size: int) -> None:
        """
        Cannot set the size manually.

        :param size:
        :return:
        """
        raise AttributeError("Cannot set the size manually.")

    @property
    def hash(self) -> str:
        """
        Return the hash of the file.

        :return:
        """
        return self._hash

    @hash.setter
    def hash(self, value: str) -> None:
        """
        Cannot set the hash manually.

        :param value:
        :return:
        """
        raise AttributeError("Cannot set the hash manually.")

    @property
    def url(self) -> str:
        """
        Return the url of the file.

        :return:
        """
        return self._url

    @url.setter
    def url(self, value: str) -> None:
        """
        Cannot set the url manually.

        :param value:
        :return:
        """
        raise AttributeError("Cannot set the url manually.")

    def update_check_exists_function(self, check_exists_function: Callable[[str], bool]) -> None:
        """
        Update the internal check exists function - which check the file still exists.

        :param check_exists_function:
        :return:
        """
        self._check_exists_function = check_exists_function

    def update_check_size_function(self, check_size_function: Callable[[str], int]) -> None:
        """
        Update the internal check size function - which check the file's size.

        :param check_size_function:
        :return:
        """
        self._check_size_function = check_size_function

    def update_check_hash_function(self, check_hash_function: Callable[[str], str]) -> None:
        """
        Update the internal check hash function - which check the file's hash.

        :param check_hash_function:
        :return:
        """
        self._check_hash_function = check_hash_function

    def recheck_self(self, all: bool = False, exists: bool = False, size: bool = False, hash: bool = False) -> bool:
        """
        Trigger a recheck of the data stored in this class.

        :return:
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
