from __future__ import annotations

from typing import Callable, Optional

import time


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
        Load the file with the tools required to actually know and check it's own status.

        :param url:
        :param exists:
        :param size:
        :param uuid:
        :param last_checked:
        :param check_exists_function:
        :param check_size_function:
        :param check_hash_function:
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
