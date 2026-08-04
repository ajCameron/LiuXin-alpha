"""Compatibility methods for updating files in a raw store.

Examples:
    Replace bytes when the concrete backend supports mutation::

        changed = backend.update_file("notes/a.txt", b"revised")
"""

from __future__ import annotations

import abc


class StoreBackendUpdateFilesAPI(abc.ABC):
    """Update files within a store.

    Use with care: in-place mutation can invalidate hashes and replica state.

    Examples:
        Append only when the backend and caller both expect mutable content::

            changed = backend.update_file("logs/import.log", b"done\n", append=True)
    """

    def update_file(
        self,
        storage_key: str,
        file_bytes: bytes,
        append: bool = False,
    ) -> bool:
        """Replace or append bytes at a storage key when supported.

        Examples:
            Replace an existing small payload::

                changed = backend.update_file("notes/a.txt", b"revised")
        """
        raise PermissionError("This store does not support file updates.")

    def update_replica(
        self,
        storage_key: str,
        file_bytes: bytes,
    ) -> bool:
        """Replace replica bytes when supported by the concrete backend.

        Examples:
            Repair a known replica storage key::

                changed = backend.update_replica("books/a.epub", repaired_bytes)
        """
        raise PermissionError("This store does not support replica updates.")
