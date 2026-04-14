from __future__ import annotations

import abc


class StoreBackendUpdateFilesAPI(abc.ABC):
    """Update files within a store. Use with care - this can cause data destruction."""

    def update_file(
        self,
        storage_key: str,
        file_bytes: bytes,
        append: bool = False,
    ) -> bool:
        raise PermissionError("This store does not support file updates.")

    def update_replica(
        self,
        storage_key: str,
        file_bytes: bytes,
    ) -> bool:
        raise PermissionError("This store does not support replica updates.")
