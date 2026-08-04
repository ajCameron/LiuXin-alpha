"""Combined compatibility contract for raw store operations.

Examples:
    Legacy backend implementations satisfy the composed API::

        def inventory(backend: StoreBackendAPI):
            return list(backend.true_files())
"""

from __future__ import annotations

from LiuXin_alpha.storage.api.store_api.storage_backend_api.storage_backend_file_add_mixin import \
    StoreBackendAddFilesAPI
from LiuXin_alpha.storage.api.store_api.storage_backend_api.storage_backend_file_delete_mixin import \
    StoreBackendDeleteFiles
from LiuXin_alpha.storage.api.store_api.storage_backend_api.storage_backend_file_metadata_mixin import \
    StoreBackendMetadataAPI
from LiuXin_alpha.storage.api.store_api.storage_backend_api.storage_backend_file_read_mixin import \
    StoreBackendReadFilesAPI
from LiuXin_alpha.storage.api.store_api.storage_backend_api.storage_backend_file_update_mixin import \
    StoreBackendUpdateFilesAPI


class StoreBackendAPI(
    StoreBackendAddFilesAPI,
    StoreBackendMetadataAPI,
    StoreBackendDeleteFiles,
    StoreBackendReadFilesAPI,
    StoreBackendUpdateFilesAPI):
    """
    Responsible for actually accessing raw files.

    This is split out from the database methods in the store because we want to use this for other purposes.
    E.g. to allow us to
    - read from archives
    - read from remote FTP servers
    e.t.c.

    Examples:
        Read one location without using database integration::

            location = backend.get_file("authors/book.epub")
    """
