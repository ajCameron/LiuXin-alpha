"""
Configure a writable local Store with a default managed allocation subtree.

Existing files and explicitly selected keys remain accessible throughout the
filesystem root. The managed prefix guides allocation and classification rather
than restricting all writes to that subtree.
"""

from __future__ import annotations

import os

from uuid import UUID

from LiuXin_alpha.storage.stores import FilesystemStore


class OnDiskExistingManagedStorageBackend(FilesystemStore):
    """
    Expose existing local files and allocate new managed keys below .liuxin-managed/objects.

    The filesystem Store supplies staged writes, collision/version checks, and path ownership.
    Explicit keys can target other locations under the root; the managed-area helpers do not create
    a separate access boundary.

    Example:
        >>> store = OnDiskExistingManagedStorageBackend("library")  # doctest: +SKIP
        >>> info = store.store_bytes(b"book", location="visible/book")  # doctest: +SKIP
    """

    store_kind = "on_disk_existing_managed"

    def __init__(
        self,
        url: str | os.PathLike[str],
        name: str | None = None,
        uuid: str | UUID | None = None,
    ) -> None:
        """
        Configure a writable filesystem Store that may create its root when started.

        Construction retains identity and the managed allocation prefix; it does not itself probe or
        create the root.

        Example:
            >>> store = OnDiskExistingManagedStorageBackend("library", name="Managed")  # doctest: +SKIP


        :param url: Filesystem root path or supported local file URI, normalized by FilesystemStore.
        :param name: Optional display name; None or empty text uses the filesystem Store's root-name fallback.
        :param uuid: UUID or UUID string for Store identity; None generates a fresh UUID.
        :return: None after configuring the inherited filesystem Store.
        """
        super().__init__(
            url,
            name=name,
            uuid=uuid,
            read_only=False,
            create_root=True,
            allocation_prefix=".liuxin-managed/objects",
        )

    @property
    def managed_area_root(self):
        """
        Compute the .liuxin-managed directory path beneath the resolved filesystem root without
        creating it.

        Example:
            >>> store.managed_area_root == store.root_path / ".liuxin-managed"  # doctest: +SKIP
            True


        :return: Path to the managed-area parent; automatically allocated objects normally live in its objects child.
        """

        return self.root_path / ".liuxin-managed"

    def is_reserved_managed_path(self, identifier) -> bool:
        """
        Locate the identifier and test its key for the .liuxin-managed/ prefix.

        The bare .liuxin-managed key does not match because the slash is required. This
        classification neither checks existence nor prevents writes elsewhere in the Store.

        Example:
            >>> store.is_reserved_managed_path(".liuxin-managed/objects/book")  # doctest: +SKIP
            True


        :param identifier: Internal key or owned Location accepted by the inherited locator.
        :return: True for a key beneath the managed directory, otherwise False; invalid or foreign locations reject.
        """

        return self.locate(identifier).key.startswith(".liuxin-managed/")


__all__ = ["OnDiskExistingManagedStorageBackend"]
