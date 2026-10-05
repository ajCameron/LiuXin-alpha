"""
Choose digest filenames for implicit local byte/file writes through FilesystemStore.

Default names use SHA-256 with a .file suffix, but a supplied Digest selects its
own value and algorithm for validation. Explicit keys remain available, and
ordinary inherited allocation still uses an objects prefix. Existing targets
follow collision policy rather than automatic deduplication.
"""

from __future__ import annotations

import hashlib
import os

from pathlib import Path
from uuid import UUID

from LiuXin_alpha.storage.api import Digest, FileInfo, StoreUnsupportedOperation
from LiuXin_alpha.storage.stores import FilesystemStore


class OnDiskFlatStorageBackend(FilesystemStore):
    """
    Default byte/file writes to <digest>.file while retaining the filesystem Store API.

    Implicit streams need a digest to choose that filename. Explicit destinations may be nested, and
    direct inherited allocation uses objects. A digest filename is not an existence reservation or
    evidence that existing bytes match; default create-only writes reject existing targets.

    Example:
        >>> store = OnDiskFlatStorageBackend("flat-library")  # doctest: +SKIP
        >>> info = store.store_bytes(b"book")  # doctest: +SKIP
        >>> info.location.key.endswith(".file")  # doctest: +SKIP
        True
    """

    store_kind = "on_disk_flat"

    def __init__(
        self,
        url: str | os.PathLike[str],
        name: str | None = None,
        uuid: str | UUID | None = None,
    ) -> None:
        """
        Configure a writable, creatable filesystem root with objects as the inherited allocation
        prefix.

        Digest naming belongs to this class's convenience overrides. Construction does not start the
        driver or create the root.

        Example:
            >>> store = OnDiskFlatStorageBackend("flat-library", name="Flat")  # doctest: +SKIP


        :param url: Filesystem root path or supported local file URI, normalized by FilesystemStore.
        :param name: Optional display name; None or empty text uses the filesystem Store's root-name fallback.
        :param uuid: UUID or UUID string for Store identity; None generates a fresh UUID.
        :return: None after inherited filesystem Store configuration.
        """
        super().__init__(
            url,
            name=name,
            uuid=uuid,
            allocation_prefix="objects",
        )

    def store_bytes(
        self,
        data: bytes,
        *,
        location=None,
        name: str | None = None,
        metadata=None,
        write_mode=None,
        expected_digest: Digest | None = None,
        mode=None,
    ) -> FileInfo:
        """
        Compute or accept a digest, choose an explicit key or digest filename, and delegate the byte
        write.

        SHA-256 is computed when no expectation is supplied, even for an explicit destination. A
        supplied digest is checked by the eventual write and its value supplies any implicit
        filename. Algorithm names are omitted from that filename. Existing targets follow collision
        policy without a preliminary content comparison.

        Example:
            >>> info = store.store_bytes(b"book", location="chosen.file")  # doctest: +SKIP


        :param data: Payload bytes hashed when necessary and passed to the inherited write.
        :param location: Explicit key/Location, or a false value to select the digest filename.
        :param name: Optional filename hint forwarded to the inherited write; it does not replace the selected key.
        :param metadata: Optional placement/filename hint source passed to the Store convenience method.
        :param write_mode: Optional collision mode or spelling; omitted mode defaults to create-only.
        :param expected_digest: Optional payload digest; None computes SHA-256 before the write.
        :param mode: Alternative collision-mode argument; supplying both mode and write_mode rejects even if they agree.
        :return: Committed FileInfo from the inherited Store write; conflicts and integrity errors propagate.
        """
        digest = expected_digest or Digest(
            "sha256",
            hashlib.sha256(data).hexdigest(),
        )
        destination = location or f"{digest.value}.file"
        return super().store_bytes(
            data,
            location=destination,
            name=name,
            metadata=metadata,
            write_mode=write_mode,
            expected_digest=digest,
            mode=mode,
        )

    def store_file(
        self,
        path: str | os.PathLike[str],
        *,
        location=None,
        name: str | None = None,
        metadata=None,
        write_mode=None,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        mode=None,
    ) -> FileInfo:
        """
        Hash a local source when needed, choose a destination, and delegate the checked file copy.

        Hashing and the inherited stat/open/copy are separate observations, so source changes can
        cause later expectation failure. An explicit digest skips this initial hash. A false
        location selects the digest filename; existing targets are not deduplicated.

        Example:
            >>> info = store.store_file("source.epub")  # doctest: +SKIP


        :param path: Local source pathname opened for hashing unless a digest is supplied.
        :param location: Explicit key/Location, or a false value to select the digest filename.
        :param name: Optional filename hint forwarded to the inherited write; it does not replace the selected key.
        :param metadata: Optional placement/filename hint source passed to the Store convenience method.
        :param write_mode: Optional collision mode or spelling; omitted mode defaults to create-only.
        :param expected_size: Optional required source size, checked by the inherited file-copy helper.
        :param expected_digest: Optional payload digest; None computes SHA-256 before the write.
        :param mode: Alternative collision-mode argument; supplying both mode and write_mode rejects even if they agree.
        :return: Committed FileInfo after the inherited size/digest checks; source streams are closed by their owners.
        """
        source_path = Path(path)
        digest = expected_digest or _file_digest(source_path)
        destination = location or f"{digest.value}.file"
        return super().store_file(
            source_path,
            location=destination,
            name=name,
            metadata=metadata,
            write_mode=write_mode,
            expected_size=expected_size,
            expected_digest=digest,
            mode=mode,
        )

    def store_stream(self, source, *, location=None, expected_digest=None, **kwargs):
        """
        Require an explicit destination or a digest and delegate streaming from the current source
        position.

        Only None selects the digest-derived filename. An empty explicit string is forwarded for
        location validation, unlike the byte/file helpers' false-value fallback. This method neither
        prehashes nor rewinds or closes the borrowed source.

        Example:
            >>> info = store.store_stream(source, location="chosen.file", expected_size=4)  # doctest: +SKIP


        :param source: Borrowed readable binary stream consumed from its current position.
        :param location: Explicit key/Location; None requires expected_digest to form the filename.
        :param expected_digest: Optional digest checked during writing; also supplies the implicit filename.
        :param kwargs: Arguments forwarded to Store.store_stream, including expected_size, name, metadata, write_mode, or mode.
        :return: Committed FileInfo; missing destination/digest raises StoreUnsupportedOperation before reading.
        """
        if location is None and expected_digest is None:
            raise StoreUnsupportedOperation(
                "flat streaming writes require an expected digest or location."
            )
        destination = location
        if destination is None:
            assert expected_digest is not None
            destination = f"{expected_digest.value}.file"
        return super().store_stream(
            source,
            location=destination,
            expected_digest=expected_digest,
            **kwargs,
        )


def _file_digest(path: Path) -> Digest:
    """
    Compute SHA-256 through 1 MiB read requests and close the local file on exit.

    No size cap, stat comparison, or snapshot is added here.

    Example:
        >>> digest = _file_digest(Path("source.epub"))  # doctest: +SKIP
        >>> digest.algorithm  # doctest: +SKIP
        'sha256'


    :param path: Source pathname to open in binary mode.
    :return: Digest with lowercase SHA-256 hex; file/read errors propagate.
    """
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return Digest("sha256", digest.hexdigest())


__all__ = ["OnDiskFlatStorageBackend"]
