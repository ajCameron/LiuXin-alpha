"""
Own the in-memory projection of native custom-field definitions.

Use ``write()`` to prepare metadata in the SQL transaction and publish it after
commit. Direct native service writes deliberately do not refresh this projection.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from LiuXin_alpha.databases.custom_columns.native_service import CustomFieldService
from LiuXin_alpha.databases.transaction_participants import after_commit

if TYPE_CHECKING:
    from LiuXin_alpha.caches.field_metadata import FieldMetadata


class CustomFieldCache:
    """
    Coordinate native writes with a caller-owned field metadata projection.

    Example:
        Use ``with coordinator.write() as fields:`` to publish definitions after commit.
    """

    def __init__(
        self, service: CustomFieldService, field_metadata: FieldMetadata
    ) -> None:
        """
        Retain the native service and caller-owned descriptor registry.

        Example:
            Several coordinators can safely publish to the same ticketed registry.


        :param service: Native custom-field persistence service; its database lifetime
            is borrowed.
        :param field_metadata: Registry that receives complete prepared definition
            snapshots.
        :return: None; performs no read or publication.
        """
        self.service = service
        self.field_metadata = field_metadata

    @contextmanager
    def write(self) -> Iterator[CustomFieldService]:
        """
        Roll back on preparation failure; publish only after the outer commit.

        Nested scopes follow savepoint rollback. The registry suppresses an older
        publication after a newer one completes. Receipt listeners describe
        committed SQL and may run before this projection.

        Example:
            A failed descriptor preparation rolls back the surrounding native write.


        :return: Context yielding the native service and scheduling publication after
            commit.
        """
        with self.service.write():
            yield self.service
            after_commit(self.service.db.macros, self.prepare())

    def refresh(self) -> None:
        """
        Refresh from a snapshot, deferring publication when already in a transaction.

        Example:
            After a direct native write, refresh() updates this derived registry explicitly.


        :return: None; publishes immediately after its owned read or defers to the
            caller's commit.
        """
        with self.service.read():
            after_commit(self.service.db.macros, self.prepare())

    def prepare(self) -> Callable[[], None]:
        """
        Prepare one native descriptor snapshot without changing visible metadata.

        Example:
            Discard a prepared callback when the surrounding transaction rolls back.


        :return: Zero-argument callback publishing the prepared definitions when still
            current.
        """
        return self.field_metadata.prepare_custom_fields(self.service.list_fields())
