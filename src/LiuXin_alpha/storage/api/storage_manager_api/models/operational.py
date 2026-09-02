"""
Operator-facing storage health and recovery values.
"""

from __future__ import annotations

import dataclasses

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from LiuXin_alpha.storage.api.models import StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.models.identifiers import (
    DigitalAssetID,
    ReplicaID,
)
from LiuXin_alpha.storage.api.storage_manager_api.models.stores import (
    StoreStatusObservation,
)


class StorageOperationalSeverity(StrEnum):
    """
    Severity of an operator-visible storage condition.

    Example:
        >>> StorageOperationalSeverity.ERROR.value
        'error'
    """

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclasses.dataclass(slots=True, frozen=True)
class StorageOperationalIssue:
    """
    One attributable storage condition requiring attention or context.

    Example:
        >>> StorageOperationalIssue(
        ...     "store_unavailable",
        ...     StorageOperationalSeverity.ERROR,
        ...     "archive is offline",
        ... ).code
        'store_unavailable'
    """

    code: str
    severity: StorageOperationalSeverity
    message: str
    operation_id: UUID | None = None
    digital_asset_id: DigitalAssetID | None = None
    replica_id: ReplicaID | None = None
    store_ref: StoreUUID | None = None

    def __post_init__(self) -> None:
        """
        Validate stable code and operator-facing message.

        Example:
            >>> StorageOperationalIssue("", StorageOperationalSeverity.ERROR, "bad")
            Traceback (most recent call last):
            ...
            ValueError: operational issue code must not be empty.


        :return:
        """

        if not self.code.strip():
            raise ValueError("operational issue code must not be empty.")
        if not self.message.strip():
            raise ValueError("operational issue message must not be empty.")


@dataclasses.dataclass(slots=True, frozen=True)
class StorageRecoveryAction:
    """
    Suggested public operation for resolving one reported condition.

    Example:
        >>> StorageRecoveryAction("verify_replica", "integrity is unknown").action
        'verify_replica'
    """

    action: str
    reason: str
    operation_id: UUID | None = None
    digital_asset_id: DigitalAssetID | None = None
    replica_id: ReplicaID | None = None
    store_ref: StoreUUID | None = None

    def __post_init__(self) -> None:
        """
        Validate the suggested operation and its rationale.

        Example:
            >>> StorageRecoveryAction("", "missing action")
            Traceback (most recent call last):
            ...
            ValueError: recovery action must not be empty.


        :return:
        """

        if not self.action.strip():
            raise ValueError("recovery action must not be empty.")
        if not self.reason.strip():
            raise ValueError("recovery action reason must not be empty.")


@dataclasses.dataclass(slots=True, frozen=True)
class StorageOperationalStatus:
    """
    Point-in-time health report across Stores, metadata, and policy.

    Example:
        >>> from datetime import UTC, datetime
        >>> StorageOperationalStatus(datetime.now(UTC)).healthy
        True
    """

    checked_at: datetime
    store_statuses: tuple[StoreStatusObservation, ...] = ()
    issues: tuple[StorageOperationalIssue, ...] = ()
    recovery_actions: tuple[StorageRecoveryAction, ...] = ()

    def __post_init__(self) -> None:
        """
        Require an unambiguous timezone-aware observation time.

        Example:
            >>> from datetime import datetime
            >>> StorageOperationalStatus(datetime.now())
            Traceback (most recent call last):
            ...
            ValueError: checked_at must be timezone-aware.


        :return:
        """

        if self.checked_at.tzinfo is None or self.checked_at.utcoffset() is None:
            raise ValueError("checked_at must be timezone-aware.")

    @property
    def healthy(self) -> bool:
        """
        Return whether no warning or error condition is currently known.

        Example:
            >>> from datetime import UTC, datetime
            >>> StorageOperationalStatus(datetime.now(UTC)).healthy
            True


        :return:
        """

        return not any(
            issue.severity
            in {
                StorageOperationalSeverity.WARNING,
                StorageOperationalSeverity.ERROR,
            }
            for issue in self.issues
        )

    def issues_for(self, code: str) -> tuple[StorageOperationalIssue, ...]:
        """
        Return issues matching one stable machine-readable code.

        Example:
            >>> from datetime import UTC, datetime
            >>> status = StorageOperationalStatus(datetime.now(UTC))
            >>> status.issues_for("store_unavailable")
            ()


        :param code:
        :return:
        """

        return tuple(issue for issue in self.issues if issue.code == code)


__all__ = [
    "StorageOperationalIssue",
    "StorageOperationalSeverity",
    "StorageOperationalStatus",
    "StorageRecoveryAction",
]
