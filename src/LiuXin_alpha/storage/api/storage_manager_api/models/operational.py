"""
Carry attributable storage conditions and suggested recovery operations.

Validation is local to text and timestamp requirements. Health predicates inspect
reported issues rather than probing Stores or performing recovery.
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
    Classify an operator-visible condition as information, warning, or error. The enum does not
    itself schedule recovery or determine whether a particular Store can be read.

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
    Retain an attributable condition with a severity and explanation. Construction rejects blank
    code/message text but preserves their spelling. Severity and attribution fields are not coerced
    or type-validated.

    Example:
        >>> StorageOperationalIssue(
        ...     "store_unavailable",
        ...     StorageOperationalSeverity.ERROR,
        ...     "archive is offline",
        ... ).code
        'store_unavailable'


    :ivar code: Nonblank machine-readable condition name, retained without stripping.
    :ivar severity: Producer severity value, expected to be StorageOperationalSeverity.
    :ivar message: Nonblank operator explanation, retained without stripping.
    :ivar operation_id: Optional ingest operation UUID supplied for attribution.
    :ivar digital_asset_id: Optional affected Asset identity.
    :ivar replica_id: Optional affected Replica identity.
    :ivar store_ref: Optional affected configured Store UUID.
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
        Reject code or message values whose strip result is empty. Original strings remain
        unchanged; severity and attribution are not validated, and string-method errors propagate.

        Example:
            >>> StorageOperationalIssue("", StorageOperationalSeverity.ERROR, "bad")
            Traceback (most recent call last):
            ...
            ValueError: operational issue code must not be empty.


        :return: None for nonblank code/message; blank values raise ValueError.
        """

        if not self.code.strip():
            raise ValueError("operational issue code must not be empty.")
        if not self.message.strip():
            raise ValueError("operational issue message must not be empty.")


@dataclasses.dataclass(slots=True, frozen=True)
class StorageRecoveryAction:
    """
    Suggest a public operation with a rationale and optional attribution. The value performs no
    recovery, validates no operation registry, and does not guarantee that its suggested action is
    currently possible.

    Example:
        >>> StorageRecoveryAction("verify_replica", "integrity is unknown").action
        'verify_replica'


    :ivar action: Nonblank suggested operation name retained without stripping.
    :ivar reason: Nonblank explanation for the suggestion.
    :ivar operation_id: Optional ingest operation UUID supplied for attribution.
    :ivar digital_asset_id: Optional affected Asset identity.
    :ivar replica_id: Optional affected Replica identity.
    :ivar store_ref: Optional affected configured Store UUID.
    """

    action: str
    reason: str
    operation_id: UUID | None = None
    digital_asset_id: DigitalAssetID | None = None
    replica_id: ReplicaID | None = None
    store_ref: StoreUUID | None = None

    def __post_init__(self) -> None:
        """
        Reject blank action/reason text without normalizing it or validating attributed identities.
        Values are expected to support strip.

        Example:
            >>> StorageRecoveryAction("", "missing action")
            Traceback (most recent call last):
            ...
            ValueError: recovery action must not be empty.


        :return: None for nonblank action/reason; blank values raise ValueError.
        """

        if not self.action.strip():
            raise ValueError("recovery action must not be empty.")
        if not self.reason.strip():
            raise ValueError("recovery action reason must not be empty.")


@dataclasses.dataclass(slots=True, frozen=True)
class StorageOperationalStatus:
    """
    Retain a timestamp, Store observations, issues, and suggested recovery actions. The timestamp
    must be aware; supplied collections are not copied, coerced, or cross-validated. healthy derives
    only from issue severities and does not independently inspect Store status or execute suggested
    actions.

    Example:
        >>> from datetime import UTC, datetime
        >>> StorageOperationalStatus(datetime.now(UTC)).healthy
        True


    :ivar checked_at: Timezone-aware producer observation time, retained in its original zone.
    :ivar store_statuses: Attributable Store observations retained as supplied.
    :ivar issues: Ordered condition records used by healthy and issues_for.
    :ivar recovery_actions: Suggested operations, without automatic execution or deduplication here.
    """

    checked_at: datetime
    store_statuses: tuple[StoreStatusObservation, ...] = ()
    issues: tuple[StorageOperationalIssue, ...] = ()
    recovery_actions: tuple[StorageRecoveryAction, ...] = ()

    def __post_init__(self) -> None:
        """
        Require timestamp timezone information and a non-None UTC offset. No datetime isinstance
        check or UTC conversion occurs, and collection contents remain unchecked.

        Example:
            >>> from datetime import datetime
            >>> StorageOperationalStatus(datetime.now())
            Traceback (most recent call last):
            ...
            ValueError: checked_at must be timezone-aware.


        :return: None for an aware timestamp; missing timezone/offset raises ValueError and attribute errors can propagate.
        """

        if self.checked_at.tzinfo is None or self.checked_at.utcoffset() is None:
            raise ValueError("checked_at must be timezone-aware.")

    @property
    def healthy(self) -> bool:
        """
        Return false when any issue severity compares equal to WARNING or ERROR. Store availability,
        suggested actions, and issue codes are ignored. Severity is not coerced here; string values
        equal to the StrEnum members also match.

        Example:
            >>> from datetime import UTC, datetime
            >>> StorageOperationalStatus(datetime.now(UTC)).healthy
            True


        :return: True when no listed issue has warning/error severity.
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
        Collect all issues whose retained code exactly equals the query, preserving order and
        duplicate records. The query is not stripped or case-normalized.

        Example:
            >>> from datetime import UTC, datetime
            >>> status = StorageOperationalStatus(datetime.now(UTC))
            >>> status.issues_for("store_unavailable")
            ()


        :param code: Exact machine-readable code to compare against each issue.
        :return: New tuple of matching retained issue objects.
        """

        return tuple(issue for issue in self.issues if issue.code == code)


__all__ = [
    "StorageOperationalIssue",
    "StorageOperationalSeverity",
    "StorageOperationalStatus",
    "StorageRecoveryAction",
]
