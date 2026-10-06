"""Adapt immutable storage-operational reports to standard structured logging."""

from __future__ import annotations

import logging

from LiuXin_alpha.storage.api.storage_manager_api.models.operational import (
    StorageOperationalSeverity,
    StorageOperationalStatus,
)


_LOG_LEVELS = {
    StorageOperationalSeverity.INFO: logging.INFO,
    StorageOperationalSeverity.WARNING: logging.WARNING,
    StorageOperationalSeverity.ERROR: logging.ERROR,
}


def log_operational_status(
    status: StorageOperationalStatus,
    logger: logging.Logger,
) -> int:
    """
    Emit each operational issue as one structured standard-library log record.

    Issue order and duplicate findings are preserved. The immutable status is not changed and
    recovery actions are not executed. Attribution values are attached under storage-prefixed
    ``LogRecord`` attributes so formatters and handlers can route them without parsing the message.

    Example:
        >>> emitted = log_operational_status(status, logging.getLogger("storage"))  # doctest: +SKIP


    :param status: Previously assembled operational snapshot whose issues should be emitted.
    :param logger: Borrowed logger; handlers, filters, propagation, and level policy remain caller-owned.
    :return: Number of issue records offered to the logger, including records filtered by logger configuration.
    """

    for issue in status.issues:
        logger.log(
            _LOG_LEVELS[issue.severity],
            "%s: %s",
            issue.code,
            issue.message,
            extra={
                "storage_issue_code": issue.code,
                "storage_recoverability": issue.recoverability.value,
                "storage_operation_id": issue.operation_id,
                "storage_digital_asset_id": issue.digital_asset_id,
                "storage_replica_id": issue.replica_id,
                "storage_store_ref": issue.store_ref,
                "storage_checked_at": status.checked_at,
            },
        )
    return len(status.issues)


__all__ = ["log_operational_status"]
