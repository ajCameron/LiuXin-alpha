"""
Specify database identity, schema versions and metadata fields.

Application user_version, schema_version and the stored component-version string
have separate meanings. Metadata reads may initialize a placeholder row in shared
SQL implementations. File modification times are filesystem observations, not
transaction clocks.
"""

from __future__ import annotations

import abc

from typing import Any, Optional


class DriverMetadataMixinAPI(abc.ABC):
    """
    Specify database identity, schema versions and metadata fields.

    Application user_version, schema_version and the stored component-version string
    have separate meanings. Metadata reads may initialize a placeholder row in shared
    SQL implementations. File modification times are filesystem observations, not
    transaction clocks. Abstract members must be implemented by a backend; their empty
    bodies return None when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverMetadataMixinAPI)
        True
    """

    # Toodo: not clear how different to database version.
    @property
    @abc.abstractmethod
    def user_version(self) -> Optional[str]:
        """
        Read the first PRAGMA user_version value from the primary connection.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.user_version  # doctest: +SKIP


        :return: The raw PRAGMA value, normally an integer, or ``None`` if no row is
            returned.
        """


    @abc.abstractmethod
    def set_database_version(self) -> None:
        """
        Upsert row 1 of database_version using the APSW driver master-version string.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Commit, then assert that the value reads back. OperationalError on the write is
        translated to an assertion suggesting that metadata tables are missing.

        Example:
            >>> driver.set_database_version()  # doctest: +SKIP


        :return: None; changes the host connection and checks the stored version.
        """

    @abc.abstractmethod
    def _initialize_md(self) -> None:
        """
        Ensure the metadata table contains one row, inserting a placeholder if empty.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        More than one row raises DatabaseIntegrityError; the inserted scratch value is the
        literal string ``None``.

        Example:
            >>> driver._initialize_md()  # doctest: +SKIP


        :return: ``True`` when exactly one row exists or was inserted.
        """

    @abc.abstractmethod
    def direct_get_schema_version(self) -> Optional[int]:
        """
        Read SQLite's schema change counter, retrying on a fresh connection after failure.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Temporary connections are closed. A failing primary handle may be closed without
        replacing ``self.conn``; failure of the retry propagates. Failed integer conversion
        preserves the raw value.

        Example:
            >>> driver.direct_get_schema_version()  # doctest: +SKIP


        :return: The integer counter, a raw non-convertible value, or ``None`` if no row is
            produced.
        """

    @abc.abstractmethod
    def direct_last_modified(self):
        """
        Read the database file mtime and convert it to a UTC datetime.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Filesystem errors propagate; no database connection is opened.

        Example:
            >>> driver.direct_last_modified()  # doctest: +SKIP


        :return: The UTC datetime produced by ``utcfromtimestamp``.
        """

    @abc.abstractmethod
    def direct_read_metadata(self, md_field_name: str) -> Any:
        """
        Read a validated metadata field, creating the placeholder row if needed.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        SQL null and values whose lowercase string is ``none`` return None. Other values are
        deep-copied. Unknown fields raise ValueError.

        Example:
            >>> driver.direct_read_metadata("scratch")  # doctest: +SKIP


        :param md_field_name: Metadata column name, with or without the
            ``database_metadata_`` prefix.
        :return: The copied field value, or ``None`` for null/none sentinels.
        """

    @abc.abstractmethod
    def direct_set_db_unique_id(self, force_value: Optional[str] = None) -> None:
        """
        Write a supplied identity or new UUID4, commit, and verify it by rereading.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        A non-null current identity updates row ID 1 without prompting. A null/missing
        identity takes the insert branch, so a pre-existing null-valued row can produce
        duplicate metadata rows. Verification failures raise DatabaseIntegrityError after
        the write commits.

        Example:
            >>> driver.direct_set_db_unique_id()  # doctest: +SKIP


        :param force_value: Identity value to write; ``None`` generates a UUID4 string.
        :return: ``True`` when rereading matches the requested value.
        """

    @abc.abstractmethod
    def direct_write_metadata(self, md_field_name: str, md_field_value: Any) -> None:
        """
        Validate a metadata field, initialize the sole row and update its value.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Unknown fields raise ValueError; multiple metadata rows raise
        DatabaseIntegrityError. Persistence is delegated to the row update helper.

        Example:
            >>> driver.direct_write_metadata("scratch", "reviewed")  # doctest: +SKIP


        :param md_field_name: Metadata column name, with or without the
            ``database_metadata_`` prefix.
        :param md_field_value: Value assigned to the validated metadata column.
        :return: ``None``.
        """

    @abc.abstractmethod
    def last_modified(self):
        """
        Read the configured database file modification time in UTC.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Stat database_path on each call and convert st_mtime with utcfromtimestamp. This is
        the main file timestamp, so it may not reflect changes still held in a WAL or an
        uncommitted transaction. The result is a timezone-aware datetime despite the legacy
        date annotation. Missing-file and other stat errors propagate; no connection is
        opened.

        Example:
            >>> driver.last_modified()  # doctest: +SKIP


        :return: UTC datetime derived from the current file mtime, using the shared
            timestamp converter.
        """
