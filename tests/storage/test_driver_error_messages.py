"""
Keep concrete driver failures typed, contextual, and appropriately redacted.

Local tests use actual missing paths or corrupt SQLite bytes. Remote HTTP, FTP,
S3, and rclone cases inject small transport doubles; filesystem exhaustion is
simulated at temporary-file creation. Assertions check selected operation/target
details and secret removal rather than claiming live-backend or exhaustive
redaction coverage. Nested fault helpers are part of this documented test scope.

Example:
    >>> test_http_failure_has_method_context_and_redacts_query_values()
"""

from __future__ import annotations

import errno
import ftplib
import tempfile
import urllib.error

from pathlib import Path
from uuid import uuid4

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.drivers import (
    FilesystemStorageDriver,
    FtpDriverOptions,
    FtpStorageDriver,
    HttpStorageDriver,
    RcloneStorageDriver,
    S3StorageDriver,
    SQLiteStorageDriver,
    SquashfsStorageDriver,
)


def test_filesystem_missing_object_identifies_operation_and_target(
    tmp_path: Path,
) -> None:
    """
    Stat an absent object under a real temporary filesystem root and verify typed absence plus
    operation, key, and OS-error context.

    Example:
        >>> test_filesystem_missing_object_identifies_operation_and_target(tmp_path)  # doctest: +SKIP


    :param tmp_path: Isolated local root, container path, or staging directory used by the driver test.
    :return: None after the stated regression assertions pass.
    """
    driver = FilesystemStorageDriver(
        tmp_path,
        address_space_uuid=uuid4(),
    )
    driver.startup()
    missing = driver.parse_object_address("missing/book.epub")

    with pytest.raises(api.StorageNotFound) as raised:
        driver.stat(missing)

    message = str(raised.value)
    assert "filesystem stat failed" in message
    assert "missing/book.epub" in message
    assert "No such file" in message


def test_filesystem_no_space_is_classified_with_write_context(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """
    Inject ENOSPC at temporary-file creation and verify StorageNoSpace retains the write operation
    and target context.

    The root is real, but the test does not exhaust a filesystem or prove recovery after an actual
    full-disk write.

    Example:
        >>> test_filesystem_no_space_is_classified_with_write_context(monkeypatch, tmp_path)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the temporary-file creation function after fault injection.
    :param tmp_path: Isolated local root, container path, or staging directory used by the driver test.
    :return: None after the stated regression assertions pass.
    """
    driver = FilesystemStorageDriver(
        tmp_path,
        address_space_uuid=uuid4(),
    )
    driver.startup()
    destination = driver.parse_object_address("objects/book.epub")

    def no_space(*args, **kwargs):
        """
        Reject every temporary-file creation call with the same synthetic ENOSPC error.

        Example:
            >>> no_space(prefix="staging-")  # doctest: +SKIP


        :param args: Positional tempfile.mkstemp arguments, deliberately ignored.
        :param kwargs: Keyword tempfile.mkstemp arguments, deliberately ignored.
        :return: Never returns; raises OSError with errno.ENOSPC.
        """
        del args, kwargs
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(tempfile, "mkstemp", no_space)

    with pytest.raises(api.StorageNoSpace) as raised:
        driver.begin_write(destination)

    message = str(raised.value)
    assert "filesystem begin write failed" in message
    assert "objects/book.epub" in message
    assert "No space left on device" in message


def test_sqlite_corrupt_container_has_typed_startup_failure(
    tmp_path: Path,
) -> None:
    """
    Write invalid SQLite bytes to a real temporary file and verify startup raises
    StorageIntegrityError with container and corruption context.

    Example:
        >>> test_sqlite_corrupt_container_has_typed_startup_failure(tmp_path)  # doctest: +SKIP


    :param tmp_path: Isolated local root, container path, or staging directory used by the driver test.
    :return: None after the stated regression assertions pass.
    """
    database_path = tmp_path / "objects.sqlite"
    database_path.write_bytes(b"this is not a sqlite database")
    driver = SQLiteStorageDriver(
        database_path,
        address_space_uuid=uuid4(),
    )

    with pytest.raises(api.StorageIntegrityError) as raised:
        driver.startup()

    message = str(raised.value)
    assert "SQLite startup failed" in message
    assert "objects.sqlite" in message
    assert "corrupt or is not a SQLite database" in message


def test_http_failure_has_method_context_and_redacts_query_values() -> None:
    """
    Inject an HTTP 401 and verify a typed authentication failure names HEAD and the status while
    hiding the private query value.

    The custom opener raises locally; no remote HTTP request is made.

    Example:
        >>> test_http_failure_has_method_context_and_redacts_query_values()


    :return: None after the stated regression assertions pass.
    """
    def unauthorized(request, timeout_s):
        """
        Raise an HTTP 401 tied to the actual request URL without opening a connection.

        Example:
            >>> unauthorized(request, timeout_s=1.0)  # doctest: +SKIP


        :param request: HTTP request whose full_url populates the synthetic error.
        :param timeout_s: Requested timeout in seconds, ignored by this failing opener.
        :return: Never returns; raises urllib.error.HTTPError with status 401.
        """
        del timeout_s
        raise urllib.error.HTTPError(
            request.full_url,
            401,
            "unauthorized",
            {},
            None,
        )

    driver = HttpStorageDriver(
        "https://example.test/library/",
        address_space_uuid=uuid4(),
        request_opener=unauthorized,
    )
    address = driver.parse_object_address(
        "books/book.epub?edition=private-value"
    )

    with pytest.raises(api.StorageAuthenticationFailed) as raised:
        driver.stat(address)

    message = str(raised.value)
    assert "HTTP HEAD failed" in message
    assert "status 401" in message
    assert "private-value" not in message
    assert "<redacted>" in message


def test_http_probe_does_not_flatten_an_unexpected_backend_fault() -> None:
    """
    Inject an unexpected opener RuntimeError and verify probe raises the exact StorageError base
    type while preserving HEAD context and redacting a token assignment.

    Example:
        >>> test_http_probe_does_not_flatten_an_unexpected_backend_fault()


    :return: None after the stated regression assertions pass.
    """
    def broken_opener(request, timeout_s):
        """
        Raise an unexpected backend fault containing a token-like value for redaction checks.

        Example:
            >>> broken_opener(request, timeout_s=1.0)  # doctest: +SKIP


        :param request: HTTP request accepted but not inspected.
        :param timeout_s: Requested timeout in seconds, ignored.
        :return: Never returns; raises the deliberate RuntimeError.
        """
        del request, timeout_s
        raise RuntimeError("adapter exploded; token=private-value")

    driver = HttpStorageDriver(
        "https://example.test/library/",
        address_space_uuid=uuid4(),
        request_opener=broken_opener,
    )

    with pytest.raises(api.StorageError) as raised:
        driver.probe()

    assert type(raised.value) is api.StorageError
    message = str(raised.value)
    assert "HTTP HEAD failed" in message
    assert "adapter exploded" in message
    assert "private-value" not in message


def test_ftp_failure_has_operation_context_without_url_credentials() -> None:
    """
    Inject FTP login rejection and verify a typed authentication failure identifies the target
    without exposing the URL username or password.

    The client double makes no network connection.

    Example:
        >>> test_ftp_failure_has_operation_context_without_url_credentials()


    :return: None after the stated regression assertions pass.
    """
    class LoginRejectedClient:
        """
        Minimal FTP client that accepts connection setup but always rejects login.

        Example:
            >>> client = LoginRejectedClient()  # doctest: +SKIP
        """
        def connect(self, *args, **kwargs) -> None:
            """
            Accept connection arguments without opening a socket or recording state.

            Example:
                >>> client.connect("example.test", 21)  # doctest: +SKIP


            :param args: Positional connect arguments, ignored.
            :param kwargs: Keyword connect arguments, ignored.
            :return: None without making a network connection.
            """
            del args, kwargs

        def login(self, *args, **kwargs) -> None:
            """
            Raise a synthetic FTP 530 response for every credential set.

            Example:
                >>> client.login("reader", "password")  # doctest: +SKIP


            :param args: Positional login arguments, ignored rather than echoed.
            :param kwargs: Keyword login arguments, ignored.
            :return: Never returns; raises ftplib.error_perm describing rejected login.
            """
            del args, kwargs
            raise ftplib.error_perm("530 Login incorrect")

        def quit(self) -> None:
            """
            Accept cleanup of the nonconnecting FTP double.

            Example:
                >>> client.quit()  # doctest: +SKIP


            :return: None without external effects.
            """
            return None

    driver = FtpStorageDriver(
        "ftp://reader:top-secret@example.test/library/",
        address_space_uuid=uuid4(),
        options=FtpDriverOptions(client_factory=LoginRejectedClient),
    )
    address = driver.parse_object_address("books/book.epub")

    with pytest.raises(api.StorageAuthenticationFailed) as raised:
        driver.stat(address)

    message = str(raised.value)
    assert "FTP stat failed" in message
    assert "authentication failed" in message
    assert "example.test/library/books/book.epub" in message
    assert "reader" not in message
    assert "top-secret" not in message


def test_s3_failure_includes_operation_backend_code_and_status(
    tmp_path: Path,
) -> None:
    """
    Inject an S3-shaped AccessDenied response and verify typed permission denial preserves the
    object URI, backend code, and HTTP status while removing secret text.

    Example:
        >>> test_s3_failure_includes_operation_backend_code_and_status(tmp_path)  # doctest: +SKIP


    :param tmp_path: Isolated local root, container path, or staging directory used by the driver test.
    :return: None after the stated regression assertions pass.
    """
    class S3Failure(RuntimeError):
        """
        Attach an S3-shaped AccessDenied/HTTP-403 response to a local RuntimeError.

        The class metadata drives error classification without requiring a cloud SDK exception or
        live response.

        Example:
            >>> error = S3Failure("access denied")  # doctest: +SKIP
        """
        response = {
            "Error": {"Code": "AccessDenied"},
            "ResponseMetadata": {"HTTPStatusCode": 403},
        }

    class Client:
        """
        Provide only the failing head_object operation needed by this regression.

        Example:
            >>> client = Client()  # doctest: +SKIP
        """
        def head_object(self, **kwargs):
            """
            Reject the supplied request with the local S3-shaped permission error.

            Example:
                >>> client.head_object(Bucket="library")  # doctest: +SKIP


            :param kwargs: S3 request keyword arguments, ignored by the double.
            :return: Never returns; raises the enclosing test's S3Failure.
            """
            del kwargs
            raise S3Failure("secret=should-not-appear")

    driver = S3StorageDriver(
        "library",
        prefix="objects",
        address_space_uuid=uuid4(),
        client=Client(),
        local_staging_directory=tmp_path,
        close_client=False,
    )
    address = driver.parse_object_address("book.epub")

    with pytest.raises(api.StoragePermissionDenied) as raised:
        driver.stat(address)

    message = str(raised.value)
    assert "S3 stat object failed" in message
    assert "s3://library/objects/book.epub" in message
    assert "AccessDenied" in message
    assert "HTTP 403" in message
    assert "should-not-appear" not in message
    driver.close()


def test_s3_probe_keeps_permission_failure_typed(
    tmp_path: Path,
) -> None:
    """
    Inject AccessDenied during the S3 bucket probe and verify StoragePermissionDenied and
    probe-operation context remain visible.

    Example:
        >>> test_s3_probe_keeps_permission_failure_typed(tmp_path)  # doctest: +SKIP


    :param tmp_path: Isolated local root, container path, or staging directory used by the driver test.
    :return: None after the stated regression assertions pass.
    """
    class S3Failure(RuntimeError):
        """
        Attach an S3-shaped AccessDenied/HTTP-403 response to a local RuntimeError.

        The class metadata drives error classification without requiring a cloud SDK exception or
        live response.

        Example:
            >>> error = S3Failure("access denied")  # doctest: +SKIP
        """
        response = {
            "Error": {"Code": "AccessDenied"},
            "ResponseMetadata": {"HTTPStatusCode": 403},
        }

    class Client:
        """
        Provide only the failing head_bucket operation needed by this regression.

        Example:
            >>> client = Client()  # doctest: +SKIP
        """
        def head_bucket(self, **kwargs):
            """
            Reject the supplied request with the local S3-shaped permission error.

            Example:
                >>> client.head_bucket(Bucket="library")  # doctest: +SKIP


            :param kwargs: S3 request keyword arguments, ignored by the double.
            :return: Never returns; raises the enclosing test's S3Failure.
            """
            del kwargs
            raise S3Failure("access denied")

    driver = S3StorageDriver(
        "library",
        address_space_uuid=uuid4(),
        client=Client(),
        local_staging_directory=tmp_path,
        close_client=False,
    )

    with pytest.raises(api.StoragePermissionDenied, match="probe bucket"):
        driver.probe()

    driver.close()


def test_rclone_unknown_failure_is_contextual_and_scrubs_secret_assignments() -> None:
    """
    Inject a JSON-runner failure and verify StorageUnavailable preserves lsjson/root context while
    replacing the token value.

    The runner and process spawner are doubles; this does not invoke a real rclone process.

    Example:
        >>> test_rclone_unknown_failure_is_contextual_and_scrubs_secret_assignments()


    :return: None after the stated regression assertions pass.
    """
    def broken_runner(arguments):
        """
        Raise a process-adapter fault containing a token assignment before producing JSON.

        Example:
            >>> broken_runner(["lsjson", "archive:books"])  # doctest: +SKIP


        :param arguments: Requested rclone argument sequence, ignored instead of executed.
        :return: Never returns; raises RuntimeError with contextual text for redaction.
        """
        del arguments
        raise RuntimeError("connection reset; token=remote-secret")

    driver = RcloneStorageDriver(
        "archive:books",
        address_space_uuid=uuid4(),
        json_runner=broken_runner,
        process_spawner=lambda arguments: None,
    )
    address = driver.parse_object_address("book.epub")

    with pytest.raises(api.StorageUnavailable) as raised:
        driver.stat(address)

    message = str(raised.value)
    assert "rclone run lsjson failed" in message
    assert "archive:books" in message
    assert "connection reset" in message
    assert "remote-secret" not in message
    assert "token=<redacted>" in message


def test_squashfs_missing_archive_identifies_configuration_target(
    tmp_path: Path,
) -> None:
    """
    Construct a driver for an absent archive and verify configuration raises StorageNotFound naming
    the target before archive access.

    Example:
        >>> test_squashfs_missing_archive_identifies_configuration_target(tmp_path)  # doctest: +SKIP


    :param tmp_path: Isolated local root, container path, or staging directory used by the driver test.
    :return: None after the stated regression assertions pass.
    """
    missing = tmp_path / "missing.squashfs"

    with pytest.raises(api.StorageNotFound) as raised:
        SquashfsStorageDriver(missing, address_space_uuid=uuid4())

    message = str(raised.value)
    assert "SquashFS configure failed" in message
    assert "missing.squashfs" in message
    assert "does not exist" in message
