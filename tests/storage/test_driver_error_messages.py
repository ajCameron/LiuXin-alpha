"""Failure-message contracts shared by the concrete Store drivers."""

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
    driver = FilesystemStorageDriver(
        tmp_path,
        address_space_uuid=uuid4(),
    )
    driver.startup()
    destination = driver.parse_object_address("objects/book.epub")

    def no_space(*args, **kwargs):
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
    def unauthorized(request, timeout_s):
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
    def broken_opener(request, timeout_s):
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
    class LoginRejectedClient:
        def connect(self, *args, **kwargs) -> None:
            del args, kwargs

        def login(self, *args, **kwargs) -> None:
            del args, kwargs
            raise ftplib.error_perm("530 Login incorrect")

        def quit(self) -> None:
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
    class S3Failure(RuntimeError):
        response = {
            "Error": {"Code": "AccessDenied"},
            "ResponseMetadata": {"HTTPStatusCode": 403},
        }

    class Client:
        def head_object(self, **kwargs):
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
    class S3Failure(RuntimeError):
        response = {
            "Error": {"Code": "AccessDenied"},
            "ResponseMetadata": {"HTTPStatusCode": 403},
        }

    class Client:
        def head_bucket(self, **kwargs):
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
    def broken_runner(arguments):
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
    missing = tmp_path / "missing.squashfs"

    with pytest.raises(api.StorageNotFound) as raised:
        SquashfsStorageDriver(missing, address_space_uuid=uuid4())

    message = str(raised.value)
    assert "SquashFS configure failed" in message
    assert "missing.squashfs" in message
    assert "does not exist" in message
