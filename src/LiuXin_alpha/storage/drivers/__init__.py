"""Concrete reusable storage drivers shipped with LiuXin."""

from LiuXin_alpha.storage.drivers.filesystem import (
    FilesystemObjectAddress,
    FilesystemStorageDriver,
)
from LiuXin_alpha.storage.drivers.ftp import (
    FtpDriverOptions,
    FtpObjectAddress,
    FtpStorageDriver,
)
from LiuXin_alpha.storage.drivers.http import HttpObjectAddress, HttpStorageDriver
from LiuXin_alpha.storage.drivers.rclone import (
    RcloneObjectAddress,
    RcloneStorageDriver,
    WritableRcloneStorageDriver,
)
from LiuXin_alpha.storage.drivers.s3 import (
    S3ClientAPI,
    S3ObjectAddress,
    S3StorageDriver,
)
from LiuXin_alpha.storage.drivers.squashfs import (
    SquashfsObjectAddress,
    SquashfsStorageDriver,
)
from LiuXin_alpha.storage.drivers.sqlite import SQLiteObjectAddress, SQLiteStorageDriver


__all__ = [
    "FilesystemObjectAddress",
    "FilesystemStorageDriver",
    "FtpDriverOptions",
    "FtpObjectAddress",
    "FtpStorageDriver",
    "HttpObjectAddress",
    "HttpStorageDriver",
    "RcloneObjectAddress",
    "RcloneStorageDriver",
    "WritableRcloneStorageDriver",
    "S3ClientAPI",
    "S3ObjectAddress",
    "S3StorageDriver",
    "SquashfsObjectAddress",
    "SquashfsStorageDriver",
    "SQLiteObjectAddress",
    "SQLiteStorageDriver",
]
