"""Configured read-only Store powered by rclone."""

from __future__ import annotations

import os
import subprocess
import threading
import time

from dataclasses import dataclass, fields, replace
from typing import Any, Dict, Optional, Sequence
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    DriverBackedStoreAPI,
    IngestMetadataAvailability,
    IngestSourceCapabilities,
    Location,
    StoreConfiguration,
    StorageInvalidAddress,
)
from LiuXin_alpha.storage.drivers.rclone import (
    RcloneObjectAddress,
    RcloneStorageDriver,
)
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

from .rclone_utils import run_rclone, run_rclone_json, which_rclone


RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT = 1200.0
RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY = (
    "rclone_http_max_requests_per_hour_default"
)


def get_default_rclone_http_requests_per_hour() -> float:
    """Return the preference-backed polite request-rate default."""

    default = float(RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT)
    try:
        from LiuXin_alpha.preferences import preferences

        raw = preferences.get(RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY, default)
        value = default if raw is None else float(raw)
    except Exception:
        return default
    return value


def _normalize_rclone_fs_root(url: str) -> str:
    """Convert a plain HTTP URL to rclone's config-less HTTP backend syntax."""

    root = str(url or "").strip()
    if not root:
        raise StorageInvalidAddress("rclone filesystem root must not be empty.")
    lowered = root.lower()
    if lowered.startswith(("http://", "https://")):
        parsed = urlsplit(root)
        if parsed.username is not None or parsed.password is not None:
            raise StorageInvalidAddress(
                "HTTP rclone roots must not embed credentials."
            )
        if parsed.query or parsed.fragment:
            raise StorageInvalidAddress(
                "HTTP rclone roots must not contain query or fragment data."
            )
        normalized_http = root.rstrip("/")
        quoted = normalized_http.replace('"', '\\"')
        return f':http,url="{quoted}":'
    return root


@dataclass
class RcloneBackendOptions:
    """Runtime options controlling rclone invocation and politeness."""

    rclone_exe: str = "rclone"
    rclone_args: Sequence[str] = ()
    env: Dict[str, str] | None = None
    timeout_s: float | None = 60.0
    max_http_requests_per_hour: float | None = None
    apply_rclone_tpslimit: bool = True
    rclone_tpslimit_burst: int = 1
    enforce_global_rate_limit: bool = True

    def __post_init__(self) -> None:
        if self.max_http_requests_per_hour is None:
            self.max_http_requests_per_hour = (
                get_default_rclone_http_requests_per_hour()
            )
        if self.rclone_tpslimit_burst < 1:
            raise ValueError("rclone_tpslimit_burst must be at least one.")


def _durable_rclone_options(
    options: RcloneBackendOptions,
) -> tuple[tuple[str, object], ...]:
    """Return non-secret, JSON-compatible rclone process options."""

    durable: list[tuple[str, object]] = []
    for field in fields(options):
        if field.name == "env":
            continue
        value = getattr(options, field.name)
        if field.name == "rclone_args":
            value = tuple(str(argument) for argument in value)
        durable.append((field.name, value))
    return tuple(durable)


class RcloneHttpReadOnlyStorageBackend(
    DriverBackedStoreAPI[RcloneObjectAddress]
):
    """One configured, completely enumerable read-only rclone filesystem."""

    store_kind = "rclone_readonly"

    def __init__(
        self,
        url: str,
        *,
        name: Optional[str] = None,
        uuid: str | UUID | None = None,
        options: RcloneBackendOptions | None = None,
        configuration: StoreConfiguration | None = None,
    ) -> None:
        self.url = _normalize_rclone_fs_root(url)
        self.options = options or RcloneBackendOptions()
        self._rate_limit_lock = threading.Lock()
        self._next_allowed_request_monotonic = 0.0
        store_uuid = (
            configuration.store_uuid
            if configuration is not None
            else uuid4() if uuid is None else (
                uuid if isinstance(uuid, UUID) else UUID(uuid)
            )
        )
        self.__driver = RcloneStorageDriver(
            self.url,
            address_space_uuid=store_uuid,
            json_runner=lambda arguments: self.run_rclone_json(
                arguments,
                check=True,
            ),
            process_spawner=self.spawn_rclone_process,
            probe=self._probe_rclone,
        )
        self._configuration = configuration or StoreConfiguration(
            store_uuid=store_uuid,
            store_name=name or self.url_to_name(self.url),
            store_kind=self.store_kind,
            store_root_uri=self.url,
            store_url=self.url,
            store_access_protocol="rclone",
            read_only=True,
            supports_folders=True,
            backend_options=_durable_rclone_options(self.options),
        )

    @property
    def configuration(self) -> StoreConfiguration:
        return self._configuration

    @property
    def _driver(self) -> RcloneStorageDriver:
        return self.__driver

    @property
    def driver(self) -> RcloneStorageDriver:
        return self.__driver

    @property
    def root_path(self) -> str:
        return self.url

    @property
    def ingest_capabilities(self) -> IngestSourceCapabilities:
        """Advertise the hashes and metadata rclone may return on inspection.

        Example:
            >>> profile = store.ingest_capabilities  # doctest: +SKIP
            >>> "sha256" in profile.authoritative_digest_algorithms  # doctest: +SKIP
            True

        :return: Rclone-specific source-ingest capability profile.
        """

        return replace(
            super().ingest_capabilities,
            authoritative_digest_algorithms=("sha256", "sha1", "md5"),
            metadata_availability=IngestMetadataAvailability.INSPECTION,
        )

    @staticmethod
    def url_to_name(url: str) -> str:
        return safe_path_to_name(url)

    def _normalized_requests_per_hour(self) -> float | None:
        value = self.options.max_http_requests_per_hour
        if value is None:
            return None
        try:
            rate = float(value)
        except (TypeError, ValueError):
            return None
        return rate if rate > 0 else None

    def _effective_rclone_args(self) -> tuple[str, ...]:
        arguments = list(self.options.rclone_args)
        rate = self._normalized_requests_per_hour()
        if not self.options.apply_rclone_tpslimit or rate is None:
            return tuple(arguments)
        has_limit = any(
            argument == "--tpslimit" or str(argument).startswith("--tpslimit=")
            for argument in arguments
        )
        has_burst = any(
            argument == "--tpslimit-burst"
            or str(argument).startswith("--tpslimit-burst=")
            for argument in arguments
        )
        if not has_limit:
            arguments.append(f"--tpslimit={rate / 3600.0:.8f}")
        if not has_burst:
            arguments.append(
                f"--tpslimit-burst={int(self.options.rclone_tpslimit_burst)}"
            )
        return tuple(arguments)

    def _acquire_rate_limit_slot(self) -> None:
        if not self.options.enforce_global_rate_limit:
            return
        rate = self._normalized_requests_per_hour()
        if rate is None:
            return
        interval = 3600.0 / rate
        sleep_for = 0.0
        with self._rate_limit_lock:
            now = time.monotonic()
            if now < self._next_allowed_request_monotonic:
                sleep_for = self._next_allowed_request_monotonic - now
                self._next_allowed_request_monotonic += interval
            else:
                self._next_allowed_request_monotonic = now + interval
        if sleep_for:
            time.sleep(sleep_for)

    def run_rclone(
        self,
        args: Sequence[str],
        *,
        check: bool = True,
        timeout_s: float | None = None,
    ):
        self._acquire_rate_limit_slot()
        return run_rclone(
            args,
            rclone_exe=self.options.rclone_exe,
            extra_args=self._effective_rclone_args(),
            env=self.options.env,
            timeout_s=self.options.timeout_s if timeout_s is None else timeout_s,
            check=check,
        )

    def run_rclone_json(
        self,
        args: Sequence[str],
        *,
        check: bool = True,
        timeout_s: float | None = None,
    ):
        self._acquire_rate_limit_slot()
        return run_rclone_json(
            args,
            rclone_exe=self.options.rclone_exe,
            extra_args=self._effective_rclone_args(),
            env=self.options.env,
            timeout_s=self.options.timeout_s if timeout_s is None else timeout_s,
            check=check,
        )

    def spawn_rclone_process(self, args: Sequence[str]) -> subprocess.Popen:
        self._acquire_rate_limit_slot()
        executable = which_rclone(self.options.rclone_exe)
        command = [executable, *self._effective_rclone_args(), *list(args)]
        environment = dict(os.environ)
        if self.options.env:
            environment.update(dict(self.options.env))
        return subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
        )

    def _probe_rclone(self) -> None:
        self.run_rclone(["version"], check=True)
        self.run_rclone_json(
            ["lsjson", "--max-depth", "1", self.url],
            check=True,
        )

    def locate(self, identifier: str | Location) -> Location:
        """Accept an opaque relative key or a root-owned rclone identifier."""

        if isinstance(identifier, Location):
            return self.require_location(identifier)
        text = str(identifier)
        prefix = self.url if self.url.endswith(":") else self.url.rstrip("/") + "/"
        if text.startswith(prefix):
            return self._location(self.__driver.object_address_from_uri(text))
        return super().locate(text)

    def self_test(self):
        return self.probe()


__all__ = [
    "RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT",
    "RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "RcloneBackendOptions",
    "RcloneHttpReadOnlyStorageBackend",
    "get_default_rclone_http_requests_per_hour",
    "run_rclone",
    "run_rclone_json",
    "which_rclone",
]
