"""
Configure read-only rclone Stores and their executable, timeout, and pacing policy.

HTTP roots are translated into config-less rclone identifiers; named and other
roots share the same driver adapter. Runtime options, durable snapshots, process
timers, and per-instance command schedules have separate lifetimes. Helpers here
do not guarantee that arbitrary command arguments or captured output are secret-free.
"""

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


_monotonic = time.monotonic
_sleep = time.sleep


RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT = 1200.0
RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY = (
    "rclone_http_max_requests_per_hour_default"
)


class _TimedRcloneProcess:
    """
    Wrap process lifecycle methods with a daemon timer that kills a still-running command at expiry.

    The timer starts during wrapper construction and owns no output-draining thread. A recorded
    expiry becomes TimeoutExpired only when wait returns; poll itself returns the underlying status.
    Caller stream ownership is unchanged.

    Example:
        >>> timed = _TimedRcloneProcess(process, timeout_s=10, command=["rclone", "cat", "archive:book"])  # doctest: +SKIP
    """

    def __init__(
        self,
        process: subprocess.Popen,
        *,
        timeout_s: float,
        command: list[str],
    ) -> None:
        """
        Retain process/command state, expose its streams, and start the daemon deadline timer.

        Example:
            >>> timed = _TimedRcloneProcess(process, timeout_s=10, command=command)  # doctest: +SKIP


        :param process: Already-spawned process whose streams and lifecycle methods are delegated.
        :param timeout_s: Timer interval in seconds, also retained in the eventual timeout diagnostic.
        :param command: Argument list retained by reference for TimeoutExpired context.
        :return: None after starting the timer; no interval validation or constructor-failure process cleanup is added.
        """
        self._process = process
        self._timeout_s = timeout_s
        self._command = command
        self._timed_out = threading.Event()
        self.stdout = process.stdout
        self.stderr = process.stderr
        self._timer = threading.Timer(timeout_s, self._expire)
        self._timer.daemon = True
        self._timer.start()

    def _expire(self) -> None:
        """
        If poll still reports running, record expiry before requesting a forceful kill.

        Example:
            >>> timed._expire()  # doctest: +SKIP


        :return: None after the check and possible kill; poll/kill exceptions are not suppressed here.
        """
        if self._process.poll() is None:
            self._timed_out.set()
            self._process.kill()

    def wait(self, timeout: float | None = None) -> int:
        """
        Delegate waiting, cancel the timer after successful wait conversion, then report any
        recorded deadline expiry.

        Example:
            >>> code = timed.wait(timeout=1)  # doctest: +SKIP


        :param timeout: Optional timeout for this wait call, separate from the timer deadline.
        :return: Integer exit status unless the timer expired, in which case TimeoutExpired is raised; a failed wait leaves timer cancellation to another path.
        """
        return_code = int(self._process.wait(timeout=timeout))
        self._timer.cancel()
        if self._timed_out.is_set():
            raise subprocess.TimeoutExpired(
                self._command,
                self._timeout_s,
            )
        return return_code

    def poll(self) -> int | None:
        """
        Delegate nonblocking completion inspection and cancel the timer only once an exit status
        exists.

        Example:
            >>> running = timed.poll() is None  # doctest: +SKIP


        :return: Underlying exit status or None; the expiry flag is not translated by poll.
        """
        return_code = self._process.poll()
        if return_code is not None:
            self._timer.cancel()
        return return_code

    def terminate(self) -> None:
        """
        Cancel the deadline timer and forward a termination request to the process.

        Example:
            >>> timed.terminate()  # doctest: +SKIP


        :return: None after the request; process-call failures propagate and no wait or stream close occurs.
        """
        self._timer.cancel()
        self._process.terminate()

    def kill(self) -> None:
        """
        Cancel the deadline timer and forward a forceful kill request to the process.

        Example:
            >>> timed.kill()  # doctest: +SKIP


        :return: None after the request; no completion wait, expiry-marker update, or stream close occurs.
        """
        self._timer.cancel()
        self._process.kill()


def get_default_rclone_http_requests_per_hour() -> float:
    """
    Read the preference-backed rate, falling back to 1200 requests per hour on ordinary
    lookup/conversion failure.

    None uses the default. The returned float is not required to be positive or finite; rate
    normalization belongs to the Store invocation policy.

    Example:
        >>> rate = get_default_rclone_http_requests_per_hour()
        >>> isinstance(rate, float)
        True


    :return: Preference value converted to float, or the float default after None or an ordinary failure.
    """

    default = float(RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT)
    try:
        from LiuXin_alpha.preferences import preferences

        raw = preferences.get(RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY, default)
        value = default if raw is None else float(raw)
    except Exception:
        return default
    return value


def _normalize_rclone_fs_root(url: str) -> str:
    """
    Wrap plain HTTP/HTTPS roots in config-less rclone HTTP syntax and retain other stripped roots.

    Reject blank input, HTTP user information, query, and fragment. Strip trailing HTTP slashes and
    escape double quotes before wrapping. This helper does not fully validate host/port/path syntax;
    raw-driver validation runs later.

    Example:
        >>> _normalize_rclone_fs_root("https://example.test/books/")
        ':http,url="https://example.test/books":'


    :param url: Root candidate stringified after falsey input becomes empty text, then stripped.
    :return: Config-less HTTP root or retained non-HTTP root text, without network access.
    """

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
    """
    Retain mutable executable, environment, timeout, pacing, and inventory settings.

    None rate resolves the shared preference during construction. Only burst and inventory/token
    lower bounds are checked here; later mutation is not revalidated. env is runtime state and
    omitted from durable configuration, but arbitrary command arguments are retained there without
    secret filtering.

    Example:
        >>> options = RcloneBackendOptions(max_http_requests_per_hour=0)
        >>> options.timeout_s, options.max_inventory_entries
        (60.0, 100000)


    :ivar rclone_exe: Executable name/path resolved for each invocation.
    :ivar rclone_args: Extra global arguments retained for invocation and durable configuration.
    :ivar env: Optional environment overlay, excluded from durable option snapshots.
    :ivar timeout_s: Captured-command timeout or streaming timer interval in seconds; None disables the explicit timeout.
    :ivar max_http_requests_per_hour: Rate used for command spacing and generated TPS flags, or None to resolve the construction-time preference.
    :ivar apply_rclone_tpslimit: Whether to append missing per-process TPS flags for a positive normalized rate.
    :ivar rclone_tpslimit_burst: Burst value appended when no explicit burst argument exists.
    :ivar enforce_global_rate_limit: Whether this Store instance spaces command starts using its own schedule.
    :ivar max_inventory_entries: Positive inventory observation bound captured by a driver at construction.
    :ivar max_json_token_chars: Positive buffered decoded-character bound captured by a driver at construction.
    """

    rclone_exe: str = "rclone"
    rclone_args: Sequence[str] = ()
    env: Dict[str, str] | None = None
    timeout_s: float | None = 60.0
    max_http_requests_per_hour: float | None = None
    apply_rclone_tpslimit: bool = True
    rclone_tpslimit_burst: int = 1
    enforce_global_rate_limit: bool = True
    max_inventory_entries: int = 100_000
    max_json_token_chars: int = 8 * 1024 * 1024

    def __post_init__(self) -> None:
        """
        Resolve an omitted rate preference and reject burst/inventory/token values below one.

        Example:
            >>> RcloneBackendOptions(max_http_requests_per_hour=0).rclone_tpslimit_burst
            1


        :return: None after preference assignment and lower-bound checks; no general type, timeout, rate, or finiteness validation is performed.
        """
        if self.max_http_requests_per_hour is None:
            self.max_http_requests_per_hour = (
                get_default_rclone_http_requests_per_hour()
            )
        if self.rclone_tpslimit_burst < 1:
            raise ValueError("rclone_tpslimit_burst must be at least one.")
        if self.max_inventory_entries < 1:
            raise ValueError("max_inventory_entries must be positive.")
        if self.max_json_token_chars < 1:
            raise ValueError("max_json_token_chars must be positive.")


def _durable_rclone_options(
    options: RcloneBackendOptions,
) -> tuple[tuple[str, object], ...]:
    """
    Snapshot option fields except env, converting rclone_args to a tuple of stringified arguments.

    Other values are retained as supplied. This is not arbitrary JSON-compatibility validation or
    secret detection: argument strings can still contain credentials.

    Example:
        >>> fields = dict(_durable_rclone_options(RcloneBackendOptions(max_http_requests_per_hour=0, env={"TOKEN": "example"})))
        >>> "env" in fields
        False


    :param options: Dataclass option record whose fields are visited in declaration order.
    :return: Tuple of name/value pairs excluding env; mutable custom field contents are not deep-copied.
    """

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
    """
    Configure a read-only rclone filesystem with shared invocation, pacing, and ingest adapters.

    The historical HTTP name also accepts other rclone roots. Runtime options are shared and
    mutable; driver inventory bounds are captured at construction. Command pacing is local to this
    instance, while generated TPS flags apply separately to each process. Construction does not
    invoke rclone.

    Example:
        >>> store = RcloneHttpReadOnlyStorageBackend("archive:", options=RcloneBackendOptions(max_http_requests_per_hour=0))
        >>> store.configuration.store_kind
        'rclone_readonly'
    """

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
        """
        Normalize the root, choose Store identity, and bind invocation callbacks to a raw read-only
        driver.

        Supplied configuration determines the UUID and is retained rather than reconciled with
        url/options/name. Otherwise create a read-only configuration with a snapshot excluding env.
        The runtime root and mutable options remain separate from that snapshot; construction
        initializes a per-instance rate schedule.

        Example:
            >>> store = RcloneHttpReadOnlyStorageBackend("archive:", options=RcloneBackendOptions(max_http_requests_per_hour=0))
            >>> store.driver.root_uri
            'archive:'


        :param url: HTTP or rclone root normalized for the runtime driver.
        :param name: Truthy display-name override when creating configuration, otherwise a path-derived label.
        :param uuid: UUID or UUID text used only without supplied configuration; None creates a fresh UUID.
        :param options: Mutable runtime option record, or a new preference-backed record when omitted.
        :param configuration: Optional configuration retained with its identity; other runtime arguments are not checked for consistency against it.
        :return: None after configuring the driver, callbacks, rate schedule, and retained/generated StoreConfiguration.
        """
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
            max_inventory_entries=self.options.max_inventory_entries,
            max_json_token_chars=self.options.max_json_token_chars,
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
        """
        Expose the retained supplied or construction-time Store configuration.

        Example:
            >>> store = RcloneHttpReadOnlyStorageBackend("archive:", options=RcloneBackendOptions(max_http_requests_per_hour=0))
            >>> store.configuration.read_only
            True


        :return: StoreConfiguration without refreshing runtime options, root, or status.
        """
        return self._configuration

    @property
    def _driver(self) -> RcloneStorageDriver:
        """
        Supply the privately retained read-only raw driver to inherited Store operations.

        Example:
            >>> store = RcloneHttpReadOnlyStorageBackend("archive:", options=RcloneBackendOptions(max_http_requests_per_hour=0))
            >>> store._driver is store.driver
            True


        :return: Existing RcloneStorageDriver; access neither starts it nor transfers ownership.
        """
        return self.__driver

    @property
    def driver(self) -> RcloneStorageDriver:
        """
        Expose the retained raw read-only driver for transport-level operations.

        Example:
            >>> store = RcloneHttpReadOnlyStorageBackend("archive:", options=RcloneBackendOptions(max_http_requests_per_hour=0))
            >>> store.driver.root_uri
            'archive:'


        :return: The configured RcloneStorageDriver used by this adapter.
        """
        return self.__driver

    @property
    def root_path(self) -> str:
        """
        Return the normalized runtime rclone root identifier.

        Example:
            >>> store = RcloneHttpReadOnlyStorageBackend("archive:", options=RcloneBackendOptions(max_http_requests_per_hour=0))
            >>> store.root_path
            'archive:'


        :return: self.url text, which may describe a remote or config-less connection rather than a local filesystem path.
        """
        return self.url

    @property
    def ingest_capabilities(self) -> IngestSourceCapabilities:
        """
        Add reported SHA-256/SHA-1/MD5 evidence and inspection metadata to the inherited ingest
        profile.

        Example:
            >>> store = RcloneHttpReadOnlyStorageBackend("archive:", options=RcloneBackendOptions(max_http_requests_per_hour=0))
            >>> store.ingest_capabilities.authoritative_digest_algorithms
            ('sha256', 'sha1', 'md5')


        :return: New profile naming supported reported digest algorithms; a particular remote/object may provide none of them.
        """

        return replace(
            super().ingest_capabilities,
            authoritative_digest_algorithms=("sha256", "sha1", "md5"),
            metadata_availability=IngestMetadataAvailability.INSPECTION,
        )

    @staticmethod
    def url_to_name(url: str) -> str:
        """
        Build a display label using shared path-name sanitization, length, and hash defaults.

        Example:
            >>> label = RcloneHttpReadOnlyStorageBackend.url_to_name("archive:books")
            >>> label.isascii() and len(label) <= 120
            True


        :param url: Path-like naming input; the helper does not parse or redact arbitrary connection credentials.
        :return: Generated ASCII label from safe_path_to_name with its default options.
        """
        return safe_path_to_name(url)

    def _normalized_requests_per_hour(self) -> float | None:
        """
        Float-convert the current rate and retain it only when greater than zero.

        Example:
            >>> store = RcloneHttpReadOnlyStorageBackend("archive:", options=RcloneBackendOptions(max_http_requests_per_hour=0))
            >>> store._normalized_requests_per_hour() is None
            True


        :return: Positive rate or None for absent, nonpositive, NaN, or TypeError/ValueError input; positive infinity is not rejected.
        """
        value = self.options.max_http_requests_per_hour
        if value is None:
            return None
        try:
            rate = float(value)
        except (TypeError, ValueError):
            return None
        return rate if rate > 0 else None

    def _effective_rclone_args(self) -> tuple[str, ...]:
        """
        Append missing TPS limit and burst arguments when generation is enabled and a positive rate
        exists.

        Example:
            >>> store = RcloneHttpReadOnlyStorageBackend("archive:", options=RcloneBackendOptions(max_http_requests_per_hour=0))
            >>> store._effective_rclone_args()
            ()


        :return: Tuple preserving caller arguments; explicit --tpslimit/--tpslimit-burst forms prevent the corresponding generated flag.
        """
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
        """
        Reserve a start time under this Store's lock and sleep outside the lock if necessary.

        Interval is 3600/rate seconds. The schedule advances before sleeping or command execution;
        later failure does not refund a slot. The historical global option coordinates calls on this
        instance, not other Stores or actual remote requests.

        Example:
            >>> store = RcloneHttpReadOnlyStorageBackend("archive:", options=RcloneBackendOptions(max_http_requests_per_hour=0))
            >>> store._acquire_rate_limit_slot()


        :return: None after a disabled-policy shortcut or reservation and any required sleep.
        """
        if not self.options.enforce_global_rate_limit:
            return
        rate = self._normalized_requests_per_hour()
        if rate is None:
            return
        interval = 3600.0 / rate
        sleep_for = 0.0
        with self._rate_limit_lock:
            now = _monotonic()
            if now < self._next_allowed_request_monotonic:
                sleep_for = self._next_allowed_request_monotonic - now
                self._next_allowed_request_monotonic += interval
            else:
                self._next_allowed_request_monotonic = now + interval
        if sleep_for:
            _sleep(sleep_for)

    def run_rclone(
        self,
        args: Sequence[str],
        *,
        check: bool = True,
        timeout_s: float | None = None,
    ):
        """
        Reserve a command slot and invoke the shared captured-output runner with current runtime
        options.

        Example:
            >>> result = store.run_rclone(["version"], timeout_s=10)  # doctest: +SKIP


        :param args: Rclone command and arguments passed after effective global options.
        :param check: Whether the shared runner rejects nonzero exit status.
        :param timeout_s: Per-call timeout override in seconds; None uses the current options value rather than disabling it.
        :return: RcloneResult from the shared runner; its unredacted command/output exceptions propagate directly here.
        """
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
        """
        Reserve a command slot and invoke the shared JSON runner using effective arguments and
        environment.

        Example:
            >>> listing = store.run_rclone_json(["lsjson", store.url])  # doctest: +SKIP


        :param args: Rclone command and arguments passed after effective global options.
        :param check: Whether a nonzero exit is rejected before output decoding.
        :param timeout_s: Per-call seconds override; None uses options.timeout_s.
        :return: Decoded value of any shape or None for blank output; direct shared-runner errors are not translated here.
        """
        self._acquire_rate_limit_slot()
        return run_rclone_json(
            args,
            rclone_exe=self.options.rclone_exe,
            extra_args=self._effective_rclone_args(),
            env=self.options.env,
            timeout_s=self.options.timeout_s if timeout_s is None else timeout_s,
            check=check,
        )

    def spawn_rclone_process(self, args: Sequence[str]):
        """
        Reserve a slot, resolve the executable, overlay the environment, and spawn piped binary
        output.

        None timeout returns Popen directly; otherwise wrap it with a deadline timer. No shell or
        output reader is started here. A failure while constructing the timeout wrapper can follow
        process creation without local cleanup.

        Example:
            >>> process = store.spawn_rclone_process(["cat", "archive:book"])  # doctest: +SKIP


        :param args: Command and arguments appended after the resolved executable and effective global options.
        :return: Owned Popen or _TimedRcloneProcess; callers must consume or close its streams and stop the process.
        """
        self._acquire_rate_limit_slot()
        executable = which_rclone(self.options.rclone_exe)
        command = [executable, *self._effective_rclone_args(), *list(args)]
        environment = dict(os.environ)
        if self.options.env:
            environment.update(dict(self.options.env))
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
        )
        if self.options.timeout_s is None:
            return process
        return _TimedRcloneProcess(
            process,
            timeout_s=float(self.options.timeout_s),
            command=command,
        )

    def _probe_rclone(self) -> None:
        """
        Run version followed by a checked depth-one JSON listing of the runtime root.

        Example:
            >>> store._probe_rclone()  # doctest: +SKIP


        :return: None after both commands return; listing shape and write permission are not validated.
        """
        self.run_rclone(["version"], check=True)
        self.run_rclone_json(
            ["lsjson", "--max-depth", "1", self.url],
            check=True,
        )

    def locate(self, identifier: str | Location) -> Location:
        """
        Accept an owned Location, strip this exact root from a full identifier, or parse remaining
        text as a relative key.

        Example:
            >>> store = RcloneHttpReadOnlyStorageBackend("archive:", options=RcloneBackendOptions(max_http_requests_per_hour=0))
            >>> store.locate("archive:book.epub").key
            'book.epub'


        :param identifier: Owned Location or text; a nonmatching remote-looking string is treated as a key by the inherited parser.
        :return: Opaque owned Location without object existence or URI percent-decoding checks.
        """

        if isinstance(identifier, Location):
            return self.require_location(identifier)
        text = str(identifier)
        prefix = self.url if self.url.endswith(":") else self.url.rstrip("/") + "/"
        if text.startswith(prefix):
            return self._location(self.__driver.object_address_from_uri(text))
        return super().locate(text)

    def self_test(self):
        """
        Delegate to the inherited Store probe and its rclone availability operations.

        Example:
            >>> status = store.self_test()  # doctest: +SKIP


        :return: Translated StoreStatus; underlying command errors follow the configured probe boundaries.
        """
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
