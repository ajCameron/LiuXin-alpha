"""Packaged command-line surface for storage discovery and ingestion."""

from __future__ import annotations

import argparse
import base64
import dataclasses
import importlib.util
import json
import logging
import os
import platform
import shutil
import signal
import socket
import sys
import tempfile
import threading
import traceback

from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager, nullcontext
from datetime import date, datetime, time
from enum import Enum
from pathlib import Path
from types import FrameType
from typing import Any, cast, final
from urllib.parse import urlparse
from uuid import UUID, uuid4

from LiuXin_alpha.constants import __version__ as liuxin_version
from LiuXin_alpha.ingest.mixed_application import (
    MixedIngestApplicationRequest,
    MixedIngestBudget,
    execute_mixed_ingest,
)
from LiuXin_alpha.surfaces.cli.common import (
    add_connection_arguments,
    add_json_output,
    decode_wire_bytes,
    emit_bytes,
    emit_json,
    load_json_object,
    open_cli_core,
)
from LiuXin_alpha.surfaces.system_profile import load_system_profile


from LiuXin_alpha.utils.lock import ExclusiveFile, LockError
from LiuXin_alpha.utils.logging import get_compat_logger
from LiuXin_alpha.utils.logging.run_logging import LoggingTextStream, RunLoggingSession
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


EXIT_OK = 0
EXIT_ISSUES = 1
EXIT_USAGE = 2
EXIT_INTERRUPTED = 130
EXIT_TERMINATED = 143

_GIB = 1024**3
_LOGGER = get_compat_logger("LiuXin_alpha.storage.ingest.mixed_cli")


class CLIUsageError(ValueError):
    """An actionable command configuration error."""


class _StorageAddCancelled(Exception):
    """The operator left the Store-add wizard before persistence."""


_SENSITIVE_STORE_OPTION_MARKERS = (
    "access_key",
    "api_key",
    "authorization",
    "credential",
    "password",
    "private_key",
    "secret",
    "token",
)


@final
class SignalCancellation:
    """Convert the first interrupt/termination signal into graceful cancellation.

    A second signal raises ``KeyboardInterrupt`` so an operator can still force
    the Python workflow boundary to unwind if graceful cancellation stalls in
    a parser or external program.
    """

    def __init__(self) -> None:
        self._requested = threading.Event()
        self._signal_number: int | None = None
        self._previous: dict[int, object] = {}
        self._installed = False

    @property
    def signal_number(self) -> int | None:
        return self._signal_number

    def requested(self) -> bool:
        return self._requested.is_set()

    def __enter__(self) -> "SignalCancellation":
        if threading.current_thread() is not threading.main_thread():
            return self
        for signal_number in (signal.SIGINT, signal.SIGTERM):
            self._previous[int(signal_number)] = signal.getsignal(signal_number)
            _ = signal.signal(signal_number, self._receive)
        self._installed = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback_value: object,
    ) -> None:
        del exc_type, exc, traceback_value
        if not self._installed:
            return
        for signal_number, previous in self._previous.items():
            _ = signal.signal(
                signal_number,
                cast("signal._HANDLER", previous),
            )
        self._installed = False

    def _receive(self, signal_number: int, _frame: FrameType | None) -> None:
        if self._requested.is_set():
            raise KeyboardInterrupt(
                f"received signal {signal_number} after cancellation was requested"
            )
        self._signal_number = int(signal_number)
        self._requested.set()


def add_storage_ingest_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the complete mixed-ingest option contract to ``parser``."""

    defaults = MixedIngestBudget()
    parser.add_argument(
        "--source-root",
        required=True,
        help="Existing local tree to inspect without following symlinks.",
    )
    parser.add_argument(
        "--database",
        help="SQLite LiuXin catalogue path (required for a real or preflight run).",
    )
    parser.add_argument(
        "--system-root",
        help=(
            "Use database, materialization, and log paths from "
            "SYSTEM_ROOT/liuxin-system.json. Explicit path options override it."
        ),
    )
    parser.add_argument(
        "--profile",
        help="Named profile, manifest path, or manifest directory.",
    )
    parser.add_argument(
        "--materialization-root",
        help=(
            "Managed cache outside source-root for nested container bytes; "
            "top-level-only runs do not require it."
        ),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--discover-only",
        action="store_true",
        help="Classify top-level files without database, Store, or cache writes.",
    )
    mode.add_argument(
        "--preflight-only",
        action="store_true",
        help=(
            "Perform discovery with no catalogue/cache writes plus path, "
            "capacity, and dependency readiness checks."
        ),
    )
    parser.add_argument(
        "--require-existing-database",
        action="store_true",
        help="Refuse to create a new catalogue when --database does not exist.",
    )
    parser.add_argument(
        "--no-recursive-filesystem",
        action="store_true",
        help="Inspect only files immediately below source-root.",
    )
    parser.add_argument(
        "--no-nested-containers",
        action="store_true",
        help="Inventory top-level containers without opening containers inside them.",
    )
    parser.add_argument(
        "--expand-ebook-containers",
        action="store_true",
        help="Treat EPUB/CBZ/CBR and other ZIP/RAR-like ebooks as containers.",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Re-read adopted bytes and mark successful Replicas verified.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Stop at the first bad source, container, or member.",
    )

    limits = parser.add_argument_group("run-wide safety limits")
    limits.add_argument(
        "--max-source-files", type=int, default=defaults.max_source_files
    )
    limits.add_argument("--max-containers", type=int, default=defaults.max_containers)
    limits.add_argument(
        "--max-container-depth", type=int, default=defaults.max_container_depth
    )
    limits.add_argument("--max-members", type=int, default=defaults.max_members)
    limits.add_argument(
        "--max-members-per-container",
        type=int,
        default=defaults.max_members_per_container,
    )
    limits.add_argument(
        "--max-member-gib",
        type=float,
        default=defaults.max_member_bytes / _GIB,
    )
    limits.add_argument(
        "--max-container-expanded-gib",
        type=float,
        default=defaults.max_container_expanded_bytes / _GIB,
    )
    limits.add_argument(
        "--max-total-expanded-gib",
        type=float,
        default=defaults.max_total_expanded_bytes / _GIB,
    )
    limits.add_argument(
        "--max-expansion-ratio",
        type=float,
        default=defaults.max_container_expansion_ratio,
    )
    limits.add_argument(
        "--max-materialized-gib",
        type=float,
        default=defaults.max_materialized_bytes / _GIB,
    )
    limits.add_argument(
        "--max-temporary-gib",
        type=float,
        default=defaults.max_temporary_bytes / _GIB,
    )
    limits.add_argument(
        "--max-path-depth", type=int, default=defaults.max_path_depth
    )
    limits.add_argument(
        "--max-path-bytes", type=int, default=defaults.max_path_bytes
    )
    limits.add_argument("--max-issues", type=int, default=defaults.max_issues)
    limits.add_argument(
        "--max-wall-time-seconds",
        type=float,
        default=defaults.max_wall_time_s,
    )

    backends = parser.add_argument_group("container backends")
    backends.add_argument(
        "--unsquashfs-exe",
        default="unsquashfs",
        help="unsquashfs executable name or path (default: unsquashfs).",
    )
    backends.add_argument(
        "--rar-extractor-exe",
        default=None,
        help="Optional unrar/rar executable for compressed RAR members.",
    )
    backends.add_argument(
        "--backend-timeout-seconds",
        type=float,
        default=300.0,
        help="Per external backend operation timeout (default: 300).",
    )

    logging_group = parser.add_argument_group("logging and reports")
    logging_group.add_argument(
        "--run-id",
        type=_uuid_argument,
        default=None,
        help="Operator correlation UUID; generated when omitted.",
    )
    logging_group.add_argument(
        "--log-directory",
        help=(
            "Directory for the rotating text and authoritative JSONL logs; "
            "defaults beside the catalogue and must be outside source-root."
        ),
    )
    logging_group.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="DEBUG",
        help="Durable minimum level (default: DEBUG, including every object).",
    )
    logging_group.add_argument(
        "--log-max-mib",
        type=int,
        default=100,
        help="Maximum human-log segment size in MiB (default: 100).",
    )
    logging_group.add_argument(
        "--log-backup-count",
        type=int,
        default=10,
        help="Rotated human-log backups to retain (default: 10).",
    )
    logging_group.add_argument(
        "--log-checkpoint-every",
        type=int,
        default=1_000,
        help="Aggregate source/member checkpoint interval (default: 1000).",
    )
    logging_group.add_argument(
        "--report-file",
        help="Full atomic JSON report path; defaults beside this run's logs.",
    )
    logging_group.add_argument(
        "--replace-report",
        action="store_true",
        help="Allow an explicit existing --report-file to be atomically replaced.",
    )
    logging_group.add_argument(
        "--compact-json",
        action="store_true",
        help="Emit compact JSON on stdout and in the report file.",
    )
    logging_group.add_argument(
        "--no-stdout-report",
        action="store_true",
        help="Write only the report file; leave stdout empty.",
    )
    logging_group.add_argument(
        "--no-console-progress",
        action="store_true",
        help="Suppress stderr progress; durable logs are unaffected.",
    )

    locking = parser.add_argument_group("concurrency")
    locking.add_argument(
        "--lock-file",
        help="Stable advisory run-lock path; defaults in the log directory.",
    )
    locking.add_argument(
        "--lock-timeout-seconds",
        type=int,
        default=0,
        help="Seconds to wait for another real ingest to release its lock.",
    )
    locking.add_argument(
        "--no-run-lock",
        action="store_true",
        help="Disable the real-run advisory lock (unsafe for one catalogue).",
    )


def _storage_query(args: argparse.Namespace, operation: str, payload: dict[str, Any]) -> int:
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.query(operation, payload)
    emit_json(result, args)
    return 0


def _storage_command(args: argparse.Namespace, operation: str, payload: dict[str, Any]) -> int:
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command(operation, payload)
    emit_json(result, args)
    return 0


def _store_reference(value: str) -> str | int:
    token = str(value).strip()
    try:
        return int(token)
    except ValueError:
        return token


def _bounded_file_bytes(path: str, max_mib: float) -> bytes:
    if max_mib <= 0:
        raise ValueError("--max-transfer-mib must be greater than zero.")
    source = Path(path).expanduser()
    limit = int(max_mib * 1024 * 1024)
    with source.open("rb") as stream:
        content = stream.read(limit + 1)
    if len(content) > limit:
        raise ValueError(
            "Input exceeds the configured {} byte transfer limit: {!s}".format(
                limit, source
            )
        )
    return content


def cmd_storage_stores_list(args: argparse.Namespace) -> int:
    return _storage_query(args, "storage.stores.list", {"refresh": args.refresh})


def cmd_storage_store_show(args: argparse.Namespace) -> int:
    return _storage_query(
        args, "storage.store.get", {"store": _store_reference(args.store)}
    )


def cmd_storage_store_save(args: argparse.Namespace) -> int:
    return _storage_command(
        args, "storage.store.save", {"store": load_json_object(args.store_file)}
    )


def _storage_stdin_is_interactive() -> bool:
    try:
        return bool(sys.stdin.isatty())
    except Exception:
        return False


def _storage_prompt_text(
    label: str,
    *,
    default: str | None = None,
    required: bool = True,
) -> str:
    suffix = "" if default in (None, "") else " [{}]".format(default)
    while True:
        try:
            value = input("{}{}: ".format(label, suffix)).strip()
        except (EOFError, KeyboardInterrupt) as error:
            raise _StorageAddCancelled from error
        if value:
            return value
        if default not in (None, ""):
            return str(default)
        if not required:
            return ""
        print("A value is required.")


def _storage_prompt_yes_no(label: str, *, default: bool) -> bool:
    suffix = "Y/n" if default else "y/N"
    while True:
        try:
            answer = input("{} [{}]: ".format(label, suffix)).strip().casefold()
        except (EOFError, KeyboardInterrupt) as error:
            raise _StorageAddCancelled from error
        if not answer:
            return default
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Answer yes or no.")


def _storage_prompt_choice(
    label: str,
    choices: tuple[tuple[str, str], ...],
    *,
    default_value: str,
) -> str:
    print(label)
    default_index = 1
    aliases: dict[str, str] = {}
    for index, (choice_label, value) in enumerate(choices, start=1):
        if value == default_value:
            default_index = index
        marker = " (default)" if value == default_value else ""
        print("  {}) {} [{}]{}".format(index, choice_label, value, marker))
        aliases[choice_label.casefold()] = value
        aliases[value.casefold()] = value
    while True:
        selected = _storage_prompt_text(
            "Choice",
            default=str(default_index),
        )
        try:
            index = int(selected)
        except ValueError:
            matched = aliases.get(selected.casefold())
            if matched is not None:
                return matched
        else:
            if 1 <= index <= len(choices):
                return choices[index - 1][1]
        print("Choose a number from 1 to {} or a displayed id.".format(len(choices)))


def _descriptor_for_kind(
    kind: str,
    providers: list[Mapping[str, Any]],
) -> Mapping[str, Any]:
    normalized = str(kind).strip().lower().replace("-", "_")
    for descriptor in providers:
        aliases = descriptor.get("aliases", ())
        names = [str(descriptor.get("kind") or "")]
        if isinstance(aliases, list):
            names.extend(str(alias) for alias in aliases)
        if normalized in {
            name.strip().lower().replace("-", "_") for name in names
        }:
            return descriptor
    choices = ", ".join(
        sorted(str(descriptor.get("kind")) for descriptor in providers)
    )
    raise ValueError(
        "Unknown storage backend {!r}. Available backends: {}.".format(
            kind,
            choices or "none",
        )
    )


def _default_store_role(descriptor: Mapping[str, Any]) -> str:
    if descriptor.get("location_type") == "file":
        return "archive"
    if bool(descriptor.get("read_only_default", False)):
        return "source"
    return "live"


def _parse_backend_option(raw: str) -> tuple[str, object]:
    key, separator, raw_value = str(raw).partition("=")
    key = key.strip()
    if not separator or not key:
        raise ValueError(
            "Backend options must use NAME=VALUE, for example "
            "`region_name=eu-west-2`."
        )
    lowered = key.casefold()
    if lowered == "env" or any(
        marker in lowered for marker in _SENSITIVE_STORE_OPTION_MARKERS
    ):
        raise ValueError(
            "Store option {!r} looks secret-bearing and will not be "
            "persisted. Configure credentials through the backend's native "
            "profile/environment or an external secret provider."
            .format(key)
        )
    value_text = raw_value.strip()
    try:
        value: object = json.loads(value_text)
    except (TypeError, ValueError, json.JSONDecodeError):
        value = value_text
    if value is None or isinstance(value, (str, int, float, bool)):
        return key, value
    if isinstance(value, list) and all(
        isinstance(item, str) for item in value
    ):
        return key, value
    raise ValueError(
        "Backend option {!r} must be a string, number, boolean, null, or "
        "string list.".format(key)
    )


def _reject_sensitive_policy(value: object, *, path: str = "policy") -> None:
    if isinstance(value, Mapping):
        for raw_key, item in value.items():
            key = str(raw_key)
            lowered = key.casefold()
            if lowered == "env" or any(
                marker in lowered
                for marker in _SENSITIVE_STORE_OPTION_MARKERS
            ):
                raise ValueError(
                    "{} contains secret-bearing field {!r}; Store policy is "
                    "durable configuration, not a credential store."
                    .format(path, key)
                )
            _reject_sensitive_policy(item, path="{}.{}".format(path, key))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_sensitive_policy(
                item,
                path="{}[{}]".format(path, index),
            )


def _backend_policy(
    args: argparse.Namespace,
    descriptor: Mapping[str, Any],
) -> dict[str, object]:
    policy: dict[str, object] = {}
    policy_file = getattr(args, "policy_file", None)
    if policy_file:
        policy.update(load_json_object(policy_file))
        _reject_sensitive_policy(policy)
    assignments = [
        *list(getattr(args, "backend_options", ()) or ()),
        *list(getattr(args, "option", ()) or ()),
    ]
    if not assignments:
        return policy
    policy_section = descriptor.get("policy_section")
    if policy_section is None:
        raise ValueError(
            "Backend {!r} does not expose durable backend options."
            .format(descriptor.get("kind"))
        )
    existing = policy.get(str(policy_section), {})
    if not isinstance(existing, Mapping):
        raise ValueError(
            "Policy section {!r} must be a JSON object."
            .format(policy_section)
        )
    options = dict(existing)
    for assignment in assignments:
        key, value = _parse_backend_option(assignment)
        options[key] = value
    policy["backend"] = str(descriptor.get("kind"))
    policy[str(policy_section)] = options
    return policy


def _store_add_payload(
    args: argparse.Namespace,
    descriptor: Mapping[str, Any],
) -> dict[str, Any]:
    descriptor_kind = str(descriptor.get("kind") or "")
    capabilities = descriptor.get("capabilities", {})
    if not isinstance(capabilities, Mapping):
        raise ValueError(
            "Core returned invalid capabilities for backend {!r}.".format(
                descriptor_kind
            )
        )
    requested_read_only = getattr(args, "read_only", None)
    read_only_default = bool(descriptor.get("read_only_default", False))
    read_only = (
        read_only_default
        if requested_read_only is None
        else bool(requested_read_only)
    )
    if read_only_default and not read_only:
        raise ValueError(
            "Backend {!r} is intrinsically read-only."
            .format(descriptor_kind)
        )
    role = getattr(args, "role", None) or _default_store_role(descriptor)
    root = str(args.root).strip()
    name = str(args.name).strip()
    if not root:
        raise ValueError("Store root must not be empty.")
    if not name:
        raise ValueError("Store name must not be empty.")
    if bool(getattr(args, "default", False)) and (
        read_only or bool(getattr(args, "offline", False))
    ):
        raise ValueError(
            "The default Store must be online and writable."
        )
    store: dict[str, Any] = {
        "store_name": name,
        "store_kind": descriptor_kind,
        "store_root_uri": root,
        "store_access_protocol": (
            getattr(args, "protocol", None)
            or str(descriptor.get("access_protocol") or "file")
        ),
        "store_is_read_only": int(read_only),
        "store_online_status": (
            "offline" if bool(getattr(args, "offline", False)) else "online"
        ),
        "store_operational_role": role,
        "store_supports_folders": int(bool(capabilities.get("folders", False))),
        "store_supports_hierarchical_list": int(
            bool(capabilities.get("hierarchical_list", False))
        ),
        "store_supports_random_read": int(
            bool(capabilities.get("random_read", False))
        ),
        "store_supports_random_write": int(
            bool(capabilities.get("random_write", False)) and not read_only
        ),
        "store_supports_delete": int(
            bool(capabilities.get("delete", False)) and not read_only
        ),
        "store_supports_checksums": int(
            bool(capabilities.get("checksums", False))
        ),
        "store_supports_immutable_objects": int(
            bool(capabilities.get("immutable_objects", False))
        ),
    }
    for option, field in (
        (getattr(args, "uuid", None), "store_uuid"),
        (getattr(args, "failure_domain", None), "store_failure_domain"),
        (getattr(args, "region", None), "store_region"),
    ):
        if option not in (None, ""):
            store[field] = option
    tags = list(getattr(args, "tag", ()) or ())
    if tags:
        store["store_tags_json"] = json.dumps(sorted(set(tags)))
    policy = _backend_policy(args, descriptor)
    if policy:
        store["store_policy_json"] = json.dumps(
            policy,
            ensure_ascii=True,
            sort_keys=True,
        )
    return store


def _refresh_failure_count(value: object) -> int:
    if not isinstance(value, Mapping):
        return 0
    report = value.get("report", value)
    if not isinstance(report, Mapping):
        return 0
    raw = report.get("failed_configurations", report.get("failed_stores", 0))
    try:
        return int(str(raw or 0))
    except (TypeError, ValueError):
        return 0


def cmd_storage_store_add(args: argparse.Namespace) -> int:
    check = bool(getattr(args, "check", False))
    with open_cli_core(args, enable_storage_manager=True) as core:
        provider_result = core.query(
            "storage.backends.list",
            {"include_internal": False},
        )
        provider_values = (
            provider_result.get("backends", [])
            if isinstance(provider_result, Mapping)
            else []
        )
        providers = [
            value for value in provider_values if isinstance(value, Mapping)
        ] if isinstance(provider_values, list) else []
        descriptor = _descriptor_for_kind(str(args.kind), providers)
        store = _store_add_payload(args, descriptor)
        saved = core.command("storage.store.save", {"store": store})
        refreshed = core.command(
            "storage.refresh",
            {
                "startup_on_add": True,
                "include_offline": bool(args.include_offline),
                "clear_existing": True,
                "strict": bool(args.strict),
            },
        )
        probe: dict[str, Any] | None = None
        probe_ok = True
        if check and store["store_online_status"] == "online":
            try:
                probe_result = core.command(
                    "storage.store.probe",
                    {"store": store["store_name"]},
                )
            except Exception as error:
                probe_ok = False
                probe = {
                    "ok": False,
                    "error": {
                        "type": type(error).__name__,
                        "message": str(error) or type(error).__name__,
                    },
                }
            else:
                status = (
                    probe_result.get("status", {})
                    if isinstance(probe_result, Mapping)
                    else {}
                )
                probe_ok = bool(
                    isinstance(status, Mapping)
                    and status.get("available", False)
                )
                probe = {"ok": probe_ok, "result": probe_result}
        selected = None
        if bool(args.default) and (not check or probe_ok):
            selected = core.command(
                "storage.default.set",
                {"store": store["store_name"]},
            )
    ok = _refresh_failure_count(refreshed) == 0 and probe_ok
    emit_json(
        {
            "ok": ok,
            "backend": {
                "kind": descriptor.get("kind"),
                "label": descriptor.get("label"),
                "location_type": descriptor.get("location_type"),
                "read_only_default": bool(
                    descriptor.get("read_only_default", False)
                ),
            },
            "saved": saved,
            "refresh": refreshed,
            "probe": probe,
            "default": selected,
            "store": store,
            "next": [
                "liuxin",
                "storage",
                "status",
            ],
        },
        args,
    )
    return 0 if ok or not check else 1


def cmd_storage_backends_list(args: argparse.Namespace) -> int:
    return _storage_query(
        args,
        "storage.backends.list",
        {"include_internal": bool(args.include_internal)},
    )


def _default_store_name(root: str, kind: str) -> str:
    text = str(root).strip().rstrip("/\\")
    parsed = urlparse(text)
    candidate = ""
    if parsed.path:
        candidate = parsed.path.rstrip("/").rsplit("/", 1)[-1]
    if not candidate and parsed.netloc:
        candidate = parsed.netloc
    if not candidate and ":" in text:
        candidate = text.split(":", 1)[0]
    if not candidate:
        candidate = Path(text).name or kind
    rendered = safe_path_to_name(
        candidate,
        max_len=80,
        add_hash=False,
        lowercase=True,
    ).strip("_-.")
    return rendered or kind


def _wizard_backend(
    providers: list[Mapping[str, Any]],
    selected_kind: str | None,
) -> Mapping[str, Any]:
    if not providers:
        raise ValueError("Core did not advertise any selectable storage backends.")
    default_kind = selected_kind or "filesystem"
    if not any(str(item.get("kind")) == default_kind for item in providers):
        default_kind = str(providers[0].get("kind"))
    chosen = _storage_prompt_choice(
        "Choose a storage backend:",
        tuple(
            (
                str(item.get("label") or item.get("kind")),
                str(item.get("kind")),
            )
            for item in providers
        ),
        default_value=default_kind,
    )
    return next(item for item in providers if str(item.get("kind")) == chosen)


@dataclasses.dataclass(frozen=True)
class _StorageAddWizardPlan:
    descriptor: Mapping[str, Any]
    kind: str
    root: str
    name: str
    role: str
    read_only: bool
    online: bool
    failure_domain: str | None
    region: str | None
    tags: tuple[str, ...]
    option_values: tuple[str, ...]
    make_default: bool
    check: bool


def _wizard_access(
    args: argparse.Namespace,
    descriptor: Mapping[str, Any],
) -> tuple[bool, bool]:
    if bool(descriptor.get("read_only_default", False)):
        read_only = True
        print("This backend is intrinsically read-only.")
    else:
        configured_read_only = getattr(args, "read_only", None)
        read_only = _storage_prompt_yes_no(
            "Configure this Store as read-only?",
            default=(
                False
                if configured_read_only is None
                else bool(configured_read_only)
            ),
        )
    online = _storage_prompt_yes_no(
        "Mark this Store online?",
        default=not bool(getattr(args, "offline", False)),
    )
    return read_only, online


def _wizard_advanced_configuration(
    args: argparse.Namespace,
    descriptor: Mapping[str, Any],
) -> tuple[str | None, str | None, tuple[str, ...], tuple[str, ...]]:
    failure_domain = getattr(args, "failure_domain", None)
    region = getattr(args, "region", None)
    tags: list[str] = list(getattr(args, "tag", ()) or ())
    option_values: list[str] = list(getattr(args, "option", ()) or ())
    if not _storage_prompt_yes_no("Edit advanced configuration?", default=False):
        return failure_domain, region, tuple(tags), tuple(option_values)

    failure_domain = _storage_prompt_text(
        "Failure domain (blank for none)",
        default=failure_domain,
        required=False,
    ) or None
    region = _storage_prompt_text(
        "Region (blank for none)",
        default=region,
        required=False,
    ) or None
    raw_tags = _storage_prompt_text(
        "Comma-separated tags (blank for none)",
        default=",".join(tags) if tags else None,
        required=False,
    )
    tags = [value.strip() for value in raw_tags.split(",") if value.strip()]
    if descriptor.get("policy_section") is not None:
        print(
            "Enter non-secret backend options as NAME=VALUE. "
            "Use a blank line when finished."
        )
        while assignment := _storage_prompt_text(
            "Backend option",
            required=False,
        ):
            _parse_backend_option(assignment)
            option_values.append(assignment)
    return failure_domain, region, tuple(tags), tuple(option_values)


def _wizard_post_save_actions(
    args: argparse.Namespace,
    *,
    online: bool,
    read_only: bool,
    role: str,
) -> tuple[bool, bool]:
    make_default = False
    if online and not read_only and role == "live":
        make_default = _storage_prompt_yes_no(
            "Make this the default Store?",
            default=bool(getattr(args, "default", False)),
        )
    check = False
    if online:
        configured_check = getattr(args, "check", None)
        check = _storage_prompt_yes_no(
            "Probe the Store after saving it?",
            default=True if configured_check is None else bool(configured_check),
        )
    return make_default, check


def _storage_add_wizard_plan(
    args: argparse.Namespace,
    providers_payload: Mapping[str, Any],
) -> _StorageAddWizardPlan:
    raw_providers = providers_payload.get("backends", [])
    providers = (
        [
            item
            for item in raw_providers
            if isinstance(item, Mapping)
            and bool(item.get("user_selectable", True))
        ]
        if isinstance(raw_providers, list)
        else []
    )
    descriptor = _wizard_backend(providers, getattr(args, "kind", None))
    kind = str(descriptor["kind"])
    location_label = {
        "dir": "Folder path on the Core host",
        "file": "File/archive path on the Core host",
        "remote": "Remote root URI or backend address",
    }.get(str(descriptor.get("location_type")), "Store root")
    root = _storage_prompt_text(
        location_label,
        default=getattr(args, "root", None),
    )
    name = _storage_prompt_text(
        "Store name",
        default=getattr(args, "name", None) or _default_store_name(root, kind),
    )
    role = _storage_prompt_choice(
        "Choose the Store's operational role:",
        (
            ("Primary/live storage", "live"),
            ("Backup copy", "backup"),
            ("Archive or sealed artifact", "archive"),
            ("Read-only ingest source", "source"),
            ("Rebuildable cache", "cache"),
        ),
        default_value=getattr(args, "role", None)
        or _default_store_role(descriptor),
    )
    read_only, online = _wizard_access(args, descriptor)
    failure_domain, region, tags, option_values = (
        _wizard_advanced_configuration(args, descriptor)
    )
    make_default, check = _wizard_post_save_actions(
        args,
        online=online,
        read_only=read_only,
        role=role,
    )
    return _StorageAddWizardPlan(
        descriptor=descriptor,
        kind=kind,
        root=root,
        name=name,
        role=role,
        read_only=read_only,
        online=online,
        failure_domain=failure_domain,
        region=region,
        tags=tags,
        option_values=option_values,
        make_default=make_default,
        check=check,
    )


def _print_storage_add_wizard_plan(plan: _StorageAddWizardPlan) -> None:
    descriptor = plan.descriptor
    print("\nStore configuration plan")
    print("  name: {}".format(plan.name))
    print("  backend: {} ({})".format(descriptor.get("label"), plan.kind))
    print("  root: {} (interpreted on the Core host)".format(plan.root))
    print("  role: {}".format(plan.role))
    print("  access: {}".format("read-only" if plan.read_only else "read/write"))
    print("  declared state: {}".format("online" if plan.online else "offline"))
    print("  default Store: {}".format("yes" if plan.make_default else "no"))
    print("  probe after save: {}".format("yes" if plan.check else "no"))
    limitations = descriptor.get("limitations", [])
    if limitations and isinstance(limitations, list):
        print("  advertised limitations:")
        for limitation in limitations:
            if isinstance(limitation, Mapping):
                print(
                    "    - {}: {}".format(
                        limitation.get("code"),
                        limitation.get("message"),
                    )
                )
    print(
        "  credentials: not persisted; use backend-native profiles, "
        "environment injection, or a secret provider"
    )


def _apply_storage_add_wizard_plan(
    args: argparse.Namespace,
    plan: _StorageAddWizardPlan,
) -> None:
    args.name = plan.name
    args.kind = plan.kind
    args.root = plan.root
    args.role = plan.role
    args.read_only = plan.read_only
    args.offline = not plan.online
    args.failure_domain = plan.failure_domain
    args.region = plan.region
    args.tag = list(plan.tags)
    args.option = list(plan.option_values)
    args.default = plan.make_default
    args.check = plan.check


def _run_storage_add_wizard(
    args: argparse.Namespace,
    providers_payload: Mapping[str, Any],
) -> int:
    plan = _storage_add_wizard_plan(args, providers_payload)
    _print_storage_add_wizard_plan(plan)
    if not _storage_prompt_yes_no("Save this Store?", default=False):
        raise _StorageAddCancelled
    _apply_storage_add_wizard_plan(args, plan)
    return cmd_storage_store_add(args)


def cmd_storage_add(args: argparse.Namespace) -> int:
    complete = all(
        getattr(args, name, None) not in (None, "")
        for name in ("name", "kind", "root")
    )
    wants_wizard = bool(args.interactive) or not complete
    if not wants_wizard:
        if args.check is None:
            args.check = True
        return cmd_storage_store_add(args)
    if not _storage_stdin_is_interactive():
        raise ValueError(
            "The storage-add wizard requires an interactive terminal. For "
            "automation use `liuxin storage add NAME KIND ROOT "
            "[OPTION=VALUE ...]`."
        )

    with open_cli_core(args, enable_storage_manager=True) as core:
        providers = core.query(
            "storage.backends.list",
            {"include_internal": False},
        )
    if not isinstance(providers, Mapping):
        raise ValueError("Core returned an invalid storage backend catalogue.")
    print("LiuXin storage configuration")
    print("No Store row is written until the final confirmation.\n")
    try:
        return _run_storage_add_wizard(args, providers)
    except _StorageAddCancelled:
        print("Storage configuration cancelled; no Store row was written.")
        return 1


def cmd_storage_store_update(args: argparse.Namespace) -> int:
    changes: dict[str, Any] = {}
    for argument, field in (
        (args.name, "name"),
        (args.root, "root"),
        (args.url, "url"),
        (args.protocol, "protocol"),
        (args.role, "operational_role"),
        (args.replication_policy_id, "replication_policy_id"),
        (args.backup_policy_id, "backup_policy_id"),
    ):
        if argument is not None:
            changes[field] = argument
    if args.failure_domain is not None or args.clear_failure_domain:
        changes["failure_domain"] = (
            None if args.clear_failure_domain else args.failure_domain
        )
    if args.region is not None or args.clear_region:
        changes["region"] = None if args.clear_region else args.region
    if args.read_only is not None:
        changes["read_only"] = bool(args.read_only)
    if args.add_tag:
        changes["add_tags"] = list(args.add_tag)
    if args.remove_tag:
        changes["remove_tags"] = list(args.remove_tag)
    return _storage_command(
        args,
        "storage.store.update",
        {"store": _store_reference(args.store), "changes": changes},
    )


def cmd_storage_store_probe(args: argparse.Namespace) -> int:
    return _storage_command(
        args, "storage.store.probe", {"store": _store_reference(args.store)}
    )


def cmd_storage_store_delete(args: argparse.Namespace) -> int:
    if not args.yes:
        raise ValueError("Store removal requires --yes.")
    return _storage_command(
        args,
        "storage.store.delete",
        {
            "store": _store_reference(args.store),
            "delete_from_database": bool(args.delete_from_database),
        },
    )


def cmd_storage_store_evacuate(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {
        "store": _store_reference(args.store),
        "max_assets": int(args.max_assets),
    }
    if args.destination_store:
        payload["destination_store"] = _store_reference(
            args.destination_store
        )
    if not args.yes:
        return _storage_query(
            args, "storage.store.evacuate.plan", payload
        )
    payload.update(
        {
            "max_actions": int(args.max_actions),
            "max_transfer_bytes": int(
                float(args.max_transfer_gib) * 1024 * 1024 * 1024
            ),
            "keep_source_bytes": bool(args.keep_source_bytes),
        }
    )
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command("storage.store.evacuate.apply", payload)
    emit_json(result, args)
    return 0 if bool(result.get("ok", False)) else 1


def cmd_storage_default_show(args: argparse.Namespace) -> int:
    return _storage_query(args, "storage.default.get", {})


def cmd_storage_default_set(args: argparse.Namespace) -> int:
    return _storage_command(
        args, "storage.default.set", {"store": _store_reference(args.store)}
    )


def cmd_storage_refresh(args: argparse.Namespace) -> int:
    return _storage_command(
        args,
        "storage.refresh",
        {
            "startup_on_add": bool(args.startup_on_add),
            "include_offline": bool(args.include_offline),
            "clear_existing": not bool(args.keep_existing),
            "strict": bool(args.strict),
        },
    )


def cmd_storage_files_list(args: argparse.Namespace) -> int:
    return _storage_query(
        args,
        "storage.files.list",
        {"limit": int(args.limit), "offset": int(args.offset)},
    )


def cmd_storage_file_locate(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"asset_id": int(args.asset_id)}
    if args.store:
        payload["store_uuid"] = args.store
    return _storage_query(args, "storage.file.locate", payload)


def cmd_storage_file_get(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"asset_id": int(args.asset_id)}
    if args.store:
        payload["store_uuid"] = args.store
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.query("storage.file.read", payload)
    content = decode_wire_bytes(result.get("content"), label="storage file content")
    emit_bytes(
        content,
        output=args.file_output,
        replace=bool(args.replace_file_output),
    )
    if args.file_output != "-":
        print(
            json.dumps(
                {"asset_id": args.asset_id, "size": len(content), "location": result.get("location")},
                ensure_ascii=True,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
    return 0


def cmd_storage_file_put(args: argparse.Namespace) -> int:
    content = _bounded_file_bytes(args.input, args.max_transfer_mib)
    source = Path(args.input).expanduser()
    payload: dict[str, Any] = {
        "content_base64": base64.b64encode(content).decode("ascii"),
        "original_name": args.original_name or source.name,
    }
    if args.store:
        payload["store_uuid"] = args.store
    if args.name:
        payload["name"] = args.name
    if args.media_type:
        payload["media_type"] = args.media_type
    if args.metadata_file:
        payload["metadata"] = load_json_object(args.metadata_file)
    return _storage_command(args, "storage.file.put", payload)


def cmd_storage_file_copy(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"asset_id": int(args.asset_id)}
    if args.store:
        payload["store"] = args.store
    if args.metadata_file:
        payload["metadata"] = load_json_object(args.metadata_file)
    return _storage_command(args, "storage.file.copy", payload)


def cmd_storage_file_delete(args: argparse.Namespace) -> int:
    if not args.yes:
        raise ValueError("Replica deletion requires --yes.")
    return _storage_command(
        args, "storage.file.delete", {"replica_id": int(args.replica_id)}
    )


def cmd_storage_location_stat(args: argparse.Namespace) -> int:
    return _storage_query(
        args,
        "storage.location.stat",
        {"store_uuid": args.store_uuid, "key": args.key},
    )


def cmd_storage_sources_list(args: argparse.Namespace) -> int:
    return _storage_query(args, "storage.sources.supported", {})


def cmd_storage_source_register(args: argparse.Namespace) -> int:
    return _storage_command(
        args,
        "storage.source.register",
        {"kind": args.kind, "options": load_json_object(args.options_file)},
    )


def cmd_storage_source_add(args: argparse.Namespace) -> int:
    kind = str(args.kind).lower().replace("-", "_")
    endpoint_fields = {
        "unmanaged_disk": "disk_path",
        "rclone_http": "remote_url",
        "wget_html": "remote_url",
        "native_html": "remote_url",
        "squashfs_open": "archive_path",
    }
    field = endpoint_fields.get(kind)
    if field is None:
        raise ValueError(
            "Typed source setup supports {}. Use `storage sources register` "
            "for another advertised kind.".format(", ".join(sorted(endpoint_fields)))
        )
    options: dict[str, Any] = {field: args.location}
    if args.name:
        options["store_name"] = args.name
    if args.extension:
        options["ebook_extensions"] = list(args.extension)
    if args.source_label:
        options["source_label"] = args.source_label
    if kind == "unmanaged_disk":
        options.update(
            {
                "compute_hash": not bool(args.no_hash),
                "follow_symlinks": bool(args.follow_symlinks),
                "attach_store_links": not bool(args.no_store_links),
                "refresh_storage_manager": not bool(args.no_refresh),
            }
        )
    elif kind in {"rclone_http", "wget_html", "native_html"}:
        if args.timeout is not None:
            options["timeout_s"] = float(args.timeout)
        if args.requests_per_hour is not None:
            options["max_http_requests_per_hour"] = float(args.requests_per_hour)
        options["attach_store_links"] = not bool(args.no_store_links)
        options["refresh_storage_manager"] = not bool(args.no_refresh)
    if args.options_file:
        options.update(load_json_object(args.options_file))
    return _storage_command(
        args,
        "storage.source.register",
        {"kind": kind, "options": options},
    )


def cmd_storage_asset_show(args: argparse.Namespace) -> int:
    return _storage_query(
        args, "storage.asset.get", {"asset_id": int(args.asset_id)}
    )


def cmd_storage_replica_verify(args: argparse.Namespace) -> int:
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command(
            "storage.replica.verify",
            {
                "replica_id": int(args.replica_id),
                "calculate_digests": not bool(args.no_digests),
            },
        )
    emit_json(result, args)
    return 0 if bool(result.get("healthy", False)) else 1


def cmd_storage_asset_verify(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"asset_id": int(args.asset_id)}
    if args.replica_id:
        payload["replica_ids"] = [int(value) for value in args.replica_id]
    if args.all_replicas:
        payload["all_replicas"] = True
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command("storage.asset.verify", payload)
    emit_json(result, args)
    return 0 if bool(result.get("healthy", False)) else 1


def cmd_storage_status(args: argparse.Namespace) -> int:
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.query(
            "storage.status", {"refresh_stores": bool(args.refresh)}
        )
    emit_json(result, args)
    return 0 if bool(result.get("healthy", False)) else 1


def cmd_storage_audit(args: argparse.Namespace) -> int:
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command(
            "storage.audit",
            {
                "limit": int(args.limit),
                "offset": int(args.offset),
                "calculate_digests": not bool(args.no_digests),
            },
        )
    emit_json(result, args)
    return 0 if bool(result.get("ok", False)) else 1


def cmd_storage_reconcile(args: argparse.Namespace) -> int:
    payload = {"refresh_stores": bool(args.refresh)}
    if args.reconcile_action == "plan":
        return _storage_query(args, "storage.reconcile.plan", payload)
    if not args.yes:
        raise ValueError("Storage reconciliation requires --yes; run `reconcile plan` first.")
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command(
            "storage.reconcile.apply",
            {
                "max_actions": int(args.max_actions),
                "include_offline": bool(args.include_offline),
            },
        )
    emit_json(result, args)
    return 0 if bool(result.get("ok", False)) else 1


def cmd_storage_repair(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"max_assets": int(args.max_assets)}
    if args.asset_id is not None:
        payload["asset_id"] = int(args.asset_id)
    if args.repair_action == "plan":
        return _storage_query(args, "storage.repair.plan", payload)
    if not args.yes:
        raise ValueError(
            "Storage repair requires --yes; run `storage repair plan` first."
        )
    payload.update(
        {
            "max_actions": int(args.max_actions),
            "max_transfer_bytes": int(
                float(args.max_transfer_gib) * 1024 * 1024 * 1024
            ),
        }
    )
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command("storage.repair.apply", payload)
    emit_json(result, args)
    return 0 if bool(result.get("ok", False)) else 1


def cmd_storage_recovery_list(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {
        "limit": int(args.limit),
        "offset": int(args.offset),
    }
    if args.state:
        payload["state"] = args.state
    return _storage_query(args, "storage.recovery.list", payload)


def cmd_storage_recovery_action(args: argparse.Namespace) -> int:
    if not args.yes:
        raise ValueError(
            "Storage ingest recovery requires --yes after reviewing "
            "`storage recovery list`."
        )
    payload: dict[str, Any] = {}
    if getattr(args, "operation_id", None):
        payload["operation_id"] = args.operation_id
    operation = (
        "storage.recovery.retry-ingest"
        if args.recovery_action == "retry-ingest"
        else "storage.recovery.recover-pending"
    )
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command(operation, payload)
    emit_json(result, args)
    return 0 if bool(result.get("ok", False)) else 1


def cmd_storage_policy(args: argparse.Namespace) -> int:
    payload = {"asset_id": int(args.asset_id)}
    return _storage_query(args, "storage.policy." + args.policy_action, payload)


def cmd_storage_policy_violations(args: argparse.Namespace) -> int:
    return _storage_query(
        args,
        "storage.policy.violations",
        {"limit": int(args.limit), "offset": int(args.offset)},
    )


def cmd_storage_policy_set(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"asset_id": int(args.asset_id)}
    if args.replication_policy_id is not None:
        payload["replication_policy_id"] = int(args.replication_policy_id)
    if args.backup_policy_id is not None:
        payload["backup_policy_id"] = int(args.backup_policy_id)
    return _storage_command(args, "storage.asset.policies.set", payload)


def cmd_storage_resources_describe(args: argparse.Namespace) -> int:
    return _storage_query(args, "storage.resources.describe", {})


def cmd_storage_resource_list(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {
        "resource": args.resource,
        "limit": int(args.limit),
        "offset": int(args.offset),
    }
    if args.where_file:
        payload["where"] = load_json_object(args.where_file)
    return _storage_query(args, "storage.resource.list", payload)


def cmd_storage_resource_get(args: argparse.Namespace) -> int:
    return _storage_query(
        args,
        "storage.resource.get",
        {"resource": args.resource, "id": int(args.resource_id)},
    )


def cmd_storage_resource_write(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {
        "resource": args.resource,
        "values": load_json_object(args.values_file),
    }
    operation = "storage.resource.create"
    if args.resource_action == "update":
        payload["id"] = int(args.resource_id)
        operation = "storage.resource.update"
    return _storage_command(args, operation, payload)


def cmd_storage_resource_delete(args: argparse.Namespace) -> int:
    if not args.yes:
        raise ValueError("Storage resource deletion requires --yes.")
    return _storage_command(
        args,
        "storage.resource.delete",
        {"resource": args.resource, "id": int(args.resource_id)},
    )


def _core_json(parser: argparse.ArgumentParser) -> None:
    add_connection_arguments(parser)
    add_json_output(parser)


def _build_storage_admin_parsers(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    backends = commands.add_parser(
        "backends",
        aliases=["providers"],
        help="List Store backend providers, capabilities, and limitations.",
    )
    _core_json(backends)
    backends.add_argument(
        "--include-internal",
        action="store_true",
        help="Include implementation-only backends that cannot be added directly.",
    )
    backends.set_defaults(handler=cmd_storage_backends_list)

    add_store = commands.add_parser(
        "add",
        help=(
            "Interactively configure a Store, or add one as "
            "NAME KIND ROOT [OPTION=VALUE ...]."
        ),
        description=(
            "With no complete NAME/KIND/ROOT triple, guide the operator through "
            "Core-advertised backend providers and show a confirmation plan. "
            "A complete triple is automation-safe and non-interactive. Roots "
            "are interpreted on the Core host. Credentials are never accepted "
            "as durable Store options."
        ),
    )
    _core_json(add_store)
    add_store.add_argument("name", nargs="?", help="Unique Store name.")
    add_store.add_argument("kind", nargs="?", help="Backend kind or alias.")
    add_store.add_argument(
        "root",
        nargs="?",
        help="Folder, archive, bucket URI, or remote address on the Core host.",
    )
    add_store.add_argument(
        "backend_options",
        nargs="*",
        metavar="OPTION=VALUE",
        help="Non-secret backend option; JSON scalar values are accepted.",
    )
    add_store.add_argument(
        "--interactive",
        action="store_true",
        help="Run the guided flow even when NAME, KIND, and ROOT are supplied.",
    )
    add_store.add_argument("--uuid")
    add_store.add_argument("--protocol")
    add_store.add_argument("--role")
    add_mutability = add_store.add_mutually_exclusive_group()
    add_mutability.add_argument(
        "--read-only",
        dest="read_only",
        action="store_true",
    )
    add_mutability.add_argument(
        "--writable",
        dest="read_only",
        action="store_false",
    )
    add_store.set_defaults(read_only=None)
    add_store.add_argument("--offline", action="store_true")
    add_store.add_argument("--failure-domain")
    add_store.add_argument("--region")
    add_store.add_argument("--tag", action="append")
    add_store.add_argument(
        "--option",
        action="append",
        help="Additional non-secret backend NAME=VALUE option.",
    )
    add_store.add_argument(
        "--policy-file",
        help="Advanced non-secret Store policy JSON on the CLI host.",
    )
    add_store.add_argument("--default", action="store_true")
    add_store.add_argument("--include-offline", action="store_true")
    add_store.add_argument("--strict", action="store_true")
    add_check = add_store.add_mutually_exclusive_group()
    add_check.add_argument(
        "--check",
        dest="check",
        action="store_true",
        help="Probe the Store after it is saved and reloaded (default).",
    )
    add_check.add_argument(
        "--no-check",
        dest="check",
        action="store_false",
        help="Save and reload without a separate probe.",
    )
    add_store.set_defaults(check=None, handler=cmd_storage_add)

    stores = commands.add_parser("stores", help="List configured stores.")
    _core_json(stores)
    stores.add_argument("--refresh", action="store_true")
    stores.set_defaults(handler=cmd_storage_stores_list)

    store = commands.add_parser("store", help="Inspect or administer one Store.")
    store_commands = store.add_subparsers(dest="store_action", required=True)
    show = store_commands.add_parser("show", aliases=["get"])
    _core_json(show)
    show.add_argument("store", help="Store UUID, database id, or unique name.")
    show.set_defaults(handler=cmd_storage_store_show)
    save = store_commands.add_parser("save", help="Save a Store configuration JSON object.")
    _core_json(save)
    save.add_argument("store_file", help="CLI-host JSON file containing the Store object.")
    save.set_defaults(handler=cmd_storage_store_save)

    add = store_commands.add_parser(
        "add",
        help="Add a Store using ordinary typed options; JSON save remains available.",
    )
    _core_json(add)
    add.add_argument("kind", help="Backend kind, for example filesystem or s3.")
    add.add_argument("root", help="Root path or backend URI as seen by Core.")
    add.add_argument("--name", required=True, help="Unique operator-visible Store name.")
    add.add_argument("--uuid")
    add.add_argument("--protocol")
    add.add_argument("--role")
    add_mutability_compat = add.add_mutually_exclusive_group()
    add_mutability_compat.add_argument(
        "--read-only",
        dest="read_only",
        action="store_true",
    )
    add_mutability_compat.add_argument(
        "--writable",
        dest="read_only",
        action="store_false",
    )
    add.set_defaults(read_only=None)
    add.add_argument("--offline", action="store_true")
    add.add_argument("--failure-domain")
    add.add_argument("--region")
    add.add_argument("--tag", action="append")
    add.add_argument("--option", action="append")
    add.add_argument("--policy-file", help="Optional backend policy JSON object.")
    add.add_argument("--default", action="store_true", help="Select it as the default Store.")
    add.add_argument("--include-offline", action="store_true")
    add.add_argument("--strict", action="store_true")
    add.add_argument(
        "--check",
        action="store_true",
        help="Probe the Store after saving and refreshing it.",
    )
    add.set_defaults(handler=cmd_storage_store_add)
    update = store_commands.add_parser(
        "update",
        help="Update common Store settings without constructing JSON.",
    )
    _core_json(update)
    update.add_argument("store")
    update.add_argument("--name")
    update.add_argument("--root")
    update.add_argument("--url")
    update.add_argument("--protocol")
    update.add_argument("--role")
    failure_domain = update.add_mutually_exclusive_group()
    failure_domain.add_argument("--failure-domain")
    failure_domain.add_argument("--clear-failure-domain", action="store_true")
    region = update.add_mutually_exclusive_group()
    region.add_argument("--region")
    region.add_argument("--clear-region", action="store_true")
    mutability = update.add_mutually_exclusive_group()
    mutability.add_argument(
        "--read-only", dest="read_only", action="store_true"
    )
    mutability.add_argument(
        "--writable", dest="read_only", action="store_false"
    )
    update.set_defaults(read_only=None)
    update.add_argument("--add-tag", action="append")
    update.add_argument("--remove-tag", action="append")
    update.add_argument("--replication-policy-id", type=int)
    update.add_argument("--backup-policy-id", type=int)
    update.set_defaults(handler=cmd_storage_store_update)
    probe = store_commands.add_parser("probe", help="Probe a Store's live status.")
    _core_json(probe)
    probe.add_argument("store")
    probe.set_defaults(handler=cmd_storage_store_probe)
    delete = store_commands.add_parser("delete", help="Unregister a Store.")
    _core_json(delete)
    delete.add_argument("store")
    delete.add_argument("--delete-from-database", action="store_true")
    delete.add_argument("--yes", action="store_true", help="Confirm Store removal.")
    delete.set_defaults(handler=cmd_storage_store_delete)
    evacuate = store_commands.add_parser(
        "evacuate",
        help="Plan, or confirm, verified movement of Replicas off a Store.",
    )
    _core_json(evacuate)
    evacuate.add_argument("store")
    evacuate.add_argument(
        "--destination-store",
        help="Restrict new replacement copies to one Store.",
    )
    evacuate.add_argument("--max-assets", type=int, default=100)
    evacuate.add_argument("--max-actions", type=int, default=1000)
    evacuate.add_argument("--max-transfer-gib", type=float, default=1024.0)
    evacuate.add_argument(
        "--keep-source-bytes",
        action="store_true",
        help="Retire Replica claims but leave writable source bytes in place.",
    )
    evacuate.add_argument(
        "--yes",
        action="store_true",
        help="Apply the freshly recomputed plan; omission is a preview.",
    )
    evacuate.set_defaults(handler=cmd_storage_store_evacuate)

    default = commands.add_parser("default", help="Inspect or set the default Store.")
    default_commands = default.add_subparsers(dest="default_action", required=True)
    default_show = default_commands.add_parser("show", aliases=["get"])
    _core_json(default_show)
    default_show.set_defaults(handler=cmd_storage_default_show)
    default_set = default_commands.add_parser("set")
    _core_json(default_set)
    default_set.add_argument("store")
    default_set.set_defaults(handler=cmd_storage_default_set)

    refresh = commands.add_parser("refresh", help="Reload Store configurations from the database.")
    _core_json(refresh)
    refresh.add_argument("--startup-on-add", action="store_true")
    refresh.add_argument("--include-offline", action="store_true")
    refresh.add_argument("--keep-existing", action="store_true")
    refresh.add_argument("--strict", action="store_true")
    refresh.set_defaults(handler=cmd_storage_refresh)

    files = commands.add_parser("files", help="List, transfer, locate, copy, or delete stored files.")
    file_commands = files.add_subparsers(dest="files_action", required=True)
    files_list = file_commands.add_parser("list")
    _core_json(files_list)
    files_list.add_argument("--limit", type=int, default=100)
    files_list.add_argument("--offset", type=int, default=0)
    files_list.set_defaults(handler=cmd_storage_files_list)
    locate = file_commands.add_parser("locate")
    _core_json(locate)
    locate.add_argument("asset_id", type=int)
    locate.add_argument("--store", help="Preferred Store UUID or unique name.")
    locate.set_defaults(handler=cmd_storage_file_locate)
    get = file_commands.add_parser("get", aliases=["read"], help="Read an asset to a CLI-host file.")
    add_connection_arguments(get)
    get.add_argument("asset_id", type=int)
    get.add_argument("file_output", help="CLI-host output path, or - for stdout.")
    get.add_argument("--store", help="Preferred Store UUID or unique name.")
    get.add_argument("--replace-file-output", action="store_true")
    get.set_defaults(handler=cmd_storage_file_get)
    put = file_commands.add_parser("put", help="Transfer a CLI-host file into managed storage.")
    _core_json(put)
    put.add_argument("input", help="Path on the CLI host.")
    put.add_argument("--store", help="Target Store UUID or unique name.")
    put.add_argument("--metadata-file", help="Optional rich storage-hint JSON object.")
    put.add_argument("--name")
    put.add_argument("--original-name")
    put.add_argument("--media-type")
    put.add_argument("--max-transfer-mib", type=float, default=512.0)
    put.set_defaults(handler=cmd_storage_file_put)
    copy = file_commands.add_parser("copy", help="Create another managed replica of an asset.")
    _core_json(copy)
    copy.add_argument("asset_id", type=int)
    copy.add_argument("--store", help="Target Store UUID or unique name.")
    copy.add_argument("--metadata-file", help="Optional rich storage-hint JSON object.")
    copy.set_defaults(handler=cmd_storage_file_copy)
    file_delete = file_commands.add_parser("delete", help="Commit deletion of one replica.")
    _core_json(file_delete)
    file_delete.add_argument("replica_id", type=int)
    file_delete.add_argument("--yes", action="store_true")
    file_delete.set_defaults(handler=cmd_storage_file_delete)

    location = commands.add_parser("location", help="Inspect an exact Store key.")
    location_commands = location.add_subparsers(dest="location_action", required=True)
    location_stat = location_commands.add_parser("stat")
    _core_json(location_stat)
    location_stat.add_argument("store_uuid")
    location_stat.add_argument("key")
    location_stat.set_defaults(handler=cmd_storage_location_stat)

    sources = commands.add_parser("sources", help="Inspect or register ingest sources.")
    source_commands = sources.add_subparsers(dest="sources_action", required=True)
    source_list = source_commands.add_parser("list", aliases=["supported"])
    _core_json(source_list)
    source_list.set_defaults(handler=cmd_storage_sources_list)
    register = source_commands.add_parser("register")
    _core_json(register)
    register.add_argument("kind")
    register.add_argument("options_file", help="CLI-host source-options JSON object.")
    register.set_defaults(handler=cmd_storage_source_register)

    source_add = source_commands.add_parser(
        "add", help="Register a common source without constructing a JSON object."
    )
    _core_json(source_add)
    source_add.add_argument(
        "kind",
        choices=("unmanaged-disk", "rclone-http", "wget-html", "native-html", "squashfs-open"),
    )
    source_add.add_argument("location", help="Disk/archive path or remote URL as seen by Core.")
    source_add.add_argument("--name")
    source_add.add_argument("--source-label")
    source_add.add_argument("--extension", action="append")
    source_add.add_argument("--no-hash", action="store_true")
    source_add.add_argument("--follow-symlinks", action="store_true")
    source_add.add_argument("--no-store-links", action="store_true")
    source_add.add_argument("--no-refresh", action="store_true")
    source_add.add_argument("--timeout", type=float)
    source_add.add_argument("--requests-per-hour", type=float)
    source_add.add_argument(
        "--options-file",
        help="Optional overrides for uncommon source-specific options.",
    )
    source_add.set_defaults(handler=cmd_storage_source_add)

    asset = commands.add_parser("asset", help="Inspect a digital asset graph.")
    asset_commands = asset.add_subparsers(dest="asset_action", required=True)
    asset_show = asset_commands.add_parser("show", aliases=["get"])
    _core_json(asset_show)
    asset_show.add_argument("asset_id", type=int)
    asset_show.set_defaults(handler=cmd_storage_asset_show)

    asset_verify = asset_commands.add_parser("verify")
    _core_json(asset_verify)
    asset_verify.add_argument("asset_id", type=int)
    asset_verify.add_argument("--replica-id", action="append", type=int)
    asset_verify.add_argument("--all-replicas", action="store_true")
    asset_verify.set_defaults(handler=cmd_storage_asset_verify)

    replica = commands.add_parser("replica", help="Verify one concrete Replica.")
    replica_commands = replica.add_subparsers(dest="replica_action", required=True)
    replica_verify = replica_commands.add_parser("verify")
    _core_json(replica_verify)
    replica_verify.add_argument("replica_id", type=int)
    replica_verify.add_argument("--no-digests", action="store_true")
    replica_verify.set_defaults(handler=cmd_storage_replica_verify)

    status = commands.add_parser(
        "status",
        help=(
            "Overview configured folder Stores, capacity, Replicas, and "
            "actionable health."
        ),
    )
    _core_json(status)
    status.add_argument(
        "--refresh",
        action="store_true",
        help="Probe Store backends instead of using their cached status.",
    )
    status.set_defaults(handler=cmd_storage_status)

    audit = commands.add_parser("audit", help="Verify a bounded page of Replicas.")
    _core_json(audit)
    audit.add_argument("--limit", type=int, default=100)
    audit.add_argument("--offset", type=int, default=0)
    audit.add_argument("--no-digests", action="store_true")
    audit.set_defaults(handler=cmd_storage_audit)

    reconcile = commands.add_parser(
        "reconcile", help="Plan or apply bounded non-destructive storage repair."
    )
    reconcile_commands = reconcile.add_subparsers(dest="reconcile_action", required=True)
    reconcile_plan = reconcile_commands.add_parser("plan")
    _core_json(reconcile_plan)
    reconcile_plan.add_argument("--refresh", action="store_true")
    reconcile_plan.set_defaults(handler=cmd_storage_reconcile)
    reconcile_apply = reconcile_commands.add_parser("apply")
    _core_json(reconcile_apply)
    reconcile_apply.add_argument("--yes", action="store_true")
    reconcile_apply.add_argument("--max-actions", type=int, default=100)
    reconcile_apply.add_argument("--include-offline", action="store_true")
    reconcile_apply.set_defaults(refresh=False, handler=cmd_storage_reconcile)

    repair = commands.add_parser(
        "repair",
        help="Plan or apply bounded, non-deleting Replica and policy repair.",
    )
    repair_commands = repair.add_subparsers(dest="repair_action", required=True)
    repair_plan = repair_commands.add_parser("plan")
    _core_json(repair_plan)
    repair_plan.add_argument("--asset-id", type=int)
    repair_plan.add_argument("--max-assets", type=int, default=100)
    repair_plan.set_defaults(handler=cmd_storage_repair)
    repair_apply = repair_commands.add_parser("apply")
    _core_json(repair_apply)
    repair_apply.add_argument("--asset-id", type=int)
    repair_apply.add_argument("--max-assets", type=int, default=100)
    repair_apply.add_argument("--max-actions", type=int, default=100)
    repair_apply.add_argument("--max-transfer-gib", type=float, default=100.0)
    repair_apply.add_argument("--yes", action="store_true")
    repair_apply.set_defaults(handler=cmd_storage_repair)

    recovery = commands.add_parser(
        "recovery", help="Inspect and act on durable ingest-journal recovery."
    )
    recovery_commands = recovery.add_subparsers(
        dest="recovery_action", required=True
    )
    recovery_list = recovery_commands.add_parser("list")
    _core_json(recovery_list)
    recovery_list.add_argument("--state")
    recovery_list.add_argument("--limit", type=int, default=100)
    recovery_list.add_argument("--offset", type=int, default=0)
    recovery_list.set_defaults(handler=cmd_storage_recovery_list)
    recover_pending = recovery_commands.add_parser("recover-pending")
    _core_json(recover_pending)
    recover_pending.add_argument("operation_id", nargs="?")
    recover_pending.add_argument("--yes", action="store_true")
    recover_pending.set_defaults(handler=cmd_storage_recovery_action)
    retry_ingest = recovery_commands.add_parser("retry-ingest")
    _core_json(retry_ingest)
    retry_ingest.add_argument("operation_id")
    retry_ingest.add_argument("--yes", action="store_true")
    retry_ingest.set_defaults(handler=cmd_storage_recovery_action)

    policies = commands.add_parser("policies", help="Assess and configure asset placement policies.")
    policy_commands = policies.add_subparsers(dest="policy_action", required=True)
    for action in ("assess", "plan"):
        command = policy_commands.add_parser(action)
        _core_json(command)
        command.add_argument("asset_id", type=int)
        command.set_defaults(handler=cmd_storage_policy)
    violations = policy_commands.add_parser("violations")
    _core_json(violations)
    violations.add_argument("--limit", type=int, default=100)
    violations.add_argument("--offset", type=int, default=0)
    violations.set_defaults(handler=cmd_storage_policy_violations)
    policy_set = policy_commands.add_parser("set")
    _core_json(policy_set)
    policy_set.add_argument("asset_id", type=int)
    policy_set.add_argument("--replication-policy-id", type=int)
    policy_set.add_argument("--backup-policy-id", type=int)
    policy_set.set_defaults(handler=cmd_storage_policy_set)

    resources = commands.add_parser("resources", help="Inspect or edit stable storage graph resources.")
    resource_commands = resources.add_subparsers(dest="resource_action", required=True)
    describe = resource_commands.add_parser("describe")
    _core_json(describe)
    describe.set_defaults(handler=cmd_storage_resources_describe)
    resource_list = resource_commands.add_parser("list")
    _core_json(resource_list)
    resource_list.add_argument("resource")
    resource_list.add_argument("--where-file")
    resource_list.add_argument("--limit", type=int, default=100)
    resource_list.add_argument("--offset", type=int, default=0)
    resource_list.set_defaults(handler=cmd_storage_resource_list)
    resource_get = resource_commands.add_parser("get")
    _core_json(resource_get)
    resource_get.add_argument("resource")
    resource_get.add_argument("resource_id", type=int)
    resource_get.set_defaults(handler=cmd_storage_resource_get)
    for action in ("create", "update"):
        command = resource_commands.add_parser(action)
        _core_json(command)
        command.add_argument("resource")
        if action == "update":
            command.add_argument("resource_id", type=int)
        command.add_argument("values_file")
        command.set_defaults(handler=cmd_storage_resource_write)
    resource_delete = resource_commands.add_parser("delete")
    _core_json(resource_delete)
    resource_delete.add_argument("resource")
    resource_delete.add_argument("resource_id", type=int)
    resource_delete.add_argument("--yes", action="store_true")
    resource_delete.set_defaults(handler=cmd_storage_resource_delete)


def build_storage_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Register the top-level ``storage`` command family."""

    parser = subparsers.add_parser(
        "storage",
        help="Ingest files and administer Stores, Replicas, sources, and policies.",
    )
    storage_subparsers = parser.add_subparsers(
        dest="storage_command",
        required=True,
    )
    ingest = storage_subparsers.add_parser(
        "ingest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help="Catalogue a mixed local tree with bounded recursive containers.",
        description=(
            "Catalogue loose files and nested SquashFS, ISO/UDF, ZIP, TAR, RAR, "
            "and 7z containers without extracting into the source tree.\n\n"
            "Exit codes: 0 complete/ready; 1 completed with issues or failed; "
            "2 command configuration; 130 SIGINT; 143 SIGTERM."
        ),
    )
    add_storage_ingest_arguments(ingest)
    ingest.set_defaults(handler=cmd_storage_ingest)
    _build_storage_admin_parsers(storage_subparsers)


def ingest_main(argv: list[str] | None = None) -> int:
    """Standalone mixed-ingest parser retained for the executable example."""

    parser = argparse.ArgumentParser(
        prog="liuxin storage ingest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Catalogue a bounded mixed local file/container tree.\n\n"
            "Prefer the installed `liuxin storage ingest` command in operations."
        ),
    )
    add_storage_ingest_arguments(parser)
    args = parser.parse_args(argv)
    return cmd_storage_ingest(args)


def cmd_storage_ingest(args: argparse.Namespace) -> int:
    """Execute one logged, report-producing mixed-ingest invocation."""

    try:
        _apply_system_root_defaults(args)
        _validate_early_options(args)
        source_root = Path(args.source_root).expanduser().resolve(strict=False)
        run_id = args.run_id if args.run_id is not None else uuid4()
        log_directory = _log_directory(args, source_root)
    except (CLIUsageError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr, flush=True)
        return EXIT_USAGE

    try:
        session_context = RunLoggingSession(
            log_directory,
            run_id=run_id,
            prefix="mixed-ingest",
            level=int(getattr(logging, str(args.log_level))),
            max_text_bytes=int(args.log_max_mib) * 1024 * 1024,
            text_backup_count=int(args.log_backup_count),
        )
        with session_context as log_session:
            assert log_session.paths is not None
            paths = log_session.paths
            report_path = _report_path(args, source_root, paths.human_log)
            lock_path = _lock_path(args, source_root, log_directory)
            print(f"Run ID: {run_id}", file=sys.stderr, flush=True)
            print(f"Human log: {paths.human_log}", file=sys.stderr, flush=True)
            print(f"Event log: {paths.event_log}", file=sys.stderr, flush=True)
            print(f"Report: {report_path}", file=sys.stderr, flush=True)
            if lock_path is not None:
                print(f"Run lock: {lock_path}", file=sys.stderr, flush=True)
            return _run_logged_command(
                args,
                source_root=source_root,
                run_id=run_id,
                report_path=report_path,
                human_log=paths.human_log,
                event_log=paths.event_log,
                lock_path=lock_path,
                log_session=log_session,
            )
    except CLIUsageError as error:
        print(f"ERROR: {error}", file=sys.stderr, flush=True)
        return EXIT_USAGE
    except (OSError, ValueError) as error:
        print(f"ERROR: could not initialize ingest logging: {error}", file=sys.stderr)
        return EXIT_USAGE


def _run_logged_command(
    args: argparse.Namespace,
    *,
    source_root: Path,
    run_id: UUID,
    report_path: Path,
    human_log: Path,
    event_log: Path,
    lock_path: Path | None,
    log_session: RunLoggingSession,
) -> int:
    controller = SignalCancellation()
    try:
        _validate_paths(
            args,
            source_root=source_root,
            report_path=report_path,
            lock_path=lock_path,
        )
        _log_cli_start(
            args,
            source_root=source_root,
            run_id=run_id,
            report_path=report_path,
            human_log=human_log,
            event_log=event_log,
            lock_path=lock_path,
        )
        lock_context = (
            _acquire_run_lock(lock_path, run_id=run_id, args=args)
            if lock_path is not None
            else nullcontext()
        )
        with controller, lock_context:
            exit_code, payload = _run_ingest(
                args,
                source_root=source_root,
                run_id=run_id,
                cancellation_callback=controller.requested,
            )

        received_signal = controller.signal_number
        if received_signal is not None:
            exit_code = 128 + received_signal
            payload["ok"] = False
            payload["status"] = "cancelled"
            payload["signal"] = received_signal
        else:
            payload["status"] = "complete" if exit_code == EXIT_OK else "issues"
        _enrich_terminal_payload(
            payload,
            args=args,
            run_id=run_id,
            exit_code=exit_code,
            report_path=report_path,
            human_log=human_log,
            event_log=event_log,
            lock_path=lock_path,
        )
        _write_report(
            report_path,
            payload,
            replace=bool(args.replace_report),
            compact=bool(args.compact_json),
        )
        terminal_event = (
            "cli_cancelled" if received_signal is not None else "cli_complete"
        )
        _log(
            logging.WARNING if exit_code else logging.INFO,
            terminal_event,
            "Mixed ingest command cancelled"
            if received_signal is not None
            else "Mixed ingest command complete",
            run_id=run_id,
            exit_code=exit_code,
            ok=bool(payload["ok"]),
            signal=received_signal,
            report_file=str(report_path),
            human_log=str(human_log),
            event_log=str(event_log),
        )
        log_session.flush()
        _print_payload(args, payload)
        return exit_code
    except KeyboardInterrupt as error:
        signal_number = controller.signal_number
        exit_code = (
            128 + signal_number
            if signal_number is not None
            else EXIT_INTERRUPTED
        )
        return _handle_failure(
            args,
            error,
            event="cli_interrupted",
            status="interrupted",
            exit_code=exit_code,
            run_id=run_id,
            report_path=report_path,
            human_log=human_log,
            event_log=event_log,
            lock_path=lock_path,
            log_session=log_session,
            signal_number=signal_number,
        )
    except CLIUsageError as error:
        return _handle_failure(
            args,
            error,
            event="cli_configuration_error",
            status="configuration_error",
            exit_code=EXIT_USAGE,
            run_id=run_id,
            report_path=report_path,
            human_log=human_log,
            event_log=event_log,
            lock_path=lock_path,
            log_session=log_session,
        )
    except Exception as error:
        return _handle_failure(
            args,
            error,
            event="cli_failed",
            status="failed",
            exit_code=EXIT_ISSUES,
            run_id=run_id,
            report_path=report_path,
            human_log=human_log,
            event_log=event_log,
            lock_path=lock_path,
            log_session=log_session,
        )


def _apply_system_root_defaults(args: argparse.Namespace) -> None:
    raw_root = getattr(args, "system_root", None)
    raw_profile = getattr(args, "profile", None)
    if args.database and not raw_root and not raw_profile:
        return
    try:
        resolved = load_system_profile(
            system_root=raw_root,
            profile=raw_profile,
            use_environment=True,
            use_persisted=True,
            required=False,
        )
    except (FileNotFoundError, ValueError) as error:
        raise CLIUsageError(str(error)) from error
    if resolved is None:
        return
    manifest_path = resolved.path
    manifest = resolved.values
    if str(manifest.get("db_type") or "SQLite").strip().lower() not in {
        "sqlite",
        "apsw",
    }:
        raise CLIUsageError(
            "mixed local ingest currently requires a SQLite/APSW system manifest"
        )
    if not args.database:
        args.database = str(manifest.get("database") or "") or None
    if not args.materialization_root:
        value = manifest.get("materialization_root")
        args.materialization_root = None if value in (None, "") else str(value)
    if not args.log_directory:
        value = manifest.get("log_directory")
        args.log_directory = None if value in (None, "") else str(value)
    if not args.database:
        raise CLIUsageError(
            "system manifest has no catalogue path: {!s}".format(manifest_path)
        )
    args.require_existing_database = True


def _run_ingest(
    args: argparse.Namespace,
    *,
    source_root: Path,
    run_id: UUID,
    cancellation_callback: Callable[[], bool],
) -> tuple[int, dict[str, object]]:
    discovery_only = bool(args.discover_only) or bool(args.preflight_only)
    database_path = (
        None
        if discovery_only
        else Path(str(args.database)).expanduser().resolve(strict=False)
    )
    captured_stdout = (
        None
        if discovery_only
        else LoggingTextStream(
            _LOGGER,
            level=logging.DEBUG,
            stream_name="database_stdout",
        )
    )

    def application_event(
        level: int,
        event: str,
        message: str,
        details: Mapping[str, object],
    ) -> None:
        _log(level, event, message, run_id=run_id, **dict(details))

    result = execute_mixed_ingest(
        MixedIngestApplicationRequest(
            source_root=source_root,
            run_id=run_id,
            budget=_budget(args),
            discovery_only=discovery_only,
            database_path=database_path,
            recursive_filesystem=not bool(args.no_recursive_filesystem),
            recurse_containers=not bool(args.no_nested_containers),
            expand_ebook_containers=bool(args.expand_ebook_containers),
            continue_on_error=not bool(args.strict),
            verify=bool(args.verify),
            materialization_root=args.materialization_root,
            unsquashfs_exe=str(args.unsquashfs_exe),
            rar_extractor_exe=args.rar_extractor_exe,
            backend_timeout_s=float(args.backend_timeout_seconds),
            log_checkpoint_every=int(args.log_checkpoint_every),
            progress_callback=(
                None if bool(args.no_console_progress) else _console_progress
            ),
            cancellation_callback=cancellation_callback,
            event_callback=application_event,
            database_stdout=captured_stdout,
        )
    )
    report = result.report
    if discovery_only:
        payload: dict[str, object] = {
            "mode": "preflight" if args.preflight_only else "discovery",
            "ok": report.ok,
            "budget": result.budget,
            "report": report,
        }
        if args.preflight_only:
            checks = _preflight_checks(args, source_root, report.recognized_formats)
            ready = report.ok and all(
                bool(check["ok"])
                for check in checks
                if check["severity"] == "error"
            )
            payload["ok"] = ready
            payload["preflight"] = {
                "ready": ready,
                "checks": checks,
            }
            _log(
                logging.INFO if ready else logging.ERROR,
                "preflight_complete",
                "Mixed ingest preflight complete",
                run_id=run_id,
                ready=ready,
                check_count=len(checks),
                failed_checks=sum(not bool(check["ok"]) for check in checks),
            )
        return (EXIT_OK if bool(payload["ok"]) else EXIT_ISSUES), payload
    assert result.database_path is not None
    payload = {
        "mode": result.mode,
        "database": str(result.database_path),
        "metadata_is_durable": result.metadata_is_durable,
        "budget": result.budget,
        "ok": result.ok,
        "report": report,
    }
    return (EXIT_OK if result.ok else EXIT_ISSUES), payload


def _console_progress(event: str, details: Mapping[str, object]) -> None:
    if event == "container_started":
        print(
            f"[depth {details['depth']}] {details['format']}: {details['path']}",
            file=sys.stderr,
            flush=True,
        )
    elif event == "container_complete":
        print(
            "  members={} issues={} ok={}".format(
                details["members_adopted"],
                details["issue_count"],
                details["ok"],
            ),
            file=sys.stderr,
            flush=True,
        )
    elif event == "source_checkpoint":
        print(
            "[source checkpoint] adopted={}/{} containers={} issues pending".format(
                details["files_adopted"],
                details["files_examined"],
                details["containers_discovered"],
            ),
            file=sys.stderr,
            flush=True,
        )
    elif event == "member_checkpoint":
        print(
            "[member checkpoint] adopted={} expanded_bytes={} queued={}".format(
                details["run_members_adopted"],
                details["run_expanded_bytes"],
                details["queued_containers"],
            ),
            file=sys.stderr,
            flush=True,
        )


def _preflight_checks(
    args: argparse.Namespace,
    source_root: Path,
    recognized_formats: tuple[tuple[str, int], ...],
) -> list[dict[str, object]]:
    formats = dict(recognized_formats)
    checks: list[dict[str, object]] = []

    def add(
        name: str,
        ok: bool,
        message: str,
        *,
        severity: str = "error",
        **details: object,
    ) -> None:
        checks.append(
            {
                "name": name,
                "ok": bool(ok),
                "severity": severity,
                "message": message,
                **details,
            }
        )

    add(
        "source_readable",
        os.access(source_root, os.R_OK | os.X_OK),
        "source root is readable/searchable"
        if os.access(source_root, os.R_OK | os.X_OK)
        else "source root is not readable/searchable",
        path=str(source_root),
    )
    database_path = Path(args.database).expanduser().resolve(strict=False)
    database_parent = _nearest_existing_parent(database_path.parent)
    database_ok = (
        os.access(database_path, os.R_OK | os.W_OK)
        if database_path.exists()
        else os.access(database_parent, os.W_OK | os.X_OK)
    )
    add(
        "database_writable",
        database_ok,
        "existing catalogue is readable/writable"
        if database_path.exists() and database_ok
        else (
            "catalogue parent can create the database"
            if database_ok
            else "catalogue path is not writable"
        ),
        path=str(database_path),
        exists=database_path.exists(),
        free_bytes=shutil.disk_usage(database_parent).free,
    )
    if args.materialization_root:
        materialization = Path(args.materialization_root).expanduser().resolve(
            strict=False
        )
        materialization_parent = _nearest_existing_parent(materialization)
        writable = os.access(materialization_parent, os.W_OK | os.X_OK)
        add(
            "materialization_writable",
            writable,
            "materialization path is writable"
            if writable
            else "materialization path is not writable",
            path=str(materialization),
            free_bytes=shutil.disk_usage(materialization_parent).free,
        )
    elif not bool(args.no_nested_containers):
        add(
            "materialization_configured",
            False,
            "no cache is configured; nested containers will be catalogued but not opened",
            severity="warning",
        )

    if formats.get("squashfs", 0):
        executable = shutil.which(str(args.unsquashfs_exe))
        add(
            "squashfs_reader",
            executable is not None,
            f"unsquashfs available at {executable}"
            if executable
            else f"unsquashfs executable not found: {args.unsquashfs_exe}",
            executable=executable,
        )
    if formats.get("7z", 0):
        available = importlib.util.find_spec("py7zr") is not None
        add(
            "sevenzip_reader",
            available,
            "py7zr is installed" if available else "install LiuXin's archives extra for py7zr",
        )
    if formats.get("rar", 0):
        module_available = importlib.util.find_spec("rarfile") is not None
        extractor = (
            shutil.which(str(args.rar_extractor_exe))
            if args.rar_extractor_exe
            else shutil.which("unrar") or shutil.which("rar")
        )
        add(
            "rar_extended_readers",
            module_available or extractor is not None,
            "RAR optional reader/extractor is available"
            if module_available or extractor
            else "stored RAR 3/4 members remain available; RAR 5/compressed members may fail",
            severity="warning",
            rarfile_available=module_available,
            extractor=extractor,
        )
    if formats.get("iso", 0):
        udf_available = importlib.util.find_spec("pycdlib") is not None
        add(
            "udf_bridge_reader",
            udf_available,
            "pycdlib is installed for UDF bridge namespaces"
            if udf_available
            else "ISO 9660 remains available; install the archives extra for UDF bridge support",
            severity="warning",
        )
    return checks


def _budget(args: argparse.Namespace) -> MixedIngestBudget:
    return MixedIngestBudget(
        max_source_files=int(args.max_source_files),
        max_containers=int(args.max_containers),
        max_container_depth=int(args.max_container_depth),
        max_members=int(args.max_members),
        max_members_per_container=int(args.max_members_per_container),
        max_member_bytes=_gib(args.max_member_gib, "--max-member-gib"),
        max_container_expanded_bytes=_gib(
            args.max_container_expanded_gib,
            "--max-container-expanded-gib",
        ),
        max_total_expanded_bytes=_gib(
            args.max_total_expanded_gib,
            "--max-total-expanded-gib",
        ),
        max_container_expansion_ratio=float(args.max_expansion_ratio),
        max_materialized_bytes=_gib(
            args.max_materialized_gib,
            "--max-materialized-gib",
        ),
        max_temporary_bytes=_gib(
            args.max_temporary_gib,
            "--max-temporary-gib",
        ),
        max_path_depth=int(args.max_path_depth),
        max_path_bytes=int(args.max_path_bytes),
        max_wall_time_s=float(args.max_wall_time_seconds),
        max_issues=int(args.max_issues),
    )


def _gib(value: object, option: str) -> int:
    if not isinstance(value, (int, float, str)):
        raise CLIUsageError(f"{option} must be a number")
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise CLIUsageError(f"{option} must be a number") from error
    if number <= 0:
        raise CLIUsageError(f"{option} must be positive")
    return int(number * _GIB)


def _validate_early_options(args: argparse.Namespace) -> None:
    if not bool(args.discover_only) and not args.database:
        raise CLIUsageError(
            "--database is required unless --discover-only is selected"
        )
    if int(args.log_max_mib) < 1:
        raise CLIUsageError("--log-max-mib must be positive")
    if int(args.log_backup_count) < 0:
        raise CLIUsageError("--log-backup-count must not be negative")
    if int(args.log_checkpoint_every) < 1:
        raise CLIUsageError("--log-checkpoint-every must be positive")
    if int(args.lock_timeout_seconds) < 0:
        raise CLIUsageError("--lock-timeout-seconds must not be negative")
    if float(args.backend_timeout_seconds) <= 0:
        raise CLIUsageError("--backend-timeout-seconds must be positive")
    _ = _budget(args)


def _validate_paths(
    args: argparse.Namespace,
    *,
    source_root: Path,
    report_path: Path,
    lock_path: Path | None,
) -> None:
    _validate_source_root(source_root)
    _validate_run_control_paths(
        args,
        source_root=source_root,
        report_path=report_path,
        lock_path=lock_path,
    )
    _validate_database_path(args, source_root=source_root)
    _validate_materialization_path(args, source_root=source_root)


def _validate_source_root(source_root: Path) -> None:
    if not source_root.exists():
        raise CLIUsageError(f"source root does not exist: {source_root}")
    if not source_root.is_dir():
        raise CLIUsageError(f"source root is not a directory: {source_root}")


def _validate_run_control_paths(
    args: argparse.Namespace,
    *,
    source_root: Path,
    report_path: Path,
    lock_path: Path | None,
) -> None:
    if _path_is_within(report_path, source_root):
        raise CLIUsageError("--report-file must be outside --source-root")
    if report_path.exists() and not bool(args.replace_report):
        raise CLIUsageError(
            f"report file already exists: {report_path}; pass --replace-report"
        )
    if lock_path is not None and _path_is_within(lock_path, source_root):
        raise CLIUsageError("--lock-file must be outside --source-root")


def _validate_database_path(
    args: argparse.Namespace,
    *,
    source_root: Path,
) -> None:
    if not args.database:
        return
    database_path = Path(args.database).expanduser().resolve(strict=False)
    if _path_is_within(database_path, source_root):
        raise CLIUsageError("--database must be outside --source-root")
    if database_path.exists() and not database_path.is_file():
        raise CLIUsageError(f"database path is not a file: {database_path}")
    if bool(args.require_existing_database) and not database_path.is_file():
        raise CLIUsageError(f"database does not exist: {database_path}")


def _validate_materialization_path(
    args: argparse.Namespace,
    *,
    source_root: Path,
) -> None:
    if not args.materialization_root:
        return
    materialization = Path(args.materialization_root).expanduser().resolve(
        strict=False
    )
    if _path_is_within(materialization, source_root):
        raise CLIUsageError(
            "--materialization-root must be outside --source-root"
        )


def _log_directory(args: argparse.Namespace, source_root: Path) -> Path:
    if args.log_directory:
        selected = Path(args.log_directory).expanduser().resolve(strict=False)
        if _path_is_within(selected, source_root):
            raise CLIUsageError("--log-directory must be outside --source-root")
    elif args.database:
        database_path = Path(args.database).expanduser().resolve(strict=False)
        selected = database_path.with_name(database_path.name + ".ingest-logs")
    else:
        selected = source_root.parent / f".{source_root.name}.liuxin-ingest-logs"
    selected = selected.resolve(strict=False)
    if _path_is_within(selected, source_root):
        selected = (
            source_root.parent / f".{source_root.name}.liuxin-ingest-logs"
        ).resolve(strict=False)
    if _path_is_within(selected, source_root):
        raise CLIUsageError("the log directory must be outside --source-root")
    return selected


def _report_path(
    args: argparse.Namespace,
    source_root: Path,
    human_log: Path,
) -> Path:
    if args.report_file:
        path = Path(args.report_file).expanduser().resolve(strict=False)
    else:
        path = human_log.with_suffix(".report.json")
    if _path_is_within(path, source_root):
        raise CLIUsageError("--report-file must be outside --source-root")
    return path


def _lock_path(
    args: argparse.Namespace,
    source_root: Path,
    log_directory: Path,
) -> Path | None:
    if bool(args.no_run_lock) or bool(args.discover_only) or bool(args.preflight_only):
        return None
    if args.lock_file:
        path = Path(args.lock_file).expanduser().resolve(strict=False)
    else:
        database_name = Path(str(args.database)).name or "catalogue"
        path = (log_directory / f".{database_name}.mixed-ingest.lock").resolve(
            strict=False
        )
    if _path_is_within(path, source_root):
        raise CLIUsageError("--lock-file must be outside --source-root")
    return path


@contextmanager
def _acquire_run_lock(
    path: Path,
    *,
    run_id: UUID,
    args: argparse.Namespace,
) -> Generator[object, None, None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with ExclusiveFile(
            str(path),
            timeout=int(args.lock_timeout_seconds),
        ) as lock_file:
            lock_file.seek(0)
            lock_file.truncate()
            record = {
                "run_id": str(run_id),
                "process_id": os.getpid(),
                "hostname": socket.gethostname(),
                "started_utc": datetime.now().astimezone().isoformat(),
                "source_root": str(
                    Path(args.source_root).expanduser().resolve(strict=False)
                ),
                "database": str(
                    Path(args.database).expanduser().resolve(strict=False)
                ),
            }
            _ = lock_file.write(
                (json.dumps(record, ensure_ascii=True, sort_keys=True) + "\n").encode(
                    "utf-8"
                )
            )
            lock_file.flush()
            try:
                path.chmod(0o600)
            except OSError:
                pass
            _log(
                logging.INFO,
                "run_lock_acquired",
                "Mixed ingest run lock acquired",
                run_id=run_id,
                lock_file=str(path),
            )
            yield lock_file
    except LockError as error:
        raise CLIUsageError(
            f"another ingest owns run lock {path}; wait or use --no-run-lock"
        ) from error


def _log_cli_start(
    args: argparse.Namespace,
    *,
    source_root: Path,
    run_id: UUID,
    report_path: Path,
    human_log: Path,
    event_log: Path,
    lock_path: Path | None,
) -> None:
    excluded = {
        "database",
        "handler",
        "materialization_root",
        "report_file",
        "source_root",
    }
    _log(
        logging.INFO,
        "cli_started",
        "Mixed ingest command started",
        run_id=run_id,
        source_root=str(source_root),
        database=(
            None
            if args.database is None
            else str(Path(args.database).expanduser().resolve(strict=False))
        ),
        materialization_root=(
            None
            if args.materialization_root is None
            else str(
                Path(args.materialization_root).expanduser().resolve(strict=False)
            )
        ),
        mode=(
            "preflight"
            if args.preflight_only
            else "discovery" if args.discover_only else "ingest"
        ),
        arguments={
            key: value for key, value in vars(args).items() if key not in excluded
        },
        budget=dataclasses.asdict(_budget(args)),
        liuxin_version=liuxin_version,
        python_version=sys.version,
        python_executable=sys.executable,
        platform=platform.platform(),
        hostname=socket.gethostname(),
        process_id=os.getpid(),
        working_directory=str(Path.cwd()),
        report_file=str(report_path),
        human_log=str(human_log),
        event_log=str(event_log),
        lock_file=None if lock_path is None else str(lock_path),
    )


def _enrich_terminal_payload(
    payload: dict[str, object],
    *,
    args: argparse.Namespace,
    run_id: UUID,
    exit_code: int,
    report_path: Path,
    human_log: Path,
    event_log: Path,
    lock_path: Path | None,
) -> None:
    payload.update(
        {
            "schema_version": 1,
            "command": "storage ingest",
            "run_id": str(run_id),
            "exit_code": int(exit_code),
            "report_file": str(report_path),
            "human_log": str(human_log),
            "event_log": str(event_log),
            "lock_file": None if lock_path is None else str(lock_path),
            "stdout_report": not bool(args.no_stdout_report),
        }
    )


def _handle_failure(
    args: argparse.Namespace,
    error: BaseException,
    *,
    event: str,
    status: str,
    exit_code: int,
    run_id: UUID,
    report_path: Path,
    human_log: Path,
    event_log: Path,
    lock_path: Path | None,
    log_session: RunLoggingSession,
    signal_number: int | None = None,
) -> int:
    level = logging.ERROR if exit_code == EXIT_USAGE else logging.CRITICAL
    _LOGGER.log(
        level,
        "Mixed ingest command failed",
        exc_info=(type(error), error, error.__traceback__),
        extra={
            "liuxin_event": event,
            "liuxin_context": {
                "run_id": str(run_id),
                "error_type": type(error).__name__,
                "error_message": str(error) or type(error).__name__,
                "signal": signal_number,
            },
        },
    )
    payload: dict[str, object] = {
        "ok": False,
        "status": status,
        "error": {
            "type": type(error).__name__,
            "message": str(error) or type(error).__name__,
            "traceback": "".join(
                traceback.format_exception(type(error), error, error.__traceback__)
            ),
        },
    }
    if signal_number is not None:
        payload["signal"] = signal_number
    _enrich_terminal_payload(
        payload,
        args=args,
        run_id=run_id,
        exit_code=exit_code,
        report_path=report_path,
        human_log=human_log,
        event_log=event_log,
        lock_path=lock_path,
    )
    try:
        _write_report(
            report_path,
            payload,
            replace=bool(args.replace_report),
            compact=bool(args.compact_json),
        )
    except Exception as report_error:
        _LOGGER.error(
            "Could not write mixed ingest failure report",
            exc_info=(
                type(report_error),
                report_error,
                report_error.__traceback__,
            ),
            extra={
                "liuxin_event": "report_write_failed",
                "liuxin_context": {
                    "run_id": str(run_id),
                    "report_file": str(report_path),
                },
            },
        )
        print(f"ERROR: could not write report: {report_error}", file=sys.stderr)
    log_session.flush()
    print(
        f"Run {run_id} {status}; inspect {event_log}",
        file=sys.stderr,
        flush=True,
    )
    _print_payload(args, payload)
    return exit_code


def _write_report(
    path: Path,
    payload: Mapping[str, object],
    *,
    replace: bool,
    compact: bool,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = _json_text(payload, compact=compact) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        text=False,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            _ = output.write(text.encode("utf-8", errors="backslashreplace"))
            output.flush()
            os.fsync(output.fileno())
        if replace:
            os.replace(temporary, path)
        else:
            try:
                os.link(temporary, path)
            except FileExistsError as error:
                raise CLIUsageError(
                    f"report file already exists: {path}; pass --replace-report"
                ) from error
            temporary.unlink()
        _fsync_directory(path.parent)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _print_payload(args: argparse.Namespace, payload: Mapping[str, object]) -> None:
    if bool(args.no_stdout_report):
        return
    try:
        print(_json_text(payload, compact=bool(args.compact_json)), flush=True)
    except BrokenPipeError:
        pass


def _json_text(value: object, *, compact: bool) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        indent=None if compact else 2,
        separators=(",", ":") if compact else None,
        default=_json_default,
    )


def _json_default(value: object) -> object:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: getattr(value, field.name)
            for field in dataclasses.fields(value)
            if not field.name.startswith("_")
        }
    if isinstance(value, (UUID, Path)):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"cannot serialize {type(value).__name__} to JSON")


def _log(
    level: int,
    event: str,
    message: str,
    *,
    run_id: UUID,
    **details: object,
) -> None:
    context = dict(details)
    context["run_id"] = str(run_id)
    _LOGGER.log(
        level,
        message,
        extra={"liuxin_event": event, "liuxin_context": context},
    )


def _uuid_argument(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a UUID") from error


def _path_is_within(path: Path, directory: Path) -> bool:
    try:
        path.relative_to(directory)
    except ValueError:
        return False
    return True


def _nearest_existing_parent(path: Path) -> Path:
    candidate = path
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    return candidate


def _fsync_directory(path: Path) -> None:
    """Best-effort durability for a newly published report directory entry."""

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


__all__ = [
    "EXIT_INTERRUPTED",
    "EXIT_ISSUES",
    "EXIT_OK",
    "EXIT_TERMINATED",
    "EXIT_USAGE",
    "SignalCancellation",
    "add_storage_ingest_arguments",
    "build_storage_parser",
    "cmd_storage_ingest",
    "ingest_main",
]
