"""Build, display, and confirm a Store-add plan before calling Core.

Interactive choices use Core-advertised backend capabilities. A complete
automation argument triple bypasses prompts and shares the same add command.
"""

from __future__ import annotations

import argparse
import dataclasses
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from LiuXin_alpha.surfaces.cli.common import open_cli_core
from LiuXin_alpha.surfaces.cli.storage_commands.prompts import (
    _storage_prompt_choice,
    _storage_prompt_text,
    _storage_prompt_yes_no,
    _storage_stdin_is_interactive,
    _StorageAddCancelled,
)
from LiuXin_alpha.surfaces.cli.storage_commands.store_add import cmd_storage_store_add
from LiuXin_alpha.surfaces.cli.storage_commands.store_options import (
    _default_store_role,
    _parse_backend_option,
)
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


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
                False if configured_read_only is None else bool(configured_read_only)
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

    failure_domain = (
        _storage_prompt_text(
            "Failure domain (blank for none)",
            default=failure_domain,
            required=False,
        )
        or None
    )
    region = (
        _storage_prompt_text(
            "Region (blank for none)",
            default=region,
            required=False,
        )
        or None
    )
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
            if isinstance(item, Mapping) and bool(item.get("user_selectable", True))
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
        default_value=getattr(args, "role", None) or _default_store_role(descriptor),
    )
    read_only, online = _wizard_access(args, descriptor)
    failure_domain, region, tags, option_values = _wizard_advanced_configuration(
        args, descriptor
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
    print(f"  name: {plan.name}")
    print("  backend: {} ({})".format(descriptor.get("label"), plan.kind))
    print(f"  root: {plan.root} (interpreted on the Core host)")
    print(f"  role: {plan.role}")
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
        getattr(args, name, None) not in (None, "") for name in ("name", "kind", "root")
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
