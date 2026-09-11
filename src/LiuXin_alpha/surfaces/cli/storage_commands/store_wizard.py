"""
Collect and confirm a Store declaration before delegating its save/refresh/probe sequence.

Interactive choices use a previously queried Core backend catalogue. A complete
automation argument triple bypasses prompts and shares the same add command,
normally with probing enabled. Confirmation is not a transaction: after acceptance
the command queries current descriptors again, and later failures do not undo saves.
Displayed plan text is not a complete dump or a comprehensive secret sanitizer.
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
    """
    Derive a short lowercase suggested name from a root's final path or address component.

    Strip outer whitespace/trailing separators, then prefer URL path tail, netloc,
    colon prefix, or host Path basename/kind. Run the candidate through the portable
    name helper with an 80-character ceiling and no hash, then trim edge punctuation.
    The final fallback is kind unchanged; uniqueness and final filename safety are
    not guaranteed, especially after trimming or fallback.

    Example:
        >>> _default_store_name("s3://bucket/My Books/", "s3")
        'my_books'


    :param root: Proposed Core-host path or backend address used only for name suggestion.
    :param kind: Backend token used when no nonempty rendered candidate remains.
    :return: Suggested display name without probing a path or checking existing Stores.
    """
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
    """
    Prompt for one advertised backend using exact kind strings and return its original mapping.

    Prefer selected_kind or filesystem as the blank-input default when present;
    otherwise use the first provider. This wizard selector does not normalize
    kinds or use descriptor aliases like the later typed-add matcher does.

    Example:
        >>> descriptor = _wizard_backend(providers, "filesystem")  # doctest: +SKIP


    :param providers: Ordered selectable backend mappings; labels are display-only.
    :param selected_kind: Optional exact kind token used to choose the initial menu default.
    :return: First original provider whose stringified kind matches the selected value.
    :raises ValueError: Core supplies no selectable providers.
    :raises _StorageAddCancelled: Menu prompting receives EOF or an interrupt.
    """
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
    """
    Retain prompted Store declaration choices pending display and final confirmation.

    Frozen fields prevent reassignment, not mutation of the referenced descriptor
    mapping. The record itself validates nothing and does not bind later execution
    to the same backend catalogue or guarantee live backend readiness.

    Example:
        >>> plan = _StorageAddWizardPlan({}, "filesystem", "/books", "Books", "live", False, True, None, None, (), (), False, True)
        >>> plan.name, plan.check
        ('Books', True)


    :ivar descriptor: Original advertised backend mapping used for defaults and display.
    :ivar kind: Selected backend kind token.
    :ivar root: Store path/address interpreted later on the Core host.
    :ivar name: Proposed operator-visible Store name.
    :ivar role: Selected live/backup/archive/source/cache role.
    :ivar read_only: Requested read-only access state.
    :ivar online: Declared online state, not a measured reachability result.
    :ivar failure_domain: Optional failure-domain declaration.
    :ivar region: Optional placement-region declaration.
    :ivar tags: Ordered prompted tags, not yet deduplicated by row construction.
    :ivar option_values: Additional NAME=VALUE options retained for later policy construction.
    :ivar make_default: Whether to request default selection after saving.
    :ivar check: Whether the subsequent add command should probe the online Store.
    """

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
    """
    Select effective read-only and declared online state using backend and CLI defaults.

    Intrinsic read-only skips the access question and prints guidance. Writable
    backends prompt with the explicit read_only value or False. Online status is
    always prompted, defaulting to the inverse of the offline option.

    Example:
        >>> read_only, online = _wizard_access(args, descriptor)  # doctest: +SKIP


    :param args: Optional read_only and offline choices used as prompt defaults.
    :param descriptor: Advertised read_only_default used to force intrinsic read-only access.
    :return: Requested (read_only, online) booleans, without probing the backend.
    """
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
    """
    Preserve or prompt advanced domain/region/tags and additional backend assignments.

    Declining preserves supplied values; accepting uses visible text defaults, so
    blank input does not clear an existing nonempty default despite prompt wording.
    Tags split on commas, strip, and retain duplicates. New backend options are
    parsed for supported shape/secret-like keys only when a policy_section exists;
    preexisting options and positional backend_options are not validated here.

    Example:
        >>> advanced = _wizard_advanced_configuration(args, descriptor)  # doctest: +SKIP


    :param args: Optional failure_domain, region, tag, and option values to retain or extend.
    :param descriptor: Backend policy_section determining whether extra assignments are prompted.
    :return: Failure domain, region, tags tuple, and option-values tuple in that order.
    :raises ValueError: A newly entered backend assignment fails parsing or key restrictions.
    """
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
    """
    Offer default selection only for online writable live Stores, and probing for any online Store.

    An unspecified check option defaults the probe prompt to True. Offline Stores
    return both choices false without either question; non-live/read-only online
    Stores still receive the probe question but cannot opt into default selection here.

    Example:
        >>> _wizard_post_save_actions(argparse.Namespace(), online=False, read_only=False, role="live")
        (False, False)


    :param args: Optional default/check flags used as eligible prompt defaults.
    :param online: Declared online state controlling both post-save offers.
    :param read_only: Access choice disqualifying default selection when true.
    :param role: Exact live role required to offer default selection.
    :return: (make_default, check) booleans; no action is executed here.
    """
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
    """
    Filter the backend catalogue and collect a complete interactive Store plan.

    Accept only list-valued backends containing mappings whose user_selectable
    value is truthy or omitted. Prompt backend, Core-host root, name, role, access,
    advanced options, and post-save choices in order. Selected declaration text is
    not checked for root existence, uniqueness, or full policy validity at this stage.

    Example:
        >>> plan = _storage_add_wizard_plan(args, backend_catalogue)  # doctest: +SKIP


    :param args: Optional prefilled choices and defaults; not mutated while building the plan.
    :param providers_payload: Mapping containing the previously queried backends list.
    :return: Frozen plan retaining the selected descriptor reference and prompted values.
    :raises ValueError: No selectable backend exists or an entered option is invalid.
    :raises _StorageAddCancelled: Any delegated prompt is cancelled.
    """
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
    """
    Print the main Store choices and mapping-shaped advertised limitations before confirmation.

    Domain/region/tags/backend assignments are not shown. Root, name, and descriptor
    text are printed verbatim; the credentials guidance is not proof that every
    selected value is secret-free or that later persistence will succeed.

    Example:
        >>> _print_storage_add_wizard_plan(plan)  # doctest: +SKIP


    :param plan: Pending declaration whose display subset is rendered to stdout.
    :return: None after printing; no Core call, redaction pass, or persistence is performed.
    """
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
    """
    Copy confirmed plan choices into the existing add-command namespace.

    Convert online to offline and tuple tags/options to fresh lists. Other settings
    such as positional backend_options, policy_file, UUID, protocol, and connection/
    output selectors remain untouched; descriptor itself is not passed as an execution lock.

    Example:
        >>> _apply_storage_add_wizard_plan(args, plan)  # doctest: +SKIP


    :param args: Mutable namespace used by the subsequent typed Store-add command.
    :param plan: Confirmed choices copied into corresponding scalar and list fields.
    :return: None; update args in place without executing a storage operation.
    """
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
    """
    Build/display a plan, require final confirmation, and delegate typed Store addition.

    Do not mutate args until confirmation succeeds. The delegated command opens
    its own session and re-queries backend descriptors; confirmation is not a saved
    execution snapshot or transaction covering subsequent persistence.

    Example:
        >>> status = _run_storage_add_wizard(args, backend_catalogue)  # doctest: +SKIP


    :param args: Parsed namespace receiving confirmed plan values before execution.
    :param providers_payload: Previously queried catalogue used only to build/display the plan.
    :return: Exit code from cmd_storage_store_add after confirmation.
    :raises _StorageAddCancelled: The operator declines saving or cancels a delegated prompt.
    """
    plan = _storage_add_wizard_plan(args, providers_payload)
    _print_storage_add_wizard_plan(plan)
    if not _storage_prompt_yes_no("Save this Store?", default=False):
        raise _StorageAddCancelled
    _apply_storage_add_wizard_plan(args, plan)
    return cmd_storage_store_add(args)


def cmd_storage_add(args: argparse.Namespace) -> int:
    """
    Choose automation or an interactive Store wizard, then delegate the common add workflow.

    A name/kind/root triple counts as complete unless a value is None or ''; whitespace
    still counts here. Complete noninteractive requests default check=None to True.
    Otherwise require terminal stdin, query backend descriptors in a storage-enabled
    session, close it, and prompt. Interactive confirmation later opens a fresh session
    through typed addition. Catch wizard cancellation as status one; other errors
    propagate, and this boundary supplies no rollback for completed effects.

    Example:
        >>> status = cmd_storage_add(parsed_storage_add_args)  # doctest: +SKIP


    :param args: Parsed add namespace including optional triple, interactive/check flags,
        declaration defaults, and shared connection/output controls; may be mutated.
    :return: Common add-command status, or one after a caught wizard cancellation.
    :raises ValueError: Wizard mode lacks a terminal or Core returns a nonmapping catalogue.
    """
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
