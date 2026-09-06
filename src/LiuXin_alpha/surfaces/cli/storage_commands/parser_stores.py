"""Storage CLI parser stores ownership."""

from __future__ import annotations

import argparse

from LiuXin_alpha.surfaces.cli.storage_commands.administration import (
    cmd_storage_backends_list,
    cmd_storage_default_set,
    cmd_storage_default_show,
    cmd_storage_refresh,
    cmd_storage_store_delete,
    cmd_storage_store_evacuate,
    cmd_storage_store_probe,
    cmd_storage_store_save,
    cmd_storage_store_show,
    cmd_storage_store_update,
    cmd_storage_stores_list,
)
from LiuXin_alpha.surfaces.cli.storage_commands.core_access import _core_json
from LiuXin_alpha.surfaces.cli.storage_commands.store_add import cmd_storage_store_add
from LiuXin_alpha.surfaces.cli.storage_commands.store_wizard import cmd_storage_add


def _add_backends_parser(
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


def _add_add_store_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
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


def _add_stores_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    stores = commands.add_parser("stores", help="List configured stores.")
    _core_json(stores)
    stores.add_argument("--refresh", action="store_true")
    stores.set_defaults(handler=cmd_storage_stores_list)


def _add_store_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    store = commands.add_parser("store", help="Inspect or administer one Store.")
    store_commands = store.add_subparsers(dest="store_action", required=True)
    show = store_commands.add_parser("show", aliases=["get"])
    _core_json(show)
    show.add_argument("store", help="Store UUID, database id, or unique name.")
    show.set_defaults(handler=cmd_storage_store_show)
    save = store_commands.add_parser(
        "save", help="Save a Store configuration JSON object."
    )
    _core_json(save)
    save.add_argument(
        "store_file", help="CLI-host JSON file containing the Store object."
    )
    save.set_defaults(handler=cmd_storage_store_save)

    add = store_commands.add_parser(
        "add",
        help="Add a Store using ordinary typed options; JSON save remains available.",
    )
    _core_json(add)
    add.add_argument("kind", help="Backend kind, for example filesystem or s3.")
    add.add_argument("root", help="Root path or backend URI as seen by Core.")
    add.add_argument(
        "--name", required=True, help="Unique operator-visible Store name."
    )
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
    add.add_argument(
        "--default", action="store_true", help="Select it as the default Store."
    )
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
    mutability.add_argument("--read-only", dest="read_only", action="store_true")
    mutability.add_argument("--writable", dest="read_only", action="store_false")
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


def _add_default_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    default = commands.add_parser("default", help="Inspect or set the default Store.")
    default_commands = default.add_subparsers(dest="default_action", required=True)
    default_show = default_commands.add_parser("show", aliases=["get"])
    _core_json(default_show)
    default_show.set_defaults(handler=cmd_storage_default_show)
    default_set = default_commands.add_parser("set")
    _core_json(default_set)
    default_set.add_argument("store")
    default_set.set_defaults(handler=cmd_storage_default_set)


def _add_refresh_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    refresh = commands.add_parser(
        "refresh", help="Reload Store configurations from the database."
    )
    _core_json(refresh)
    refresh.add_argument("--startup-on-add", action="store_true")
    refresh.add_argument("--include-offline", action="store_true")
    refresh.add_argument("--keep-existing", action="store_true")
    refresh.add_argument("--strict", action="store_true")
    refresh.set_defaults(handler=cmd_storage_refresh)
