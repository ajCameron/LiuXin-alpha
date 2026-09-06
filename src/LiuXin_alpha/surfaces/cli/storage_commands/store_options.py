"""Storage CLI store options ownership."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from typing import Any

from LiuXin_alpha.surfaces.cli.common import load_json_object

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
        if normalized in {name.strip().lower().replace("-", "_") for name in names}:
            return descriptor
    choices = ", ".join(sorted(str(descriptor.get("kind")) for descriptor in providers))
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
            "Backend options must use NAME=VALUE, for example `region_name=eu-west-2`."
        )
    lowered = key.casefold()
    if lowered == "env" or any(
        marker in lowered for marker in _SENSITIVE_STORE_OPTION_MARKERS
    ):
        raise ValueError(
            f"Store option {key!r} looks secret-bearing and will not be "
            "persisted. Configure credentials through the backend's native "
            "profile/environment or an external secret provider."
        )
    value_text = raw_value.strip()
    try:
        value: object = json.loads(value_text)
    except (TypeError, ValueError, json.JSONDecodeError):
        value = value_text
    if value is None or isinstance(value, (str, int, float, bool)):
        return key, value
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return key, value
    raise ValueError(
        f"Backend option {key!r} must be a string, number, boolean, null, or "
        "string list."
    )


def _reject_sensitive_policy(value: object, *, path: str = "policy") -> None:
    if isinstance(value, Mapping):
        for raw_key, item in value.items():
            key = str(raw_key)
            lowered = key.casefold()
            if lowered == "env" or any(
                marker in lowered for marker in _SENSITIVE_STORE_OPTION_MARKERS
            ):
                raise ValueError(
                    f"{path} contains secret-bearing field {key!r}; Store policy is "
                    "durable configuration, not a credential store."
                )
            _reject_sensitive_policy(item, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_sensitive_policy(
                item,
                path=f"{path}[{index}]",
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
            "Backend {!r} does not expose durable backend options.".format(
                descriptor.get("kind")
            )
        )
    existing = policy.get(str(policy_section), {})
    if not isinstance(existing, Mapping):
        raise ValueError(f"Policy section {policy_section!r} must be a JSON object.")
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
            f"Core returned invalid capabilities for backend {descriptor_kind!r}."
        )
    requested_read_only = getattr(args, "read_only", None)
    read_only_default = bool(descriptor.get("read_only_default", False))
    read_only = (
        read_only_default if requested_read_only is None else bool(requested_read_only)
    )
    if read_only_default and not read_only:
        raise ValueError(f"Backend {descriptor_kind!r} is intrinsically read-only.")
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
        raise ValueError("The default Store must be online and writable.")
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
        "store_supports_random_read": int(bool(capabilities.get("random_read", False))),
        "store_supports_random_write": int(
            bool(capabilities.get("random_write", False)) and not read_only
        ),
        "store_supports_delete": int(
            bool(capabilities.get("delete", False)) and not read_only
        ),
        "store_supports_checksums": int(bool(capabilities.get("checksums", False))),
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
