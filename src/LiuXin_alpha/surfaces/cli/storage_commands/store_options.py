"""
Build typed Store declarations and merge backend policy using advertised capabilities.

Descriptor matching accepts normalized kinds and list-valued aliases. Durable
policy rejects selected secret-like field names, not arbitrary secret values;
this is not comprehensive credential detection. Store construction does not probe
the backend, validate every URI/role, or persist anything by itself.
"""

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
    """
    Return the first backend descriptor matching a normalized kind or list-valued alias.

    Strip, lowercase, and replace hyphens with underscores on both sides. Aliases
    in tuples or other non-list containers are ignored; matching returns the
    original descriptor rather than a copy, and duplicate matches favor the first.

    Example:
        >>> descriptor = {"kind": "local_disk", "aliases": ["disk"]}
        >>> _descriptor_for_kind(" LOCAL-DISK ", [descriptor]) is descriptor
        True


    :param kind: User-supplied backend token normalized for comparison.
    :param providers: Ordered advertised mappings supplying kind and optional aliases.
    :return: First matching mapping by identity.
    :raises ValueError: No advertised kind or accepted alias matches the token.
    """
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
    """
    Infer archive/source/live role from location type and default read-only policy.

    Exact location_type='file' takes precedence over read_only_default. This is
    a UI/declaration default, not a runtime capability probe.

    Example:
        >>> _default_store_role({"location_type": "file", "read_only_default": True})
        'archive'
        >>> _default_store_role({"read_only_default": True})
        'source'


    :param descriptor: Advertised backend fields used without broader validation.
    :return: 'archive' for file locations, otherwise 'source' if read-only, else 'live'.
    """
    if descriptor.get("location_type") == "file":
        return "archive"
    if bool(descriptor.get("read_only_default", False)):
        return "source"
    return "live"


def _parse_backend_option(raw: str) -> tuple[str, object]:
    """
    Parse one NAME=VALUE option into a safe-named JSON scalar/string-list or text value.

    Split at the first equals sign and strip key/value text. Reject env and listed
    secret-like substrings in keys case-insensitively, but preserve the key's case.
    Valid JSON must be scalar or a string-only list; unparseable JSON stays text.
    Empty strings and encoder-supported nonfinite numbers remain possible, and
    secret-looking values under an accepted key are not inspected.

    Example:
        >>> _parse_backend_option('region_name = eu-west-2')
        ('region_name', 'eu-west-2')
        >>> _parse_backend_option('enabled=true')
        ('enabled', True)
        >>> _parse_backend_option('names=["a", "b"]')
        ('names', ['a', 'b'])


    :param raw: Assignment text, stringified before first-equals splitting.
    :return: Stripped key and parsed scalar/string-list or stripped fallback text.
    :raises ValueError: Missing/empty key, secret-like key, or unsupported decoded value shape.
    """
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
    """
    Recursively reject known secret-like mapping keys within mappings and lists.

    Stringify keys only for checking/diagnostics; no input is rewritten. Values,
    tuples, and other container types are not inspected, and there is no cycle
    detection or depth cap. This naming heuristic is not comprehensive sanitization.

    Example:
        >>> _reject_sensitive_policy({"s3": {"region_name": "eu-west-2"}})


    :param value: Policy value traversed only when it is a Mapping or list.
    :param path: Dotted/indexed diagnostic prefix identifying a rejected field's parent.
    :return: None when no visited key matches env or a sensitive-name marker.
    :raises ValueError: A visited mapping key looks credential-bearing.
    """
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
    """
    Merge a bounded policy file with backend-specific assignments, rejecting sensitive keys.

    Validate file policy first, even if assignments would overwrite a rejected
    field. Apply backend_options then option entries in order, with later keys
    winning. Assignments require a non-None policy_section and a mapping-valued
    existing section. Only when assignments exist, stamp backend and that section;
    a file-only policy is returned without descriptor alignment or schema checks.

    Example:
        >>> args = argparse.Namespace(option=["region_name=eu-west-2"])
        >>> _backend_policy(args, {"kind": "s3", "policy_section": "s3"})
        {'backend': 's3', 's3': {'region_name': 'eu-west-2'}}


    :param args: Optional policy_file and backend_options/option assignment sequences.
    :param descriptor: Backend kind and optional policy_section for assignment placement.
    :return: New outer policy dictionary with loaded nested values and a copied edited section.
    :raises ValueError: Policy loading/key checks, assignment parsing, or section validation fails.
    """
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
    """
    Build a Store row declaration from CLI choices and one advertised backend descriptor.

    Require mapping-shaped capabilities and nonempty stripped name/root. Respect
    read_only_default, rejecting attempts to make intrinsic read-only backends
    writable; default selection also rejects offline/read-only Stores. Default role
    follows the descriptor, not the effective requested read-only flag. Capability
    booleans become integers, with random-write/delete additionally disabled for
    read-only selection. Preserve optional identifiers/domain/region, sort/deduplicate
    tags, and serialize nonempty backend policy as ASCII-escaped sorted JSON.

    No root existence/URI, UUID, custom role, or live capability check is performed;
    the result is a declaration, not proof that the backend is available.

    Example:
        >>> store = _store_add_payload(argparse.Namespace(root=" /books ", name=" Books "), {"kind": "local", "capabilities": {"random_write": True}})
        >>> store["store_name"], store["store_root_uri"], store["store_supports_random_write"]
        ('Books', '/books', 1)


    :param args: Required root/name and optional access, role, status, protocol, tags,
        identity/domain/region, default-selection, and policy controls.
    :param descriptor: Advertised kind, defaults, protocol, capabilities, and policy section.
    :return: New database-field-keyed Store dictionary, without persistence or backend probing.
    :raises ValueError: Capability shape, name/root, access/default policy, or backend options fail.
    """
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
