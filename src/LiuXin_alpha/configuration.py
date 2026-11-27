"""Resolve configuration paths and scope application settings before opening resources.

Paths are immutable snapshots. Explicit paths beat environment overrides, which
beat deployment-local defaults. A profile never changes process environment or
module constants. Core dispatch and submitted worker tasks carry a settings
context; standalone callers use the lazy process default or ``use_preferences``.
"""

from __future__ import annotations

import base64
import math
import os
import threading
from collections.abc import Iterator, Mapping, MutableMapping
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class ConfigurationPaths:
    """Absolute paths for application settings and independent component files."""

    preferences: Path
    components: Path

    @classmethod
    def resolve(
        cls,
        *,
        system_root: str | os.PathLike[str] | None = None,
        preferences_dir: str | os.PathLike[str] | None = None,
        config_dir: str | os.PathLike[str] | None = None,
        environment: Mapping[str, str] | None = None,
    ) -> ConfigurationPaths:
        """Choose explicit > environment > deployment/user paths, without I/O.

        LIUXIN_CONFIG_DIR wins over CALIBRE_CONFIG_DIRECTORY. Without a deployment,
        LIUXIN_BASE_DIR retains its development-tree preference location; normal
        installations use XDG_CONFIG_HOME/liuxin (or ~/.config/liuxin).
        """
        env = os.environ if environment is None else environment
        root = Path(system_root).expanduser() if system_root else None
        if root is None and env.get("LIUXIN_BASE_DIR"):
            root = Path(env["LIUXIN_BASE_DIR"]).expanduser()
        prefs = preferences_dir or env.get("LIUXIN_PREFS_DIR")
        if not prefs:
            prefs = (
                root / "LiuXin_prefs"
                if root is not None
                else user_configuration_directory(env)
            )
        preference_path = Path(prefs).expanduser().resolve()
        components = (
            config_dir
            or env.get("LIUXIN_CONFIG_DIR")
            or env.get("CALIBRE_CONFIG_DIRECTORY")
        )
        component_path = (
            Path(components).expanduser().resolve()
            if components
            else preference_path / "calibre_config"
        )
        return cls(preference_path, component_path)


def user_configuration_directory(environment: Mapping[str, str] | None = None) -> Path:
    """Locate connection pointers and named profiles independently of a deployment."""
    env = os.environ if environment is None else environment
    root = (
        Path(env["XDG_CONFIG_HOME"]).expanduser()
        if env.get("XDG_CONFIG_HOME")
        else Path.home() / ".config"
    )
    return (root / "liuxin").resolve()


def _pack_preference(value: Any, ancestors: set[int], depth: int = 0) -> Any:
    """Encode literal settings without pickle, imports or lossy container coercion."""
    if depth > 48:
        raise ValueError("Captured preferences exceed the nesting limit")
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("Captured preferences require finite numbers")
        return value
    if isinstance(value, Path):
        return {"kind": "path", "items": str(value)}
    if isinstance(value, bytes):
        return {"kind": "bytes", "items": base64.b64encode(value).decode("ascii")}
    if id(value) in ancestors:
        raise ValueError("Captured preferences contain a cycle")
    ancestors.add(id(value))
    try:
        if isinstance(value, Mapping):
            return {
                "kind": "dict",
                "items": [
                    [
                        _pack_preference(key, ancestors, depth + 1),
                        _pack_preference(item, ancestors, depth + 1),
                    ]
                    for key, item in value.items()
                ],
            }
        if type(value) in (list, tuple, set, frozenset):
            return {
                "kind": type(value).__name__,
                "items": [
                    _pack_preference(item, ancestors, depth + 1) for item in value
                ],
            }
        raise TypeError(f"Unsupported captured preference type: {type(value).__name__}")
    finally:
        ancestors.remove(id(value))


def _unpack_preference(value: Any, depth: int = 0) -> Any:
    """Restore only explicitly supported literal types from a captured setting."""
    if depth > 48:
        raise ValueError("Captured preferences exceed the nesting limit")
    if not isinstance(value, dict):
        if (
            value is None
            or type(value) in (str, bool, int)
            or (type(value) is float and math.isfinite(value))
        ):
            return value
        raise ValueError("Invalid captured preference literal")
    if set(value) != {"kind", "items"}:
        raise ValueError("Invalid captured preference encoding")
    kind, items = value["kind"], value["items"]
    if kind == "path" and isinstance(items, str):
        return Path(items)
    if kind == "bytes" and isinstance(items, str):
        return base64.b64decode(items, validate=True)
    if not isinstance(items, list):
        raise ValueError("Invalid captured preference items")
    if kind == "dict":
        return {
            _unpack_preference(pair[0], depth + 1): _unpack_preference(
                pair[1], depth + 1
            )
            for pair in items
        }
    constructors = {"list": list, "tuple": tuple, "set": set, "frozenset": frozenset}
    if kind not in constructors:
        raise ValueError("Unknown captured preference kind")
    return constructors[kind](_unpack_preference(item, depth + 1) for item in items)


class CapturedPreferences(dict[str, Any]):
    """Private worker settings restored from literal values, without file access."""

    def __init__(
        self,
        values: dict[str, Any],
        paths: dict[str, str] | None,
        defaults: dict[str, Any] | None = None,
        encoding_version: int = 0,
    ):
        if encoding_version not in (0, 1):
            raise ValueError("Unknown captured preferences version")
        decode = _unpack_preference if encoding_version else deepcopy
        super().__init__((key, decode(value)) for key, value in values.items())
        self._defaults = {key: decode(value) for key, value in (defaults or {}).items()}
        self.paths = (
            None
            if paths is None
            else ConfigurationPaths(
                Path(paths["preferences"]), Path(paths["components"])
            )
        )

    @property
    def defaults(self) -> dict[str, Any]:
        """Return an independent copy, matching the loaded Preferences contract."""
        return deepcopy(self._defaults)


def preferences_snapshot(preferences: Any) -> dict[str, Any]:
    """Capture values and defaults for a durable worker without reloading settings."""
    paths = getattr(preferences, "paths", None)
    defaults = getattr(preferences, "defaults", {})
    if callable(defaults):
        defaults = defaults()
    if not isinstance(defaults, Mapping):
        raise TypeError("Captured preference defaults must be a mapping")
    return {
        "encoding_version": 1,
        "values": {
            key: _pack_preference(value, set())
            for key, value in dict(preferences).items()
        },
        "defaults": {
            key: _pack_preference(value, set()) for key, value in defaults.items()
        },
        "paths": None
        if paths is None
        else {
            "preferences": str(paths.preferences),
            "components": str(paths.components),
        },
    }


_current: ContextVar[Any | None] = ContextVar("liuxin_preferences", default=None)
_default: Any | None = None
_default_lock = threading.RLock()


def current_preferences() -> Any:
    """Return the context store, lazily constructing the process default on first use."""
    selected = _current.get()
    if selected is not None:
        return selected
    global _default
    with _default_lock:
        if _default is None:
            from LiuXin_alpha.preferences import Preferences

            _default = Preferences(
                paths=ConfigurationPaths.resolve(), upgrade_on_load=False
            )
        return _default


def capture_preferences(store: Any | None = None) -> Any:
    """Pin a concrete store for a component lifetime, preserving empty mappings.

    ``None`` and the live proxy resolve the current selection now. Retaining the
    proxy itself would allow later dispatches to redirect an existing component.
    Values are shared in this process, rather than copied or frozen.
    """
    return (
        current_preferences()
        if store is None or isinstance(store, PreferenceView)
        else store
    )


@contextmanager
def use_preferences(store: Any) -> Iterator[None]:
    """Scope an already composed store; restore the previous selection even on failure."""
    # Retaining the proxy itself would make its next lookup recurse forever.
    selected = current_preferences() if isinstance(store, PreferenceView) else store
    token = _current.set(selected)
    try:
        yield
    finally:
        _current.reset(token)


class PreferenceView(MutableMapping[str, Any]):
    """Live access to the selected store, without a second cache or persistence owner."""

    def __getitem__(self, key: str) -> Any:
        return current_preferences()[key]

    def __setitem__(self, key: str, value: Any) -> None:
        current_preferences()[key] = value

    def __delitem__(self, key: str) -> None:
        del current_preferences()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(current_preferences())

    def __len__(self) -> int:
        return len(current_preferences())

    def set(self, key: str, value: Any) -> None:
        self[key] = value

    def __getattr__(self, name: str) -> Any:
        return getattr(current_preferences(), name)


preferences = PreferenceView()


def configuration_paths() -> ConfigurationPaths:
    """Resolve paths without loading settings, or use the active store's fixed paths."""
    selected = _current.get()
    if selected is None:
        selected = _default
    return (
        selected.paths
        if selected is not None and hasattr(selected, "paths")
        else ConfigurationPaths.resolve()
    )


def bound_preferences() -> Any | None:
    """Return only an explicit context selection, for transport to a worker process."""
    return _current.get()
