"""Resolve configuration paths and scope application settings before opening resources.

Paths are immutable snapshots. Explicit paths beat environment overrides, which
beat deployment-local defaults. A profile never changes process environment or
module constants. Core dispatch and submitted worker tasks carry a settings
context; standalone callers use the lazy process default or ``use_preferences``.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Iterator, Mapping, MutableMapping
from contextlib import contextmanager
from contextvars import ContextVar
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
