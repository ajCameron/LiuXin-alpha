"""Registry facade for metadata file-source readers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, TypeVar, overload


ReaderPluginT = TypeVar("ReaderPluginT", bound=type)

_builtin_reader_plugins: tuple[type, ...] | None = None
_registered_reader_plugins: list[type] = []
_registry_revision = 0


@dataclass(frozen=True)
class MetadataReaderEntry:
    """Static registry metadata for a reader plugin class."""

    plugin_cls: type
    file_types: tuple[str, ...]
    normalized_file_types: tuple[str, ...]
    inplace_run_cost: str

    @property
    def name(self) -> str:
        return self.plugin_cls.__name__


def normalize_file_type(raw_file_type: str | None) -> str:
    """Normalize extension aliases used by metadata-reader registration."""
    ext = (raw_file_type or "").lower().lstrip(".")
    if ext in {"html", "htm", "xhtml", "xhtm", "xml"}:
        return "html"
    if ext in {"mobi", "prc", "azw"}:
        return "mobi"
    if ext in {"odt", "ods", "odp", "odg", "odf"}:
        return "odt"
    return ext


def _load_builtin_reader_plugins() -> tuple[type, ...]:
    global _builtin_reader_plugins
    if _builtin_reader_plugins is None:
        from LiuXin_alpha.customize.builtins.metadata_readers import (
            get_metadata_reader_plugins as get_builtin_metadata_reader_plugins,
        )

        _builtin_reader_plugins = tuple(get_builtin_metadata_reader_plugins())
    return _builtin_reader_plugins


def _validate_reader_plugin(plugin_cls: type) -> None:
    if not isinstance(plugin_cls, type):
        raise TypeError("metadata reader plugin must be a class.")
    file_types = getattr(plugin_cls, "file_types", None)
    if not file_types:
        raise ValueError("metadata reader plugin must declare at least one file type.")
    if not callable(getattr(plugin_cls, "get_metadata", None)):
        raise TypeError("metadata reader plugin must define get_metadata().")


def _plugin_identity(plugin_cls: type) -> tuple[str, str]:
    return (plugin_cls.__module__, plugin_cls.__qualname__)


def _bump_revision() -> None:
    global _registry_revision
    _registry_revision += 1


@overload
def register_metadata_reader_plugin(plugin_cls: ReaderPluginT, *, replace: bool = False) -> ReaderPluginT: ...


@overload
def register_metadata_reader_plugin(
    plugin_cls: None = None,
    *,
    replace: bool = False,
) -> Callable[[ReaderPluginT], ReaderPluginT]: ...


def register_metadata_reader_plugin(plugin_cls=None, *, replace: bool = False):
    """
    Register an additional metadata-reader plugin class.

    Can be used directly or as a class decorator. Registered plugins are layered
    after builtin readers and are visible through the file-source dispatcher.
    """

    def _register(cls):
        _validate_reader_plugin(cls)
        identity = _plugin_identity(cls)
        existing_index = next(
            (idx for idx, existing in enumerate(_registered_reader_plugins) if _plugin_identity(existing) == identity),
            None,
        )
        if existing_index is not None:
            if not replace:
                raise ValueError(f"metadata reader plugin is already registered: {cls.__module__}.{cls.__qualname__}")
            _registered_reader_plugins[existing_index] = cls
        else:
            _registered_reader_plugins.append(cls)
        _bump_revision()
        return cls

    if plugin_cls is None:
        return _register
    return _register(plugin_cls)


def unregister_metadata_reader_plugin(plugin_cls: type) -> None:
    """Remove a plugin previously added with register_metadata_reader_plugin()."""
    identity = _plugin_identity(plugin_cls)
    original_len = len(_registered_reader_plugins)
    _registered_reader_plugins[:] = [
        existing for existing in _registered_reader_plugins if _plugin_identity(existing) != identity
    ]
    if len(_registered_reader_plugins) != original_len:
        _bump_revision()


def clear_registered_metadata_reader_plugins() -> None:
    """Remove runtime-registered metadata readers while keeping builtin readers."""
    if _registered_reader_plugins:
        _registered_reader_plugins.clear()
        _bump_revision()


def reset_metadata_reader_registry(*, reload_builtins: bool = False) -> None:
    """
    Reset runtime registrations, optionally forcing builtin reader rediscovery.

    This is intended for tests and plugin-loader lifecycle boundaries.
    """
    global _builtin_reader_plugins
    if _registered_reader_plugins:
        _registered_reader_plugins.clear()
        _bump_revision()
    if reload_builtins:
        _builtin_reader_plugins = None
        _bump_revision()


def get_metadata_reader_registry_revision() -> int:
    return _registry_revision


def get_metadata_reader_plugins() -> tuple[type, ...]:
    """Return builtin and runtime-registered metadata-reader plugin classes."""
    return _load_builtin_reader_plugins() + tuple(_registered_reader_plugins)


def iter_metadata_reader_entries() -> tuple[MetadataReaderEntry, ...]:
    entries = []
    for plugin_cls in get_metadata_reader_plugins():
        file_types = tuple(str(file_type).lower().lstrip(".") for file_type in getattr(plugin_cls, "file_types", ()))
        normalized_file_types = tuple(dict.fromkeys(normalize_file_type(file_type) for file_type in file_types))
        entries.append(
            MetadataReaderEntry(
                plugin_cls=plugin_cls,
                file_types=file_types,
                normalized_file_types=normalized_file_types,
                inplace_run_cost=str(getattr(plugin_cls, "inplace_run_cost", "high")).lower(),
            )
        )
    return tuple(entries)


def iter_metadata_reader_entries_for_extension(ext: str) -> tuple[MetadataReaderEntry, ...]:
    normalized_ext = normalize_file_type(ext)
    return tuple(entry for entry in iter_metadata_reader_entries() if normalized_ext in entry.normalized_file_types)


def known_metadata_file_types() -> frozenset[str]:
    known: set[str] = set()
    for entry in iter_metadata_reader_entries():
        known.update(entry.normalized_file_types)
    return frozenset(known)


__all__ = [
    "MetadataReaderEntry",
    "clear_registered_metadata_reader_plugins",
    "get_metadata_reader_plugins",
    "get_metadata_reader_registry_revision",
    "iter_metadata_reader_entries",
    "iter_metadata_reader_entries_for_extension",
    "known_metadata_file_types",
    "normalize_file_type",
    "register_metadata_reader_plugin",
    "reset_metadata_reader_registry",
    "unregister_metadata_reader_plugin",
]
