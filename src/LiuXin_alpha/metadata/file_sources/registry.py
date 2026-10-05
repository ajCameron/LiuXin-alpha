"""
Maintain the built-in and runtime metadata-reader plugin registry with normalized file types and revision tracking.

The module keeps malformed-input, optional dependency and resource ownership
behavior explicit for registry callers.

Example:
    Exercise registry with pytest::

        python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, TypeVar, overload


ReaderPluginT = TypeVar("ReaderPluginT", bound=type)

_builtin_reader_plugins: tuple[type, ...] | None = None
_registered_reader_plugins: list[type] = []
_registry_revision = 0


@dataclass(frozen=True)
class MetadataReaderEntry:
    """
    Pair a validated metadata-reader plugin class with one normalized supported extension.

    Example:
        Exercise MetadataReaderEntry with pytest::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py
    """

    plugin_cls: type
    file_types: tuple[str, ...]
    normalized_file_types: tuple[str, ...]
    inplace_run_cost: str

    @property
    def name(self) -> str:
        """
        Implement name for the bound metadata helper while preserving its container invariants.

        Example:
            Exercise MetadataReaderEntry.name with pytest::

                python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


        :return: Parsed, normalized or serialized value described above.
        """
        return self.plugin_cls.__name__


def normalize_file_type(raw_file_type: str | None) -> str:
    """
    Normalize a file-type label by trimming separators, case-folding and applying aliases.

    Example:
        Exercise normalize file type with pytest::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :param raw_file_type: Value supplied for raw file type.
    :return: Parsed, normalized or serialized value described above.
    """
    ext = (raw_file_type or "").lower().lstrip(".")
    if ext in {"html", "htm", "xhtml", "xhtm", "xml"}:
        return "html"
    if ext in {"mobi", "prc", "azw"}:
        return "mobi"
    if ext in {"odt", "ods", "odp", "odg", "odf"}:
        return "odt"
    return ext


def _load_builtin_reader_plugins() -> tuple[type, ...]:
    """
    Perform the format-specific load builtin reader plugins operation used by this metadata source.

    Example:
        Exercise  load builtin reader plugins with pytest::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :return: Parsed, normalized or serialized value described above.
    """
    global _builtin_reader_plugins
    if _builtin_reader_plugins is None:
        from LiuXin_alpha.customize.builtins.metadata_readers import (
            get_metadata_reader_plugins as get_builtin_metadata_reader_plugins,
        )

        _builtin_reader_plugins = tuple(get_builtin_metadata_reader_plugins())
    return _builtin_reader_plugins


def _validate_reader_plugin(plugin_cls: type) -> None:
    """
    Perform the format-specific validate reader plugin operation used by this metadata source.

    Example:
        Exercise  validate reader plugin with pytest::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :param plugin_cls: Metadata-reader plugin class to validate, register or remove.
    :return: Parsed, normalized or serialized value described above.
    """
    if not isinstance(plugin_cls, type):
        raise TypeError("metadata reader plugin must be a class.")
    file_types = getattr(plugin_cls, "file_types", None)
    if not file_types:
        raise ValueError("metadata reader plugin must declare at least one file type.")
    if not callable(getattr(plugin_cls, "get_metadata", None)):
        raise TypeError("metadata reader plugin must define get_metadata().")


def _plugin_identity(plugin_cls: type) -> tuple[str, str]:
    """
    Perform the format-specific plugin identity operation used by this metadata source.

    Example:
        Exercise  plugin identity with pytest::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :param plugin_cls: Metadata-reader plugin class to validate, register or remove.
    :return: Parsed, normalized or serialized value described above.
    """
    return (plugin_cls.__module__, plugin_cls.__qualname__)


def _bump_revision() -> None:
    """
    Perform the format-specific bump revision operation used by this metadata source.

    Example:
        Exercise  bump revision with pytest::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :return: None.
    """
    global _registry_revision
    _registry_revision += 1


@overload
def register_metadata_reader_plugin(plugin_cls: ReaderPluginT, *, replace: bool = False) -> ReaderPluginT:
    """
    Register or decorate a validated reader plugin, optionally replacing the same identity.

    Example:
        Exercise register metadata reader plugin with pytest::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :param plugin_cls: Metadata-reader plugin class to validate, register or remove.
    :param replace: Policy flag controlling the behavior described above.
    :return: Parsed, normalized or serialized value described above.
    """
    ...


@overload
def register_metadata_reader_plugin(
    plugin_cls: None = None,
    *,
    replace: bool = False,
) -> Callable[[ReaderPluginT], ReaderPluginT]:
    """
    Register or decorate a validated reader plugin, optionally replacing the same identity.

    Example:
        Exercise register metadata reader plugin with pytest::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :param plugin_cls: Metadata-reader plugin class to validate, register or remove.
    :param replace: Policy flag controlling the behavior described above.
    :return: Parsed, normalized or serialized value described above.
    """
    ...


def register_metadata_reader_plugin(plugin_cls=None, *, replace: bool = False):
    """
    Register or decorate a validated reader plugin, optionally replacing the same identity.

    Example:
        Exercise register metadata reader plugin with pytest::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :param plugin_cls: Metadata-reader plugin class to validate, register or remove.
    :param replace: Policy flag controlling the behavior described above.
    :return: Parsed, normalized or serialized value described above.
    """

    def _register(cls):
        """
        Perform the format-specific register operation used by this metadata source.

        Example:
            Exercise register metadata reader plugin. register with pytest::

                python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


        :param cls: Value supplied for cls.
        :return: Parsed, normalized or serialized value described above.
        """
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
    """
    Remove a runtime reader plugin and advance the registry revision when present.

    Example:
        Exercise unregister metadata reader plugin with pytest::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :param plugin_cls: Metadata-reader plugin class to validate, register or remove.
    :return: None.
    """
    identity = _plugin_identity(plugin_cls)
    original_len = len(_registered_reader_plugins)
    _registered_reader_plugins[:] = [
        existing for existing in _registered_reader_plugins if _plugin_identity(existing) != identity
    ]
    if len(_registered_reader_plugins) != original_len:
        _bump_revision()


def clear_registered_metadata_reader_plugins() -> None:
    """
    Remove all runtime reader plugins and advance the revision only when state changes.

    Example:
        Exercise clear registered metadata reader plugins with pytest::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :return: None.
    """
    if _registered_reader_plugins:
        _registered_reader_plugins.clear()
        _bump_revision()


def reset_metadata_reader_registry(*, reload_builtins: bool = False) -> None:
    """
    Clear runtime plugins and optionally invalidate the cached built-in plugin tuple.

    Example:
        Exercise reset metadata reader registry with pytest::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :param reload_builtins: Policy flag controlling the behavior described above.
    :return: None.
    """
    global _builtin_reader_plugins
    if _registered_reader_plugins:
        _registered_reader_plugins.clear()
        _bump_revision()
    if reload_builtins:
        _builtin_reader_plugins = None
        _bump_revision()


def get_metadata_reader_registry_revision() -> int:
    """
    Return metadata reader registry revision from current parser, container or registry state.

    Example:
        Exercise get metadata reader registry revision with pytest::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :return: Parsed, normalized or serialized value described above.
    """
    return _registry_revision


def get_metadata_reader_plugins() -> tuple[type, ...]:
    """
    Return metadata reader plugins from current parser, container or registry state.

    Example:
        Exercise get metadata reader plugins with pytest::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :return: Parsed, normalized or serialized value described above.
    """
    return _load_builtin_reader_plugins() + tuple(_registered_reader_plugins)


def iter_metadata_reader_entries() -> tuple[MetadataReaderEntry, ...]:
    """
    Return deterministic plugin/extension entries for built-in and runtime readers.

    Example:
        Exercise iter metadata reader entries with pytest::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :return: Parsed, normalized or serialized value described above.
    """
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
    """
    Return reader entries that accept the normalized requested extension.

    Example:
        Exercise iter metadata reader entries for extension with pytest::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :param ext: Name, type or encoding selector used for lookup or interpretation.
    :return: Parsed, normalized or serialized value described above.
    """
    normalized_ext = normalize_file_type(ext)
    return tuple(entry for entry in iter_metadata_reader_entries() if normalized_ext in entry.normalized_file_types)


def known_metadata_file_types() -> frozenset[str]:
    """
    Return all normalized extensions currently advertised by reader plugins.

    Example:
        Exercise known metadata file types with pytest::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :return: Parsed, normalized or serialized value described above.
    """
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
