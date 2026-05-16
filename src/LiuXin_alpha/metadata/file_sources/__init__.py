"""
Metadata source dispatcher.

Historically this module implemented an ad-hoc plugin loader that scanned files
in this package. The modern path uses registered metadata-reader plugins from
`LiuXin_alpha.customize.builtins.metadata_readers`, while preserving the public
helpers used by legacy call sites.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from LiuXin_alpha.metadata.file_sources.registry import (
    get_metadata_reader_plugins,
    get_metadata_reader_registry_revision,
    known_metadata_file_types,
    normalize_file_type,
    register_metadata_reader_plugin,
    unregister_metadata_reader_plugin,
)
from LiuXin_alpha.utils.logging import default_log

__folder__ = os.path.realpath(os.path.dirname(__file__))

# Backwards-compatible globals kept for callers that inspect this module.
valid_plugins: list["MetaDataReaderPlugin"] = []
valid_file_formats: set[str] = set()
_loaded_registry_revision = -1


class InvalidMetadataExtractor(Exception):
    pass


@dataclass
class MetaDataReaderPlugin:
    """
    Small compatibility adapter around builtin metadata-reader plugin classes.
    """

    plugin_cls: type

    @property
    def module_name(self) -> str:
        return self.plugin_cls.__name__

    @property
    def file_path(self) -> str:
        module = self.plugin_cls.__module__.replace(".", "/")
        return f"{module}.py"

    @property
    def VALID_FOR(self) -> list[str]:
        return [x.upper() for x in getattr(self.plugin_cls, "file_types", [])]

    @property
    def PRIORITY_FOR(self) -> list[str]:
        # Legacy loaders expected this field; file types were commonly reused.
        return self.VALID_FOR

    @property
    def RUN_COST(self) -> list[str]:
        cost = str(getattr(self.plugin_cls, "inplace_run_cost", "high")).upper()
        return [cost]

    def get_metadata(self, target_object, force_type: str | None = None):
        return _run_metadata_reader(self.plugin_cls, target_object, ftype=force_type)


def _normalize_ext(raw_ext: str | None) -> str:
    return normalize_file_type(raw_ext)


def _target_path_hint(target_object) -> str | None:
    if isinstance(target_object, os.PathLike):
        return os.fspath(target_object)
    if isinstance(target_object, str):
        return target_object
    name = getattr(target_object, "name", None)
    if isinstance(name, str) and name:
        return name
    return None


def _resolve_extension(target_object, force_type: str | bool | None = None) -> str:
    if force_type:
        return _normalize_ext(str(force_type))

    source_path = _target_path_hint(target_object)
    dotted_ext = os.path.splitext(source_path or "")[1]
    ext = dotted_ext[1:] if dotted_ext.startswith(".") else dotted_ext
    ext = _normalize_ext(ext)
    if not ext:
        raise ValueError("Could not infer extension for metadata extraction. Pass force_type to override.")
    return ext


def _is_path_like(target_object) -> bool:
    return isinstance(target_object, (str, bytes, os.PathLike))


def _run_metadata_reader(plugin_cls: type, target_object, *, ftype: str):
    plugin = plugin_cls(None)
    if _is_path_like(target_object):
        path = os.fspath(target_object)
        if hasattr(plugin, "get_metadata_inplace"):
            return plugin.get_metadata_inplace(path, ftype)
        with open(path, "rb") as stream:
            return plugin.get_metadata(stream=stream, ftype=ftype)

    if hasattr(target_object, "read"):
        return plugin.get_metadata(stream=target_object, ftype=ftype)

    raise TypeError("target_object must be a filesystem path or a readable binary stream.")


def sort_plugins_by_run_cost(plugins):
    """
    Sort plugin adapters from highest to lowest speed preference.
    """
    run_cost_dict = {"HIGH": 1, "MEDIUM": 2, "LOW": 3}

    sortable_index = []
    for plugin in plugins:
        plugin_cost = plugin.RUN_COST[0]
        if plugin_cost not in run_cost_dict:
            raise AssertionError(
                "Unrecognized run cost detected\n"
                f"Plugin name: {plugin.module_name}\n"
                f"Given RUN_COST: {plugin.RUN_COST!r}"
            )
        sortable_index.append((plugin, run_cost_dict[plugin_cost]))

    sortable_index.sort(key=lambda x: x[1])
    return [item[0] for item in sortable_index]


def load_plugins():
    """
    Populate compatibility plugin adapters from builtin metadata-reader plugins.
    """
    global _loaded_registry_revision
    valid_plugins[:] = [MetaDataReaderPlugin(cls) for cls in get_metadata_reader_plugins()]
    valid_file_formats.clear()
    for plugin in valid_plugins:
        valid_file_formats.update(plugin.VALID_FOR)
    _loaded_registry_revision = get_metadata_reader_registry_revision()


def get_plugins_for_extension(ext: str):
    ext = _normalize_ext(ext).upper()
    if not valid_plugins or _loaded_registry_revision != get_metadata_reader_registry_revision():
        load_plugins()
    plugins = [plugin for plugin in valid_plugins if ext in plugin.VALID_FOR]
    return sort_plugins_by_run_cost(plugins)


def filter_plugin_sources(plugin_sources_names):
    """
    Legacy helper retained for compatibility.
    """
    plugin_sources_names = list(plugin_sources_names)
    plugin_sources_names = [name for name in plugin_sources_names if name != "__init__.py"]
    plugin_sources_names = [name for name in plugin_sources_names if not name.endswith(".pyc")]
    return plugin_sources_names


def get_metadata(target_object, force_type: str | bool = False):
    """
    Read metadata from a path or stream using registered metadata-reader plugins.
    """
    ext = _resolve_extension(target_object, force_type=force_type)
    plugins = get_plugins_for_extension(ext)
    if not plugins:
        raise InvalidMetadataExtractor(f"No metadata reader plugin is registered for extension: {ext!r}")

    errors: list[tuple[str, Exception]] = []
    for plugin in plugins:
        try:
            md = plugin.get_metadata(target_object, force_type=ext)
            if md is not None:
                return md
        except Exception as err:
            errors.append((plugin.module_name, err))
            default_log.log_exception(
                "Error while running metadata extractor plugin.",
                err,
                "DEBUG",
                ("plugin_name", plugin.module_name),
                ("plugin_path", plugin.file_path),
                ("extension", ext),
            )

    if errors:
        plugin_names = [name for name, _ in errors]
        raise RuntimeError(
            "Metadata extraction failed for extension %r. Tried plugins: %s"
            % (ext, ", ".join(plugin_names))
        ) from errors[-1][1]
    return None


__all__ = [
    "InvalidMetadataExtractor",
    "MetaDataReaderPlugin",
    "filter_plugin_sources",
    "get_metadata",
    "get_plugins_for_extension",
    "known_metadata_file_types",
    "load_plugins",
    "register_metadata_reader_plugin",
    "sort_plugins_by_run_cost",
    "unregister_metadata_reader_plugin",
]
