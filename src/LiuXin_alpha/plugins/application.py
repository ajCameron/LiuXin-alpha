"""Compose and manage the application's plugins explicitly.

Importing this API does not discover or initialize providers. Call
``initialize_plugins`` at host startup; application lookups initialize on first
use for compatibility. File-only operations belong to ``file_formats.plugin_runtime``.
This is the sole application registry owner; the customize namespace is retired.
"""

from __future__ import annotations

import os
import shutil
import sys
import traceback
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING

from LiuXin_alpha.file_formats import plugin_runtime
from LiuXin_alpha.plugins.archives import get_compressor_plugins
from LiuXin_alpha.plugins.contracts import (
    Archive,
    CatalogPlugin,
    EditBookToolPlugin,
    InvalidPlugin,
    LibraryClosedPlugin,
    MDInputTransform,
    Plugin,
    PreferencesPlugin,
    ViewerPlugin,
    platform,
)
from LiuXin_alpha.plugins.contracts import (
    InterfaceActionBase as InterfaceAction,
)
from LiuXin_alpha.plugins.contracts import (
    StoreBase as Store,
)
from LiuXin_alpha.plugins.loading import build_plugins
from LiuXin_alpha.plugins.zipplugin import loader
from LiuXin_alpha.utils.config.config_base import (
    make_config_dir,
    plugin_dir,
)
from LiuXin_alpha.utils.config.config_tools import OptionParser
from LiuXin_alpha.utils.localization import trans as _

try:
    from past.builtins import basestring
except ModuleNotFoundError:
    basestring = str

if TYPE_CHECKING:
    from LiuXin_alpha.plugins.builtins.standardization import (
        BaseNameGenerator,
        CreatorStandardize,
        TitlePhashHandler,
    )

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"


from LiuXin_alpha.file_formats.plugin_runtime import (
    ApplyNullMetadata as ApplyNullMetadata,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    ForceIdentifiers as ForceIdentifiers,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    QuickMetadata as QuickMetadata,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    all_input_formats as all_input_formats,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    apply_null_metadata as apply_null_metadata,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    available_input_formats as available_input_formats,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    available_output_formats as available_output_formats,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    bind_plugins as bind_plugins,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    can_set_metadata as can_set_metadata,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    force_identifiers as force_identifiers,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    get_file_type_metadata as get_file_type_metadata,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    input_format_plugins as input_format_plugins,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    input_profiles as input_profiles,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    metadata_readers as metadata_readers,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    metadata_writers as metadata_writers,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    output_format_plugins as output_format_plugins,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    output_profiles as output_profiles,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    plugin_for_input_format as plugin_for_input_format,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    plugin_for_output_format as plugin_for_output_format,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    quick_metadata as quick_metadata,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    reread_filetype_plugins as reread_filetype_plugins,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    reread_metadata_plugins as reread_metadata_plugins,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    run_import_plugins as run_import_plugins,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    run_plugins_on_import as run_plugins_on_import,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    run_plugins_on_postadd as run_plugins_on_postadd,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    run_plugins_on_postimport as run_plugins_on_postimport,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    run_plugins_on_postprocess as run_plugins_on_postprocess,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    run_plugins_on_preprocess as run_plugins_on_preprocess,
)
from LiuXin_alpha.file_formats.plugin_runtime import (
    set_file_type_metadata as set_file_type_metadata,
)
from LiuXin_alpha.plugins.configuration import (
    _config as _config,
)
from LiuXin_alpha.plugins.configuration import (
    config as config,
)
from LiuXin_alpha.plugins.configuration import (
    customize_plugin as customize_plugin,
)
from LiuXin_alpha.plugins.configuration import (
    default_disabled_plugins as default_disabled_plugins,
)
from LiuXin_alpha.plugins.configuration import (
    is_disabled as is_disabled,
)
from LiuXin_alpha.plugins.configuration import (
    plugin_customization as plugin_customization,
)


def builtin_plugins() -> list[type[Plugin]]:
    """Discover the application providers only when composition is requested."""
    from LiuXin_alpha.plugins.builtins.registry import plugins

    return list(plugins)


class NameConflict(ValueError):
    """
    Provide the nameconflict contract for validated ebook processing.

    Example:
        Exercise NameConflict through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py
    """

    pass


def find_plugin(name: str) -> Plugin | None:
    """
    Searches the initialized plugins for the plugin by name and returns it if it can be found.

    Example:
        Exercise find plugin through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for plugin in initialized_plugins():
        if plugin.name == name:
            return plugin


def load_plugin(path_to_zip_file: str | os.PathLike) -> Plugin:  # {{{
    """
    Load plugin from zip file or raise InvalidPlugin error

    Example:
        Exercise load plugin through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param path_to_zip_file: Value supplied for path to zip file under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return loader.load(path_to_zip_file)


# }}}

# Enable/disable plugins {{{


def disable_plugin(plugin_or_name: Plugin | str) -> None:
    """
    Pass in the plugin, or it's name, disable it and note it in preferences.

    Example:
        Exercise disable plugin through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param plugin_or_name: Value supplied for plugin or name under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    x = getattr(plugin_or_name, "name", plugin_or_name)
    plugin = find_plugin(x)
    if not plugin.can_be_disabled:
        raise ValueError(f"Plugin {x} cannot be disabled")
    dp = config["disabled_plugins"]
    dp.add(x)
    config["disabled_plugins"] = dp
    ep = config["enabled_plugins"]
    if x in ep:
        ep.remove(x)
    config["enabled_plugins"] = ep


def enable_plugin(plugin_or_name: Plugin | str) -> None:
    """
    Pass in the plugin - or it's name - enable it and note it in preferences.

    Example:
        Exercise enable plugin through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param plugin_or_name: Value supplied for plugin or name under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    x = getattr(plugin_or_name, "name", plugin_or_name)
    dp = config["disabled_plugins"]
    if x in dp:
        dp.remove(x)
    config["disabled_plugins"] = dp
    ep = config["enabled_plugins"]
    ep.add(x)
    config["enabled_plugins"] = ep


def restore_plugin_state_to_default(plugin_or_name: Plugin | str) -> None:
    """
    If the plugin defaults enabled - enable it and visa versa.

    Example:
        Exercise restore plugin state to default through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param plugin_or_name: Value supplied for plugin or name under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    x = getattr(plugin_or_name, "name", plugin_or_name)
    dp = config["disabled_plugins"]
    if x in dp:
        dp.remove(x)
    config["disabled_plugins"] = dp
    ep = config["enabled_plugins"]
    if x in ep:
        ep.remove(x)
    config["enabled_plugins"] = ep


# Plugins which should start off as disabled


# }}}

# File type plugins {{{


# ------------------------------------------------------
# - Public interfaces for the plugins system
# ------------------------------------------------------

# Runs plugins before importing a file

# Runs plugins before converting? a file

# Runs plugins after converting? a file


# Todo: Check these are actually the same in calibre - there might be a meaningful semantic difference

# }}}


# Plugin customization {{{


# }}}


# Input/Output profiles {{{


# }}}


# Interface Actions # {{{
def interface_actions() -> Iterator[InterfaceAction]:
    """
    Yields all interface actions.

    Example:
        Exercise interface actions through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: An iterator yielding the normalized values described above.
    """
    customization = config["plugin_customization"]
    for plugin in initialized_plugins():
        if isinstance(plugin, InterfaceAction):
            if not is_disabled(plugin):
                plugin.site_customization = customization.get(plugin.name, "")
                yield plugin


# }}}

# Preferences Plugins # {{{


def preferences_plugins() -> Iterator[PreferencesPlugin]:
    """
    Yields all preferences plugins.

    Example:
        Exercise preferences plugins through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: An iterator yielding the normalized values described above.
    """
    customization = config["plugin_customization"]
    for plugin in initialized_plugins():
        if isinstance(plugin, PreferencesPlugin):
            if not is_disabled(plugin):
                plugin.site_customization = customization.get(plugin.name, "")
                yield plugin


# }}}

# Store Plugins # {{{


def store_plugins() -> Iterator[Store]:
    """
    Yields all store plugins.

    Example:
        Exercise store plugins through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: An iterator yielding the normalized values described above.
    """
    customization = config["plugin_customization"]
    for plugin in initialized_plugins():
        if isinstance(plugin, Store):
            plugin.site_customization = customization.get(plugin.name, "")
            yield plugin


def available_store_plugins() -> Iterator[Store]:
    """
    Yields all store plugins which have not been disabled.

    Example:
        Exercise available store plugins through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: An iterator yielding the normalized values described above.
    """
    for plugin in store_plugins():
        if not is_disabled(plugin):
            yield plugin


def stores() -> set[Store]:
    """
    Returns a set of all store plugins - including currently disabled ones.

    Example:
        Exercise stores through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    stores = set([])
    for plugin in store_plugins():
        stores.add(plugin.name)
    return stores


def available_stores() -> set[Store]:
    """
    Returns a set of all the available store plugins.

    Example:
        Exercise available stores through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    stores = set([])
    for plugin in available_store_plugins():
        stores.add(plugin.name)
    return stores


# }}}

# Metadata read/write {{{


# Todo: Swallows exceptions


# Todo: Swallows exceptions


# Todo: Insert MetaData synthesis call here


# Todo: Only feed this method unambiguous metadata - re-institute the calibre_metadata class and feed it in here


# }}}

# Add/remove plugins {{{


def add_plugin(path_to_zip_file: str | os.PathLike) -> Plugin:
    """
    Add a plugin from a zip file.

    Example:
        Exercise add plugin through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param path_to_zip_file: Value supplied for path to zip file under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    make_config_dir()

    plugin = load_plugin(path_to_zip_file)

    if plugin.name in {provider.name for provider in builtin_plugins()}:
        raise NameConflict(
            f"A builtin plugin with the name {plugin.name!r} already exists"
        )

    plugin = initialize_plugin(plugin, path_to_zip_file)
    plugins = config["plugins"]

    # Plugins will be stored in the plugin_dir
    zfp = os.path.join(plugin_dir, plugin.name + ".zip")
    if os.path.exists(zfp):
        os.remove(zfp)
    shutil.copyfile(path_to_zip_file, zfp)

    plugins[plugin.name] = zfp
    config["plugins"] = plugins

    initialize_plugins()

    return plugin


def remove_plugin(plugin_or_name: Plugin | str) -> bool:
    """
    Takes a plugin or the name of the plugin - removes it from the active plugins.

    Example:
        Exercise remove plugin through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param plugin_or_name: Value supplied for plugin or name under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    name = getattr(plugin_or_name, "name", plugin_or_name)
    plugins = config["plugins"]
    removed = False
    if name in plugins:
        removed = True
        try:
            zfp = os.path.join(plugin_dir, name + ".zip")
            if os.path.exists(zfp):
                os.remove(zfp)
            zfp = plugins[name]
            if os.path.exists(zfp):
                os.remove(zfp)
        except OSError:
            pass
        plugins.pop(name)
    config["plugins"] = plugins
    initialize_plugins()
    return removed


# }}}

# Input/Output format plugins {{{


# }}}

# Catalog plugins {{{


def catalog_plugins() -> Iterator[CatalogPlugin]:
    """
    Iterator which yields all the catalog plugins

    Example:
        Exercise catalog plugins through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: An iterator yielding the normalized values described above.
    """
    for plugin in initialized_plugins():
        if isinstance(plugin, CatalogPlugin):
            yield plugin


def available_catalog_formats() -> set[str]:
    """
    Which catalog formats are currently available?

    Example:
        Exercise available catalog formats through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    formats = set([])
    for plugin in catalog_plugins():
        if not is_disabled(plugin):
            for format in plugin.file_types:
                formats.add(format)
    return formats


def plugin_for_catalog_format(fmt: str) -> Plugin | None:
    """
    Returns the catalog plugin for that format.

    Example:
        Exercise plugin for catalog format through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param fmt: Date, number or template format specification.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for plugin in catalog_plugins():
        if fmt.lower() in plugin.file_types:
            return plugin


# }}}


# Device plugins {{{
def device_plugins(include_disabled: bool = False) -> Iterator[Plugin]:
    """
    Returns an iterator over all the devices that the program can talk to, initializing them if required.

    Example:
        Exercise device plugins through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param include_disabled: Value supplied for include disabled under the utility
        contract.
    :return: An iterator yielding the normalized values described above.
    """

    try:
        from LiuXin_alpha.devices.interface import DevicePlugin
    except ModuleNotFoundError as exc:
        if exc.name not in {"LiuXin_alpha.devices", "LiuXin_alpha.devices.interface"}:
            raise
        return

    for plugin in initialized_plugins():
        if isinstance(plugin, DevicePlugin):
            if include_disabled or not is_disabled(plugin):
                if platform in plugin.supported_platforms:
                    if getattr(plugin, "plugin_needs_delayed_initialization", False):
                        plugin.do_delayed_plugin_initialization()
                    yield plugin


def disabled_device_plugins() -> Iterator[Plugin]:
    """
    Device plugins which are currently disabled.

    Example:
        Exercise disabled device plugins through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: An iterator yielding the normalized values described above.
    """

    try:
        from LiuXin_alpha.devices.interface import DevicePlugin
    except ModuleNotFoundError as exc:
        if exc.name not in {"LiuXin_alpha.devices", "LiuXin_alpha.devices.interface"}:
            raise
        return

    for plugin in initialized_plugins():
        if isinstance(plugin, DevicePlugin):
            if is_disabled(plugin):
                if platform in plugin.supported_platforms:
                    yield plugin


# }}}


# Metadata sources2 {{{
def metadata_plugins(capabilities: Iterable[str]) -> Iterator[Plugin]:
    """
    Returns an iterator of all plugins which have a certain capability.

    Example:
        Exercise metadata plugins through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param capabilities: Value supplied for capabilities under the utility contract.
    :return: An iterator yielding the normalized values described above.
    """
    capabilities = frozenset(capabilities)
    for plugin in all_metadata_plugins():
        if plugin.capabilities.intersection(capabilities) and not is_disabled(plugin):
            yield plugin


def all_metadata_plugins() -> Iterator[Plugin]:
    """
    Yields all metadata source plugins.

    Example:
        Exercise all metadata plugins through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: An iterator yielding the normalized values described above.
    """

    from LiuXin_alpha.metadata.web_sources.base import Source

    for plugin in initialized_plugins():
        if isinstance(plugin, Source):
            yield plugin


# }}}


# Viewer plugins {{{
# Todo: Where are these? Should probably be moved to GUI
def all_viewer_plugins() -> Iterator[ViewerPlugin]:
    """
    Plugins for the document viewer to enable it to read different file formats

    Example:
        Exercise all viewer plugins through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: An iterator yielding the normalized values described above.
    """
    for plugin in initialized_plugins():
        if isinstance(plugin, ViewerPlugin):
            yield plugin


# }}}


# Editor plugins {{{
# Todo: Likewise, where are these? Should probably be moved to GUI
def all_edit_book_tool_plugins() -> Iterator[EditBookToolPlugin]:
    """
    Tools to enable editing books from the viewer.

    Example:
        Exercise all edit book tool plugins through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: An iterator yielding the normalized values described above.
    """
    for plugin in initialized_plugins():
        if isinstance(plugin, EditBookToolPlugin):
            yield plugin


# }}}


# Library Closed Plugins # {{{
def available_library_closed_plugins() -> Iterator[LibraryClosedPlugin]:
    """
    Get the currently enabled LibraryClosedPlugins.

    Example:
        Exercise available library closed plugins through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: An iterator yielding the normalized values described above.
    """
    customization = config["plugin_customization"]
    for plugin in initialized_plugins():
        if isinstance(plugin, LibraryClosedPlugin):
            if not is_disabled(plugin):
                plugin.site_customization = customization.get(plugin.name, "")
                yield plugin


def has_library_closed_plugins() -> bool:
    """
    Check to see if there are any enabled LibraryClosedPlugin in the system.

    Example:
        Exercise has library closed plugins through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: True when the documented condition holds; otherwise False.
    """
    for plugin in initialized_plugins():
        if isinstance(plugin, LibraryClosedPlugin):
            if not is_disabled(plugin):
                return True
    return False


# }}}


# MD transform plugins {{{
def all_metadata_synthesis_plugins() -> Iterator[MDInputTransform]:
    """
    All plugins which are used to transform MetaData.

    Example:
        Exercise all metadata synthesis plugins through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: An iterator yielding the normalized values described above.
    """
    for plugin in initialized_plugins():
        if isinstance(plugin, MDInputTransform):
            yield plugin


# }}}


# Initialize plugins {{{
_initialized_plugins: tuple[Plugin, ...] | None = None
_initializing = False


def initialize_plugin(
    plugin: type[Plugin], path_to_zip_file: str | os.PathLike | None
) -> Plugin:
    """
    Initialize a given plugin.

    Example:
        Exercise initialize plugin through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param plugin: Value supplied for plugin under the utility contract.
    :param path_to_zip_file: Value supplied for path to zip file under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        p = plugin(path_to_zip_file)
        p.initialize()
        return p
    except Exception as exc:
        print("Failed to initialize plugin:", plugin.name, plugin.version)
        tb = traceback.format_exc()
        raise InvalidPlugin(
            (_("Initialization of plugin %s failed with traceback:") % tb) + "\n" + tb
        ) from exc


def has_external_plugins() -> bool:
    """
    True if there are updatable (zip file based) plugins.

    Example:
        Exercise has external plugins through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: True when the documented condition holds; otherwise False.
    """
    return bool(config["plugins"])


def initialize_plugins(perf: bool = False) -> None:
    """Refresh the host registry and bind its exact instances into file dispatch.

    Priority is descending, with installed providers first on ties. Conflicting
    installed names are skipped without deleting archives or changing settings.
    Reentrant lookups see the prior host snapshot (empty on first startup).
    An interrupted refresh preserves the previously bound file registry.
    """
    global _initialized_plugins, _initializing
    with plugin_runtime.initialization_scope():
        if _initializing:
            return
        _initializing = True
        try:
            plugins = plugin_runtime.replace_plugins(
                lambda: build_plugins(
                    builtin_plugins(), config["plugins"], plugin_dir, perf=perf
                )
            )
            _initialized_plugins = tuple(plugins)
        finally:
            _initializing = False


def initialized_plugins() -> Iterator[Plugin]:
    """Return a stable snapshot, initializing the application on first lookup."""
    with plugin_runtime.initialization_scope():
        if _initialized_plugins is None and not _initializing:
            initialize_plugins()
        plugins = _initialized_plugins or ()
    yield from plugins


# }}}

# ----------------------------------------------------------------------------------------------------------------------
#
# - ADDITIONS FOR LIUXIN ONLY PLUGINS START HERE


def extract_metadata(file_path: str | os.PathLike):
    """
    Takes a file path or a stream - extract metadata from it using all available plugins.

    Example:
        Exercise extract metadata through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param file_path: Value supplied for file path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    pass


def synthesize_metadata(metadata):
    """
    Takes a collection of metadata objects (which can be a single object). Feeds them through the plugins and returns.

    Example:
        Exercise synthesize metadata through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param metadata: Value supplied for metadata under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    pass


def creator_standardization_plugin() -> type[CreatorStandardize]:
    """
    Returns the creator standardization plugin for the system.

    Example:
        Exercise creator standardization plugin through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    from LiuXin_alpha.plugins.builtins.standardization import CreatorStandardize

    return CreatorStandardize


def title_phash_handler() -> type[TitlePhashHandler]:
    """
    Prepares and returns the title_phash_handler plugin for the system,

    Example:
        Exercise title phash handler through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    from LiuXin_alpha.plugins.builtins.standardization import TitlePhashHandler

    # Overwrite the default creator_standardize method with the actual one
    cs_plugin = creator_standardization_plugin()
    TitlePhashHandler.creator_standardize = cs_plugin.standardize_creator

    return TitlePhashHandler


def name_generator() -> type[BaseNameGenerator]:
    """
    Returns the name generator plugin - which is used to create file and folder names.

    Example:
        Exercise name generator through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    from LiuXin_alpha.plugins.builtins.standardization import BaseNameGenerator

    return BaseNameGenerator


def get_compressor_plugin(read: bool = True, arc_type: str = "zip") -> type[Archive]:
    """
    Returns a compressor plugin suitable for handling an archive of a particular type.

    Example:
        Exercise get compressor plugin through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param read: Value supplied for read under the utility contract.
    :param arc_type: Value supplied for arc type under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    compressor_plugins = get_compressor_plugins()
    if read:
        for plugin in compressor_plugins:
            if arc_type in plugin.read_formats:
                return plugin
    else:
        for plugin in compressor_plugins:
            if arc_type in plugin.write_formats:
                return plugin

    err_str = "Cannot find a plugin to handle the archive of the required type in the required way\n"
    err_str += f"read: {read}\n"
    err_str += f"arc_type: {arc_type}\n"
    raise NotImplementedError(err_str)


#
# ----------------------------------------------------------------------------------------------------------------------

# CLI {{{


def build_plugin(path: str) -> None:
    """
    Adds a plugin to LiuXin after preforming some basic checks.

    Example:
        Exercise build plugin through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.calibre.ptempfile import PersistentTemporaryFile
    from LiuXin_alpha.utils.calibre_utils.calibre_zipfile import ZIP_STORED, ZipFile
    from LiuXin_alpha.utils.logging import prints

    path = str(path)
    names = frozenset(os.listdir(path))
    if "__init__.py" not in names:
        prints(path, " is not a valid plugin")
        raise SystemExit(1)
    t = PersistentTemporaryFile(".zip")
    with ZipFile(t, "w", ZIP_STORED) as zf:
        zf.add_dir(path, simple_filter=lambda x: x in {".git", ".bzr", ".svn", ".hg"})
    t.close()
    plugin = add_plugin(t.name)
    os.remove(t.name)
    prints("Plugin updated:", plugin.name, plugin.version)


def option_parser() -> OptionParser:
    """
    Provides a command line interface to this module.

    Example:
        Exercise option parser through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    parser = OptionParser(
        usage=_(
            """\
    %prog options

    Customize calibre by loading external plugins.
    """
        )
    )
    parser.add_option(
        "-a",
        "--add-plugin",
        default=None,
        help=_("Add a plugin by specifying the path to the zip file containing it."),
    )
    parser.add_option(
        "-b",
        "--build-plugin",
        default=None,
        help=_(
            "For plugin developers: Path to the directory where you are"
            " developing the plugin. This command will automatically zip "
            "up the plugin and update it in calibre."
        ),
    )
    parser.add_option(
        "-r",
        "--remove-plugin",
        default=None,
        help=_("Remove a custom plugin by name. Has no effect on builtin plugins"),
    )
    parser.add_option(
        "--customize-plugin",
        default=None,
        help=_(
            "Customize plugin. Specify name of plugin and customization string separated by a comma."
        ),
    )
    parser.add_option(
        "-l",
        "--list-plugins",
        default=False,
        action="store_true",
        help=_("List all installed plugins"),
    )
    parser.add_option(
        "--enable-plugin", default=None, help=_("Enable the named plugin")
    )
    parser.add_option(
        "--disable-plugin", default=None, help=_("Disable the named plugin")
    )
    return parser


def main(args=sys.argv) -> int:
    """
    Various options to check the loaded plugins or build ones.

    Example:
        Exercise main through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param args: Positional values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    parser = option_parser()
    if len(args) < 2:
        parser.print_help()
        return 1
    opts, args = parser.parse_args(args)
    if opts.add_plugin is not None:
        plugin = add_plugin(opts.add_plugin)
        print("Plugin added:", plugin.name, plugin.version)
    if opts.build_plugin is not None:
        build_plugin(opts.build_plugin)
    if opts.remove_plugin is not None:
        if remove_plugin(opts.remove_plugin):
            print("Plugin removed")
        else:
            print("No custom plugin named", opts.remove_plugin)
    if opts.customize_plugin is not None:
        name, custom = opts.customize_plugin.split(",")
        plugin = find_plugin(name.strip())
        if plugin is None:
            print(f"No plugin with the name {name} exists")
            return 1
        customize_plugin(plugin, custom)
    if opts.enable_plugin is not None:
        enable_plugin(opts.enable_plugin.strip())
    if opts.disable_plugin is not None:
        disable_plugin(opts.disable_plugin.strip())
    if opts.list_plugins:
        fmt = "%-15s%-20s%-15s%-15s%s"
        print(fmt % tuple("Type|Name|Version|Disabled|Site Customization".split("|")))
        print()
        for plugin in initialized_plugins():
            print(
                fmt
                % (
                    plugin.plugin_type,
                    plugin.name,
                    plugin.version,
                    is_disabled(plugin),
                    plugin_customization(plugin),
                )
            )
            print("\t", plugin.description)
            if plugin.is_customizable():
                try:
                    print("\t", plugin.customization_help())
                except NotImplementedError:
                    pass
            print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
# }}}
