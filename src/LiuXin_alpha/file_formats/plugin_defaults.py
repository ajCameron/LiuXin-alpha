"""Compose the standalone file-plugin fleet from explicit domain-owned providers.

Application-only plugins are composed by their host. Installed ZIP plugins that
implement file hooks, metadata I/O, conversion, or profiles participate here with
the same persisted customization and enable/disable settings.
"""

from __future__ import annotations

import os
import sys
import traceback

from LiuXin_alpha.plugin_system.base import Plugin
from LiuXin_alpha.plugin_system.configuration import config
from LiuXin_alpha.utils.config.config_base import plugin_dir
from LiuXin_alpha.utils.logging import default_log


def builtin_plugins() -> list[type[Plugin]]:
    """Return bundled file-plugin classes in their historical composition order."""
    from LiuXin_alpha.file_formats.conversion.builtins import (
        get_input_plugins,
        get_output_plugins,
    )
    from LiuXin_alpha.file_formats.conversion.profiles import (
        input_profiles,
        output_profiles,
    )
    from LiuXin_alpha.file_formats.import_plugins import get_file_type_plugins
    from LiuXin_alpha.metadata.file_sources.builtin_readers import (
        get_metadata_reader_plugins,
    )
    from LiuXin_alpha.metadata.file_sources.builtin_writers import (
        get_metadata_set_plugins,
    )

    optional: list[type[Plugin]] = []
    try:
        from LiuXin_alpha.file_formats.html.to_zip import HTML2ZIP
    except Exception as exc:
        default_log.log_exception("Failed to import HTML2ZIP", exc, "DEBUG")
    else:
        optional.append(HTML2ZIP)
    try:
        from LiuXin_alpha.metadata.file_sources.archive import ArchiveExtract
    except Exception as exc:
        default_log.log_exception(
            "Failed to import archive metadata plugins", exc, "DEBUG"
        )
    else:
        optional.append(ArchiveExtract)

    return [
        *get_file_type_plugins(),
        *optional,
        *get_metadata_reader_plugins(),
        *get_metadata_set_plugins(),
        *get_input_plugins(),
        *get_output_plugins(),
        *input_profiles,
        *output_profiles,
    ]


def build_plugins() -> list[Plugin]:
    """Initialize installed file plugins before bundled providers, retaining order."""
    from LiuXin_alpha.file_formats.conversion.plugin import (
        InputFormatPlugin,
        OutputFormatPlugin,
    )
    from LiuXin_alpha.file_formats.conversion.profiles import (
        InputProfile,
        OutputProfile,
    )
    from LiuXin_alpha.plugin_system.file_types import (
        FileTypePlugin,
        MetadataReaderPlugin,
        MetadataWriterPlugin,
    )

    families = (
        FileTypePlugin,
        MetadataReaderPlugin,
        MetadataWriterPlugin,
        InputFormatPlugin,
        OutputFormatPlugin,
        InputProfile,
        OutputProfile,
    )
    stdout, stderr = sys.stdout, sys.stderr
    plugins: list[Plugin] = []
    try:
        bundled = builtin_plugins()
        bundled_names = {plugin.name for plugin in bundled}
        candidates: list[tuple[type[Plugin], str | None]] = []
        for name, stored_path in config["plugins"].items():
            if name in bundled_names:
                continue
            path = os.path.join(plugin_dir, name + ".zip")
            if not os.path.exists(path):
                path = stored_path
            try:
                from LiuXin_alpha.plugin_system.zipplugin import loader

                plugin_class = loader.load(path)
                if issubclass(plugin_class, families):
                    candidates.append((plugin_class, path))
            except Exception:
                print(f"Failed to load file plugin: {name}", file=stderr)
                traceback.print_exc(file=stderr)
            finally:
                sys.stdout, sys.stderr = stdout, stderr

        candidates.extend((plugin_class, None) for plugin_class in bundled)
        for plugin_class, path in candidates:
            try:
                plugin = plugin_class(path)
                plugin.initialize()
                plugins.append(plugin)
            except Exception:
                print(
                    f"Failed to initialize file plugin: {plugin_class.name}",
                    file=stderr,
                )
                traceback.print_exc(file=stderr)
            finally:
                sys.stdout, sys.stderr = stdout, stderr
    finally:
        sys.stdout, sys.stderr = stdout, stderr
    return plugins
