"""Compose the standalone file-plugin fleet from explicit domain-owned providers.

Application-only plugins are composed by their host. Installed ZIP plugins that
implement file hooks, metadata I/O, conversion, or profiles participate here with
the same persisted customization and enable/disable settings.
"""

from __future__ import annotations

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
    """Compose file providers using the shared loading and priority rules."""
    from LiuXin_alpha.plugin_system.loading import build_plugins as load_plugins
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
    return load_plugins(
        builtin_plugins(), config["plugins"], plugin_dir, families=families
    )
