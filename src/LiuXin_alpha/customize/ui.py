"""Compatibility exports for the explicit application plugin owner.

New application callers use ``customize.application``. File-only callers use
``file_formats.plugin_runtime``. Importing this module never starts plugins.
"""

from LiuXin_alpha.customize.application import (
    ApplyNullMetadata as ApplyNullMetadata,
)
from LiuXin_alpha.customize.application import (
    ForceIdentifiers as ForceIdentifiers,
)
from LiuXin_alpha.customize.application import (
    NameConflict as NameConflict,
)
from LiuXin_alpha.customize.application import (
    QuickMetadata as QuickMetadata,
)
from LiuXin_alpha.customize.application import (
    _config as _config,
)
from LiuXin_alpha.customize.application import (
    add_plugin as add_plugin,
)
from LiuXin_alpha.customize.application import (
    all_edit_book_tool_plugins as all_edit_book_tool_plugins,
)
from LiuXin_alpha.customize.application import (
    all_input_formats as all_input_formats,
)
from LiuXin_alpha.customize.application import (
    all_metadata_plugins as all_metadata_plugins,
)
from LiuXin_alpha.customize.application import (
    all_metadata_synthesis_plugins as all_metadata_synthesis_plugins,
)
from LiuXin_alpha.customize.application import (
    all_viewer_plugins as all_viewer_plugins,
)
from LiuXin_alpha.customize.application import (
    apply_null_metadata as apply_null_metadata,
)
from LiuXin_alpha.customize.application import (
    available_catalog_formats as available_catalog_formats,
)
from LiuXin_alpha.customize.application import (
    available_input_formats as available_input_formats,
)
from LiuXin_alpha.customize.application import (
    available_library_closed_plugins as available_library_closed_plugins,
)
from LiuXin_alpha.customize.application import (
    available_output_formats as available_output_formats,
)
from LiuXin_alpha.customize.application import (
    available_store_plugins as available_store_plugins,
)
from LiuXin_alpha.customize.application import (
    available_stores as available_stores,
)
from LiuXin_alpha.customize.application import (
    bind_plugins as bind_plugins,
)
from LiuXin_alpha.customize.application import (
    build_plugin as build_plugin,
)
from LiuXin_alpha.customize.application import (
    can_set_metadata as can_set_metadata,
)
from LiuXin_alpha.customize.application import (
    catalog_plugins as catalog_plugins,
)
from LiuXin_alpha.customize.application import (
    config as config,
)
from LiuXin_alpha.customize.application import (
    creator_standardization_plugin as creator_standardization_plugin,
)
from LiuXin_alpha.customize.application import (
    customize_plugin as customize_plugin,
)
from LiuXin_alpha.customize.application import (
    default_disabled_plugins as default_disabled_plugins,
)
from LiuXin_alpha.customize.application import (
    device_plugins as device_plugins,
)
from LiuXin_alpha.customize.application import (
    disable_plugin as disable_plugin,
)
from LiuXin_alpha.customize.application import (
    disabled_device_plugins as disabled_device_plugins,
)
from LiuXin_alpha.customize.application import (
    enable_plugin as enable_plugin,
)
from LiuXin_alpha.customize.application import (
    extract_metadata as extract_metadata,
)
from LiuXin_alpha.customize.application import (
    find_plugin as find_plugin,
)
from LiuXin_alpha.customize.application import (
    force_identifiers as force_identifiers,
)
from LiuXin_alpha.customize.application import (
    get_compressor_plugin as get_compressor_plugin,
)
from LiuXin_alpha.customize.application import (
    get_file_type_metadata as get_file_type_metadata,
)
from LiuXin_alpha.customize.application import (
    has_external_plugins as has_external_plugins,
)
from LiuXin_alpha.customize.application import (
    has_library_closed_plugins as has_library_closed_plugins,
)
from LiuXin_alpha.customize.application import (
    initialize_plugin as initialize_plugin,
)
from LiuXin_alpha.customize.application import (
    initialize_plugins as initialize_plugins,
)
from LiuXin_alpha.customize.application import (
    initialized_plugins as initialized_plugins,
)
from LiuXin_alpha.customize.application import (
    input_format_plugins as input_format_plugins,
)
from LiuXin_alpha.customize.application import (
    input_profiles as input_profiles,
)
from LiuXin_alpha.customize.application import (
    interface_actions as interface_actions,
)
from LiuXin_alpha.customize.application import (
    is_disabled as is_disabled,
)
from LiuXin_alpha.customize.application import (
    load_plugin as load_plugin,
)
from LiuXin_alpha.customize.application import (
    main as main,
)
from LiuXin_alpha.customize.application import (
    metadata_plugins as metadata_plugins,
)
from LiuXin_alpha.customize.application import (
    metadata_readers as metadata_readers,
)
from LiuXin_alpha.customize.application import (
    metadata_writers as metadata_writers,
)
from LiuXin_alpha.customize.application import (
    name_generator as name_generator,
)
from LiuXin_alpha.customize.application import (
    option_parser as option_parser,
)
from LiuXin_alpha.customize.application import (
    output_format_plugins as output_format_plugins,
)
from LiuXin_alpha.customize.application import (
    output_profiles as output_profiles,
)
from LiuXin_alpha.customize.application import (
    plugin_customization as plugin_customization,
)
from LiuXin_alpha.customize.application import (
    plugin_for_catalog_format as plugin_for_catalog_format,
)
from LiuXin_alpha.customize.application import (
    plugin_for_input_format as plugin_for_input_format,
)
from LiuXin_alpha.customize.application import (
    plugin_for_output_format as plugin_for_output_format,
)
from LiuXin_alpha.customize.application import (
    preferences_plugins as preferences_plugins,
)
from LiuXin_alpha.customize.application import (
    quick_metadata as quick_metadata,
)
from LiuXin_alpha.customize.application import (
    remove_plugin as remove_plugin,
)
from LiuXin_alpha.customize.application import (
    reread_filetype_plugins as reread_filetype_plugins,
)
from LiuXin_alpha.customize.application import (
    reread_metadata_plugins as reread_metadata_plugins,
)
from LiuXin_alpha.customize.application import (
    restore_plugin_state_to_default as restore_plugin_state_to_default,
)
from LiuXin_alpha.customize.application import (
    run_import_plugins as run_import_plugins,
)
from LiuXin_alpha.customize.application import (
    run_plugins_on_import as run_plugins_on_import,
)
from LiuXin_alpha.customize.application import (
    run_plugins_on_postadd as run_plugins_on_postadd,
)
from LiuXin_alpha.customize.application import (
    run_plugins_on_postimport as run_plugins_on_postimport,
)
from LiuXin_alpha.customize.application import (
    run_plugins_on_postprocess as run_plugins_on_postprocess,
)
from LiuXin_alpha.customize.application import (
    run_plugins_on_preprocess as run_plugins_on_preprocess,
)
from LiuXin_alpha.customize.application import (
    set_file_type_metadata as set_file_type_metadata,
)
from LiuXin_alpha.customize.application import (
    store_plugins as store_plugins,
)
from LiuXin_alpha.customize.application import (
    stores as stores,
)
from LiuXin_alpha.customize.application import (
    synthesize_metadata as synthesize_metadata,
)
from LiuXin_alpha.customize.application import (
    title_phash_handler as title_phash_handler,
)


def __getattr__(name: str) -> object:
    """Resolve historical provider-list attributes only when requested."""
    if name in {"builtin_plugins", "builtin_names"}:
        from LiuXin_alpha.customize.builtins.registry import plugins

        return (
            plugins if name == "builtin_plugins" else frozenset(p.name for p in plugins)
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


if __name__ == "__main__":
    import sys

    sys.exit(main())
