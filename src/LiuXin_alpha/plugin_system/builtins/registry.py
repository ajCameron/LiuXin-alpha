# -*- coding: utf-8 -*-

"""
Assemble the complete built-in plugin compatibility registry.

Importing this explicit composition module loads the bundled providers and builds
``plugins`` from shared file providers plus application-only families. The package
initializer deliberately does not.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/plugin_system/test_plugin_base.py
"""

# Todo: Make sure all MD extractors can cope with a file or path being passed in

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.file_formats.plugin_defaults import builtin_plugins as file_builtin_plugins

try:
    from LiuXin_alpha.plugin_system.builtins.device_drivers import get_device_driver_plugins
except Exception as e:
    default_log.log_exception("Failed to import device driver plugins", e, "DEBUG")

    def get_device_driver_plugins():
        """
        Return device driver plugins under the format's safety and compatibility rules.

        Example:
            Exercise get device driver plugins through a consuming regression::

                python -m pytest -q tests/plugin_system/test_plugin_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return []

from LiuXin_alpha.plugin_system.builtins.metadata_downloaders import get_web_md_plugins
try:
    from LiuXin_alpha.library.catalogs.csv_xml import CSV_XML
    from LiuXin_alpha.library.catalogs.bibtex import BIBTEX
    from LiuXin_alpha.library.catalogs.epub_mobi import EPUB_MOBI
except Exception as e:
    default_log.log_exception("Failed to import catalog plugins", e, "DEBUG")
    CSV_XML = BIBTEX = EPUB_MOBI = None

try:
    from LiuXin_alpha.metadata.liuxin_plugins.md_synthesizer import SynthesisMDInputTransform
    from LiuXin_alpha.metadata.liuxin_plugins.isbn_extractor import ISBNMDInputTransform
except Exception as e:
    default_log.log_exception("Failed to import metadata transform plugins", e, "DEBUG")
    SynthesisMDInputTransform = ISBNMDInputTransform = None

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"

# File providers have one catalogue for both standalone and application hosts.
# Application-only families extend that catalogue below.
plugins = file_builtin_plugins()

# Catalog plugins {{{

for optional_plugin in (CSV_XML, BIBTEX, EPUB_MOBI):
    if optional_plugin is not None:
        plugins.append(optional_plugin)
# }}}

# Device driver plugins {{{

plugins += get_device_driver_plugins()
# }}}

# New metadata download plugins {{{
default_log.info("Beginning get_web_md_plugins")

plugins += get_web_md_plugins()
default_log.info("Finishing get_web_md_plugins")
# }}}

# - LIUXIN PLUGINS START HERE
#
# ----------------------------------------------------------------------------------------------------------------------

# ------------------------------------------------
# - COMPLETE CONVERSION PLUGINS START HERE
# ------------------------------------------------


# ------------------------------------------------
# - METADATA SYNTHESIS PLUGINS START HERE
# ------------------------------------------------

for optional_plugin in (SynthesisMDInputTransform, ISBNMDInputTransform):
    if optional_plugin is not None:
        plugins.append(optional_plugin)

default_log.info("")

# ----------------------------------------------------------------------------------------------------------------------
#
# - TEST SUITE STARTS HERE
#
# ----------------------------------------------------------------------------------------------------------------------
