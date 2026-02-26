# -*- coding: utf-8 -*-

"""
API for the builtin plugins included directly in LiuXin's code.
"""

# Todo: Make sure all MD extractors can cope with a file or path being passed in

from LiuXin_alpha.utils.logging import default_log

try:
    from LiuXin_alpha.file_formats.html.to_zip import HTML2ZIP
except Exception as e:
    default_log.log_exception("Failed to import HTML2ZIP", e, "DEBUG")
    HTML2ZIP = None

from LiuXin_alpha.customize.builtins.conversion import get_input_plugins
from LiuXin_alpha.customize.builtins.conversion import get_output_plugins
try:
    from LiuXin_alpha.customize.builtins.device_drivers import get_device_driver_plugins
except Exception as e:
    default_log.log_exception("Failed to import device driver plugins", e, "DEBUG")

    def get_device_driver_plugins():
        return []

from LiuXin_alpha.customize.builtins.on_import import get_file_type_plugins
from LiuXin_alpha.customize.builtins.metadata_downloaders import get_web_md_plugins
from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins
from LiuXin_alpha.customize.builtins.metadata_writers import get_metadata_set_plugins
try:
    from LiuXin_alpha.customize.profiles import input_profiles, output_profiles
except Exception as e:
    default_log.log_exception("Failed to import profiles", e, "DEBUG")
    input_profiles, output_profiles = [], []

try:
    from LiuXin_alpha.library.catalogs.csv_xml import CSV_XML
    from LiuXin_alpha.library.catalogs.bibtex import BIBTEX
    from LiuXin_alpha.library.catalogs.epub_mobi import EPUB_MOBI
except Exception as e:
    default_log.log_exception("Failed to import catalog plugins", e, "DEBUG")
    CSV_XML = BIBTEX = EPUB_MOBI = None

try:
    from LiuXin_alpha.metadata.file_sources.archive import ArchiveExtract, get_comic_metadata
except Exception as e:
    default_log.log_exception("Failed to import archive metadata plugins", e, "DEBUG")
    ArchiveExtract = None
    get_comic_metadata = None

try:
    from LiuXin_alpha.metadata.liuxin_plugins.md_synthesizer import SynthesisMDInputTransform
    from LiuXin_alpha.metadata.liuxin_plugins.isbn_extractor import ISBNMDInputTransform
except Exception as e:
    default_log.log_exception("Failed to import metadata transform plugins", e, "DEBUG")
    SynthesisMDInputTransform = ISBNMDInputTransform = None

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"

plugins = []

# builtin plugins from calibre - LiuXin has been provided with calibre emulation  features so that they should all
# continue to work, and new plugins should just function after the conversiopn process

# To archive plugins {{{

plugins += get_file_type_plugins()
for optional_plugin in (HTML2ZIP, ArchiveExtract):
    if optional_plugin is not None:
        plugins.append(optional_plugin)

# }}}

# Add in the metadata reader plugins
# {{{

plugins += get_metadata_reader_plugins()
# }}}

# ----------------------------------------------------------------------------------------------------------------------
#
# - METADATA WRITER PLUGINS START HERE
#
# ----------------------------------------------------------------------------------------------------------------------

# Metadata writer plugins {{


plugins += get_metadata_set_plugins()

# }}}

# Conversion plugins {{{

plugins += get_input_plugins()

plugins += get_output_plugins()


# }}}

# Catalog plugins {{{

for optional_plugin in (CSV_XML, BIBTEX, EPUB_MOBI):
    if optional_plugin is not None:
        plugins.append(optional_plugin)
# }}}

# Profiles {{{

plugins += list(input_profiles) + list(output_profiles)
# }}}

# Device driver plugins {{{

plugins += get_device_driver_plugins()
# }}}

# New metadata download plugins {{{
default_log.info("Beginning get_web_md_plugins")

plugins += get_web_md_plugins()
default_log.info("Finishing get_web_md_plugins")
# }}}

# Moved over into gui2 customize.builtins - and not imported for all methods by default
# {{{

# # Interface Actions {{{
# default_log.info("Beginning get_ia_plugins")
# from LiuXin.customize.builtins.interface_actions import get_ia_plugins
# plugins += get_ia_plugins()
# default_log.info("Finishing get_ia_plugins")
# # }}}

# Now not done by default - if you wan to include the gui interface actions plugins use the method
# # Preferences Plugins {{{
# default_log.info("Beginning get_preferences_plugins")
# from LiuXin.customize.builtins.preferences import get_preferences_plugins
# plugins += get_preferences_plugins()
# default_log.info("Finishing get_preferences_plugins")
# # }}}

#
# # Store plugins {{{
# default_log.info("Beginning get_store_plugins")
# from LiuXin.customize.builtins.stores import get_store_plugins
# plugins += get_store_plugins()
# default_log.info("Finishing get_store_plugins")
# # }}}

# }}}

# ----------------------------------------------------------------------------------------------------------------------
#
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

if __name__ == "__main__":

    import pprint

    assert True is False, pprint.pformat(plugins)

    # Test load speed
    import subprocess
    import textwrap

    try:
        subprocess.check_call(
            [
                "python",
                "-c",
                textwrap.dedent(
                    """
        import init_calibre  # noqa

        def doit():
            import LiuXin_alpha.utils.calibre.customize.builtins as b
        def show_stats():
            from pstats import Stats
            s = Stats('/tmp/calibre_stats')
            s.sort_stats('cumulative')
            s.print_stats(30)

        import cProfile
        cProfile.run('doit()', '/tmp/calibre_stats')
        show_stats()

        """
                ),
            ]
        )
    except subprocess.CalledProcessError:
        raise SystemExit(1)

    try:
        subprocess.check_call(
            [
                "python",
                "-c",
                textwrap.dedent(
                    """
        from __future__ import print_function
        import time, sys, init_calibre
        st = time.time()
        import LiuXin_alpha.utils.calibre.customize.builtins
        t = time.time() - st
        ret = 0

        for x in ('lxml', 'calibre.ebooks.BeautifulSoup', 'uuid',
            'calibre.utils.terminal', 'calibre.utils.magick', 'PIL', 'Image',
            'sqlite3', 'liuxin_mechanize', 'httplib', 'xml', 'inspect', 'urllib',
            'calibre.utils.date', 'calibre.utils.config', 'platform',
            'calibre.utils.zipfile', 'calibre.utils.formatter',
        ):
            if x in sys.modules:
                ret = 1
                print (x, 'has been loaded by a plugin')
        if ret:
            print ('\\nA good way to track down what is loading something is to run'
            ' python -c "import init_calibre; import calibre.customize.builtins"')
            print()
        print ('Time taken to import all plugins: %.2f'%t)
        sys.exit(ret)

        """
                ),
            ]
        )
    except subprocess.CalledProcessError:
        raise SystemExit(1)
