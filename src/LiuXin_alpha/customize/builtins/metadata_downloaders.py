"""
Metadata downloaders try and complete sets of metadata for books from the internet.

For example, you could feed one of them an ISBN, and it'd get the title.
Or you could feed one of them the title, and it'd get the cover.
"""


from LiuXin_alpha.utils.logger import default_log

web_md_plugins = []

# The metadata plugins not imported below are optional ones or ones that require additional configuration.
# So will not be loaded here.
try:
    default_log.info("About to attempt to load GoogleBooks web_sources")
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks
except Exception as e:
    debug_str = "Unable to import {0} from {1}".format("LiuXin.metadata.web_sources.google", "GoogleBooks")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info(
        "{1} from {0} was successfully imported".format("LiuXin.metadata.web_sources.google", "GoogleBooks")
    )
    web_md_plugins += [GoogleBooks]

try:
    from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages
except Exception as e:
    debug_str = "Unable to import {0} from {1}".format("LiuXin.metadata.web_sources.google_images", "GoogleImages")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info(
        "{1} from {0} was successfully imported".format("LiuXin.metadata.web_sources.google_images", "GoogleImages")
    )
    web_md_plugins += [GoogleImages]

try:
    from LiuXin_alpha.metadata.web_sources.amazon import Amazon
except Exception as e:
    debug_str = "Unable to import {0} from {1}".format("LiuXin.metadata.web_sources.amazon", "Amazon")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("{1} from {0} was successfully imported".format("LiuXin.metadata.web_sources.amazon", "Amazon"))
    web_md_plugins += [Amazon]

try:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss
except Exception as e:
    debug_str = "Unable to import {0} from {1}".format("LiuXin.metadata.web_sources.edelweiss", "Edelweiss")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info(
        "{1} from {0} was successfully imported".format("LiuXin.metadata.web_sources.edelweiss", "Edelweiss")
    )
    web_md_plugins += [Edelweiss]

try:
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary
except Exception as e:
    debug_str = "Unable to import {0} from {1}".format("LiuXin.metadata.web_sources.openlibrary", "OpenLibrary")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info(
        "{1} from {0} was successfully imported".format("LiuXin.metadata.web_sources.openlibrary", "OpenLibrary")
    )
    web_md_plugins += [OpenLibrary]

try:
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB
except Exception as e:
    debug_str = "Unable to import {0} from {1}".format("LiuXin.metadata.web_sources.isbndb", "ISBNDB")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("{1} from {0} was successfully imported".format("LiuXin.metadata.web_sources.isbndb", "ISBNDB"))
    web_md_plugins += [ISBNDB]

try:
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive
except Exception as e:
    debug_str = "Unable to import {0} from {1}".format("LiuXin.metadata.web_sources.overdrive", "OverDrive")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info(
        "{1} from {0} was successfully imported".format("LiuXin.metadata.web_sources.overdrive", "OverDrive")
    )
    web_md_plugins += [OverDrive]

try:
    from LiuXin_alpha.metadata.web_sources.douban import Douban
except Exception as e:
    debug_str = "Unable to import {0} from {1}".format("LiuXin.metadata.web_sources.douban", "Douban")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("{1} from {0} was successfully imported".format("LiuXin.metadata.web_sources.douban", "Douban"))
    web_md_plugins += [Douban]

try:
    from LiuXin_alpha.metadata.web_sources.ozon import Ozon
except Exception as e:
    debug_str = "Unable to import {0} from {1}".format("LiuXin.metadata.web_sources.ozon", "Ozon")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("{1} from {0} was successfully imported".format("LiuXin.metadata.web_sources.ozon", "Ozon"))
    web_md_plugins += [Ozon]

try:
    from LiuXin_alpha.metadata.web_sources.big_book_search import BigBookSearch
except Exception as e:
    debug_str = "Unable to import {0} from {1}".format("LiuXin.metadata.web_sources.big_book_search", "BigBookSearch")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info(
        "{1} from {0} was successfully imported".format("LiuXin.metadata.web_sources.big_book_search", "BigBookSearch")
    )
    web_md_plugins += [BigBookSearch]

default_log.info("netadata_downloader imports complete")


def get_web_md_plugins():
    """
    Return all valid downloaders of metadata form the web.

    :return:
    """
    return web_md_plugins
