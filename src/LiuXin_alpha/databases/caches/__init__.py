from __future__ import unicode_literals, division, absolute_import, print_function

"""
Performance cache for data from the library database.
"""

# The individual caches are broken down into the following components
# - cache - The main API - responsible for being thread safe and providing a unified interface to everything
# - fields - Provide access apis to the data stored in the tables. A single table can have many fields - for example
#          - titles dor books, which have many
# - tables - represent the tables on the database. Cache data is stored here and they are responsible for updating the
#          - database after changes to it


from datetime import datetime, timedelta

try:
    from LiuXin.customize.ui import run_plugins_on_import
except ImportError:

    def run_plugins_on_import(file):
        return file


try:
    from LiuXin.customize.ui import run_plugins_on_postimport
except ImportError:

    def run_plugins_on_postimport(file):
        return file


try:
    from LiuXin.customize.ui import run_plugins_on_postadd
except ImportError:

    def run_plugins_on_postadd(file, *args, **kwargs):
        return file


from LiuXin_alpha.databases.locking import create_locks, DowngradeLockError

from LiuXin.databases.search import Search

from LiuXin.library.metadata import Metadata as LibraryMetadata

from LiuXin.utils.date import parse_date, UNDEFINED_DATE, utc_tz

from LiuXin.utils.logger import default_log
from LiuXin.utils.plugins import plugins

# Todo: Merge in base cache from library.caches

# Preform plugin load
default_log.info("Before calling speedup plugin")
_c_speedup = plugins["speedup"][0].parse_date
default_log.info("After calling speedup plugin")


# Todo - should not be here
def c_parse(val):
    """
    Parse a value into a datetime object.

    :param val:
    :return:
    """
    # The value may be coming off the database this way
    if isinstance(val, datetime):
        return datetime

    try:
        year, month, day, hour, minutes, seconds, tzsecs = _c_speedup(val)

    except (AttributeError, TypeError):
        # If a value like 2001 is stored in the column, apsw will return it as an int
        if isinstance(val, (int, float)):
            return datetime(int(val), 1, 3, tzinfo=utc_tz)
        if val is None:
            return UNDEFINED_DATE

    except Exception as e:
        err_str = "Failed to parse datetime string"
        default_log.log_exception(err_str, e, "INFO")

    else:
        try:
            ans = datetime(year, month, day, hour, minutes, seconds, tzinfo=utc_tz)
            if tzsecs is not 0:
                ans -= timedelta(seconds=tzsecs)
        except OverflowError:
            ans = UNDEFINED_DATE
        return ans
    try:
        return parse_date(val, as_utc=True, assume_utc=True)
    except (ValueError, TypeError):
        return UNDEFINED_DATE
