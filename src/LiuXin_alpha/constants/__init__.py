
"""
System level constants - can be overridden by preferences.
"""

from __future__ import print_function

from typing import Optional

import sys
import locale
import codecs
import os
import importlib
import string

from LiuXin_alpha.constants.file_extensions import IMAGE_EXTENSIONS, COMPRESSED_FILE_EXTENSIONS
from LiuXin_alpha.constants.paths import LiuXin_calibre_caches, LiuXin_calibre_config_folder, config_dir, CONFIG_DIR_MODE

from LiuXin_alpha.utils.logging import LiuXin_print

# Uses semantic versioning - see https://semver.org/
__license__ = "GPL v3"
__appname__ = "LiuXin"


LIUXIN_NUMERIC_VERSION = (0, 0, 8)
__version__ = ".".join(map(str, LIUXIN_NUMERIC_VERSION))


# The version of calibre which is embedded in LiuXin and is used to provide a lot of its functionality
CALIBRE_NUMERIC_VERSION = (2, 12, 0)
__calibre_version__ = ".".join(map(str, CALIBRE_NUMERIC_VERSION))

# Constants, some of which have to be set at run time. Used as the basis for the OS localization features.
__lx_constants_version__ = (0, 0, 2)


WINDOWS_RESERVED_NAMES = frozenset(
    "CON PRN AUX NUL COM1 COM2 COM3 COM4 COM5 COM6 COM7 COM8 COM9 LPT1 LPT2 LPT3 LPT4 "
    "LPT5 LPT6 LPT7 LPT8 LPT9".split()
)

isportable = os.environ.get("CALIBRE_PORTABLE_BUILD", None) is not None


def get_portable_base() -> Optional[str]:
    """
    Return path to the directory that contains calibre-portable.exe or None.

    :return:
    """
    if isportable:
        return os.path.dirname(os.path.dirname(os.environ["CALIBRE_PORTABLE_BUILD"]))
    return None


# These constants control the mode LiuXin is running in
# ALTER WITH CARE - DOING SO AFFECTS BEHAVIORS THROUGHOUT THE SYSTEM.

VERBOSE_DEBUG = False

# Causes LiuXin to print a lot.
# This included, but is not limited to
# 1 - Any statement executed on the databases
# 2 - Any warning produced during normal operation
# 3 - Anything that seems unusual. Including, but not limited to
# - - OPF files having two sections identifiable as metadata

VERBOSE_LOGGING = True

# The LOG_LEVEL is the level of severity of events which should be recorded by the log. It goes
# DEBUG    = 0
# INFO     = 1
# WARN     = 2
# ERROR    = 3
# CRITICAL = 4
LOG_LEVEL = 0

# Should log files be written as LiuXin operates
FILE_LOGGING = True

DEBUG_FILE_DUMP = True
# Dumps any file which LiuXin is having problems with to a folder at the root of LiuXin called LiuXin_debug
# To aid in the debugging process.

AUTONOMOUS_MODE = True
# Causes LiuXin to run in autonomous mode. It attempts to do more itself, asks it's human "master" for less input
# generally behaves more like an A.I. A really badly coded A.I.

DEV_MODE = True
# When you are hacking on LiuXin, and need it to throw errors when unexpected things are happening

DEBUG = True
# Which is slightly different from DEV_MODE. In some nebulous ways.


class Resource_Error(Exception):
    def __init__(self, argument):
        self.argument = argument
        LiuXin_print(self.argument)

    def __str__(self):
        return repr(self.argument)


# A common folder at the root of LiuXin folder is designated the import_cache. Here files can be stored while they're
# being processed before being written out to the folder stores


# DatabasePing constants
# Adding to this later - as time and sanity permits
ALLOWED_DOC_TYPES = ["ebook"]


_plat = sys.platform.lower()
iswindows = "win32" in _plat or "win64" in _plat
isosx = "darwin" in _plat
isnewosx = isosx and getattr(sys, "new_app_bundle", False)
isfreebsd = "freebsd" in _plat
isnetbsd = "netbsd" in _plat
isdragonflybsd = "dragonfly" in _plat
isbsd = isfreebsd or isnetbsd or isdragonflybsd
islinux = not (iswindows or isosx or isbsd)
isfrozen = hasattr(sys, "frozen")
isunix = isosx or islinux
isportable = os.environ.get("CALIBRE_PORTABLE_BUILD", None) is not None
ispy3 = sys.version_info.major > 2
# Deals with the fact that sys.getwindowsversion is not defined on all systems
try:
    isxp = iswindows and sys.getwindowsversion().major < 6
except AttributeError:
    isxp = False
is64bit = sys.maxsize > (1 << 32)


# TODO: Make sure that everything is re-encoded to utf-8 if the file system is not encoded that way
# setting the preferred encoding LiuXin will use
# WARNING - CHANGE OFF UTF-8 AT YOU'RE OWN RISK
try:
    preferred_encoding = locale.getpreferredencoding()
    codecs.lookup(preferred_encoding)
except:
    preferred_encoding = "utf-8"


# setting the file system encoding
filesystem_encoding = sys.getfilesystemencoding()
if filesystem_encoding is None:
    filesystem_encoding = "utf-8"
else:
    try:
        if codecs.lookup(filesystem_encoding).name == "ascii":
            filesystem_encoding = "utf-8"
            # On linux, unicode arguments to os file functions are coerced to an ascii
            # bytestring if sys.getfilesystemencoding() == 'ascii', which is
            # just plain dumb. This is fixed by the icu.py module which, when
            # imported changes ascii to utf-8
    except:
        filesystem_encoding = "utf-8"


def is_ext_compressed(extension):
    """Takes the extension of a potentially compressed file. Works out if it's compressed."""
    ext_local = str(extension)

    if ext_local in COMPRESSED_FILE_EXTENSIONS:
        return True

    try:
        if ext_local[:2] == ".r" and int(ext_local[2:]) > 0:
            return True
    except:
        return False

    return False


def get_os_type():
    """Returns the type of OS we are dealing with. Currently, working for Windows and Linux."""
    if iswindows:
        return "windows"
    else:
        return "linux"


def get_windows_drive_letters():
    """Returns an index of windows drive letters."""
    drive_letters = []
    CAPITALS = string.ascii_uppercase

    for letter in CAPITALS:
        root = letter + ":"
        root = str(root)
        drive_letters.append(root)

    return drive_letters


# Todo: Hive all this off into some form of validation methods store. Possibly with plugins.
from copy import deepcopy
import os

# Todo: Get a collection of image files and run them through imghdr - see what comes out


# Todo: WTF is all this?
# Todo: Overwrite OS with home baked madness
# Cover tupples are the way LiuXin stores cover data.
# They take the form (type, data). This method takes one, does it's best to tell if it's valid abd returns it
# The checks run are as follows
# if it is a file (type = u'file')
# - does the file exists? Can it be opened? Does the file correspond to the stated extension?
# if it is any other tpye
# - does the claimed type correspond to the data?
# returns True or False, with an error message
def check_image_tuple(image_tuple):
    """
    Takes a cover tuple - checks to see if all is as it should be, and returns True or False
    depending on if it is on not
    :param cover_tupple:
    :return: True or False, and error message (blank if True)
    """

    # checking that the tuple is of the right length
    image_tuple = deepcopy(image_tuple)
    if len(image_tuple) != 2:
        return False, "wrong length"

    type = image_tuple[0]
    data = image_tuple[1]

    # if both entries are blank no more processing is needed
    if type is None and data is None:
        return True, "Both entries are None"

    # if the type entry is not None and the data entry is None the tupple fails again
    if type is "file" and data is None:
        return False, "No data provided"

    # Todo: Add more checks to make sure imghdr works as intended - and to parse it's output properly
    if type is "file" and data is not None:

        # Todo: Check the file actually corresponds to the claimed type
        # if the file actually exists, we can run test on it. Hmm. Testing. Cake. E.t.c.
        status = os.path.exists(data)
        if status:
            extension = os.path.splitext(data)[0]
            if len(extension) == 0:
                return False, "Path not valid"
            else:
                if extension not in VALID_IMAGE_EXTENSIONS:
                    return False, "Not an image type"
                else:
                    return True, "The file opens and everything"

        else:
            return False, "File not found"

    elif (type in VALID_IMAGE_EXTENSIONS) and data is not None:
        # Todo: Get a collection of images and make this actually work - not just be a placeholder
        return (
            True,
            "Probably. But I need to get the MetaData classes up and running before I care.",
        )

    return (
        True,
        "This is so broken - and I'm in the middle of something else so a fix can wait",
    )


# ----------------------------------------------------------------------------------------------------------------------
#
# -- Constants concerning names start here
#
# ----------------------------------------------------------------------------------------------------------------------

# Todo: Find the calibre name pre and suffixes somewhere and add them in
# Name prefix list - keyed by the short form, valued with the long form. Valid only for english.
# Todo: Shift this sideways into the language pack section
# http://notes.ericwillis.com/2009/11/common-name-prefixes-titles-and-honorifics/
name_prefixes = {
    "Ms": "Ms",
    "Miss": "Miss",
    "Mr": "Mr",
    "Master": "Master",
    "Rev": "Reverend",
    "Fr": "Father",
    "Dr": "Doctor",
    "Atty": "Attorney",
    "Prof": "Professor",
    "Hon": "Honorable",
    "Pres": "President",
    "Gov": "Governor",
    "Coach": "Coach",
    "Ofc": "Officer",
    "Msgr": "Monsignor",
    "Sr": "Sister",
    "Br": "Brother",
    "Supt": "Superintendent",
    "Rep": "Representative",
    "Amb": "Ambassador",
    "Treas": "Treasurer",
    "Sec": "Secretary",
    "Pvt": "Private",
    "Cpl": "Corporal",
    "Sgt": "Sargent",
    "Adm": "Administrative",
    "Maj": "Major",
    "Capt": "Captain",
    "Cmdr": "Commander",
    "Lt": "Lieutenant",
    "Lt Col": "Lieutenant Colonel",
    "Col": "Colonel",
    "Gen": "General",
}

# https://www.lehigh.edu/lewis/suffix.htm
name_suffixes = {
    "B.V.M.": "Blessed Virgin Mary",
    "CFRE": "Certified Fund Raising Executive",
    "CLU": "Chartered Life Underwriter",
    "CPA": "Certified Public Accountant",
    "C.S.C.": "Congregation of Holy Cross",
    "C.S.J.": "Sisters of St. Joseph",
    "D.C.": "Chiropractic Doctor",
    "D.D.": "Doctor of Divinity",
    "D.D.S": "Doctor of Dental Surgery",
    "D.M.D": "Doctor of Dental Medicine",
    "D.O.": "Doctor of Osteopathy",
    "D.V.M.": "Doctor of Veterinary Medicine",
    "Ed.D.": "Doctor of Education",
    "Esq.": "Esquire",
    "II": "the Second",
    "III": "the Third",
    "IV": "the Fourth",
    "V": "the Fifth",
    "Inc.": "Incorporated",
    "J.D.": "Juris Doctor",
    "LL.D.": "Doctor of Laws",
    "Lord": "Lord",
    "Ltd.": "Limited",
    "M.D.": "Doctor of Medicine",
    "O.D.": "Doctor of Optometry",
    "O.S.B.": "Order of St Benedict",
    "P.C.": "Past Commander, Police Constable, Post Commander",
    "P.E.": "Protestant Episcopal",
    "Ph.D.": "Doctor of Philosophy",
    "Ret.": "Retired",
    "R.G.S": "Sisters of Our Lady of Charity of the Good Shepherd",
    "R.N.": "Registered Nurse, Royal Navy",
    "R.N.C.": "Registered Nurse Clinician",
    "S.J.": "Society of Jesus",
    "Sr.": "Senior",
    "Sir": "Sir",
    "USA": "United States Army",
    "USAF": "United States Air Force",
    "USAFR": "United States Air Force Reserve",
    "USAR": "United States Army Reserve",
    "USCG": "United States Coast Guard",
    "USMC": "United States Marine Corps",
    "USMCR": "United States Marine Corps Reserve",
    "USN": "United States Navy",
    "USNR": "United States Navy Reserve",
}

# Todo: Need to build out this list into something useful

# MARC code list for relators
# http://www.loc.gov/marc/relators/relaterm.html
CREATOR_MARC_DICT = {"author": "aut", "authors": "aut"}

python_plugins = []

# config_dir {{{

if "CALIBRE_CONFIG_DIRECTORY" in os.environ:
    config_dir = os.path.abspath(os.environ["CALIBRE_CONFIG_DIRECTORY"])

elif iswindows:
    config_dir = LiuXin_calibre_config_folder

# elif iswindows:
#     if plugins['winutil'][0] is None:
#         raise Exception(plugins['winutil'][1])
#     config_dir = plugins['winutil'][0].special_folder_path(plugins['winutil'][0].CSIDL_APPDATA)
#     if not os.access(config_dir, os.W_OK|os.X_OK):
#         config_dir = os.path.expanduser('~')
#     config_dir = os.path.join(config_dir, 'calibre')

elif isosx:
    config_dir = os.path.expanduser("~/Library/Preferences/calibre")

else:
    bdir = os.path.abspath(os.path.expanduser(os.environ.get("XDG_CONFIG_HOME", "~/.config")))
    config_dir = os.path.join(bdir, "calibre")
    try:
        os.makedirs(config_dir, mode=CONFIG_DIR_MODE)
    except:
        pass
    if not os.path.exists(config_dir) or not os.access(config_dir, os.W_OK) or not os.access(config_dir, os.X_OK):
        print("No write acces to", config_dir, "using a temporary dir instead")
        import tempfile, atexit

        config_dir = tempfile.mkdtemp(prefix="calibre-config-")

        def cleanup_cdir():
            try:
                import shutil

                shutil.rmtree(config_dir)
            except:
                pass

        atexit.register(cleanup_cdir)
# }}}

# ----------------------------------------------------------------------------------------------------------------------
#
# - CALIBRE EMULATION CONSTANTS START HERE


__author__ = "Alex Cameron"


def get_version():
    """
    Returns the version in a human readable string.
    :return:
    """
    return str(__version__)


# Todo: Add fall backs in case LX is running on a system without these installed
win32event = importlib.import_module("win32event") if iswindows else None
try:
    winerror = importlib.import_module("winerror") if iswindows else None
except ImportError:
    # From http://bugs.python.org/file7326/winerror.py
    # Seems to just be a simple mapping of errors codes - should work
    import utils.lx_libraries.liuxin_winerror as winerror
win32api = importlib.import_module("win32api") if iswindows else None
fcntl = None if iswindows else importlib.import_module("fcntl")


def load_library(name, cdll):
    if iswindows:
        return cdll.LoadLibrary(name)
    if isosx:
        name += ".dylib"
        if hasattr(sys, "frameworks_dir"):
            return cdll.LoadLibrary(os.path.join(getattr(sys, "frameworks_dir"), name))
        return cdll.LoadLibrary(name)
    return cdll.LoadLibrary(name + ".so")


def cache_dir():
    return LiuXin_calibre_caches


def get_windows_username():
    """
    Why is this even here?

    :return:
    """
    raise NotImplementedError("winutil is not supported")


def get_windows_temp_path():
    import ctypes

    n = ctypes.windll.kernel32.GetTempPathW(0, None)
    if n == 0:
        return None
    buf = ctypes.create_unicode_buffer("\0" * n)
    ctypes.windll.kernel32.GetTempPathW(n, buf)
    ans = buf.value
    return ans if ans else None


def get_unicode_windows_env_var(name):
    import ctypes

    name = str(name)
    n = ctypes.windll.kernel32.GetEnvironmentVariableW(name, None, 0)
    if n == 0:
        return None
    buf = ctypes.create_unicode_buffer("\0" * n)
    ctypes.windll.kernel32.GetEnvironmentVariableW(name, buf, n)
    return buf.value


#
# ----------------------------------------------------------------------------------------------------------------------
