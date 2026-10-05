# rarfile.py
#
# Copyright (c) 2005-2013  Marko Kreen <markokr@gmail.com>
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

"""
Read RAR metadata, entries and streams using external or direct extraction backends.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise rarfile through a consuming regression::

        python -m pytest -q tests/utils/decompression/test_archives.py
"""

__version__ = "2.6"

# export only interesting items
__all__ = ["is_rarfile", "RarInfo", "RarFile", "RarExtFile"]

##
## Imports and compat - support both Python 2.x and 3.x
##

import sys, os, struct, errno
from struct import pack, unpack
from binascii import crc32
from tempfile import mkstemp
from subprocess import Popen, PIPE, STDOUT
from datetime import datetime

# only needed for encryped headers
try:
    from Crypto.Cipher import AES

    try:
        from hashlib import sha1
    except ImportError:
        from sha import new as sha1
    _have_crypto = 1
except ImportError:
    _have_crypto = 0

# compat with 2.x
if sys.hexversion < 0x3000000:
    # prefer 3.x behaviour
    range = xrange
    # py2.6 has broken bytes()
    def bytes(s, enc):
        """
        Perform the bytes utility operation under explicit compatibility rules.

        Example:
            Exercise bytes through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param s: Value supplied for s under the utility contract.
        :param enc: Value supplied for enc under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return str(s)


# see if compat bytearray() is needed
try:
    bytearray
except NameError:
    import array

    class bytearray:
        """
        Provide the bytearray utility contract with explicit state and cleanup behavior.

        Example:
            Exercise bytearray through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py
        """
        def __init__(self, val=""):
            """
            Initialize and validate the bytearray state.

            Example:
                Exercise bytearray.  init   through a consuming regression::

                    python -m pytest -q tests/utils/decompression/test_archives.py


            :param val: Template or metadata value evaluated by the operation.
            :return: None; validated state is stored on the receiving object.
            """
            self.arr = array.array("B", val)
            self.append = self.arr.append
            self.__getitem__ = self.arr.__getitem__
            self.__len__ = self.arr.__len__

        def decode(self, *args):
            """
            Perform the decode utility operation under explicit compatibility rules.

            Example:
                Exercise bytearray.decode through a consuming regression::

                    python -m pytest -q tests/utils/decompression/test_archives.py


            :param args: Positional values forwarded to the compatibility implementation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.arr.tostring().decode(*args)


# Optimized .readinto() requires memoryview
try:
    memoryview
    have_memoryview = 1
except NameError:
    have_memoryview = 0

# Struct() for older python
try:
    from struct import Struct
except ImportError:

    class Struct:
        """
        Provide the Struct utility contract with explicit state and cleanup behavior.

        Example:
            Exercise Struct through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py
        """
        def __init__(self, fmt):
            """
            Initialize and validate the Struct state.

            Example:
                Exercise Struct.  init   through a consuming regression::

                    python -m pytest -q tests/utils/decompression/test_archives.py


            :param fmt: Date, number or template format specification.
            :return: None; validated state is stored on the receiving object.
            """
            self.format = fmt
            self.size = struct.calcsize(fmt)

        def unpack(self, buf):
            """
            Perform the unpack utility operation under explicit compatibility rules.

            Example:
                Exercise Struct.unpack through a consuming regression::

                    python -m pytest -q tests/utils/decompression/test_archives.py


            :param buf: Value supplied for buf under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return unpack(self.format, buf)

        def unpack_from(self, buf, ofs=0):
            """
            Perform the unpack from utility operation under explicit compatibility rules.

            Example:
                Exercise Struct.unpack from through a consuming regression::

                    python -m pytest -q tests/utils/decompression/test_archives.py


            :param buf: Value supplied for buf under the utility contract.
            :param ofs: Value supplied for ofs under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return unpack(self.format, buf[ofs : ofs + self.size])

        def pack(self, *args):
            """
            Perform the pack utility operation under explicit compatibility rules.

            Example:
                Exercise Struct.pack through a consuming regression::

                    python -m pytest -q tests/utils/decompression/test_archives.py


            :param args: Positional values forwarded to the compatibility implementation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return pack(self.format, *args)


# file object superclass
try:
    from io import RawIOBase
except ImportError:

    class RawIOBase(object):
        """
        Provide the RawIOBase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise RawIOBase through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py
        """
        def close(self):
            """
            Forward the close operation while preserving adapter ownership rules.

            Example:
                Exercise RawIOBase.close through a consuming regression::

                    python -m pytest -q tests/utils/decompression/test_archives.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            pass


##
## Module configuration.  Can be tuned after importing.
##

#: default fallback charset
DEFAULT_CHARSET = "windows-1252"

#: list of encodings to try, with fallback to DEFAULT_CHARSET if none succeed
TRY_ENCODINGS = ("utf8", "utf-16le")

#: 'unrar', 'rar' or full path to either one
UNRAR_TOOL = "unrar"

#: Command line args to use for opening file for reading.
OPEN_ARGS = ("p", "-inul")

#: Command line args to use for extracting file to disk.
EXTRACT_ARGS = ("x", "-y", "-idq")

#: args for testrar()
TEST_ARGS = ("t", "-idq")

#: whether to speed up decompression by using tmp archive
USE_EXTRACT_HACK = 1

#: limit the filesize for tmp archive usage
HACK_SIZE_LIMIT = 20 * 1024 * 1024

#: whether to parse file/archive comments.
NEED_COMMENTS = 1

#: whether to convert comments to unicode strings
UNICODE_COMMENTS = 0

#: When RAR is corrupt, stopping on bad header is better
#: On unknown/misparsed RAR headers reporting is better
REPORT_BAD_HEADER = 0

#: Convert RAR time tuple into datetime() object
USE_DATETIME = 0

#: Separator for path name components.  RAR internally uses '\\'.
#: Use '/' to be similar with zipfile.
PATH_SEP = "\\"

##
## rar constants
##

# block types
RAR_BLOCK_MARK = 0x72  # r
RAR_BLOCK_MAIN = 0x73  # s
RAR_BLOCK_FILE = 0x74  # t
RAR_BLOCK_OLD_COMMENT = 0x75  # u
RAR_BLOCK_OLD_EXTRA = 0x76  # v
RAR_BLOCK_OLD_SUB = 0x77  # w
RAR_BLOCK_OLD_RECOVERY = 0x78  # x
RAR_BLOCK_OLD_AUTH = 0x79  # y
RAR_BLOCK_SUB = 0x7A  # z
RAR_BLOCK_ENDARC = 0x7B  # {

# flags for RAR_BLOCK_MAIN
RAR_MAIN_VOLUME = 0x0001
RAR_MAIN_COMMENT = 0x0002
RAR_MAIN_LOCK = 0x0004
RAR_MAIN_SOLID = 0x0008
RAR_MAIN_NEWNUMBERING = 0x0010
RAR_MAIN_AUTH = 0x0020
RAR_MAIN_RECOVERY = 0x0040
RAR_MAIN_PASSWORD = 0x0080
RAR_MAIN_FIRSTVOLUME = 0x0100
RAR_MAIN_ENCRYPTVER = 0x0200

# flags for RAR_BLOCK_FILE
RAR_FILE_SPLIT_BEFORE = 0x0001
RAR_FILE_SPLIT_AFTER = 0x0002
RAR_FILE_PASSWORD = 0x0004
RAR_FILE_COMMENT = 0x0008
RAR_FILE_SOLID = 0x0010
RAR_FILE_DICTMASK = 0x00E0
RAR_FILE_DICT64 = 0x0000
RAR_FILE_DICT128 = 0x0020
RAR_FILE_DICT256 = 0x0040
RAR_FILE_DICT512 = 0x0060
RAR_FILE_DICT1024 = 0x0080
RAR_FILE_DICT2048 = 0x00A0
RAR_FILE_DICT4096 = 0x00C0
RAR_FILE_DIRECTORY = 0x00E0
RAR_FILE_LARGE = 0x0100
RAR_FILE_UNICODE = 0x0200
RAR_FILE_SALT = 0x0400
RAR_FILE_VERSION = 0x0800
RAR_FILE_EXTTIME = 0x1000
RAR_FILE_EXTFLAGS = 0x2000

# flags for RAR_BLOCK_ENDARC
RAR_ENDARC_NEXT_VOLUME = 0x0001
RAR_ENDARC_DATACRC = 0x0002
RAR_ENDARC_REVSPACE = 0x0004
RAR_ENDARC_VOLNR = 0x0008

# flags common to all blocks
RAR_SKIP_IF_UNKNOWN = 0x4000
RAR_LONG_BLOCK = 0x8000

# Host OS types
RAR_OS_MSDOS = 0
RAR_OS_OS2 = 1
RAR_OS_WIN32 = 2
RAR_OS_UNIX = 3
RAR_OS_MACOS = 4
RAR_OS_BEOS = 5

# Compression methods - '0'..'5'
RAR_M0 = 0x30
RAR_M1 = 0x31
RAR_M2 = 0x32
RAR_M3 = 0x33
RAR_M4 = 0x34
RAR_M5 = 0x35

##
## internal constants
##

RAR_ID = bytes("Rar!\x1a\x07\x00", "ascii")
ZERO = bytes("\0", "ascii")
EMPTY = bytes("", "ascii")

S_BLK_HDR = Struct("<HBHH")
S_FILE_HDR = Struct("<LLBLLBBHL")
S_LONG = Struct("<L")
S_SHORT = Struct("<H")
S_BYTE = Struct("<B")
S_COMMENT_HDR = Struct("<HBBH")

##
## Public interface
##


class Error(Exception):
    """
    Base class for rarfile errors.

    Example:
        Exercise Error through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class BadRarFile(Error):
    """
    Incorrect data in archive.

    Example:
        Exercise BadRarFile through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class NotRarFile(Error):
    """
    The file is not RAR archive.

    Example:
        Exercise NotRarFile through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class BadRarName(Error):
    """
    Cannot guess multipart name components.

    Example:
        Exercise BadRarName through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class NoRarEntry(Error):
    """
    File not found in RAR

    Example:
        Exercise NoRarEntry through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class PasswordRequired(Error):
    """
    File requires password

    Example:
        Exercise PasswordRequired through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class NeedFirstVolume(Error):
    """
    Need to start from first volume.

    Example:
        Exercise NeedFirstVolume through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class NoCrypto(Error):
    """
    Cannot parse encrypted headers - no crypto available.

    Example:
        Exercise NoCrypto through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class RarExecError(Error):
    """
    Problem reported by unrar/rar.

    Example:
        Exercise RarExecError through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class RarWarning(RarExecError):
    """
    Non-fatal error

    Example:
        Exercise RarWarning through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class RarFatalError(RarExecError):
    """
    Fatal error

    Example:
        Exercise RarFatalError through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class RarCRCError(RarExecError):
    """
    CRC error during unpacking

    Example:
        Exercise RarCRCError through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class RarLockedArchiveError(RarExecError):
    """
    Must not modify locked archive

    Example:
        Exercise RarLockedArchiveError through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class RarWriteError(RarExecError):
    """
    Write error

    Example:
        Exercise RarWriteError through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class RarOpenError(RarExecError):
    """
    Open error

    Example:
        Exercise RarOpenError through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class RarUserError(RarExecError):
    """
    User error

    Example:
        Exercise RarUserError through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class RarMemoryError(RarExecError):
    """
    Memory error

    Example:
        Exercise RarMemoryError through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class RarCreateError(RarExecError):
    """
    Create error

    Example:
        Exercise RarCreateError through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class RarNoFilesError(RarExecError):
    """
    No files that match pattern were found

    Example:
        Exercise RarNoFilesError through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class RarUserBreak(RarExecError):
    """
    User stop

    Example:
        Exercise RarUserBreak through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class RarUnknownError(RarExecError):
    """
    Unknown exit code

    Example:
        Exercise RarUnknownError through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


class RarSignalExit(RarExecError):
    """
    Unrar exited with signal

    Example:
        Exercise RarSignalExit through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """


def is_rarfile(fn):
    """
    Check quickly whether file is rar archive.

    Example:
        Exercise is rarfile through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param fn: Value supplied for fn under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """
    buf = open(fn, "rb").read(len(RAR_ID))
    return buf == RAR_ID


class RarInfo(object):
    """
    An entry in rar archive.

    Example:
        Exercise RarInfo through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """

    __slots__ = (
        # zipfile-compatible fields
        "filename",
        "file_size",
        "compress_size",
        "date_time",
        "comment",
        "CRC",
        "volume",
        "orig_filename",  # bytes in unknown encoding
        # rar-specific fields
        "extract_version",
        "compress_type",
        "host_os",
        "mode",
        "type",
        "flags",
        # optional extended time fields
        # tuple where the sec is float, or datetime().
        "mtime",  # same as .date_time
        "ctime",
        "atime",
        "arctime",
        # RAR internals
        "name_size",
        "header_size",
        "header_crc",
        "file_offset",
        "add_size",
        "header_data",
        "header_base",
        "header_offset",
        "salt",
        "volume_file",
    )

    def isdir(self):
        """
        Returns True if the entry is a directory.

        Example:
            Exercise RarInfo.isdir through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.type == RAR_BLOCK_FILE:
            return (self.flags & RAR_FILE_DIRECTORY) == RAR_FILE_DIRECTORY
        return False

    def needs_password(self):
        """
        Perform the needs password utility operation under explicit compatibility rules.

        Example:
            Exercise RarInfo.needs password through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.flags & RAR_FILE_PASSWORD


class RarFile(object):
    """
    Parse RAR structure, provide access to files in archive.

    Example:
        Exercise RarFile through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """

    #: Archive comment.  Byte string or None.  Use UNICODE_COMMENTS
    #: to get automatic decoding to unicode.
    comment = None

    def __init__(self, rarfile, mode="r", charset=None, info_callback=None, crc_check=True):
        """
        Open and parse a RAR archive.

        Example:
            Exercise RarFile.  init   through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param rarfile: Value supplied for rarfile under the utility contract.
        :param mode: Open or adapter mode controlling read/write behavior.
        :param charset: Value supplied for charset under the utility contract.
        :param info_callback: Value supplied for info callback under the utility contract.
        :param crc_check: Value supplied for crc check under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.rarfile = rarfile
        self.comment = None
        self._charset = charset or DEFAULT_CHARSET
        self._info_callback = info_callback

        self._info_list = []
        self._info_map = {}
        self._needs_password = False
        self._password = None
        self._crc_check = crc_check
        self._vol_list = []

        self._main = None

        if mode != "r":
            raise NotImplementedError("RarFile supports only mode=r")

        self._parse()

    def __enter__(self):
        """
        Implement the resource's enter lifecycle operation.

        Example:
            Exercise RarFile.  enter   through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self

    def __exit__(self, type, value, traceback):
        """
        Implement the resource's exit lifecycle operation.

        Example:
            Exercise RarFile.  exit   through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param type: Value supplied for type under the utility contract.
        :param value: Value normalized, stored, formatted or returned.
        :param traceback: Value supplied for traceback under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.close()

    def setpassword(self, password):
        """
        Sets the password to use when extracting.

        Example:
            Exercise RarFile.setpassword through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param password: Value supplied for password under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._password = password
        if not self._main:
            self._parse()

    def needs_password(self):
        """
        Returns True if any archive entries require password for extraction.

        Example:
            Exercise RarFile.needs password through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._needs_password

    def namelist(self):
        """
        Return list of filenames in archive.

        Example:
            Exercise RarFile.namelist through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return [f.filename for f in self._info_list]

    def infolist(self):
        """
        Return RarInfo objects for all files/directories in archive.

        Example:
            Exercise RarFile.infolist through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._info_list

    def volumelist(self):
        """
        Returns filenames of archive volumes.

        Example:
            Exercise RarFile.volumelist through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._vol_list

    def getinfo(self, fname):
        """
        Return RarInfo for file.

        Example:
            Exercise RarFile.getinfo through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param fname: Value supplied for fname under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        if isinstance(fname, RarInfo):
            return fname

        # accept both ways here
        if PATH_SEP == "/":
            fname2 = fname.replace("\\", "/")
        else:
            fname2 = fname.replace("/", "\\")

        try:
            return self._info_map[fname]
        except KeyError:
            try:
                return self._info_map[fname2]
            except KeyError:
                raise NoRarEntry("No such file: " + fname)

    def open(self, fname, mode="r", psw=None):
        """
        Returns file-like object (:class:`RarExtFile`), from where the data can be read.

        Example:
            Exercise RarFile.open through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param fname: Value supplied for fname under the utility contract.
        :param mode: Open or adapter mode controlling read/write behavior.
        :param psw: Value supplied for psw under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        if mode != "r":
            raise NotImplementedError("RarFile.open() supports only mode=r")

        # entry lookup
        inf = self.getinfo(fname)
        if inf.isdir():
            raise TypeError("Directory does not have any data: " + inf.filename)

        if inf.flags & RAR_FILE_SPLIT_BEFORE:
            raise NeedFirstVolume("Partial file, please start from first volume: " + inf.filename)

        # check password
        if inf.needs_password():
            psw = psw or self._password
            if psw is None:
                raise PasswordRequired("File %s requires password" % inf.filename)
        else:
            psw = None

        # is temp write usable?
        if not USE_EXTRACT_HACK or not self._main:
            use_hack = 0
        elif self._main.flags & (RAR_MAIN_SOLID | RAR_MAIN_PASSWORD):
            use_hack = 0
        elif inf.flags & (RAR_FILE_SPLIT_BEFORE | RAR_FILE_SPLIT_AFTER):
            use_hack = 0
        elif inf.file_size > HACK_SIZE_LIMIT:
            use_hack = 0
        else:
            use_hack = 1

        # now extract
        if inf.compress_type == RAR_M0 and (inf.flags & RAR_FILE_PASSWORD) == 0:
            return self._open_clear(inf)
        elif use_hack:
            return self._open_hack(inf, psw)
        else:
            return self._open_unrar(self.rarfile, inf, psw)

    def read(self, fname, psw=None):
        """
        Return uncompressed data for archive entry.

        Example:
            Exercise RarFile.read through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param fname: Value supplied for fname under the utility contract.
        :param psw: Value supplied for psw under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        f = self.open(fname, "r", psw)
        try:
            return f.read()
        finally:
            f.close()

    def close(self):
        """
        Release open resources.

        Example:
            Exercise RarFile.close through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def printdir(self):
        """
        Print archive file list to stdout.

        Example:
            Exercise RarFile.printdir through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for f in self._info_list:
            print(f.filename)

    def extract(self, member, path=None, pwd=None):
        """
        Extract single file into current directory.

        Example:
            Exercise RarFile.extract through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param member: Value supplied for member under the utility contract.
        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param pwd: Value supplied for pwd under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if isinstance(member, RarInfo):
            fname = member.filename
        else:
            fname = member
        self._extract([fname], path, pwd)

    def extractall(self, path=None, members=None, pwd=None):
        """
        Extract all files into current directory.

        Example:
            Exercise RarFile.extractall through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param members: Value supplied for members under the utility contract.
        :param pwd: Value supplied for pwd under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        fnlist = []
        if members is not None:
            for m in members:
                if isinstance(m, RarInfo):
                    fnlist.append(m.filename)
                else:
                    fnlist.append(m)
        self._extract(fnlist, path, pwd)

    def testrar(self):
        """
        Let 'unrar' test the archive.

        Example:
            Exercise RarFile.testrar through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cmd = [UNRAR_TOOL] + list(TEST_ARGS)
        if self._password is not None:
            cmd.append("-p" + self._password)
        else:
            cmd.append("-p-")
        cmd.append(self.rarfile)
        p = custom_popen(cmd)
        output = p.communicate()[0]
        check_returncode(p, output)

    ##
    ## private methods
    ##

    # store entry
    def _process_entry(self, item):
        """
        Perform the process entry utility operation under explicit compatibility rules.

        Example:
            Exercise RarFile. process entry through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param item: Value supplied for item under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if item.type == RAR_BLOCK_FILE:
            # use only first part
            if (item.flags & RAR_FILE_SPLIT_BEFORE) == 0:
                self._info_map[item.filename] = item
                self._info_list.append(item)
                # remember if any items require password
                if item.needs_password():
                    self._needs_password = True
            elif len(self._info_list) > 0:
                # final crc is in last block
                old = self._info_list[-1]
                old.CRC = item.CRC
                old.compress_size += item.compress_size

        # parse new-style comment
        if item.type == RAR_BLOCK_SUB and item.filename == "CMT":
            if not NEED_COMMENTS:
                pass
            elif item.flags & (RAR_FILE_SPLIT_BEFORE | RAR_FILE_SPLIT_AFTER):
                pass
            elif item.flags & RAR_FILE_SOLID:
                # file comment
                cmt = self._read_comment_v3(item, self._password)
                if len(self._info_list) > 0:
                    old = self._info_list[-1]
                    old.comment = cmt
            else:
                # archive comment
                cmt = self._read_comment_v3(item, self._password)
                self.comment = cmt

        if self._info_callback:
            self._info_callback(item)

    # read rar
    def _parse(self):
        """
        Perform the parse utility operation under explicit compatibility rules.

        Example:
            Exercise RarFile. parse through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._fd = None
        try:
            self._parse_real()
        finally:
            if self._fd:
                self._fd.close()
                self._fd = None

    def _parse_real(self):
        """
        Parse real under the documented compatibility and safety rules.

        Example:
            Exercise RarFile. parse real through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        fd = open(self.rarfile, "rb")
        self._fd = fd
        id = fd.read(len(RAR_ID))
        if id != RAR_ID:
            raise NotRarFile("Not a Rar archive: " + self.rarfile)

        volume = 0  # first vol (.rar) is 0
        more_vols = 0
        endarc = 0
        volfile = self.rarfile
        self._vol_list = [self.rarfile]
        while 1:
            if endarc:
                h = None  # don't read past ENDARC
            else:
                h = self._parse_header(fd)
            if not h:
                if more_vols:
                    volume += 1
                    volfile = self._next_volname(volfile)
                    fd.close()
                    fd = open(volfile, "rb")
                    self._fd = fd
                    more_vols = 0
                    endarc = 0
                    self._vol_list.append(volfile)
                    continue
                break
            h.volume = volume
            h.volume_file = volfile

            if h.type == RAR_BLOCK_MAIN and not self._main:
                self._main = h
                if h.flags & RAR_MAIN_NEWNUMBERING:
                    # RAR 2.x does not set FIRSTVOLUME,
                    # so check it only if NEWNUMBERING is used
                    if (h.flags & RAR_MAIN_FIRSTVOLUME) == 0:
                        raise NeedFirstVolume("Need to start from first volume")
                if h.flags & RAR_MAIN_PASSWORD:
                    self._needs_password = True
                    if not self._password:
                        self._main = None
                        break
            elif h.type == RAR_BLOCK_ENDARC:
                more_vols = h.flags & RAR_ENDARC_NEXT_VOLUME
                endarc = 1
            elif h.type == RAR_BLOCK_FILE:
                # RAR 2.x does not write RAR_BLOCK_ENDARC
                if h.flags & RAR_FILE_SPLIT_AFTER:
                    more_vols = 1
                # RAR 2.x does not set RAR_MAIN_FIRSTVOLUME
                if volume == 0 and h.flags & RAR_FILE_SPLIT_BEFORE:
                    raise NeedFirstVolume("Need to start from first volume")

            # store it
            self._process_entry(h)

            # go to next header
            if h.add_size > 0:
                fd.seek(h.file_offset + h.add_size, 0)

    # AES encrypted headers
    _last_aes_key = (None, None, None)  # (salt, key, iv)

    def _decrypt_header(self, fd):
        """
        Perform the decrypt header utility operation under explicit compatibility rules.

        Example:
            Exercise RarFile. decrypt header through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param fd: Value supplied for fd under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not _have_crypto:
            raise NoCrypto("Cannot parse encrypted headers - no crypto")
        salt = fd.read(8)
        if self._last_aes_key[0] == salt:
            key, iv = self._last_aes_key[1:]
        else:
            key, iv = rar3_s2k(self._password, salt)
            self._last_aes_key = (salt, key, iv)
        return HeaderDecrypt(fd, key, iv)

    # read single header
    def _parse_header(self, fd):
        """
        Parse header under the documented compatibility and safety rules.

        Example:
            Exercise RarFile. parse header through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param fd: Value supplied for fd under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            # handle encrypted headers
            if self._main and self._main.flags & RAR_MAIN_PASSWORD:
                if not self._password:
                    return
                fd = self._decrypt_header(fd)

            # now read actual header
            return self._parse_block_header(fd)
        except struct.error:
            if REPORT_BAD_HEADER:
                raise BadRarFile("Broken header in RAR file")
            return None

    # common header
    def _parse_block_header(self, fd):
        """
        Parse block header under the documented compatibility and safety rules.

        Example:
            Exercise RarFile. parse block header through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param fd: Value supplied for fd under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        h = RarInfo()
        h.header_offset = fd.tell()
        h.comment = None

        # read and parse base header
        buf = fd.read(S_BLK_HDR.size)
        if not buf:
            return None
        t = S_BLK_HDR.unpack_from(buf)
        h.header_crc, h.type, h.flags, h.header_size = t
        h.header_base = S_BLK_HDR.size
        pos = S_BLK_HDR.size

        # read full header
        if h.header_size > S_BLK_HDR.size:
            h.header_data = buf + fd.read(h.header_size - S_BLK_HDR.size)
        else:
            h.header_data = buf
        h.file_offset = fd.tell()

        # unexpected EOF?
        if len(h.header_data) != h.header_size:
            if REPORT_BAD_HEADER:
                raise BadRarFile("Unexpected EOF when reading header")
            return None

        # block has data assiciated with it?
        if h.flags & RAR_LONG_BLOCK:
            h.add_size = S_LONG.unpack_from(h.header_data, pos)[0]
        else:
            h.add_size = 0

        # parse interesting ones, decide header boundaries for crc
        if h.type == RAR_BLOCK_MARK:
            return h
        elif h.type == RAR_BLOCK_MAIN:
            h.header_base += 6
            if h.flags & RAR_MAIN_ENCRYPTVER:
                h.header_base += 1
            if h.flags & RAR_MAIN_COMMENT:
                self._parse_subblocks(h, h.header_base)
                self.comment = h.comment
        elif h.type == RAR_BLOCK_FILE:
            self._parse_file_header(h, pos)
        elif h.type == RAR_BLOCK_SUB:
            self._parse_file_header(h, pos)
            h.header_base = h.header_size
        elif h.type == RAR_BLOCK_OLD_AUTH:
            h.header_base += 8
        elif h.type == RAR_BLOCK_OLD_EXTRA:
            h.header_base += 7
        else:
            h.header_base = h.header_size

        # check crc
        if h.type == RAR_BLOCK_OLD_SUB:
            crcdat = h.header_data[2:] + fd.read(h.add_size)
        else:
            crcdat = h.header_data[2 : h.header_base]

        calc_crc = crc32(crcdat) & 0xFFFF

        # return good header
        if h.header_crc == calc_crc:
            return h

        # need to panic?
        if REPORT_BAD_HEADER:
            xlen = len(crcdat)
            crcdat = h.header_data[2:]
            msg = "Header CRC error (%02x): exp=%x got=%x (xlen = %d)" % (
                h.type,
                h.header_crc,
                calc_crc,
                xlen,
            )
            xlen = len(crcdat)
            while xlen >= S_BLK_HDR.size - 2:
                crc = crc32(crcdat[:xlen]) & 0xFFFF
                if crc == h.header_crc:
                    msg += " / crc match, xlen = %d" % xlen
                xlen -= 1
            raise BadRarFile(msg)

        # instead panicing, send eof
        return None

    # read file-specific header
    def _parse_file_header(self, h, pos):
        """
        Parse file header under the documented compatibility and safety rules.

        Example:
            Exercise RarFile. parse file header through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param h: Value supplied for h under the utility contract.
        :param pos: Value supplied for pos under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        fld = S_FILE_HDR.unpack_from(h.header_data, pos)
        h.compress_size = fld[0]
        h.file_size = fld[1]
        h.host_os = fld[2]
        h.CRC = fld[3]
        h.date_time = parse_dos_time(fld[4])
        h.extract_version = fld[5]
        h.compress_type = fld[6]
        h.name_size = fld[7]
        h.mode = fld[8]
        pos += S_FILE_HDR.size

        if h.flags & RAR_FILE_LARGE:
            h1 = S_LONG.unpack_from(h.header_data, pos)[0]
            h2 = S_LONG.unpack_from(h.header_data, pos + 4)[0]
            h.compress_size |= h1 << 32
            h.file_size |= h2 << 32
            pos += 8
            h.add_size = h.compress_size

        name = h.header_data[pos : pos + h.name_size]
        pos += h.name_size
        if h.flags & RAR_FILE_UNICODE:
            nul = name.find(ZERO)
            h.orig_filename = name[:nul]
            u = UnicodeFilename(h.orig_filename, name[nul + 1 :])
            h.filename = u.decode()

            # if parsing failed fall back to simple name
            if u.failed:
                h.filename = self._decode(h.orig_filename)
        else:
            h.orig_filename = name
            h.filename = self._decode(name)

        # change separator, if requested
        if PATH_SEP != "\\":
            h.filename = h.filename.replace("\\", PATH_SEP)

        if h.flags & RAR_FILE_SALT:
            h.salt = h.header_data[pos : pos + 8]
            pos += 8
        else:
            h.salt = None

        # optional extended time stamps
        if h.flags & RAR_FILE_EXTTIME:
            pos = self._parse_ext_time(h, pos)
        else:
            h.mtime = h.atime = h.ctime = h.arctime = None

        # base header end
        h.header_base = pos

        if h.flags & RAR_FILE_COMMENT:
            self._parse_subblocks(h, pos)

        # convert timestamps
        if USE_DATETIME:
            h.date_time = to_datetime(h.date_time)
            h.mtime = to_datetime(h.mtime)
            h.atime = to_datetime(h.atime)
            h.ctime = to_datetime(h.ctime)
            h.arctime = to_datetime(h.arctime)

        # .mtime is .date_time with more precision
        if h.mtime:
            if USE_DATETIME:
                h.date_time = h.mtime
            else:
                # keep seconds int
                h.date_time = h.mtime[:5] + (int(h.mtime[5]),)

        return pos

    # find old-style comment subblock
    def _parse_subblocks(self, h, pos):
        """
        Parse subblocks under the documented compatibility and safety rules.

        Example:
            Exercise RarFile. parse subblocks through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param h: Value supplied for h under the utility contract.
        :param pos: Value supplied for pos under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        hdata = h.header_data
        while pos < len(hdata):
            # ordinary block header
            t = S_BLK_HDR.unpack_from(hdata, pos)
            scrc, stype, sflags, slen = t
            pos_next = pos + slen
            pos += S_BLK_HDR.size

            # corrupt header
            if pos_next < pos:
                break

            # followed by block-specific header
            if stype == RAR_BLOCK_OLD_COMMENT and pos + S_COMMENT_HDR.size <= pos_next:
                declen, ver, meth, crc = S_COMMENT_HDR.unpack_from(hdata, pos)
                pos += S_COMMENT_HDR.size
                data = hdata[pos:pos_next]
                cmt = rar_decompress(ver, meth, data, declen, sflags, crc, self._password)
                if not self._crc_check:
                    h.comment = self._decode_comment(cmt)
                elif crc32(cmt) & 0xFFFF == crc:
                    h.comment = self._decode_comment(cmt)

            pos = pos_next

    def _parse_ext_time(self, h, pos):
        """
        Parse ext time under the documented compatibility and safety rules.

        Example:
            Exercise RarFile. parse ext time through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param h: Value supplied for h under the utility contract.
        :param pos: Value supplied for pos under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        data = h.header_data

        # flags and rest of data can be missing
        flags = 0
        if pos + 2 <= len(data):
            flags = S_SHORT.unpack_from(data, pos)[0]
            pos += 2

        h.mtime, pos = self._parse_xtime(flags >> 3 * 4, data, pos, h.date_time)
        h.ctime, pos = self._parse_xtime(flags >> 2 * 4, data, pos)
        h.atime, pos = self._parse_xtime(flags >> 1 * 4, data, pos)
        h.arctime, pos = self._parse_xtime(flags >> 0 * 4, data, pos)
        return pos

    def _parse_xtime(self, flag, data, pos, dostime=None):
        """
        Parse xtime under the documented compatibility and safety rules.

        Example:
            Exercise RarFile. parse xtime through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param flag: Value supplied for flag under the utility contract.
        :param data: Value supplied for data under the utility contract.
        :param pos: Value supplied for pos under the utility contract.
        :param dostime: Value supplied for dostime under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        unit = 10000000.0  # 100 ns units
        if flag & 8:
            if not dostime:
                t = S_LONG.unpack_from(data, pos)[0]
                dostime = parse_dos_time(t)
                pos += 4
            rem = 0
            cnt = flag & 3
            for i in range(cnt):
                b = S_BYTE.unpack_from(data, pos)[0]
                rem = (b << 16) | (rem >> 8)
                pos += 1
            sec = dostime[5] + rem / unit
            if flag & 4:
                sec += 1
            dostime = dostime[:5] + (sec,)
        return dostime, pos

    # given current vol name, construct next one
    def _next_volname(self, volfile):
        """
        Perform the next volname utility operation under explicit compatibility rules.

        Example:
            Exercise RarFile. next volname through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param volfile: Value supplied for volfile under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self._main.flags & RAR_MAIN_NEWNUMBERING:
            return self._next_newvol(volfile)
        return self._next_oldvol(volfile)

    # new-style next volume
    def _next_newvol(self, volfile):
        """
        Perform the next newvol utility operation under explicit compatibility rules.

        Example:
            Exercise RarFile. next newvol through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param volfile: Value supplied for volfile under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        i = len(volfile) - 1
        while i >= 0:
            if volfile[i] >= "0" and volfile[i] <= "9":
                return self._inc_volname(volfile, i)
            i -= 1
        raise BadRarName("Cannot construct volume name: " + volfile)

    # old-style next volume
    def _next_oldvol(self, volfile):
        # rar -> r00
        """
        Perform the next oldvol utility operation under explicit compatibility rules.

        Example:
            Exercise RarFile. next oldvol through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param volfile: Value supplied for volfile under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if volfile[-4:].lower() == ".rar":
            return volfile[:-2] + "00"
        return self._inc_volname(volfile, len(volfile) - 1)

    # increase digits with carry, otherwise just increment char
    def _inc_volname(self, volfile, i):
        """
        Perform the inc volname utility operation under explicit compatibility rules.

        Example:
            Exercise RarFile. inc volname through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param volfile: Value supplied for volfile under the utility contract.
        :param i: Value supplied for i under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        fn = list(volfile)
        while i >= 0:
            if fn[i] != "9":
                fn[i] = chr(ord(fn[i]) + 1)
                break
            fn[i] = "0"
            i -= 1
        return "".join(fn)

    def _open_clear(self, inf):
        """
        Perform the open clear utility operation under explicit compatibility rules.

        Example:
            Exercise RarFile. open clear through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param inf: Value supplied for inf under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return DirectReader(self, inf)

    # put file compressed data into temporary .rar archive, and run
    # unrar on that, thus avoiding unrar going over whole archive
    def _open_hack(self, inf, psw=None):
        """
        Perform the open hack utility operation under explicit compatibility rules.

        Example:
            Exercise RarFile. open hack through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param inf: Value supplied for inf under the utility contract.
        :param psw: Value supplied for psw under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        BSIZE = 32 * 1024

        size = inf.compress_size + inf.header_size
        rf = open(inf.volume_file, "rb", 0)
        rf.seek(inf.header_offset)

        tmpfd, tmpname = mkstemp(suffix=".rar")
        tmpf = os.fdopen(tmpfd, "wb")

        try:
            # create main header: crc, type, flags, size, res1, res2
            mh = S_BLK_HDR.pack(0x90CF, 0x73, 0, 13) + ZERO * (2 + 4)
            tmpf.write(RAR_ID + mh)
            while size > 0:
                if size > BSIZE:
                    buf = rf.read(BSIZE)
                else:
                    buf = rf.read(size)
                if not buf:
                    raise BadRarFile("read failed: " + inf.filename)
                tmpf.write(buf)
                size -= len(buf)
            tmpf.close()
            rf.close()
        except:
            rf.close()
            tmpf.close()
            os.unlink(tmpname)
            raise

        return self._open_unrar(tmpname, inf, psw, tmpname)

    def _read_comment_v3(self, inf, psw=None):

        # read data
        """
        Read comment v3 under the documented compatibility and safety rules.

        Example:
            Exercise RarFile. read comment v3 through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param inf: Value supplied for inf under the utility contract.
        :param psw: Value supplied for psw under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        rf = open(inf.volume_file, "rb")
        rf.seek(inf.file_offset)
        data = rf.read(inf.compress_size)
        rf.close()

        # decompress
        cmt = rar_decompress(
            inf.extract_version,
            inf.compress_type,
            data,
            inf.file_size,
            inf.flags,
            inf.CRC,
            psw,
            inf.salt,
        )

        # check crc
        if self._crc_check:
            crc = crc32(cmt)
            if crc < 0:
                crc += long(1) << 32
            if crc != inf.CRC:
                return None

        return self._decode_comment(cmt)

    # extract using unrar
    def _open_unrar(self, rarfile, inf, psw=None, tmpfile=None):
        """
        Perform the open unrar utility operation under explicit compatibility rules.

        Example:
            Exercise RarFile. open unrar through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param rarfile: Value supplied for rarfile under the utility contract.
        :param inf: Value supplied for inf under the utility contract.
        :param psw: Value supplied for psw under the utility contract.
        :param tmpfile: Value supplied for tmpfile under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        cmd = [UNRAR_TOOL] + list(OPEN_ARGS)
        if psw is not None:
            cmd.append("-p" + psw)
        cmd.append(rarfile)

        # not giving filename avoids encoding related problems
        if not tmpfile:
            fn = inf.filename
            if PATH_SEP != os.sep:
                fn = fn.replace(PATH_SEP, os.sep)
            cmd.append(fn)

        # read from unrar pipe
        return PipeReader(self, inf, cmd, tmpfile)

    def _decode(self, val):
        """
        Perform the decode utility operation under explicit compatibility rules.

        Example:
            Exercise RarFile. decode through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for c in TRY_ENCODINGS:
            try:
                return val.decode(c)
            except UnicodeError:
                pass
        return val.decode(self._charset, "replace")

    def _decode_comment(self, val):
        """
        Perform the decode comment utility operation under explicit compatibility rules.

        Example:
            Exercise RarFile. decode comment through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if UNICODE_COMMENTS:
            return self._decode(val)
        return val

    # call unrar to extract a file
    def _extract(self, fnlist, path=None, psw=None):
        """
        Perform the extract utility operation under explicit compatibility rules.

        Example:
            Exercise RarFile. extract through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param fnlist: Value supplied for fnlist under the utility contract.
        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param psw: Value supplied for psw under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cmd = [UNRAR_TOOL] + list(EXTRACT_ARGS)

        # pasoword
        psw = psw or self._password
        if psw is not None:
            cmd.append("-p" + psw)
        else:
            cmd.append("-p-")

        # rar file
        cmd.append(self.rarfile)

        # file list
        for fn in fnlist:
            if os.sep != PATH_SEP:
                fn = fn.replace(PATH_SEP, os.sep)
            cmd.append(fn)

        # destination path
        if path is not None:
            cmd.append(path + os.sep)

        # call
        p = custom_popen(cmd)
        output = p.communicate()[0]
        check_returncode(p, output)


##
## Utility classes
##


class UnicodeFilename:
    """
    Handle unicode filename decompression

    Example:
        Exercise UnicodeFilename through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """

    def __init__(self, name, encdata):
        """
        Initialize and validate the UnicodeFilename state.

        Example:
            Exercise UnicodeFilename.  init   through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param encdata: Value supplied for encdata under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.std_name = bytearray(name)
        self.encdata = bytearray(encdata)
        self.pos = self.encpos = 0
        self.buf = bytearray()
        self.failed = 0

    def enc_byte(self):
        """
        Perform the enc byte utility operation under explicit compatibility rules.

        Example:
            Exercise UnicodeFilename.enc byte through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            c = self.encdata[self.encpos]
            self.encpos += 1
            return c
        except IndexError:
            self.failed = 1
            return 0

    def std_byte(self):
        """
        Perform the std byte utility operation under explicit compatibility rules.

        Example:
            Exercise UnicodeFilename.std byte through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return self.std_name[self.pos]
        except IndexError:
            self.failed = 1
            return ord("?")

    def put(self, lo, hi):
        """
        Perform the put utility operation under explicit compatibility rules.

        Example:
            Exercise UnicodeFilename.put through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param lo: Value supplied for lo under the utility contract.
        :param hi: Value supplied for hi under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.buf.append(lo)
        self.buf.append(hi)
        self.pos += 1

    def decode(self):
        """
        Perform the decode utility operation under explicit compatibility rules.

        Example:
            Exercise UnicodeFilename.decode through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        hi = self.enc_byte()
        flagbits = 0
        while self.encpos < len(self.encdata):
            if flagbits == 0:
                flags = self.enc_byte()
                flagbits = 8
            flagbits -= 2
            t = (flags >> flagbits) & 3
            if t == 0:
                self.put(self.enc_byte(), 0)
            elif t == 1:
                self.put(self.enc_byte(), hi)
            elif t == 2:
                self.put(self.enc_byte(), self.enc_byte())
            else:
                n = self.enc_byte()
                if n & 0x80:
                    c = self.enc_byte()
                    for i in range((n & 0x7F) + 2):
                        lo = (self.std_byte() + c) & 0xFF
                        self.put(lo, hi)
                else:
                    for i in range(n + 2):
                        self.put(self.std_byte(), 0)
        return self.buf.decode("utf-16le", "replace")


class RarExtFile(RawIOBase):
    """
    Base class for file-like object that :meth:`RarFile.open` returns.

    Example:
        Exercise RarExtFile through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """

    #: Filename of the archive entry
    name = None

    def __init__(self, rf, inf):
        """
        Initialize and validate the RarExtFile state.

        Example:
            Exercise RarExtFile.  init   through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param rf: Value supplied for rf under the utility contract.
        :param inf: Value supplied for inf under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        RawIOBase.__init__(self)

        # standard io.* properties
        self.name = inf.filename
        self.mode = "rb"

        self.rf = rf
        self.inf = inf
        self.crc_check = rf._crc_check
        self.fd = None
        self.CRC = 0
        self.remain = 0
        self.returncode = 0

        self._open()

    def _open(self):
        """
        Perform the open utility operation under explicit compatibility rules.

        Example:
            Exercise RarExtFile. open through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.fd:
            self.fd.close()
        self.fd = None
        self.CRC = 0
        self.remain = self.inf.file_size

    def read(self, cnt=None):
        """
        Read all or specified amount of data from archive entry.

        Example:
            Exercise RarExtFile.read through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param cnt: Value supplied for cnt under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        # sanitize cnt
        if cnt is None or cnt < 0:
            cnt = self.remain
        elif cnt > self.remain:
            cnt = self.remain
        if cnt == 0:
            return EMPTY

        # actual read
        data = self._read(cnt)
        if data:
            self.CRC = crc32(data, self.CRC)
            self.remain -= len(data)
        if len(data) != cnt:
            raise BadRarFile("Failed the read enough data")

        # done?
        if not data or self.remain == 0:
            # self.close()
            self._check()
        return data

    def _check(self):
        """
        Check final CRC.

        Example:
            Exercise RarExtFile. check through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not self.crc_check:
            return
        if self.returncode:
            check_returncode(self, "")
        if self.remain != 0:
            raise BadRarFile("Failed the read enough data")
        crc = self.CRC
        if crc < 0:
            crc += long(1) << 32
        if crc != self.inf.CRC:
            raise BadRarFile("Corrupt file - CRC check failed: " + self.inf.filename)

    def _read(self, cnt):
        """
        Actual read that gets sanitized cnt.

        Example:
            Exercise RarExtFile. read through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param cnt: Value supplied for cnt under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    def close(self):
        """
        Close open resources.

        Example:
            Exercise RarExtFile.close through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        RawIOBase.close(self)

        if self.fd:
            self.fd.close()
            self.fd = None

    def __del__(self):
        """
        Hook delete to make sure tempfile is removed.

        Example:
            Exercise RarExtFile.  del   through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.close()

    def readinto(self, buf):
        """
        Zero-copy read directly into buffer.

        Example:
            Exercise RarExtFile.readinto through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param buf: Value supplied for buf under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        data = self.read(len(buf))
        n = len(data)
        try:
            buf[:n] = data
        except TypeError:
            import array

            if not isinstance(buf, array.array):
                raise
            buf[:n] = array.array(buf.typecode, data)
        return n

    def tell(self):
        """
        Return current reading position in uncompressed data.

        Example:
            Exercise RarExtFile.tell through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.inf.file_size - self.remain

    def seek(self, ofs, whence=0):
        """
        Seek in data.

        Example:
            Exercise RarExtFile.seek through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param ofs: Value supplied for ofs under the utility contract.
        :param whence: Value supplied for whence under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        # disable crc check when seeking
        self.crc_check = 0

        fsize = self.inf.file_size
        cur_ofs = self.tell()

        if whence == 0:  # seek from beginning of file
            new_ofs = ofs
        elif whence == 1:  # seek from current position
            new_ofs = cur_ofs + ofs
        elif whence == 2:  # seek from end of file
            new_ofs = fsize + ofs
        else:
            raise ValueError("Invalid value for whence")

        # sanity check
        if new_ofs < 0:
            new_ofs = 0
        elif new_ofs > fsize:
            new_ofs = fsize

        # do the actual seek
        if new_ofs >= cur_ofs:
            self._skip(new_ofs - cur_ofs)
        else:
            # process old data ?
            # self._skip(fsize - cur_ofs)
            # reopen and seek
            self._open()
            self._skip(new_ofs)
        return self.tell()

    def _skip(self, cnt):
        """
        Read and discard data

        Example:
            Exercise RarExtFile. skip through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param cnt: Value supplied for cnt under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        while cnt > 0:
            if cnt > 8192:
                buf = self.read(8192)
            else:
                buf = self.read(cnt)
            if not buf:
                break
            cnt -= len(buf)

    def readable(self):
        """
        Returns True

        Example:
            Exercise RarExtFile.readable through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return True

    def writable(self):
        """
        Returns False.

        Example:
            Exercise RarExtFile.writable through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return False

    def seekable(self):
        """
        Returns True.

        Example:
            Exercise RarExtFile.seekable through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return True

    def readall(self):
        """
        Read all remaining data

        Example:
            Exercise RarExtFile.readall through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # avoid RawIOBase default impl
        return self.read()


class PipeReader(RarExtFile):
    """
    Read data from pipe, handle tempfile cleanup.

    Example:
        Exercise PipeReader through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """

    def __init__(self, rf, inf, cmd, tempfile=None):
        """
        Initialize and validate the PipeReader state.

        Example:
            Exercise PipeReader.  init   through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param rf: Value supplied for rf under the utility contract.
        :param inf: Value supplied for inf under the utility contract.
        :param cmd: Value supplied for cmd under the utility contract.
        :param tempfile: Value supplied for tempfile under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.cmd = cmd
        self.proc = None
        self.tempfile = tempfile
        RarExtFile.__init__(self, rf, inf)

    def _close_proc(self):
        """
        Perform the close proc utility operation under explicit compatibility rules.

        Example:
            Exercise PipeReader. close proc through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not self.proc:
            return
        if self.proc.stdout:
            self.proc.stdout.close()
        if self.proc.stdin:
            self.proc.stdin.close()
        if self.proc.stderr:
            self.proc.stderr.close()
        self.proc.wait()
        self.returncode = self.proc.returncode
        self.proc = None

    def _open(self):
        """
        Perform the open utility operation under explicit compatibility rules.

        Example:
            Exercise PipeReader. open through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        RarExtFile._open(self)

        # stop old process
        self._close_proc()

        # launch new process
        self.returncode = 0
        self.proc = custom_popen(self.cmd)
        self.fd = self.proc.stdout

        # avoid situation where unrar waits on stdin
        if self.proc.stdin:
            self.proc.stdin.close()

    def _read(self, cnt):
        """
        Read from pipe.

        Example:
            Exercise PipeReader. read through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param cnt: Value supplied for cnt under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        # normal read is usually enough
        data = self.fd.read(cnt)
        if len(data) == cnt or not data:
            return data

        # short read, try looping
        buf = [data]
        cnt -= len(data)
        while cnt > 0:
            data = self.fd.read(cnt)
            if not data:
                break
            cnt -= len(data)
            buf.append(data)
        return EMPTY.join(buf)

    def close(self):
        """
        Close open resources.

        Example:
            Exercise PipeReader.close through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        self._close_proc()
        RarExtFile.close(self)

        if self.tempfile:
            try:
                os.unlink(self.tempfile)
            except OSError:
                pass
            self.tempfile = None

    if have_memoryview:

        def readinto(self, buf):
            """
            Zero-copy read directly into buffer.

            Example:
                Exercise PipeReader.readinto through a consuming regression::

                    python -m pytest -q tests/utils/decompression/test_archives.py


            :param buf: Value supplied for buf under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            cnt = len(buf)
            if cnt > self.remain:
                cnt = self.remain
            vbuf = memoryview(buf)
            res = got = 0
            while got < cnt:
                res = self.fd.readinto(vbuf[got:cnt])
                if not res:
                    break
                if self.crc_check:
                    self.CRC = crc32(vbuf[got : got + res], self.CRC)
                self.remain -= res
                got += res
            return got


class DirectReader(RarExtFile):
    """
    Read uncompressed data directly from archive.

    Example:
        Exercise DirectReader through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """

    def _open(self):
        """
        Perform the open utility operation under explicit compatibility rules.

        Example:
            Exercise DirectReader. open through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        RarExtFile._open(self)

        self.volfile = self.inf.volume_file
        self.fd = open(self.volfile, "rb", 0)
        self.fd.seek(self.inf.header_offset, 0)
        self.cur = self.rf._parse_header(self.fd)
        self.cur_avail = self.cur.add_size

    def _skip(self, cnt):
        """
        RAR Seek, skipping through rar files to get to correct position

        Example:
            Exercise DirectReader. skip through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param cnt: Value supplied for cnt under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        while cnt > 0:
            # next vol needed?
            if self.cur_avail == 0:
                if not self._open_next():
                    break

            # fd is in read pos, do the read
            if cnt > self.cur_avail:
                cnt -= self.cur_avail
                self.remain -= self.cur_avail
                self.cur_avail = 0
            else:
                self.fd.seek(cnt, 1)
                self.cur_avail -= cnt
                self.remain -= cnt
                cnt = 0

    def _read(self, cnt):
        """
        Read from potentially multi-volume archive.

        Example:
            Exercise DirectReader. read through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param cnt: Value supplied for cnt under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        buf = []
        while cnt > 0:
            # next vol needed?
            if self.cur_avail == 0:
                if not self._open_next():
                    break

            # fd is in read pos, do the read
            if cnt > self.cur_avail:
                data = self.fd.read(self.cur_avail)
            else:
                data = self.fd.read(cnt)
            if not data:
                break

            # got some data
            cnt -= len(data)
            self.cur_avail -= len(data)
            buf.append(data)

        if len(buf) == 1:
            return buf[0]
        return EMPTY.join(buf)

    def _open_next(self):
        """
        Proceed to next volume.

        Example:
            Exercise DirectReader. open next through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        # is the file split over archives?
        if (self.cur.flags & RAR_FILE_SPLIT_AFTER) == 0:
            return False

        if self.fd:
            self.fd.close()
            self.fd = None

        # open next part
        self.volfile = self.rf._next_volname(self.volfile)
        fd = open(self.volfile, "rb", 0)
        self.fd = fd

        # loop until first file header
        while 1:
            cur = self.rf._parse_header(fd)
            if not cur:
                raise BadRarFile("Unexpected EOF")
            if cur.type in (RAR_BLOCK_MARK, RAR_BLOCK_MAIN):
                if cur.add_size:
                    fd.seek(cur.add_size, 1)
                continue
            if cur.orig_filename != self.inf.orig_filename:
                raise BadRarFile("Did not found file entry")
            self.cur = cur
            self.cur_avail = cur.add_size
            return True

    if have_memoryview:

        def readinto(self, buf):
            """
            Zero-copy read directly into buffer.

            Example:
                Exercise DirectReader.readinto through a consuming regression::

                    python -m pytest -q tests/utils/decompression/test_archives.py


            :param buf: Value supplied for buf under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            got = 0
            vbuf = memoryview(buf)
            while got < len(buf):
                # next vol needed?
                if self.cur_avail == 0:
                    if not self._open_next():
                        break

                # lenght for next read
                cnt = len(buf) - got
                if cnt > self.cur_avail:
                    cnt = self.cur_avail

                # read into temp view
                res = self.fd.readinto(vbuf[got : got + cnt])
                if not res:
                    break
                if self.crc_check:
                    self.CRC = crc32(vbuf[got : got + res], self.CRC)
                self.cur_avail -= res
                self.remain -= res
                got += res
            return got


class HeaderDecrypt:
    """
    File-like object that decrypts from another file

    Example:
        Exercise HeaderDecrypt through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """

    def __init__(self, f, key, iv):
        """
        Initialize and validate the HeaderDecrypt state.

        Example:
            Exercise HeaderDecrypt.  init   through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param f: Value supplied for f under the utility contract.
        :param key: Metadata, identifier or local-variable key.
        :param iv: Value supplied for iv under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.f = f
        self.ciph = AES.new(key, AES.MODE_CBC, iv)
        self.buf = EMPTY

    def tell(self):
        """
        Perform the tell utility operation under explicit compatibility rules.

        Example:
            Exercise HeaderDecrypt.tell through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.f.tell()

    def read(self, cnt=None):
        """
        Forward the read operation while preserving adapter ownership rules.

        Example:
            Exercise HeaderDecrypt.read through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param cnt: Value supplied for cnt under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if cnt > 8 * 1024:
            raise BadRarFile("Bad count to header decrypt - wrong password?")

        # consume old data
        if cnt <= len(self.buf):
            res = self.buf[:cnt]
            self.buf = self.buf[cnt:]
            return res
        res = self.buf
        self.buf = EMPTY
        cnt -= len(res)

        # decrypt new data
        BLK = self.ciph.block_size
        while cnt > 0:
            enc = self.f.read(BLK)
            if len(enc) < BLK:
                break
            dec = self.ciph.decrypt(enc)
            if cnt >= len(dec):
                res += dec
                cnt -= len(dec)
            else:
                res += dec[:cnt]
                self.buf = dec[cnt:]
                cnt = 0

        return res


##
## Utility functions
##


def rar3_s2k(psw, salt):
    """
    String-to-key hash for RAR3.

    Example:
        Exercise rar3 s2k through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param psw: Value supplied for psw under the utility contract.
    :param salt: Value supplied for salt under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    seed = psw.encode("utf-16le") + salt
    iv = EMPTY
    h = sha1()
    for i in range(16):
        for j in range(0x4000):
            cnt = S_LONG.pack(i * 0x4000 + j)
            h.update(seed + cnt[:3])
            if j == 0:
                iv += h.digest()[19:20]
    key_be = h.digest()[:16]
    key_le = pack("<LLLL", *unpack(">LLLL", key_be))
    return key_le, iv


def rar_decompress(vers, meth, data, declen=0, flags=0, crc=0, psw=None, salt=None):
    """
    Decompress blob of compressed data.

    Example:
        Exercise rar decompress through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param vers: Value supplied for vers under the utility contract.
    :param meth: Value supplied for meth under the utility contract.
    :param data: Value supplied for data under the utility contract.
    :param declen: Value supplied for declen under the utility contract.
    :param flags: Value supplied for flags under the utility contract.
    :param crc: Value supplied for crc under the utility contract.
    :param psw: Value supplied for psw under the utility contract.
    :param salt: Value supplied for salt under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    # already uncompressed?
    if meth == RAR_M0 and (flags & RAR_FILE_PASSWORD) == 0:
        return data

    # take only necessary flags
    flags = flags & (RAR_FILE_PASSWORD | RAR_FILE_SALT | RAR_FILE_DICTMASK)
    flags |= RAR_LONG_BLOCK

    # file header
    fname = bytes("data", "ascii")
    date = 0
    mode = 0x20
    fhdr = S_FILE_HDR.pack(len(data), declen, RAR_OS_MSDOS, crc, date, vers, meth, len(fname), mode)
    fhdr += fname
    if flags & RAR_FILE_SALT:
        if not salt:
            return EMPTY
        fhdr += salt

    # full header
    hlen = S_BLK_HDR.size + len(fhdr)
    hdr = S_BLK_HDR.pack(0, RAR_BLOCK_FILE, flags, hlen) + fhdr
    hcrc = crc32(hdr[2:]) & 0xFFFF
    hdr = S_BLK_HDR.pack(hcrc, RAR_BLOCK_FILE, flags, hlen) + fhdr

    # archive main header
    mh = S_BLK_HDR.pack(0x90CF, RAR_BLOCK_MAIN, 0, 13) + ZERO * (2 + 4)

    # decompress via temp rar
    tmpfd, tmpname = mkstemp(suffix=".rar")
    tmpf = os.fdopen(tmpfd, "wb")
    try:
        tmpf.write(RAR_ID + mh + hdr + data)
        tmpf.close()

        cmd = [UNRAR_TOOL] + list(OPEN_ARGS)
        if psw is not None and (flags & RAR_FILE_PASSWORD):
            cmd.append("-p" + psw)
        else:
            cmd.append("-p-")
        cmd.append(tmpname)

        p = custom_popen(cmd)
        return p.communicate()[0]
    finally:
        tmpf.close()
        os.unlink(tmpname)


def to_datetime(t):
    """
    Convert 6-part time tuple into datetime object.

    Example:
        Exercise to datetime through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param t: Value supplied for t under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    if t is None:
        return None

    # extract values
    year, mon, day, h, m, xs = t
    s = int(xs)
    us = int(1000000 * (xs - s))

    # assume the values are valid
    try:
        return datetime(year, mon, day, h, m, s, us)
    except ValueError:
        pass

    # sanitize invalid values
    MDAY = (0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
    if mon < 1:
        mon = 1
    if mon > 12:
        mon = 12
    if day < 1:
        day = 1
    if day > MDAY[mon]:
        day = MDAY[mon]
    if h > 23:
        h = 23
    if m > 59:
        m = 59
    if s > 59:
        s = 59
    if mon == 2 and day == 29:
        try:
            return datetime(year, mon, day, h, m, s, us)
        except ValueError:
            day = 28
    return datetime(year, mon, day, h, m, s, us)


def parse_dos_time(stamp):
    """
    Parse standard 32-bit DOS timestamp.

    Example:
        Exercise parse dos time through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param stamp: Value supplied for stamp under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    sec = stamp & 0x1F
    stamp = stamp >> 5
    min = stamp & 0x3F
    stamp = stamp >> 6
    hr = stamp & 0x1F
    stamp = stamp >> 5
    day = stamp & 0x1F
    stamp = stamp >> 5
    mon = stamp & 0x0F
    stamp = stamp >> 4
    yr = (stamp & 0x7F) + 1980
    return (yr, mon, day, hr, min, sec * 2)


def custom_popen(cmd):
    """
    Disconnect cmd from parent fds, read only from stdout.

    Example:
        Exercise custom popen through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param cmd: Value supplied for cmd under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    # needed for py2exe
    creationflags = 0
    if sys.platform == "win32":
        creationflags = 0x08000000  # CREATE_NO_WINDOW

    # run command
    try:
        p = Popen(
            cmd,
            bufsize=0,
            stdout=PIPE,
            stdin=PIPE,
            stderr=STDOUT,
            creationflags=creationflags,
        )
    except OSError:
        ex = sys.exc_info()[1]
        if ex.errno == errno.ENOENT:
            raise RarExecError("Unrar not installed? (rarfile.UNRAR_TOOL=%r)" % UNRAR_TOOL)
        raise
    return p


def check_returncode(p, out):
    """
    Raise exception according to unrar exit code

    Example:
        Exercise check returncode through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param p: Path-like value normalized or validated by the operation.
    :param out: Value supplied for out under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """

    code = p.returncode
    if code == 0:
        return

    # map return code to exception class
    errmap = [
        None,
        RarWarning,
        RarFatalError,
        RarCRCError,
        RarLockedArchiveError,
        RarWriteError,
        RarOpenError,
        RarUserError,
        RarMemoryError,
        RarCreateError,
        RarNoFilesError,
    ]  # codes from rar.txt
    if code > 0 and code < len(errmap):
        exc = errmap[code]
    elif code == 255:
        exc = RarUserBreak
    elif code < 0:
        exc = RarSignalExit
    else:
        exc = RarUnknownError

    # format message
    if out:
        msg = "%s [%d]: %s" % (exc.__doc__, p.returncode, out)
    else:
        msg = "%s [%d]" % (exc.__doc__, p.returncode)

    raise exc(msg)
