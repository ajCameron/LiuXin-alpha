#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

"""
Expose seekable local ZIP access with explicit member and stream ownership.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise localunzip through a consuming regression::

        python -m pytest -q tests/utils/decompression/test_archives.py
"""
from __future__ import absolute_import, division, print_function, unicode_literals

"""
Try to read invalid zip files with missing or damaged central directories.
These are apparently produced in large numbers by the fruitcakes over at B&N.

Tries to only use the local headers to extract data from the damaged zip file.
"""

import os
import shutil
import sys
import zlib
from collections import OrderedDict, namedtuple
from struct import calcsize, pack, unpack
from tempfile import SpooledTemporaryFile

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


HEADER_SIG = 0x04034B50
HEADER_BYTE_SIG = pack(b"<L", HEADER_SIG)
local_header_fmt = b"<L5HL2L2H"
local_header_sz = calcsize(local_header_fmt)
ZIP_STORED, ZIP_DEFLATED = 0, 8
DATA_DESCRIPTOR_SIG = pack(b"<L", 0x08074B50)

LocalHeader = namedtuple(
    "LocalHeader",
    "signature min_version flags compression_method mod_time mod_date "
    "crc32 compressed_size uncompressed_size filename_length extra_length "
    "filename extra",
)


def decode_arcname(name):
    """
    Perform the decode arcname utility operation under explicit compatibility rules.

    Example:
        Exercise decode arcname through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(name, bytes):
        from LiuXin_alpha.utils.libraries.calibre_chardet import detect

        try:
            name = name.decode("utf-8")
        except:
            res = detect(name)
            encoding = res["encoding"]
            try:
                name = name.decode(encoding)
            except:
                name = name.decode("utf-8", "replace")
    return name


def find_local_header(f):
    """
    Find local header under the documented compatibility and safety rules.

    Example:
        Exercise find local header through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param f: Value supplied for f under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    pos = f.tell()
    raw = f.read(50 * 1024)
    try:
        f.seek(pos + raw.index(HEADER_BYTE_SIG))
    except ValueError:
        f.seek(pos)
        return
    raw = f.read(local_header_sz)
    if len(raw) != local_header_sz:
        f.seek(pos)
        return
    header = LocalHeader(*(unpack(local_header_fmt, raw) + (None, None)))
    if header.signature == HEADER_SIG:
        return header
    f.seek(pos)


def find_data_descriptor(f):
    """
    Find data descriptor under the documented compatibility and safety rules.

    Example:
        Exercise find data descriptor through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param f: Value supplied for f under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    pos = f.tell()
    DD = namedtuple("DataDescriptor", "crc32 compressed_size uncompressed_size")
    raw = b"a" * 16
    try:
        while len(raw) >= 16:
            raw = f.read(50 * 1024)
            idx = raw.find(DATA_DESCRIPTOR_SIG)
            if idx != -1:
                f.seek(f.tell() - len(raw) + idx + len(DATA_DESCRIPTOR_SIG))
                return DD(*unpack(b"<LLL", f.read(12)))
            # Rewind to handle the case of the signature being cut off
            # by the 50K boundary
            f.seek(f.tell() - len(DATA_DESCRIPTOR_SIG))

        raise ValueError(
            "Failed to find data descriptor signature. " "Data descriptors without signatures are not " "supported."
        )
    finally:
        f.seek(pos)


def read_local_file_header(f):
    """
    Read local file header under the documented compatibility and safety rules.

    Example:
        Exercise read local file header through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param f: Value supplied for f under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    pos = f.tell()
    raw = f.read(local_header_sz)
    if len(raw) != local_header_sz:
        f.seek(pos)
        return
    header = LocalHeader(*(unpack(local_header_fmt, raw) + (None, None)))
    if header.signature != HEADER_SIG:
        f.seek(pos)
        header = find_local_header(f)
        if header is None:
            return
    if header.min_version > 20:
        raise ValueError("This ZIP file uses unsupported features")
    if header.flags & 0b1:
        raise ValueError("This ZIP file is encrypted")
    if header.flags & (1 << 13):
        raise ValueError("This ZIP file uses masking, unsupported.")
    if header.compression_method not in {ZIP_STORED, ZIP_DEFLATED}:
        raise ValueError("This ZIP file uses an unsupported compression method")
    has_data_descriptors = header.flags & (1 << 3)
    fname = extra = None
    if header.filename_length > 0:
        fname = f.read(header.filename_length)
        if len(fname) != header.filename_length:
            return
        try:
            fname = fname.decode("ascii")
        except UnicodeDecodeError:
            if header.flags & (1 << 11):
                try:
                    fname = fname.decode("utf-8")
                except UnicodeDecodeError:
                    pass
        fname = decode_arcname(fname).replace("\\", "/")

    if header.extra_length > 0:
        extra = f.read(header.extra_length)
        if len(extra) != header.extra_length:
            return
    if has_data_descriptors:
        desc = find_data_descriptor(f)
        header = header._replace(
            crc32=desc.crc32,
            compressed_size=desc.compressed_size,
            uncompressed_size=desc.uncompressed_size,
        )
    return LocalHeader(*(header[:-2] + (fname, extra)))


def read_compressed_data(f, header):
    """
    Read compressed data under the documented compatibility and safety rules.

    Example:
        Exercise read compressed data through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param f: Value supplied for f under the utility contract.
    :param header: Value supplied for header under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    cdata = f.read(header.compressed_size)
    return cdata


def copy_stored_file(src, size, dest):
    """
    Perform the copy stored file utility operation under explicit compatibility rules.

    Example:
        Exercise copy stored file through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param src: Value supplied for src under the utility contract.
    :param size: Value supplied for size under the utility contract.
    :param dest: Value supplied for dest under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    read = 0
    amt = min(size, 20 * 1024)
    while read < size:
        raw = src.read(min(size - read, amt))
        if not raw:
            raise ValueError("Premature end of file")
        dest.write(raw)
        read += len(raw)


def copy_compressed_file(src, size, dest):
    """
    Perform the copy compressed file utility operation under explicit compatibility rules.

    Example:
        Exercise copy compressed file through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param src: Value supplied for src under the utility contract.
    :param size: Value supplied for size under the utility contract.
    :param dest: Value supplied for dest under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    d = zlib.decompressobj(-15)
    read = 0
    amt = min(size, 20 * 1024)
    while read < size:
        raw = src.read(min(size - read, amt))
        if not raw and read < size:
            raise ValueError("Invalid ZIP file, local header is damaged")
        read += len(raw)
        dest.write(d.decompress(raw, 200 * 1024))
        count = 0
        while d.unconsumed_tail:
            count += 1
            dest.write(d.decompress(d.unconsumed_tail, 200 * 1024))

            if count > 100:
                raise ValueError("This ZIP file contains a ZIP bomb in %s" % os.path.basename(dest.name))


def _extractall(f, path=None, file_info=None):
    """
    Perform the extractall utility operation under explicit compatibility rules.

    Example:
        Exercise  extractall through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param f: Value supplied for f under the utility contract.
    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param file_info: Value supplied for file info under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    found = False
    while True:
        header = read_local_file_header(f)
        if not header:
            break
        has_data_descriptors = header.flags & (1 << 3)
        seekval = header.compressed_size + (16 if has_data_descriptors else 0)
        found = True
        # Sanitize path changing absolute to relative paths and removing .. and
        # .
        fname = header.filename.replace(os.sep, "/")
        fname = os.path.splitdrive(fname)[1]
        parts = [x for x in fname.split("/") if x not in {"", os.path.pardir, os.path.curdir}]
        if not parts:
            continue
        if header.uncompressed_size == 0:
            # Directory
            f.seek(f.tell() + seekval)
            if path is not None:
                bdir = os.path.join(path, *parts)
                if not os.path.exists(bdir):
                    os.makedirs(bdir)
            continue

        # File
        if file_info is not None:
            file_info[header.filename] = (f.tell(), header)
        if path is not None:
            bdir = os.path.join(path, *(parts[:-1]))
            if not os.path.exists(bdir):
                os.makedirs(bdir)
            dest = os.path.join(path, *parts)
            with open(dest, "wb") as o:
                if header.compression_method == ZIP_STORED:
                    copy_stored_file(f, header.compressed_size, o)
                else:
                    copy_compressed_file(f, header.compressed_size, o)
        else:
            f.seek(f.tell() + seekval)

    if not found:
        raise ValueError("Not a ZIP file")


def extractall(path_or_stream, path=None):
    """
    Perform the extractall utility operation under explicit compatibility rules.

    Example:
        Exercise extractall through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param path_or_stream: Value supplied for path or stream under the utility contract.
    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    f = path_or_stream
    close_at_end = False
    if not hasattr(f, "read"):
        f = open(f, "rb")
        close_at_end = True
    if path is None:
        path = os.getcwdu()
    pos = f.tell()
    try:
        _extractall(f, path)
    finally:
        f.seek(pos)
        if close_at_end:
            f.close()


class LocalZipFile(object):
    """
    Provide the LocalZipFile utility contract with explicit state and cleanup behavior.

    Example:
        Exercise LocalZipFile through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """
    def __init__(self, stream):
        """
        Initialize and validate the LocalZipFile state.

        Example:
            Exercise LocalZipFile.  init   through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :return: None; validated state is stored on the receiving object.
        """
        self.file_info = OrderedDict()
        _extractall(stream, file_info=self.file_info)
        self.stream = stream

    def open(self, name, spool_size=5 * 1024 * 1024):
        """
        Perform the open utility operation under explicit compatibility rules.

        Example:
            Exercise LocalZipFile.open through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param spool_size: Value supplied for spool size under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if isinstance(name, LocalHeader):
            name = name.filename
        try:
            offset, header = self.file_info.get(name)
        except KeyError:
            raise ValueError("This ZIP container has no file named: %s" % name)

        self.stream.seek(offset)
        dest = SpooledTemporaryFile(max_size=spool_size)

        if header.compression_method == ZIP_STORED:
            copy_stored_file(self.stream, header.compressed_size, dest)
        else:
            copy_compressed_file(self.stream, header.compressed_size, dest)
        dest.seek(0)
        return dest

    def getinfo(self, name):
        """
        Perform the getinfo utility operation under explicit compatibility rules.

        Example:
            Exercise LocalZipFile.getinfo through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            offset, header = self.file_info.get(name)
        except KeyError:
            raise ValueError("This ZIP container has no file named: %s" % name)
        return header

    def read(self, name, spool_size=5 * 1024 * 1024):
        """
        Forward the read operation while preserving adapter ownership rules.

        Example:
            Exercise LocalZipFile.read through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param spool_size: Value supplied for spool size under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        with self.open(name, spool_size=spool_size) as f:
            return f.read()

    def extractall(self, path=None):
        """
        Perform the extractall utility operation under explicit compatibility rules.

        Example:
            Exercise LocalZipFile.extractall through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.stream.seek(0)
        _extractall(self.stream, path=(path or os.getcwdu()))

    def close(self):
        """
        Forward the close operation while preserving adapter ownership rules.

        Example:
            Exercise LocalZipFile.close through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def safe_replace(self, name, datastream, extra_replacements=None, add_missing=False):
        """
        Preform a safe replace in a damaged file.

        Example:
            Exercise LocalZipFile.safe replace through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param datastream: Value supplied for datastream under the utility contract.
        :param extra_replacements: Value supplied for extra replacements under the utility
            contract.
        :param add_missing: Value supplied for add missing under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if extra_replacements is None:
            extra_replacements = {}

        from LiuXin_alpha.utils.calibre_utils.calibre_zipfile import ZipFile, ZipInfo

        replacements = {name: datastream}
        replacements.update(extra_replacements)
        names = frozenset(replacements.keys())
        found = set([])
        with SpooledTemporaryFile(max_size=100 * 1024 * 1024) as temp:
            ztemp = ZipFile(temp, "w")
            for offset, header in self.file_info.itervalues():
                if header.filename in names:
                    zi = ZipInfo(header.filename)
                    zi.compress_type = header.compression_method
                    ztemp.writestr(zi, replacements[header.filename].read())
                    found.add(header.filename)
                else:
                    ztemp.writestr(header.filename, self.read(header.filename, spool_size=0))
            if add_missing:
                for name in names - found:
                    ztemp.writestr(name, replacements[name].read())
            ztemp.close()
            zipstream = self.stream
            temp.seek(0)
            zipstream.seek(0)
            zipstream.truncate()
            shutil.copyfileobj(temp, zipstream)
            zipstream.flush()


if __name__ == "__main__":
    extractall(sys.argv[-1])
