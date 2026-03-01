"""
Read meta information from RB files
"""

import struct

from LiuXin.metadata import string_to_authors
from LiuXin.metadata.metadata import MetaData as MetaInformation

from LiuXin.utils.logger import default_log

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import six_string_types
from LiuXin.utils.lx_libraries.liuxin_six import six_unicode

MAGIC = "\xb0\x0c\xb0\x0c\x02\x00NUVO\x00\x00\x00\x00"

__license__ = "GPL v3"
__copyright__ = "2008, Ashish Kulkarni <kulkarni.ashish@gmail.com>"

VALID_FOR = ["RB"]
PRIORITY_FOR = ["RB"]
RUN_COST = ["LOW"]


def get_metadata(target_file):
    """
    Return metadata as a L{MetaInfo} object
    :param target_file:
    :return:
    """
    stream_needs_close = False

    # constructing the stream, if no stream exists
    if isinstance(target_file, six_string_types):
        stream_needs_close = True
        stream = open(target_file, "rb")
    elif isinstance(target_file, file):
        stream = target_file
    else:
        raise NotImplementedError("target_file type not recongized")

    try:
        return read_metadata_from_stream(stream)
    finally:
        if stream_needs_close:
            stream.close()


def read_metadata_from_stream(stream):
    mi = MetaInformation()
    stream.seek(0)
    try:
        if not stream.read(14) == MAGIC:
            default_log.warn("Couldn't read RB header from file")
            return mi
        stream.read(10)

        def read_i32():
            return struct.unpack("<I", stream.read(4))[0]

        stream.seek(read_i32())
        toc_count = read_i32()

        for i in range(toc_count):
            stream.read(32)
            length, offset, flag = read_i32(), read_i32(), read_i32()
            if flag == 2:
                break
        else:
            default_log.warn("Couldn't find INFO from RB file")
            return mi

        stream.seek(offset)
        info = stream.read(length).splitlines()
        for line in info:
            if "=" not in line:
                continue
            key, value = line.split("=")
            if key.strip() == "TITLE":
                mi.title = value.strip()
            elif key.strip() == "AUTHOR":
                mi.author = value
                mi.authors = string_to_authors(value)
    except Exception as err:
        msg = six_unicode("Couldn't read metadata from rb: %s with error %s") % (
            mi.title,
            six_unicode(err),
        )
        default_log.log_exception(msg, err, "ERROR")
        raise
    return mi
