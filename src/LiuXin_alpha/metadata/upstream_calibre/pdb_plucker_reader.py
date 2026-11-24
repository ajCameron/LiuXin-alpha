# License: GPLv3 Copyright: 20011, John Schember <john@nachtimwald.com>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/pdb/plucker/reader.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat.
# Only DATATYPE_METADATA, MIBNUM_TO_NAME and SectionHeader are ported, with the imports they use;
# the rest is conversion code the metadata readers do not use.
# SPDX-License-Identifier: GPL-3.0-only

import struct


DATATYPE_METADATA = 10


# IETF IANA MIBenum value for the character set.
# See the http://www.iana.org/assignments/character-sets for valid values.
# Not all character sets are handled by Python. This is a small subset that
# the MIBenum maps to Python standard encodings
# from http://docs.python.org/library/codecs.html#standard-encodings
MIBNUM_TO_NAME = {
    3: 'ascii',
    4: 'latin_1',
    5: 'iso8859_2',
    6: 'iso8859_3',
    7: 'iso8859_4',
    8: 'iso8859_5',
    9: 'iso8859_6',
    10: 'iso8859_7',
    11: 'iso8859_8',
    12: 'iso8859_9',
    13: 'iso8859_10',
    17: 'shift_jis',
    18: 'euc_jp',
    27: 'utf_7',
    36: 'euc_kr',
    37: 'iso2022_kr',
    38: 'euc_kr',
    39: 'iso2022_jp',
    40: 'iso2022_jp_2',
    106: 'utf-8',
    109: 'iso8859_13',
    110: 'iso8859_14',
    111: 'iso8859_15',
    112: 'iso8859_16',
    1013: 'utf_16_be',
    1014: 'utf_16_le',
    1015: 'utf_16',
    2009: 'cp850',
    2010: 'cp852',
    2011: 'cp437',
    2013: 'cp862',
    2025: 'gb2312',
    2026: 'big5',
    2028: 'cp037',
    2043: 'cp424',
    2044: 'cp500',
    2046: 'cp855',
    2047: 'cp857',
    2048: 'cp860',
    2049: 'cp861',
    2050: 'cp863',
    2051: 'cp864',
    2052: 'cp865',
    2054: 'cp869',
    2063: 'cp1026',
    2085: 'hz',
    2086: 'cp866',
    2087: 'cp775',
    2089: 'cp858',
    2091: 'cp1140',
    2102: 'big5hkscs',
    2250: 'cp1250',
    2251: 'cp1251',
    2252: 'cp1252',
    2253: 'cp1253',
    2254: 'cp1254',
    2255: 'cp1255',
    2256: 'cp1256',
    2257: 'cp1257',
    2258: 'cp1258',
}


class SectionHeader:
    """
    Every sections (record) has this header. It gives
    details about the section such as it's uid.
    """

    def __init__(self, raw: bytes) -> None:
        (self.uid,) = struct.unpack('>H', raw[0:2])
        (self.paragraphs,) = struct.unpack('>H', raw[2:4])
        (self.size,) = struct.unpack('>H', raw[4:6])
        (self.type,) = struct.unpack('>B', raw[6:7])
        (self.flags,) = struct.unpack('>B', raw[7:8])
