'''
Support for reading LIT files.
'''
from __future__ import annotations

import typing as _typing

__license__   = 'GPL v3'
__copyright__ = ('2008, Kovid Goyal <kovid at kovidgoyal.net> '
                 'and Marshall T. Vandegrift <llasram@gmail.com>')

import functools
import io
import logging
import os
import re
import struct
from urllib.parse import urldefrag

from LiuXin_alpha.utils.libraries.liuxin_etree import etree

from LiuXin_alpha.file_formats import DRMError
from LiuXin_alpha.file_formats.lit import LitError
import LiuXin_alpha.file_formats.lit.mssha1 as mssha1
from LiuXin_alpha.file_formats.lit.maps import HTML_MAP, OPF_MAP
from LiuXin_alpha.file_formats.oeb.base import urlnormalize, xpath
from LiuXin_alpha.file_formats.oeb.reader import OEBReader
from LiuXin_alpha.utils.plugins import plugins

_lzx_mod, _lzx_err = plugins["lzx"]
_msdes_mod, _msdes_err = plugins["msdes"]

if _lzx_mod is None:
    raise RuntimeError(f"Failed to load lzx plugin: {_lzx_err}")
if _msdes_mod is None:
    raise RuntimeError(f"Failed to load msdes plugin: {_msdes_err}")


class _LZXCompat:

    LZXError = getattr(_lzx_mod, "LZXError", Exception)

    def __init__(self: _typing.Self, mod: _typing.Any) -> None:
        self._mod = mod
        self._state = None

    def init(self: _typing.Self, window_size: _typing.Any) -> None:
        self._state = self._mod.LZXinit(window_size)

    def reset(self: _typing.Self) -> _typing.Any:
        return self._mod.LZXreset(self._state)

    def decompress(self: _typing.Self, content: _typing.Any, outlen: _typing.Any) -> _typing.Any:
        return self._mod.LZXdecompress(self._state, content, outlen)


if all(hasattr(_lzx_mod, name) for name in ("init", "reset", "decompress", "LZXError")):
    lzx = _lzx_mod
elif all(hasattr(_lzx_mod, name) for name in ("LZXinit", "LZXreset", "LZXdecompress", "LZXError")):
    lzx = _LZXCompat(_lzx_mod)
else:
    raise RuntimeError("LZX plugin does not expose a supported API")

msdes = _msdes_mod
from urllib.parse import unquote as urlunquote

__all__ = ["LitReader"]

logger = logging.getLogger(__name__)

XML_DECL = '''<?xml version="1.0" encoding="UTF-8" ?>
'''
OPF_DECL = '''<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE package
  PUBLIC "+//ISBN 0-9673008-1-9//DTD OEB 1.0.1 Package//EN"
  "http://openebook.org/dtds/oeb-1.0.1/oebpkg101.dtd">
'''
HTML_DECL = '''<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE html PUBLIC
 "+//ISBN 0-9673008-1-9//DTD OEB 1.0.1 Document//EN"
 "http://openebook.org/dtds/oeb-1.0.1/oebdoc101.dtd">
'''

DESENCRYPT_GUID = '{67F6E4A2-60BF-11D3-8540-00C04F58C3CF}'
LZXCOMPRESS_GUID = '{0A9007C6-4076-11D3-8789-0000F8105754}'

CONTROL_TAG = 4
CONTROL_WINDOW_SIZE = 12
RESET_NENTRIES = 4
RESET_HDRLEN = 12
RESET_UCLENGTH = 16
RESET_INTERVAL = 32

FLAG_OPENING = (1 << 0)
FLAG_CLOSING = (1 << 1)
FLAG_BLOCK   = (1 << 2)
FLAG_HEAD    = (1 << 3)
FLAG_ATOM    = (1 << 4)


def _unpack_bytes(byts: _typing.Any, size: _typing.Any, label: _typing.Any) -> _typing.Any:
    if len(byts) < size:
        raise LitError(f'Truncated {label}')
    return byts[:size]


def _unpack(fmt: _typing.Any, byts: _typing.Any, size: _typing.Any, label: _typing.Any) -> _typing.Any:
    byts = _unpack_bytes(byts, size, label)
    return struct.unpack(fmt, byts)[0]


def u32(bytes: _typing.Any) -> _typing.Any:
    return _unpack('<L', bytes, 4, '32-bit integer')


def u16(bytes: _typing.Any) -> _typing.Any:
    return _unpack('<H', bytes, 2, '16-bit integer')


def int32(bytes: _typing.Any) -> _typing.Any:
    return _unpack('<l', bytes, 4, 'signed 32-bit integer')


def encint(byts: _typing.Any, remaining: _typing.Any) -> tuple[_typing.Any, ...]:
    pos, val = 0, 0
    ba = bytearray(byts)
    if remaining < 0:
        raise LitError('Invalid encoded integer length')
    last = 0
    while remaining > 0:
        if pos >= len(ba):
            raise LitError('Truncated encoded integer')
        b = ba[pos]
        last = b
        pos += 1
        remaining -= 1
        val <<= 7
        val |= (b & 0x7f)
        if b & 0x80 == 0:
            break
    else:
        if pos and last & 0x80:
            raise LitError('Truncated encoded integer')
    return val, byts[pos:], remaining


def msguid(bytes: _typing.Any) -> _typing.Any:
    values = struct.unpack('<LHHBBBBBBBB', _unpack_bytes(bytes, 16, 'GUID'))
    return '{{{:08X}-{:04X}-{:04X}-{:02X}{:02X}-{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}}}'.format(*values)


def read_utf8_char(bytes: _typing.Any, pos: _typing.Any) -> tuple[_typing.Any, ...]:
    if pos >= len(bytes):
        raise LitError('Invalid UTF8 character: end of input')

    lead = bytes[pos]
    if lead < 0x80:
        return chr(lead), pos + 1

    if 0xC2 <= lead <= 0xDF:
        elsize = 2
        codepoint = lead & 0x1F
        min_codepoint = 0x80
    elif 0xE0 <= lead <= 0xEF:
        elsize = 3
        codepoint = lead & 0x0F
        min_codepoint = 0x800
    elif 0xF0 <= lead <= 0xF4:
        elsize = 4
        codepoint = lead & 0x07
        min_codepoint = 0x10000
    else:
        raise LitError(f'Invalid UTF8 character: {bytes[pos:pos+1]!r}')

    end = pos + elsize
    if end > len(bytes):
        raise LitError(f'Invalid UTF8 character: {bytes[pos:]!r}')

    for i in range(pos + 1, end):
        b = bytes[i]
        if (b & 0xC0) != 0x80:
            raise LitError(f'Invalid UTF8 character: {bytes[pos:end]!r}')
        codepoint = (codepoint << 6) | (b & 0x3F)

    # Reject overlong forms, surrogate code points, and > U+10FFFF.
    if (
        codepoint < min_codepoint
        or 0xD800 <= codepoint <= 0xDFFF
        or codepoint > 0x10FFFF
    ):
        raise LitError(f'Invalid UTF8 character: {bytes[pos:end]!r}')

    return chr(codepoint), end


def consume_sized_utf8_string(bytes: _typing.Any, zpad: bool = False) -> tuple[_typing.Any, ...]:
    result = []
    slen, pos = read_utf8_char(bytes, 0)
    for i in range(ord(slen)):
        char, pos = read_utf8_char(bytes, pos)
        result.append(char)
    if zpad and bytes[pos:pos+1] == b'\0':
        pos += 1
    return ''.join(result), bytes[pos:]


def encode(string: _typing.Any) -> _typing.Any:
    return str(string).encode('ascii', 'xmlcharrefreplace')


class UnBinary:
    AMPERSAND_RE = re.compile(
        br'&(?!(?:#[0-9]+|#x[0-9a-fA-F]+|[a-zA-Z_:][a-zA-Z0-9.-_:]+);)')
    OPEN_ANGLE_RE = re.compile(br'<<(?![!]--)')
    CLOSE_ANGLE_RE = re.compile(br'(?<!--)>>(?=>>|[^>])')
    DOUBLE_ANGLE_RE = re.compile(br'([<>])\1')
    EMPTY_ATOMS = ({},{})

    def __init__(self: _typing.Self, bin: _typing.Any, path: _typing.Any, manifest: dict[_typing.Any, _typing.Any] = {}, map: _typing.Any = HTML_MAP, atoms: _typing.Any = EMPTY_ATOMS) -> None:
        self.manifest = manifest
        self.tag_map, self.attr_map, self.tag_to_attr_map = map
        self.is_html = map is HTML_MAP
        self.tag_atoms, self.attr_atoms = atoms
        self.dir = os.path.dirname(path)
        buf = io.BytesIO()
        self.binary_to_text(bin, buf)
        self.raw = buf.getvalue().lstrip()
        self.escape_reserved()
        self._tree = None

    def escape_reserved(self: _typing.Self) -> None:
        raw = self.raw
        raw = self.AMPERSAND_RE.sub(br'&amp;', raw)
        raw = self.OPEN_ANGLE_RE.sub(br'&lt;', raw)
        raw = self.CLOSE_ANGLE_RE.sub(br'&gt;', raw)
        raw = self.DOUBLE_ANGLE_RE.sub(br'\1', raw)
        self.raw = raw

    def item_path(self: _typing.Self, internal_id: _typing.Any) -> _typing.Any:
        try:
            target = self.manifest[internal_id].path
        except KeyError:
            return internal_id
        if not self.dir:
            return target
        target = target.split('/')
        base = self.dir.split('/')
        for index in range(min(len(base), len(target))):
            if base[index] != target[index]:
                break
        else:
            index += 1
        relpath = (['..'] * (len(base) - index)) + target[index:]
        return '/'.join(relpath)

    @property
    def binary_representation(self: _typing.Self) -> _typing.Any:
        return self.raw

    @property
    def unicode_representation(self: _typing.Self) -> _typing.Any:
        return self.raw.decode('utf-8')

    def __unicode__(self: _typing.Self) -> _typing.Any:
        return self.unicode_representation

    def __str__(self: _typing.Self) -> _typing.Any:
        return self.unicode_representation

    def binary_to_text(self: _typing.Self, bin: _typing.Any, buf: _typing.Any) -> None:
        stack = [(0, None, None, 0, 0, False, False, 'text', 0)]
        self.cpos = 0
        while stack:
            self.binary_to_text_inner(bin, buf, stack)
        del self.cpos

    def binary_to_text_inner(self: _typing.Self, bin: _typing.Any, buf: _typing.Any, stack: _typing.Any) -> None:
        (depth, tag_name, current_map, dynamic_tag, errors,
                in_censorship, is_goingdown, state, flags) = stack.pop()

        if state == 'close tag':
            if not tag_name:
                raise LitError('Tag ends before it begins.')
            buf.write(encode(''.join(('</', tag_name, '>'))))
            dynamic_tag = 0
            tag_name = None
            state = 'text'

        while self.cpos < len(bin):
            c, self.cpos = read_utf8_char(bin, self.cpos)
            oc = ord(c)

            if state == 'text':
                if oc == 0:
                    state = 'get flags'
                    continue
                elif c == '\v':
                    c = '\n'
                elif c == '>':
                    c = '>>'
                elif c == '<':
                    c = '<<'
                buf.write(encode(c))

            elif state == 'get flags':
                if oc == 0:
                    state = 'text'
                    continue
                flags = oc
                state = 'get tag'

            elif state == 'get tag':
                state = 'text' if oc == 0 else 'get attr'
                if flags & FLAG_OPENING:
                    tag = oc
                    buf.write(b'<')
                    if not (flags & FLAG_CLOSING):
                        is_goingdown = True
                    if tag == 0x8000:
                        state = 'get custom length'
                        continue
                    if flags & FLAG_ATOM:
                        if not self.tag_atoms or tag not in self.tag_atoms:
                            raise LitError(
                                f'atom tag {tag} not in atom tag list')
                        tag_name = self.tag_atoms[tag]
                        current_map = self.attr_atoms
                    elif tag < len(self.tag_map):
                        tag_name = self.tag_map[tag]
                        current_map = self.tag_to_attr_map[tag]
                    else:
                        dynamic_tag += 1
                        errors += 1
                        tag_name = '?'+chr(tag)+'?'
                        current_map = self.tag_to_attr_map[tag]
                        logger.warning("Unknown LIT tag encountered: %r", chr(tag))
                    buf.write(encode(tag_name))
                elif flags & FLAG_CLOSING:
                    if depth == 0:
                        raise LitError(f'Extra closing tag {tag_name} at {self.cpos}')
                    break

            elif state == 'get attr':
                in_censorship = False
                if oc == 0:
                    state = 'text'
                    if not is_goingdown:
                        tag_name = None
                        dynamic_tag = 0
                        buf.write(b' />')
                    else:
                        buf.write(b'>')
                        frame = (depth, tag_name, current_map,
                            dynamic_tag, errors, in_censorship, False,
                            'close tag', flags)
                        stack.append(frame)
                        frame = (depth+1, None, None, 0, 0,
                                False, False, 'text', 0)
                        stack.append(frame)
                        break
                else:
                    if oc == 0x8000:
                        state = 'get attr length'
                        continue
                    attr = None
                    if current_map and oc in current_map and current_map[oc]:
                        attr = current_map[oc]
                    elif oc in self.attr_map:
                        attr = self.attr_map[oc]
                    if not attr or not isinstance(attr, (str, bytes)):
                        raise LitError(
                            f'Unknown attribute {oc} in tag {tag_name}')
                    if attr.startswith('%'):
                        in_censorship = True
                        state = 'get value length'
                        continue
                    buf.write(b' ' + encode(attr) + b'=')
                    if attr in ['href', 'src']:
                        state = 'get href length'
                    else:
                        state = 'get value length'

            elif state == 'get value length':
                if not in_censorship:
                    buf.write(b'"')
                count = oc - 1
                if count == 0:
                    if not in_censorship:
                        buf.write(b'"')
                    in_censorship = False
                    state = 'get attr'
                    continue
                state = 'get value'
                if oc == 0xffff:
                    continue
                if count < 0 or count > (len(bin) - self.cpos):
                    raise LitError(f'Invalid character count {count}')

            elif state == 'get value':
                if count == 0xfffe:
                    if not in_censorship:
                        buf.write(encode(f'{oc-1}"'))
                    in_censorship = False
                    state = 'get attr'
                elif count > 0:
                    if not in_censorship:
                        if c == '"':
                            c = '&quot;'
                        elif c == '<':
                            c = '&lt;'
                        if isinstance(c, str):
                            c = c.encode('ascii', 'xmlcharrefreplace')
                        buf.write(c)
                    count -= 1
                if count == 0:
                    if not in_censorship:
                        buf.write(b'"')
                    in_censorship = False
                    state = 'get attr'

            elif state == 'get custom length':
                count = oc - 1
                if count <= 0 or count > len(bin)-self.cpos:
                    raise LitError(f'Invalid character count {count}')
                dynamic_tag += 1
                state = 'get custom'
                tag_name = ''

            elif state == 'get custom':
                tag_name += c
                count -= 1
                if count == 0:
                    buf.write(encode(tag_name))
                    state = 'get attr'

            elif state == 'get attr length':
                count = oc - 1
                if count <= 0 or count > (len(bin) - self.cpos):
                    raise LitError(f'Invalid character count {count}')
                buf.write(b' ')
                state = 'get custom attr'

            elif state == 'get custom attr':
                buf.write(encode(c))
                count -= 1
                if count == 0:
                    buf.write(b'=')
                    state = 'get value length'

            elif state == 'get href length':
                count = oc - 1
                if count <= 0 or count > (len(bin) - self.cpos):
                    raise LitError(f'Invalid character count {count}')
                href = ''
                state = 'get href'

            elif state == 'get href':
                href += c
                count -= 1
                if count == 0:
                    doc, frag = urldefrag(href[1:])
                    path = self.item_path(doc)
                    if frag:
                        path = '#'.join((path, frag))
                    path = urlnormalize(path)
                    buf.write(encode(f'"{path}"'))
                    state = 'get attr'
        if state != 'text':
            raise LitError('Truncated binary markup')


class DirectoryEntry:

    def __init__(self: _typing.Self, name: _typing.Any, section: _typing.Any, offset: _typing.Any, size: _typing.Any) -> None:
        self.name = name
        self.section = section
        self.offset = offset
        self.size = size

    def __repr__(self: _typing.Self) -> str:
        return f'DirectoryEntry(name={self.name!r}, section={self.section}, offset={self.offset}, size={self.size})'

    def __str__(self: _typing.Self) -> _typing.Any:
        return repr(self)


class ManifestItem:

    def __init__(self: _typing.Self, original: _typing.Any, internal: _typing.Any, mime_type: _typing.Any, offset: _typing.Any, root: _typing.Any, state: _typing.Any) -> None:
        self.original = original
        self.internal = internal
        self.mime_type = mime_type.lower() if hasattr(mime_type, 'lower') else mime_type
        self.offset = offset
        self.root = root
        self.state = state
        # Some LIT files have Windows-style paths
        path = original.replace('\\', '/')
        if path[1:3] == ':/':
            path = path[2:]
        # Some paths in Fictionwise "multiformat" LIT files contain '..' (!?)
        path = os.path.normpath(path).replace('\\', '/')
        while path.startswith('../'):
            path = path[3:]
        self.path = path

    def __eq__(self: _typing.Self, other: _typing.Any) -> bool:
        if hasattr(other, 'internal'):
            return self.internal == other.internal
        return self.internal == other

    def __repr__(self: _typing.Self) -> _typing.Any:
        return (
            f'ManifestItem(internal={self.internal!r}, path={self.path!r}, mime_type={self.mime_type!r},'
            f' offset={self.offset}, root={self.root!r}, state={self.state!r})')


def preserve(function: _typing.Any) -> _typing.Any:
    def wrapper(self: _typing.Any, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        opos = self.stream.tell()
        try:
            return function(self, *args, **kwargs)
        finally:
            self.stream.seek(opos)
    functools.update_wrapper(wrapper, function)
    return wrapper


class LitFile:
    PIECE_SIZE = 16

    def __init__(self: _typing.Self, filename_or_stream: _typing.Any, log: _typing.Any) -> None:
        self._warn = log.warn
        if hasattr(filename_or_stream, 'read'):
            self.stream = filename_or_stream
        else:
            self.stream = open(filename_or_stream, 'rb')
        try:
            self.opf_path = os.path.splitext(
                os.path.basename(self.stream.name))[0] + '.opf'
        except AttributeError:
            self.opf_path = 'content.opf'
        if self.magic != b'ITOLITLS':
            raise LitError('Not a valid LIT file')
        if self.version != 1:
            raise LitError(f'Unknown LIT version {self.version}')
        self.read_secondary_header()
        self.read_header_pieces()
        self.read_section_names()
        self.read_manifest()
        self.read_drm()

    def warn(self: _typing.Self, msg: _typing.Any) -> None:
        self._warn(msg)

    def magic() -> _typing.Any:
        @preserve
        def fget(self: _typing.Any) -> _typing.Any:
            self.stream.seek(0)
            return self.stream.read(8)
        return property(fget=fget)
    magic = magic()

    def version() -> _typing.Any:
        def fget(self: _typing.Any) -> _typing.Any:
            self.stream.seek(8)
            return u32(self.stream.read(4))
        return property(fget=fget)
    version = version()

    def hdr_len() -> _typing.Any:
        @preserve
        def fget(self: _typing.Any) -> _typing.Any:
            self.stream.seek(12)
            return int32(self.stream.read(4))
        return property(fget=fget)
    hdr_len = hdr_len()

    def num_pieces() -> _typing.Any:
        @preserve
        def fget(self: _typing.Any) -> _typing.Any:
            self.stream.seek(16)
            return int32(self.stream.read(4))
        return property(fget=fget)
    num_pieces = num_pieces()

    def sec_hdr_len() -> _typing.Any:
        @preserve
        def fget(self: _typing.Any) -> _typing.Any:
            self.stream.seek(20)
            return int32(self.stream.read(4))
        return property(fget=fget)
    sec_hdr_len = sec_hdr_len()

    def guid() -> _typing.Any:
        @preserve
        def fget(self: _typing.Any) -> _typing.Any:
            self.stream.seek(24)
            return self.stream.read(16)
        return property(fget=fget)
    guid = guid()

    def header() -> _typing.Any:
        @preserve
        def fget(self: _typing.Any) -> _typing.Any:
            size = self.hdr_len \
                + (self.num_pieces * self.PIECE_SIZE) \
                + self.sec_hdr_len
            self.stream.seek(0)
            return self.stream.read(size)
        return property(fget=fget)
    header = header()

    @preserve
    def __len__(self: _typing.Self) -> _typing.Any:
        self.stream.seek(0, 2)
        return self.stream.tell()

    @preserve
    def read_raw(self: _typing.Self, offset: _typing.Any, size: _typing.Any) -> _typing.Any:
        self.stream.seek(offset)
        return self.stream.read(size)

    def read_content(self: _typing.Self, offset: _typing.Any, size: _typing.Any) -> _typing.Any:
        return self.read_raw(self.content_offset + offset, size)

    def read_secondary_header(self: _typing.Self) -> None:
        offset = self.hdr_len + (self.num_pieces * self.PIECE_SIZE)
        byts = self.read_raw(offset, self.sec_hdr_len)
        if len(byts) < 8:
            raise LitError('Invalid secondary header')
        offset = int32(byts[4:])
        if offset < 0 or offset > len(byts):
            raise LitError('Invalid secondary header')
        while offset < len(byts):
            if offset + 8 > len(byts):
                raise LitError('Invalid secondary header')
            blocktype = byts[offset:offset+4]
            blockver  = u32(byts[offset+4:])
            if blocktype == b'CAOL':
                if offset + 48 > len(byts):
                    raise LitError('Invalid secondary header')
                if blockver != 2:
                    raise LitError(
                        f'Unknown CAOL block format {blockver}')
                self.creator_id     = u32(byts[offset+12:])
                self.entry_chunklen = u32(byts[offset+20:])
                self.count_chunklen = u32(byts[offset+24:])
                self.entry_unknown  = u32(byts[offset+28:])
                self.count_unknown  = u32(byts[offset+32:])
                offset += 48
            elif blocktype == b'ITSF':
                if offset + 32 > len(byts):
                    raise LitError('Invalid secondary header')
                if blockver != 4:
                    raise LitError(
                        f'Unknown ITSF block format {blockver}')
                if u32(byts[offset+4+16:]):
                    raise LitError('This file has a 64bit content offset')
                self.content_offset = u32(byts[offset+16:])
                self.timestamp      = u32(byts[offset+24:])
                self.language_id    = u32(byts[offset+28:])
                offset += 48
            else:
                raise LitError(f'Unrecognized secondary header block: {blocktype!r}')
        if not hasattr(self, 'content_offset'):
            raise LitError('Could not figure out the content offset')

    def read_header_pieces(self: _typing.Self) -> None:
        src = self.header[self.hdr_len:]
        for i in range(self.num_pieces):
            piece = src[i * self.PIECE_SIZE:(i + 1) * self.PIECE_SIZE]
            if u32(piece[4:]) != 0 or u32(piece[12:]) != 0:
                raise LitError(f'Piece {piece!r} has 64bit value')
            offset, size = u32(piece), int32(piece[8:])
            piece = self.read_raw(offset, size)
            if i == 0:
                continue  # Don't need this piece
            elif i == 1:
                if u32(piece[8:])  != self.entry_chunklen or \
                   u32(piece[12:]) != self.entry_unknown:
                    raise LitError('Secondary header does not match piece')
                self.read_directory(piece)
            elif i == 2:
                if u32(piece[8:])  != self.count_chunklen or \
                   u32(piece[12:]) != self.count_unknown:
                    raise LitError('Secondary header does not match piece')
                continue  # No data needed from this piece
            elif i == 3:
                self.piece3_guid = piece
            elif i == 4:
                self.piece4_guid = piece

    def read_directory(self: _typing.Self, piece: _typing.Any) -> None:
        if not piece.startswith(b'IFCM'):
            raise LitError('Header piece #1 is not main directory.')
        chunk_size, num_chunks = int32(piece[8:12]), int32(piece[24:28])
        if (32 + (num_chunks * chunk_size)) != len(piece):
            raise LitError('IFCM header has incorrect length')
        self.entries = {}
        for i in range(num_chunks):
            offset = 32 + (i * chunk_size)
            chunk = piece[offset:offset + chunk_size]
            tag, chunk = chunk[:4], chunk[4:]
            if tag != b'AOLL':
                continue
            remaining, chunk = int32(chunk[:4]), chunk[4:]
            if remaining >= chunk_size:
                raise LitError('AOLL remaining count is negative')
            remaining = chunk_size - (remaining + 48)
            entries = u16(chunk[-2:])
            if entries == 0:
                # Hopefully will work even without a correct entries count
                entries = (2 ** 16) - 1
            chunk = chunk[40:]
            for j in range(entries):
                if remaining <= 0:
                    break
                namelen, chunk, remaining = encint(chunk, remaining)
                if namelen != (namelen & 0x7fffffff):
                    raise LitError('Directory entry had 64bit name length.')
                if namelen > remaining - 3:
                    raise LitError('Read past end of directory chunk')
                try:
                    name = chunk[:namelen].decode('utf-8')
                    chunk = chunk[namelen:]
                    remaining -= namelen
                except UnicodeDecodeError:
                    break
                section, chunk, remaining = encint(chunk, remaining)
                offset, chunk, remaining = encint(chunk, remaining)
                size, chunk, remaining = encint(chunk, remaining)
                entry = DirectoryEntry(name, section, offset, size)
                self.entries[name] = entry

    def read_section_names(self: _typing.Self) -> None:
        if '::DataSpace/NameList' not in self.entries:
            raise LitError('Lit file does not have a valid NameList')
        raw = self.get_file('::DataSpace/NameList')
        if len(raw) < 4:
            raise LitError('Invalid Namelist section')
        pos = 4
        num_sections = u16(raw[2:pos])
        self.section_names = [''] * num_sections
        self.section_data = [None] * num_sections
        for section in range(num_sections):
            if pos + 2 > len(raw):
                raise LitError('Invalid Namelist section')
            size = u16(raw[pos:pos+2])
            pos += 2
            size = size*2 + 2
            if pos + size > len(raw):
                raise LitError('Invalid Namelist section')
            self.section_names[section] = \
                raw[pos:pos+size].decode('utf-16-le').rstrip('\0')
            pos += size

    def read_manifest(self: _typing.Self) -> None:
        if '/manifest' not in self.entries:
            raise LitError('Lit file does not have a valid manifest')
        raw = self.get_file('/manifest')
        if not raw:
            raise LitError('Truncated manifest')
        self.manifest = {}
        self.paths = {self.opf_path: None}
        while raw:
            slen, raw = ord(raw[0:1]), raw[1:]
            if slen == 0:
                break
            try:
                root, raw = raw[:slen].decode('utf8'), raw[slen:]
            except UnicodeDecodeError as err:
                raise LitError('Manifest contains invalid UTF-8 in root path') from err
            if not raw:
                raise LitError('Truncated manifest')
            for state in ['spine', 'not spine', 'css', 'images']:
                if len(raw) < 4:
                    raise LitError('Truncated manifest')
                num_files, raw = int32(raw), raw[4:]
                if num_files == 0:
                    continue
                for i in range(num_files):
                    if len(raw) < 5:
                        raise LitError('Truncated manifest')
                    offset, raw = u32(raw), raw[4:]
                    internal, raw = consume_sized_utf8_string(raw)
                    original, raw = consume_sized_utf8_string(raw)
                    # The path should be stored unquoted, but not always
                    original = urlunquote(original)
                    # Is this last one UTF-8 or ASCIIZ?
                    mime_type, raw = consume_sized_utf8_string(raw, zpad=True)
                    self.manifest[internal] = ManifestItem(
                        original, internal, mime_type, offset, root, state)
        mlist = list(self.manifest.values())
        # Remove any common path elements
        if len(mlist) > 1:
            shared = mlist[0].path
            for item in mlist[1:]:
                path = item.path
                while shared and not path.startswith(shared):
                    try:
                        shared = shared[:shared.rindex('/', 0, -2) + 1]
                    except ValueError:
                        shared = None
                if not shared:
                    break
            if shared:
                slen = len(shared)
                for item in mlist:
                    item.path = item.path[slen:]
        # Fix any straggling absolute paths
        for item in mlist:
            if item.path and item.path[0] == '/':
                item.path = os.path.basename(item.path)
            self.paths[item.path] = item

    def read_drm(self: _typing.Self) -> None:
        self.drmlevel = 0
        self.bookkey = None
        self.drm_fallback = False
        if '/DRMStorage/Licenses/EUL' in self.entries:
            self.drmlevel = 5
        elif '/DRMStorage/DRMBookplate' in self.entries:
            self.drmlevel = 3
        elif '/DRMStorage/DRMSealed' in self.entries:
            self.drmlevel = 1
        else:
            return
        if self.drmlevel < 5:
            try:
                msdes.deskey(self.calculate_deskey(), msdes.DE1)
                bookkey = msdes.des(self.get_file('/DRMStorage/DRMSealed'))
            except Exception as err:
                self.drm_fallback = True
                self.warn(
                    'Unable to initialize DRM decryption; continuing in best-effort mode: %s'
                    % err
                )
                return
            if bookkey[0:1] != b'\0':
                self.drm_fallback = True
                self.warn(
                    'Unable to decrypt title key; continuing in best-effort mode for unencrypted sections'
                )
                return
            self.bookkey = bookkey[1:9]
        else:
            raise DRMError('Cannot access DRM-protected book')

    def calculate_deskey(self: _typing.Self) -> _typing.Any:
        hashfiles = ['/meta', '/DRMStorage/DRMSource']
        if self.drmlevel == 3:
            hashfiles.append('/DRMStorage/DRMBookplate')
        prepad = 2
        hash = mssha1.new()
        for name in hashfiles:
            data = self.get_file(name)
            if prepad > 0:
                data = (b'\000' * prepad) + data
                prepad = 0
            postpad = 64 - (len(data) % 64)
            if postpad < 64:
                data = data + (b'\000' * postpad)
            hash.update(data)
        digest = hash.digest()
        if not isinstance(digest, bytes):
            digest = digest.encode('ascii')
        digest = bytearray(digest)
        key = bytearray(8)
        for i, d in enumerate(digest):
            key[i % 8] ^= d
        return bytes(key)

    def get_file(self: _typing.Self, name: _typing.Any) -> _typing.Any:
        entry = self.entries[name]
        if entry.section == 0:
            return self.read_content(entry.offset, entry.size)
        section = self.get_section(entry.section)
        return section[entry.offset:entry.offset+entry.size]

    def get_section(self: _typing.Self, section: _typing.Any) -> _typing.Any:
        data = self.section_data[section]
        if not data:
            data = self.get_section_uncached(section)
            self.section_data[section] = data
        return data

    def get_section_uncached(self: _typing.Self, section: _typing.Any) -> _typing.Any:
        name = self.section_names[section]
        path = '::DataSpace/Storage/' + name
        transform = self.get_file(path + '/Transform/List')
        content = self.get_file(path + '/Content')
        control = self.get_file(path + '/ControlData')
        while len(transform) >= 16:
            csize = (int32(control) + 1) * 4
            if csize > len(control) or csize <= 0:
                raise LitError('ControlData is too short')
            guid = msguid(transform)
            if guid == DESENCRYPT_GUID:
                content = self.decrypt(content)
                control = control[csize:]
            elif guid == LZXCOMPRESS_GUID:
                reset_table = self.get_file(
                    '/'.join(('::DataSpace/Storage', name, 'Transform',
                              LZXCOMPRESS_GUID, 'InstanceData/ResetTable')))
                content = self.decompress(content, control, reset_table)
                control = control[csize:]
            else:
                raise LitError(f'Unrecognized transform: {guid!r}.')
            transform = transform[16:]
        return content

    def decrypt(self: _typing.Self, content: _typing.Any) -> _typing.Any:
        if not self.bookkey:
            raise LitError(
                'LIT contains encrypted data but no title key is available'
            )
        length = len(content)
        extra = length & 0x7
        if extra > 0:
            self.warn('content length not a multiple of block size')
            content += b'\0' * (8 - extra)
        msdes.deskey(self.bookkey, msdes.DE1)
        return msdes.des(content)

    def decompress(self: _typing.Self, content: _typing.Any, control: _typing.Any, reset_table: _typing.Any) -> _typing.Any:
        if len(control) < 32 or control[CONTROL_TAG:CONTROL_TAG+4] != b'LZXC':
            raise LitError('Invalid ControlData tag value')
        if len(reset_table) < (RESET_INTERVAL + 8):
            raise LitError('Reset table is too short')
        if u32(reset_table[RESET_UCLENGTH + 4:]) != 0:
            raise LitError('Reset table has 64bit value for UCLENGTH')

        result = []

        window_size = 14
        u = u32(control[CONTROL_WINDOW_SIZE:])
        while u > 0:
            u >>= 1
            window_size += 1
        if window_size < 15 or window_size > 21:
            raise LitError('Invalid window in ControlData')
        lzx.init(window_size)

        ofs_entry = int32(reset_table[RESET_HDRLEN:]) + 8
        uclength = int32(reset_table[RESET_UCLENGTH:])
        accum = int32(reset_table[RESET_INTERVAL:])
        bytes_remaining = uclength
        window_bytes = (1 << window_size)
        base = 0

        while ofs_entry < len(reset_table):
            if accum >= window_bytes:
                accum = 0
                size = int32(reset_table[ofs_entry:])
                u = int32(reset_table[ofs_entry + 4:])
                if u != 0:
                    raise LitError('Reset table entry greater than 32 bits')
                if size >= len(content):
                    self._warn('LZX reset table entry out of bounds')
                if bytes_remaining >= window_bytes:
                    lzx.reset()
                    try:
                        result.append(
                            lzx.decompress(content[base:size], window_bytes))
                    except lzx.LZXError:
                        self.warn('LZX decompression error; skipping chunk')
                    bytes_remaining -= window_bytes
                    base = size
            accum += int32(reset_table[RESET_INTERVAL:])
            ofs_entry += 8
        if bytes_remaining < window_bytes and bytes_remaining > 0:
            lzx.reset()
            try:
                result.append(lzx.decompress(content[base:], bytes_remaining))
            except lzx.LZXError:
                self.warn('LZX decompression error; skipping chunk')
            bytes_remaining = 0
        if bytes_remaining > 0:
            raise LitError('Failed to completely decompress section')
        return b''.join(result)

    def get_atoms(self: _typing.Self, entry: _typing.Any) -> tuple[_typing.Any, ...]:
        name = '/'.join(('/data', entry.internal, 'atom'))
        if name not in self.entries:
            return {}, {}
        data = self.get_file(name)
        nentries, data = u32(data), data[4:]
        tags = {}
        for i in range(1, nentries + 1):
            if len(data) <= 1:
                break
            size, data = ord(data[0:1]), data[1:]
            if size == 0 or len(data) < size:
                break
            tags[i], data = data[:size], data[size:]
        if len(tags) != nentries:
            self._warn('damaged or invalid atoms tag table')
        if len(data) < 4:
            return tags, {}
        attrs = {}
        nentries, data = u32(data), data[4:]
        for i in range(1, nentries + 1):
            if len(data) <= 4:
                break
            size, data = u32(data), data[4:]
            if size == 0 or len(data) < size:
                break
            attrs[i], data = data[:size], data[size:]
        if len(attrs) != nentries:
            self._warn('damaged or invalid atoms attributes table')
        return tags, attrs


class LitContainer:
    '''Simple Container-interface, read-only accessor for LIT files.'''

    def __init__(self: _typing.Self, filename_or_stream: _typing.Any, log: _typing.Any) -> None:
        self._litfile = LitFile(filename_or_stream, log)
        self.log = log

    def namelist(self: _typing.Self) -> _typing.Any:
        return self._litfile.paths.keys()

    def exists(self: _typing.Self, name: _typing.Any) -> bool:
        return urlunquote(name) in self._litfile.paths

    def read(self: _typing.Self, name: _typing.Any) -> _typing.Any:
        entry = self._litfile.paths[urlunquote(name)] if name else None
        if entry is None:
            content = OPF_DECL + self._read_meta()
        elif 'spine' in entry.state:
            internal = '/'.join(('/data', entry.internal, 'content'))
            raw = self._litfile.get_file(internal)
            manifest = self._litfile.manifest
            atoms = self._litfile.get_atoms(entry)
            unbin = UnBinary(raw, name, manifest, HTML_MAP, atoms)
            content = HTML_DECL + unbin.unicode_representation
            tags = ('personname', 'place', 'city', 'country-region')
            pat = r'(?i)</{{0,1}}st1:({})>'.format('|'.join(tags))
            content = re.sub(pat, '', content)
            content = re.sub(r'<(/{0,1})form>', r'<\1div>', content)
        else:
            internal = '/'.join(('/data', entry.internal))
            content = self._litfile.get_file(internal)
        return content

    def _read_meta(self: _typing.Self) -> _typing.Any:
        path = 'content.opf'
        raw = self._litfile.get_file('/meta')
        try:
            unbin = UnBinary(raw, path, self._litfile.manifest, OPF_MAP)
        except LitError:
            if b'PENGUIN group' not in raw:
                raise
            logger.warning("Attempting PENGUIN malformed OPF fix")
            raw = raw.replace(
                b'PENGUIN group', b'\x00\x01\x18\x00PENGUIN group', 1)
            unbin = UnBinary(raw, path, self._litfile.manifest, OPF_MAP)
        return unbin.unicode_representation

    def get_metadata(self: _typing.Self) -> _typing.Any:
        return self._read_meta()


class LitReader(OEBReader):
    Container = LitContainer
    DEFAULT_PROFILE = 'MSReader'

    def _spine_from_opf(self: _typing.Self, opf: _typing.Any) -> None:
        from LiuXin_alpha.file_formats.oeb.base import OEB_DOCS, OEBError

        manifest = self.oeb.manifest
        for elem in xpath(opf, '/o2:package/o2:spine/o2:itemref'):
            idref = elem.get('idref')
            if idref not in manifest.ids:
                continue
            item = manifest.ids[idref]
            if (item.media_type.lower() == 'application/xml' and
                hasattr(item.data, 'xpath') and item.data.xpath('/html')):
                item.media_type = 'application/xhtml+xml'
                item.data = item._parse_xhtml(etree.tostring(item.data))
        try:
            super()._spine_from_opf(opf)
        except OEBError as err:
            if 'Spine is empty' not in str(err):
                raise

            self.log.warn(
                'OPF spine is empty after pruning invalid content; rebuilding from manifest documents'
            )

            added = 0
            for item in sorted(manifest.values(), key=lambda x: x.href):
                media_type = (item.media_type or '').lower()
                if media_type not in OEB_DOCS:
                    continue
                try:
                    data = item.data
                except Exception as e:
                    self.log.warn('Skipping fallback spine item %r: %s' % (item.href, e))
                    continue

                if media_type == 'application/xml' and hasattr(data, 'xpath') and data.xpath('/html'):
                    item.media_type = 'application/xhtml+xml'
                    item.data = item._parse_xhtml(etree.tostring(data))
                    data = item.data

                if hasattr(data, 'xpath') or (
                    hasattr(data, 'tag') and data.tag and data.tag.endswith('}html')
                ):
                    self.oeb.spine.add(item, True)
                    added += 1

            if added == 0:
                raise OEBError(
                    'Spine is empty and no fallback manifest documents were readable'
                ) from err

            self.log.warn('Recovered %d fallback spine document(s)' % added)
            self._spine_add_extra()
            for val in xpath(opf, '/o2:package/o2:spine/@page-progression-direction'):
                if val in {'ltr', 'rtl'}:
                    self.oeb.spine.page_progression_direction = val
