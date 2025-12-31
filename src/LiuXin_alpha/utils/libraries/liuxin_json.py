#!/usr/bin/env python2
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import print_function

# Utility class which adds the capacity to JSON to encode all the strings in base64.
# This is to get around some annoying, improperly escaped characters in the calibre tweaks, so that they can be all
# merged with preferences

import base64
import re
import sys

try:
    from json.decoder import errmsg
except ImportError:
    errmsg = str
from json.decoder import _decode_uXXXX
from json.decoder import WHITESPACE
from json.decoder import WHITESPACE_STR
from json.decoder import scanstring as original_scanstr

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode, six_string_types as basestring, six_unichar as unichr

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode as unicode



class ParseError(Exception):
    pass


class LiuXinJSON(object):
    def __init__(self):

        import LiuXin_alpha.utils.libraries.json_local_clone as m

        modded_json = m

        modded_json.encoder.encode_basestring_ascii = self.to_base64_str
        modded_json.encoder.encode_basestring = self.to_base64_str

        # Patching the decoder so that it decodes all strings from base64 before returning them
        local_decoder = modded_json._default_decoder

        def scanstring_base64_decoder_wrap(func):
            def scanstring_wrapper(*args, **kwargs):
                scaned_str, end = func(*args, **kwargs)
                # Trim the quotes from the beginning and end of the string
                return self.from_base64_str(base64.b64decode(scaned_str)), end

            return scanstring_wrapper

        self.modded_json = modded_json
        self.local_decoder = local_decoder

        # Patch with the modified functions
        self.local_decoder.parse_string = py_scanstring
        self.local_decoder.parse_object = JSONObject

        # Update the scanner with all the patched functions
        self.local_decoder.scan_once = py_make_scanner(local_decoder)

    @staticmethod
    def to_base64_str(s: str) -> str:
        s_encoded = base64.b64encode(s.encode("utf-8"))
        return '"' + s_encoded.decode("utf-8") + '"'

    @staticmethod
    def from_base64_str(s):
        return base64.b64decode(s)

    def dumps(self, s):
        return self.modded_json.dumps(s)

    def loads(self, s):
        return self.local_decoder.decode(s)


NUMBER_RE = re.compile(
    r"(-?(?:0|[1-9]\d*))(\.\d+)?([eE][-+]?\d+)?",
    (re.VERBOSE | re.MULTILINE | re.DOTALL),
)

FLAGS = re.VERBOSE | re.MULTILINE | re.DOTALL

DEFAULT_ENCODING = "utf-8"
STRINGCHUNK = re.compile(r'(.*?)(["\\\x00-\x1f])', FLAGS)
BACKSLASH = {
    '"': '"',
    "\\": "\\",
    "/": "/",
    "b": "\b",
    "f": "\f",
    "n": "\n",
    "r": "\r",
    "t": "\t",
}


def JSONObject(
    s_and_end,
    encoding,
    strict,
    scan_once,
    object_hook,
    object_pairs_hook,
    _w=WHITESPACE.match,
    _ws=WHITESPACE_STR,
):
    """
    Noew converts all the dictionary keyed back from base64 (if they are strings)
    :param s_and_end:
    :param encoding:
    :param strict:
    :param scan_once:
    :param object_hook:
    :param object_pairs_hook:
    :param _w:
    :param _ws:
    :return:
    """
    s, end = s_and_end
    pairs = []
    pairs_append = pairs.append
    # Use a slice to prevent IndexError from being raised, the following check will raise a more specific ValueError if
    #  the string is empty
    nextchar = s[end : end + 1]

    # Normally we expect nextchar == '"'
    if nextchar != '"':
        if nextchar in _ws:
            end = _w(s, end).end()
            nextchar = s[end : end + 1]

        # Trivial empty object
        if nextchar == "}":
            if object_pairs_hook is not None:
                result = object_pairs_hook(pairs)
                return result, end + 1
            pairs = {}
            if object_hook is not None:
                pairs = object_hook(pairs)
            return pairs, end + 1
        elif nextchar != '"':
            raise ValueError(errmsg("Expecting property name enclosed in double quotes".encode("utf-8"), s, end))
    end += 1

    while True:
        key, end = original_scanstr(s, end, strict)

        # To skip some function call overhead we optimize the fast paths where
        # the JSON key separator is ": " or just ":".
        if s[end : end + 1] != ":":
            end = _w(s, end).end()
            if s[end : end + 1] != ":":
                raise ValueError(errmsg("Expecting ':' delimiter".encode("utf-8"), s, end))
        end += 1

        try:
            if s[end] in _ws:
                end += 1
                if s[end] in _ws:
                    end = _w(s, end + 1).end()
        except IndexError:
            pass

        try:
            value, end = scan_once(s, end)
        except StopIteration:
            raise ValueError(errmsg("Expecting object", s, end))
        pairs_append((key, value))

        try:
            nextchar = s[end]
            if nextchar in _ws:
                end = _w(s, end + 1).end()
                nextchar = s[end]
        except IndexError:
            nextchar = ""
        end += 1

        if nextchar == "}":
            break
        elif nextchar != ",":
            raise ValueError(errmsg("Expecting ',' delimiter", s, end - 1))

        try:
            nextchar = s[end]
            if nextchar in _ws:
                end += 1
                nextchar = s[end]
                if nextchar in _ws:
                    end = _w(s, end + 1).end()
                    nextchar = s[end]
        except IndexError:
            nextchar = ""

        end += 1
        if nextchar != '"':
            raise ValueError(errmsg("Expecting property name enclosed in double quotes", s, end - 1))
    if object_pairs_hook is not None:
        result = object_pairs_hook(pairs)
        return result, end

    new_pairs = []
    for key, val in pairs:
        # Values should be taken care of elsewhere
        if isinstance(key, basestring):
            new_pairs.append((base64.b64decode(key), val))
        else:
            new_pairs.append((key, val))

    pairs = dict(new_pairs)
    if object_hook is not None:
        pairs = object_hook(pairs)
    return pairs, end


def py_scanstring(s, end, encoding=None, strict=True, _b=BACKSLASH, _m=STRINGCHUNK.match):
    """
    Modified to decode any string values from base64 before returing them

    Scan the string s for a JSON string. End is the index of the character in s after the quote that started the JSON
    string.
    Unescapes all valid JSON string escape sequences and raises ValueError on attempt to decode an invalid string.
    If strict is False then literal control characters are allowed in the string.

    Returns a tuple of the decoded string and the index of the character in s
    after the end quote.
    """
    if encoding is None:
        encoding = DEFAULT_ENCODING
    chunks = []
    _append = chunks.append
    begin = end - 1
    while 1:
        chunk = _m(s, end)
        if chunk is None:
            raise ValueError(errmsg("Unterminated string starting at", s, begin))
        end = chunk.end()
        content, terminator = chunk.groups()
        # Content is contains zero or more unescaped string characters
        if content:
            if not isinstance(content, unicode):
                content = six_unicode(content, encoding)
            _append(content)
        # Terminator is the end of string, a literal control character,
        # or a backslash denoting that an escape sequence follows
        if terminator == '"':
            break
        elif terminator != "\\":
            if strict:
                # msg = "Invalid control character %r at" % (terminator,)
                msg = "Invalid control character {0!r} at".format(terminator)
                raise ValueError(errmsg(msg, s, end))
            else:
                _append(terminator)
                continue
        try:
            esc = s[end]
        except IndexError:
            raise ValueError(errmsg("Unterminated string starting at", s, begin))
        # If not a unicode escape sequence, must be in the lookup table
        if esc != "u":
            try:
                char = _b[esc]
            except KeyError:
                msg = "Invalid \\escape: " + repr(esc)
                raise ValueError(errmsg(msg, s, end))
            end += 1
        else:
            # Unicode escape sequence
            uni = _decode_uXXXX(s, end)
            end += 5
            # Check for surrogate pair on UCS-4 systems
            if sys.maxunicode > 65535 and 0xD800 <= uni <= 0xDBFF and s[end : end + 2] == "\\u":
                uni2 = _decode_uXXXX(s, end + 1)
                if 0xDC00 <= uni2 <= 0xDFFF:
                    uni = 0x10000 + (((uni - 0xD800) << 10) | (uni2 - 0xDC00))
                    end += 6
            char = unichr(uni)
        # Append the unescaped character
        _append(char)
    return base64.b64decode("".join(chunks)), end


# Used to construct the scanner after all the other objects have been updated
def py_make_scanner(context):
    """

    Performs the following translations in decoding by default:

    +---------------+-------------------+
    | JSON          | Python            |
    +===============+===================+
    | object        | dict              |
    +---------------+-------------------+
    | array         | list              |
    +---------------+-------------------+
    | string        | unicode           |
    +---------------+-------------------+
    | number (int)  | int, long         |
    +---------------+-------------------+
    | number (real) | float             |
    +---------------+-------------------+
    | true          | True              |
    +---------------+-------------------+
    | false         | False             |
    +---------------+-------------------+
    | null          | None              |
    +---------------+-------------------+

    It also understands ``NaN``, ``Infinity``, and ``-Infinity`` as
    their corresponding ``float`` values, which is outside the JSON spec.

    """
    parse_object = context.parse_object
    parse_array = context.parse_array
    parse_string = context.parse_string
    match_number = NUMBER_RE.match
    # encoding = context.encoding
    encoding = "utf-8"
    strict = context.strict
    parse_float = context.parse_float
    parse_int = context.parse_int
    parse_constant = context.parse_constant
    object_hook = context.object_hook
    object_pairs_hook = context.object_pairs_hook

    def _scan_once(string, idx):
        try:
            nextchar = string[idx]
        except IndexError:
            # important: provide the position
            raise StopIteration(idx) from None

        if nextchar == '"':
            return parse_string(string, idx + 1, encoding, strict)
        elif nextchar == "{":
            return parse_object(
                (string, idx + 1),
                encoding,
                strict,
                _scan_once,
                object_hook,
                object_pairs_hook,
            )
        elif nextchar == "[":
            return parse_array((string, idx + 1), _scan_once)
        elif nextchar == "n" and string[idx : idx + 4] == "null":
            return None, idx + 4
        elif nextchar == "t" and string[idx : idx + 4] == "true":
            return True, idx + 4
        elif nextchar == "f" and string[idx : idx + 5] == "false":
            return False, idx + 5

        m = match_number(string, idx)
        if m is not None:
            integer, frac, exp = m.groups()
            if frac or exp:
                res = parse_float(integer + (frac or "") + (exp or ""))
            else:
                res = parse_int(integer)
            return res, m.end()
        elif nextchar == "N" and string[idx : idx + 3] == "NaN":
            return parse_constant("NaN"), idx + 3
        elif nextchar == "I" and string[idx : idx + 8] == "Infinity":
            return parse_constant("Infinity"), idx + 8
        elif nextchar == "-" and string[idx : idx + 9] == "-Infinity":
            return parse_constant("-Infinity"), idx + 9
        else:
            raise StopIteration

    return _scan_once
