#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import unicode_literals, division, absolute_import, print_function

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def base64_decode(raw):
    """
    Preform a base 64 decode of a raw string.
    :param raw:
    :return:
    """
    from io import BytesIO
    from base64 import b64decode

    # First try the python implementation as it is faster
    try:
        return b64decode(raw)
    except TypeError:
        pass

    # Try a more robust version (adapted from FBReader sources)
    # cap_a = A and cap_z = Z
    cap_a, cap_z, a, z, zero, nine, plus, slash, equal = bytearray(b"AZaz09+/=")
    raw = bytearray(raw)
    out = BytesIO()
    pos = 0
    while pos < len(raw):
        tot = 0
        i = 0
        while i < 4 and pos < len(raw):
            byt = raw[pos]
            pos += 1
            if cap_a <= byt <= cap_z:
                num = byt - cap_a
            elif a <= byt <= z:
                num = byt - a + 26
            elif zero <= byt <= nine:
                num = byt - zero + 52
            else:
                num = {plus: 62, slash: 63, equal: 64}.get(byt, None)
                if num is None:
                    # Ignore this byte
                    continue
            tot += num << (6 * (3 - i))
            i += 1
        triple = bytearray(3)
        for j in (2, 1, 0):
            triple[j] = tot & 0xFF
            tot >>= 8
        out.write(bytes(triple))
    return out.getvalue()
