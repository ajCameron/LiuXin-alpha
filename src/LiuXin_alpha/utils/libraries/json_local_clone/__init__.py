"""
Expose the supported json local clone compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py
"""

__version__ = '2.0.9'
__all__ = [
    'dump', 'dumps', 'load', 'loads',
    'JSONDecoder', 'JSONDecodeError', 'JSONEncoder',
]

__author__ = 'Bob Ippolito <bob@redivi.com>'

from LiuXin_alpha.utils.libraries.json_local_clone.decoder import JSONDecoder, JSONDecodeError
from LiuXin_alpha.utils.libraries.json_local_clone.encoder import JSONEncoder

import codecs

_default_encoder = JSONEncoder(
    skipkeys=False,
    ensure_ascii=True,
    check_circular=True,
    allow_nan=True,
    indent=None,
    separators=None,
    default=None,
)

def dump(obj, fp, *, skipkeys=False, ensure_ascii=True, check_circular=True,
        allow_nan=True, cls=None, indent=None, separators=None,
        default=None, sort_keys=False, **kw):
    """
    Serialize ``obj`` as a JSON formatted stream to ``fp`` (a ``.write()``-supporting file-like object).

    Example:
        Exercise dump through a consuming regression::

            python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


    :param obj: Value supplied for obj under the utility contract.
    :param fp: Value supplied for fp under the utility contract.
    :param skipkeys: Value supplied for skipkeys under the utility contract.
    :param ensure_ascii: Value supplied for ensure ascii under the utility contract.
    :param check_circular: Value supplied for check circular under the utility contract.
    :param allow_nan: Value supplied for allow nan under the utility contract.
    :param cls: Value supplied for cls under the utility contract.
    :param indent: Value supplied for indent under the utility contract.
    :param separators: Value supplied for separators under the utility contract.
    :param default: Value supplied for default under the utility contract.
    :param sort_keys: Value supplied for sort keys under the utility contract.
    :param kw: Value supplied for kw under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    # cached encoder
    if (not skipkeys and ensure_ascii and
        check_circular and allow_nan and
        cls is None and indent is None and separators is None and
        default is None and not sort_keys and not kw):
        iterable = _default_encoder.iterencode(obj)
    else:
        if cls is None:
            cls = JSONEncoder
        iterable = cls(skipkeys=skipkeys, ensure_ascii=ensure_ascii,
            check_circular=check_circular, allow_nan=allow_nan, indent=indent,
            separators=separators,
            default=default, sort_keys=sort_keys, **kw).iterencode(obj)
    # could accelerate with writelines in some versions of Python, at
    # a debuggability cost
    for chunk in iterable:
        fp.write(chunk)


def dumps(obj, *, skipkeys=False, ensure_ascii=True, check_circular=True,
        allow_nan=True, cls=None, indent=None, separators=None,
        default=None, sort_keys=False, **kw):
    """
    Serialize ``obj`` to a JSON formatted ``str``.

    Example:
        Exercise dumps through a consuming regression::

            python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


    :param obj: Value supplied for obj under the utility contract.
    :param skipkeys: Value supplied for skipkeys under the utility contract.
    :param ensure_ascii: Value supplied for ensure ascii under the utility contract.
    :param check_circular: Value supplied for check circular under the utility contract.
    :param allow_nan: Value supplied for allow nan under the utility contract.
    :param cls: Value supplied for cls under the utility contract.
    :param indent: Value supplied for indent under the utility contract.
    :param separators: Value supplied for separators under the utility contract.
    :param default: Value supplied for default under the utility contract.
    :param sort_keys: Value supplied for sort keys under the utility contract.
    :param kw: Value supplied for kw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    # cached encoder
    if (not skipkeys and ensure_ascii and
        check_circular and allow_nan and
        cls is None and indent is None and separators is None and
        default is None and not sort_keys and not kw):
        return _default_encoder.encode(obj)
    if cls is None:
        cls = JSONEncoder
    return cls(
        skipkeys=skipkeys, ensure_ascii=ensure_ascii,
        check_circular=check_circular, allow_nan=allow_nan, indent=indent,
        separators=separators, default=default, sort_keys=sort_keys,
        **kw).encode(obj)


_default_decoder = JSONDecoder(object_hook=None, object_pairs_hook=None)


def detect_encoding(b):
    """
    Perform the detect encoding utility operation under explicit compatibility rules.

    Example:
        Exercise detect encoding through a consuming regression::

            python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


    :param b: Value supplied for b under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    bstartswith = b.startswith
    if bstartswith((codecs.BOM_UTF32_BE, codecs.BOM_UTF32_LE)):
        return 'utf-32'
    if bstartswith((codecs.BOM_UTF16_BE, codecs.BOM_UTF16_LE)):
        return 'utf-16'
    if bstartswith(codecs.BOM_UTF8):
        return 'utf-8-sig'

    if len(b) >= 4:
        if not b[0]:
            # 00 00 -- -- - utf-32-be
            # 00 XX -- -- - utf-16-be
            return 'utf-16-be' if b[1] else 'utf-32-be'
        if not b[1]:
            # XX 00 00 00 - utf-32-le
            # XX 00 00 XX - utf-16-le
            # XX 00 XX -- - utf-16-le
            return 'utf-16-le' if b[2] or b[3] else 'utf-32-le'
    elif len(b) == 2:
        if not b[0]:
            # 00 XX - utf-16-be
            return 'utf-16-be'
        if not b[1]:
            # XX 00 - utf-16-le
            return 'utf-16-le'
    # default
    return 'utf-8'


def load(fp, *, cls=None, object_hook=None, parse_float=None,
        parse_int=None, parse_constant=None, object_pairs_hook=None, **kw):
    """
    Deserialize ``fp`` (a ``.read()``-supporting file-like object containing a JSON document) to a Python object.

    Example:
        Exercise load through a consuming regression::

            python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


    :param fp: Value supplied for fp under the utility contract.
    :param cls: Value supplied for cls under the utility contract.
    :param object_hook: Value supplied for object hook under the utility contract.
    :param parse_float: Value supplied for parse float under the utility contract.
    :param parse_int: Value supplied for parse int under the utility contract.
    :param parse_constant: Value supplied for parse constant under the utility contract.
    :param object_pairs_hook: Value supplied for object pairs hook under the utility
        contract.
    :param kw: Value supplied for kw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return loads(fp.read(),
        cls=cls, object_hook=object_hook,
        parse_float=parse_float, parse_int=parse_int,
        parse_constant=parse_constant, object_pairs_hook=object_pairs_hook, **kw)


def loads(s, *, cls=None, object_hook=None, parse_float=None,
        parse_int=None, parse_constant=None, object_pairs_hook=None, **kw):
    """
    Deserialize ``s`` (a ``str``, ``bytes`` or ``bytearray`` instance containing a JSON document) to a Python object.

    Example:
        Exercise loads through a consuming regression::

            python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


    :param s: Value supplied for s under the utility contract.
    :param cls: Value supplied for cls under the utility contract.
    :param object_hook: Value supplied for object hook under the utility contract.
    :param parse_float: Value supplied for parse float under the utility contract.
    :param parse_int: Value supplied for parse int under the utility contract.
    :param parse_constant: Value supplied for parse constant under the utility contract.
    :param object_pairs_hook: Value supplied for object pairs hook under the utility
        contract.
    :param kw: Value supplied for kw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(s, str):
        if s.startswith('\ufeff'):
            raise JSONDecodeError("Unexpected UTF-8 BOM (decode using utf-8-sig)",
                                  s, 0)
    else:
        if not isinstance(s, (bytes, bytearray)):
            raise TypeError(f'the JSON object must be str, bytes or bytearray, '
                            f'not {s.__class__.__name__}')
        s = s.decode(detect_encoding(s), 'surrogatepass')

    if (cls is None and object_hook is None and
            parse_int is None and parse_float is None and
            parse_constant is None and object_pairs_hook is None and not kw):
        return _default_decoder.decode(s)
    if cls is None:
        cls = JSONDecoder
    if object_hook is not None:
        kw['object_hook'] = object_hook
    if object_pairs_hook is not None:
        kw['object_pairs_hook'] = object_pairs_hook
    if parse_float is not None:
        kw['parse_float'] = parse_float
    if parse_int is not None:
        kw['parse_int'] = parse_int
    if parse_constant is not None:
        kw['parse_constant'] = parse_constant
    return cls(**kw).decode(s)
