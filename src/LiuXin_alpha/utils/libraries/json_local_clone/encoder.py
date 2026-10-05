"""
Encode Python values through the bundled local JSON implementation.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise encoder through a consuming regression::

        python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py
"""
import re

try:
    from _json import encode_basestring_ascii as c_encode_basestring_ascii
except ImportError:
    c_encode_basestring_ascii = None
try:
    from _json import encode_basestring as c_encode_basestring
except ImportError:
    c_encode_basestring = None
try:
    from _json import make_encoder as c_make_encoder
except ImportError:
    c_make_encoder = None

ESCAPE = re.compile(r'[\x00-\x1f\\"\b\f\n\r\t]')
ESCAPE_ASCII = re.compile(r'([\\"]|[^\ -~])')
HAS_UTF8 = re.compile(b'[\x80-\xff]')
ESCAPE_DCT = {
    '\\': '\\\\',
    '"': '\\"',
    '\b': '\\b',
    '\f': '\\f',
    '\n': '\\n',
    '\r': '\\r',
    '\t': '\\t',
}
for i in range(0x20):
    ESCAPE_DCT.setdefault(chr(i), '\\u{0:04x}'.format(i))
    #ESCAPE_DCT.setdefault(chr(i), '\\u%04x' % (i,))
del i

INFINITY = float('inf')

def py_encode_basestring(s):
    """
    Return a JSON representation of a Python string

    Example:
        Exercise py encode basestring through a consuming regression::

            python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


    :param s: Value supplied for s under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def replace(match):
        """
        Perform the replace utility operation under explicit compatibility rules.

        Example:
            Exercise py encode basestring.replace through a consuming regression::

                python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


        :param match: Value supplied for match under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return ESCAPE_DCT[match.group(0)]
    return '"' + ESCAPE.sub(replace, s) + '"'


encode_basestring = (c_encode_basestring or py_encode_basestring)


def py_encode_basestring_ascii(s):
    """
    Return an ASCII-only JSON representation of a Python string

    Example:
        Exercise py encode basestring ascii through a consuming regression::

            python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


    :param s: Value supplied for s under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def replace(match):
        """
        Perform the replace utility operation under explicit compatibility rules.

        Example:
            Exercise py encode basestring ascii.replace through a consuming regression::

                python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


        :param match: Value supplied for match under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        s = match.group(0)
        try:
            return ESCAPE_DCT[s]
        except KeyError:
            n = ord(s)
            if n < 0x10000:
                return '\\u{0:04x}'.format(n)
                #return '\\u%04x' % (n,)
            else:
                # surrogate pair
                n -= 0x10000
                s1 = 0xd800 | ((n >> 10) & 0x3ff)
                s2 = 0xdc00 | (n & 0x3ff)
                return '\\u{0:04x}\\u{1:04x}'.format(s1, s2)
    return '"' + ESCAPE_ASCII.sub(replace, s) + '"'


encode_basestring_ascii = (
    c_encode_basestring_ascii or py_encode_basestring_ascii)

class JSONEncoder(object):
    """
    Extensible JSON <https://json.org> encoder for Python data structures.

    Example:
        Exercise JSONEncoder through a consuming regression::

            python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py
    """
    item_separator = ', '
    key_separator = ': '
    def __init__(self, *, skipkeys=False, ensure_ascii=True,
            check_circular=True, allow_nan=True, sort_keys=False,
            indent=None, separators=None, default=None):
        """
        Constructor for JSONEncoder, with sensible defaults.

        Example:
            Exercise JSONEncoder.  init   through a consuming regression::

                python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


        :param skipkeys: Value supplied for skipkeys under the utility contract.
        :param ensure_ascii: Value supplied for ensure ascii under the utility contract.
        :param check_circular: Value supplied for check circular under the utility contract.
        :param allow_nan: Value supplied for allow nan under the utility contract.
        :param sort_keys: Value supplied for sort keys under the utility contract.
        :param indent: Value supplied for indent under the utility contract.
        :param separators: Value supplied for separators under the utility contract.
        :param default: Value supplied for default under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """

        self.skipkeys = skipkeys
        self.ensure_ascii = ensure_ascii
        self.check_circular = check_circular
        self.allow_nan = allow_nan
        self.sort_keys = sort_keys
        self.indent = indent
        if separators is not None:
            self.item_separator, self.key_separator = separators
        elif indent is not None:
            self.item_separator = ','
        if default is not None:
            self.default = default

    def default(self, o):
        """
        Implement this method in a subclass such that it returns a serializable object for ``o``, or calls the base implementation (to raise a ``TypeError``).

        Example:
            Exercise JSONEncoder.default through a consuming regression::

                python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


        :param o: Value supplied for o under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise TypeError(f'Object of type {o.__class__.__name__} '
                        f'is not JSON serializable')

    def encode(self, o):
        """
        Return a JSON string representation of a Python data structure.

        Example:
            Exercise JSONEncoder.encode through a consuming regression::

                python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


        :param o: Value supplied for o under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # This is for extremely simple cases and benchmarks.
        if isinstance(o, str):
            if self.ensure_ascii:
                return encode_basestring_ascii(o)
            else:
                return encode_basestring(o)
        # This doesn't pass the iterator directly to ''.join() because the
        # exceptions aren't as detailed.  The list call should be roughly
        # equivalent to the PySequence_Fast that ''.join() would do.
        chunks = self.iterencode(o, _one_shot=True)
        if not isinstance(chunks, (list, tuple)):
            chunks = list(chunks)
        return ''.join(chunks)

    def iterencode(self, o, _one_shot=False):
        """
        Encode the given object and yield each string representation as available.

        Example:
            Exercise JSONEncoder.iterencode through a consuming regression::

                python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


        :param o: Value supplied for o under the utility contract.
        :param _one_shot: Value supplied for one shot under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.check_circular:
            markers = {}
        else:
            markers = None
        if self.ensure_ascii:
            _encoder = encode_basestring_ascii
        else:
            _encoder = encode_basestring

        def floatstr(o, allow_nan=self.allow_nan,
                _repr=float.__repr__, _inf=INFINITY, _neginf=-INFINITY):
            # Check for specials.  Note that this type of test is processor
            # and/or platform-specific, so do tests which don't depend on the
            # internals.

            """
            Perform the floatstr utility operation under explicit compatibility rules.

            Example:
                Exercise JSONEncoder.iterencode.floatstr through a consuming regression::

                    python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


            :param o: Value supplied for o under the utility contract.
            :param allow_nan: Value supplied for allow nan under the utility contract.
            :param _repr: Value supplied for repr under the utility contract.
            :param _inf: Value supplied for inf under the utility contract.
            :param _neginf: Value supplied for neginf under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if o != o:
                text = 'NaN'
            elif o == _inf:
                text = 'Infinity'
            elif o == _neginf:
                text = '-Infinity'
            else:
                return _repr(o)

            if not allow_nan:
                raise ValueError(
                    "Out of range float values are not JSON compliant: " +
                    repr(o))

            return text


        if self.indent is None or isinstance(self.indent, str):
            indent = self.indent
        else:
            indent = ' ' * self.indent
        if _one_shot and c_make_encoder is not None:
            _iterencode = c_make_encoder(
                markers, self.default, _encoder, indent,
                self.key_separator, self.item_separator, self.sort_keys,
                self.skipkeys, self.allow_nan)
        else:
            _iterencode = _make_iterencode(
                markers, self.default, _encoder, indent, floatstr,
                self.key_separator, self.item_separator, self.sort_keys,
                self.skipkeys, _one_shot)
        return _iterencode(o, 0)

def _make_iterencode(markers, _default, _encoder, _indent, _floatstr,
        _key_separator, _item_separator, _sort_keys, _skipkeys, _one_shot,
        ## HACK: hand-optimized bytecode; turn globals into locals
        ValueError=ValueError,
        dict=dict,
        float=float,
        id=id,
        int=int,
        isinstance=isinstance,
        list=list,
        str=str,
        tuple=tuple,
        _intstr=int.__repr__,
    ):

    """
    Perform the make iterencode utility operation under explicit compatibility rules.

    Example:
        Exercise  make iterencode through a consuming regression::

            python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


    :param markers: Value supplied for markers under the utility contract.
    :param _default: Value supplied for default under the utility contract.
    :param _encoder: Value supplied for encoder under the utility contract.
    :param _indent: Value supplied for indent under the utility contract.
    :param _floatstr: Value supplied for floatstr under the utility contract.
    :param _key_separator: Value supplied for key separator under the utility contract.
    :param _item_separator: Value supplied for item separator under the utility
        contract.
    :param _sort_keys: Value supplied for sort keys under the utility contract.
    :param _skipkeys: Value supplied for skipkeys under the utility contract.
    :param _one_shot: Value supplied for one shot under the utility contract.
    :param ValueError: Value supplied for ValueError under the utility contract.
    :param dict: Value supplied for dict under the utility contract.
    :param float: Value supplied for float under the utility contract.
    :param id: Value supplied for id under the utility contract.
    :param int: Value supplied for int under the utility contract.
    :param isinstance: Value supplied for isinstance under the utility contract.
    :param list: Value supplied for list under the utility contract.
    :param str: Value supplied for str under the utility contract.
    :param tuple: Value supplied for tuple under the utility contract.
    :param _intstr: Value supplied for intstr under the utility contract.
    :return: An iterator yielding the normalized values described above.
    """
    def _iterencode_list(lst, _current_indent_level):
        """
        Perform the iterencode list utility operation under explicit compatibility rules.

        Example:
            Exercise  make iterencode. iterencode list through a consuming regression::

                python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


        :param lst: Value supplied for lst under the utility contract.
        :param _current_indent_level: Value supplied for current indent level under the
            utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        if not lst:
            yield '[]'
            return
        if markers is not None:
            markerid = id(lst)
            if markerid in markers:
                raise ValueError("Circular reference detected")
            markers[markerid] = lst
        buf = '['
        if _indent is not None:
            _current_indent_level += 1
            newline_indent = '\n' + _indent * _current_indent_level
            separator = _item_separator + newline_indent
            buf += newline_indent
        else:
            newline_indent = None
            separator = _item_separator
        for i, value in enumerate(lst):
            if i:
                buf = separator
            try:
                if isinstance(value, str):
                    yield buf + _encoder(value)
                elif value is None:
                    yield buf + 'null'
                elif value is True:
                    yield buf + 'true'
                elif value is False:
                    yield buf + 'false'
                elif isinstance(value, int):
                    # Subclasses of int/float may override __repr__, but we still
                    # want to encode them as integers/floats in JSON. One example
                    # within the standard library is IntEnum.
                    yield buf + _intstr(value)
                elif isinstance(value, float):
                    # see comment above for int
                    yield buf + _floatstr(value)
                else:
                    yield buf
                    if isinstance(value, (list, tuple)):
                        chunks = _iterencode_list(value, _current_indent_level)
                    elif isinstance(value, dict):
                        chunks = _iterencode_dict(value, _current_indent_level)
                    else:
                        chunks = _iterencode(value, _current_indent_level)
                    yield from chunks
            except GeneratorExit:
                raise
            except BaseException as exc:
                exc.add_note(f'when serializing {type(lst).__name__} item {i}')
                raise
        if newline_indent is not None:
            _current_indent_level -= 1
            yield '\n' + _indent * _current_indent_level
        yield ']'
        if markers is not None:
            del markers[markerid]

    def _iterencode_dict(dct, _current_indent_level):
        """
        Perform the iterencode dict utility operation under explicit compatibility rules.

        Example:
            Exercise  make iterencode. iterencode dict through a consuming regression::

                python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


        :param dct: Value supplied for dct under the utility contract.
        :param _current_indent_level: Value supplied for current indent level under the
            utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        if not dct:
            yield '{}'
            return
        if markers is not None:
            markerid = id(dct)
            if markerid in markers:
                raise ValueError("Circular reference detected")
            markers[markerid] = dct
        yield '{'
        if _indent is not None:
            _current_indent_level += 1
            newline_indent = '\n' + _indent * _current_indent_level
            item_separator = _item_separator + newline_indent
        else:
            newline_indent = None
            item_separator = _item_separator
        first = True
        if _sort_keys:
            items = sorted(dct.items())
        else:
            items = dct.items()
        for key, value in items:
            if isinstance(key, str):
                pass
            # JavaScript is weakly typed for these, so it makes sense to
            # also allow them.  Many encoders seem to do something like this.
            elif isinstance(key, float):
                # see comment for int/float in _make_iterencode
                key = _floatstr(key)
            elif key is True:
                key = 'true'
            elif key is False:
                key = 'false'
            elif key is None:
                key = 'null'
            elif isinstance(key, int):
                # see comment for int/float in _make_iterencode
                key = _intstr(key)
            elif _skipkeys:
                continue
            else:
                raise TypeError(f'keys must be str, int, float, bool or None, '
                                f'not {key.__class__.__name__}')
            if first:
                first = False
                if newline_indent is not None:
                    yield newline_indent
            else:
                yield item_separator
            yield _encoder(key)
            yield _key_separator
            try:
                if isinstance(value, str):
                    yield _encoder(value)
                elif value is None:
                    yield 'null'
                elif value is True:
                    yield 'true'
                elif value is False:
                    yield 'false'
                elif isinstance(value, int):
                    # see comment for int/float in _make_iterencode
                    yield _intstr(value)
                elif isinstance(value, float):
                    # see comment for int/float in _make_iterencode
                    yield _floatstr(value)
                else:
                    if isinstance(value, (list, tuple)):
                        chunks = _iterencode_list(value, _current_indent_level)
                    elif isinstance(value, dict):
                        chunks = _iterencode_dict(value, _current_indent_level)
                    else:
                        chunks = _iterencode(value, _current_indent_level)
                    yield from chunks
            except GeneratorExit:
                raise
            except BaseException as exc:
                exc.add_note(f'when serializing {type(dct).__name__} item {key!r}')
                raise
        if not first and newline_indent is not None:
            _current_indent_level -= 1
            yield '\n' + _indent * _current_indent_level
        yield '}'
        if markers is not None:
            del markers[markerid]

    def _iterencode(o, _current_indent_level):
        """
        Perform the iterencode utility operation under explicit compatibility rules.

        Example:
            Exercise  make iterencode. iterencode through a consuming regression::

                python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


        :param o: Value supplied for o under the utility contract.
        :param _current_indent_level: Value supplied for current indent level under the
            utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        if isinstance(o, str):
            yield _encoder(o)
        elif o is None:
            yield 'null'
        elif o is True:
            yield 'true'
        elif o is False:
            yield 'false'
        elif isinstance(o, int):
            # see comment for int/float in _make_iterencode
            yield _intstr(o)
        elif isinstance(o, float):
            # see comment for int/float in _make_iterencode
            yield _floatstr(o)
        elif isinstance(o, (list, tuple)):
            yield from _iterencode_list(o, _current_indent_level)
        elif isinstance(o, dict):
            yield from _iterencode_dict(o, _current_indent_level)
        else:
            if markers is not None:
                markerid = id(o)
                if markerid in markers:
                    raise ValueError("Circular reference detected")
                markers[markerid] = o
            newobj = _default(o)
            try:
                yield from _iterencode(newobj, _current_indent_level)
            except GeneratorExit:
                raise
            except BaseException as exc:
                exc.add_note(f'when serializing {type(o).__name__} object')
                raise
            if markers is not None:
                del markers[markerid]
    return _iterencode
