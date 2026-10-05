
"""
Expose the supported text compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/utils/text/test_text_core.py
"""

from typing import Optional, Union
from copy import deepcopy

import re

from LiuXin_alpha.constants import preferred_encoding


def isbytestring(obj : Union[bytes, str]) -> bool:
    """
    Perform the isbytestring utility operation under explicit compatibility rules.

    Example:
        Exercise isbytestring through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param obj: Value supplied for obj under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return isinstance(obj, (str, bytes))


def url_slash_cleaner(url: str) -> str:
    """
    Removes redundant /'s from urls.

    Example:
        Exercise url slash cleaner through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param url: Value supplied for url under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return re.sub(r"(?<!:)/{2,}", "/", url)



def as_unicode(obj, enc: Optional[None] = None):

    """
    Perform the as unicode utility operation under explicit compatibility rules.

    Example:
        Exercise as unicode through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param obj: Value supplied for obj under the utility contract.
    :param enc: Value supplied for enc under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.constants import force_unicode
    from LiuXin_alpha.utils.libraries.calibre_polyglot.builtins import native_string_type

    if enc is None:
        from LiuXin_alpha.constants import preferred_encoding
        enc = preferred_encoding

    if not isbytestring(obj):
        try:
            obj = str(obj)
        except Exception:
            try:
                obj = native_string_type(obj)
            except Exception:
                obj = repr(obj)

    return force_unicode(obj, enc=enc)


def human_readable(size, sep=" "):
    """
    Convert a size in bytes into a human-readable form.

    Example:
        Exercise human readable through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param size: Value supplied for size under the utility contract.
    :param sep: Delimiter used to split or join list values.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    divisor, suffix = 1, "B"
    for i, candidate in enumerate(("B", "KB", "MB", "GB", "TB", "PB", "EB")):
        if size < (1 << ((i + 1) * 10)):
            divisor, suffix = (1 << (i * 10)), candidate
            break
    size = str(float(size) / divisor)
    if size.find(".") > -1:
        size = size[: size.find(".") + 2]
    if size.endswith(".0"):
        size = size[:-2]
    return size + sep + suffix


def remove_bracketed_text(src, brackets: Optional[dict[str, str]] = None) -> str:
    """
    Remove bracketed text from a given string.

    Example:
        Exercise remove bracketed text through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param src: Value supplied for src under the utility contract.
    :param brackets: Value supplied for brackets under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    brackets = brackets if brackets is not None else {"(": ")", "[": "]", "{": "}"}

    from collections import Counter

    counts = Counter()
    buf = []
    src = str(src)
    rmap = dict([(v, k) for k, v in brackets.items()])
    for char in src:
        if char in brackets:
            counts[char] += 1
        elif char in rmap:
            idx = rmap[char]
            if counts[idx] > 0:
                counts[idx] -= 1
        elif sum(counts.values()) < 1:
            buf.append(char)
    return "".join(buf)


def my_unichr(num):
    """
    Perform the my unichr utility operation under explicit compatibility rules.

    Example:
        Exercise my unichr through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param num: Value supplied for num under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.utils.text.icu import safe_chr
    try:
        return safe_chr(num)
    except (ValueError, OverflowError):
        return "?"


def entity_to_unicode(match, exceptions=[], encoding="cp1252", result_exceptions={}):
    """
    Perform the entity to unicode utility operation under explicit compatibility rules.

    Example:
        Exercise entity to unicode through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param match: Value supplied for match under the utility contract.
    :param exceptions: Value supplied for exceptions under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :param result_exceptions: Value supplied for result exceptions under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    def check(ch):
        """
        Perform the check utility operation under explicit compatibility rules.

        Example:
            Exercise entity to unicode.check through a consuming regression::

                python -m pytest -q tests/utils/text/test_text_core.py


        :param ch: Value supplied for ch under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return result_exceptions.get(ch, ch)

    ent = match.group(1)
    if ent in exceptions:
        return "&" + ent + ";"
    if ent in {"apos", "squot"}:  # squot is generated by some broken CMS software
        return check("'")
    if ent == "hellips":
        ent = "hellip"
    if ent.startswith("#"):
        try:
            if ent[1] in ("x", "X"):
                num = int(ent[2:], 16)
            else:
                num = int(ent[1:])
        except:
            return "&" + ent + ";"
        if encoding is None or num > 255:
            return check(my_unichr(num))
        try:
            return check(bytes(bytearray((num,))).decode(encoding))
        except UnicodeDecodeError:
            return check(my_unichr(num))
    from LiuXin_alpha.file_formats.html_entities import html5_entities

    try:
        return check(html5_entities[ent])
    except KeyError:
        pass
    from LiuXin_alpha.utils.calibre_utils.calibre_polyglot.html_entities import name2codepoint

    try:
        return check(my_unichr(name2codepoint[ent]))
    except KeyError:
        return "&" + ent + ";"





BRACKETS = ("<>", "{}", "()", "[]")


def drop_bracketed_text(target_string, parenthesis_types=None):
    """
    Drops any text surrounded by parenthesis of the given types. If None is supplied defaults to (u'<>', u'{}', u'()', u'[]'). Takes a parenthesis list of the form (u'[first_parenthesis_1][second_parenthesis_1]', ... ). This will also normalize whitespace to single spaces.

    Example:
        Exercise drop bracketed text through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param target_string: Value supplied for target string under the utility contract.
    :param parenthesis_types: Value supplied for parenthesis types under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    from LiuXin_alpha.constants import VERBOSE_DEBUG
    from LiuXin_alpha.errors import InputIntegrityError

    target_string = deepcopy(target_string)
    if parenthesis_types is None:
        parenthesis_types = BRACKETS

    # Builds an index of the left and right parenthesis - to try and ensure a clean drop in the case of overlapping
    # parenthesis (e.g. the case of (something has [ clearly gone ) very wrong]
    try:
        l_index = [pair[0] for pair in parenthesis_types]
        r_index = [pair[1] for pair in parenthesis_types]
        sep_index = zip(l_index, r_index)
    except IndexError:
        if VERBOSE_DEBUG:
            err_str = "Custom parenthesis_types passed into drop_bracketed_text are supposed to be of the form\n"
            err_str += "(u'[first_parenthesis_1][second_parenthesis_1]', ... )\n"
            err_str += "target_string: " + repr(target_string) + "\n"
            err_str += "parenthesis_types: " + repr(parenthesis_types) + "\n"
            raise InputIntegrityError(err_str)
        else:
            raise InputIntegrityError

    # Scans through the string, looking for a left separator. Then continues searching for the corresponding right
    # separator. Adds the position of these to the drop_index regex and zeros them.
    drop_index = []
    parenthesis_regex = r"(.*)\{}([^\{}]*)\{}(.*)"
    for seps in sep_index:
        l_sep = seps[0]
        r_sep = seps[1]
        current_par_regex = parenthesis_regex.format(l_sep, r_sep, r_sep)
        current_par_pat = re.compile(current_par_regex)
        while current_par_pat.match(target_string) is not None:
            match = current_par_pat.match(target_string)
            target_string = match.group(1) + " " + match.group(2) + " " + match.group(3)
            drop_index.append((len(match.group(1)), len(target_string) - len(match.group(3))))

    for index in drop_index:
        if index[0] >= index[1]:
            pass
        else:
            l_position = index[0]
            r_position = index[1]
            target_string = (
                target_string[:l_position] + " " * (r_position - l_position + 1) + target_string[r_position + 1 :]
            )

    target_string = re.sub(r"\s+", " ", target_string)
    return target_string
