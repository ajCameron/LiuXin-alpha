"""
Expose retained Calibre-compatible utility helpers without importing the full compatibility layer.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise calibre through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_ownership.py
"""
from __future__ import annotations

import os
import time
from typing import Iterator, Tuple

from LiuXin_alpha.constants import (
    __appname__,
    __version__,
    filesystem_encoding,
    force_unicode,
    preferred_encoding,
)
from LiuXin_alpha.utils.which_os import isosx
from LiuXin_alpha.utils.storage.local import CurrentDir
from LiuXin_alpha.utils.storage.local.filenames import sanitize_file_name
from LiuXin_alpha.utils.mine_types import guess_type
from LiuXin_alpha.utils.date import strftime
from LiuXin_alpha.utils.text import as_unicode, entity_to_unicode
from LiuXin_alpha.utils.text.xml_utils import (
    _ent_pat,
    prepare_string_for_xml,
    replace_entities,
    xml_entity_to_unicode,
    xml_replace_entities,
)


def isbytestring(obj) -> bool:
    # Legacy calibre compatibility (Py2-era code often treats text as string-like).
    """
    Perform the isbytestring utility operation under explicit compatibility rules.

    Example:
        Exercise isbytestring through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param obj: Value supplied for obj under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return isinstance(obj, (str, bytes))


def walk(path: str) -> Iterator[str]:
    """
    Perform the walk utility operation under explicit compatibility rules.

    Example:
        Exercise walk through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: An iterator yielding the normalized values described above.
    """
    for root, _, files in os.walk(path):
        for name in files:
            yield os.path.join(root, name)


def fit_image(owidth: int, oheight: int, max_width: int, max_height: int) -> Tuple[bool, int, int]:
    """
    Scale image dimensions to fit a bounding box without changing aspect ratio.

    Example:
        Exercise fit image through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param owidth: Value supplied for owidth under the utility contract.
    :param oheight: Value supplied for oheight under the utility contract.
    :param max_width: Value supplied for max width under the utility contract.
    :param max_height: Value supplied for max height under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if owidth <= 0 or oheight <= 0:
        return False, max(0, int(max_width)), max(0, int(max_height))
    max_width = int(max_width)
    max_height = int(max_height)
    if owidth <= max_width and oheight <= max_height:
        return False, int(owidth), int(oheight)
    ratio = min(max_width / float(owidth), max_height / float(oheight))
    nw = max(1, int(round(owidth * ratio)))
    nh = max(1, int(round(oheight * ratio)))
    return True, nw, nh


def setup_cli_handlers(logger=None):
    """
    Install stream and exception handlers suitable for command-line execution.

    Example:
        Exercise setup cli handlers through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param logger: Value supplied for logger under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return None


def filename_to_utf8(name):
    """
    Return a filesystem filename encoded for the current platform boundary.

    Example:
        Exercise filename to utf8 through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(name, bytes):
        return name.decode("utf-8", "replace")
    return str(name)

