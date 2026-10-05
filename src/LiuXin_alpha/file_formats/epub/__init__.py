"""
Expose the supported epub compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/epub/test_epub_modernized.py
"""
from __future__ import with_statement
from __future__ import annotations

import typing as _typing

"""
Conversion to EPUB
"""

from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile, ZIP_STORED

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal kovid@kovidgoyal.net"
__docformat__ = "restructuredtext en"


def rules(stylesheets: _typing.Any) -> _typing.Iterator[_typing.Any]:
    """
    Perform the rules operation under explicit file-format and conversion rules.

    Example:
        Exercise rules through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_modernized.py


    :param stylesheets: Value supplied for stylesheets under the utility contract.
    :return: An iterator yielding the normalized values described above.
    """
    for s in stylesheets:
        if hasattr(s, "cssText"):
            for r in s:
                if r.type == r.STYLE_RULE:
                    yield r


def initialize_container(path_to_container: _typing.Any, opf_name: str = "metadata.opf", extra_entries: _typing.Any = None) -> _typing.Any:
    """
    Create an empty EPUB document, with a default skeleton.

    Example:
        Exercise initialize container through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_modernized.py


    :param path_to_container: Value supplied for path to container under the utility
        contract.
    :param opf_name: Value supplied for opf name under the utility contract.
    :param extra_entries: Value supplied for extra entries under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if extra_entries is None:
        extra_entries = []

    rootfiles = ""
    for path, mimetype, _ in extra_entries:
        rootfiles += '<rootfile full-path="{0}" media-type="{1}"/>'.format(path, mimetype)
    container = """\
<?xml version="1.0"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
   <rootfiles>
      <rootfile full-path="{0}" media-type="application/oebps-package+xml"/>
      {extra_entries}
   </rootfiles>
</container>
    """.format(
        opf_name, extra_entries=rootfiles
    ).encode(
        "utf-8"
    )
    zf = ZipFile(path_to_container, "w")
    zf.writestr("mimetype", "application/epub+zip", compression=ZIP_STORED)
    zf.writestr("META-INF/", "", 0o0755)
    zf.writestr("META-INF/container.xml", container)
    for path, _, data in extra_entries:
        zf.writestr(path, data)
    return zf
