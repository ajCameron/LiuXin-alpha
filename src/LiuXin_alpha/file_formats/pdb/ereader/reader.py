# -*- coding: utf-8 -*-

"""
Read the package format into normalized text, metadata and resources.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise reader through a consuming regression::

        python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
"""
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.file_formats.pdb.ereader import EreaderError
from LiuXin_alpha.file_formats.pdb.ereader.reader132 import Reader132
from LiuXin_alpha.file_formats.pdb.ereader.reader202 import Reader202
from LiuXin_alpha.file_formats.pdb.formatreader import FormatReader

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class Reader(FormatReader):
    """
    Parse reader data into normalized ebook structures.

    Example:
        Exercise Reader through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
    """
    def __init__(self: _typing.Self, header: _typing.Any, stream: _typing.Any, log: _typing.Any, options: _typing.Any) -> None:
        """
        Initialize and validate the reader state.

        Example:
            Exercise Reader.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param header: Value supplied for header under the utility contract.
        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param log: Value supplied for log under the utility contract.
        :param options: Value supplied for options under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        record0_size = len(header.section_data(0))

        if record0_size == 132:
            self.reader = Reader132(header, stream, log, options)
        elif record0_size in (116, 202):
            self.reader = Reader202(header, stream, log, options)
        else:
            raise EreaderError("Size mismatch. eReader header record size %s KB is not supported." % record0_size)

    def extract_content(self: _typing.Self, output_dir: _typing.Any) -> _typing.Any:
        """
        Extract content under the format's safety and compatibility rules.

        Example:
            Exercise Reader.extract content through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param output_dir: Value supplied for output dir under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.reader.extract_content(output_dir)

    def dump_pml(self: _typing.Self) -> _typing.Any:
        """
        Perform the dump pml operation under explicit file-format and conversion rules.

        Example:
            Exercise Reader.dump pml through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.reader.dump_pml()

    def dump_images(self: _typing.Self, out_dir: _typing.Any) -> _typing.Any:
        """
        Perform the dump images operation under explicit file-format and conversion rules.

        Example:
            Exercise Reader.dump images through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param out_dir: Value supplied for out dir under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.reader.dump_images(out_dir)
