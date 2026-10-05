# -*- coding: utf-8 -*-

"""
Convert PDB content into the normalized ebook conversion pipeline.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise pdb input through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
"""
from __future__ import annotations

import typing as _typing
import os

from LiuXin_alpha.customize.conversion import InputFormatPlugin
from LiuXin_alpha.file_formats.conversion.plugins._workdir import (
    choose_conversion_workdir,
)

from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class PDBInput(InputFormatPlugin):

    """
    Convert pdbinput sources into the normalized OEB pipeline model.

    Example:
        Exercise PDBInput through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
    """
    name = "PDB Input"
    author = "John Schember"
    description = "Convert PDB to HTML"
    file_types = {"pdb", "updb"}

    def convert(self: _typing.Self, stream: _typing.Any, options: _typing.Any, file_ext: _typing.Any, log: _typing.Any, accelerators: _typing.Any) -> _typing.Any:
        """
        Convert the supplied source into the stage's normalized output representation.

        Example:
            Exercise PDBInput.convert through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param options: Value supplied for options under the utility contract.
        :param file_ext: Value supplied for file ext under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param accelerators: Value supplied for accelerators under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.pdb.header import PdbHeaderReader
        from LiuXin_alpha.file_formats.pdb import PDBError, IDENTITY_TO_NAME, get_reader

        header = PdbHeaderReader(stream)
        reader = get_reader(header.ident)

        if reader is None:
            raise PDBError(
                "No reader available for format within container.\n Identity is %s. Book type is %s"
                % (header.ident, IDENTITY_TO_NAME.get(header.ident, _("Unknown")))
            )

        log.debug("Detected ebook format as: %s with identity: %s" % (IDENTITY_TO_NAME[header.ident], header.ident))

        reader = reader(header, stream, log, options)
        opf = reader.extract_content(choose_conversion_workdir("_pdb_input"))

        return opf
