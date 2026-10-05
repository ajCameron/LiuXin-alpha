"""
Convert ODT content into the normalized ebook conversion pipeline.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise odt input through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
"""
from __future__ import with_statement
from __future__ import annotations

import typing as _typing

"""
Convert an ODT file into a Open Ebook
"""

from LiuXin_alpha.customize.conversion import InputFormatPlugin
from LiuXin_alpha.file_formats.conversion.plugins._workdir import (
    choose_conversion_workdir,
)

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal kovid@kovidgoyal.net"
__docformat__ = "restructuredtext en"


class ODTInput(InputFormatPlugin):

    """
    Convert odtinput sources into the normalized OEB pipeline model.

    Example:
        Exercise ODTInput through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
    """
    name = "ODT Input"
    author = "Kovid Goyal"
    description = "Convert ODT (OpenOffice) files to HTML"
    file_types = {"odt"}

    def convert(self: _typing.Self, stream: _typing.Any, options: _typing.Any, file_ext: _typing.Any, log: _typing.Any, accelerators: _typing.Any) -> _typing.Any:
        """
        Convert the supplied source into the stage's normalized output representation.

        Example:
            Exercise ODTInput.convert through a consuming regression::

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
        from LiuXin_alpha.file_formats.odt.input import Extract

        return Extract()(stream, choose_conversion_workdir("_odt_input"), log)
