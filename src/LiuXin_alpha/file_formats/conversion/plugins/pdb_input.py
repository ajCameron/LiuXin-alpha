# -*- coding: utf-8 -*-

import os

from LiuXin_alpha.customize.conversion import InputFormatPlugin

from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class PDBInput(InputFormatPlugin):

    name = "PDB Input"
    author = "John Schember"
    description = "Convert PDB to HTML"
    file_types = {"pdb", "updb"}

    def convert(self, stream, options, file_ext, log, accelerators):
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
        opf = reader.extract_content(os.getcwdu())

        return opf
