# -*- coding: utf-8 -*-

from __future__ import annotations

import typing as _typing
__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class PDBError(Exception):
    pass


FORMAT_READERS = None


def _import_readers() -> None:
    global FORMAT_READERS
    from LiuXin_alpha.file_formats.pdb.ereader.reader import Reader as Ereader_Reader
    from LiuXin_alpha.file_formats.pdb.palmdoc.reader import Reader as Palmdoc_Reader
    from LiuXin_alpha.file_formats.pdb.ztxt.reader import Reader as ZTXT_Reader
    from LiuXin_alpha.file_formats.pdb.pdf.reader import Reader as PDF_Reader
    from LiuXin_alpha.file_formats.pdb.plucker.reader import Reader as Plucker_Reader
    from LiuXin_alpha.file_formats.pdb.haodoo.reader import Reader as Haodoo_Reader

    FORMAT_READERS = {
        "PNPdPPrs": Ereader_Reader,
        "PNRdPPrs": Ereader_Reader,
        "zTXTGPlm": ZTXT_Reader,
        "TEXtREAd": Palmdoc_Reader,
        ".pdfADBE": PDF_Reader,
        "DataPlkr": Plucker_Reader,
        "BOOKMTIT": Haodoo_Reader,
        "BOOKMTIU": Haodoo_Reader,
    }


ALL_FORMAT_WRITERS = {"doc", "ztxt", "ereader"}
FORMAT_WRITERS = None


def _import_writers() -> None:
    global FORMAT_WRITERS
    from LiuXin_alpha.file_formats.pdb.palmdoc.writer import Writer as Palmdoc_Writer
    from LiuXin_alpha.file_formats.pdb.ztxt.writer import Writer as ZTXT_Writer
    from LiuXin_alpha.file_formats.pdb.ereader.writer import Writer as Ereader_Writer

    FORMAT_WRITERS = {
        "doc": Palmdoc_Writer,
        "ztxt": ZTXT_Writer,
        "ereader": Ereader_Writer,
    }


IDENTITY_TO_NAME = {
    "PNPdPPrs": "eReader",
    "PNRdPPrs": "eReader",
    "zTXTGPlm": "zTXT",
    "TEXtREAd": "PalmDOC",
    ".pdfADBE": "Adobe Reader",
    "DataPlkr": "Plucker",
    "BOOKMTIT": "Haodoo.net",
    "BOOKMTIU": "Haodoo.net",
    "BVokBDIC": "BDicty",
    "DB99DBOS": "DB (DatabasePing program)",
    "vIMGView": "FireViewer (ImageViewer)",
    "PmDBPmDB": "HanDBase",
    "InfoINDB": "InfoView",
    "ToGoToGo": "iSilo",
    "SDocSilX": "iSilo 3",
    "JbDbJBas": "JFile",
    "JfDbJFil": "JFile Pro",
    "DATALSdb": "LIST",
    "Mdb1Mdb1": "MobileDB",
    "BOOKMOBI": "MobiPocket",
    "DataSprd": "QuickSheet",
    "SM01SMem": "SuperMemo",
    "TEXtTlDc": "TealDoc",
    "InfoTlIf": "TealInfo",
    "DataTlMl": "TealMeal",
    "DataTlPt": "TealPaint",
    "dataTDBP": "ThinkDB",
    "TdatTide": "Tides",
    "ToRaTRPW": "TomeRaider",
    "BDOCWrdS": "WordSmith",
}


def get_reader(identity: _typing.Any) -> _typing.Any:
    """
    Returns None if no reader is found for the identity.
    :param identity:
    :return:
    """
    global FORMAT_READERS
    if FORMAT_READERS is None:
        _import_readers()
    return FORMAT_READERS.get(identity, None)


def get_writer(extension: _typing.Any) -> _typing.Any:
    """
    Returns None if no writer is found for extension.
    :param extension:
    :return:
    """
    global FORMAT_WRITERS
    if FORMAT_WRITERS is None:
        _import_writers()
    return FORMAT_WRITERS.get(extension, None)
