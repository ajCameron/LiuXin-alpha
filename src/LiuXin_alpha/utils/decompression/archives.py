# Some helper methods for handling various kinds of archive

import importlib
import os


def _import_extractor(*module_names):
    last_error = None
    for module_name in module_names:
        try:
            return importlib.import_module(module_name).extract
        except Exception as err:
            last_error = err
    if last_error is not None:
        raise last_error
    raise ImportError("No extractor module names provided")


def extract(path, dir):
    extractor = None
    # First use the file header to identify its type
    with open(path, "rb") as f:
        id_ = f.read(3)
    if id_ == b"Rar":
        extractor = _import_extractor(
            "LiuXin.utils.decompression.unrar",
            "LiuXin_alpha.utils.decompression.unrar",
        )
    elif id_.startswith(b"PK"):
        extractor = _import_extractor(
            "LiuXin.utils.libunzip",
            "LiuXin_alpha.utils.decompression.libunzip",
        )
    if extractor is None:
        # Fallback to file extension
        ext = os.path.splitext(path)[1][1:].lower()
        if ext in ["zip", "cbz", "epub", "oebzip"]:
            extractor = _import_extractor(
                "LiuXin.utils.libunzip",
                "LiuXin_alpha.utils.decompression.libunzip",
            )
        elif ext in ["cbr", "rar"]:
            extractor = _import_extractor(
                "LiuXin.utils.decompression.unrar",
                "LiuXin_alpha.utils.decompression.unrar",
            )
    if extractor is None:
        raise Exception("Unknown archive type")
    extractor(path, dir)
