# Some helper methods for handling various kinds of archive

import os


def extract(path, dir):
    extractor = None
    # First use the file header to identify its type
    with open(path, "rb") as f:
        id_ = f.read(3)
    if id_ == b"Rar":
        from LiuXin.utils.decompression.unrar import extract as rarextract

        extractor = rarextract
    elif id_.startswith(b"PK"):
        from LiuXin.utils.libunzip import extract as zipextract

        extractor = zipextract
    if extractor is None:
        # Fallback to file extension
        ext = os.path.splitext(path)[1][1:].lower()
        if ext in ["zip", "cbz", "epub", "oebzip"]:
            from LiuXin.utils.libunzip import extract as zipextract

            extractor = zipextract
        elif ext in ["cbr", "rar"]:
            from LiuXin.utils.decompression.unrar import extract as rarextract

            extractor = rarextract
    if extractor is None:
        raise Exception("Unknown archive type")
    extractor(path, dir)
