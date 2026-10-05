# Some helper methods for handling various kinds of archive

"""
Select archive readers and extract members through a normalized decompression interface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise archives through a consuming regression::

        python -m pytest -q tests/utils/decompression/test_archives.py
"""
import importlib
import os


def _import_extractor(*module_names):
    """
    Perform the import extractor utility operation under explicit compatibility rules.

    Example:
        Exercise  import extractor through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param module_names: Value supplied for module names under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Extract the selected archive members into the destination under the active safety policy.

    Example:
        Exercise extract through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param dir: Value supplied for dir under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
