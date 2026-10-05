"""
Infer MIME types from filenames while preserving LiuXin-specific format aliases.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise mine types through a consuming regression::

        python -m pytest -q tests/utils/test_mine_types.py
"""
from LiuXin_alpha.utils.resources import resource_to_path

_mt_inited = False


def _init_mimetypes():
    """
    Perform the init mimetypes utility operation under explicit compatibility rules.

    Example:
        Exercise  init mimetypes through a consuming regression::

            python -m pytest -q tests/utils/test_mine_types.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    global _mt_inited
    import mimetypes

    target_path = resource_to_path("mime.types")

    mimetypes.init(
        [
            target_path,
        ]
    )


    # Ensure common ebook container types exist even if the bundled mime.types is missing entries.
    mimetypes.add_type("application/x-dtbncx+xml", ".ncx")
    mimetypes.add_type("application/oebps-package+xml", ".opf")
    mimetypes.add_type("application/epub+zip", ".epub")
    _mt_inited = True


def guess_all_extensions(*args, **kwargs):
    """
    Perform the guess all extensions utility operation under explicit compatibility rules.

    Example:
        Exercise guess all extensions through a consuming regression::

            python -m pytest -q tests/utils/test_mine_types.py


    :param args: Positional values forwarded to the compatibility implementation.
    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import mimetypes

    if not _mt_inited:
        _init_mimetypes()
    return mimetypes.guess_all_extensions(*args, **kwargs)


def guess_extension(*args, **kwargs):
    """
    Perform the guess extension utility operation under explicit compatibility rules.

    Example:
        Exercise guess extension through a consuming regression::

            python -m pytest -q tests/utils/test_mine_types.py


    :param args: Positional values forwarded to the compatibility implementation.
    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import mimetypes

    if not _mt_inited:
        _init_mimetypes()
    ext = mimetypes.guess_extension(*args, **kwargs)
    if not ext and args and args[0] == "application/x-palmreader":
        ext = ".pdb"
    return ext


def get_types_map():
    """
    Return types map under the documented compatibility and safety rules.

    Example:
        Exercise get types map through a consuming regression::

            python -m pytest -q tests/utils/test_mine_types.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import mimetypes

    if not _mt_inited:
        _init_mimetypes()
    return mimetypes.types_map




# probably safe
def guess_type(*args, **kwargs):
    """
    Perform the guess type utility operation under explicit compatibility rules.

    Example:
        Exercise guess type through a consuming regression::

            python -m pytest -q tests/utils/test_mine_types.py


    :param args: Positional values forwarded to the compatibility implementation.
    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import mimetypes

    if not _mt_inited:
        _init_mimetypes()
    return mimetypes.guess_type(*args, **kwargs)

