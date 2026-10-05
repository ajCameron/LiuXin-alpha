"""
Expose the customization cache's read/write interface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise read write api through a consuming regression::

        python -m pytest -q tests/customize/test_customize_base.py
"""


def api(f):
    """
    Perform the api operation under explicit file-format and conversion rules.

    Example:
        Exercise api through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :param f: Value supplied for f under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    f.is_cache_api = True
    return f


def read_api(f):
    """
    Read api under the format's safety and compatibility rules.

    Example:
        Exercise read api through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :param f: Value supplied for f under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    f = api(f)
    f.is_read_api = True
    return f


def write_api(f):
    """
    Write api under the format's safety and compatibility rules.

    Example:
        Exercise write api through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :param f: Value supplied for f under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    f = api(f)
    f.is_read_api = False
    return f
