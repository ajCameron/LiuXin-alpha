
"""
Expose ICU-compatible collation, case, search and transliteration helpers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise icu through a consuming regression::

        python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_icu.py
"""


def lower(some_string: str) -> str:
    """
    Perform the lower utility operation under explicit compatibility rules.

    Example:
        Exercise lower through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_icu.py


    :param some_string: Value supplied for some string under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return some_string.lower()


def upper(some_string: str) -> str:
    """
    Perform the upper utility operation under explicit compatibility rules.

    Example:
        Exercise upper through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_icu.py


    :param some_string: Value supplied for some string under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return some_string.lower()

def sort_key(some_string: str) -> str:
    """
    Perform the sort key utility operation under explicit compatibility rules.

    Example:
        Exercise sort key through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_icu.py


    :param some_string: Value supplied for some string under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return some_string.lower()

