"""
Register the built-in database output plugin.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise db output through a consuming regression::

        python -m pytest -q tests/customize/test_customize_base.py
"""


class DBOutputPlugin:
    """
    Base class for the DB output plugin - which takes an entry on the database and outputs something.

    Example:
        Exercise DBOutputPlugin through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    pass
