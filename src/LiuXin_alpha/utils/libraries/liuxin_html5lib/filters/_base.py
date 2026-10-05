"""
Provide base utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise  base through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals


class Filter(object):
    """
    Provide the Filter utility contract with explicit state and cleanup behavior.

    Example:
        Exercise Filter through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __init__(self, source):
        """
        Initialize and validate the Filter state.

        Example:
            Exercise Filter.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param source: Value supplied for source under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.source = source

    def __iter__(self):
        """
        Expose iter behavior for the compatibility container.

        Example:
            Exercise Filter.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return iter(self.source)

    def __getattr__(self, name):
        """
        Perform the getattr utility operation under explicit compatibility rules.

        Example:
            Exercise Filter.  getattr   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return getattr(self.source, name)
