"""
Normalize and classify external bibliographic identifiers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise identifiers through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_ownership.py
"""
__author__ = "Cameron"

import uuid
import time



def get_unique_group_id() -> str:
    """
    Produces a unique string intended to be used as an id.

    Example:
        Exercise get unique group id through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return str(uuid.uuid4()) + str(time.clock())
