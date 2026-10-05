"""
Expose the supported book compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
"""

from LiuXin_alpha.utils.calibre_compat.metadata.calibre_metadata_constants import (
    ALL_METADATA_FIELDS,
    BOOK_STRUCTURE_FIELDS,
    CALIBRE_METADATA_FIELDS,
    DEVICE_METADATA_FIELDS,
    PUBLICATION_METADATA_FIELDS,
    SC_COPYABLE_FIELDS,
    SC_FIELDS_COPY_NOT_NULL,
    SC_FIELDS_NOT_COPIED,
    SERIALIZABLE_FIELDS,
    SOCIAL_METADATA_FIELDS,
    STANDARD_METADATA_FIELDS,
    TOP_LEVEL_IDENTIFIERS,
    USER_METADATA_FIELDS,
)

__all__ = [
    "ALL_METADATA_FIELDS",
    "BOOK_STRUCTURE_FIELDS",
    "CALIBRE_METADATA_FIELDS",
    "DEVICE_METADATA_FIELDS",
    "PUBLICATION_METADATA_FIELDS",
    "SC_COPYABLE_FIELDS",
    "SC_FIELDS_COPY_NOT_NULL",
    "SC_FIELDS_NOT_COPIED",
    "SERIALIZABLE_FIELDS",
    "SOCIAL_METADATA_FIELDS",
    "STANDARD_METADATA_FIELDS",
    "TOP_LEVEL_IDENTIFIERS",
    "USER_METADATA_FIELDS",
]

