
"""
Fields are fields in a table (or multiple tables in composite or link form).
"""

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.base_field import (
    FieldBasicInterfaceAPI,
    RelationFieldBasicInterfaceAPI,
    ScalarFieldBasicInterfaceAPI,
)

__all__ = [
    "FieldBasicInterfaceAPI",
    "RelationFieldBasicInterfaceAPI",
    "ScalarFieldBasicInterfaceAPI",
]
