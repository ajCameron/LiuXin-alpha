"""
Preserve legacy imports of the common storage field hierarchy.

Re-export the canonical basic, scalar and relation interfaces from
base_field_api. Each exported name retains the original class identity.
"""

from .base_field_api import (
    FieldBasicInterfaceAPI,
    RelationFieldBasicInterfaceAPI,
    ScalarFieldBasicInterfaceAPI,
)

__all__ = [
    "FieldBasicInterfaceAPI",
    "RelationFieldBasicInterfaceAPI",
    "ScalarFieldBasicInterfaceAPI",
]
