from __future__ import annotations

import pytest

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields import (
    FieldBasicInterfaceAPI,
    RelationFieldBasicInterfaceAPI,
    ScalarFieldBasicInterfaceAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.many_many_field import (
    ManyToManyFieldAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.many_one_field import (
    ManyToOneFieldAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.one_many_field import (
    OneToManyFieldAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.one_one_field import (
    CacheOneOneInSameTableFieldAPI,
    CacheOneOneInTwoTableFieldAPI,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.one_one_field import (
    SchemaBackedSameTableField,
)


def test_scalar_field_api_is_explicit_and_owner_safe() -> None:
    assert issubclass(ScalarFieldBasicInterfaceAPI, FieldBasicInterfaceAPI)
    assert issubclass(CacheOneOneInSameTableFieldAPI, ScalarFieldBasicInterfaceAPI)
    assert not issubclass(CacheOneOneInSameTableFieldAPI, RelationFieldBasicInterfaceAPI)

    assert CacheOneOneInSameTableFieldAPI.field_storage_shape == "scalar"
    assert CacheOneOneInSameTableFieldAPI.mutates_links is False
    assert CacheOneOneInSameTableFieldAPI.creates_related_rows is False
    assert CacheOneOneInSameTableFieldAPI.deletes_related_rows is False
    assert CacheOneOneInSameTableFieldAPI.deletes_owner_rows is False

    assert SchemaBackedSameTableField.field_storage_shape == "scalar"
    assert SchemaBackedSameTableField.deletes_owner_rows is False


@pytest.mark.parametrize(
    "field_cls",
    [
        CacheOneOneInTwoTableFieldAPI,
        OneToManyFieldAPI,
        ManyToOneFieldAPI,
        ManyToManyFieldAPI,
    ],
)
def test_relation_field_apis_are_explicit_and_owner_safe(field_cls: type[FieldBasicInterfaceAPI]) -> None:
    assert issubclass(field_cls, RelationFieldBasicInterfaceAPI)
    assert not issubclass(field_cls, ScalarFieldBasicInterfaceAPI)

    assert field_cls.field_storage_shape == "relation"
    assert field_cls.mutates_links is True
    assert field_cls.creates_related_rows is False
    assert field_cls.deletes_related_rows is False
    assert field_cls.deletes_owner_rows is False
