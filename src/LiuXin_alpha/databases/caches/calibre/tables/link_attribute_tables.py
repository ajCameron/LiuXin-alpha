from collections import defaultdict
from copy import deepcopy
from six import iteritems

from typing import Union, Optional, Any, TypeVar, Literal

from LiuXin.customize.cache.base_tables import BaseLinkAttributeTable
from LiuXin.databases.caches.calibre.tables.base import CalibreBaseTable

from LiuXin.databases.caches.calibre.tables.one_many_tables import (
    CalibrePriorityOneToManyTable,
)

from LiuXin.databases.caches.calibre.tables.many_one_tables import (
    CalibrePriorityManyToOneTable,
)

from LiuXin.databases.caches.calibre.tables.many_many_tables import (
    CalibrePriorityManyToManyTable,
)
from LiuXin.databases.caches.calibre.fields import BaseField

from LiuXin.databases.db_types import MainTableName, InterLinkTableName, TableColumnName, SrcTableID, DstTableID

T = TypeVar("T")


class CalibreLinkAttributeTable(BaseLinkAttributeTable[T]):
    """
    Represents a link property in a calibre table.
    """

    # Todo: The name is on the recognized list
    # Todo: Not sure "name" is the right concept for this - link type?
    def __init__(
        self,
        name: str,
        link_table_name: Optional[InterLinkTableName],
        link_table: CalibreBaseTable,
        main_table: MainTableName,
        auxiliary_table: MainTableName,
    ) -> None:
        """
        Startup. Stores the name of the property this class represents as well as the underlying table.

        :param name: Name of the property - has to be one of the recognized property types ("index", "datestamp" e.t.c)
                     "index" - the index of a title in a series - for example
                     "datestamp" - when the link was created
        :param link_table_name: The property is defined in the following link table
        :param link_table: Table class representing the underlying link table
        :param main_table:
        :param auxiliary_table:
        """
        super(CalibreLinkAttributeTable, self).__init__(name, link_table_name, link_table, main_table, auxiliary_table)

        # Keyed with the main_id, then valued with auxiliary_id, then valued with the property value
        self.main_auxiliary_property_map = self._nested_dict_factory()
        self.auxiliary_main_property_map = self._nested_dict_factory()

    @staticmethod
    def _nested_dict_factory() -> dict[SrcTableID, dict[DstTableID, T]]:
        """
        Used to contain the cached data internally in the table.

        :return:
        """
        return defaultdict(dict)

    def read(self, db) -> None:
        """
        Preforms a read of information from the database into this table.

        After this method has been called, the table should be populated with data.
        :param db:
        :return:
        """
        self.set_link_properties(db)

        # Keyed with the main table id and valued with the auxilary table id
        main_aux_property_map = self.main_auxiliary_property_map
        # Keyed with the auxilary table id and valued with the main table id
        aux_main_property_map = self.auxiliary_main_property_map

        for link_attr, main_id, aux_id in db.macros.read_link_property_trios(
            link_table=self.link_table_name,
            link_property_col=self.property_column,
            first_id=self.main_id_col,
            second_id=self.auxiliary_id_col,
        ):
            main_aux_property_map[main_id][aux_id] = self._property_adapter(link_attr)
            aux_main_property_map[aux_id][main_id] = self._property_adapter(link_attr)

    @staticmethod
    def _property_adapter(link_attr: Any) -> T:
        """
        Used when reading properties off the database - affects how the data is locally stored for purposes of sorting.

        Unless overridden will just return the identity.
        :param link_attr:
        :return:
        """
        return link_attr

    def get_property(self, main_id: SrcTableID, auxiliary_id: DstTableID) -> T:
        """
        Return the property for a given main_id and auxilary_id.

        E.g. In the case of an "index" attribute for a "series"/"title" link, returns the index of that title in that
        series.
        :param main_id:
        :param auxiliary_id:
        :return:
        """
        # Should be equivalent to self.auxiliary_main_property_map[auxiliary_id][main_id]
        return self.main_auxiliary_property_map[main_id][auxiliary_id]

    def get_auxiliary_val_dict(self, main_id: SrcTableID) -> dict[DstTableID, T]:
        """
        Return a dictionary keyed with the auxiliary ids and valued with the link value for that aux id.

        :param main_id:
        :return:
        """
        return deepcopy(self.main_auxiliary_property_map[main_id])

    def get_main_val_dict(self, auxiliary_id: DstTableID) -> dict[SrcTableID, T]:
        """
        Return a dictionary keyed with the main ids and valued with the link value for that aux id.

        :param auxiliary_id:
        :return:
        """
        return deepcopy(self.auxiliary_main_property_map[auxiliary_id])

    def get_sorted_main_values(self, auxiliary_id: DstTableID, sort: str = "attr") -> list[SrcTableID]:
        """
        Return main ids which correspond to the auxiliary id.

        How they're sorted is optional.
        :param auxiliary_id:
        :param sort: The type of sort to use - depends on the details of the store type.
                     Default is to sort on the link attribute. Because that's the only thing that's certain to be
                     available.
        :return:
        """
        if sort == "attr":
            main_ids_map = self.auxiliary_main_property_map[auxiliary_id]
            main_id_attr_pairs = [p for p in iteritems(main_ids_map)]
            return [x[0] for x in sorted(main_id_attr_pairs, key=lambda x: x[1])]
        else:
            raise NotImplementedError

    def get_sorted_auxiliary_values(self, main_id: SrcTableID, sort: str = "attr") -> list[DstTableID]:
        """
        Return auxiliary ids which correspond to the main id, sorted by the property
        :param main_id:
        :param sort: The type of sort to use - depends on the details of the store type.
                     Default is to sort on the link attribute. Because that's the only thing that's certain to be
                     available.
        :return:
        """
        if sort == "attr":
            aux_ids_map = self.main_auxiliary_property_map[main_id]
            aux_id_attr_pairs = [p for p in iteritems(aux_ids_map)]
            return [x[0] for x in sorted(aux_id_attr_pairs, key=lambda x: x[1])]
        else:
            raise NotImplementedError


class CalibreIndexLinkAttributeTablePrioritySort(CalibreLinkAttributeTable):
    """
    Aan Index link attribute for a table with priority sort.
    """

    def __init__(
        self,
        name: str,
        link_table_name: Optional[InterLinkTableName],
        link_table: CalibreBaseTable,
        main_table: MainTableName,
        auxiliary_table: MainTableName,
    ) -> None:
        """
        Startup. Stores the name of the property this class represents as well as the underlying table.

        :param name: Name of the property
        :param link_table_name: The property is defined in the following link table
        :param link_table: Table class representing the underlying link table
        :param main_table:
        :param auxiliary_table:
        """
        super(CalibreIndexLinkAttributeTablePrioritySort, self).__init__(
            name, link_table_name, link_table, main_table, auxiliary_table
        )

        # Keyed with the main_id, then valued with auxiliary_id, then valued with the property value
        self.main_auxiliary_priority_map = self._nested_dict_factory()
        self.auxiliary_main_priority_map = self._nested_dict_factory()

        self.priority_column = None

    def set_link_properties(self, db) -> None:
        """
        Additionally sets the priority_column.

        :param db:
        :return:
        """
        super(CalibreIndexLinkAttributeTablePrioritySort, self).set_link_properties(db)

        # Set the priority column as well
        self.priority_column = db.driver_wrapper.get_interlink_column(self.main_table, self.auxiliary_table, "priority")

    def read(self, db) -> None:
        """
        Read data off the database.

        :param db:
        :return:
        """

        super(CalibreIndexLinkAttributeTablePrioritySort, self).read(db)

        # Todo: Pretty sure this is redundant

        main_aux_priority_map = self.main_auxiliary_priority_map
        aux_main_priority_map = self.auxiliary_main_priority_map

        for link_attr, main_id, aux_id in db.macros.read_link_property_trios(
            link_table=self.link_table_name,
            link_property_col=self.priority_column,
            first_id=self.main_id_col,
            second_id=self.auxiliary_id_col,
        ):
            main_aux_priority_map[main_id][aux_id] = self._property_adapter(link_attr)
            aux_main_priority_map[aux_id][main_id] = self._property_adapter(link_attr)

    @staticmethod
    def _property_adapter(link_attr: Any) -> int:
        """
        Turns the data stored on the table into something which can be stored in the dicts.

        :param link_attr:
        :return:
        """
        return int(link_attr)

    def get_sorted_main_values(
        self, auxiliary_id: DstTableID, sort: Literal["attr", "priority"] = "attr"
    ) -> list[T, ...]:
        """
        Return main ids which correspond to the auxiliary id. How they're sorted is optional

        Note - this is intended for a ManyToOne series configuration.
        E.g. many titles linked to one series.
        :param auxiliary_id: The id to sort on for in the secondary tables.
        :param sort: The type of sort to use - depends on the details of the store type.
                     Default is to sort on the link attribute. Because that's the only thing that's certain to be
                     available.
        :return:
        """
        if sort == "attr":
            return super(CalibreIndexLinkAttributeTablePrioritySort, self).get_sorted_main_values(
                auxiliary_id=auxiliary_id, sort=sort
            )

        elif sort == "priority":
            # Get the main ids associated with the aux id - sorted by priority
            main_ids_map = self.auxiliary_main_priority_map[auxiliary_id]
            main_id_attr_pairs = [p for p in iteritems(main_ids_map)]
            priority_sorted_main_ids = [x[0] for x in sorted(main_id_attr_pairs, key=lambda x: x[1], reverse=True)]

            # Read and return the attribute values in priority order
            return [self.get_property(main_id, auxiliary_id) for main_id in priority_sorted_main_ids]

        else:
            raise NotImplementedError

    def get_sorted_auxiliary_values(
        self, main_id: SrcTableID, sort: Literal["attr", "priority"] = "attr"
    ) -> list[T, ...]:
        """
        Return auxiliary ids which correspond to the main id, sorted by the property
        :param main_id:
        :param sort: The type of sort to use - depends on the details of the store type.
                     Default is to sort on the link attribute. Because that's the only thing that's certain to be
                     available.
        :return:
        """
        if sort == "attr":
            return super(CalibreIndexLinkAttributeTablePrioritySort, self).get_sorted_auxiliary_values(
                main_id=main_id, sort=sort
            )
        elif sort == "priority":
            # Get the aux ids associated with the main id - sorted by priority
            aux_ids_map = self.main_auxiliary_priority_map[main_id]
            aux_id_attr_pairs = [p for p in iteritems(aux_ids_map)]
            priority_sorted_aux_ids = [x[0] for x in sorted(aux_id_attr_pairs, key=lambda x: x[1], reverse=True)]

            # Read and return the attribute values in priority order
            return [self.get_property(main_id, aux_id) for aux_id in priority_sorted_aux_ids]
        else:
            raise NotImplementedError


def create_link_attribute_table(
    link_field: BaseField, attribute_name: TableColumnName
) -> Union[CalibreIndexLinkAttributeTablePrioritySort, CalibreLinkAttributeTable]:
    """
    Create and return an appropriate link table for the given type of table and type of link.

    :param link_field:
    :param attribute_name:
    :return:
    """
    # Examine the table to check that we CAN emulate a OneToOne field with the given table
    # Preform the needed changes to the access methods to accommodate the backend table
    if isinstance(
        link_field.table,
        (
            CalibrePriorityOneToManyTable,
            CalibrePriorityManyToOneTable,
            CalibrePriorityManyToManyTable,
        ),
    ):
        if attribute_name == "index":
            link_attr_class = CalibreIndexLinkAttributeTablePrioritySort
        else:
            raise NotImplementedError

    else:

        link_attr_class = CalibreLinkAttributeTable

    return link_attr_class(
        name=attribute_name,
        link_table_name=None,
        link_table=link_field.table,
        main_table=link_field.main_table,
        auxiliary_table=link_field.auxiliary_table,
    )
