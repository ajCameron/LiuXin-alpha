"""
Model cached link-attribute relations.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise link attribute tables through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""
from collections import defaultdict
from copy import deepcopy
from LiuXin_alpha.utils.libraries.liuxin_six import iteritems

from typing import Union, Optional, Any, TypeVar, Literal

from LiuXin_alpha.customize.cache.base_tables import BaseLinkAttributeTable
from LiuXin_alpha.library.caches.calibre.tables.base import CalibreBaseTable

from LiuXin_alpha.library.caches.calibre.tables.one_many_tables import (
    CalibrePriorityOneToManyTable,
)

from LiuXin_alpha.library.caches.calibre.tables.many_one_tables import (
    CalibrePriorityManyToOneTable,
)

from LiuXin_alpha.library.caches.calibre.tables.many_many_tables import (
    CalibrePriorityManyToManyTable,
)
from LiuXin_alpha.library.caches.calibre.fields import BaseField

from LiuXin_alpha.databases.db_types import MainTableName, InterLinkTableName, TableColumnName, SrcTableID, DstTableID

T = TypeVar("T")


class CalibreLinkAttributeTable(BaseLinkAttributeTable[T]):
    """
    Represents a link property in a calibre table.

    Example:
        Exercise CalibreLinkAttributeTable through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
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

        Example:
            Exercise CalibreLinkAttributeTable.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param link_table_name: Value supplied for link table name under the utility
            contract.
        :param link_table: Value supplied for link table under the utility contract.
        :param main_table: Value supplied for main table under the utility contract.
        :param auxiliary_table: Value supplied for auxiliary table under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(CalibreLinkAttributeTable, self).__init__(name, link_table_name, link_table, main_table, auxiliary_table)

        # Keyed with the main_id, then valued with auxiliary_id, then valued with the property value
        self.main_auxiliary_property_map = self._nested_dict_factory()
        self.auxiliary_main_property_map = self._nested_dict_factory()

    @staticmethod
    def _nested_dict_factory() -> dict[SrcTableID, dict[DstTableID, T]]:
        """
        Used to contain the cached data internally in the table.

        Example:
            Exercise CalibreLinkAttributeTable. nested dict factory through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return defaultdict(dict)

    def read(self, db) -> None:
        """
        Preforms a read of information from the database into this table.

        Example:
            Exercise CalibreLinkAttributeTable.read through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
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

        Example:
            Exercise CalibreLinkAttributeTable. property adapter through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param link_attr: Value supplied for link attr under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return link_attr

    def get_property(self, main_id: SrcTableID, auxiliary_id: DstTableID) -> T:
        """
        Return the property for a given main_id and auxilary_id.

        Example:
            Exercise CalibreLinkAttributeTable.get property through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param main_id: Value supplied for main id under the utility contract.
        :param auxiliary_id: Value supplied for auxiliary id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Should be equivalent to self.auxiliary_main_property_map[auxiliary_id][main_id]
        return self.main_auxiliary_property_map[main_id][auxiliary_id]

    def get_auxiliary_val_dict(self, main_id: SrcTableID) -> dict[DstTableID, T]:
        """
        Return a dictionary keyed with the auxiliary ids and valued with the link value for that aux id.

        Example:
            Exercise CalibreLinkAttributeTable.get auxiliary val dict through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param main_id: Value supplied for main id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return deepcopy(self.main_auxiliary_property_map[main_id])

    def get_main_val_dict(self, auxiliary_id: DstTableID) -> dict[SrcTableID, T]:
        """
        Return a dictionary keyed with the main ids and valued with the link value for that aux id.

        Example:
            Exercise CalibreLinkAttributeTable.get main val dict through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param auxiliary_id: Value supplied for auxiliary id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return deepcopy(self.auxiliary_main_property_map[auxiliary_id])

    def get_sorted_main_values(self, auxiliary_id: DstTableID, sort: str = "attr") -> list[SrcTableID]:
        """
        Return main ids which correspond to the auxiliary id.

        Example:
            Exercise CalibreLinkAttributeTable.get sorted main values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param auxiliary_id: Value supplied for auxiliary id under the utility contract.
        :param sort: Value supplied for sort under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
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

        Example:
            Exercise CalibreLinkAttributeTable.get sorted auxiliary values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param main_id: Value supplied for main id under the utility contract.
        :param sort: Value supplied for sort under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
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

    Example:
        Exercise CalibreIndexLinkAttributeTablePrioritySort through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
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

        Example:
            Exercise CalibreIndexLinkAttributeTablePrioritySort.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param link_table_name: Value supplied for link table name under the utility
            contract.
        :param link_table: Value supplied for link table under the utility contract.
        :param main_table: Value supplied for main table under the utility contract.
        :param auxiliary_table: Value supplied for auxiliary table under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
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

        Example:
            Exercise CalibreIndexLinkAttributeTablePrioritySort.set link properties through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        super(CalibreIndexLinkAttributeTablePrioritySort, self).set_link_properties(db)

        # Set the priority column as well
        self.priority_column = db.driver_wrapper.get_interlink_column(self.main_table, self.auxiliary_table, "priority")

    def read(self, db) -> None:
        """
        Read data off the database.

        Example:
            Exercise CalibreIndexLinkAttributeTablePrioritySort.read through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
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

        Example:
            Exercise CalibreIndexLinkAttributeTablePrioritySort. property adapter through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param link_attr: Value supplied for link attr under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return int(link_attr)

    def get_sorted_main_values(
        self, auxiliary_id: DstTableID, sort: Literal["attr", "priority"] = "attr"
    ) -> list[T, ...]:
        """
        Return main ids which correspond to the auxiliary id. How they're sorted is optional

        Example:
            Exercise CalibreIndexLinkAttributeTablePrioritySort.get sorted main values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param auxiliary_id: Value supplied for auxiliary id under the utility contract.
        :param sort: Value supplied for sort under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
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

        Example:
            Exercise CalibreIndexLinkAttributeTablePrioritySort.get sorted auxiliary values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param main_id: Value supplied for main id under the utility contract.
        :param sort: Value supplied for sort under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
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

    Example:
        Exercise create link attribute table through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param link_field: Value supplied for link field under the utility contract.
    :param attribute_name: Value supplied for attribute name under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
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
