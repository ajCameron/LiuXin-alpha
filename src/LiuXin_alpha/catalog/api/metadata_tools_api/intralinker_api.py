"""
Define named compatibility contracts for same-family row relationships.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from LiuXin_alpha.databases.api import DatabaseAPI, RowAPI


@runtime_checkable
class IntralinkerAPI(Protocol):
    """
    Wrap database intralink_rows without additional family or type validation.

    All named methods and generic perform the same delegation. Database policy
    determines supported tables, link types, reuse and transaction behavior.

    Example:
        Use a named method to express expected row families; the database still
        validates whether that relationship is supported.
    """

    db: DatabaseAPI

    def creator_creator(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """
        Delegate an intralink between two Creator/Agent rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        ...

    def cover_cover(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """
        Delegate an intralink between two Cover rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        ...

    def file_file(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """
        Delegate an intralink between two File rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        ...

    def folder_store_folder_store(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """
        Delegate an intralink between two folder/store rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        ...

    def identifier_identifier(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """
        Delegate an intralink between two Identifier rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        ...

    def tag_tag(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """
        Delegate an intralink between two Tag rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        ...

    def title_title(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """
        Delegate an intralink between two Title rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        ...

    def publisher_publisher(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """
        Delegate an intralink between two publisher/organisation rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        ...

    def generic(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """
        Delegate an intralink between two compatible same-family rows.

        No priority argument or local schema check is added. Backend failures
        propagate and this wrapper opens no transaction.

        Example:
            The supplied primary and secondary order is retained; this helper does
            not retry with reversed endpoints.


        :param primary: Primary endpoint Row passed unchanged.
        :param secondary: Secondary endpoint Row passed unchanged.
        :param link_type: Relationship type passed unchanged; None uses database policy.
        :return: Link result returned by database.intralink_rows.
        """
        ...


__all__ = ["IntralinkerAPI"]
