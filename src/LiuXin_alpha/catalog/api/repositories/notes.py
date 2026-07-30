"""Note repository API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import EntityId, RowInput, RowMapping, WemiLevel
from .exact_entity import ExactEntityRepositoryAPI


@runtime_checkable
class NoteRepositoryAPI(ExactEntityRepositoryAPI, Protocol):
    """Storage and linking API for reusable WEMI notes.

    ``create`` creates an unattached Note. ``add_for_wemi`` creates and links in
    one operation. Use inherited exact matching only when intentional note
    reuse is appropriate.

    Example::

        note_id = catalog.notes.add_for_wemi(
            level="work",
            entity_id=work_id,
            data={"text": "First published anonymously."},
        )
    """

    def add_for_wemi(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> EntityId:
        """
        Create a note and link it to a WEMI entity.

        :param level: WEMI level that will own the relationship.
        :param entity_id: Existing entity ID at ``level``.
        :param data: Note values; ``text`` is the public alias for the note
            body.
        :return: Newly created Note ID.
        """

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        """
        Return notes linked to a WEMI entity.

        :param level: WEMI level containing ``entity_id``.
        :param entity_id: Existing entity ID.
        :return: Linked Note mappings with ``"_catalog_link"`` metadata, or an
            empty sequence when the schema has no such relationship.
        """
