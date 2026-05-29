from __future__ import unicode_literals

from LiuXin_alpha.catalog.metadata_tools.add.agent_creator_org_adder_mixin import AgentCreatorOrgMixin
from LiuXin_alpha.catalog.metadata_tools.add.book_and_title_adder import BookAndTitleAdderMixin
from LiuXin_alpha.catalog.metadata_tools.add.comment_adder_mixin import CommentAdderMixin
from LiuXin_alpha.catalog.metadata_tools.add.genre_adder_mixin import GenreAdderMixin
from LiuXin_alpha.catalog.metadata_tools.add.identifier_adder_mixin import IdentifierAdderMixin
from LiuXin_alpha.catalog.metadata_tools.add.language_adder_mixin import LanguageAdderMixin
from LiuXin_alpha.catalog.metadata_tools.add.note_adder_mixin import NoteAdderMixin
from LiuXin_alpha.catalog.metadata_tools.add.series_adder_mixin import SeriesAdderMixin
from LiuXin_alpha.catalog.metadata_tools.add.subject_adder_mixin import SubjectAdderMixin
from LiuXin_alpha.catalog.metadata_tools.add.synopsis_adder_mixin import SynopsisAdderMixin
from LiuXin_alpha.catalog.metadata_tools.add.tag_adder_mixin import TagAdderMixin
from LiuXin_alpha.catalog.metadata_tools.add.wemi_adder import WEMIAdderMixin


class Add(
    BookAndTitleAdderMixin,
    WEMIAdderMixin,
    AgentCreatorOrgMixin,
    CommentAdderMixin,
    GenreAdderMixin,
    IdentifierAdderMixin,
    LanguageAdderMixin,
    NoteAdderMixin,
    SeriesAdderMixin,
    SubjectAdderMixin,
    SynopsisAdderMixin,
    TagAdderMixin,
):
    """
    Composition root for adder mixins.
    """

    def __init__(self, database):
        self.db = database
        self.ensure = None
        self.apply = None
        self._last_title_wemi_bundle = None


__all__ = ["Add"]
