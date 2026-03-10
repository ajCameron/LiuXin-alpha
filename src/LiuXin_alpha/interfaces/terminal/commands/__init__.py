"""Terminal command extensions for the text browser."""

from __future__ import annotations

from .base import TerminalCommandAPI
from .ingest import IngestDiskCommand
from .link import LinkCommand, LinksCommand, UnlinkCommand
from .new_creator import NewCreatorWizardCommand
from .new_expression import NewExpressionWizardCommand
from .new_genre import NewGenreWizardCommand
from .new_item import NewItemWizardCommand
from .new_manifestation import NewManifestationWizardCommand
from .new_note import NewNoteWizardCommand
from .new_organisation import NewOrganisationWizardCommand
from .new_publisher import NewPublisherWizardCommand
from .new_series import NewSeriesWizardCommand
from .new_store import NewStoreWizardCommand
from .new_subject import NewSubjectWizardCommand
from .new_tag import NewTagWizardCommand
from .new_title import NewTitleWizardCommand
from .new_work import NewWorkWizardCommand
from .note_on import NoteOnCommand
from .off import (
    OffGenreCommand,
    OffLanguageCommand,
    OffNoteCommand,
    OffSeriesCommand,
    OffSubjectCommand,
    OffTagCommand,
)
from .on import (
    OnGenreCommand,
    OnLanguageCommand,
    OnNoteCommand,
    OnSeriesCommand,
    OnSubjectCommand,
    OnTagCommand,
)
from .quit import QuitCommand
from .show import (
    ShowAllCommand,
    ShowGenresCommand,
    ShowLanguageCommand,
    ShowNotesCommand,
    ShowSeriesCommand,
    ShowSubjectsCommand,
    ShowTagsCommand,
)
from .summary import SummaryCommand
from .sync import SyncStoreCommand
from .top import TopCommand
from .store_view import StoreFilesCommand, StoreListCommand, StoreShowCommand

__all__ = [
    "TerminalCommandAPI",
    "IngestDiskCommand",
    "SummaryCommand",
    "SyncStoreCommand",
    "QuitCommand",
    "LinkCommand",
    "UnlinkCommand",
    "LinksCommand",
    "NewStoreWizardCommand",
    "NewCreatorWizardCommand",
    "NewExpressionWizardCommand",
    "NewItemWizardCommand",
    "NewGenreWizardCommand",
    "NewNoteWizardCommand",
    "NewOrganisationWizardCommand",
    "NewPublisherWizardCommand",
    "NewSeriesWizardCommand",
    "NewSubjectWizardCommand",
    "NewTagWizardCommand",
    "NewTitleWizardCommand",
    "NewWorkWizardCommand",
    "NewManifestationWizardCommand",
    "OnNoteCommand",
    "OnTagCommand",
    "OnGenreCommand",
    "OnSubjectCommand",
    "OnLanguageCommand",
    "OnSeriesCommand",
    "OffNoteCommand",
    "OffTagCommand",
    "OffGenreCommand",
    "OffSubjectCommand",
    "OffLanguageCommand",
    "OffSeriesCommand",
    "NoteOnCommand",
    "ShowTagsCommand",
    "ShowNotesCommand",
    "ShowGenresCommand",
    "ShowSubjectsCommand",
    "ShowLanguageCommand",
    "ShowSeriesCommand",
    "ShowAllCommand",
    "TopCommand",
    "StoreListCommand",
    "StoreShowCommand",
    "StoreFilesCommand",
]
