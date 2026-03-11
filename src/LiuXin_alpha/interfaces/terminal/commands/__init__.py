"""Terminal command extensions for the text browser."""

from __future__ import annotations

from .base import TerminalCommandAPI
from .core import (
    BrowseCommand,
    CountCommand,
    HelpCommand,
    NextCommand,
    PageSizeCommand,
    PrevCommand,
    RowCommand,
    SchemaCommand,
    TablesCommand,
    UseCommand,
)
from .db import DbUnlockCommand
from .ingest import IngestDiskCommand
from .jobs import JobsCancelCommand, JobsListCommand, JobsPanelCommand, JobsShowCommand
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
from .search import SearchCommand
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

DEFAULT_COMMAND_CLASSES = (
    HelpCommand,
    TablesCommand,
    UseCommand,
    SchemaCommand,
    CountCommand,
    BrowseCommand,
    NextCommand,
    PrevCommand,
    RowCommand,
    PageSizeCommand,
    QuitCommand,
    SummaryCommand,
    SearchCommand,
    DbUnlockCommand,
    JobsListCommand,
    JobsShowCommand,
    JobsCancelCommand,
    JobsPanelCommand,
    TopCommand,
    LinkCommand,
    UnlinkCommand,
    LinksCommand,
    NoteOnCommand,
    OnNoteCommand,
    OnTagCommand,
    OnGenreCommand,
    OnSubjectCommand,
    OnLanguageCommand,
    OnSeriesCommand,
    OffNoteCommand,
    OffTagCommand,
    OffGenreCommand,
    OffSubjectCommand,
    OffLanguageCommand,
    OffSeriesCommand,
    ShowTagsCommand,
    ShowNotesCommand,
    ShowGenresCommand,
    ShowSubjectsCommand,
    ShowLanguageCommand,
    ShowSeriesCommand,
    ShowAllCommand,
    NewStoreWizardCommand,
    NewCreatorWizardCommand,
    NewExpressionWizardCommand,
    NewItemWizardCommand,
    NewGenreWizardCommand,
    NewNoteWizardCommand,
    NewOrganisationWizardCommand,
    NewPublisherWizardCommand,
    NewSeriesWizardCommand,
    NewSubjectWizardCommand,
    NewTagWizardCommand,
    NewTitleWizardCommand,
    NewWorkWizardCommand,
    NewManifestationWizardCommand,
    IngestDiskCommand,
    SyncStoreCommand,
    StoreListCommand,
    StoreShowCommand,
    StoreFilesCommand,
)


def build_default_commands() -> list[TerminalCommandAPI]:
    """Create one instance of each default terminal command class."""
    return [command_class() for command_class in DEFAULT_COMMAND_CLASSES]


__all__ = [
    "TerminalCommandAPI",
    "HelpCommand",
    "TablesCommand",
    "UseCommand",
    "SchemaCommand",
    "CountCommand",
    "BrowseCommand",
    "NextCommand",
    "PrevCommand",
    "RowCommand",
    "PageSizeCommand",
    "IngestDiskCommand",
    "SummaryCommand",
    "SearchCommand",
    "DbUnlockCommand",
    "JobsListCommand",
    "JobsShowCommand",
    "JobsCancelCommand",
    "JobsPanelCommand",
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
    "DEFAULT_COMMAND_CLASSES",
    "build_default_commands",
]
