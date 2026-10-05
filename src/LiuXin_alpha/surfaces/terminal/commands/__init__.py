"""
Collect built-in terminal commands and construct their ordered default instances.

Imports eagerly load the command implementations. At import time, classes in
``MUTATING_COMMAND_CLASSES`` are marked for the browser's post-command refresh;
this changes class metadata, not database contents. The factory returns command
objects but leaves registration and execution to the caller.
"""

from __future__ import annotations

from .base import TerminalCommandAPI
from .clear import ClearCommand
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
from .jobs import (
    JobsCancelCommand,
    JobsListCommand,
    JobsPanelCommand,
    JobsShowCommand,
    JobsTailCommand,
)
from .link import LinkCommand, LinksCommand, UnlinkCommand
from .mutate import DeleteCommand, EditCommand, SetCommand
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
from .telemetry import TelemetryPanelCommand
from .top import TopCommand
from .store_view import StoreFilesCommand, StoreListCommand, StoreShowCommand

DEFAULT_COMMAND_CLASSES = (
    ClearCommand,
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
    SetCommand,
    EditCommand,
    DeleteCommand,
    DbUnlockCommand,
    JobsListCommand,
    JobsShowCommand,
    JobsTailCommand,
    JobsCancelCommand,
    JobsPanelCommand,
    TelemetryPanelCommand,
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

MUTATING_COMMAND_CLASSES = (
    SetCommand,
    EditCommand,
    DeleteCommand,
    LinkCommand,
    UnlinkCommand,
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
)

for _command_class in MUTATING_COMMAND_CLASSES:
    _command_class.mutates_data = True


def build_default_commands() -> list[TerminalCommandAPI]:
    """
    Instantiate each configured default command once, preserving declaration order.

    Each call returns a new list of fresh instances. No browser is supplied and
    no command is registered or executed. Constructor failures propagate rather
    than returning a partial list.

    Example:
        >>> commands = build_default_commands()
        >>> [command.name for command in commands[:3]]
        ['clear', 'help', 'tables']
        >>> commands[0] is build_default_commands()[0]
        False


    :return: Command instances in ``DEFAULT_COMMAND_CLASSES`` order, ready for registration.
    """
    return [command_class() for command_class in DEFAULT_COMMAND_CLASSES]


__all__ = [
    "TerminalCommandAPI",
    "ClearCommand",
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
    "SetCommand",
    "EditCommand",
    "DeleteCommand",
    "DbUnlockCommand",
    "JobsListCommand",
    "JobsShowCommand",
    "JobsTailCommand",
    "JobsCancelCommand",
    "JobsPanelCommand",
    "TelemetryPanelCommand",
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
