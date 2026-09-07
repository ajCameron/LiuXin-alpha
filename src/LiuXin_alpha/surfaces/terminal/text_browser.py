"""Historical terminal imports backed by explicit implementation owners.

Application startup lives in app; browser execution and the curses adapter
never import this facade. Replace dependencies in their consuming owners.
"""

import LiuXin_alpha.surfaces.terminal.presentation as _presentation
from LiuXin_alpha.surfaces.terminal.app import (
    build_parser as build_parser,
)
from LiuXin_alpha.surfaces.terminal.app import (
    main as main,
)
from LiuXin_alpha.surfaces.terminal.app import (
    run_windowed_text_browser as run_windowed_text_browser,
)
from LiuXin_alpha.surfaces.terminal.browser import (
    TextDatabaseBrowser as TextDatabaseBrowser,
)
from LiuXin_alpha.surfaces.terminal.browser import (
    _BrowseWindow as _BrowseWindow,
)
from LiuXin_alpha.surfaces.terminal.browser import (
    _CommandCompletion as _CommandCompletion,
)
from LiuXin_alpha.surfaces.terminal.browser import (
    _default_history_file_path as _default_history_file_path,
)
from LiuXin_alpha.surfaces.terminal.database_creation import (
    DatabaseCreationWizardConfig as DatabaseCreationWizardConfig,
)
from LiuXin_alpha.surfaces.terminal.database_creation import (
    create_database_from_wizard as create_database_from_wizard,
)
from LiuXin_alpha.surfaces.terminal.database_creation import (
    run_database_creation_wizard as run_database_creation_wizard,
)

# Explicit historical private aliases; implementations import their owners.
_truncate = _presentation.truncate
_truncate_text = _presentation.truncate_text
_summarize_exception = _presentation.summarize_exception
_stringify_table_cell = _presentation.stringify_table_cell
_preview_row_text = _presentation.preview_row_text
_row_detail_group = _presentation.row_detail_group
_pretty_row_detail_group = _presentation.pretty_row_detail_group
_shorten_column_headers = _presentation.shorten_column_headers
_render_ascii_table = _presentation.render_ascii_table
_safe_int = _presentation.safe_int
_looks_like_id_selector = _presentation.looks_like_id_selector
_ask_text = _presentation.ask_text
_ask_yes_no = _presentation.ask_yes_no

__all__ = [
    "DatabaseCreationWizardConfig",
    "TextDatabaseBrowser",
    "run_database_creation_wizard",
    "create_database_from_wizard",
    "build_parser",
    "run_windowed_text_browser",
    "main",
]
