"""Single declarations for supplemental settings and deliberate LiuXin tweak defaults.

The packaged Calibre tweak file supplies common defaults. Only LiuXin differences
and additional keys live here. Operational defaults remain in Preferences.
"""

import os

GLOBAL_DEFAULTS = {
    "database_path": os.path.expanduser("~/library1.db"),
    "filename_pattern": "(?P<title>.+) - (?P<author>[^_]+)",
    "isbndb_com_key": "",
    "network_timeout": 5,
    "output_format": "EPUB",
    "input_format_order": [
        "EPUB",
        "AZW3",
        "MOBI",
        "LIT",
        "PRC",
        "FB2",
        "HTML",
        "HTM",
        "XHTM",
        "SHTML",
        "XHTML",
        "ZIP",
        "ODT",
        "RTF",
        "PDF",
        "TXT",
    ],
    "read_file_metadata": True,
    "worker_process_priority": "normal",
    "swap_author_names": False,
    "add_formats_to_existing": True,
    "check_for_dupes_on_ctl": False,
    "installation_uuid": None,
    "new_book_tags": [],
    "mark_new_books": False,
    "saved_searches": "",
    "user_categories": None,
    "manage_device_metadata": "manual",
    "limit_search_columns": False,
    "limit_search_columns_to": ["title", "authors", "tags", "series", "publisher"],
    "use_primary_find_in_search": True,
    "migrated": False,
}

TWEAK_DEFAULT_OVERRIDES = {
    "author_name_copywords": (
        "Corporation",
        "Company",
        "Co.",
        "Agency",
        "Council",
        "Committee",
        "Inc.",
        "Institute",
        "Society",
        "Club",
        "Team",
    ),
    "tag_browser_category_order": {"*": 1},
    "per_language_title_sort_articles": {
        "eng": ("A\\s+", "The\\s+", "An\\s+"),
        "epo": ("La\\s+", "L'", "L´"),
        "spa": (
            "El\\s+",
            "La\\s+",
            "Lo\\s+",
            "Los\\s+",
            "Las\\s+",
            "Un\\s+",
            "Una\\s+",
            "Unos\\s+",
            "Unas\\s+",
        ),
        "fra": (
            "Le\\s+",
            "La\\s+",
            "L'",
            "L´",
            "L’",
            "Les\\s+",
            "Un\\s+",
            "Une\\s+",
            "Des\\s+",
            "De\\s+La\\s+",
            "De\\s+",
            "D'",
            "D´",
            "L’",
        ),
        "ita": (
            "Lo\\s+",
            "Il\\s+",
            "L'",
            "L´",
            "La\\s+",
            "Gli\\s+",
            "I\\s+",
            "Le\\s+",
            "Uno\\s+",
            "Un\\s+",
            "Una\\s+",
            "Un'",
            "Un´",
            "Dei\\s+",
            "Degli\\s+",
            "Delle\\s+",
            "Del\\s+",
            "Della\\s+",
            "Dello\\s+",
            "Dell'",
            "Dell´",
        ),
        "por": (
            "A\\s+",
            "O\\s+",
            "Os\\s+",
            "As\\s+",
            "Um\\s+",
            "Uns\\s+",
            "Uma\\s+",
            "Umas\\s+",
        ),
        "ron": ("Un\\s+", "O\\s+", "Nişte\\s+"),
        "deu": (
            "Der\\s+",
            "Die\\s+",
            "Das\\s+",
            "Den\\s+",
            "Ein\\s+",
            "Eine\\s+",
            "Einen\\s+",
            "Dem\\s+",
            "Des\\s+",
            "Einem\\s+",
            "Eines\\s+",
        ),
        "nld": (
            "De\\s+",
            "Het\\s+",
            "Een\\s+",
            "'n\\s+",
            "'s\\s+",
            "Ene\\s+",
            "Ener\\s+",
            "Enes\\s+",
            "Den\\s+",
            "Der\\s+",
            "Des\\s+",
            "'t\\s+",
        ),
        "swe": ("En\\s+", "Ett\\s+", "Det\\s+", "Den\\s+", "De\\s+"),
        "tur": ("Bir\\s+",),
        "afr": ("'n\\s+", "Die\\s+"),
        "ell": (
            "O\\s+",
            "I\\s+",
            "To\\s+",
            "Ta\\s+",
            "Tus\\s+",
            "Tis\\s+",
            "'Enas\\s+",
            "'Mia\\s+",
            "'Ena\\s+",
            "'Enan\\s+",
        ),
        "hun": ("A\\s+", "Az\\s+", "Egy\\s+"),
    },
    "add_new_book_tags_when_importing_books": True,
    "max_content_server_tags_shown": 5,
    "content_server_will_display": ["*"],
    "content_server_wont_display": [],
    "horizontal_scrolling_per_column": True,
    "metadata_single_use_2_cols_for_custom_fields": True,
    "metadata_edit_custom_column_order": [],
    "public_smtp_relay_host_suffixes": ["gmail.com", "live.com", "gmx.com"],
    "server_listen_on": "0.0.0.0",
    "compile_gpm_templates": True,
    "numeric_collation": False,
    "highlight_virtual_library": "yellow",
}


def parse_tweak_values(
    source: str | bytes, *, filename: str = "<tweaks>"
) -> dict[str, object]:
    """Read literal assignments and documentation strings; never execute settings.

    Calls, imports, control flow and assignment to attributes/subscripts are
    rejected. Errors identify the input and line, without evaluating any code.
    """
    import ast

    try:
        tree = ast.parse(source, filename=filename)
    except SyntaxError as error:
        raise ValueError(
            f"Invalid tweak settings in {filename}: {error.msg}"
        ) from error
    values: dict[str, object] = {}
    for node in tree.body:
        if (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            continue
        if not isinstance(node, ast.Assign) or not all(
            isinstance(target, ast.Name) for target in node.targets
        ):
            raise ValueError(
                f"Only literal setting assignments are allowed in {filename}:{node.lineno}."
            )
        try:
            value = ast.literal_eval(node.value)
        except (ValueError, TypeError) as error:
            raise ValueError(
                f"Only literal setting values are allowed in {filename}:{node.lineno}."
            ) from error
        for target in node.targets:
            assert isinstance(target, ast.Name)
            if target.id.startswith("_"):
                raise ValueError(
                    f"Private setting names are not allowed in {filename}:{node.lineno}."
                )
            values[target.id] = value
    return values
