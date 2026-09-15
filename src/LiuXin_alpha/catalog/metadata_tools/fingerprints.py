"""
Build best-effort relationship token sets for legacy book/title comparison.

Tokens contain table names and row IDs, not normalized title text or
cryptographic hashes. Database IDs make them meaningful only within a shared
identity space. Suppressed relationship errors can leave incomplete sets.
"""

# Methods to cope with fingerprint assets (files and folders).
# Bit of a mess, frankly.
# Todo: Think there are other implementations out there - need to be centralized here

from copy import deepcopy


def _row_value(row, key, default=None):
    """
    Read an optional row value while tolerating non-dict row access failures.

    Plain dict lookup uses get directly. Other row access catches every Exception;
    it does not catch BaseException subclasses.

    Example:
        >>> _row_value({"book_id": None}, "book_id", 7) is None
        True


    :param row: Dictionary or row-like object supporting membership/subscription.
    :param key: Column/key to inspect.
    :param default: Fallback for missing values or non-dict access errors.
    :return: Stored value, including None, or the fallback.
    """

    if isinstance(row, dict):
        return row.get(key, default)
    try:
        if key in row:
            return row[key]
        return default
    except Exception:
        return default


def generate_book_fingerprint(db, book_row):
    """
    Union book-linked tokens with the resolved title family's tokens.

    Fallback to book_id occurs only when book_title is None. A missing title
    yields an empty starting set. Book-side reads exclude books and titles;
    per-table discovery/retrieval errors are skipped. Title lookup, initial
    title fingerprinting, and iteration of retrieved rows can still raise.

    Example:
        A book with no title but a linked tag with ID 7 can produce {"tags_7"}.


    :param db: Database exposing legacy books/titles and relationship helpers.
    :param book_row: Book row or mapping; book_title is preferred over book_id.
    :return: Set of table_ID relationship tokens; an incomplete set is possible.
    """
    title_id = _row_value(book_row, "book_title", None)
    if title_id is None:
        title_id = _row_value(book_row, "book_id", None)

    title_row = db.get_row_from_id("titles", title_id) if title_id is not None else None
    if title_row is not None:
        fingerprint = generate_title_fingerprint(db=db, title_row=title_row)
    else:
        fingerprint = set()

    # Include all other main tables in the fingerprint
    main_tables = set(deepcopy(db.main_tables))
    main_tables.discard("books")
    main_tables.discard("titles")
    for table in main_tables:

        base_print = deepcopy(table) + "_{}"
        try:
            if not db.driver_wrapper.get_link_table_name("books", table):
                continue
            linked_rows = db.get_interlinked_rows(primary_row=book_row, secondary_table=table)
        except Exception:
            continue
        for row in linked_rows:
            fingerprint.add(base_print.format(row.row_id))

    return fingerprint


def generate_title_fingerprint(db, title_row):
    """
    Union a title's tokens with its immediate intralinked titles' tokens.

    Start with the title's own fingerprint. If title intralinks exist, visit
    both directions. Each directional loop suppresses Exception, retaining
    already unioned results; the initial title fingerprint and intralink-table
    capability check can still raise.

    Example:
        A translation linked directly to the title can contribute its linked
        metadata IDs; a translation-of-translation is not recursively traversed.


    :param db: Database providing relationship discovery and retrieval.
    :param title_row: Title row whose direct and adjacent relationships are inspected.
    :return: Set of table_ID tokens; no recursive traversal or title-text normalization.
    """
    fingerprint = set()

    # generate the title fingerprint and add it
    fingerprint = fingerprint.union(generate_one_title_fingerprint(db=db, title_row=title_row))

    if db.driver_wrapper.check_for_intralink_table("titles"):
        # Match the title as primary row
        try:
            for p_title_row in db.get_intralinked_rows(primary_row=title_row, secondary_row=None):
                fingerprint = fingerprint.union(generate_one_title_fingerprint(db=db, title_row=p_title_row))
        except Exception:
            pass

        # Match the title as secondary rows
        try:
            for s_title_row in db.get_intralinked_rows(primary_row=None, secondary_row=title_row):
                fingerprint = fingerprint.union(generate_one_title_fingerprint(db=db, title_row=s_title_row))
        except Exception:
            pass

    return fingerprint


def generate_one_title_fingerprint(db, title_row):
    """
    Collect table_ID tokens for relationships of one legacy title.

    Link discovery/retrieval failures are skipped per table. Iteration and
    row_id access happen outside that exception handler and may still fail.
    Books are included when a link route exists. This does not follow title
    intralinks or hash the title's text.

    Example:
        A linked tag row with ID 7 contributes ``tags_7``.


    :param db: Database exposing main_tables, link discovery and row retrieval.
    :param title_row: Title row used as the primary endpoint.
    :return: Set of tokens from linked rows, excluding the titles table itself.
    """
    fp = set()

    main_tables = set(deepcopy(db.main_tables))
    main_tables.discard("titles")

    for table in main_tables:

        base_print = deepcopy(table) + "_{}"
        try:
            if not db.driver_wrapper.get_link_table_name("titles", table):
                continue
            linked_rows = db.get_interlinked_rows(primary_row=title_row, secondary_table=table)
        except Exception:
            continue
        for row in linked_rows:
            fp.add(base_print.format(row.row_id))

    return fp
