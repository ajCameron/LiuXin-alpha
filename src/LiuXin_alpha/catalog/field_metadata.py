
"""
Describe Catalog/cache fields, their storage mappings and search namespaces.

These compatibility containers retain mutable descriptors in internal maps.
They expose field definitions rather than entity values. Keep the two
container surfaces and their legacy built-in factories behaviorally aligned
with callers; they are not general-purpose dict replacements.
"""


import traceback
from collections import OrderedDict

from LiuXin_alpha.utils.libraries.liuxin_six import iterkeys
from LiuXin_alpha.utils.libraries.liuxin_six import itervalues
from LiuXin_alpha.utils.libraries.liuxin_six import iteritems

from LiuXin_alpha.preferences import preferences as tweaks
from LiuXin_alpha.utils.language_tools.icu import lower as icu_lower
from LiuXin_alpha.utils.localization import _

from LiuXin_alpha.databases.constants import VALID_DATA_TYPES


def calibre_name_to_liuxin_name(table_name: str) -> str:
    """
    Translate five legacy singular table names to their plural spellings.

    Example:
        >>> calibre_name_to_liuxin_name("publisher")
        'publishers'


    :param table_name: Case-sensitive table name; no normalization is applied.
    :return: publishers, ratings, comments, covers or genres for the recognized names;
        otherwise the original value.
    """
    # Translation between the calibre and LiuXin names
    if table_name == "publisher":
        return "publishers"
    if table_name == "rating":
        return "ratings"
    if table_name == "comment":
        return "comments"
    if table_name == "cover":
        return "covers"
    if table_name == "genre":
        return "genres"

    return table_name


# Todo: We've currently got stuff like the fields to declare and use in multiple different places - need to make that
#       dryer - should only declare and act on the field names here.
# Todo: the field is being seperately noted as custom elsewhere - when it's actually in the metadata - fix
# Todo: Implement and test a last modified field for all tables in the database
# Todo: "in_table" and "main_table" have become degenerate - need to remove them
def _builtin_field_metadata():
    """
    Build fresh LiuXin-compatible field descriptors in category order.

    Storage hints cover legacy titles/books, auxiliary/link tables and virtual
    fields. The factory does not validate the referenced database schema.

    Example:
        Changing the active UI language affects newly generated translated names.


    :return: List of key/dictionary pairs with independent nested literals.
    """
    return [
        (
            "authors",
            {
                "table": "authors",
                "column": "author",
                "link_column": "author",
                "category_sort": "sort",
                "datatype": "text",
                "is_multiple": {
                    "cache_to_list": ",",
                    "ui_to_list": "&",
                    "list_to_ui": " & ",
                },
                "kind": "field",
                "name": _("Authors"),
                "search_terms": ["authors", "author"],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
                "main_table": "titles",
                "auxiliary_table": "creators",
            },
        ),
        (
            "languages",
            {
                "table": "languages",
                "column": "language_code",
                "link_column": "lang_code",
                "category_sort": "language_code",
                "datatype": "text",
                "is_multiple": {
                    "cache_to_list": ",",
                    "ui_to_list": ",",
                    "list_to_ui": ", ",
                },
                "kind": "field",
                "name": _("Languages"),
                "search_terms": ["languages", "language"],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
                "main_table": "titles",
                "auxiliary_table": "languages",
            },
        ),
        (
            "series",
            {
                "table": "series",
                "table_id": "series_id",
                "column": "series",
                "link_column": "series",
                "category_sort": "(title_sort(name))",
                "datatype": "series",
                "is_multiple": {},
                "kind": "field",
                "name": _("Series"),
                "search_terms": ["series"],
                "link_attrs": [
                    "index",
                ],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
                "main_table": "titles",
                "auxiliary_table": "series",
            },
        ),
        (
            "genre",
            {
                "table": "genres",
                "table_id": "genre_id",
                "column": "genre",
                "link_column": "genres",
                "category_sort": "(title_sort(name))",
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Genre"),
                "search_terms": ["genre"],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
                "main_table": "titles",
                "auxiliary_table": "genres",
            },
        ),
        (
            "subjects",
            {
                "table": "subjects",
                "table_id": "subject_id",
                "column": "subject",
                "link_column": "subjects",
                "category_sort": "(title_sort(name))",
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Subject"),
                "search_terms": ["subject"],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
                "main_table": "titles",
                "auxiliary_table": "subjects",
            },
        ),
        (
            "synopses",
            {
                "table": "synopses",
                "table_id": "synopsis_id",
                "column": "synopsis",
                "link_column": "synopses",
                "category_sort": "(title_sort(name))",
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Synopsis"),
                "search_terms": ["synopsis"],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
                "main_table": "titles",
                "auxiliary_table": "synopses",
            },
        ),
        (
            "notes",
            {
                "table": "notes",
                "table_id": "note_id",
                "column": "note",
                "link_column": "notes",
                "category_sort": "(title_sort(name))",
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Note"),
                "search_terms": ["note"],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
                "main_table": "titles",
                "auxiliary_table": "notes",
                "val_unique": False,
            },
        ),
        (
            "formats",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {
                    "cache_to_list": ",",
                    "ui_to_list": ",",
                    "list_to_ui": ", ",
                },
                "kind": "field",
                "name": _("Formats"),
                "search_terms": ["formats", "format"],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
                "main_table": "books",
                "auxiliary_table": "formats",
            },
        ),
        (
            "covers",
            {
                "table": None,
                "column": "has_cover",
                "datatype": "text",
                "is_multiple": {
                    "cache_to_list": ",",
                    "ui_to_list": ",",
                    "list_to_ui": ", ",
                },
                "kind": "field",
                "name": _("Covers"),
                "search_terms": ["covers"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
                "main_table": "books",
                "auxiliary_table": "covers",
            },
        ),
        (
            "publisher",
            {
                "table": "publishers",
                "table_id": "publisher_id",
                "column": "publisher",
                "link_column": "publisher",
                "category_sort": "publisher",
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Publisher"),
                "search_terms": ["publisher"],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
                "main_table": "titles",
                "auxiliary_table": "publishers",
            },
        ),
        (
            "rating",
            {
                "table": "ratings",
                "column": "rating",
                "link_column": "rating",
                "category_sort": "rating",
                "datatype": "rating",
                "is_multiple": {},
                "kind": "field",
                "name": _("Rating"),
                "search_terms": ["rating"],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
                "main_table": "titles",
                "auxiliary_table": "ratings",
            },
        ),
        # Todo: Not sure if actually in use
        (
            "news",
            {
                "table": "news",
                "column": "name",
                "category_sort": "name",
                "datatype": None,
                "is_multiple": {},
                "kind": "category",
                "name": _("News"),
                "search_terms": [],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
                "main_table": "titles",
                "auxiliary_table": "news",
            },
        ),
        (
            "tags",
            {
                "table": "tags",
                "column": "tag",
                "link_column": "tag",
                "category_sort": "name",
                "datatype": "text",
                "is_multiple": {
                    "cache_to_list": ",",
                    "ui_to_list": ",",
                    "list_to_ui": ", ",
                },
                "kind": "field",
                "name": _("Tags"),
                "search_terms": ["tags", "tag"],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
                "main_table": "titles",
                "auxiliary_table": "tags",
            },
        ),
        # Todo: Automatically extend search_terms with all the identifiers known to constants
        (
            "identifiers",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {
                    "cache_to_list": ",",
                    "ui_to_list": ",",
                    "list_to_ui": ", ",
                },
                "kind": "field",
                "name": _("Identifiers"),
                "search_terms": ["identifiers", "identifier", "isbn"],
                "is_custom": False,
                "is_category": True,
                "is_csp": True,
                "main_table": "titles",
                "auxiliary_table": "identifiers",
            },
        ),
        (
            "author_sort",
            {
                "table": None,
                "column": "author_sort",
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Author Sort"),
                "search_terms": ["author_sort"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
                "in_table": "books",
                "liuxin_table_name": "title_creator_sort",
                "main_table": "titles",
                "auxiliary_table": "titles",
            },
        ),
        (
            "au_map",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {
                    "cache_to_list": ",",
                    "ui_to_list": None,
                    "list_to_ui": None,
                },
                "kind": "field",
                "name": None,
                "search_terms": [],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
        (
            "comments",
            {
                "table": "comments",
                "column": "comment",
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Comments"),
                "search_terms": ["comments", "comment"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
                "main_table": "titles",
                "auxiliary_table": "comments",
            },
        ),
        (
            "cover",
            {
                "table": None,
                "column": None,
                "datatype": "int",
                "is_multiple": {},
                "kind": "field",
                "name": _("Cover"),
                "search_terms": ["cover"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
                "in_table": "books",
                "main_table": "books",
                "auxiliary_table": "covers",
            },
        ),
        (
            "id",
            {
                "table": None,
                "column": None,
                "datatype": "int",
                "is_multiple": {},
                "kind": "field",
                "name": None,
                "search_terms": ["id"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
                "in_table": "titles",
                "main_table": "titles",
                "auxiliary_table": "titles",
            },
        ),
        (
            "last_modified",
            {
                "table": None,
                "column": None,
                "datatype": "datetime",
                "is_multiple": {},
                "kind": "field",
                "name": _("Modified"),
                "search_terms": ["last_modified"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
                "main_table": "titles",
                "auxiliary_table": "titles",
            },
        ),
        (
            "ondevice",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("On Device"),
                "search_terms": ["ondevice"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
                "main_table": "virtual",
                "auxiliary_table": "virtual",
            },
        ),
        (
            "path",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Path"),
                "search_terms": [],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
                "main_table": "books",
                "auxiliary_table": "books",
            },
        ),
        (
            "pubdate",
            {
                "table": None,
                "column": None,
                "datatype": "datetime",
                "is_multiple": {},
                "kind": "field",
                "name": _("Published"),
                "search_terms": ["pubdate"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
                "main_table": "titles",
                "auxiliary_table": "titles",
            },
        ),
        (
            "marked",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": None,
                "search_terms": ["marked"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
                "main_table": "virtual",
                "auxiliary_table": "virtual",
            },
        ),
        # Todo: Not sure 'in_table' for this method actually makes good sense
        (
            "series_index",
            {
                "table": None,
                "column": None,
                "datatype": "float",
                "is_multiple": {},
                "kind": "field",
                "name": None,
                "search_terms": ["series_index"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
                "in_table": "series_title_links",
                "main_table": "titles",
                "auxiliary_table": "series",
            },
        ),
        (
            "series_sort",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Series Sort"),
                "search_terms": ["series_sort"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
                "main_table": "titles",
                "auxiliary_table": "titles",
            },
        ),
        (
            "sort",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Title Sort"),
                "search_terms": ["title_sort"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
                "in_table": "titles",
                "main_table": "titles",
                "auxiliary_table": "titles",
            },
        ),
        (
            "size",
            {
                "table": None,
                "column": None,
                "datatype": "float",
                "is_multiple": {},
                "kind": "field",
                "name": _("Size"),
                "search_terms": ["size"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
                "main_table": "meta",
                "auxiliary_table": "meta",
            },
        ),
        (
            "timestamp",
            {
                "table": None,
                "column": "datestamp",
                "datatype": "datetime",
                "is_multiple": {},
                "kind": "field",
                "name": _("Date"),
                "search_terms": ["date"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
                "liuxin_table_name": "title_datestamp",
                "main_table": "titles",
                "auxiliary_table": "titles",
            },
        ),
        (
            "title",
            {
                "table": None,
                "column": "title",
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Title"),
                "search_terms": ["title"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
                "in_table": "titles",
                "main_table": "titles",
                "auxiliary_table": "titles",
            },
        ),
        (
            "uuid",
            {
                "table": None,
                "column": "uuid",
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": None,
                "search_terms": ["uuid"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
                "in_table": "books",
                "main_table": "books",
                "auxiliary_table": "books",
            },
        ),
    ]


# }}}


class FieldMetadata(dict):
    """
    Manage mutable field descriptors in ordered internal registries.

    Descriptors describe schema, storage, display hints and search aliases, not
    values on a particular book. Standard keys are unprefixed; custom labels
    normally use #. Kind distinguishes fields, built-in categories, user
    categories and saved searches. Multiplicity mappings define cache/UI split
    and join separators; an empty mapping denotes a single value. Table, column,
    link_column and category_sort describe storage or specialized category reads;
    None may require a derived/cache value. Name is the display label, rec_index
    is the database result position and is_csp denotes colon-separated pairs.

    The dict superclass is retained for compatibility. Supported mapping methods
    use _tb_cats; unrelated inherited dict methods need not reflect those records.
    Direct assignment is forbidden but returned descriptors are live and mutable.
    Custom maps and aliases require the dedicated registration/removal methods.

    Example:
        Use ``metadata["title"]["datatype"]`` to inspect schema; use repositories
        to retrieve the title value belonging to a Work.
    """

    VALID_DATA_TYPES: frozenset[str] = VALID_DATA_TYPES

    # search labels that are not db columns
    search_items = ["all", "search"]

    def __init__(self) -> None:
        """
        Build fresh ordered descriptors, search aliases and custom-field registries.

        Both container classes use _builtin_field_metadata, including the Calibre
        class. Validate builtin field datatypes and alias uniqueness, then read date
        display formats from preferences. No database is opened. The bound get method
        does not implement the title_sort alias supported by subscription.

        Example:
            For a fresh container, ``metadata.get("title_sort")`` is None while
            ``metadata["title_sort"]`` resolves sort.


        :return: None; stores fresh built-ins and binds get to the exact-key map lookup.
        """
        super(FieldMetadata, self).__init__()
        self._field_metadata = _builtin_field_metadata()
        self._tb_cats = OrderedDict()
        self._tb_custom_fields = {}
        self._search_term_map = {}
        self.custom_label_to_key_map = {}

        for k, v in self._field_metadata:
            if v["kind"] == "field" and v["datatype"] not in self.VALID_DATA_TYPES:
                raise ValueError("Unknown datatype %s for field %s" % (v["datatype"], k))
            self._tb_cats[k] = v
            self._tb_cats[k]["label"] = k
            self._tb_cats[k]["display"] = {}
            self._tb_cats[k]["is_editable"] = True
            self._add_search_terms_to_map(k, v["search_terms"])

        self._tb_cats["timestamp"]["display"] = {"date_format": tweaks["gui_timestamp_display_format"]}
        self._tb_cats["pubdate"]["display"] = {"date_format": tweaks["gui_pubdate_display_format"]}
        self._tb_cats["last_modified"]["display"] = {"date_format": tweaks["gui_last_modified_display_format"]}
        self.custom_field_prefix = "#"
        self.get = self._tb_cats.get

    def __getitem__(self, key):
        """
        Read a live descriptor, with title_sort treated as an alias of sort.

        Example:
            Reading ``metadata["title_sort"]`` returns the same record as ``metadata["sort"]``.


        :param key: Internal metadata key; most methods do not resolve search aliases.
        :return: Mutable record stored under the resolved key.
        :raises KeyError: The resolved key is absent.
        """

        if key == "title_sort":
            return self._tb_cats["sort"]
        return self._tb_cats[key]

    def __setitem__(self, key, val):
        """
        Reject direct descriptor assignment.

        Example:
            Use add_custom_field or a category method to insert records.


        :param key: Requested key, ignored.
        :param val: Requested value, ignored.
        :return: Never returns normally.
        :raises AttributeError: Assignment is forbidden for these containers.
        """

        raise AttributeError("Assigning to this object is forbidden")

    def __delitem__(self, key):
        """
        Delete a record from the main ordered map only.

        Search aliases, custom records, label maps and companion fields are not
        cleaned up. No title_sort alias resolution is performed.

        Example:
            Deleting a custom key can leave its record in custom_field_metadata().


        :param key: Internal metadata key; most methods do not resolve search aliases.
        :return: None; updates in-memory metadata only.
        :raises KeyError: The exact key is absent.
        """

        del self._tb_cats[key]

    def __iter__(self):
        """
        Iterate the main map in insertion order.

        Example:
            The virtual title_sort alias is absent unless explicitly stored.


        :return: Iterator of current keys; mutation during iteration can invalidate it.
        """

        for key in self._tb_cats:
            yield key

    def __contains__(self, key):
        """
        Recognize stored keys and the unconditional title_sort alias.

        Example:
            Even after sort is removed, title_sort still reports membership.


        :param key: Hashable candidate key.
        :return: True for a stored key or title_sort.
        """

        return key in self._tb_cats or key == "title_sort"

    def __len__(self):
        """
        Count entries in the main descriptor map.

        Example:
            Adding one user category increases the count by one.


        :return: Number of stored keys, excluding virtual aliases.
        """

        return len(self._tb_cats)

    def __bool__(self):
        """
        Test whether the main descriptor map has any entries.

        Example:
            An emptied map is false even though the title_sort membership alias still exists.


        :return: False only when that map is empty.
        """

        return bool(self._tb_cats)

    def copy(self):
        """
        Copy the main map while sharing its descriptor objects.

        Example:
            Changing ``metadata.copy()["title"]["name"]`` changes the live descriptor too.


        :return: New plain dictionary containing the same mutable records.
        """

        return dict(self._tb_cats)

    def has_key(self, key):
        """
        Expose the legacy spelling of the membership operation.

        Example:
            ``metadata.has_key("title_sort")`` includes the virtual alias.


        :param key: Internal metadata key; most methods do not resolve search aliases.
        :return: Same boolean as the in operator.
        """

        return key in self

    def keys(self):
        """
        Expose a live view of stored metadata keys.

        Example:
            A previously obtained keys view includes a subsequently added category.


        :return: Keys view reflecting subsequent main-map changes.
        """

        return self._tb_cats.keys()

    def sortable_field_keys(self):
        """
        Select field records with a non-None datatype.

        Display configuration and actual comparison support are not inspected.

        Example:
            Series index fields qualify; category records do not.


        :return: New list in main-map order.
        """

        return [
            k
            for k in self._tb_cats.keys()
            if self._tb_cats[k]["kind"] == "field" and self._tb_cats[k]["datatype"] is not None
        ]

    def displayable_field_keys(self):
        """
        Select typed fields while excluding internal display helpers and indexes.

        Example:
            au_map, marked, ondevice, cover, series_sort and recognized series indexes are excluded.


        :return: New list in main-map order.
        """

        return [
            k
            for k in self._tb_cats.keys()
            if self._tb_cats[k]["kind"] == "field"
            and self._tb_cats[k]["datatype"] is not None
            and k not in ("au_map", "marked", "ondevice", "cover", "series_sort")
            and not self.is_series_index(k)
        ]

    def standard_field_keys(self):
        """
        Select non-custom records whose kind is field.

        Example:
            A custom Series companion has is_custom=False and can appear in this list.


        :return: New list in main-map order.
        """

        return [
            k for k in self._tb_cats.keys() if self._tb_cats[k]["kind"] == "field" and not self._tb_cats[k]["is_custom"]
        ]

    def custom_field_keys(self, include_composites=True):
        """
        Select custom field records, optionally omitting composites.

        Example:
            With include_composites=False, a calculated composite field is omitted.


        :param include_composites: Whether datatype=composite records are included.
        :return: New list in main-map order.
        """

        res = []
        for k in self._tb_cats.keys():
            fm = self._tb_cats[k]
            if fm["kind"] == "field" and fm["is_custom"] and (fm["datatype"] != "composite" or include_composites):
                res.append(k)
        return res

    def all_field_keys(self):
        """
        Select records whose kind is field, including custom fields.

        Example:
            The news category and user categories do not appear in this field-only list.


        :return: New list excluding category, user and search records.
        """

        return [k for k in self._tb_cats.keys() if self._tb_cats[k]["kind"] == "field"]

    def iterkeys(self):
        """
        Expose the legacy iterator spelling for main-map keys.

        Example:
            Iterating keys includes dynamic categories but does not synthesize aliases.


        :return: Key iterator in insertion order.
        """

        for key in self._tb_cats:
            yield key

    def itervalues(self):
        """
        Iterate live descriptor objects from the main map.

        Example:
            Mutating a yielded descriptor changes the container; no record copy is made.


        :return: Iterator over shared records in insertion order.
        """

        return itervalues(self._tb_cats)

    def values(self):
        """
        Expose a live view of the main-map descriptors.

        Example:
            New categories appear in an already obtained values view.


        :return: Values view holding shared mutable records.
        """

        return self._tb_cats.values()

    def iteritems(self):
        """
        Yield stored key/descriptor pairs in insertion order.

        Example:
            A caller can inspect each record's kind without copying all descriptors.


        :return: Iterator of pairs sharing the live descriptor objects.
        """

        for key in self._tb_cats:
            yield (key, self._tb_cats[key])

    def custom_iteritems(self):
        """
        Yield pairs from the dedicated custom-field map.

        Example:
            Generated Series companions are absent because they are not stored in the custom map.


        :return: Iterator of custom keys and shared records.
        """

        for key, meta in iteritems(self._tb_custom_fields):
            yield (key, meta)

    def items(self):
        """
        Snapshot the main map's pairs without copying descriptors.

        Example:
            Later additions do not extend this list, but existing record mutations remain visible.


        :return: New list of key/record tuples in insertion order.
        """

        return list(self._tb_cats.items())

    def is_custom_field(self, key):
        """
        Classify a key solely by its custom prefix.

        Example:
            An unknown ``#missing`` key still counts as custom by this predicate.


        :param key: String key to inspect; existence is not required.
        :return: Whether the key starts with custom_field_prefix.
        """

        return key.startswith(self.custom_field_prefix)

    def is_ignorable_field(self, key):
        """
        Classify custom-prefixed and @-prefixed names as ignorable.

        Example:
            ``@Shelf`` is ignorable even if no such user category has been registered.


        :param key: String key to inspect; existence is not required.
        :return: True for either namespace prefix.
        """
        return self.is_custom_field(key) or key.startswith("@")

    def ignorable_field_keys(self):
        """
        Select stored keys in the custom or @ namespace.

        Example:
            A custom Series companion is included even though its is_custom flag is false.


        :return: New list in main-map order.
        """

        return [k for k in iterkeys(self._tb_cats) if self.is_ignorable_field(k)]

    def is_series_index(self, key):
        """
        Recognize float _index fields with an existing base key.

        Example:
            A float ``#cycle_index`` qualifies when ``#cycle`` exists, regardless of its datatype.


        :param key: Candidate key, including unknown or malformed values.
        :return: Boolean; common lookup/type/attribute errors produce False.
        """

        try:
            m = self._tb_cats[key]
            return m["datatype"] == "float" and key.endswith("_index") and key[:-6] in self._tb_cats
        except (KeyError, ValueError, TypeError, AttributeError):
            return False

    def key_to_label(self, key):
        """
        Read a stored label, falling back to the exact key.

        Example:
            A stored None label is returned as None; it does not trigger the key fallback.


        :param key: Internal metadata key; most methods do not resolve search aliases.
        :return: Stored label value, or key if no label entry exists.
        :raises KeyError: The exact key is absent; title_sort is not resolved here.
        """

        if "label" not in self._tb_cats[key]:
            return key
        return self._tb_cats[key]["label"]

    def label_to_key(self, label, prefer_custom=False):
        """
        Resolve internal keys, non-custom labels and custom labels in precedence order.

        Without custom preference, exact keys win, followed by the first non-custom
        record with a matching label, then the custom label map.

        Example:
            If both title and #title exist, prefer_custom=True selects #title for label title.


        :param label: Internal key or stored label; not a translated display name.
        :param prefer_custom: Try the custom label map before exact keys and non-custom labels.
        :return: Resolved internal key.
        :raises ValueError: No candidate resolves the label.
        """

        if prefer_custom:
            if label in self.custom_label_to_key_map:
                return self.custom_label_to_key_map[label]
        if label in self._tb_cats:
            return label
        for key, metadata in self._tb_cats.items():
            if metadata.get("label") == label and not self.is_custom_field(key):
                return key
        if not prefer_custom:
            if label in self.custom_label_to_key_map:
                return self.custom_label_to_key_map[label]
        raise ValueError("Unknown key [%s]" % label)

    def all_metadata(self):
        """
        Snapshot the main map while sharing live record values.

        Example:
            This includes dynamic categories; it is not a serialized copy of auxiliary maps.


        :return: New plain dictionary of all stored descriptors.
        """

        l = {}
        for k in self._tb_cats:
            l[k] = self._tb_cats[k]
        return l

    def custom_field_metadata(self, include_composites=True):
        """
        Expose custom records, optionally filtering calculated composites.

        Example:
            Changing the unfiltered mapping itself changes the dedicated custom registry.


        :param include_composites: True returns the live custom map; false builds a filtered main-map selection.
        :return: Live custom map when unfiltered, otherwise a new dictionary sharing records.
        """

        if include_composites:
            return self._tb_custom_fields
        l = {}
        for k in self.custom_field_keys(False):
            l[k] = self._tb_cats[k]
        return l

    def add_custom_field(
        self,
        label,
        table,
        column,
        datatype,
        colnum,
        name,
        display,
        is_editable,
        is_multiple,
        is_category,
        is_csp=False,
        in_table="books",
    ):
        """
        Register or refresh an in-memory custom descriptor and optional Series companion.

        A same-key record is refreshed only when is_custom is exactly True and its
        label, colnum and effective in_table agree. Otherwise it is a duplicate error.
        Datatype validation precedes record updates. Series fields add a non-custom
        float _index companion and aliases. Refreshing an existing companion changes
        only in_table; changing away from Series does not remove the old companion.
        New records and alias registration are not atomic: a duplicate alias may fail
        after map insertion. This method creates no database columns.

        Example:
            Refreshing the same label, colnum and in_table preserves the descriptor object.


        :param label: Unprefixed custom label; custom_field_prefix is prepended.
        :param table: Storage table name or None.
        :param column: Storage column name or None.
        :param datatype: Logical datatype, validated against VALID_DATA_TYPES.
        :param colnum: Custom-column number used in refresh identity checks.
        :param name: Display name stored unchanged.
        :param display: Display mapping retained by reference, not made immutable.
        :param is_editable: Editability flag stored without coercion.
        :param is_multiple: Multiplicity/separator mapping retained by reference.
        :param is_category: Whether the field forms a browse category.
        :param is_csp: Colon-separated-pair flag; not a composite-datatype flag.
        :param in_table: Main table label; also part of refresh identity.
        :return: None; updates in-memory metadata only.
        :raises ValueError: The key conflicts, datatype is unsupported, or a new alias is duplicated.
        """
        key = self.custom_field_prefix + label
        if key in self._tb_cats:
            # Idempotent refresh: startup/tests can rebuild FieldMetadata multiple times.
            # If this is the *same* custom field (same label/colnum/in_table), update in place.
            existing = self._tb_cats[key]
            if (
                existing.get("is_custom") is True
                and existing.get("label") == label
                and existing.get("colnum") == colnum
                and existing.get("in_table", "books") == in_table
            ):
                if datatype not in self.VALID_DATA_TYPES:
                    raise ValueError("Unknown datatype %s for field %s" % (datatype, key))

                existing.update(
                    {
                        "table": table,
                        "column": column,
                        "datatype": datatype,
                        "is_multiple": is_multiple,
                        "kind": "field",
                        "name": name,
                        "search_terms": [key],
                        "label": label,
                        "colnum": colnum,
                        "display": display,
                        "is_custom": True,
                        "is_category": is_category,
                        "link_column": "value",
                        "category_sort": "value",
                        "is_csp": is_csp,
                        "is_editable": is_editable,
                        "in_table": in_table,
                    }
                )
                self._tb_cats[key] = existing
                self._tb_custom_fields[key] = existing
                if key not in self._search_term_map:
                    self._add_search_terms_to_map(key, [key])
                self.custom_label_to_key_map[label] = key

                if datatype == "series":
                    idx_key = key + "_index"
                    if idx_key in self._tb_cats:
                        self._tb_cats[idx_key]["in_table"] = in_table
                    else:
                        self._tb_cats[idx_key] = {
                            "table": None,
                            "column": None,
                            "datatype": "float",
                            "is_multiple": {},
                            "kind": "field",
                            "name": "",
                            "search_terms": [idx_key],
                            "label": label + "_index",
                            "colnum": None,
                            "display": {},
                            "is_custom": False,
                            "is_category": False,
                            "link_column": None,
                            "category_sort": None,
                            "is_editable": False,
                            "is_csp": False,
                            "in_table": in_table,
                        }
                    if idx_key not in self._search_term_map:
                        self._add_search_terms_to_map(idx_key, [idx_key])
                    self.custom_label_to_key_map[label + "_index"] = idx_key
                return

            raise ValueError("Duplicate custom field [%s]" % label)

        if datatype not in self.VALID_DATA_TYPES:
            raise ValueError("Unknown datatype %s for field %s" % (datatype, key))
        self._tb_cats[key] = {
            "table": table,
            "column": column,
            "datatype": datatype,
            "is_multiple": is_multiple,
            "kind": "field",
            "name": name,
            "search_terms": [key],
            "label": label,
            "colnum": colnum,
            "display": display,
            "is_custom": True,
            "is_category": is_category,
            "link_column": "value",
            "category_sort": "value",
            "is_csp": is_csp,
            "is_editable": is_editable,
            "in_table": in_table,
        }
        self._tb_custom_fields[key] = self._tb_cats[key]
        self._add_search_terms_to_map(key, [key])
        self.custom_label_to_key_map[label] = key
        if datatype == "series":
            key += "_index"
            self._tb_cats[key] = {
                "table": None,
                "column": None,
                "datatype": "float",
                "is_multiple": {},
                "kind": "field",
                "name": "",
                "search_terms": [key],
                "label": label + "_index",
                "colnum": None,
                "display": {},
                "is_custom": False,
                "is_category": False,
                "link_column": None,
                "category_sort": None,
                "is_editable": False,
                "is_csp": False,
                "in_table": in_table,
            }
            self._add_search_terms_to_map(key, [key])
            self.custom_label_to_key_map[label + "_index"] = key

    def remove_dynamic_categories(self):
        """
        Remove category records of kind user or search and their declared aliases.

        Only records with a truthy is_category flag qualify. Other maps are retained.

        Example:
            Standard fields, custom fields and the news category remain.


        :return: None; updates in-memory metadata only.
        """

        for key in list(self._tb_cats.keys()):
            val = self._tb_cats[key]
            if val["is_category"] and val["kind"] in ("user", "search"):
                for k in self._tb_cats[key]["search_terms"]:
                    if k in self._search_term_map:
                        del self._search_term_map[k]
                del self._tb_cats[key]

    def remove_user_categories(self):
        """
        Remove user-category records and their declared search aliases.

        Example:
            Saved-search category records remain; missing aliases are tolerated.


        :return: None; updates in-memory metadata only.
        """

        for key in list(self._tb_cats.keys()):
            val = self._tb_cats[key]
            if val["is_category"] and val["kind"] == "user":
                for k in self._tb_cats[key]["search_terms"]:
                    if k in self._search_term_map:
                        del self._search_term_map[k]
                del self._tb_cats[key]

    def _remove_grouped_search_terms(self):
        """
        Remove aliases whose target is a list.

        Example:
            A group targeting ["title", "authors"] is removed; a string-target alias survives.


        :return: None; updates in-memory metadata only.
        """

        to_remove = [v for v in self._search_term_map if isinstance(self._search_term_map[v], list)]
        for v in to_remove:
            del self._search_term_map[v]

    def add_grouped_search_terms(self, gst):
        """
        Replace list-target groups, then register supplied aliases.

        Duplicate-term ValueErrors are printed as tracebacks and processing continues.
        Targets are retained by reference. Other failures propagate without rollback.

        Example:
            Calling this with {} removes list-target groups but preserves old string-target aliases.


        :param gst: Group name to string or list target mapping; targets are not validated.
        :return: None; updates in-memory metadata only.
        """

        self._remove_grouped_search_terms()
        for t in gst:
            try:
                self._add_search_terms_to_map(gst[t], [t])
            except ValueError:
                traceback.print_exc()

    def cc_series_index_column_for(self, key):
        """
        Compute the tuple position immediately after a field's rec_index.

        Example:
            A field at rec_index=8 yields 9 even if it is not a Series.


        :param key: Internal metadata key; most methods do not resolve search aliases.
        :return: Stored rec_index plus one; no datatype or companion validation is performed.
        :raises KeyError: The field or its rec_index is absent.
        """

        return self._tb_cats[key]["rec_index"] + 1

    def add_user_category(self, label, name):
        """
        Insert a user category with exact and ICU-lowercase search aliases.

        Duplicate keys fail before insertion. Alias conflicts can fail after the
        record and earlier aliases are inserted.

        Example:
            ``@Shelf`` registers both @Shelf and @shelf aliases.


        :param label: Exact category key, commonly beginning with @.
        :param name: Display name stored unchanged.
        :return: None; updates in-memory metadata only.
        :raises ValueError: The key or one of its aliases already exists.
        """

        if label in self._tb_cats:
            raise ValueError("Duplicate user field [%s]" % label)
        st = [label]
        if icu_lower(label) != label:
            st.append(icu_lower(label))
        self._tb_cats[label] = {
            "table": None,
            "column": None,
            "datatype": None,
            "is_multiple": {},
            "kind": "user",
            "name": name,
            "search_terms": st,
            "is_custom": False,
            "is_category": True,
            "is_csp": False,
        }
        self._add_search_terms_to_map(label, st)

    def add_search_category(self, label, name):
        """
        Insert a saved-search category without search aliases.

        Example:
            A saved-search category appears in keys() but not all_field_keys().


        :param label: Exact category key.
        :param name: Display name stored unchanged.
        :return: None; updates in-memory metadata only.
        :raises ValueError: The key already exists.
        """

        if label in self._tb_cats:
            raise ValueError("Duplicate user field [%s]" % label)
        self._tb_cats[label] = {
            "table": None,
            "column": None,
            "datatype": None,
            "is_multiple": {},
            "kind": "search",
            "name": name,
            "search_terms": [],
            "is_custom": False,
            "is_category": True,
            "is_csp": False,
        }

    def set_field_record_index(self, label, index, prefer_custom=False):
        """
        Set a record position using exact keys and prefixed custom keys.

        Resolution here does not use label_to_key or its stored-label search.

        Example:
            For label title, custom preference chooses #title if present, otherwise title.


        :param label: Field key or unprefixed custom label.
        :param index: Position stored without type or range validation.
        :param prefer_custom: Try the prefixed key before the exact key.
        :return: None; updates in-memory metadata only.
        :raises KeyError: Neither selected key exists.
        """

        if prefer_custom:
            key = self.custom_field_prefix + label
            if key not in self._tb_cats:
                key = label
        else:
            if label in self._tb_cats:
                key = label
            else:
                key = self.custom_field_prefix + label
        self._tb_cats[key]["rec_index"] = index  # let the exception fly ...

    def get_search_terms(self):
        """
        List sorted registered aliases followed by special search items.

        Example:
            Defaults append all and search after sorting aliases; the result is not globally sorted.


        :return: New list ending in the current search_items order.
        """

        s_keys = sorted(self._search_term_map.keys())
        for v in self.search_items:
            s_keys.append(v)
        return s_keys

    def _add_search_terms_to_map(self, key, terms):
        """
        Register aliases for a target without normalizing names.

        Example:
            If the second alias conflicts, the first newly registered alias remains.


        :param key: String or grouped-list target retained unchanged.
        :param terms: Iterable of alias keys, or None to do nothing.
        :return: None; updates in-memory metadata only.
        :raises ValueError: An alias already exists, even for the same target.
        """

        if terms is not None:
            for t in terms:
                if t in self._search_term_map:
                    raise ValueError('Attempt to add duplicate search term "%s"' % t)
                self._search_term_map[t] = key

    def search_term_to_field_key(self, term):
        """
        Resolve an exact search alias, preserving unknown terms.

        Example:
            An unknown ``unregistered`` alias returns ``unregistered`` rather than raising KeyError.


        :param term: Case-sensitive alias; lowercasing is not performed here.
        :return: Registered string/list target, or the original term when unknown.
        """

        return self._search_term_map.get(term, term)

    def searchable_fields(self):
        """
        Select field records declaring at least one search term.

        This inspects record declarations, not whether aliases still exist in the map.

        Example:
            Grouped aliases do not themselves add fields; news and dynamic categories are excluded.


        :return: New list in main-map order.
        """

        return [
            k
            for k in self._tb_cats.keys()
            if self._tb_cats[k]["kind"] == "field" and len(self._tb_cats[k]["search_terms"]) > 0
        ]



def _calibre_builtin_field_metadata():
    # This is a function so that changing the UI language allows newly created
    # field metadata objects to have correctly translated labels for builtin
    # fields.
    """
    Build the retained Calibre storage descriptor reference list.

    Translated names are evaluated for each invocation.

    Example:
        The authors reference uses column name; current containers initialize from
        _builtin_field_metadata instead.


    :return: Fresh ordered list of key/dictionary pairs.
    """

    return [
        (
            "authors",
            {
                "table": "authors",
                "column": "name",
                "link_column": "author",
                "category_sort": "sort",
                "datatype": "text",
                "is_multiple": {
                    "cache_to_list": ",",
                    "ui_to_list": "&",
                    "list_to_ui": " & ",
                },
                "kind": "field",
                "name": _("Authors"),
                "search_terms": ["authors", "author"],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
            },
        ),
        (
            "languages",
            {
                "table": "languages",
                "column": "lang_code",
                "link_column": "lang_code",
                "category_sort": "lang_code",
                "datatype": "text",
                "is_multiple": {
                    "cache_to_list": ",",
                    "ui_to_list": ",",
                    "list_to_ui": ", ",
                },
                "kind": "field",
                "name": _("Languages"),
                "search_terms": ["languages", "language"],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
            },
        ),
        (
            "series",
            {
                "table": "series",
                "column": "name",
                "link_column": "series",
                "category_sort": "(title_sort(name))",
                "datatype": "series",
                "is_multiple": {},
                "kind": "field",
                # 'name':ngettext('Series', 'Series', 1),
                "name": _("Series"),
                "search_terms": ["series"],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
            },
        ),
        (
            "formats",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {
                    "cache_to_list": ",",
                    "ui_to_list": ",",
                    "list_to_ui": ", ",
                },
                "kind": "field",
                "name": _("Formats"),
                "search_terms": ["formats", "format"],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
            },
        ),
        (
            "publisher",
            {
                "table": "publishers",
                "column": "name",
                "link_column": "publisher",
                "category_sort": "name",
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Publisher"),
                "search_terms": ["publisher"],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
            },
        ),
        (
            "rating",
            {
                "table": "ratings",
                "column": "rating",
                "link_column": "rating",
                "category_sort": "rating",
                "datatype": "rating",
                "is_multiple": {},
                "kind": "field",
                "name": _("Rating"),
                "search_terms": ["rating"],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
                "clear_unused": False,
            },
        ),
        (
            "news",
            {
                "table": "news",
                "column": "name",
                "category_sort": "name",
                "datatype": None,
                "is_multiple": {},
                "kind": "category",
                "name": _("News"),
                "search_terms": [],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
            },
        ),
        (
            "tags",
            {
                "table": "tags",
                "column": "name",
                "link_column": "tag",
                "category_sort": "name",
                "datatype": "text",
                "is_multiple": {
                    "cache_to_list": ",",
                    "ui_to_list": ",",
                    "list_to_ui": ", ",
                },
                "kind": "field",
                "name": _("Tags"),
                "search_terms": ["tags", "tag"],
                "is_custom": False,
                "is_category": True,
                "is_csp": False,
            },
        ),
        (
            "identifiers",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {
                    "cache_to_list": ",",
                    "ui_to_list": ",",
                    "list_to_ui": ", ",
                },
                "kind": "field",
                "name": _("Identifiers"),
                "search_terms": ["identifiers", "identifier", "isbn"],
                "is_custom": False,
                "is_category": True,
                "is_csp": True,
            },
        ),
        (
            "author_sort",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Author Sort"),
                "search_terms": ["author_sort"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
        (
            "au_map",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {
                    "cache_to_list": ",",
                    "ui_to_list": None,
                    "list_to_ui": None,
                },
                "kind": "field",
                "name": None,
                "search_terms": [],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
        (
            "comments",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Comments"),
                "search_terms": ["comments", "comment"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
        (
            "cover",
            {
                "table": None,
                "column": None,
                "datatype": "int",
                "is_multiple": {},
                "kind": "field",
                "name": _("Cover"),
                "search_terms": ["cover"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
        (
            "id",
            {
                "table": None,
                "column": None,
                "datatype": "int",
                "is_multiple": {},
                "kind": "field",
                "name": None,
                "search_terms": ["id"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
        (
            "last_modified",
            {
                "table": None,
                "column": None,
                "datatype": "datetime",
                "is_multiple": {},
                "kind": "field",
                "name": _("Modified"),
                "search_terms": ["last_modified"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
        (
            "ondevice",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("On Device"),
                "search_terms": ["ondevice"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
        (
            "path",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Path"),
                "search_terms": [],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
        (
            "pubdate",
            {
                "table": None,
                "column": None,
                "datatype": "datetime",
                "is_multiple": {},
                "kind": "field",
                "name": _("Published"),
                "search_terms": ["pubdate"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
        (
            "marked",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": None,
                "search_terms": ["marked"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
        (
            "series_index",
            {
                "table": None,
                "column": None,
                "datatype": "float",
                "is_multiple": {},
                "kind": "field",
                "name": None,
                "search_terms": ["series_index"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
        (
            "series_sort",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Series Sort"),
                "search_terms": ["series_sort"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
        (
            "sort",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Title Sort"),
                "search_terms": ["title_sort"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
        (
            "size",
            {
                "table": None,
                "column": None,
                "datatype": "float",
                "is_multiple": {},
                "kind": "field",
                "name": _("Size"),
                "search_terms": ["size"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
        (
            "timestamp",
            {
                "table": None,
                "column": None,
                "datatype": "datetime",
                "is_multiple": {},
                "kind": "field",
                "name": _("Date"),
                "search_terms": ["date"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
        (
            "title",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": _("Title"),
                "search_terms": ["title"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
        (
            "uuid",
            {
                "table": None,
                "column": None,
                "datatype": "text",
                "is_multiple": {},
                "kind": "field",
                "name": None,
                "search_terms": ["uuid"],
                "is_custom": False,
                "is_category": False,
                "is_csp": False,
            },
        ),
    ]


# }}}


class CalibreFieldMetadata(dict):
    """
    Expose the field registry with Calibre tuple-index assignment support.

    Descriptors describe schema, storage, display hints and search aliases, not
    values on a particular book. Standard keys are unprefixed; custom labels
    normally use #. Kind distinguishes fields, built-in categories, user
    categories and saved searches. Multiplicity mappings define cache/UI split
    and join separators; an empty mapping denotes a single value. Table, column,
    link_column and category_sort describe storage or specialized category reads;
    None may require a derived/cache value. Name is the display label, rec_index
    is the database result position and is_csp denotes colon-separated pairs.

    The dict superclass is retained for compatibility. Supported mapping methods
    use _tb_cats; unrelated inherited dict methods need not reflect those records.
    Direct assignment is forbidden but returned descriptors are live and mutable.
    Custom maps and aliases require the dedicated registration/removal methods.

    Despite its name this class currently initializes from the same LiuXin
    built-in factory as FieldMetadata; the separate Calibre factory is retained
    reference data.

    Example:
        Use ``metadata["title"]["datatype"]`` to inspect schema; use repositories
        to retrieve the title value belonging to a Work.
    """

    VALID_DATA_TYPES = frozenset(
        [
            None,
            "rating",
            "text",
            "comments",
            "datetime",
            "int",
            "float",
            "bool",
            "series",
            "composite",
            "enumeration",
        ]
    )

    # search labels that are not db columns
    search_items = ["all", "search"]

    def __init__(self):
        """
        Build fresh ordered descriptors, search aliases and custom-field registries.

        Both container classes use _builtin_field_metadata, including the Calibre
        class. Validate builtin field datatypes and alias uniqueness, then read date
        display formats from preferences. No database is opened. The bound get method
        does not implement the title_sort alias supported by subscription.

        Example:
            For a fresh container, ``metadata.get("title_sort")`` is None while
            ``metadata["title_sort"]`` resolves sort.


        :return: None; stores fresh built-ins and binds get to the exact-key map lookup.
        """

        self._field_metadata = _builtin_field_metadata()
        self._tb_cats = OrderedDict()
        self._tb_custom_fields = {}
        self._search_term_map = {}
        self.custom_label_to_key_map = {}
        for k, v in self._field_metadata:
            if v["kind"] == "field" and v["datatype"] not in self.VALID_DATA_TYPES:
                raise ValueError("Unknown datatype %s for field %s" % (v["datatype"], k))
            self._tb_cats[k] = v
            self._tb_cats[k]["label"] = k
            self._tb_cats[k]["display"] = {}
            self._tb_cats[k]["is_editable"] = True
            self._add_search_terms_to_map(k, v["search_terms"])
        self._tb_cats["timestamp"]["display"] = {"date_format": tweaks["gui_timestamp_display_format"]}
        self._tb_cats["pubdate"]["display"] = {"date_format": tweaks["gui_pubdate_display_format"]}
        self._tb_cats["last_modified"]["display"] = {"date_format": tweaks["gui_last_modified_display_format"]}
        self.custom_field_prefix = "#"
        self.get = self._tb_cats.get

    def __getitem__(self, key):
        """
        Read a live descriptor, with title_sort treated as an alias of sort.

        Example:
            Reading ``metadata["title_sort"]`` returns the same record as ``metadata["sort"]``.


        :param key: Internal metadata key; most methods do not resolve search aliases.
        :return: Mutable record stored under the resolved key.
        :raises KeyError: The resolved key is absent.
        """

        if key == "title_sort":
            return self._tb_cats["sort"]
        return self._tb_cats[key]

    def __setitem__(self, key, val):
        """
        Reject direct descriptor assignment.

        Example:
            Use add_custom_field or a category method to insert records.


        :param key: Requested key, ignored.
        :param val: Requested value, ignored.
        :return: Never returns normally.
        :raises AttributeError: Assignment is forbidden for these containers.
        """

        raise AttributeError("Assigning to this object is forbidden")

    def __delitem__(self, key):
        """
        Delete a record from the main ordered map only.

        Search aliases, custom records, label maps and companion fields are not
        cleaned up. No title_sort alias resolution is performed.

        Example:
            Deleting a custom key can leave its record in custom_field_metadata().


        :param key: Internal metadata key; most methods do not resolve search aliases.
        :return: None; updates in-memory metadata only.
        :raises KeyError: The exact key is absent.
        """

        del self._tb_cats[key]

    def __iter__(self):
        """
        Iterate the main map in insertion order.

        Example:
            The virtual title_sort alias is absent unless explicitly stored.


        :return: Iterator of current keys; mutation during iteration can invalidate it.
        """

        for key in self._tb_cats:
            yield key

    def __contains__(self, key):
        """
        Recognize stored keys and the unconditional title_sort alias.

        Example:
            Even after sort is removed, title_sort still reports membership.


        :param key: Hashable candidate key.
        :return: True for a stored key or title_sort.
        """

        return key in self._tb_cats or key == "title_sort"

    def __len__(self):
        """
        Count entries in the main descriptor map.

        Example:
            Adding one user category increases the count by one.


        :return: Number of stored keys, excluding virtual aliases.
        """

        return len(self._tb_cats)

    def __bool__(self):
        """
        Test whether the main descriptor map has any entries.

        Example:
            An emptied map is false even though the title_sort membership alias still exists.


        :return: False only when that map is empty.
        """

        return bool(self._tb_cats)

    def copy(self):
        """
        Copy the main map while sharing its descriptor objects.

        Example:
            Changing ``metadata.copy()["title"]["name"]`` changes the live descriptor too.


        :return: New plain dictionary containing the same mutable records.
        """

        return dict(self._tb_cats)

    def has_key(self, key):
        """
        Expose the legacy spelling of the membership operation.

        Example:
            ``metadata.has_key("title_sort")`` includes the virtual alias.


        :param key: Internal metadata key; most methods do not resolve search aliases.
        :return: Same boolean as the in operator.
        """

        return key in self

    def keys(self):
        """
        Expose a live view of stored metadata keys.

        Example:
            A previously obtained keys view includes a subsequently added category.


        :return: Keys view reflecting subsequent main-map changes.
        """

        return self._tb_cats.keys()

    def sortable_field_keys(self):
        """
        Select field records with a non-None datatype.

        Display configuration and actual comparison support are not inspected.

        Example:
            Series index fields qualify; category records do not.


        :return: New list in main-map order.
        """

        return [
            k
            for k in self._tb_cats.keys()
            if self._tb_cats[k]["kind"] == "field" and self._tb_cats[k]["datatype"] is not None
        ]

    def displayable_field_keys(self):
        """
        Select typed fields while excluding internal display helpers and indexes.

        Example:
            au_map, marked, ondevice, cover, series_sort and recognized series indexes are excluded.


        :return: New list in main-map order.
        """

        return [
            k
            for k in self._tb_cats.keys()
            if self._tb_cats[k]["kind"] == "field"
            and self._tb_cats[k]["datatype"] is not None
            and k not in ("au_map", "marked", "ondevice", "cover", "series_sort")
            and not self.is_series_index(k)
        ]

    def standard_field_keys(self):
        """
        Select non-custom records whose kind is field.

        Example:
            A custom Series companion has is_custom=False and can appear in this list.


        :return: New list in main-map order.
        """

        return [
            k for k in self._tb_cats.keys() if self._tb_cats[k]["kind"] == "field" and not self._tb_cats[k]["is_custom"]
        ]

    def custom_field_keys(self, include_composites=True):
        """
        Select custom field records, optionally omitting composites.

        Example:
            With include_composites=False, a calculated composite field is omitted.


        :param include_composites: Whether datatype=composite records are included.
        :return: New list in main-map order.
        """

        res = []
        for k in self._tb_cats.keys():
            fm = self._tb_cats[k]
            if fm["kind"] == "field" and fm["is_custom"] and (fm["datatype"] != "composite" or include_composites):
                res.append(k)
        return res

    def all_field_keys(self):
        """
        Select records whose kind is field, including custom fields.

        Example:
            The news category and user categories do not appear in this field-only list.


        :return: New list excluding category, user and search records.
        """

        return [k for k in self._tb_cats.keys() if self._tb_cats[k]["kind"] == "field"]

    def iterkeys(self):
        """
        Expose the legacy iterator spelling for main-map keys.

        Example:
            Iterating keys includes dynamic categories but does not synthesize aliases.


        :return: Key iterator in insertion order.
        """

        for key in self._tb_cats:
            yield key

    def itervalues(self):
        """
        Iterate live descriptor objects from the main map.

        Example:
            Mutating a yielded descriptor changes the container; no record copy is made.


        :return: Iterator over shared records in insertion order.
        """

        return iter(self._tb_cats.values())

    def values(self):
        """
        Expose a live view of the main-map descriptors.

        Example:
            New categories appear in an already obtained values view.


        :return: Values view holding shared mutable records.
        """

        return self._tb_cats.values()

    def iteritems(self):
        """
        Yield stored key/descriptor pairs in insertion order.

        Example:
            A caller can inspect each record's kind without copying all descriptors.


        :return: Iterator of pairs sharing the live descriptor objects.
        """

        for key in self._tb_cats:
            yield (key, self._tb_cats[key])

    def custom_iteritems(self):
        """
        Yield pairs from the dedicated custom-field map.

        Example:
            Generated Series companions are absent because they are not stored in the custom map.


        :return: Iterator of custom keys and shared records.
        """

        for key, meta in self._tb_custom_fields.items():
            yield (key, meta)

    def items(self):
        """
        Snapshot the main map's pairs without copying descriptors.

        Example:
            Later additions do not extend this list, but existing record mutations remain visible.


        :return: New list of key/record tuples in insertion order.
        """

        return list(self._tb_cats.items())

    def is_custom_field(self, key):
        """
        Classify a key solely by its custom prefix.

        Example:
            An unknown ``#missing`` key still counts as custom by this predicate.


        :param key: String key to inspect; existence is not required.
        :return: Whether the key starts with custom_field_prefix.
        """

        return key.startswith(self.custom_field_prefix)

    def is_ignorable_field(self, key):
        """
        Classify custom-prefixed and @-prefixed names as ignorable.

        Example:
            ``@Shelf`` is ignorable even if no such user category has been registered.


        :param key: String key to inspect; existence is not required.
        :return: True for either namespace prefix.
        """
        return self.is_custom_field(key) or key.startswith("@")

    def ignorable_field_keys(self):
        """
        Select stored keys in the custom or @ namespace.

        Example:
            A custom Series companion is included even though its is_custom flag is false.


        :return: New list in main-map order.
        """

        return [k for k in self._tb_cats if self.is_ignorable_field(k)]

    def is_series_index(self, key):
        """
        Recognize float _index fields with an existing base key.

        Example:
            A float ``#cycle_index`` qualifies when ``#cycle`` exists, regardless of its datatype.


        :param key: Candidate key, including unknown or malformed values.
        :return: Boolean; common lookup/type/attribute errors produce False.
        """

        try:
            m = self._tb_cats[key]
            return m["datatype"] == "float" and key.endswith("_index") and key[:-6] in self._tb_cats
        except (KeyError, ValueError, TypeError, AttributeError):
            return False

    def key_to_label(self, key):
        """
        Read a stored label, falling back to the exact key.

        Example:
            A stored None label is returned as None; it does not trigger the key fallback.


        :param key: Internal metadata key; most methods do not resolve search aliases.
        :return: Stored label value, or key if no label entry exists.
        :raises KeyError: The exact key is absent; title_sort is not resolved here.
        """

        if "label" not in self._tb_cats[key]:
            return key
        return self._tb_cats[key]["label"]

    def label_to_key(self, label, prefer_custom=False):
        """
        Resolve internal keys, non-custom labels and custom labels in precedence order.

        Without custom preference, exact keys win, followed by the first non-custom
        record with a matching label, then the custom label map.

        Example:
            If both title and #title exist, prefer_custom=True selects #title for label title.


        :param label: Internal key or stored label; not a translated display name.
        :param prefer_custom: Try the custom label map before exact keys and non-custom labels.
        :return: Resolved internal key.
        :raises ValueError: No candidate resolves the label.
        """

        if prefer_custom:
            if label in self.custom_label_to_key_map:
                return self.custom_label_to_key_map[label]
        if label in self._tb_cats:
            return label
        for key, metadata in self._tb_cats.items():
            if metadata.get("label") == label and not self.is_custom_field(key):
                return key
        if not prefer_custom:
            if label in self.custom_label_to_key_map:
                return self.custom_label_to_key_map[label]
        raise ValueError("Unknown key [%s]" % (label))

    def all_metadata(self):
        """
        Snapshot the main map while sharing live record values.

        Example:
            This includes dynamic categories; it is not a serialized copy of auxiliary maps.


        :return: New plain dictionary of all stored descriptors.
        """

        l = {}
        for k in self._tb_cats:
            l[k] = self._tb_cats[k]
        return l

    def custom_field_metadata(self, include_composites=True):
        """
        Expose custom records, optionally filtering calculated composites.

        Example:
            Changing the unfiltered mapping itself changes the dedicated custom registry.


        :param include_composites: True returns the live custom map; false builds a filtered main-map selection.
        :return: Live custom map when unfiltered, otherwise a new dictionary sharing records.
        """

        if include_composites:
            return self._tb_custom_fields
        l = {}
        for k in self.custom_field_keys(include_composites):
            l[k] = self._tb_cats[k]
        return l

    def add_custom_field(
        self,
        label,
        table,
        column,
        datatype,
        colnum,
        name,
        display,
        is_editable,
        is_multiple,
        is_category,
        is_csp=False,
        in_table="books",
    ):
        """
        Register or refresh an in-memory custom descriptor and optional Series companion.

        A same-key record is refreshed only when is_custom is exactly True and its
        label, colnum and effective in_table agree. Otherwise it is a duplicate error.
        Datatype validation precedes record updates. Series fields add a non-custom
        float _index companion and aliases. Refreshing an existing companion changes
        only in_table; changing away from Series does not remove the old companion.
        New records and alias registration are not atomic: a duplicate alias may fail
        after map insertion. This method creates no database columns.

        Example:
            Refreshing the same label, colnum and in_table preserves the descriptor object.


        :param label: Unprefixed custom label; custom_field_prefix is prepended.
        :param table: Storage table name or None.
        :param column: Storage column name or None.
        :param datatype: Logical datatype, validated against VALID_DATA_TYPES.
        :param colnum: Custom-column number used in refresh identity checks.
        :param name: Display name stored unchanged.
        :param display: Display mapping retained by reference, not made immutable.
        :param is_editable: Editability flag stored without coercion.
        :param is_multiple: Multiplicity/separator mapping retained by reference.
        :param is_category: Whether the field forms a browse category.
        :param is_csp: Colon-separated-pair flag; not a composite-datatype flag.
        :param in_table: Main table label; also part of refresh identity.
        :return: None; updates in-memory metadata only.
        :raises ValueError: The key conflicts, datatype is unsupported, or a new alias is duplicated.
        """

        key = self.custom_field_prefix + label
        if key in self._tb_cats:
            # Idempotent refresh: startup/tests can rebuild FieldMetadata multiple times.
            # If this is the *same* custom field (same label/colnum/in_table), update in place.
            existing = self._tb_cats[key]
            if (
                existing.get("is_custom") is True
                and existing.get("label") == label
                and existing.get("colnum") == colnum
                and existing.get("in_table", "books") == in_table
            ):
                if datatype not in self.VALID_DATA_TYPES:
                    raise ValueError("Unknown datatype %s for field %s" % (datatype, key))

                existing.update(
                    {
                        "table": table,
                        "column": column,
                        "datatype": datatype,
                        "is_multiple": is_multiple,
                        "kind": "field",
                        "name": name,
                        "search_terms": [key],
                        "label": label,
                        "colnum": colnum,
                        "display": display,
                        "is_custom": True,
                        "is_category": is_category,
                        "link_column": "value",
                        "category_sort": "value",
                        "is_csp": is_csp,
                        "is_editable": is_editable,
                        "in_table": in_table,
                    }
                )
                self._tb_cats[key] = existing
                self._tb_custom_fields[key] = existing
                if key not in self._search_term_map:
                    self._add_search_terms_to_map(key, [key])
                self.custom_label_to_key_map[label] = key

                if datatype == "series":
                    idx_key = key + "_index"
                    if idx_key in self._tb_cats:
                        self._tb_cats[idx_key]["in_table"] = in_table
                    else:
                        self._tb_cats[idx_key] = {
                            "table": None,
                            "column": None,
                            "datatype": "float",
                            "is_multiple": {},
                            "kind": "field",
                            "name": "",
                            "search_terms": [idx_key],
                            "label": label + "_index",
                            "colnum": None,
                            "display": {},
                            "is_custom": False,
                            "is_category": False,
                            "link_column": None,
                            "category_sort": None,
                            "is_editable": False,
                            "is_csp": False,
                            "in_table": in_table,
                        }
                    if idx_key not in self._search_term_map:
                        self._add_search_terms_to_map(idx_key, [idx_key])
                    self.custom_label_to_key_map[label + "_index"] = idx_key
                return

            raise ValueError("Duplicate custom field [%s]" % label)

        if datatype not in self.VALID_DATA_TYPES:
            raise ValueError("Unknown datatype %s for field %s" % (datatype, key))
        self._tb_cats[key] = {
            "table": table,
            "column": column,
            "datatype": datatype,
            "is_multiple": is_multiple,
            "kind": "field",
            "name": name,
            "search_terms": [key],
            "label": label,
            "colnum": colnum,
            "display": display,
            "is_custom": True,
            "is_category": is_category,
            "link_column": "value",
            "category_sort": "value",
            "is_csp": is_csp,
            "is_editable": is_editable,
            "in_table": in_table,
        }
        self._tb_custom_fields[key] = self._tb_cats[key]
        self._add_search_terms_to_map(key, [key])
        self.custom_label_to_key_map[label] = key
        if datatype == "series":
            key += "_index"
            self._tb_cats[key] = {
                "table": None,
                "column": None,
                "datatype": "float",
                "is_multiple": {},
                "kind": "field",
                "name": "",
                "search_terms": [key],
                "label": label + "_index",
                "colnum": None,
                "display": {},
                "is_custom": False,
                "is_category": False,
                "link_column": None,
                "category_sort": None,
                "is_editable": False,
                "is_csp": False,
                "in_table": in_table,
            }
            self._add_search_terms_to_map(key, [key])
            self.custom_label_to_key_map[label + "_index"] = key

    def remove_dynamic_categories(self):
        """
        Remove category records of kind user or search and their declared aliases.

        Only records with a truthy is_category flag qualify. Other maps are retained.

        Example:
            Standard fields, custom fields and the news category remain.


        :return: None; updates in-memory metadata only.
        """

        for key in list(self._tb_cats.keys()):
            val = self._tb_cats[key]
            if val["is_category"] and val["kind"] in ("user", "search"):
                for k in self._tb_cats[key]["search_terms"]:
                    if k in self._search_term_map:
                        del self._search_term_map[k]
                del self._tb_cats[key]

    def remove_user_categories(self):
        """
        Remove user-category records and their declared search aliases.

        Example:
            Saved-search category records remain; missing aliases are tolerated.


        :return: None; updates in-memory metadata only.
        """

        for key in list(self._tb_cats.keys()):
            val = self._tb_cats[key]
            if val["is_category"] and val["kind"] == "user":
                for k in self._tb_cats[key]["search_terms"]:
                    if k in self._search_term_map:
                        del self._search_term_map[k]
                del self._tb_cats[key]

    def _remove_grouped_search_terms(self):
        """
        Remove aliases whose target is a list.

        Example:
            A group targeting ["title", "authors"] is removed; a string-target alias survives.


        :return: None; updates in-memory metadata only.
        """

        to_remove = [v for v in self._search_term_map if isinstance(self._search_term_map[v], list)]
        for v in to_remove:
            del self._search_term_map[v]

    def add_grouped_search_terms(self, gst):
        """
        Replace list-target groups, then register supplied aliases.

        Duplicate-term ValueErrors are printed as tracebacks and processing continues.
        Targets are retained by reference. Other failures propagate without rollback.

        Example:
            Calling this with {} removes list-target groups but preserves old string-target aliases.


        :param gst: Group name to string or list target mapping; targets are not validated.
        :return: None; updates in-memory metadata only.
        """

        self._remove_grouped_search_terms()
        for t in gst:
            try:
                self._add_search_terms_to_map(gst[t], [t])
            except ValueError:
                traceback.print_exc()

    def cc_series_index_column_for(self, key):
        """
        Compute the tuple position immediately after a field's rec_index.

        Example:
            A field at rec_index=8 yields 9 even if it is not a Series.


        :param key: Internal metadata key; most methods do not resolve search aliases.
        :return: Stored rec_index plus one; no datatype or companion validation is performed.
        :raises KeyError: The field or its rec_index is absent.
        """

        return self._tb_cats[key]["rec_index"] + 1

    def add_user_category(self, label, name):
        """
        Insert a user category with exact and ICU-lowercase search aliases.

        Duplicate keys fail before insertion. Alias conflicts can fail after the
        record and earlier aliases are inserted.

        Example:
            ``@Shelf`` registers both @Shelf and @shelf aliases.


        :param label: Exact category key, commonly beginning with @.
        :param name: Display name stored unchanged.
        :return: None; updates in-memory metadata only.
        :raises ValueError: The key or one of its aliases already exists.
        """

        if label in self._tb_cats:
            raise ValueError("Duplicate user field [%s]" % (label))
        st = [label]
        if icu_lower(label) != label:
            st.append(icu_lower(label))
        self._tb_cats[label] = {
            "table": None,
            "column": None,
            "datatype": None,
            "is_multiple": {},
            "kind": "user",
            "name": name,
            "search_terms": st,
            "is_custom": False,
            "is_category": True,
            "is_csp": False,
        }
        self._add_search_terms_to_map(label, st)

    def add_search_category(self, label, name):
        """
        Insert a saved-search category without search aliases.

        Example:
            A saved-search category appears in keys() but not all_field_keys().


        :param label: Exact category key.
        :param name: Display name stored unchanged.
        :return: None; updates in-memory metadata only.
        :raises ValueError: The key already exists.
        """

        if label in self._tb_cats:
            raise ValueError("Duplicate user field [%s]" % label)
        self._tb_cats[label] = {
            "table": None,
            "column": None,
            "datatype": None,
            "is_multiple": {},
            "kind": "search",
            "name": name,
            "search_terms": [],
            "is_custom": False,
            "is_category": True,
            "is_csp": False,
        }

    def set_field_record_index_from_field_map(self, field_map):
        """
        Assign tuple positions sequentially with standard-key preference.

        Example:
            If a later key is unknown, positions already assigned to earlier keys remain.


        :param field_map: Mapping from field keys/custom labels to record positions.
        :return: None; updates in-memory metadata only.
        :raises KeyError: A key cannot be resolved by set_field_record_index.
        """
        for (
            k,
            v,
        ) in field_map.items():
            self.set_field_record_index(k, v, prefer_custom=False)

    def set_field_record_index(self, label, index, prefer_custom=False):
        """
        Set a record position using exact keys and prefixed custom keys.

        Resolution here does not use label_to_key or its stored-label search.

        Example:
            For label title, custom preference chooses #title if present, otherwise title.


        :param label: Field key or unprefixed custom label.
        :param index: Position stored without type or range validation.
        :param prefer_custom: Try the prefixed key before the exact key.
        :return: None; updates in-memory metadata only.
        :raises KeyError: Neither selected key exists.
        """
        if prefer_custom:
            key = self.custom_field_prefix + label
            if key not in self._tb_cats:
                key = label
        else:
            if label in self._tb_cats:
                key = label
            else:
                key = self.custom_field_prefix + label
        self._tb_cats[key]["rec_index"] = index  # let the exception fly ...

    def get_search_terms(self):
        """
        List sorted registered aliases followed by special search items.

        Example:
            Defaults append all and search after sorting aliases; the result is not globally sorted.


        :return: New list ending in the current search_items order.
        """

        s_keys = sorted(self._search_term_map.keys())
        for v in self.search_items:
            s_keys.append(v)
        return s_keys

    def _add_search_terms_to_map(self, key, terms):
        """
        Register aliases for a target without normalizing names.

        Example:
            If the second alias conflicts, the first newly registered alias remains.


        :param key: String or grouped-list target retained unchanged.
        :param terms: Iterable of alias keys, or None to do nothing.
        :return: None; updates in-memory metadata only.
        :raises ValueError: An alias already exists, even for the same target.
        """

        if terms is not None:
            for t in terms:
                if t in self._search_term_map:
                    raise ValueError('Attempt to add duplicate search term "%s"' % t)
                self._search_term_map[t] = key

    def search_term_to_field_key(self, term):
        """
        Resolve an exact search alias, preserving unknown terms.

        Example:
            An unknown ``unregistered`` alias returns ``unregistered`` rather than raising KeyError.


        :param term: Case-sensitive alias; lowercasing is not performed here.
        :return: Registered string/list target, or the original term when unknown.
        """

        return self._search_term_map.get(term, term)

    def searchable_fields(self):
        """
        Select field records declaring at least one search term.

        This inspects record declarations, not whether aliases still exist in the map.

        Example:
            Grouped aliases do not themselves add fields; news and dynamic categories are excluded.


        :return: New list in main-map order.
        """

        return [
            k
            for k in self._tb_cats.keys()
            if self._tb_cats[k]["kind"] == "field" and len(self._tb_cats[k]["search_terms"]) > 0
        ]


def fm_from_dict(src):
    """
    Overlay borrowed serialized dynamic state on fresh LiuXin built-ins.

    The supplied search map replaces builtin aliases wholesale. Overlay order is
    custom fields, user categories, then search categories; later keys win. No
    validation or reconstruction of absent Series companions occurs. The bound
    get method continues to reference the augmented main map.

    Example:
        Mutating a supplied custom record is visible in the reconstructed container.


    :param src: Mapping containing custom_fields, user_categories, search_categories,
        search_term_map and custom_label_to_key_map.
    :return: New FieldMetadata sharing supplied maps and descriptor objects.
    :raises KeyError: A required serialized-state key is missing.
    """
    ans = FieldMetadata()
    ans._tb_custom_fields = src["custom_fields"]
    ans._search_term_map = src["search_term_map"]
    ans.custom_label_to_key_map = src["custom_label_to_key_map"]
    for q in ("custom_fields", "user_categories", "search_categories"):
        for k, v in src[q].items():
            ans._tb_cats[k] = v
    return ans
