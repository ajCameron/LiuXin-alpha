
"""
Search in a field of bools.
"""


from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from LiuXin_alpha.databases.utils import force_to_bool
from LiuXin_alpha.utils.localization import _
from LiuXin_alpha.utils.search_query_parser import ParseException
from LiuXin_alpha.utils.text.icu import lower as icu_lower


class BooleanSearch:
    """
    Conduct a search of a boolean field.

    Which might contains text info - for added perversity.
    """
    def __init__(self) -> None:
        """
        Constructor.
        """
        self.local_no = icu_lower(_("no"))
        self.local_yes = icu_lower(_("yes"))
        self.local_unchecked = icu_lower(_("unchecked"))
        self.local_checked = icu_lower(_("checked"))
        self.local_empty = icu_lower(_("empty"))
        self.local_blank = icu_lower(_("blank"))
        self.local_bool_values = {
            self.local_no,
            self.local_unchecked,
            "_no",
            "false",
            "no",
            "unchecked",
            "_unchecked",
            self.local_yes,
            self.local_checked,
            "checked",
            "_checked",
            "_yes",
            "true",
            "yes",
            self.local_empty,
            self.local_blank,
            "blank",
            "_blank",
            "_empty",
            "empty",
        }

    def __call__(self, query, field_iter, bools_are_tristate) -> set[int]:
        matches = set()
        if query not in self.local_bool_values:
            raise ParseException(_('Invalid boolean query "{0}"').format(query))
        for val, book_ids in field_iter():
            val = force_to_bool(val)
            if not bools_are_tristate:
                if val is None or not val:  # item is None or set to false
                    if query in {
                        self.local_no,
                        self.local_unchecked,
                        "unchecked",
                        "_unchecked",
                        "no",
                        "_no",
                        "false",
                    }:
                        matches |= book_ids
                else:  # item is explicitly set to true
                    if query in {
                        self.local_yes,
                        self.local_checked,
                        "checked",
                        "_checked",
                        "yes",
                        "_yes",
                        "true",
                    }:
                        matches |= book_ids
            else:
                if val is None:
                    if query in {
                        self.local_empty,
                        self.local_blank,
                        "blank",
                        "_blank",
                        "empty",
                        "_empty",
                        "false",
                    }:
                        matches |= book_ids
                elif not val:  # is not None and false
                    if query in {
                        self.local_no,
                        self.local_unchecked,
                        "unchecked",
                        "_unchecked",
                        "no",
                        "_no",
                        "true",
                    }:
                        matches |= book_ids
                else:  # item is not None and true
                    if query in {
                        self.local_yes,
                        self.local_checked,
                        "checked",
                        "_checked",
                        "yes",
                        "_yes",
                        "true",
                    }:
                        matches |= book_ids
        return matches
