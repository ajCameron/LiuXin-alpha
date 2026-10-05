#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai
"""
Build legacy Calibre-cache category lists from field providers, user preferences, and saved searches.

Tag values are mutable facet items, not category-tree containers or Core wire
records. Generation can mutate provider items, shared ID sets, user-category
preferences, and an icon map; it is not a pure projection. Field discovery applies
the legacy books-table category rules, not the modern FRBR read-model API.

Retained compatibility behavior includes best-effort user-preference persistence,
case-sensitive quirks in grouped-item consolidation, and Python-2-era Tag string
hooks that recurse under the current Python 3 six_unicode alias. Call __unicode__
explicitly for the legacy text representation rather than assuming str/repr work.
"""


from __future__ import unicode_literals, division, absolute_import, print_function

import copy
from functools import partial
from operator import attrgetter
from builtins import map

from LiuXin_alpha.metadata.utils import author_to_author_sort


from LiuXin_alpha.utils.config.config_base import tweaks
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.text.icu import sort_key, lower as icu_lower

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems, dict_iterkeys as iterkeys, six_unicode

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


CATEGORY_SORTS = ("name", "popularity", "rating")  # This has to be a tuple not a set


class Tag(object):
    """
    Hold one mutable category item with display text, identity, counts, selection state, and matching book IDs.

    Slots restrict attribute names but do not validate values or derive count from
    id_set. Supplied ID sets are retained by reference. Average ratings are halved
    at construction, and the initial tooltip can include a localized rating line.
    The category attribute names the containing facet; this object has no child
    collection or automatic hierarchy construction.

    Example:
        >>> tag = Tag("Fiction", id=7, count=2, category="tags", id_set={1, 2})
        >>> (tag.name, tag.count, tag.is_hierarchical)
        ('Fiction', 2, '')
    """


    __slots__ = (
        "name",
        "original_name",
        "id",
        "count",
        "state",
        "is_hierarchical",
        "is_editable",
        "is_searchable",
        "id_set",
        "avg_rating",
        "sort",
        "use_sort_as_name",
        "tooltip",
        "icon",
        "category",
    )

    def __init__(
        self,
        name,
        id=None,
        count=0,
        state=0,
        avg=0,
        sort=None,
        tooltip=None,
        icon=None,
        category=None,
        id_set=None,
        is_editable=True,
        is_searchable=True,
        use_sort_as_name=False,
    ):
        """
        Store a category item and compose its initial tooltip without normalizing or copying most inputs.

        name initializes both name and original_name; is_hierarchical starts as an
        empty string. A None ID set gets a fresh empty set. A None tooltip defaults
        to (category:name); a positive halved average appends localized rating text.
        Counts and display/selection flags are not checked against the ID set.

        Example:
            >>> tag = Tag("Fiction", category="tags", avg=None)
            >>> (tag.original_name, tag.avg_rating, tag.tooltip)
            ('Fiction', 0, '(tags:Fiction)')


        :param name: Display name retained unchanged as both current and original name.
        :param id: Optional category-item identifier, not a book identifier lookup.
        :param count: Declared matching-book count, stored without recomputation.
        :param state: Caller-owned selection/display state value.
        :param avg: Average rating halved with division by 2.0, or None for zero.
        :param sort: Optional sort value preferred over name when truthy during name sorting.
        :param tooltip: Explicit tooltip text, or None to generate a category/name tooltip.
        :param icon: Optional icon object retained without loading or validation.
        :param category: Containing category key used in display and compatibility metadata.
        :param id_set: Matching-book ID collection retained by reference, or None for a fresh set.
        :param is_editable: Caller-provided editability flag, stored unchanged.
        :param is_searchable: Caller-provided searchability flag, stored unchanged.
        :param use_sort_as_name: Whether presentation should prefer the sort value as the visible name.
        :return: None after initializing every slot and composing any rating tooltip suffix.
        """
        self.name = self.original_name = name
        self.id = id
        self.count = count
        self.state = state
        self.is_hierarchical = ""
        self.is_editable = is_editable
        self.is_searchable = is_searchable
        self.id_set = id_set if id_set is not None else set([])
        self.avg_rating = avg / 2.0 if avg is not None else 0
        self.sort = sort
        self.use_sort_as_name = use_sort_as_name
        if tooltip is None:
            tooltip = "(%s:%s)" % (category, name)
        if self.avg_rating > 0:
            if tooltip:
                tooltip += ": "
            tooltip = _("%(tt)sAverage rating is %(rating)3.1f") % dict(tt=tooltip, rating=self.avg_rating)
        self.tooltip = tooltip
        self.icon = icon
        self.category = category

    def __unicode__(self):
        """
        Format six legacy display fields as colon-separated text without escaping separators.

        Example:
            >>> Tag("Fiction", id=7, count=2, category="tags").__unicode__()
            'Fiction:2:7:0:tags:(tags:Fiction)'


        :return: Text containing name, count, ID, state, category, and tooltip in that order.
        """
        return "%s:%s:%s:%s:%s:%s" % (
            self.name,
            self.count,
            self.id,
            self.state,
            self.category,
            self.tooltip,
        )

    def __str__(self):
        """
        Retain the legacy UTF-8 string hook, which recursively calls itself under the current Python 3 alias.

        six_unicode is str in this checkout, so converting self re-enters this
        method before encoding can occur. This is not a functioning Python 3
        string conversion; use __unicode__ explicitly for its text counterpart.

        Example:
            >>> Tag("Fiction").__str__()  # doctest: +ELLIPSIS
            Traceback (most recent call last):
            ...
            RecursionError: ...


        :return: No normal result with the current alias; the intended legacy bytes conversion never completes.
        :raises RecursionError: When six_unicode(self) repeatedly re-enters this hook.
        """
        return six_unicode(self).encode("utf-8")

    def __repr__(self):
        """
        Delegate representation to the legacy string hook, inheriting its Python 3 recursion failure.

        Example:
            >>> repr(Tag("Fiction"))  # doctest: +ELLIPSIS
            Traceback (most recent call last):
            ...
            RecursionError: ...


        :return: No normal result with the current str-based compatibility alias.
        :raises RecursionError: Through the retained __str__ implementation.
        """
        return str(self)


def find_categories(field_metadata):
    """
    Yield eligible legacy book-category fields and composite-category declarations in metadata iteration order.

    Standard is_category entries exclude user/search kinds. Entries of kind field
    must target books, defaulting an absent in_table to books. If that standard
    branch does not apply, a composite with display.make_category may qualify, also
    only for books. Expected metadata keys and nested mappings are not validated
    separately, so missing keys and provider iteration failures propagate lazily.

    Example:
        >>> eligible = list(find_categories(field_metadata))  # doctest: +SKIP


    :param field_metadata: Metadata provider exposing iteritems and the legacy field-description mappings.
    :return: Iterator of (category key, cache_to_list separator or None, composite-branch flag) triples.
    """
    for category, cat in field_metadata.iteritems():
        # Calibre calls the UI a "Tag Browser" but it is actually a *category* browser.
        # In LiuXin we keep compatibility, but apply one important rule:
        #   - categories are facets over *books* (for now), so fields attached to non-books tables
        #     must not appear as tag-browser categories.
        #
        # Standard categories/fields (non user/search)
        if cat["is_category"] and cat["kind"] not in {"user", "search"}:
            if cat.get("kind") == "field" and cat.get("in_table", "books") != "books":
                continue
            yield (category, cat["is_multiple"].get("cache_to_list", None), False)

        # Composite columns use display.make_category instead of is_category
        elif cat["datatype"] == "composite" and cat["display"].get("make_category", False):
            if cat.get("in_table", "books") != "books":
                continue
            yield (category, cat["is_multiple"].get("cache_to_list", None), True)


def create_tag_class(category, fm, icon_map):
    """
    Return a partial Tag constructor with category-specific icons, editability, and sort-as-name defaults.

    This creates no new class. Noneditable built-ins and composites are fixed by
    category/datatype; the author-name tweak can enable sort-as-name for authors
    or eligible custom multiple-name text fields. A truthy icon map supplies
    built-in icons by field label after checking category-key membership; those
    two names need not coincide. Custom fields reuse custom: and add a category
    alias to the same map. Missing required metadata/icon keys propagate.

    Example:
        >>> factory = create_tag_class("tags", field_metadata, icons)  # doctest: +SKIP
        >>> tag = factory("Fiction", count=2)  # doctest: +SKIP


    :param category: Metadata field key used for the bound category and policy decisions.
    :param fm: Field metadata supporting subscription, key_to_label, and is_custom_field.
    :param icon_map: Optional mutable icon mapping; custom-category aliases are added when truthy.
    :return: functools.partial binding Tag defaults, which callers can override with explicit keywords.
    """
    cat = fm[category]
    dt = cat["datatype"]
    icon = None
    label = fm.key_to_label(category)
    if icon_map:
        if not fm.is_custom_field(category):
            if category in icon_map:
                icon = icon_map[label]
        else:
            icon = icon_map["custom:"]
            icon_map[category] = icon
    is_editable = category not in {"news", "rating", "languages", "formats", "identifiers"} and dt != "composite"

    if tweaks["categories_use_field_for_author_name"] == "author_sort" and (
        category == "authors"
        or (cat["display"].get("is_names", False) and cat["is_custom"] and cat["is_multiple"] and dt == "text")
    ):
        use_sort_as_name = True
    else:
        use_sort_as_name = False

    return partial(
        Tag,
        use_sort_as_name=use_sort_as_name,
        icon=icon,
        is_editable=is_editable,
        category=category,
    )


def clean_user_categories(dbcache):
    """
    Normalize dotted user-category names and attempt to persist a changed mapping without propagating save errors.

    Strip components and discard empty components. Entirely blank names receive
    the first positive integer string absent from the original mapping, not from
    names already generated; collisions therefore keep the later value. Normalized
    collisions likewise overwrite earlier entries. Values are shared, not copied.
    Only comparison/persistence is inside the bare exception handler, which also
    suppresses BaseException subclasses; preference loading and key parsing can fail.

    Example:
        >>> cleaned = clean_user_categories(cache)  # doctest: +SKIP


    :param dbcache: Legacy cache exposing pref and set_pref for user_categories.
    :return: New normalized mapping even if persisting it fails, with original value objects retained.
    """
    user_cats = dbcache.pref("user_categories", {})
    new_cats = {}
    for k in user_cats:
        comps = [c.strip() for c in k.split(".") if c.strip()]
        if len(comps) == 0:
            i = 1
            while True:
                if six_unicode(i) not in user_cats:
                    new_cats[six_unicode(i)] = user_cats[k]
                    break
                i += 1
        else:
            new_cats[".".join(comps)] = user_cats[k]
    try:
        if new_cats != user_cats:
            dbcache.set_pref("user_categories", new_cats)
    except:
        pass
    return new_cats


def sort_categories(items, sort):
    """
    Sort category items in place by descending count/rating or ascending ICU display sort key.

    popularity uses count and rating uses avg_rating. Every other token takes
    the name path, preferring a truthy item.sort to item.name; this helper does not
    validate CATEGORY_SORTS. Python's stable sort preserves tie order.

    Example:
        >>> items = [Tag("less", count=1), Tag("more", count=3)]
        >>> ordered = sort_categories(items, "popularity")
        >>> ordered is items, [tag.name for tag in items]
        (True, ['more', 'less'])


    :param items: Mutable list of Tag-like values with the selected sort attributes.
    :param sort: popularity, rating, or any other value to select ascending name sorting.
    :return: The same list after sorting; missing attributes and key-conversion failures propagate.
    """
    reverse = True
    if sort == "popularity":
        key = attrgetter("count")
    elif sort == "rating":
        key = attrgetter("avg_rating")
    else:
        key = lambda x: sort_key(x.sort or x.name)
        reverse = False
    items.sort(key=key, reverse=reverse)
    return items


# Todo: Want to shift TagsIcons over to display logic
# Todo: Update so that first_letter_sort is actually supported
def get_categories(dbcache, sort="name", book_ids=None, icon_map=None, first_letter_sort=False):
    """
    Assemble legacy field, user/grouped, and saved-search categories using mutable provider Tag values.

    Non-None icon maps must have exactly type TagsIcons, not a subclass. The sort
    token is validated before provider access. Truthy book selections become a
    frozenset; empty selections retain their original type/value. Composite fields
    lazily share selected/all book IDs and a per-call proxy-metadata cache. Rating
    and language maps are required even before field discovery. A rating category
    is subsequently indexed unconditionally.

    Provider lists are sorted in place. Multiple-name text fields other than
    authors have sort text converted to author-sort form. Equal-name rating nodes
    with distinct IDs are merged one match at a time while mutating the list;
    this is not a general complete deduplication pass.

    User categories reuse original Tag objects; grouped-search categories are
    considered only when cleaned user categories are nonempty. Grouped entries
    use shallow Tag copies, sharing ID sets, and their consolidation lookup uses
    lowercase names while storing original spellings. Counts/grouping can therefore
    depend on name case, and ID-set unions can alter source tags. Icon aliases and
    cleaned preferences may be mutated. Saved searches are appended in ICU name
    order regardless of the requested category sort. No outer rollback is provided.

    Example:
        >>> categories = get_categories(cache, sort="popularity", book_ids={1, 2})  # doctest: +SKIP


    :param dbcache: Legacy cache supplying field metadata/providers, rating/language maps, preferences, proxies, and saved searches.
    :param sort: Required category ordering token: name, popularity, or rating.
    :param book_ids: Optional book restriction; None requests all books, while empty collections are forwarded as empty.
    :param icon_map: Optional exact TagsIcons instance, extended with custom/user-category aliases during generation.
    :param first_letter_sort: Retained compatibility argument, currently ignored.
    :return: Category-key mapping to sorted Tag lists, with @-prefixed user categories and nonempty saved searches when available.
    :raises TypeError: If a supplied icon map is not exactly a TagsIcons instance.
    :raises ValueError: If sort is not a supported CATEGORY_SORTS token.
    """
    from LiuXin_alpha.surfaces.tags_icons import TagsIcons

    if icon_map is not None and type(icon_map) != TagsIcons:
        raise TypeError("icon_map passed to get_categories must be of type TagIcons")
    if sort not in CATEGORY_SORTS:
        raise ValueError("sort " + sort + " not a valid value")

    fm = dbcache.field_metadata
    book_rating_map = dbcache.fields["rating"].book_value_map
    lang_map = dbcache.fields["languages"].book_value_map

    categories = {}
    book_ids = frozenset(book_ids) if book_ids else book_ids
    pm_cache = {}

    def get_metadata(book_id):
        """
        Reuse a non-None proxy-metadata result within this category-generation call.

        A provider result of None is stored but treated as a miss on the next
        access, so it is fetched again. Provider errors propagate without caching.

        Example:
            >>> metadata = get_metadata(book_id)  # doctest: +SKIP


        :param book_id: Cache key and identifier forwarded unchanged to _get_proxy_metadata.
        :return: Cached non-None proxy or the provider's newly returned value, possibly None.
        """
        ans = pm_cache.get(book_id)
        if ans is None:
            ans = pm_cache[book_id] = dbcache._get_proxy_metadata(book_id)
        return ans

    bids = None

    for category, is_multiple, is_composite in find_categories(fm):
        tag_class = create_tag_class(category, fm, icon_map)
        if is_composite:
            if bids is None:
                bids = dbcache._all_book_ids() if book_ids is None else book_ids
            cats = dbcache.fields[category].get_composite_categories(
                tag_class, book_rating_map, bids, is_multiple, get_metadata
            )
        elif category == "news":
            cats = dbcache.fields["tags"].get_news_category(tag_class, book_ids)
        else:
            cat = fm[category]
            brm = book_rating_map

            if cat["datatype"] == "rating" and category != "rating":
                brm = dbcache.fields[category].book_value_map

            cats = dbcache.fields[category].get_categories(tag_class, brm, lang_map, book_ids)
            if (
                category != "authors"
                and cat["datatype"] == "text"
                and cat["is_multiple"]
                and cat["display"].get("is_names", False)
            ):
                for item in cats:
                    item.sort = author_to_author_sort(item.sort)

        sort_categories(cats, sort)
        categories[category] = cats

    # Needed for legacy databases that have multiple ratings that
    # map to n stars
    for r in categories["rating"]:
        for x in tuple(categories["rating"]):
            if r.name == x.name and r.id != x.id:
                r.id_set |= x.id_set
                r.count = r.count + x.count
                categories["rating"].remove(x)
                break

    # User categories
    user_categories = clean_user_categories(dbcache).copy()
    if user_categories:
        # We want to use same node in the user category as in the source
        # category. To do that, we need to find the original Tag node. There is
        # a time/space tradeoff here. By converting the tags into a map, we can
        # do the verification in the category loop much faster, at the cost of
        # temporarily duplicating the categories lists.
        taglist = {}
        for c, items in iteritems(categories):
            taglist[c] = dict(map(lambda t: (icu_lower(t.name), t), items))

        muc = dbcache.pref("grouped_search_make_user_categories", [])
        gst = dbcache.pref("grouped_search_terms", {})
        for c in gst:
            if c not in muc:
                continue
            user_categories[c] = []
            for sc in gst[c]:
                if sc in categories.keys():
                    for t in categories[sc]:
                        user_categories[c].append([t.name, sc, 0])

        gst_icon = icon_map["gst"] if icon_map else None
        for user_cat in sorted(iterkeys(user_categories), key=sort_key):
            items = []
            names_seen = {}
            for name, label, ign in user_categories[user_cat]:
                n = icu_lower(name)
                if label in taglist and n in taglist[label]:
                    if user_cat in gst:
                        # for gst items, make copy and consolidate the tags by name.
                        if n in names_seen:
                            t = names_seen[n]
                            t.id_set |= taglist[label][n].id_set
                            t.count += taglist[label][n].count
                            t.tooltip = t.tooltip.replace(")", ", " + label + ")")
                        else:
                            t = copy.copy(taglist[label][n])
                            t.icon = gst_icon
                            names_seen[t.name] = t
                            items.append(t)
                    else:
                        items.append(taglist[label][n])
                # else: do nothing, to not include nodes w zero counts
            cat_name = "@" + user_cat  # add the '@' to avoid name collision
            # Not a problem if we accumulate entries in the icon map
            if icon_map is not None:
                icon_map[cat_name] = icon_map["user:"]
            categories[cat_name] = sort_categories(items, sort)

    # ### Finally, the saved searches category ####
    items = []
    icon = None
    if icon_map and "search" in icon_map:
        icon = icon_map["search"]
    queries = dbcache._search_api.saved_searches.queries
    for srch in sorted(queries, key=sort_key):
        items.append(
            Tag(
                srch,
                tooltip=queries[srch],
                sort=srch,
                icon=icon,
                category="search",
                is_editable=False,
            )
        )
    if len(items):
        categories["search"] = items

    return categories
