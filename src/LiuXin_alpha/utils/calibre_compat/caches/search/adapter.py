"""Compile Calibre metadata expressions into native candidate-set execution.

Input records are explicit book projections keyed by CalibreBookRef. Callers
replace the adapter when their projection changes; no live database fallback or
implicit reinterpretation of a Work ID occurs. Template execution is opt-in for
one trusted local request and is never a property of a stored search.
"""

from __future__ import annotations

import warnings
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import RLock

from LiuXin_alpha.caches.api import UnsupportedCacheQueryError
from LiuXin_alpha.caches.predicates import PredicateEvaluator
from LiuXin_alpha.caches.projection import ProjectionQueryEngine
from LiuXin_alpha.databases.db_types.query_types import BooleanMatch, ProvidedMatch
from LiuXin_alpha.metadata.calibre_conversion import CalibreBookRef
from LiuXin_alpha.utils.calibre_compat.metadata.field_metadata import FieldMetadata
from LiuXin_alpha.utils.text.icu import sort_key

from .formatter_support import template_function_context
from .upstream import BooleanSearch, DateSearch, KeyPairSearch, NumericSearch, Parser
from .upstream_formatter import TemplateFormatter
from .upstream_parser import ParseException
from .upstream_parser import Parser as Grammar
from .upstream_support import search_context


class TemplateExecutionWarning(UserWarning):
    """Template execution can run arbitrary code with the process's privileges."""


class TrustedTemplateExecution:
    """One-use capability acknowledging that templates are not sandboxed."""

    WARNING = (
        "DANGER: Calibre templates and user functions can execute arbitrary code "
        "with your account's access to files, credentials and network. This is "
        "not a sandbox. Enable only code you have reviewed and trust. Permission "
        "applies to this request only, including referenced saved searches."
    )

    def __init__(
        self, *, trusted_local: bool, acknowledge_arbitrary_code_execution: bool
    ):
        if (
            trusted_local is not True
            or acknowledge_arbitrary_code_execution is not True
        ):
            raise ValueError(self.WARNING)
        self._used = False
        self._lock = RLock()

    def consume(self):
        with self._lock:
            if self._used:
                raise PermissionError(
                    "Template permission has already been used; acknowledge each request"
                )
            self._used = True
            with warnings.catch_warnings():
                warnings.simplefilter("always", TemplateExecutionWarning)
                warnings.warn(self.WARNING, TemplateExecutionWarning, stacklevel=3)


class SearchDefinitions:
    """Persist definitions in one namespaced preference, never execution permission."""

    KEY = "calibre_compat.metadata_search.v1"
    KINDS = frozenset(
        {
            "saved_searches",
            "virtual_libraries",
            "grouped_search_terms",
            "user_categories",
        }
    )

    def __init__(self, preferences):
        self.preferences = preferences

    def snapshot(self):
        return deepcopy(self.preferences.get(self.KEY, {}))

    def set(self, kind, name, value):
        if kind not in self.KINDS or not isinstance(name, str) or not name.strip():
            raise ValueError("Unknown definition kind or empty name")
        if kind in {"saved_searches", "virtual_libraries"} and not isinstance(
            value, str
        ):
            raise TypeError("Search definitions contain expression text")
        state = self.snapshot()
        values = state.setdefault(kind, {})
        existing = next(
            (key for key in values if key.casefold() == name.casefold()), name
        )
        values[existing] = deepcopy(value)
        self.preferences[self.KEY] = state

    def delete(self, kind, name):
        if kind not in self.KINDS:
            raise ValueError("Unknown definition kind")
        state = self.snapshot()
        values = state.get(kind, {})
        for key in tuple(values):
            if key.casefold() == name.casefold():
                del values[key]
        self.preferences[self.KEY] = state


class _Metadata(dict):
    def __init__(self, *args, resolver=None, formatter_factory=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._resolver = resolver
        self._formatter_factory = formatter_factory or _Formatter

    def get(self, key, default=None):
        if self._resolver is not None and key != "_descriptors":
            return self._resolver(key, default)
        return super().get(key, default)

    def __getattr__(self, key):
        if key == "formatter":
            return self._formatter_factory()
        return self.get(key)

    def all_field_keys(self):
        return set(self.get("_descriptors", {}))

    def get_user_metadata(self, key, make_copy=False):
        return self.get("_descriptors", {}).get(key)


class _Formatter(TemplateFormatter):
    def __init__(self, functions=None):
        super().__init__()
        self.funcs.update(functions or {})

    def safe_format(self, *args, **kwargs):
        kwargs["template_functions"] = self.funcs
        return super().safe_format(*args, **kwargs)

    def get_value(self, key, args, kwargs):
        value = self.book.get(key, "")
        return (
            ", ".join(map(str, value))
            if isinstance(value, (tuple, list))
            else ""
            if value is None
            else value
        )


class _Fields(dict):
    def __init__(self, custom, definitions):
        original = FieldMetadata()
        super().__init__(
            (name, deepcopy(original[name])) for name in original.all_field_keys()
        )
        self.update(deepcopy(custom))
        self.aliases = {}

        def add_alias(alias, name):
            alias = alias.casefold()
            previous = self.aliases.get(alias)
            if previous is not None and previous != name:
                raise ValueError("Ambiguous search alias: " + alias)
            self.aliases[alias] = name

        for name in self:
            add_alias(name, name)
        for name, field in self.items():
            field.setdefault("datatype", "text")
            field.setdefault("is_multiple", False)
            field.setdefault("display", {})
            field.setdefault("search_terms", [name])
            for alias in field["search_terms"]:
                add_alias(alias, name)
        self.aliases.update(
            {
                "all": "all",
                "search": "search",
                "vl": "vl",
                "template": "template",
                "isbn": "identifiers",
                "date": "timestamp",
            }
        )
        self.aliases.update(definitions.get("grouped_search_terms", {}))
        for name in definitions.get("user_categories", {}):
            self.aliases["@" + name] = "@" + name
        self.aliases = {name.casefold(): value for name, value in self.aliases.items()}

    def all_field_keys(self):
        return set(self)

    def iter_items(self):
        return self.items()

    def search_term_to_field_key(self, term):
        return self.aliases.get(term.casefold(), term)


class _Field:
    def __init__(self, owner, name):
        self.owner, self.name = owner, name
        self.descriptor = deepcopy(owner._field_metadata[name])
        self.is_many = bool(self.descriptor["is_multiple"])

    @property
    def book_value_map(self):
        return {i: self.value(i) for i in self.owner._rows}

    def value(self, book_id):
        # Public field reads cannot borrow another thread's active permission.
        with self.owner._lock:
            return deepcopy(self._value(book_id))

    def _value(self, book_id):
        row = self.owner._rows[book_id]
        if self.descriptor["datatype"] == "composite":
            if not self.owner._template_authorized:
                raise PermissionError(TrustedTemplateExecution.WARNING)
            identity = (book_id, self.name)
            if identity in self.owner._computing:
                raise ParseException("Recursive composite: " + self.name)
            if identity in self.owner._computed:
                return self.owner._computed[identity]
            template = self.descriptor["display"].get("composite_template", "")
            self.owner._computing.add(identity)
            try:
                value = self.owner._formatter().unsafe_format(
                    template, {}, self.owner._get_proxy_metadata(book_id)
                )
                if self.is_many:
                    value = tuple(x.strip() for x in value.split(",") if x.strip())
                self.owner._computed[identity] = value
            finally:
                self.owner._computing.remove(identity)
            return value
        return row.get(self.name)

    def iter_searchable_values(self, get_metadata, candidates):
        # Group shared values once, keeping identifiers as mappings.
        grouped = {}
        for book_id in candidates:
            value = self.value(book_id)
            if self.name == "identifiers":
                if value:
                    yield value, {book_id}
                continue
            values = (
                value
                if self.is_many and isinstance(value, (list, tuple, set, frozenset))
                else (value,)
            )
            for item in values:
                grouped.setdefault(item, set()).add(book_id)
        yield from grouped.items()

    def iter_counts(self, candidates, get_metadata=None):
        for book_id in candidates:
            value = self.value(book_id)
            yield (
                (
                    len(value)
                    if value is not None and self.is_many
                    else int(value is not None)
                ),
                {book_id},
            )

    def iter_searchable_values_for_sort(self, candidates, lang_map):
        for book_id in candidates:
            yield (
                self.owner._rows[book_id].get("series_sort", self.value(book_id)),
                {book_id},
            )


@dataclass(frozen=True)
class _Leaf:
    parser: object
    location: str
    query: str

    def validate(self, table):
        if table != "books":
            raise ValueError(
                "Compatibility predicates require their projected book table"
            )

    def match(self, table, candidates):
        if self.parser.field_metadata.search_term_to_field_key(self.location) == "id":
            return self.parser.num_search(
                self.query,
                lambda: ((self.parser.dbcache._rows[i]["id"], {i}) for i in candidates),
                "id",
                "int",
                set(candidates),
            )
        return self.parser.get_matches(self.location, self.query, set(candidates))


class _TemplateDatabase:
    """Read-only metadata operations exposed inside an authorized template call."""

    def __init__(self, adapter):
        self.adapter = adapter
        self.new_api = self

    @property
    def field_metadata(self):
        return self.adapter.field_metadata

    def search(self, query, restriction=None):
        refs = self.adapter.search(query, restriction=restriction or "")
        return {self.adapter._ids_by_ref[ref] for ref in refs}

    def search_getting_ids(self, query, restriction=None, use_virtual_library=True):
        if use_virtual_library:
            name = self.adapter._settings.get("current_virtual_library")
            if name:
                restriction = self.adapter._definition_state.get(
                    "virtual_libraries", {}
                )[name]
        return self.search(query, restriction)

    def get_proxy_metadata(self, book_id):
        return self.adapter._get_proxy_metadata(book_id)

    def field_for(self, name, book_id, default=None):
        return self.get_proxy_metadata(book_id).get(name, default)

    def __getattr__(self, name):
        raise UnsupportedCacheQueryError(
            "Template operation requires an application provider: " + name
        )


class CalibreSearchAdapter:
    """Search a copied projection; mappings and settings are private to this view.

    Application-dependent values (ondevice, marked, in_tag_browser) must be
    supplied explicitly in records or providers. A provider is a mapping from
    CalibreBookRef to value, sampled when constructing this view.
    """

    def __init__(
        self,
        records,
        *,
        fields=None,
        definitions=None,
        settings=None,
        providers=None,
        plan_cache_size=100,
        result_cache_size=50,
        template_functions=None,
    ):
        if plan_cache_size < 0 or result_cache_size < 0:
            raise ValueError("Cache limits cannot be negative")
        self._lock = RLock()
        self._definitions = definitions
        self._custom = deepcopy(fields or {})
        self._settings = deepcopy(settings or {})
        self._settings.setdefault("bools_are_tristate", True)
        self._template_functions = dict(template_functions or {})
        self._plans, self._results = OrderedDict(), OrderedDict()
        self._computed, self._computing = {}, set()
        self._template_authorized = False
        self._active_queries = set()
        self._template_database = _TemplateDatabase(self)
        self.plan_cache_size, self.result_cache_size = (
            plan_cache_size,
            result_cache_size,
        )
        self._refs = dict(enumerate(records, 1))
        if not all(isinstance(ref, CalibreBookRef) for ref in self._refs.values()):
            raise TypeError("Use explicit CalibreBookRef identities")
        self._ids_by_ref = {ref: i for i, ref in self._refs.items()}
        self._rows = {i: deepcopy(dict(records[ref])) for i, ref in self._refs.items()}
        self._available = set().union(*(set(row) for row in self._rows.values()))
        for name, provider in (providers or {}).items():
            for i, ref in self._refs.items():
                self._rows[i][name] = deepcopy(provider[ref])
            self._available.add(name)
        for i, row in self._rows.items():
            row.setdefault("id", self._refs[i].expression_id)
            row.setdefault("identifiers", {})
            row.setdefault("languages", ())
            for name in ("last_modified", "timestamp", "pubdate"):
                if isinstance(row.get(name), int):
                    row[name] = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
                        milliseconds=row[name]
                    )
        self._available |= {"id", "identifiers", "languages"}
        self._inferred_fields = {}
        self._definition_state = None
        self._refresh_definitions()
        # Definitions change aliases and expressions, never projected values or
        # columns. Retain the same engine snapshot for this adapter's lifetime.
        self._engine = ProjectionQueryEngine(
            "books", self._rows, self._field_metadata, result_cache_size=0
        )

    @property
    def field_metadata(self):
        """Return descriptors without exposing the view's mutable internals."""
        return deepcopy(self._field_metadata)

    @property
    def fields(self):
        return {name: _Field(self, name) for name in self._field_metadata}

    def _formatter(self):
        return _Formatter(self._template_functions)

    @classmethod
    def from_catalog(cls, catalog, references, **kwargs):
        """Take an explicit snapshot using the same projection used for display.

        The caller owns any database transaction needed for atomic capture and
        constructs a new adapter after writes. This never labels live reads as
        belonging to a separately held cache generation.
        """
        records = {ref: catalog.calibre.project(ref) for ref in references}
        fields = dict(kwargs.pop("fields", {}))
        for row in records.values():
            for name, descriptor in row.pop("_search_fields", {}).items():
                if name in fields:
                    previous = {
                        key: value
                        for key, value in fields[name].items()
                        if key != "search_terms"
                    }
                    current = {
                        key: value
                        for key, value in descriptor.items()
                        if key != "search_terms"
                    }
                    if previous != current:
                        raise ValueError("Conflicting projection descriptors: " + name)
                fields[name] = descriptor
        aliases = {}
        for name in fields:
            if name.startswith("#") and "." in name:
                aliases.setdefault("#" + name.rsplit(".", 1)[-1], set()).add(name)
        for alias, names in aliases.items():
            for name in names:
                terms = [
                    term
                    for term in fields[name].get("search_terms", [name])
                    if term != alias
                ]
                fields[name]["search_terms"] = terms + (
                    [alias] if len(names) == 1 else []
                )
        searchable = (
            set(FieldMetadata().all_field_keys()) | set(fields) | {"genre", "subjects"}
        )
        searchable |= {name + "_index" for name in fields}
        # Projection identities and policy diagnostics are display context, not metadata terms.
        records = {
            ref: {name: value for name, value in row.items() if name in searchable}
            for ref, row in records.items()
        }
        return cls(records, fields=fields, **kwargs)

    def _refresh_definitions(self):
        state = (
            self._definitions.snapshot()
            if isinstance(self._definitions, SearchDefinitions)
            else deepcopy(self._definitions or {})
        )
        if state == self._definition_state:
            return
        self._plans.clear()
        self._results.clear()
        metadata = _Fields(self._custom, state)
        for name in self._available - metadata.keys():
            if name not in self._inferred_fields:
                sample = next(
                    (
                        row[name]
                        for row in self._rows.values()
                        if row.get(name) is not None
                    ),
                    None,
                )
                multiple = isinstance(sample, (tuple, list, set, frozenset))
                if multiple:
                    sample = next(iter(sample), None)
                datatype = (
                    "bool"
                    if isinstance(sample, bool)
                    else "int"
                    if isinstance(sample, int)
                    else "float"
                    if isinstance(sample, float)
                    else "datetime"
                    if isinstance(sample, datetime)
                    else "text"
                )
                self._inferred_fields[name] = {
                    "datatype": datatype,
                    "display": {},
                    "is_multiple": multiple,
                    "search_terms": [name],
                }
            metadata[name] = deepcopy(self._inferred_fields[name])
            alias = name.casefold()
            previous = metadata.aliases.get(alias)
            if previous is not None and previous != name:
                raise ValueError("Ambiguous search alias: " + alias)
            metadata.aliases[alias] = name
        # A rejected alias revision must leave the last valid descriptors intact.
        self._field_metadata = metadata
        self._fields = {name: _Field(self, name) for name in self._field_metadata}
        self._definition_state = state

    def _candidate_ids(self, references):
        """Resolve only the supplied identities; None reuses the full frozen scope."""
        if references is None:
            return self._engine.row_ids
        return frozenset(
            row_id
            for ref in references
            if (row_id := self._ids_by_ref.get(ref)) is not None
        )

    def _pref(self, name, default=None):
        return self._definition_state.get(name, self._settings.get(name, default))

    def _get_proxy_metadata(self, book_id):
        def resolve(name, default):
            field = self._field_metadata.search_term_to_field_key(name)
            return (
                self._fields[field].value(book_id)
                if isinstance(field, str) and field in self._fields
                else self._rows[book_id].get(name, default)
            )

        metadata = _Metadata(
            self._rows[book_id],
            resolver=resolve,
            formatter_factory=self._formatter,
            _descriptors=self._field_metadata,
        )
        metadata._search_database = self._template_database
        return metadata

    get_proxy_metadata = _get_proxy_metadata

    @staticmethod
    def _lookup(values, name):
        return next(
            (
                value
                for key, value in values.items()
                if key.casefold() == name.removeprefix("=").casefold()
            ),
            None,
        )

    def _expanded(self, text, seen=()):
        key = text
        if key in self._plans:
            self._plans.move_to_end(key)
            return self._plans[key]
        if not text.strip():
            return ("and",)
        tree = Grammar().parse(text, self._field_metadata.aliases)

        def expand(node):
            if node[0] != "token":
                return (node[0], *(expand(child) for child in node[1:]))
            _, loc, query = node
            if loc in {"search", "vl"}:
                kind = "saved_searches" if loc == "search" else "virtual_libraries"
                identity = (kind, query.removeprefix("=").casefold())
                if identity in seen:
                    raise ParseException("Recursive search definition: " + query)
                definition = self._lookup(self._definition_state.get(kind, {}), query)
                if definition is None:
                    raise ParseException("Unknown search definition: " + query)
                return self._expanded(definition, (*seen, identity))
            return ("token", loc, query)

        result = expand(tree)
        # Recursive expansions are not independently memoized: their ancestry matters.
        if not seen:
            self._engine._remember(self._plans, key, result, self.plan_cache_size)
        return result

    def _inspect(self, tree):
        uses_templates = False
        dynamic = False

        def location(name, seen=()):
            nonlocal uses_templates, dynamic
            resolved = self._field_metadata.search_term_to_field_key(
                name.removeprefix("@")
                if name.startswith("@")
                and name[1:] in self._definition_state.get("grouped_search_terms", {})
                else name
            )
            if isinstance(resolved, list):
                if name in seen:
                    raise ParseException("Recursive grouped search")
                for child in resolved:
                    location(child, (*seen, name))
                return
            if resolved.startswith("@"):
                if name in seen:
                    raise ParseException("Recursive user category")
                for category, values in self._definition_state.get(
                    "user_categories", {}
                ).items():
                    if category == resolved[1:] or category.startswith(
                        resolved[1:] + "."
                    ):
                        for _, field, _ in values:
                            location(field, (*seen, name))
                return
            if resolved == "all":
                for field in self._field_metadata:
                    if (
                        field in self._available
                        or self._field_metadata[field]["datatype"] == "composite"
                    ) and field not in {
                        "ondevice",
                        "marked",
                        "in_tag_browser",
                        "uuid",
                        "series_sort",
                        "id",
                    }:
                        location(field)
                return
            if resolved == "template":
                uses_templates = dynamic = True
                return
            descriptor = self._field_metadata.get(resolved)
            if descriptor is None:
                raise UnsupportedCacheQueryError(
                    "Unknown search field: " + str(resolved)
                )
            if descriptor["datatype"] == "composite":
                uses_templates = dynamic = True
            elif descriptor["datatype"] == "datetime":
                dynamic = True
            if (
                resolved not in self._available
                and descriptor["datatype"] != "composite"
            ):
                # Empty supplied datasets still validate descriptor types.
                if self._rows:
                    raise UnsupportedCacheQueryError(
                        "Projection/provider does not supply field: " + resolved
                    )

        def walk(node):
            if node[0] == "token":
                location(node[1])
            else:
                for child in node[1:]:
                    walk(child)

        walk(tree)
        return uses_templates, dynamic

    def search(
        self,
        query,
        restriction="",
        *,
        references=None,
        template_execution=None,
        remote=False,
        now=None,
    ):
        """Evaluate one request with isolated composite state and one-use permission."""
        with self._lock:
            identity = (query, restriction)
            if identity in self._active_queries:
                raise ParseException("Recursive template search")
            self._active_queries.add(identity)
            state = self._computed, self._computing, self._template_authorized
            self._computed, self._computing = {}, set()
            try:
                return self._search(
                    query,
                    restriction,
                    references=references,
                    template_execution=template_execution,
                    remote=remote,
                    now=now,
                )
            finally:
                self._computed, self._computing, self._template_authorized = state
                self._active_queries.remove(identity)

    def _search(
        self,
        query,
        restriction="",
        *,
        references=None,
        template_execution=None,
        remote=False,
        now=None,
    ):
        with self._lock:
            self._refresh_definitions()
            tree = ("and", self._expanded(restriction or ""), self._expanded(query))
            templates, dynamic = self._inspect(tree)
            if template_execution is not None:
                if remote or not isinstance(
                    template_execution, TrustedTemplateExecution
                ):
                    raise PermissionError(
                        "Remote callers cannot enable template execution"
                    )
                template_execution.consume()
                self._template_authorized = True
            if templates and (remote or not self._template_authorized):
                raise PermissionError(TrustedTemplateExecution.WARNING)
            candidates = self._candidate_ids(references)
            key = (tree, candidates)
            if not dynamic and key in self._results:
                self._results.move_to_end(key)
                return self._results[key]
            ids = set(candidates)
            parser = Parser(
                self,
                ids,
                self._definition_state.get("grouped_search_terms", {}),
                DateSearch(),
                NumericSearch(),
                BooleanSearch(),
                KeyPairSearch(),
                self._settings.get("limit_search_columns", False),
                self._settings.get("limit_search_columns_to", ()),
                self._field_metadata.aliases,
                None,
                lambda name: None,
                {},
                allow_templates=templates,
            )

            def compile_node(node):
                if node[0] == "token":
                    return ProvidedMatch(_Leaf(parser, node[1], node[2]))
                return BooleanMatch(
                    node[0], tuple(compile_node(child) for child in node[1:])
                )

            predicate = compile_node(tree)
            evaluator = PredicateEvaluator(self._engine)
            evaluator.validate("books", predicate)
            with (
                search_context(self._settings, now),
                template_function_context(self._template_functions),
            ):
                result = evaluator.evaluate("books", predicate, ids)
            references = frozenset(self._refs[i] for i in result)
            if not dynamic:
                self._engine._remember(
                    self._results, key, references, self.result_cache_size
                )
            return references

    def multisort(
        self,
        fields,
        references=None,
        *,
        template_execution=None,
        remote=False,
        now=None,
    ):
        """Sort the projection with stable identities and the same template gate.

        ``fields`` contains (field name, ascending) pairs. Missing values sort last
        in either direction. Composite sorting requires one trusted local request.
        """
        with self._lock:
            self._refresh_definitions()
            specs = [
                ({"title": "sort", "authors": "author_sort"}.get(name, name), ascending)
                for name, ascending in fields
            ]
            specs = [
                (self._field_metadata.search_term_to_field_key(name), ascending)
                for name, ascending in specs
            ]
            tree = ("and", *(("token", name, "") for name, _ in specs))
            templates, _ = self._inspect(tree)
            state = self._computed, self._computing, self._template_authorized
            self._computed, self._computing = {}, set()
            try:
                if template_execution is not None:
                    if remote or not isinstance(
                        template_execution, TrustedTemplateExecution
                    ):
                        raise PermissionError(
                            "Remote callers cannot enable template execution"
                        )
                    template_execution.consume()
                    self._template_authorized = True
                if templates and (remote or not self._template_authorized):
                    raise PermissionError(TrustedTemplateExecution.WARNING)
                ids = sorted(
                    self._candidate_ids(references),
                    key=lambda i: (
                        self._refs[i].expression_id,
                        self._refs[i].work_id or 0,
                        self._refs[i].manifestation_id or 0,
                    ),
                )
                with (
                    search_context(self._settings, now),
                    template_function_context(self._template_functions),
                ):
                    for name, ascending in reversed(specs):
                        if name not in self._fields:
                            raise UnsupportedCacheQueryError(
                                "Unknown sort field: " + str(name)
                            )
                        descriptor = self._field_metadata[name]
                        datatype = descriptor["datatype"]
                        if datatype == "composite":
                            datatype = descriptor["display"].get(
                                "composite_sort", "text"
                            )
                        values = {i: self._fields[name].value(i) for i in ids}

                        def key(i, name=name, datatype=datatype, values=values):
                            value = values[i]
                            if datatype in {"int", "float", "number", "rating"}:
                                try:
                                    value = float(value)
                                except (TypeError, ValueError):
                                    value = 0.0
                            elif datatype in {"date", "datetime"}:
                                from LiuXin_alpha.utils.date import parse_date

                                value = (
                                    value
                                    if isinstance(value, datetime)
                                    else parse_date(str(value))
                                )
                                value = value.timestamp()
                            elif isinstance(value, str):
                                value = sort_key(value)
                            elif isinstance(value, (tuple, list, set, frozenset)):
                                value = tuple(sorted(sort_key(str(v)) for v in value))
                            if datatype == "series":
                                index = self._rows[i].get(name + "_index")
                                return value, 1.0 if index is None else index
                            return value

                        present = [i for i in ids if values[i] is not None]
                        missing = [i for i in ids if values[i] is None]
                        ids = sorted(present, key=key, reverse=not ascending) + missing
                return tuple(self._refs[i] for i in ids)
            finally:
                self._computed, self._computing, self._template_authorized = state
