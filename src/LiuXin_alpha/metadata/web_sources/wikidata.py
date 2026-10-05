"""
Identify book entities and linked bibliographic metadata through Wikidata search, entities and SPARQL APIs.

The module keeps network, parsing, caching, cancellation and result-order behavior
explicit for callers.

Example:
    Exercise wikidata with the owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py
"""

from __future__ import annotations

import json
import re
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from urllib.parse import parse_qs, quote, urlencode, urlparse

from LiuXin_alpha.metadata.utils import calibreMetaInformation, check_isbn
from LiuXin_alpha.metadata.web_sources.base import Source
from LiuXin_alpha.metadata.web_sources.http_client import RetryPolicy, call_with_backoff, compute_backoff_delay
from LiuXin_alpha.metadata.web_sources.http_client import decode_http_body
from LiuXin_alpha.metadata.web_sources.http_client import log_message
from LiuXin_alpha.metadata.web_sources.http_client import wait_for_backoff
from LiuXin_alpha.utils.date import parse_only_date
from LiuXin_alpha.utils.localization import canonicalize_lang
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__docformat__ = "restructuredtext en"


_QID_RE = re.compile(r"\bQ[1-9]\d*\b", re.I)
_YEAR_RE = re.compile(r"^[+-]?(\d{1,4})(?:-\d{2})?(?:-\d{2})?")
_WIKIDATA_ID_KEYS = ("wikidata", "wikidata_id", "wd", "qid")
_BOOKISH_TYPE_QIDS = frozenset(
    {
        "Q571",  # book
        "Q8261",  # novel
        "Q3331189",  # version, edition, or translation
        "Q47461344",  # written work
        "Q7725634",  # literary work
    }
)
_LANGUAGE_QIDS = {
    "Q1860": "en",
    "Q150": "fr",
    "Q188": "de",
    "Q1321": "es",
    "Q652": "it",
    "Q7737": "ru",
    "Q809": "pl",
    "Q5287": "ja",
    "Q7850": "zh",
    "Q7411": "nl",
    "Q9027": "sv",
    "Q5146": "pt",
    "Q9035": "da",
    "Q1412": "fi",
    "Q25167": "no",
}


def _as_text(raw) -> str:
    """
    Convert optional or hostile input to text without propagating conversion failures.

    Example:
        Exercise  as text with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "replace")
    try:
        return str(raw)
    except Exception:
        return ""


def _first(raw):
    """
    Perform the wikidata first operation with explicit ordering and failure behavior.

    Example:
        Exercise  first with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if raw is None:
        return None
    if isinstance(raw, (str, bytes)):
        return raw
    if isinstance(raw, Mapping):
        for key in raw:
            return raw[key]
        return None
    if isinstance(raw, Iterable):
        for item in raw:
            return item
        return None
    return raw


def _as_list(raw) -> list:
    """
    Perform the wikidata as list operation with explicit ordering and failure behavior.

    Example:
        Exercise  as list with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if raw is None:
        return []
    if isinstance(raw, (str, bytes)):
        return [raw]
    if isinstance(raw, Mapping):
        return [raw]
    if isinstance(raw, Iterable):
        return list(raw)
    return [raw]


def _first_identifier_value(identifiers, key):
    """
    Perform the wikidata first identifier value operation with explicit ordering and failure behavior.

    Example:
        Exercise  first identifier value with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param identifiers: Metadata identifier mapping used for direct lookup and cache
        resolution.
    :param key: Value supplied for key.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if not isinstance(identifiers, Mapping):
        return None
    return _first(identifiers.get(key))


def _safe_isbn(identifiers) -> str | None:
    """
    Return a validated isbn or the documented empty fallback.

    Example:
        Exercise  safe isbn with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param identifiers: Metadata identifier mapping used for direct lookup and cache
        resolution.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    for key in ("isbn", "isbn13", "isbn10"):
        raw = _first_identifier_value(identifiers or {}, key)
        if raw is None:
            continue
        try:
            isbn = check_isbn(_as_text(raw))
        except Exception:
            continue
        if isbn:
            return isbn
    return None


def _normalize_qid(raw) -> str | None:
    """
    Normalize normalize qid into the provider's canonical safe representation.

    Example:
        Exercise  normalize qid with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    text = _as_text(raw).strip()
    if not text:
        return None
    match = _QID_RE.search(text)
    return match.group(0).upper() if match else None


def _wikidata_id_from_identifiers(identifiers) -> str | None:
    """
    Perform the wikidata wikidata id from identifiers operation with explicit ordering and failure behavior.

    Example:
        Exercise  wikidata id from identifiers with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param identifiers: Metadata identifier mapping used for direct lookup and cache
        resolution.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if not isinstance(identifiers, Mapping):
        return None
    for key in _WIKIDATA_ID_KEYS:
        qid = _normalize_qid(_first_identifier_value(identifiers, key))
        if qid:
            return qid
    return None


def _dedupe_text(values) -> list[str]:
    """
    Perform the wikidata dedupe text operation with explicit ordering and failure behavior.

    Example:
        Exercise  dedupe text with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param values: Input values to normalize and deduplicate.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    seen = OrderedDict()
    for raw in _as_list(values):
        text = _as_text(raw).strip()
        if text:
            seen[text] = True
    return list(seen)


def _label_from_entity(entity: Mapping, preferred=("en", "mul")) -> str | None:
    """
    Perform the wikidata label from entity operation with explicit ordering and failure behavior.

    Example:
        Exercise  label from entity with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param entity: Wikidata entity mapping supplying labels, claims and identifiers.
    :param preferred: Preferred language codes used for label selection.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    labels = entity.get("labels")
    if not isinstance(labels, Mapping):
        return None
    for lang in preferred:
        label = labels.get(lang)
        if isinstance(label, Mapping):
            text = _as_text(label.get("value")).strip()
            if text:
                return text
    for label in labels.values():
        if isinstance(label, Mapping):
            text = _as_text(label.get("value")).strip()
            if text:
                return text
    return None


def _description_from_entity(entity: Mapping, preferred=("en", "mul")) -> str | None:
    """
    Perform the wikidata description from entity operation with explicit ordering and failure behavior.

    Example:
        Exercise  description from entity with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param entity: Wikidata entity mapping supplying labels, claims and identifiers.
    :param preferred: Preferred language codes used for label selection.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    descriptions = entity.get("descriptions")
    if not isinstance(descriptions, Mapping):
        return None
    for lang in preferred:
        desc = descriptions.get(lang)
        if isinstance(desc, Mapping):
            text = _as_text(desc.get("value")).strip()
            if text:
                return text
    return None


def _claim_value(claim) -> object | None:
    """
    Perform the wikidata claim value operation with explicit ordering and failure behavior.

    Example:
        Exercise  claim value with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param claim: Wikidata claim object whose snak value is inspected.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if not isinstance(claim, Mapping):
        return None
    snak = claim.get("mainsnak")
    if not isinstance(snak, Mapping):
        return None
    datavalue = snak.get("datavalue")
    if not isinstance(datavalue, Mapping):
        return None
    return datavalue.get("value")


def _claim_values(entity: Mapping, prop: str) -> list:
    """
    Perform the wikidata claim values operation with explicit ordering and failure behavior.

    Example:
        Exercise  claim values with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param entity: Wikidata entity mapping supplying labels, claims and identifiers.
    :param prop: Wikidata claim property identifier.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    claims = entity.get("claims")
    if not isinstance(claims, Mapping):
        return []
    out = []
    for claim in _as_list(claims.get(prop)):
        value = _claim_value(claim)
        if value is not None:
            out.append(value)
    return out


def _entity_id_from_value(value) -> str | None:
    """
    Perform the wikidata entity id from value operation with explicit ordering and failure behavior.

    Example:
        Exercise  entity id from value with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param value: Input value to normalize, compare, store or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if isinstance(value, Mapping):
        qid = _normalize_qid(value.get("id"))
        if qid:
            return qid
        numeric = value.get("numeric-id")
        if isinstance(numeric, int) and numeric > 0:
            return f"Q{numeric}"
    return _normalize_qid(value)


def _entity_ids_from_claim(entity: Mapping, prop: str) -> list[str]:
    """
    Perform the wikidata entity ids from claim operation with explicit ordering and failure behavior.

    Example:
        Exercise  entity ids from claim with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param entity: Wikidata entity mapping supplying labels, claims and identifiers.
    :param prop: Wikidata claim property identifier.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    out = []
    for value in _claim_values(entity, prop):
        qid = _entity_id_from_value(value)
        if qid and qid not in out:
            out.append(qid)
    return out


def _string_values_from_claim(entity: Mapping, prop: str) -> list[str]:
    """
    Perform the wikidata string values from claim operation with explicit ordering and failure behavior.

    Example:
        Exercise  string values from claim with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param entity: Wikidata entity mapping supplying labels, claims and identifiers.
    :param prop: Wikidata claim property identifier.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    out = []
    for value in _claim_values(entity, prop):
        if isinstance(value, Mapping) and "text" in value:
            text = _as_text(value.get("text")).strip()
        else:
            text = _as_text(value).strip()
        if text and text not in out:
            out.append(text)
    return out


def _best_monolingual_text(entity: Mapping, prop: str, preferred=("en", "mul")) -> str | None:
    """
    Perform the wikidata best monolingual text operation with explicit ordering and failure behavior.

    Example:
        Exercise  best monolingual text with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param entity: Wikidata entity mapping supplying labels, claims and identifiers.
    :param prop: Wikidata claim property identifier.
    :param preferred: Preferred language codes used for label selection.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    values = []
    for value in _claim_values(entity, prop):
        if isinstance(value, Mapping):
            text = _as_text(value.get("text")).strip()
            lang = _as_text(value.get("language")).strip()
            if text:
                values.append((lang, text))
        else:
            text = _as_text(value).strip()
            if text:
                values.append(("", text))
    for lang in preferred:
        for value_lang, text in values:
            if value_lang == lang:
                return text
    return values[0][1] if values else None


def _wikidata_time_to_date(value):
    """
    Perform the wikidata wikidata time to date operation with explicit ordering and failure behavior.

    Example:
        Exercise  wikidata time to date with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param value: Input value to normalize, compare, store or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if not isinstance(value, Mapping):
        return None
    raw_time = _as_text(value.get("time")).strip()
    if not raw_time:
        return None
    precision = value.get("precision")
    match = _YEAR_RE.match(raw_time)
    if not match:
        return None
    text = raw_time.lstrip("+")
    if precision == 9:
        text = match.group(1).rjust(4, "0")
    elif precision == 10:
        text = text.split("T", 1)[0].rsplit("-", 1)[0]
    else:
        text = text.split("T", 1)[0]
    try:
        return parse_only_date(text)
    except Exception:
        return None


def _linked_labels(label_map: Mapping[str, str], qids: Iterable[str]) -> list[str]:
    """
    Perform the wikidata linked labels operation with explicit ordering and failure behavior.

    Example:
        Exercise  linked labels with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


    :param label_map: Entity-id to label mapping used for linked values.
    :param qids: Wikidata entity identifiers requested or resolved.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    out = []
    for qid in qids:
        label = _as_text(label_map.get(qid)).strip()
        if label and label not in out:
            out.append(label)
    return out


class Wikidata(Source):
    """
    Implement the wikidata metadata-source integration and its explicit recovery policy.

    Example:
        Exercise Wikidata with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py
    """
    name = "Wikidata"
    version = (1, 0, 0)
    description = _("Downloads conservative metadata enrichment from Wikidata")

    capabilities = frozenset({"identify"})
    touched_fields = frozenset(
        {
            "title",
            "authors",
            "tags",
            "pubdate",
            "comments",
            "publisher",
            "identifier:wikidata",
            "identifier:isbn",
            "identifier:lccn",
            "identifier:oclc",
            "languages",
        }
    )
    supports_gzip_transfer_encoding = True
    cached_cover_url_is_reliable = False

    API = "https://www.wikidata.org/w/api.php"
    ENTITY = "https://www.wikidata.org/wiki/%s"
    SPARQL = "https://query.wikidata.org/sparql"
    SEARCH_RESULT_COUNT = 10
    ENTITY_FETCH_LIMIT = 50

    HTTP_RETRY_ATTEMPTS = 4
    HTTP_RETRY_BASE_SECONDS = 0.5
    HTTP_RETRY_MAX_SECONDS = 6.0

    # URL/query helpers {{{
    def get_book_url(self, identifiers):
        """
        Return canonical provider link tuples for recognized metadata identifiers.

        Example:
            Exercise Wikidata.get book url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        qid = _wikidata_id_from_identifiers(identifiers or {})
        if not qid:
            return None
        return ("wikidata", qid, self.ENTITY % quote(qid, safe=""))

    def id_from_url(self, url):
        """
        Extract a normalized provider identifier from a recognized canonical URL.

        Example:
            Exercise Wikidata.id from url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param url: Provider URL to normalize, request or associate with cached data.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        try:
            parsed = urlparse(_as_text(url))
        except Exception:
            return None
        host = (parsed.netloc or "").split(":", 1)[0].lower()
        if host not in {"wikidata.org", "www.wikidata.org"}:
            return None
        parts = [part for part in parsed.path.split("/") if part]
        for idx, part in enumerate(parts):
            if part in {"wiki", "entity"} and idx + 1 < len(parts):
                qid = _normalize_qid(parts[idx + 1])
                if qid:
                    return ("wikidata", qid)
        query_qid = _normalize_qid(_first(parse_qs(parsed.query).get("id")))
        return ("wikidata", query_qid) if query_qid else None

    def create_query(self, title=None, authors=None, identifiers=None):
        """
        Build create query from normalized identifiers and search inputs.

        Example:
            Exercise Wikidata.create query with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param title: Book title used to construct or rank the provider query.
        :param authors: Author names used to construct or rank the provider query.
        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        identifiers = identifiers or {}
        qid = _wikidata_id_from_identifiers(identifiers)
        if qid:
            return [("entities", self._build_entities_url([qid]))]

        isbn = _safe_isbn(identifiers)
        if isbn:
            return [("sparql", self._build_sparql_url(self._isbn_sparql(isbn)))]

        title_tokens = " ".join(self.get_title_tokens(title, strip_subtitle=True) or [])
        author_tokens = " ".join(self.get_author_tokens(authors, only_first_author=True) or [])
        search = " ".join(x for x in (title_tokens, author_tokens) if x).strip()
        if not search:
            return []
        return [("search", self._build_search_url(search))]

    def _build_search_url(self, search: str, *, limit: int | None = None) -> str:
        """
        Build search url from normalized identifiers and search inputs.

        Example:
            Exercise Wikidata. build search url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param search: Human-readable Wikidata search expression.
        :param limit: Maximum number of unique provider results to retain.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        params = {
            "action": "wbsearchentities",
            "search": search,
            "language": "en",
            "uselang": "en",
            "type": "item",
            "limit": str(int(limit or self.SEARCH_RESULT_COUNT)),
            "format": "json",
            "formatversion": "2",
        }
        return self.API + "?" + urlencode(params)

    def _build_entities_url(self, qids: Iterable[str], *, props: str = "labels|descriptions|claims") -> str:
        """
        Build entities url from normalized identifiers and search inputs.

        Example:
            Exercise Wikidata. build entities url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param qids: Wikidata entity identifiers requested or resolved.
        :param props: Wikidata entity property groups requested from the API.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        ids = []
        for raw in qids:
            qid = _normalize_qid(raw)
            if qid and qid not in ids:
                ids.append(qid)
            if len(ids) >= self.ENTITY_FETCH_LIMIT:
                break
        params = {
            "action": "wbgetentities",
            "ids": "|".join(ids),
            "props": props,
            "languages": "en|mul",
            "languagefallback": "1",
            "format": "json",
            "formatversion": "2",
        }
        return self.API + "?" + urlencode(params)

    def _build_sparql_url(self, sparql: str) -> str:
        """
        Build sparql url from normalized identifiers and search inputs.

        Example:
            Exercise Wikidata. build sparql url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param sparql: SPARQL query encoded for the Wikidata endpoint.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return self.SPARQL + "?" + urlencode({"format": "json", "query": sparql})

    @staticmethod
    def _isbn_sparql(isbn: str) -> str:
        """
        Perform the wikidata isbn sparql operation with explicit ordering and failure behavior.

        Example:
            Exercise Wikidata. isbn sparql with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param isbn: ISBN value used for direct lookup or related-edition resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        escaped = _as_text(isbn).replace("\\", "\\\\").replace('"', '\\"')
        return (
            "SELECT ?item WHERE {\n"
            f'  VALUES ?isbn {{ "{escaped}" }}\n'
            "  { ?item wdt:P212 ?isbn. } UNION { ?item wdt:P957 ?isbn. }\n"
            "}\n"
            "LIMIT 10"
        )

    # }}}

    # Request helpers {{{
    def _retry_policy(self) -> RetryPolicy:
        """
        Build the bounded retry policy used by this provider's HTTP requests.

        Example:
            Exercise Wikidata. retry policy with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return RetryPolicy(
            attempts=int(self.HTTP_RETRY_ATTEMPTS),
            base_delay=float(self.HTTP_RETRY_BASE_SECONDS),
            max_delay=float(self.HTTP_RETRY_MAX_SECONDS),
        )

    def _retry_backoff(self, attempt: int) -> float:
        """
        Compute the capped delay for one provider retry attempt.

        Example:
            Exercise Wikidata. retry backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param attempt: Zero-based retry attempt used to calculate backoff.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return compute_backoff_delay(
            attempt=attempt,
            base_delay=float(self.HTTP_RETRY_BASE_SECONDS),
            max_delay=float(self.HTTP_RETRY_MAX_SECONDS),
        )

    def _wait_for_backoff(self, abort, delay: float) -> bool:
        """
        Wait interruptibly for a retry delay and report whether it completed.

        Example:
            Exercise Wikidata. wait for backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param abort: Event-like cancellation signal checked before and during network work.
        :param delay: Backoff duration in seconds.
        :return: True when the described condition is satisfied; otherwise False.
        """
        return wait_for_backoff(abort, delay)

    def _request_bytes(self, url: str, timeout: int = 30) -> bytes:
        """
        Perform the provider request bytes operation with explicit timeout and response policy.

        Example:
            Exercise Wikidata. request bytes with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param url: Provider URL to normalize, request or associate with cached data.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return self.browser().open_novisit(url, timeout=timeout).read()

    def _request_json(self, url: str, timeout: int = 30):
        """
        Perform the provider request json operation with explicit timeout and response policy.

        Example:
            Exercise Wikidata. request json with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param url: Provider URL to normalize, request or associate with cached data.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        raw = self._request_bytes(url, timeout=timeout)
        return json.loads(decode_http_body(raw))

    def _request_json_with_backoff(self, log, abort, url: str, timeout: int, context: str):
        """
        Run the request json operation with bounded retry, diagnostics and cancellation.

        Example:
            Exercise Wikidata. request json with backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param log: Logger receiving structured provider diagnostics.
        :param abort: Event-like cancellation signal checked before and during network work.
        :param url: Provider URL to normalize, request or associate with cached data.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :param context: Short operation label included in retry diagnostics.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return call_with_backoff(
            lambda: self._request_json(url, timeout=timeout),
            log=log,
            abort=abort,
            context=context,
            policy=self._retry_policy(),
            timeout_seconds=timeout,
            url=url,
            retry_message="Transient Wikidata request error; retrying with backoff",
            error_message="Wikidata request failed",
            abort_result=None,
            backoff_fn=self._retry_backoff,
            wait_for_backoff_fn=self._wait_for_backoff,
        )

    def _request_json_or_none(self, log, abort, url: str, timeout: int, context: str):
        """
        Perform the provider request json or none operation with explicit timeout and response policy.

        Example:
            Exercise Wikidata. request json or none with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param log: Logger receiving structured provider diagnostics.
        :param abort: Event-like cancellation signal checked before and during network work.
        :param url: Provider URL to normalize, request or associate with cached data.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :param context: Short operation label included in retry diagnostics.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        try:
            return self._request_json_with_backoff(
                log=log,
                abort=abort,
                url=url,
                timeout=timeout,
                context=context,
            )
        except Exception as err:
            log_message(
                log,
                "warning",
                "Wikidata request failed; continuing without this source result",
                {"url": url, "error_type": type(err).__name__, "error": str(err)},
            )
            return None

    # }}}

    # Parsing helpers {{{
    @staticmethod
    def _qids_from_search_payload(payload) -> list[str]:
        """
        Perform the wikidata qids from search payload operation with explicit ordering and failure behavior.

        Example:
            Exercise Wikidata. qids from search payload with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param payload: Provider response payload or bytes processed by the operation.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        if not isinstance(payload, Mapping):
            return []
        out = []
        for item in _as_list(payload.get("search")):
            if not isinstance(item, Mapping):
                continue
            qid = _normalize_qid(item.get("id"))
            if qid and qid not in out:
                out.append(qid)
        return out

    @staticmethod
    def _qids_from_sparql_payload(payload) -> list[str]:
        """
        Perform the wikidata qids from sparql payload operation with explicit ordering and failure behavior.

        Example:
            Exercise Wikidata. qids from sparql payload with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param payload: Provider response payload or bytes processed by the operation.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        if not isinstance(payload, Mapping):
            return []
        results = payload.get("results")
        if not isinstance(results, Mapping):
            return []
        out = []
        for binding in _as_list(results.get("bindings")):
            if not isinstance(binding, Mapping):
                continue
            item = binding.get("item")
            if not isinstance(item, Mapping):
                continue
            qid = _normalize_qid(item.get("value"))
            if qid and qid not in out:
                out.append(qid)
        return out

    @staticmethod
    def _entities_from_payload(payload) -> dict[str, Mapping]:
        """
        Perform the wikidata entities from payload operation with explicit ordering and failure behavior.

        Example:
            Exercise Wikidata. entities from payload with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param payload: Provider response payload or bytes processed by the operation.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        if not isinstance(payload, Mapping):
            return {}
        entities = payload.get("entities")
        if not isinstance(entities, Mapping):
            return {}
        out = {}
        for raw_qid, entity in entities.items():
            if not isinstance(entity, Mapping) or entity.get("missing") is True:
                continue
            qid = _normalize_qid(entity.get("id") or raw_qid)
            if qid:
                out[qid] = entity
        return out

    @staticmethod
    def _label_map_from_payload(payload) -> dict[str, str]:
        """
        Perform the wikidata label map from payload operation with explicit ordering and failure behavior.

        Example:
            Exercise Wikidata. label map from payload with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param payload: Provider response payload or bytes processed by the operation.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        entities = Wikidata._entities_from_payload(payload)
        out = {}
        for qid, entity in entities.items():
            label = _label_from_entity(entity)
            if label:
                out[qid] = label
        return out

    @staticmethod
    def _entity_is_bookish(entity: Mapping) -> bool:
        """
        Perform the wikidata entity is bookish operation with explicit ordering and failure behavior.

        Example:
            Exercise Wikidata. entity is bookish with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param entity: Wikidata entity mapping supplying labels, claims and identifiers.
        :return: True when the described condition is satisfied; otherwise False.
        """
        if _string_values_from_claim(entity, "P212") or _string_values_from_claim(entity, "P957"):
            return True
        return any(qid in _BOOKISH_TYPE_QIDS for qid in _entity_ids_from_claim(entity, "P31"))

    @staticmethod
    def _linked_entity_ids_for_labels(entity: Mapping) -> list[str]:
        """
        Perform the wikidata linked entity ids for labels operation with explicit ordering and failure behavior.

        Example:
            Exercise Wikidata. linked entity ids for labels with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param entity: Wikidata entity mapping supplying labels, claims and identifiers.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        ids = []
        for prop in ("P50", "P123", "P136", "P921", "P407"):
            for qid in _entity_ids_from_claim(entity, prop):
                if qid not in ids:
                    ids.append(qid)
        return ids

    @staticmethod
    def _identifier_values_from_entity(entity: Mapping) -> dict[str, list[str]]:
        """
        Perform the wikidata identifier values from entity operation with explicit ordering and failure behavior.

        Example:
            Exercise Wikidata. identifier values from entity with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param entity: Wikidata entity mapping supplying labels, claims and identifiers.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        values: dict[str, list[str]] = {"isbn": [], "lccn": [], "oclc": []}

        def add(key: str, value: str | None) -> None:
            """
            Perform the wikidata add operation with explicit ordering and failure behavior.

            Example:
                Exercise Wikidata. identifier values from entity.add with the owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


            :param key: Value supplied for key.
            :param value: Input value to normalize, compare, store or parse.
            :return: The normalized provider value, metadata result or collection described
                above.
            """
            if not value:
                return
            if value not in values[key]:
                values[key].append(value)

        for prop in ("P212", "P957"):
            for raw in _string_values_from_claim(entity, prop):
                isbn = check_isbn(raw)
                add("isbn", isbn)
        for raw in _string_values_from_claim(entity, "P1144"):
            compact = re.sub(r"\s+", "", raw).strip("/,.;:")
            add("lccn", compact)
        for raw in _string_values_from_claim(entity, "P243"):
            compact = re.sub(r"\D+", "", raw)
            add("oclc", compact)
        return values

    @staticmethod
    def _publication_date_from_entity(entity: Mapping):
        """
        Perform the wikidata publication date from entity operation with explicit ordering and failure behavior.

        Example:
            Exercise Wikidata. publication date from entity with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param entity: Wikidata entity mapping supplying labels, claims and identifiers.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        for value in _claim_values(entity, "P577"):
            parsed = _wikidata_time_to_date(value)
            if parsed is not None:
                return parsed
        return None

    @staticmethod
    def _language_from_entity(entity: Mapping, label_map: Mapping[str, str]) -> str | None:
        """
        Perform the wikidata language from entity operation with explicit ordering and failure behavior.

        Example:
            Exercise Wikidata. language from entity with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param entity: Wikidata entity mapping supplying labels, claims and identifiers.
        :param label_map: Entity-id to label mapping used for linked values.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        for qid in _entity_ids_from_claim(entity, "P407"):
            mapped = _LANGUAGE_QIDS.get(qid)
            if mapped:
                return mapped
            label = label_map.get(qid)
            if label:
                try:
                    lang = canonicalize_lang(label)
                except Exception:
                    lang = None
                if lang and lang != "und":
                    return lang
        return None

    def _metadata_from_entity(self, entity: Mapping, label_map: Mapping[str, str] | None = None, relevance: int = 0):
        """
        Project one provider record into normalized metadata and retain source relevance.

        Example:
            Exercise Wikidata. metadata from entity with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param entity: Wikidata entity mapping supplying labels, claims and identifiers.
        :param label_map: Entity-id to label mapping used for linked values.
        :param relevance: Zero-based provider result relevance.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        label_map = label_map or {}
        qid = _normalize_qid(entity.get("id"))
        title = _best_monolingual_text(entity, "P1476") or _label_from_entity(entity) or _("Unknown")

        authors = _linked_labels(label_map, _entity_ids_from_claim(entity, "P50"))
        authors.extend(name for name in _string_values_from_claim(entity, "P2093") if name not in authors)
        if not authors:
            authors = [_("Unknown")]

        mi = calibreMetaInformation(title, authors)
        mi.source_relevance = relevance
        if qid:
            mi.set_identifier("wikidata", qid)

        ids = self._identifier_values_from_entity(entity)
        if ids["isbn"]:
            unique_isbns = sorted(set(ids["isbn"]), key=lambda value: (len(value), value))
            mi.all_isbns = unique_isbns
            mi.set_identifier("isbn", unique_isbns[-1])
        for key in ("lccn", "oclc"):
            if ids[key]:
                mi.set_identifier(key, ids[key][0])

        desc = _description_from_entity(entity)
        if desc:
            mi.comments = desc

        publishers = _linked_labels(label_map, _entity_ids_from_claim(entity, "P123"))
        if publishers:
            mi.publisher = publishers[0]

        pubdate = self._publication_date_from_entity(entity)
        if pubdate is not None:
            mi.pubdate = pubdate

        language = self._language_from_entity(entity, label_map)
        if language:
            mi.language = language

        tags = _linked_labels(label_map, _entity_ids_from_claim(entity, "P136") + _entity_ids_from_claim(entity, "P921"))
        if tags:
            mi.tags = tags[:20]

        return mi

    def _postprocess_downloaded_metadata(self, mi, relevance: int = 0):
        """
        Apply source relevance, identifier caches and shared cleanup to downloaded metadata.

        Example:
            Exercise Wikidata. postprocess downloaded metadata with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param mi: Metadata object supplying identifiers or receiving normalized fields.
        :param relevance: Zero-based provider result relevance.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        if mi is None:
            return None
        mi.source_relevance = relevance
        identifiers = mi.get_identifiers() or {}
        qid = identifiers.get("wikidata")
        if qid:
            for isbn in getattr(mi, "all_isbns", []) or []:
                self.cache_isbn_to_identifier(isbn, qid)
        self.clean_downloaded_metadata(mi)
        return mi

    # }}}

    # Source API {{{
    def identify(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers=None,
        timeout=30,
    ):
        """
        Run provider lookup, honor cancellation, isolate per-result failures and enqueue normalized metadata.

        Example:
            Exercise Wikidata.identify with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_wikidata.py


        :param log: Logger receiving structured provider diagnostics.
        :param result_queue: Queue receiving normalized metadata or cover results.
        :param abort: Event-like cancellation signal checked before and during network work.
        :param title: Book title used to construct or rank the provider query.
        :param authors: Author names used to construct or rank the provider query.
        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :return: None.
        """
        identifiers = identifiers or {}
        if abort.is_set():
            return

        queries = self.create_query(title=title, authors=authors, identifiers=identifiers)
        if not queries:
            return

        qids = []
        queried_entities = {}
        search_mode = False
        for mode, url in queries:
            if abort.is_set():
                return
            payload = self._request_json_or_none(log, abort, url, timeout, f"Wikidata {mode}")
            if mode == "entities":
                found = self._entities_from_payload(payload)
                queried_entities.update(found)
                qids.extend(found)
            elif mode == "sparql":
                qids.extend(self._qids_from_sparql_payload(payload))
            elif mode == "search":
                search_mode = True
                qids.extend(self._qids_from_search_payload(payload))

        qids = list(OrderedDict((qid, True) for qid in qids))
        if not qids:
            return

        entities = dict(queried_entities)
        missing_qids = [qid for qid in qids if qid not in entities]
        if missing_qids:
            payload = self._request_json_or_none(
                log,
                abort,
                self._build_entities_url(missing_qids),
                timeout,
                "Wikidata entity lookup",
            )
            entities.update(self._entities_from_payload(payload))
        if not entities:
            return

        label_ids = []
        for entity in entities.values():
            for qid in self._linked_entity_ids_for_labels(entity):
                if qid not in label_ids:
                    label_ids.append(qid)
        label_map = {}
        if label_ids:
            label_payload = self._request_json_or_none(
                log,
                abort,
                self._build_entities_url(label_ids, props="labels"),
                timeout,
                "Wikidata label lookup",
            )
            label_map = self._label_map_from_payload(label_payload)

        seen = set()
        relevance = 0
        for qid in qids:
            if abort.is_set():
                return
            entity = entities.get(qid)
            if not entity:
                continue
            if search_mode and not self._entity_is_bookish(entity):
                continue
            try:
                mi = self._metadata_from_entity(entity, label_map=label_map, relevance=relevance)
                mi = self._postprocess_downloaded_metadata(mi, relevance=relevance)
            except Exception:
                log_message(log, "exception", "Failed to parse Wikidata result item")
                continue
            if mi is None:
                continue
            key = (mi.title, tuple(mi.authors), tuple(sorted((mi.get_identifiers() or {}).items())))
            if key in seen:
                continue
            seen.add(key)
            relevance += 1
            result_queue.put(mi)

    # }}}


__all__ = ["Wikidata"]
