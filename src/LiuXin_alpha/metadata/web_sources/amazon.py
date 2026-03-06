"""
Amazon metadata source.

This is a dependency-light, robust port that supports:
- identify by ASIN/ISBN/title+author
- cover download via cached or discovered cover URL
- conservative HTML parsing with graceful failure paths
"""

from __future__ import annotations

import json
import re
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from datetime import datetime
from html import unescape
from queue import Empty, Queue
from urllib.parse import urlencode, urlparse

from LiuXin_alpha.metadata.utils import calibreMetaInformation, check_isbn
from LiuXin_alpha.metadata.web_sources.base import Option, Source, fixauthors, fixcase
from LiuXin_alpha.metadata.web_sources.http_client import RetryPolicy, call_with_backoff, compute_backoff_delay
from LiuXin_alpha.metadata.web_sources.http_client import decode_http_body
from LiuXin_alpha.metadata.web_sources.http_client import log_message as _shared_log_message
from LiuXin_alpha.metadata.web_sources.http_client import wait_for_backoff
from LiuXin_alpha.utils.localization import canonicalize_lang
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class SearchFailed(ValueError):
    pass


class CaptchaError(SearchFailed):
    pass


def _as_text(raw) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "replace")
    try:
        return str(raw)
    except Exception:
        return ""


def _first(raw):
    if raw is None:
        return None
    if isinstance(raw, (str, bytes)):
        return raw
    if isinstance(raw, Mapping):
        for key in raw:
            return key
        return None
    if isinstance(raw, Iterable):
        for item in raw:
            return item
    return raw


def _first_identifier_value(identifiers, key):
    if not isinstance(identifiers, Mapping):
        return None
    return _first(identifiers.get(key))


def _log(log, level: str, *parts) -> None:
    _shared_log_message(log, level, *parts)


def _strip_tags(raw: str) -> str:
    text = re.sub(r"<\s*br\s*/?\s*>", "\n", _as_text(raw), flags=re.IGNORECASE)
    text = re.sub(r"</(p|li|div|tr|h[1-6])\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n+", "\n", text)
    return text.strip()


def _extract_json_ld_objects(raw_html: str):
    out = []
    for block in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        raw_html,
        re.IGNORECASE | re.DOTALL,
    ):
        payload = unescape(block).strip()
        if not payload:
            continue
        try:
            data = json.loads(payload)
        except Exception:
            continue
        if isinstance(data, list):
            out.extend(data)
        else:
            out.append(data)
    return out


def _parse_pubdate(raw: str) -> datetime | None:
    text = _as_text(raw).strip()
    if not text:
        return None
    text = re.sub(r"^[^(]*\(", "", text)
    text = text.rstrip(")")
    text = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", text, flags=re.IGNORECASE)
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            pass
    m = re.search(r"\b(19|20)\d{2}\b", text)
    if m:
        try:
            return datetime(int(m.group(0)), 6, 15)
        except Exception:
            return None
    return None


_LANG_NAME_TO_CODE = {
    "english": "eng",
    "french": "fra",
    "italian": "ita",
    "german": "deu",
    "spanish": "spa",
    "japanese": "jpn",
    "portuguese": "por",
    "dutch": "nld",
    "chinese": "zho",
    "swedish": "swe",
}

_AMAZON_DOMAIN_CHOICES = OrderedDict(
    (
        ("com", _("US")),
        ("uk", _("UK")),
        ("de", _("Germany")),
        ("fr", _("France")),
        ("it", _("Italy")),
        ("es", _("Spain")),
        ("br", _("Brazil")),
        ("jp", _("Japan")),
    )
)


def _canonicalize_language(raw: str | None) -> str | None:
    if not raw:
        return None
    text = _as_text(raw).strip()
    if not text:
        return None
    mapped = _LANG_NAME_TO_CODE.get(text.lower(), text)
    lang = canonicalize_lang(mapped)
    if lang and lang != "und":
        return lang
    return None


def _safe_isbn_from_identifiers(identifiers) -> str | None:
    for key in ("isbn", "isbn13", "isbn10"):
        raw = _first_identifier_value(identifiers, key)
        if raw is None:
            continue
        try:
            isbn = check_isbn(_as_text(raw))
        except Exception:
            continue
        if isbn:
            return isbn
    return None


class Amazon(Source):
    name = "Amazon.com"
    version = (1, 0, 0)
    description = _("Downloads metadata and covers from Amazon")

    capabilities = frozenset({"identify", "cover"})
    touched_fields = frozenset(
        {
            "title",
            "authors",
            "identifier:amazon",
            "rating",
            "comments",
            "publisher",
            "pubdate",
            "languages",
            "series",
        }
    )
    has_html_comments = True
    supports_gzip_transfer_encoding = True
    prefer_results_with_isbn = False

    AMAZON_DOMAINS = {
        "com": _("US"),
        "fr": _("France"),
        "de": _("Germany"),
        "uk": _("UK"),
        "it": _("Italy"),
        "jp": _("Japan"),
        "es": _("Spain"),
        "br": _("Brazil"),
    }

    options = (
        Option(
            "domain",
            "choices",
            "com",
            _("Amazon website to use:"),
            _("Metadata from Amazon will be fetched using this country's Amazon website."),
            choices=_AMAZON_DOMAIN_CHOICES,
        ),
    )

    HTTP_RETRY_ATTEMPTS = 4
    HTTP_RETRY_BASE_SECONDS = 0.5
    HTTP_RETRY_MAX_SECONDS = 6.0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._set_amazon_id_touched_fields()

    def _preferred_domain(self) -> str:
        candidate = _as_text(self.prefs.get("domain", "com")).lower().strip()
        if candidate not in self.AMAZON_DOMAINS:
            return "com"
        return candidate

    def _amazon_identifier_key(self, domain: str) -> str:
        return "amazon" if domain == "com" else f"amazon_{domain}"

    def _set_amazon_id_touched_fields(self) -> None:
        domain = self._preferred_domain()
        id_name = "identifier:" + self._amazon_identifier_key(domain)
        base = [x for x in self.touched_fields if not x.startswith("identifier:amazon")]
        self.touched_fields = frozenset(base + [id_name])

    def save_settings(self, config_widget):
        super().save_settings(config_widget)
        self._set_amazon_id_touched_fields()

    def _website_domain(self, domain: str) -> str:
        return {"uk": "co.uk", "jp": "co.jp", "br": "com.br"}.get(domain, domain)

    def _host_for_domain(self, domain: str) -> str:
        return f"www.amazon.{self._website_domain(domain)}"

    def _detail_url(self, domain: str, asin: str) -> str:
        host = self._host_for_domain(domain)
        return f"https://{host}/dp/{asin}"

    def get_domain_and_asin(self, identifiers):
        if not isinstance(identifiers, Mapping):
            return None, None
        for key, raw_val in identifiers.items():
            norm_key = _as_text(key).strip().lower()
            asin = _as_text(_first(raw_val)).strip()
            if not asin:
                continue
            if norm_key in {"amazon", "asin"}:
                return "com", asin
            if norm_key.startswith("amazon_"):
                domain = norm_key.split("_", 1)[-1]
                if domain in self.AMAZON_DOMAINS:
                    return domain, asin
        return None, None

    def get_book_url(self, identifiers):
        domain, asin = self.get_domain_and_asin(identifiers or {})
        if domain and asin:
            idtype = self._amazon_identifier_key(domain)
            return idtype, asin, self._detail_url(domain, asin)
        return None

    def get_book_url_name(self, idtype, idval, url):
        del idval, url
        if idtype == "amazon":
            return self.name
        return "A" + _as_text(idtype).replace("_", ".")[1:]

    def id_from_url(self, url):
        try:
            parsed = urlparse(_as_text(url))
        except Exception:
            return None
        host = (parsed.netloc or "").split(":", 1)[0].lower()
        if "amazon." not in host:
            return None
        m = re.search(r"/(?:dp|gp/product)/([A-Z0-9]{10})\b", parsed.path or "", re.IGNORECASE)
        if not m:
            return None
        asin = m.group(1).upper()
        domain = "com"
        if host.endswith("co.uk"):
            domain = "uk"
        elif host.endswith("co.jp"):
            domain = "jp"
        elif host.endswith("com.br"):
            domain = "br"
        else:
            last = host.rsplit(".", 1)[-1]
            if last in self.AMAZON_DOMAINS:
                domain = last
        return self._amazon_identifier_key(domain), asin

    def clean_downloaded_metadata(self, mi):
        do_case = getattr(mi, "language", None) == "eng" or (
            getattr(mi, "is_null", lambda x: False)("language") and self._preferred_domain() in {"com", "uk"}
        )
        if getattr(mi, "title", None) and do_case:
            mi.title = fixcase(mi.title)
        mi.authors = fixauthors(getattr(mi, "authors", None))
        if getattr(mi, "tags", None) and do_case:
            mi.tags = [fixcase(x) for x in mi.tags]
        isbn = _safe_isbn_from_identifiers(mi.get_identifiers() or {})
        if isbn:
            mi.set_identifier("isbn", isbn)

    def create_query(self, title=None, authors=None, identifiers=None, domain=None):
        identifiers = identifiers or {}
        domain = _as_text(domain or self._preferred_domain()).lower()
        if domain not in self.AMAZON_DOMAINS:
            domain = "com"
        idomain, asin = self.get_domain_and_asin(identifiers)
        if idomain:
            domain = idomain
        isbn = _safe_isbn_from_identifiers(identifiers)

        q = {"search-alias": "aps", "unfiltered": "1"}
        q["sort"] = "relevanceexprank" if domain == "com" else "relevancerank"

        if asin:
            q["field-keywords"] = asin
        elif isbn:
            q["field-isbn"] = isbn
        else:
            q["search-alias"] = "digital-text" if domain == "br" else "stripbooks"
            if title:
                tokens = list(self.get_title_tokens(title))
                if tokens:
                    q["field-title"] = " ".join(tokens)
            if authors:
                tokens = list(self.get_author_tokens(authors, only_first_author=True))
                if tokens:
                    q["field-author"] = " ".join(tokens)

        if not any(key in q for key in ("field-keywords", "field-isbn", "field-title")):
            return None, None

        if domain == "jp":
            q["__mk_ja_JP"] = "カタカナ"
        host = self._host_for_domain(domain)
        return f"https://{host}/s/?{urlencode(q)}", domain

    def parse_results_page(self, raw_html, result_count=5):
        html = _as_text(raw_html)
        matches = []
        seen = set()

        for asin in re.findall(r'data-asin=["\']([A-Z0-9]{10})["\']', html, re.IGNORECASE):
            asin = asin.upper()
            if asin in seen:
                continue
            seen.add(asin)
            matches.append(asin)
            if len(matches) >= result_count:
                return matches

        for asin in re.findall(r"/(?:dp|gp/product)/([A-Z0-9]{10})\b", html, re.IGNORECASE):
            asin = asin.upper()
            if asin in seen:
                continue
            seen.add(asin)
            matches.append(asin)
            if len(matches) >= result_count:
                return matches
        return matches

    def _retry_policy(self) -> RetryPolicy:
        return RetryPolicy(
            attempts=int(self.HTTP_RETRY_ATTEMPTS),
            base_delay=float(self.HTTP_RETRY_BASE_SECONDS),
            max_delay=float(self.HTTP_RETRY_MAX_SECONDS),
        )

    def _retry_backoff(self, attempt: int) -> float:
        return compute_backoff_delay(
            attempt=attempt,
            base_delay=float(self.HTTP_RETRY_BASE_SECONDS),
            max_delay=float(self.HTTP_RETRY_MAX_SECONDS),
        )

    def _wait_for_backoff(self, abort, delay: float) -> bool:
        return wait_for_backoff(abort, delay)

    def _open_bytes_with_backoff(self, log, abort, url: str, timeout: int, context: str):
        return call_with_backoff(
            lambda: self.browser().open_novisit(url, timeout=timeout).read(),
            log=log,
            abort=abort,
            context=context,
            policy=self._retry_policy(),
            timeout_seconds=timeout,
            url=url,
            retry_message="Transient Amazon request error; retrying with backoff",
            error_message="Amazon request failed",
            abort_result=b"",
            backoff_fn=self._retry_backoff,
            wait_for_backoff_fn=self._wait_for_backoff,
        )

    def _open_text_with_backoff(self, log, abort, url: str, timeout: int, context: str):
        raw = self._open_bytes_with_backoff(log=log, abort=abort, url=url, timeout=timeout, context=context)
        if not raw:
            return ""
        return decode_http_body(raw)

    def _parse_detail_rows(self, raw_html: str):
        lines = _strip_tags(raw_html).splitlines()
        rows = {}
        for line in lines:
            m = re.match(r"([^:：]{2,40})\s*[:：]\s*(.+)$", line.strip())
            if not m:
                continue
            key = m.group(1).strip().lower()
            value = m.group(2).strip()
            if key and value and key not in rows:
                rows[key] = value
        return rows

    def _parse_cover_url(self, raw_html: str) -> str | None:
        patterns = (
            r'id=["\']landingImage["\'][^>]*data-old-hires=["\']([^"\']+)["\']',
            r'id=["\']landingImage["\'][^>]*src=["\']([^"\']+)["\']',
            r'property=["\']og:image["\'][^>]*content=["\']([^"\']+)["\']',
            r'id=["\']imgBlkFront["\'][^>]*src=["\']([^"\']+)["\']',
        )
        for pat in patterns:
            m = re.search(pat, raw_html, re.IGNORECASE)
            if m:
                return unescape(m.group(1).strip())
        return None

    def _parse_title(self, raw_html: str) -> str | None:
        patterns = (
            r'<span[^>]+id=["\']productTitle["\'][^>]*>(.*?)</span>',
            r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\'](.*?)["\']',
            r"<title>(.*?)</title>",
        )
        for pat in patterns:
            m = re.search(pat, raw_html, re.IGNORECASE | re.DOTALL)
            if m:
                title = _strip_tags(m.group(1))
                title = re.sub(r"\s*:\s*Amazon\..*$", "", title, flags=re.IGNORECASE)
                if title:
                    return title
        return None

    def _parse_authors(self, raw_html: str):
        authors = []
        for obj in _extract_json_ld_objects(raw_html):
            if not isinstance(obj, Mapping):
                continue
            author = obj.get("author")
            if isinstance(author, Mapping):
                name = _as_text(author.get("name", "")).strip()
                if name:
                    authors.append(name)
            elif isinstance(author, list):
                for item in author:
                    if isinstance(item, Mapping):
                        name = _as_text(item.get("name", "")).strip()
                    else:
                        name = _as_text(item).strip()
                    if name:
                        authors.append(name)

        if authors:
            return list(dict.fromkeys(authors))

        byline = re.search(r'<span[^>]+id=["\']bylineInfo["\'][^>]*>(.*?)</span>', raw_html, re.IGNORECASE | re.DOTALL)
        if byline:
            for val in re.findall(r"<a[^>]*>(.*?)</a>", byline.group(1), re.IGNORECASE | re.DOTALL):
                author = _strip_tags(val)
                author = re.sub(r"^\s*by\s+", "", author, flags=re.IGNORECASE)
                if author and author.lower() not in {"visit amazon's", "search results"}:
                    authors.append(author)
        if authors:
            return list(dict.fromkeys(authors))

        m = re.search(r'<meta[^>]+name=["\']author["\'][^>]+content=["\'](.*?)["\']', raw_html, re.IGNORECASE)
        if m:
            author = _strip_tags(m.group(1))
            if author:
                return [author]
        return []

    def _parse_comments(self, raw_html: str):
        m = re.search(
            r'<div[^>]+id=["\']bookDescription_feature_div["\'][^>]*>(.*?)</div>',
            raw_html,
            re.IGNORECASE | re.DOTALL,
        )
        if m:
            raw = m.group(1).strip()
            if raw:
                return raw
        for obj in _extract_json_ld_objects(raw_html):
            if not isinstance(obj, Mapping):
                continue
            desc = obj.get("description")
            if desc:
                return "<p>" + _strip_tags(_as_text(desc)) + "</p>"
        return None

    def _parse_rating(self, raw_html: str) -> float | None:
        patterns = (
            r"([0-9]+(?:[.,][0-9]+)?)\s*(?:out of|von|de|sur|av|つ星のうち)\s*5",
            r"([0-9]+(?:[.,][0-9]+)?)\s*颗星，最多\s*5",
        )
        for pat in patterns:
            m = re.search(pat, raw_html, re.IGNORECASE)
            if not m:
                continue
            try:
                stars = float(m.group(1).replace(",", "."))
            except Exception:
                continue
            return max(0.0, min(10.0, stars * 2.0))
        return None

    def _parse_metadata_from_details(self, raw_html: str, domain: str, asin: str, relevance: int):
        if "validatecaptcha" in raw_html.lower() or "Type the characters you see in this image" in raw_html:
            raise CaptchaError("Amazon returned a CAPTCHA page")

        title = self._parse_title(raw_html) or _("Unknown")
        authors = self._parse_authors(raw_html) or [_("Unknown")]
        mi = calibreMetaInformation(title, authors)
        mi.source_relevance = relevance
        mi.set_identifier(self._amazon_identifier_key(domain), asin)

        rows = self._parse_detail_rows(raw_html)
        publisher = rows.get("publisher") or rows.get("verlag") or rows.get("editeur") or rows.get("editorial")
        pubdate = rows.get("publication date") or rows.get("date de publication") or rows.get("erscheinungstermin")
        language = rows.get("language") or rows.get("sprache") or rows.get("langue") or rows.get("idioma")
        isbn13 = rows.get("isbn-13")
        isbn10 = rows.get("isbn-10")

        if publisher:
            pm = re.match(r"(.*?)\s*\(([^)]+)\)\s*$", publisher)
            if pm:
                mi.publisher = pm.group(1).strip()
                if not pubdate:
                    pubdate = pm.group(2).strip()
            else:
                mi.publisher = publisher

        if pubdate:
            dt = _parse_pubdate(pubdate)
            if dt is not None:
                mi.pubdate = dt

        lang = _canonicalize_language(language)
        if lang:
            mi.language = lang

        for candidate in (isbn13, isbn10):
            checked = check_isbn(_as_text(candidate or ""))
            if checked:
                mi.set_identifier("isbn", checked)
                self.cache_isbn_to_identifier(checked, asin)
                break

        comments = self._parse_comments(raw_html)
        if comments:
            mi.comments = comments

        rating = self._parse_rating(raw_html)
        if rating is not None:
            mi.rating = rating

        cover_url = self._parse_cover_url(raw_html)
        if cover_url:
            self.cache_identifier_to_cover_url(asin, cover_url)

        self.clean_downloaded_metadata(mi)
        return mi

    def _search_asins(self, log, abort, query_url: str, timeout: int, result_count: int = 5):
        html = self._open_text_with_backoff(
            log=log,
            abort=abort,
            url=query_url,
            timeout=timeout,
            context="Amazon search",
        )
        if not html:
            return []
        return self.parse_results_page(html, result_count=result_count)

    def get_cached_cover_url(self, identifiers):
        domain, asin = self.get_domain_and_asin(identifiers or {})
        if asin is None:
            isbn = _safe_isbn_from_identifiers(identifiers or {})
            if isbn is not None:
                asin = self.cached_isbn_to_identifier(isbn)
        if not asin:
            return None
        return self.cached_identifier_to_cover_url(_as_text(asin))

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
        identifiers = identifiers or {}
        if abort.is_set():
            return

        domain, asin = self.get_domain_and_asin(identifiers)
        if domain is None:
            domain = self._preferred_domain()

        asins = []
        if asin:
            asins = [_as_text(asin).upper()]
        else:
            query_url, qdomain = self.create_query(title=title, authors=authors, identifiers=identifiers, domain=domain)
            if not query_url:
                return
            domain = qdomain or domain
            asins = self._search_asins(log=log, abort=abort, query_url=query_url, timeout=timeout, result_count=5)
            if not asins and _safe_isbn_from_identifiers(identifiers) and (title or authors):
                retry_url, _ = self.create_query(title=title, authors=authors, identifiers={}, domain=domain)
                if retry_url:
                    _log(log, "info", "Amazon ISBN search yielded no results, retrying with title/author query")
                    asins = self._search_asins(log=log, abort=abort, query_url=retry_url, timeout=timeout, result_count=5)

        for relevance, found_asin in enumerate(asins):
            if abort.is_set():
                break
            detail_url = self._detail_url(domain, found_asin)
            try:
                html = self._open_text_with_backoff(
                    log=log,
                    abort=abort,
                    url=detail_url,
                    timeout=timeout,
                    context="Amazon detail page",
                )
                if not html:
                    continue
                mi = self._parse_metadata_from_details(
                    raw_html=html,
                    domain=domain,
                    asin=found_asin,
                    relevance=relevance,
                )
            except CaptchaError:
                _log(log, "error", "Amazon identify aborted by CAPTCHA response", {"url": detail_url})
                continue
            except Exception:
                _log(log, "exception", "Failed to parse Amazon detail page", {"url": detail_url, "asin": found_asin})
                continue
            if mi is not None:
                result_queue.put(mi)

    def download_cover(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers=None,
        timeout=30,
        get_best_cover=False,
    ):
        del get_best_cover
        identifiers = identifiers or {}
        cached_url = self.get_cached_cover_url(identifiers)
        if cached_url is None:
            _log(log, "info", "No cached Amazon cover found, running identify")
            rq = Queue()
            self.identify(log, rq, abort, title=title, authors=authors, identifiers=identifiers, timeout=timeout)
            if abort.is_set():
                return
            results = []
            while True:
                try:
                    results.append(rq.get_nowait())
                except Empty:
                    break
            results.sort(key=self.identify_results_keygen(title=title, authors=authors, identifiers=identifiers))
            for mi in results:
                cached_url = self.get_cached_cover_url(mi.get_identifiers())
                if cached_url:
                    break
        if not cached_url:
            _log(log, "info", "No Amazon cover found")
            return
        try:
            payload = self._open_bytes_with_backoff(
                log=log,
                abort=abort,
                url=cached_url,
                timeout=timeout,
                context="Amazon cover download",
            )
        except Exception:
            return
        if payload:
            result_queue.put((self, payload))


__all__ = [
    "Amazon",
    "CaptchaError",
    "SearchFailed",
]
