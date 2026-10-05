"""
Discover cached file-like URLs from wget spider diagnostics under shared scope rules.

The subprocess handles traversal while this layer normalizes, filters, and
counts observed URLs. Accepted diagnostics do not prove object readability or a
complete inventory. Callbacks can publish effects before a later subprocess or
resource-limit failure; this layer provides neither rollback nor a cancellation token.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Sequence

from LiuXin_alpha.ingest.sources.api import (
    DiscoveredUrlCallback,
    DiscoverySourceAPI,
    LogLineCallback,
    ObservedUrlCallback,
)
from LiuXin_alpha.ingest.sources.crawler_defaults import (
    CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT,
    CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY,
    LEGACY_WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY,
    get_default_crawler_http_requests_per_hour,
)
from LiuXin_alpha.ingest.sources.html_common import (
    is_within_root_scope,
    looks_like_file_url,
    normalize_http_url,
)
from LiuXin_alpha.ingest.sources.wget_utils import (
    extract_http_urls_from_wget_output,
    run_wget,
)
from LiuXin_alpha.utils.logging.event_logs.in_memory_list import InMemoryEventLog

WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT = CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT
WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY = CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY


def get_default_wget_http_requests_per_hour() -> float:
    """
    Return the default request-rate limit for wget-backed discovery.

    Prefer the shared modern key, falling back to the wget legacy key only when
    absent. Float conversion and fallback behavior belong to the shared resolver.

    Example:
        >>> rate = get_default_wget_http_requests_per_hour()  # doctest: +SKIP


    :return: Requests-per-hour preference/default, without finite-positive validation here.
    """
    return get_default_crawler_http_requests_per_hour(LEGACY_WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY)


@dataclass
class WgetBackendOptions:
    """
    Retain mutable wget process/traversal controls with limited construction validation.

    Only positive observation/output ceilings are checked; None rate is filled
    from preferences. Command arguments, environment, timeout, depth, and rate
    finiteness are trusted for later interpretation. Mutation does not revalidate.

    Example:
        >>> WgetBackendOptions(max_http_requests_per_hour=0).recurse
        True


    :ivar wget_exe: Executable selector used for version checks and crawling.
    :ivar wget_args: Extra process tokens prepended to generated crawl arguments.
    :ivar env: Optional subprocess environment overlay used by discovery, not startup.
    :ivar timeout_s: Crawl-process timeout seconds, or None without a deadline.
    :ivar max_http_requests_per_hour: Rate used to derive wget wait seconds; nonpositive disables it.
    :ivar recurse: Whether generated arguments request recursive spider traversal.
    :ivar max_depth: Optional recursive level, clamped to at least one when rendered.
    :ivar no_parent: Whether generated arguments and shared result filtering restrict parents.
    :ivar span_hosts: Whether wget/result filtering may include other same-scheme authorities.
    :ivar respect_robots: Whether to retain wget's robots policy rather than explicitly disable it.
    :ivar user_agent: Optional user-agent argument inserted as one process token.
    :ivar no_verbose: Whether generated arguments request reduced wget verbosity.
    :ivar max_observed_urls: Positive cap on extracted URL occurrences considered across callbacks.
    :ivar max_output_chars: Positive retained-output character ceiling passed to run_wget.
    """

    wget_exe: str = "wget"
    wget_args: Sequence[str] = ()
    env: Dict[str, str] | None = None
    timeout_s: float | None = 300.0
    max_http_requests_per_hour: float | None = None
    recurse: bool = True
    max_depth: int | None = None
    no_parent: bool = True
    span_hosts: bool = False
    respect_robots: bool = True
    user_agent: str | None = None
    no_verbose: bool = True
    max_observed_urls: int = 100_000
    max_output_chars: int = 8 * 1024 * 1024

    def __post_init__(self) -> None:
        """
        Fill an unset request rate and reject observation/output ceilings below one.

        No integer-type, timeout, or finite-rate validation is performed.

        Example:
            >>> WgetBackendOptions(max_http_requests_per_hour=0, max_output_chars=0)
            Traceback (most recent call last):
            ...
            ValueError: max_output_chars must be positive.


        :return: None after optional rate assignment and ceiling comparisons.
        :raises ValueError: Either resource ceiling is less than one.
        """
        if self.max_http_requests_per_hour is None:
            self.max_http_requests_per_hour = get_default_crawler_http_requests_per_hour(
                LEGACY_WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY
            )
        if self.max_observed_urls < 1:
            raise ValueError("max_observed_urls must be positive.")
        if self.max_output_chars < 1:
            raise ValueError("max_output_chars must be positive.")


class WgetHtmlDiscoverySource(DiscoverySourceAPI):
    """
    Filter wget spider observations and cache the accepted file-like URL list.

    Construction does not invoke wget. Successful caches have no expiry and are
    not invalidated when caller-owned options change. Instances have no locking
    for concurrent discovery; startup and discovery remain separate operations.

    Example:
        >>> WgetHtmlDiscoverySource('HTTPS://example.test/books/').url
        'https://example.test/books/'
    """

    def __init__(self, url: str, *, options: WgetBackendOptions | None = None) -> None:
        """
        Normalize the root and initialize options, diagnostic storage, and an empty cache.

        Supplied options are retained by reference rather than copied/validated again.

        Example:
            >>> source = WgetHtmlDiscoverySource('https://example.test/')
            >>> source.url
            'https://example.test/'


        :param url: HTTP(S) root accepted by the shared URL policy.
        :param options: Mutable caller settings, or None for preference-backed defaults.
        :return: None without probing an executable or contacting the remote root.
        :raises ValueError: Root normalization rejects the supplied address.
        """
        normalized = normalize_http_url(url)
        if normalized is None:
            raise ValueError("wget discovery requires a valid HTTP(S) root URL.")
        super().__init__(url=normalized)
        self.options = options or WgetBackendOptions()
        self._event_log = InMemoryEventLog()
        self._crawl_cache_urls: list[str] | None = None

    def _normalized_requests_per_hour(self) -> float | None:
        """
        Float-convert the configured rate, disabling waits for absence/error/nonpositive values.

        NaN and positive infinity survive because finiteness is not checked.

        Example:
            >>> source = WgetHtmlDiscoverySource('https://example.test/', options=WgetBackendOptions(max_http_requests_per_hour=0))
            >>> source._normalized_requests_per_hour() is None
            True


        :return: Converted rate, including unchecked nonfinite values, or None.
        """
        value = self.options.max_http_requests_per_hour
        if value is None:
            return None
        try:
            rate = float(value)
        except Exception:
            return None
        if rate <= 0:
            return None
        return rate

    def _build_wget_args(self) -> list[str]:
        """
        Render spider/traversal/rate controls followed by stdout diagnostics and the root URL.

        Recursive depth None becomes infinity; explicit depth is int-converted
        and clamped to one. Rate spacing is 3600/rate rounded to three decimals,
        not a measured hourly quota. Extra caller arguments are inserted by the
        runner, not included in this list.

        Example:
            >>> source = WgetHtmlDiscoverySource('https://example.test/', options=WgetBackendOptions(max_http_requests_per_hour=60))
            >>> '--wait=60.000' in source._build_wget_args()
            True


        :return: New ordered argument list without executable or options.wget_args.
        """
        args: list[str] = []
        if self.options.no_verbose:
            args.append("--no-verbose")
        args.append("--spider")
        if self.options.recurse:
            args.append("--recursive")
            if self.options.max_depth is None:
                args.append("--level=inf")
            else:
                args.append("--level={}".format(max(1, int(self.options.max_depth))))
        if self.options.no_parent:
            args.append("--no-parent")
        if self.options.span_hosts:
            args.append("--span-hosts")
        if not self.options.respect_robots:
            args.append("--execute=robots=off")
        if self.options.user_agent:
            args.append("--user-agent={}".format(self.options.user_agent))

        rate = self._normalized_requests_per_hour()
        if rate is not None:
            args.append("--wait={:.3f}".format(3600.0 / rate))

        args.append("--output-file=-")
        args.append(self.url)
        return args

    def _run_wget(self, args, **kwargs):  # noqa: ANN001 - passthrough for testability
        """
        Delegate subprocess execution through the replaceable module-level runner seam.

        Example:
            >>> result = source._run_wget(['--version'], timeout_s=15)  # doctest: +SKIP


        :param args: Invocation tokens passed as the runner's positional argument.
        :param kwargs: Runner options forwarded unchanged.
        :return: WgetResult from run_wget, with its exceptions propagated.
        """
        return run_wget(args, **kwargs)

    def _is_within_root_scope(self, candidate_url: str) -> bool:
        """
        Apply shared root comparison with current span-hosts/no-parent options.

        Example:
            >>> source = WgetHtmlDiscoverySource('https://example.test/books/')
            >>> source._is_within_root_scope('https://example.test/books/one.epub')
            True


        :param candidate_url: Candidate normalized by the shared comparison helper.
        :return: Whether textual scheme/authority/path scope accepts the URL.
        """
        return is_within_root_scope(
            self.url,
            candidate_url,
            span_hosts=bool(self.options.span_hosts),
            no_parent=bool(self.options.no_parent),
        )

    def startup(self) -> None:
        """
        Run the selected executable's version command with a fixed fifteen-second timeout.

        Ignore configured crawl args, environment overlay, crawl timeout, and
        output ceiling here. This validates execution, not root reachability,
        installed version compatibility, or a successful crawl.

        Example:
            >>> source.startup()  # doctest: +SKIP


        :return: None after checked version execution; runner errors propagate.
        """
        self._run_wget(["--version"], wget_exe=self.options.wget_exe, check=True, timeout_s=15.0)

    def discover_urls(
        self,
        *,
        force: bool = False,
        log_line_callback: LogLineCallback | None = None,
        discovered_url_callback: DiscoveredUrlCallback | None = None,
        observed_url_callback: ObservedUrlCallback | None = None,
    ) -> list[str]:
        """
        Replay the cache or run a checked spider while filtering diagnostic URLs.

        Cached replay returns a copy and emits accepted observations before
        discovered callbacks, without log lines. Fresh discovery processes
        streamed lines, then unseen tokens from retained output for compatible
        non-streaming doubles. Count considered occurrences before seen-set
        rejection; duplicates across lines can exhaust the URL ceiling. Invalid
        tokens rejected by extraction are not counted.

        Acceptance means in-scope and dotted-path-leaf, not ebook validity or a
        successful fetch. Ordinary user callback errors are swallowed; internal
        limit errors propagate through the runner. Abort leaves old cache and
        earlier callback effects intact. Cache only after successful processing
        and return a separate list.

        Example:
            >>> urls = source.discover_urls(force=True)  # doctest: +SKIP


        :param force: Ignore an existing successful cache for this attempt.
        :param log_line_callback: Optional raw diagnostic consumer; text is not secret-scrubbed.
        :param discovered_url_callback: Optional accepted-URL consumer whose effects may precede failure.
        :param observed_url_callback: Optional classification consumer for unique considered URLs.
        :return: Ordered accepted URL list from cache or the completed crawl.
        :raises RuntimeError: Observation/output limits or checked subprocess execution fail.
        """
        if (not force) and (self._crawl_cache_urls is not None):
            cached = list(self._crawl_cache_urls)
            if observed_url_callback is not None:
                for url in cached:
                    try:
                        observed_url_callback({"url": url, "accepted": True, "reason": "accepted"})
                    except Exception:
                        pass
            if discovered_url_callback is not None:
                for url in cached:
                    try:
                        discovered_url_callback(url)
                    except Exception:
                        pass
            return cached

        filtered: list[str] = []
        seen: set[str] = set()
        observed_count = 0

        def _consider_url(url: str) -> None:
            """
            Count an extracted occurrence, then classify and emit each distinct candidate once.

            Seen-set insertion precedes classification. Callback Exceptions are
            ignored; the internal count limit raises before deduplication.

            Example:
                >>> _consider_url('https://example.test/books/one.epub')  # doctest: +SKIP


            :param url: Normalized token supplied by diagnostic extraction.
            :return: None after counter/seen/list updates and applicable callbacks.
            :raises RuntimeError: Considered occurrence count exceeds max_observed_urls.
            """
            nonlocal observed_count
            observed_count += 1
            if observed_count > self.options.max_observed_urls:
                raise RuntimeError(
                    "wget crawl exceeded its configured observed-URL limit"
                )
            if url in seen:
                return
            seen.add(url)
            within_scope = self._is_within_root_scope(url)
            file_like = looks_like_file_url(url)
            accepted = within_scope and file_like
            reason = "accepted"
            if not within_scope:
                reason = "out_of_scope"
            elif not file_like:
                reason = "not_file_like"
            if observed_url_callback is not None:
                try:
                    observed_url_callback(
                        {
                            "url": url,
                            "accepted": accepted,
                            "within_scope": within_scope,
                            "file_like": file_like,
                            "reason": reason,
                        }
                    )
                except Exception:
                    pass
            if not accepted:
                return
            filtered.append(url)
            if discovered_url_callback is not None:
                try:
                    discovered_url_callback(url)
                except Exception:
                    pass

        def _on_wget_line(raw_line: str) -> None:
            """
            Forward one diagnostic line and consider its unique normalized URL tokens.

            Swallow ordinary user log-consumer errors, but let internal extraction
            and observation-limit failures reach the subprocess runner.

            Example:
                >>> _on_wget_line('https://example.test/books/one.epub')  # doctest: +SKIP


            :param raw_line: Diagnostic text delivered by the runner without line endings.
            :return: None after log delivery and per-token consideration.
            """
            if log_line_callback is not None:
                try:
                    log_line_callback(raw_line)
                except Exception:
                    pass
            for candidate in extract_http_urls_from_wget_output(str(raw_line)):
                _consider_url(candidate)

        result = self._run_wget(
            self._build_wget_args(),
            wget_exe=self.options.wget_exe,
            extra_args=self.options.wget_args,
            env=self.options.env,
            timeout_s=self.options.timeout_s,
            check=True,
            line_callback=_on_wget_line,
            max_output_chars=self.options.max_output_chars,
        )

        combined_output = "{}\n{}".format(result.stdout or "", result.stderr or "")
        if len(combined_output) > self.options.max_output_chars + 1:
            raise RuntimeError("wget output exceeded its configured size limit")
        for candidate in extract_http_urls_from_wget_output(combined_output):
            if candidate not in seen:
                _consider_url(candidate)

        self._crawl_cache_urls = filtered
        return list(filtered)


    def file_exists(self, file_url: str) -> bool:
        """
        Test exact text membership in cached/fresh discovery, not remote object readability.

        A missing cache may trigger an entire crawl. The candidate is str-coerced
        but not normalized for membership. Ordinary discovery errors become False.

        Example:
            >>> listed = source.file_exists(candidate_url)  # doctest: +SKIP


        :param file_url: Candidate whose exact text is compared with accepted URL strings.
        :return: Whether the candidate appears in discovery, or False on a caught Exception.
        """
        try:
            return str(file_url) in set(self.discover_urls(force=False))
        except Exception:
            return False


__all__ = [
    "WgetBackendOptions",
    "WgetHtmlDiscoverySource",
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT",
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "get_default_crawler_http_requests_per_hour",
    "get_default_wget_http_requests_per_hour",
]
