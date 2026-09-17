"""
Resolve PostgreSQL URLs, service profiles, schemas and process-local credentials.

Configuration is read at call time. URL recognition checks the scheme rather than
connection validity; service names use a conservative character allowlist. Redaction
helpers mask known URL fields and are not general secret scanners.
"""

from __future__ import annotations

import getpass
import os
import re
import shlex
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit


POSTGRES_URL_ENV = "LIUXIN_POSTGRES_URL"
DATABASE_URL_ENV = "LIUXIN_DATABASE_URL"
POSTGRES_PASSWORD_ENV = "LIUXIN_POSTGRES_PASSWORD"
POSTGRES_SCHEMA_ENV = "LIUXIN_POSTGRES_SCHEMA"
POSTGRES_SERVICE_ENV = "LIUXIN_POSTGRES_SERVICE"
PGSERVICE_ENV = "PGSERVICE"
DEFAULT_POSTGRES_SCHEMA = "public"

POSTGRES_SCHEMES = {"postgres", "postgresql"}
SECRET_QUERY_KEYS = ("pass", "password", "token", "secret", "key")
METADATA_URL_KEYS = ("postgres_url", "database_url", "dsn", "url", "database_path")
METADATA_SERVICE_KEYS = ("postgres_service", "database_service", "service")
POSTGRES_TARGET_KINDS = Literal["", "url", "service"]
SERVICE_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


class PostgresConfigError(RuntimeError):
    """
    Report absent or invalid PostgreSQL configuration or an unavailable password prompt.

    Example:
        Catch ``PostgresConfigError`` when interactive credentials are required in
        a process without a terminal.
    """


@dataclass(frozen=True)
class PostgresConnectionTarget:
    """
    Immutable target kind and value resolved from PostgreSQL settings.

    ``kind`` is url, service or the empty string; ``value`` holds the corresponding
    URL/profile. Construction itself performs no validation or redaction.

    Example:
        >>> PostgresConnectionTarget("service", "library").label
        'service=library'
    """

    kind: POSTGRES_TARGET_KINDS
    value: str

    @property
    def configured(self) -> bool:
        """
        Check that both target fields are nonempty without validating their contents.

        Example:
            >>> PostgresConnectionTarget("", "").configured
            False


        :return: True when both kind and value are truthy.
        """
        return bool(self.kind and self.value)

    @property
    def label(self) -> str:
        """
        Return a service= label or the unchanged target value.

        URL labels may contain credentials; use redact_postgres_target for status output.

        Example:
            >>> PostgresConnectionTarget("service", "library").label
            'service=library'


        :return: Service label or raw URL/value.
        """
        if self.kind == "service":
            return f"service={self.value}"
        return self.value


def is_postgres_url(value: object) -> bool:
    """
    Recognize postgres or postgresql schemes after trimming whitespace.

    Malformed URL parsing returns False; a recognized scheme does not prove that host,
    database or credentials are usable.

    Example:
        >>> is_postgres_url("postgresql:///library")
        True


    :param value: Value converted to text before inspection.
    :return: Whether parsing finds a supported scheme.
    """

    text = str(value or "").strip()
    if not text:
        return False
    try:
        return urlsplit(text).scheme.casefold() in POSTGRES_SCHEMES
    except ValueError:
        return False


def is_postgres_service_name(value: object) -> bool:
    """
    Validate a trimmed service name after removing an optional service= prefix.

    Example:
        >>> is_postgres_service_name("service=library_read")
        True


    :param value: Value converted to text before inspection.
    :return: True for nonempty names containing only letters, digits, underscore, dot or hyphen.
    """

    text = _normalise_service_name(value)
    return bool(text and SERVICE_NAME_RE.fullmatch(text))


def configured_postgres_url(
    metadata: Mapping[str, object] | None = None,
    explicit: str | None = None,
) -> str:
    """
    Return the first recognized URL from explicit, metadata then environment candidates.

    Metadata uses METADATA_URL_KEYS order; environment uses LIUXIN_POSTGRES_URL before
    LIUXIN_DATABASE_URL. Invalid candidates are skipped rather than rejected.

    Example:
        >>> configured_postgres_url(explicit="postgresql:///library")
        'postgresql:///library'


    :param metadata: Optional database metadata mapping; recognized keys are checked in documented precedence order.
    :param explicit: Explicit candidate considered before metadata and environment values.
    :return: First valid-scheme URL with surrounding whitespace removed, or an empty string.
    """

    candidates: list[object] = []
    if explicit not in (None, ""):
        candidates.append(explicit)
    if metadata:
        candidates.extend(metadata.get(key) for key in METADATA_URL_KEYS)
    candidates.extend((os.environ.get(POSTGRES_URL_ENV), os.environ.get(DATABASE_URL_ENV)))

    for candidate in candidates:
        text = str(candidate or "").strip()
        if is_postgres_url(text):
            return text
    return ""


def configured_postgres_service(
    metadata: Mapping[str, object] | None = None,
    explicit: str | None = None,
) -> str:
    """
    Return the first valid service profile from explicit, metadata then environment.

    Metadata uses METADATA_SERVICE_KEYS order; LIUXIN_POSTGRES_SERVICE precedes PGSERVICE.
    Invalid candidates are skipped and service= prefixes are removed.

    Example:
        >>> configured_postgres_service(explicit="service=library")
        'library'


    :param metadata: Optional database metadata mapping; recognized keys are checked in documented precedence order.
    :param explicit: Explicit candidate considered before metadata and environment values.
    :return: Normalized profile name, or an empty string.
    """

    candidates: list[object] = []
    if explicit not in (None, ""):
        candidates.append(explicit)
    if metadata:
        candidates.extend(metadata.get(key) for key in METADATA_SERVICE_KEYS)
    candidates.extend((os.environ.get(POSTGRES_SERVICE_ENV), os.environ.get(PGSERVICE_ENV)))

    for candidate in candidates:
        text = _normalise_service_name(candidate)
        if is_postgres_service_name(text):
            return text
    return ""


def configured_postgres_target(
    metadata: Mapping[str, object] | None = None,
    *,
    explicit_url: str | None = None,
    explicit_service: str | None = None,
) -> PostgresConnectionTarget:
    """
    Choose a URL or service from explicit inputs, metadata and environment.

    Explicit URL resolution runs first, then explicit service, metadata URL/service and
    environment URL/service. Each explicit resolver can itself fall back to environment
    settings when its explicit candidate is invalid; this can outrank later candidates.

    Example:
        >>> configured_postgres_target(explicit_url="postgresql:///library").kind
        'url'


    :param metadata: Optional database metadata mapping; recognized keys are checked in documented precedence order.
    :param explicit_url: URL candidate resolved before the explicit service.
    :param explicit_service: Service candidate resolved if explicit URL resolution yielded nothing.
    :return: Resolved target, or an empty-kind/empty-value target.
    """

    if explicit_url not in (None, ""):
        url = configured_postgres_url(metadata=None, explicit=explicit_url)
        if url:
            return PostgresConnectionTarget("url", url)
    if explicit_service not in (None, ""):
        service = configured_postgres_service(metadata=None, explicit=explicit_service)
        if service:
            return PostgresConnectionTarget("service", service)
    if metadata:
        url = _configured_postgres_url_from_metadata_only(metadata)
        if url:
            return PostgresConnectionTarget("url", url)
        service = _configured_postgres_service_from_metadata_only(metadata)
        if service:
            return PostgresConnectionTarget("service", service)
    url = configured_postgres_url(metadata=None, explicit=None)
    if url:
        return PostgresConnectionTarget("url", url)
    service = configured_postgres_service(metadata=None, explicit=None)
    if service:
        return PostgresConnectionTarget("service", service)
    return PostgresConnectionTarget("", "")


def configured_postgres_password(explicit: str | None = None) -> str:
    """
    Read an explicit nonempty password or LIUXIN_POSTGRES_PASSWORD.

    The returned secret is not persisted by this function.

    Example:
        >>> configured_postgres_password(explicit="example-password")
        'example-password'


    :param explicit: Password preferred when neither None nor the empty string.
    :return: Explicit password converted to text, or the environment value/default empty string.
    """

    if explicit not in (None, ""):
        return str(explicit)
    return os.environ.get(POSTGRES_PASSWORD_ENV, "")


def configured_postgres_schema(
    metadata: Mapping[str, object] | None = None,
    explicit: str | None = None,
) -> str:
    """
    Resolve explicit, metadata schema, environment then public.

    An explicit whitespace-only value selects public immediately; blank metadata falls
    through to LIUXIN_POSTGRES_SCHEMA.

    Example:
        >>> configured_postgres_schema({"schema": "library"})
        'library'


    :param metadata: Optional database metadata mapping; recognized keys are checked in documented precedence order.
    :param explicit: Explicit candidate considered before metadata and environment values.
    :return: Trimmed schema name, defaulting to public.
    """

    if explicit not in (None, ""):
        return str(explicit).strip() or DEFAULT_POSTGRES_SCHEMA
    if metadata:
        value = str(metadata.get("schema") or "").strip()
        if value:
            return value
    return os.environ.get(POSTGRES_SCHEMA_ENV, "").strip() or DEFAULT_POSTGRES_SCHEMA


def store_postgres_password(password: str) -> str:
    """
    Store a nonempty password in this process environment for subsequent connections.

    No file is written. An empty input leaves any existing environment value untouched;
    child processes may inherit the environment.

    Example:
        ``store_postgres_password(prompted)`` lets later connections reuse a prompt result.


    :param password: Password text supplied by the caller.
    :return: Supplied password coerced to text, including an empty string.
    """

    text = str(password or "")
    if text:
        os.environ[POSTGRES_PASSWORD_ENV] = text
    return text


def prompt_and_store_postgres_password(url: str, *, overwrite: bool = False) -> str:
    """
    Reuse the configured password or prompt and store a replacement.

    Example:
        ``prompt_and_store_postgres_password(target.label, overwrite=True)`` requests
        a fresh password when stdin is interactive.


    :param url: PostgreSQL URL or service label used by the operation.
    :param overwrite: Whether to prompt even when an environment password already exists.
    :return: Existing or newly prompted password.
    """

    existing = configured_postgres_password()
    if existing and not overwrite:
        return existing
    return store_postgres_password(prompt_postgres_password(url))


def write_postgres_env_file(
    path: str | os.PathLike[str],
    *,
    url: str | None,
    service: str | None = None,
    password: str = "",
    include_password: bool = False,
    schema: str | None = None,
) -> Path:
    """
    Overwrite a shell export file with the selected target and optional settings.

    Create missing parent directories, write quoted exports, then chmod the file to 0600.
    The chmod occurs after writing. include_password controls a separate password export;
    a password already embedded in a URL remains in that URL export. Invalid unresolved
    targets raise PostgresConfigError; filesystem errors propagate.

    Example:
        ``write_postgres_env_file(path, url=None, service="library")`` writes a
        LIUXIN_POSTGRES_SERVICE export for subsequent shell commands.


    :param path: Destination filename; an existing file is overwritten.
    :param url: PostgreSQL URL or service label used by the operation.
    :param service: Optional explicit service candidate used by target resolution.
    :param password: Password text supplied by the caller.
    :param include_password: Whether to include a separate LIUXIN_POSTGRES_PASSWORD export.
    :param schema: Optional nonblank schema exported as LIUXIN_POSTGRES_SCHEMA.
    :return: Expanded path of the written file.
    """

    target_config = configured_postgres_target(explicit_url=url, explicit_service=service)
    if not target_config.configured:
        raise PostgresConfigError("A valid PostgreSQL URL or service profile is required to write an env file.")

    target = Path(path).expanduser()
    if not target.name:
        raise PostgresConfigError("PostgreSQL env file path must name a file.")
    target.parent.mkdir(parents=True, exist_ok=True)

    lines = ["# Generated by LiuXin PostgreSQL CLI."]
    if target_config.kind == "service":
        lines.append(f"export {POSTGRES_SERVICE_ENV}={shlex.quote(target_config.value)}")
    else:
        lines.append(f"export {POSTGRES_URL_ENV}={shlex.quote(target_config.value)}")
    schema_name = str(schema or "").strip()
    if schema_name:
        lines.append(f"export {POSTGRES_SCHEMA_ENV}={shlex.quote(schema_name)}")
    if include_password:
        lines.append(f"export {POSTGRES_PASSWORD_ENV}={shlex.quote(str(password or ''))}")
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.chmod(target, 0o600)
    return target


def redact_postgres_target(value: object) -> str:
    """
    Format service targets unchanged or pass URL targets through URL redaction.

    Example:
        >>> redact_postgres_target(PostgresConnectionTarget("service", "library"))
        'service=library'


    :param value: Resolved target or text; literal service= labels bypass URL redaction.
    :return: Service label or redacted URL text.
    """

    if isinstance(value, PostgresConnectionTarget):
        if value.kind == "service":
            return f"service={value.value}"
        return redact_postgres_url(value.value)
    text = str(value or "")
    if text.startswith("service="):
        return text
    return redact_postgres_url(text)


def redact_postgres_url(value: object) -> str:
    """
    Mask authority passwords and query values whose keys contain secret markers.

    Markers are pass, password, token, secret and key, matched case-insensitively.
    URLs without a scheme or network location pass through unchanged, including their
    query text. An initial parsing failure returns an invalid-URL marker; this helper
    is not a general credential scrubber.

    Example:
        >>> redact_postgres_url("postgresql://reader:example@localhost/library")
        'postgresql://reader:***@localhost/library'


    :param value: Value converted to text before inspection.
    :return: Rebuilt redacted URL, unchanged unsupported text or an invalid-URL marker.
    """

    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parts = urlsplit(text)
    except ValueError:
        return "<invalid database url>"
    if not parts.scheme or not parts.netloc:
        return text

    username = parts.username or ""
    hostname = parts.hostname or ""
    try:
        port = f":{parts.port}" if parts.port is not None else ""
    except ValueError:
        port = ""
    host = f"[{hostname}]" if ":" in hostname and not hostname.startswith("[") else hostname
    if username:
        auth = f"{username}:***@" if parts.password is not None else f"{username}@"
    else:
        auth = "***@" if parts.password is not None else ""
    netloc = f"{auth}{host}{port}"

    query_items: list[tuple[str, str]] = []
    for key, item_value in parse_qsl(parts.query, keep_blank_values=True):
        lowered = key.casefold()
        if any(secret in lowered for secret in SECRET_QUERY_KEYS):
            query_items.append((key, "***"))
        else:
            query_items.append((key, item_value))
    query = urlencode(query_items, doseq=True)
    return urlunsplit((parts.scheme, netloc, parts.path, query, parts.fragment))


def add_password_to_url(url: str, password: str) -> str:
    """
    Insert an encoded password only when no authority password is present.

    An empty supplied password or an existing password returns the original URL.
    Otherwise credentials are quoted and the authority is rebuilt; parse errors propagate.

    Example:
        >>> add_password_to_url("postgresql://reader@localhost/library", "sample")
        'postgresql://reader:sample@localhost/library'


    :param url: PostgreSQL URL or service label used by the operation.
    :param password: Password text supplied by the caller.
    :return: Original or rebuilt URL containing credentials.
    """

    if not password:
        return url
    parts = urlsplit(url)
    if parts.password is not None:
        return url
    username = parts.username or ""
    hostname = parts.hostname or ""
    try:
        port = f":{parts.port}" if parts.port is not None else ""
    except ValueError:
        port = ""
    host = f"[{hostname}]" if ":" in hostname and not hostname.startswith("[") else hostname
    auth = f"{quote(username, safe='')}:{quote(password, safe='')}@" if username else f":{quote(password, safe='')}@"
    return urlunsplit((parts.scheme, f"{auth}{host}{port}", parts.path, parts.query, parts.fragment))


def url_has_password(url: str) -> bool:
    """
    Check whether parsing exposes an authority password, including an empty password.

    Example:
        >>> url_has_password("postgresql://reader:@localhost/library")
        True


    :param url: PostgreSQL URL or service label used by the operation.
    :return: False on parse errors or absent password; True otherwise.
    """

    try:
        return urlsplit(url).password is not None
    except ValueError:
        return False


def password_prompt_label(url: str) -> str:
    """
    Describe a target using user, host and database without its authority password.

    Service labels pass through; missing URL components use configured-* placeholders.

    Example:
        >>> password_prompt_label("postgresql://reader@localhost/library")
        'reader@localhost/library'


    :param url: PostgreSQL URL or service label used by the operation.
    :return: Prompt label, or PostgreSQL when initial URL parsing fails.
    """

    text = str(url or "").strip()
    if text.casefold().startswith("service="):
        return text
    try:
        parts = urlsplit(text)
    except ValueError:
        return "PostgreSQL"
    user = parts.username or "configured user"
    host = parts.hostname or "configured host"
    db = parts.path.lstrip("/") or "configured database"
    return f"{user}@{host}/{db}"


def prompt_postgres_password(url: str) -> str:
    """
    Prompt through getpass only when stdin is a terminal.

    Raise PostgresConfigError for noninteractive stdin; do not store the answer here.

    Example:
        In a terminal, ``prompt_postgres_password("service=library")`` asks for
        the profile password without echoing it.


    :param url: PostgreSQL URL or service label used by the operation.
    :return: Password entered by the user.
    """

    if not sys.stdin.isatty():
        raise PostgresConfigError(
            "PostgreSQL password was required, but stdin is not interactive. "
            f"Set {POSTGRES_PASSWORD_ENV}, include a password in the URL, or configure .pgpass/PGSERVICE."
        )
    return getpass.getpass(f"PostgreSQL password for {password_prompt_label(url)}: ")


def should_prompt_for_password_error(exc: BaseException) -> bool:
    """
    Recognize password-related error text that may justify one prompt.

    Example:
        >>> should_prompt_for_password_error(RuntimeError("no password supplied"))
        True


    :param exc: Exception whose string is inspected; exception type is not checked.
    :return: Whether the lowercased message contains a supported authentication marker.
    """

    message = str(exc).casefold()
    return (
        "no password supplied" in message
        or "password authentication failed" in message
        or "fe_sendauth" in message
    )


def _normalise_service_name(value: object) -> str:
    """
    Strip whitespace and one case-insensitive service= prefix without validating.

    Example:
        >>> _normalise_service_name(" SERVICE= library ")
        'library'


    :param value: Value converted to text before inspection.
    :return: Trimmed profile text, possibly empty.
    """
    text = str(value or "").strip()
    if text.casefold().startswith("service="):
        return text.split("=", 1)[1].strip()
    return text


def _configured_postgres_url_from_metadata_only(metadata: Mapping[str, object]) -> str:
    """
    Select the first supported URL from metadata without environment fallback.

    Example:
        >>> _configured_postgres_url_from_metadata_only({"dsn": "postgresql:///books"})
        'postgresql:///books'


    :param metadata: Optional database metadata mapping; recognized keys are checked in documented precedence order.
    :return: First trimmed metadata URL or an empty string.
    """
    for key in METADATA_URL_KEYS:
        text = str(metadata.get(key) or "").strip()
        if is_postgres_url(text):
            return text
    return ""


def _configured_postgres_service_from_metadata_only(metadata: Mapping[str, object]) -> str:
    """
    Select the first valid metadata service without environment fallback.

    Example:
        >>> _configured_postgres_service_from_metadata_only({"service": "books"})
        'books'


    :param metadata: Optional database metadata mapping; recognized keys are checked in documented precedence order.
    :return: First normalized metadata profile or an empty string.
    """
    for key in METADATA_SERVICE_KEYS:
        text = _normalise_service_name(metadata.get(key))
        if is_postgres_service_name(text):
            return text
    return ""
