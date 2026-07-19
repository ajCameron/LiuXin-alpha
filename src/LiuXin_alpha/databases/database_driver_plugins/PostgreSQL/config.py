"""Runtime configuration helpers for the LiuXin PostgreSQL backend."""

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
    """Raised when PostgreSQL configuration is missing or invalid."""


@dataclass(frozen=True)
class PostgresConnectionTarget:
    """Resolved PostgreSQL connection target."""

    kind: POSTGRES_TARGET_KINDS
    value: str

    @property
    def configured(self) -> bool:
        return bool(self.kind and self.value)

    @property
    def label(self) -> str:
        if self.kind == "service":
            return f"service={self.value}"
        return self.value


def is_postgres_url(value: object) -> bool:
    """Return True when *value* is a PostgreSQL URL."""

    text = str(value or "").strip()
    if not text:
        return False
    try:
        return urlsplit(text).scheme.casefold() in POSTGRES_SCHEMES
    except ValueError:
        return False


def is_postgres_service_name(value: object) -> bool:
    """Return True when *value* is a conservative PostgreSQL service name."""

    text = _normalise_service_name(value)
    return bool(text and SERVICE_NAME_RE.fullmatch(text))


def configured_postgres_url(
    metadata: Mapping[str, object] | None = None,
    explicit: str | None = None,
) -> str:
    """Return the configured PostgreSQL URL, preferring explicit and metadata values."""

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
    """Return the configured PostgreSQL service profile name."""

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
    """Return the configured PostgreSQL URL or service target."""

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
    """Return a configured PostgreSQL password without persisting it."""

    if explicit not in (None, ""):
        return str(explicit)
    return os.environ.get(POSTGRES_PASSWORD_ENV, "")


def configured_postgres_schema(
    metadata: Mapping[str, object] | None = None,
    explicit: str | None = None,
) -> str:
    """Return the configured PostgreSQL schema name."""

    if explicit not in (None, ""):
        return str(explicit).strip() or DEFAULT_POSTGRES_SCHEMA
    if metadata:
        value = str(metadata.get("schema") or "").strip()
        if value:
            return value
    return os.environ.get(POSTGRES_SCHEMA_ENV, "").strip() or DEFAULT_POSTGRES_SCHEMA


def store_postgres_password(password: str) -> str:
    """
    Store a prompted PostgreSQL password for reuse within this process only.

    Persistent secret storage should remain in ``.pgpass``, ``PGSERVICE``, the
    shell environment, or the user's password manager.
    """

    text = str(password or "")
    if text:
        os.environ[POSTGRES_PASSWORD_ENV] = text
    return text


def prompt_and_store_postgres_password(url: str, *, overwrite: bool = False) -> str:
    """Prompt for a PostgreSQL password and store it for this process."""

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
    Write shell exports for LiuXin PostgreSQL commands.

    The file is written mode 0600 because URLs describe private infrastructure and
    may optionally include a password export.
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
    """Redact a resolved PostgreSQL target for logs and status output."""

    if isinstance(value, PostgresConnectionTarget):
        if value.kind == "service":
            return f"service={value.value}"
        return redact_postgres_url(value.value)
    text = str(value or "")
    if text.startswith("service="):
        return text
    return redact_postgres_url(text)


def redact_postgres_url(value: object) -> str:
    """Redact passwords and secret query parameters from a PostgreSQL URL."""

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
    """Return *url* with *password* inserted when the URL has no password."""

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
    """Return True when *url* embeds a password."""

    try:
        return urlsplit(url).password is not None
    except ValueError:
        return False


def password_prompt_label(url: str) -> str:
    """Return a human-readable label for a PostgreSQL password prompt."""

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
    """Prompt for a PostgreSQL password when stdin is interactive."""

    if not sys.stdin.isatty():
        raise PostgresConfigError(
            "PostgreSQL password was required, but stdin is not interactive. "
            f"Set {POSTGRES_PASSWORD_ENV}, include a password in the URL, or configure .pgpass/PGSERVICE."
        )
    return getpass.getpass(f"PostgreSQL password for {password_prompt_label(url)}: ")


def should_prompt_for_password_error(exc: BaseException) -> bool:
    """Return True for psycopg authentication failures that may be solved by prompting."""

    message = str(exc).casefold()
    return (
        "no password supplied" in message
        or "password authentication failed" in message
        or "fe_sendauth" in message
    )


def _normalise_service_name(value: object) -> str:
    text = str(value or "").strip()
    if text.casefold().startswith("service="):
        return text.split("=", 1)[1].strip()
    return text


def _configured_postgres_url_from_metadata_only(metadata: Mapping[str, object]) -> str:
    for key in METADATA_URL_KEYS:
        text = str(metadata.get(key) or "").strip()
        if is_postgres_url(text):
            return text
    return ""


def _configured_postgres_service_from_metadata_only(metadata: Mapping[str, object]) -> str:
    for key in METADATA_SERVICE_KEYS:
        text = _normalise_service_name(metadata.get(key))
        if is_postgres_service_name(text):
            return text
    return ""
