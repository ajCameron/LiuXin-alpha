"""Declare macro hosts without importing backend implementations or opening handles."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Literal, Protocol, cast, overload

from LiuXin_alpha.errors import DatabaseDriverError, DatabaseIntegrityError

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.api.macros_api import MacrosAPI
    from LiuXin_alpha.databases.api.portable_macros_api import DatabaseMacrosAPI
    from LiuXin_alpha.databases.db_types.driver_types import SQLConnection


class MacroHostState:
    """Retain an optional host while requiring a live binding for facade operations.

    Standalone drivers construct their macro providers before a facade exists.
    Keeping that state explicit prevents annotating a detached provider as ready.
    """

    db: DatabaseAPI | None

    @property
    def _macro_database(self) -> DatabaseAPI:
        """Return the attached facade or reject use of a detached provider."""
        return macro_database(self)


class MacroTransactionState(threading.local):
    """Keep nesting and borrowed handles isolated to each calling thread.

    Do not add slots: threading.local slots are shared between threads. Defaults
    apply to threads which have not entered a transaction on this provider yet.
    """

    depth: int = 0
    connection: SQLConnection | None = None
    serialized: bool = False


@overload
def macro_transaction_state(
    host: object, *, create: Literal[True]
) -> MacroTransactionState: ...


@overload
def macro_transaction_state(
    host: object, *, create: Literal[False] = False
) -> MacroTransactionState | None: ...


def macro_transaction_state(
    host: object, *, create: bool = False
) -> MacroTransactionState | None:
    """Read owned instance state without probing a compatibility __getattr__."""
    values = vars(host)
    state = values.get("_macro_transaction_state")
    if state is None and create:
        state = values.setdefault("_macro_transaction_state", MacroTransactionState())
    return cast("MacroTransactionState | None", state)


def macro_operations(host: object) -> DatabaseMacrosAPI:
    """Resolve sibling methods against the canonical composed macro contract."""
    return cast("DatabaseMacrosAPI", host)


class _MacroConnectionHost(Protocol):
    def _macro_connection(self) -> SQLConnection: ...


def macro_connection(host: object) -> SQLConnection:
    """Resolve the private portable connection hook of a composed macro host."""
    return cast(_MacroConnectionHost, host)._macro_connection()


def macro_database(host: object) -> DatabaseAPI:
    """Read a bound facade, including on documented unbound structural hosts."""
    database = cast(MacroHostState, host).db
    if database is None:
        raise DatabaseIntegrityError("Database macros are not attached to a database.")
    return database


def require_legacy_macros(provider: DatabaseMacrosAPI) -> MacrosAPI:
    """Require the explicitly declared legacy SQLite compatibility capability.

    Generic drivers expose only DatabaseMacrosAPI. A legacy caller must narrow
    its provider before issuing custom-column/cache SQL. Reject unsupported
    backends before executing any legacy statement or probing __getattr__.
    """
    from LiuXin_alpha.databases.api.macros_api import MacrosAPI

    if not isinstance(provider, MacrosAPI):
        raise DatabaseDriverError(
            "This operation requires legacy SQLite macros; "
            f"{type(provider).__name__} does not provide that capability."
        )
    return provider
