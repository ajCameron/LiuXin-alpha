"""Immutable link DDL compiled before any relationship tables are executed."""

from dataclasses import dataclass


@dataclass(frozen=True)
class LinkBuildStep:
    """One link and its enumeration, independent of mutable parser state.

    Placeholder mode takes precedence over a nonnullable endpoint request. Disable
    allow_placeholders in the owning TOML document to honor nullable literally.
    """

    table: str
    column_base: str
    statements: tuple[str, ...]
    allowed_types: tuple[str, ...] | None
    requested_nullable: bool
    allow_placeholders: bool

    @property
    def effective_nullable(self) -> bool:
        """Whether the compiled endpoints permit incomplete/blank rows."""
        return self.requested_nullable or self.allow_placeholders
