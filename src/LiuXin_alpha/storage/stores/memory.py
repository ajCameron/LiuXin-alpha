"""Bind a transient in-memory driver to configured Store identity."""

from __future__ import annotations

from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    ReplicaMode,
    StoreConfiguration,
    StoreStatus,
)
from LiuXin_alpha.storage.api.store_api.driver_backed_api import DriverBackedStoreAPI
from LiuXin_alpha.storage.drivers.memory import (
    MemoryObjectAddress,
    MemoryStorageDriver,
)


class MemoryStore(DriverBackedStoreAPI[MemoryObjectAddress]):
    """Expose process-local object storage for cache and transient replicas.

    A Store starts offline. ``startup`` enables operations, ``close`` disables
    them while retaining bytes on that same object, and discarding the object or
    ending the process discards every stored byte. It must not be used as the
    sole authoritative or durable copy of an Asset.
    """

    store_kind = "memory"

    def __init__(
        self,
        name: str = "memory",
        uuid: str | UUID | None = None,
        *,
        max_bytes: int | None = None,
        configuration: StoreConfiguration | None = None,
    ) -> None:
        """
        Create an empty unstarted Store with an optional portable total byte ceiling.

        Direct construction records a non-None ``max_bytes`` value in ``backend_options`` so the
        Store can be reconstructed with the same bound. When a complete configuration is supplied,
        its ``max_bytes`` option is authoritative; a simultaneous conflicting argument is rejected
        rather than creating a runtime/configuration mismatch.
        """
        store_uuid = (
            configuration.store_uuid if configuration is not None else _store_uuid(uuid)
        )
        if configuration is None:
            self._configuration = StoreConfiguration(
                store_uuid=store_uuid,
                store_name=name,
                store_kind=self.store_kind,
                store_root_uri=f"memory://{store_uuid}",
                store_access_protocol="memory",
                supported_replica_modes=frozenset(
                    {ReplicaMode.CACHE, ReplicaMode.TRANSIENT}
                ),
                operational_role="cache",
                backend_options=(
                    () if max_bytes is None else (("max_bytes", max_bytes),)
                ),
            )
        else:
            configured_max_bytes = _configuration_max_bytes(configuration)
            if max_bytes is not None and max_bytes != configured_max_bytes:
                raise ValueError(
                    "max_bytes must match the supplied Store configuration."
                )
            max_bytes = configured_max_bytes
            self._configuration = configuration
        self.__driver = MemoryStorageDriver(
            address_space_uuid=store_uuid,
            root_uri=self._configuration.store_root_uri,
            read_only=self._configuration.read_only,
            max_bytes=max_bytes,
        )

    @property
    def configuration(self) -> StoreConfiguration:
        """Return the retained portable Store configuration."""
        return self._configuration

    @property
    def _driver(self) -> MemoryStorageDriver:
        """Supply the owned raw driver to inherited routing operations."""
        return self.__driver

    @property
    def driver(self) -> MemoryStorageDriver:
        """Expose the owned raw driver for diagnostics and import workflows."""
        return self.__driver

    @classmethod
    def from_configuration(
        cls,
        configuration: StoreConfiguration,
    ) -> MemoryStore:
        """
        Recreate an empty Store from configuration and its portable ``max_bytes`` option.

        Only the configuration is forwarded: the constructor parses and validates the option once,
        preventing the retained configuration and live driver from acquiring different limits.
        Previously stored process-local bytes are intentionally never reconstructed.
        """
        return cls(configuration=configuration)

    def self_test(self) -> StoreStatus:
        """Return a fresh lifecycle/capacity observation for this process-local Store."""
        return self.probe()


def _store_uuid(value: str | UUID | None) -> UUID:
    """Preserve a UUID, parse UUID text, or create a fresh Store identity."""
    if value is None:
        return uuid4()
    return value if isinstance(value, UUID) else UUID(value)


def _configuration_max_bytes(configuration: StoreConfiguration) -> int | None:
    """
    Parse the sole supported memory backend option without mutating retained configuration.

    Integer text is accepted for compatibility with row-based configuration sources. Bool is
    rejected explicitly despite being an int subtype; positivity is finally enforced by the raw
    memory driver constructor.

    :param configuration: Portable Store configuration whose backend options are inspected.
    :return: Parsed positive-integer candidate or None; unknown and malformed options raise.
    """

    options = dict(configuration.backend_options)
    unknown = set(options).difference({"max_bytes"})
    if unknown:
        names = ", ".join(sorted(unknown))
        raise ValueError(f"unsupported memory backend options: {names}.")
    raw_max_bytes = options.get("max_bytes")
    if raw_max_bytes is None:
        return None
    if isinstance(raw_max_bytes, bool):
        raise TypeError("memory max_bytes must be a positive integer or None.")
    if isinstance(raw_max_bytes, int):
        return raw_max_bytes
    if isinstance(raw_max_bytes, str):
        try:
            return int(raw_max_bytes)
        except ValueError as error:
            raise ValueError("memory max_bytes must be integer text.") from error
    raise TypeError("memory max_bytes must be a positive integer or None.")


__all__ = ["MemoryStore"]
