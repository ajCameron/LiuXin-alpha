"""
Compose Core services and dispatch named command/query envelopes with events, introspection, and job control.

Handler bodies share one reentrant lock; registration/subscriber maps use a
separate lock. Event delivery and result conversion are outside the handler lock.
Transport-stable endpoints normalize results even for direct calls, while generic
invoke retains compatibility values. These boundaries do not add transactions,
authorization, or proof that a query cannot mutate its target.
"""

# pyright: reportImportCycles=false

from __future__ import annotations

import inspect
import threading
import uuid

from typing import Any, Callable, Mapping, Optional, cast

from LiuXin_alpha.core.api import CoreAPI
from LiuXin_alpha.core.commands import CoreCommand, CoreCommandResult
from LiuXin_alpha.core.description import (
    CoreEndpointDescription,
    CoreMethodDescription,
    CoreParameterDescription,
    CorePayloadFieldDescription,
    CoreTargetDescription,
)
from LiuXin_alpha.core.dispatch import looks_like_write_method
from LiuXin_alpha.core.errors import (
    CoreDispatchError,
    CoreHandlerError,
    CoreShutdownError,
    core_error_details,
)
from LiuXin_alpha.core.events import CoreEvent, make_core_event
from LiuXin_alpha.core.queries import CoreQuery, CoreQueryResult
from LiuXin_alpha.core.services import CoreServices
from LiuXin_alpha.core.wire import to_wire
from LiuXin_alpha.utils.jobs import JobRequest, default_job_manager
from LiuXin_alpha.utils.jobs.manager import JobManagerAPI, JobState


CommandHandler = Callable[["CoreRuntime", CoreCommand], Any]
QueryHandler = Callable[["CoreRuntime", CoreQuery], Any]
EventSubscriber = Callable[[CoreEvent], None]


class CoreRuntime(CoreAPI):
    """
    Host one service graph, named endpoint registries, synchronous subscribers, and a managed-job interface.

    Construction installs built-in and application/program endpoint families.
    Local facade properties expose shared objects directly; named calls provide
    the execution/error/wire boundaries described by their methods. Shutdown uses
    configured ownership, not blanket ownership of every supplied service.

    Example:
        >>> runtime = CoreRuntime(library=library, core_uuid="session-1")  # doctest: +SKIP
        >>> status = runtime.health()  # doctest: +SKIP
    """

    def __init__(
        self,
        *,
        library: Any,
        core_uuid: Optional[str] = None,
        core_version: str = "2.0.0",
        api_version: str = "2.0",
        job_manager: JobManagerAPI | None = None,
        catalog: Any | None = None,
        cache: Any | None = None,
        cache_type: str | None = None,
        cache_kwargs: Mapping[str, Any] | None = None,
        read_source: Any | None = None,
        preferences: Any | None = None,
        library_preferences: Any | None = None,
        field_metadata: Any | None = None,
        maintenance: Any | None = None,
        cache_allow_database_fallback: bool = True,
        close_cache_on_shutdown: bool | None = None,
        close_library_on_shutdown: bool = False,
        close_job_manager_on_shutdown: bool | None = None,
    ) -> None:
        """
        Compose services, establish identity/locks, select a job manager, and install all endpoint families.

        Falsey core_uuid values receive a generated UUID; version labels are
        stringified without validation. The process-default job manager is shared,
        not owned. Unless explicitly overridden, neither it nor an injected manager
        is closed at shutdown. Service preparation and incremental registrations
        precede completion; construction failure has no compensating teardown here.

        Example:
            >>> runtime = CoreRuntime(library=library, job_manager=manager)  # doctest: +SKIP


        :param library: Required library facade used by CoreServices and generic target invocation.
        :param core_uuid: Optional advertised identity; a falsey value generates a fresh UUID4 string.
        :param core_version: Runtime version label converted to text.
        :param api_version: Named-API contract version label converted to text.
        :param job_manager: Optional borrowed manager; omission selects the process-global default.
        :param catalog: Optional semantic Catalog facade injected into CoreServices.
        :param cache: Optional cache facade/backend injected instead of selecting a cache type.
        :param cache_type: Optional selector for a cache constructed by CoreServices.
        :param cache_kwargs: Optional keyword arguments for selected-cache construction.
        :param read_source: Optional metadata read source instead of lazy database/cache composition.
        :param preferences: Optional application preference facade passed to CoreServices.
        :param library_preferences: Optional per-library preference facade passed to CoreServices.
        :param field_metadata: Optional field-description object passed to CoreServices.
        :param maintenance: Optional maintenance service passed to CoreServices.
        :param cache_allow_database_fallback: Whether a composed cache read source may fall back to the database.
        :param close_cache_on_shutdown: Explicit cache cleanup policy; None delegates ownership-based selection to CoreServices.
        :param close_library_on_shutdown: Whether CoreServices should close the hosted library during shutdown.
        :param close_job_manager_on_shutdown: Explicit manager shutdown flag; None currently selects false.
        :return: ``None`` after successful composition and handler installation, without starting an HTTP server.
        """
        self._services = CoreServices(
            library=library,
            catalog=catalog,
            cache=cache,
            cache_type=cache_type,
            cache_kwargs=cache_kwargs,
            read_source=read_source,
            preferences=preferences,
            library_preferences=library_preferences,
            field_metadata=field_metadata,
            maintenance=maintenance,
            cache_allow_database_fallback=cache_allow_database_fallback,
            close_cache_on_shutdown=close_cache_on_shutdown,
            close_library_on_shutdown=close_library_on_shutdown,
        )
        self._library = self._services.library
        self._core_uuid = str(core_uuid or uuid.uuid4())
        self._core_version = str(core_version)
        self._api_version = str(api_version)
        self._shutdown = False
        # ``default_job_manager()`` is process-global and may be shared by
        # several direct Core sessions. A runtime must not shut it down merely
        # because the caller omitted an explicit manager.
        owns_job_manager = False
        self._job_manager: JobManagerAPI = (
            job_manager if job_manager is not None else default_job_manager()
        )
        if close_job_manager_on_shutdown is None:
            close_job_manager_on_shutdown = owns_job_manager
        self._close_job_manager_on_shutdown = bool(close_job_manager_on_shutdown)

        self._command_handlers: dict[str, CommandHandler] = {}
        self._query_handlers: dict[str, QueryHandler] = {}
        self._command_descriptions: dict[str, CoreEndpointDescription] = {}
        self._query_descriptions: dict[str, CoreEndpointDescription] = {}
        self._event_subscribers: list[EventSubscriber] = []

        # SQLite-backed runtimes may be reached from HTTP request threads.
        # Serialize every handler against the shared local service graph.
        self._command_lock = threading.RLock()
        # Protects handler and subscriber maps.
        self._state_lock = threading.RLock()

        self.register_command_handler(
            "invoke",
            self._handle_invoke_command,
            summary="Invoke a write-path method on a hosted target.",
            description="Generic escape hatch for library/database/storage write calls.",
            payload_fields=(
                CorePayloadFieldDescription(
                    name="target", required=True, field_type="string"
                ),
                CorePayloadFieldDescription(
                    name="method", required=True, field_type="string"
                ),
                CorePayloadFieldDescription(name="args", field_type="array"),
                CorePayloadFieldDescription(name="kwargs", field_type="object"),
            ),
            tags=("generic", "invoke"),
            transport_stable=False,
        )
        self.register_command_handler(
            "shutdown",
            self._handle_shutdown_command,
            summary="Shut the runtime down.",
            tags=("lifecycle",),
        )
        self.register_command_handler(
            "sync.store.start",
            self._handle_sync_store_start_command,
            summary="Submit a store sync as a managed background job.",
            payload_fields=(
                CorePayloadFieldDescription(
                    name="sync_kwargs", required=True, field_type="object"
                ),
                CorePayloadFieldDescription(name="job_timeout_s", field_type="number"),
                CorePayloadFieldDescription(name="job_no_output", field_type="boolean"),
                CorePayloadFieldDescription(name="job_backend", field_type="string"),
                CorePayloadFieldDescription(name="label", field_type="string"),
            ),
            tags=("sync", "jobs"),
        )
        self.register_command_handler(
            "sync.store.cancel",
            self._handle_sync_store_cancel_command,
            summary="Cancel a previously submitted sync job.",
            payload_fields=(
                CorePayloadFieldDescription(
                    name="job_id", required=True, field_type="string"
                ),
            ),
            tags=("sync", "jobs"),
        )
        self.register_command_handler(
            "jobs.cancel",
            self._handle_jobs_cancel_command,
            summary="Cancel an existing managed job.",
            payload_fields=(
                CorePayloadFieldDescription(
                    name="job_id", required=True, field_type="string"
                ),
            ),
            tags=("jobs",),
        )
        self.register_command_handler(
            "jobs.retry",
            self._handle_jobs_retry_command,
            summary="Replay one terminal managed job as a new linked run.",
            payload_fields=(
                CorePayloadFieldDescription(
                    name="job_id", required=True, field_type="string"
                ),
                CorePayloadFieldDescription(name="label", field_type="string|null"),
                CorePayloadFieldDescription(
                    name="allow_succeeded", field_type="boolean"
                ),
            ),
            tags=("jobs", "write"),
        )
        self.register_command_handler(
            "metadata.write",
            self._handle_metadata_write_command,
            summary="Write supported item-centred metadata fields.",
            payload_fields=(
                CorePayloadFieldDescription(
                    name="item_id", required=True, field_type="integer"
                ),
                CorePayloadFieldDescription(
                    name="values", required=True, field_type="object"
                ),
                CorePayloadFieldDescription(name="fields", field_type="array"),
                CorePayloadFieldDescription(name="kind", field_type="string"),
                CorePayloadFieldDescription(name="replace", field_type="boolean"),
                CorePayloadFieldDescription(name="target_level", field_type="string"),
                CorePayloadFieldDescription(name="mark_dirty", field_type="boolean"),
            ),
            tags=("metadata", "write"),
        )
        self.register_command_handler(
            "metadata.tags.replace",
            self._handle_metadata_tags_replace_command,
            summary="Replace the tags for one item-centred metadata row.",
            payload_fields=(
                CorePayloadFieldDescription(
                    name="item_id", required=True, field_type="integer"
                ),
                CorePayloadFieldDescription(
                    name="tags", required=True, field_type="array"
                ),
                CorePayloadFieldDescription(name="kind", field_type="string"),
            ),
            tags=("metadata", "tags", "write"),
        )
        self.register_command_handler(
            "metadata.labels.replace",
            self._handle_metadata_labels_replace_command,
            summary="Replace the labels for one item-centred metadata row.",
            payload_fields=(
                CorePayloadFieldDescription(
                    name="item_id", required=True, field_type="integer"
                ),
                CorePayloadFieldDescription(
                    name="labels", required=True, field_type="array"
                ),
                CorePayloadFieldDescription(name="kind", field_type="string"),
            ),
            tags=("metadata", "labels", "write"),
        )
        self.register_command_handler(
            "metadata.genre.replace",
            self._handle_metadata_genre_replace_command,
            summary="Replace the genre values for one item-centred metadata row.",
            payload_fields=(
                CorePayloadFieldDescription(
                    name="item_id", required=True, field_type="integer"
                ),
                CorePayloadFieldDescription(
                    name="genre", required=True, field_type="array"
                ),
                CorePayloadFieldDescription(name="kind", field_type="string"),
            ),
            tags=("metadata", "genre", "write"),
        )
        self.register_command_handler(
            "metadata.series.replace",
            self._handle_metadata_series_replace_command,
            summary="Replace the series values for one item-centred metadata row.",
            payload_fields=(
                CorePayloadFieldDescription(
                    name="item_id", required=True, field_type="integer"
                ),
                CorePayloadFieldDescription(
                    name="series", required=True, field_type="array"
                ),
                CorePayloadFieldDescription(name="kind", field_type="string"),
            ),
            tags=("metadata", "series", "write"),
        )
        self.register_command_handler(
            "metadata.identifiers.replace",
            self._handle_metadata_identifiers_replace_command,
            summary="Replace identifiers for one item-centred metadata row.",
            payload_fields=(
                CorePayloadFieldDescription(
                    name="item_id", required=True, field_type="integer"
                ),
                CorePayloadFieldDescription(
                    name="identifiers", required=True, field_type="object"
                ),
                CorePayloadFieldDescription(name="kind", field_type="string"),
            ),
            tags=("metadata", "identifiers", "write"),
        )
        self.register_query_handler(
            "invoke",
            self._handle_invoke_query,
            summary="Invoke a read-path method on a hosted target.",
            description="Generic escape hatch for library/database/storage query calls.",
            payload_fields=(
                CorePayloadFieldDescription(
                    name="target", required=True, field_type="string"
                ),
                CorePayloadFieldDescription(
                    name="method", required=True, field_type="string"
                ),
                CorePayloadFieldDescription(name="args", field_type="array"),
                CorePayloadFieldDescription(name="kwargs", field_type="object"),
            ),
            tags=("generic", "invoke"),
            transport_stable=False,
        )
        self.register_query_handler(
            "health",
            self._handle_health_query,
            summary="Return runtime health and registration state.",
            tags=("lifecycle", "health"),
        )
        self.register_query_handler(
            "api.describe",
            self._handle_api_describe_query,
            summary="Describe the available core API surface.",
            payload_fields=(
                CorePayloadFieldDescription(
                    name="include_targets", field_type="boolean"
                ),
                CorePayloadFieldDescription(name="target", field_type="string"),
            ),
            tags=("api", "introspection"),
        )
        self.register_query_handler(
            "jobs.list",
            self._handle_jobs_list_query,
            summary="List managed jobs with optional filtering and pagination.",
            payload_fields=(
                CorePayloadFieldDescription(name="states", field_type="array"),
                CorePayloadFieldDescription(name="limit", field_type="integer"),
                CorePayloadFieldDescription(name="offset", field_type="integer"),
            ),
            tags=("jobs",),
        )
        self.register_query_handler(
            "jobs.get",
            self._handle_jobs_get_query,
            summary="Fetch one managed job by id.",
            payload_fields=(
                CorePayloadFieldDescription(
                    name="job_id", required=True, field_type="string"
                ),
            ),
            tags=("jobs",),
        )
        self.register_query_handler(
            "jobs.wait",
            self._handle_jobs_wait_query,
            summary="Wait for a managed job to finish.",
            payload_fields=(
                CorePayloadFieldDescription(
                    name="job_id", required=True, field_type="string"
                ),
                CorePayloadFieldDescription(name="timeout_s", field_type="number"),
            ),
            tags=("jobs",),
        )
        from LiuXin_alpha.core.application_api import install_application_api

        self._application_api = install_application_api(self)
        from LiuXin_alpha.core.program_api import install_program_api

        self._program_api = install_program_api(self)
        from LiuXin_alpha.core.storage_graph_api import install_storage_graph_api

        self._storage_graph_api = install_storage_graph_api(self)
        from LiuXin_alpha.core.browse_api import install_browse_api

        self._browse_api = install_browse_api(self)
        from LiuXin_alpha.core.database_semantics_api import (
            install_database_semantics_api,
        )

        self._database_semantics_api = install_database_semantics_api(self)

    @property
    def core_uuid(self) -> str:
        """
        Return the runtime identity captured at construction without consulting any external service.

        Example:
            >>> identity = runtime.core_uuid  # doctest: +SKIP


        :return: Stored advertised instance identifier string.
        """
        return self._core_uuid

    @property
    def core_version(self) -> str:
        """
        Return the stored runtime implementation version label without interpreting it.

        Example:
            >>> version = runtime.core_version  # doctest: +SKIP


        :return: Implementation-version text supplied at construction.
        """
        return self._core_version

    @property
    def api_version(self) -> str:
        """
        Return the stored named-API version label without compatibility negotiation.

        Example:
            >>> version = runtime.api_version  # doctest: +SKIP


        :return: Advertised command/query contract version text.
        """
        return self._api_version

    @property
    def library(self) -> Any:
        """
        Expose the hosted library reference directly to in-process callers.

        Access does not check shutdown state or wrap subsequent library calls in
        Core's dispatch, event, or serialization boundaries.

        Example:
            >>> library = runtime.library  # doctest: +SKIP


        :return: Shared library object retained during service composition.
        """
        return self._library

    @property
    def services(self) -> CoreServices:
        """
        Expose the composed service graph and its configured ownership policies.

        Example:
            >>> services = runtime.services  # doctest: +SKIP


        :return: Shared CoreServices instance, without copying or a shutdown-state check.
        """

        return self._services

    @property
    def database(self) -> Any:
        """
        Return the canonical database selected by CoreServices, which may be borrowed rather than owned.

        Example:
            >>> database = runtime.database  # doctest: +SKIP


        :return: Shared library database object outside the named-call transport boundary.
        """

        return self.services.database

    @property
    def catalog(self) -> Any:
        """
        Resolve the semantic Catalog through CoreServices, constructing it lazily when not supplied.

        Example:
            >>> catalog = runtime.catalog  # doctest: +SKIP


        :return: Shared injected or lazily composed Catalog facade.
        """

        return self.services.catalog

    @property
    def cache(self) -> Any | None:
        """
        Expose the optional prepared cache without creating a new cache on access.

        Example:
            >>> cache = runtime.cache  # doctest: +SKIP


        :return: Configured cache facade or None when no cache was selected.
        """

        return self.services.cache

    @property
    def read_source(self) -> Any:
        """
        Resolve the shared metadata read source through CoreServices' lazy composition policy.

        Example:
            >>> source = runtime.read_source  # doctest: +SKIP


        :return: Injected or lazily constructed database/cache metadata adapter.
        """

        return self.services.read_source

    @property
    def job_manager(self) -> JobManagerAPI:
        """
        Expose the injected or process-default manager used by named job handlers.

        Example:
            >>> manager = runtime.job_manager  # doctest: +SKIP


        :return: Retained manager reference, without transferring ownership to the caller.
        """
        return self._job_manager

    @property
    def is_shutdown(self) -> bool:
        """
        Read the shutdown-request flag rather than probe whether all cleanup or jobs have finished.

        Example:
            >>> stopped = runtime.is_shutdown  # doctest: +SKIP


        :return: True once the first shutdown call sets the flag, including after cleanup failure.
        """
        return bool(self._shutdown)

    def shutdown(self) -> int:
        """
        Mark shutdown, close configured services, optionally stop the job manager, and emit core.shutdown on success.

        The flag is set before cleanup and repeated calls return zero without
        retrying failed cleanup. Opted-in manager shutdown runs in finally with
        wait false/cancel_pending true; its error can replace a service-close error.
        Cleanup failure prevents the shutdown event. This method does not acquire
        the handler lock or await running jobs.

        Example:
            >>> exit_code = runtime.shutdown()  # doctest: +SKIP


        :return: Zero after successful cleanup/event delivery or on an already-flagged runtime.
        """
        if self._shutdown:
            return 0
        self._shutdown = True
        try:
            self.services.close()
        finally:
            if self._close_job_manager_on_shutdown:
                self.job_manager.shutdown(
                    wait=False,
                    cancel_pending=True,
                )
        self.emit_event("core.shutdown", {"reason": "explicit"})
        return 0

    def __enter__(self) -> "CoreRuntime":
        """
        Return this already-constructed runtime without starting services or checking shutdown state.

        Example:
            >>> with runtime as active:  # doctest: +SKIP
            ...     status = active.health()


        :return: This runtime instance unchanged.
        """
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """
        Request shutdown on context exit without deliberately suppressing the block's exception.

        A shutdown failure can itself propagate and replace the original exception.

        Example:
            >>> runtime.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Ignored exception type supplied by the context-manager protocol.
        :param exc: Ignored exception instance from the with block.
        :param tb: Ignored traceback from the with block.
        :return: None after shutdown returns, leaving ordinary exception propagation enabled.
        """
        del exc_type, exc, tb
        self.shutdown()

    @staticmethod
    def _doc_summary(obj: Any) -> str:
        """
        Extract the first stripped line of inspect.getdoc's cleaned, possibly inherited documentation.

        Example:
            >>> from types import SimpleNamespace
            >>> CoreRuntime._doc_summary(SimpleNamespace(__doc__=" First line " + chr(10) + "Details"))
            'First line'


        :param obj: Object whose docstring inspect.getdoc should resolve.
        :return: First non-surrounding-whitespace line, or empty text if documentation is absent/blank.
        """
        doc = inspect.getdoc(obj) or ""
        text = doc.strip()
        if not text:
            return ""
        return text.splitlines()[0].strip()

    @staticmethod
    def _normalize_payload_fields(
        payload_fields: tuple[CorePayloadFieldDescription, ...]
        | list[CorePayloadFieldDescription]
        | None,
    ) -> tuple[CorePayloadFieldDescription, ...]:
        """
        Freeze the outer payload-field sequence as a tuple without validating its elements or schema meaning.

        Example:
            >>> CoreRuntime._normalize_payload_fields([CorePayloadFieldDescription("limit")])[0].name
            'limit'


        :param payload_fields: Optional ordered field records; falsey values become an empty tuple.
        :return: Tuple of the same field objects in supplied order, without deduplication.
        """
        if not payload_fields:
            return ()
        return tuple(payload_fields)

    def register_command_handler(
        self,
        name: str,
        handler: CommandHandler,
        *,
        summary: str | None = None,
        description: str = "",
        payload_fields: tuple[CorePayloadFieldDescription, ...]
        | list[CorePayloadFieldDescription]
        | None = None,
        tags: tuple[str, ...] | list[str] | None = None,
        transport_stable: bool = True,
    ) -> None:
        """
        Register or replace a normalized command route and its descriptive endpoint metadata.

        Names are stringified, stripped, and lowercased. Existing registrations are
        overwritten without collision checks or callable/signature validation.
        Handler assignment precedes description construction, so a metadata error
        can leave a new handler with old/missing metadata. Payload fields describe
        expectations; this registration does not install payload validation.

        Example:
            >>> runtime.register_command_handler("example.update", handler, summary="Update an example.")  # doctest: +SKIP


        :param name: Command route name normalized to nonblank lowercase text.
        :param handler: Callable invoked as handler(runtime, command), retained without validation.
        :param summary: Explicit summary, including empty text; None derives the handler's first docstring line.
        :param description: Longer descriptive text; falsey input becomes empty text.
        :param payload_fields: Optional ordered field descriptions converted to a tuple without schema enforcement.
        :param tags: Optional tag values stringified in order without stripping or deduplication.
        :param transport_stable: Truth-converted flag selecting result wire conversion during execution.
        :return: None after updating the command handler and description maps under their state lock.
        :raises ValueError: If the normalized route name is blank.
        """
        token = str(name).strip().lower()
        if not token:
            raise ValueError("Command name cannot be blank.")
        with self._state_lock:
            self._command_handlers[token] = handler
            self._command_descriptions[token] = CoreEndpointDescription(
                name=token,
                kind="command",
                summary=str(
                    summary if summary is not None else self._doc_summary(handler)
                ),
                description=str(description or ""),
                payload_fields=self._normalize_payload_fields(payload_fields),
                tags=tuple(str(tag) for tag in (tags or ())),
                transport_stable=bool(transport_stable),
            )

    def register_query_handler(
        self,
        name: str,
        handler: QueryHandler,
        *,
        summary: str | None = None,
        description: str = "",
        payload_fields: tuple[CorePayloadFieldDescription, ...]
        | list[CorePayloadFieldDescription]
        | None = None,
        tags: tuple[str, ...] | list[str] | None = None,
        transport_stable: bool = True,
    ) -> None:
        """
        Register or replace a normalized query route and its description without proving read-only behavior.

        Registration accepts repeated names and does not verify callability,
        signature, or declared payload fields. The handler map changes before
        metadata construction, so a later metadata error is not rolled back.

        Example:
            >>> runtime.register_query_handler("example.lookup", handler, summary="Read an example.")  # doctest: +SKIP


        :param name: Query route normalized by stringification, stripping, and lowercasing.
        :param handler: Callable expected to accept runtime and query envelope.
        :param summary: Explicit summary or None to use the first cleaned handler docstring line.
        :param description: Longer metadata text, with falsey values represented as empty text.
        :param payload_fields: Ordered descriptive field records, not executable validation rules.
        :param tags: Optional ordered tags stringified without deduplication.
        :param transport_stable: Whether query results should be normalized by the Core wire encoder.
        :return: None after replacing the route and metadata under the state lock.
        :raises ValueError: If name normalization yields blank text.
        """
        token = str(name).strip().lower()
        if not token:
            raise ValueError("Query name cannot be blank.")
        with self._state_lock:
            self._query_handlers[token] = handler
            self._query_descriptions[token] = CoreEndpointDescription(
                name=token,
                kind="query",
                summary=str(
                    summary if summary is not None else self._doc_summary(handler)
                ),
                description=str(description or ""),
                payload_fields=self._normalize_payload_fields(payload_fields),
                tags=tuple(str(tag) for tag in (tags or ())),
                transport_stable=bool(transport_stable),
            )

    def subscribe(self, callback: EventSubscriber) -> Callable[[], None]:
        """
        Append a subscriber and return a closure that removes one equal callback registration.

        Duplicates and noncallable values are not rejected. Removal is by list
        equality, not a unique registration token, so repeated closure calls can
        remove multiple duplicate registrations. There is no replay or delivery queue.

        Example:
            >>> unsubscribe = runtime.subscribe(events.append)  # doctest: +SKIP


        :param callback: Subscriber expected to accept one CoreEvent when synchronous delivery occurs.
        :return: Zero-argument closure requesting removal of the first matching callback.
        """
        with self._state_lock:
            self._event_subscribers.append(callback)

        def _unsubscribe() -> None:
            """
            Remove one matching registration of the captured callback from this runtime.

            Repeated calls can remove other equal registrations; in-flight event
            delivery may still hold the callback in a snapshot.

            Example:
                >>> unsubscribe()  # doctest: +SKIP


            :return: None after attempting one callback removal.
            """
            self.unsubscribe(callback)

        return _unsubscribe

    def unsubscribe(self, callback: EventSubscriber) -> None:
        """
        Remove the first equal subscriber under the state lock, ignoring absence.

        Existing delivery snapshots are unaffected; this does not wait for a
        running callback or remove every duplicate registration.

        Example:
            >>> runtime.unsubscribe(events.append)  # doctest: +SKIP


        :param callback: Subscriber compared by list.remove equality against registered callbacks.
        :return: None after one removal or when no equal callback exists.
        """
        with self._state_lock:
            try:
                self._event_subscribers.remove(callback)
            except ValueError:
                pass

    def emit_event(
        self, event_type: str, payload: Mapping[str, Any] | None = None
    ) -> CoreEvent:
        """
        Create an event and synchronously deliver the same object to a snapshot of subscribers.

        Snapshotting occurs under the state lock; delivery occurs outside it on
        the caller's thread. Ordinary callback exceptions are ignored, but event
        construction errors and BaseException subclasses can propagate. Payload
        copying is shallow, so one subscriber can mutate data seen by later ones.

        Example:
            >>> event = runtime.emit_event("example.ready", {"count": 1})  # doctest: +SKIP


        :param event_type: Event routing label stringified by the event factory.
        :param payload: Optional event data shallow-copied by the event factory.
        :return: Event object delivered to the subscriber snapshot, including any subscriber-made nested mutations.
        """
        event = make_core_event(
            core_uuid=self.core_uuid, event_type=event_type, payload=payload
        )
        with self._state_lock:
            subscribers = list(self._event_subscribers)
        for subscriber in subscribers:
            try:
                subscriber(event)
            except Exception:
                # Event handlers are best-effort and must not break runtime flow.
                continue
        return event

    def execute_command(self, command: CoreCommand) -> CoreCommandResult:
        """
        Execute a registered command under the handler lock, wire-normalize stable results, and emit lifecycle events.

        Shutdown/name/handler checks precede command.started and acquiring the
        handler lock; shutdown is not rechecked after waiting. Ordinary handler
        and wire-conversion failures emit command.failed and raise CoreHandlerError
        when error classification succeeds. No rollback or automatic retry occurs.

        Conversion and completion events happen after releasing the handler lock.
        Stability metadata is looked up again after execution, so concurrent
        re-registration can change the result policy. Selected name prefixes emit
        write.completed before command.finished; this describes command completion,
        not proof of durable commit or completion of a submitted background job.

        Example:
            >>> response = runtime.execute_command(CoreCommand("jobs.cancel", {"job_id": "job-1"}))  # doctest: +SKIP


        :param command: Envelope passed unchanged to the handler; only routing uses a normalized name.
        :return: Successful command envelope with the original request/correlation IDs and final result value.
        :raises CoreShutdownError: If shutdown was already flagged when execution began.
        :raises CoreDispatchError: For a blank or unregistered command name.
        :raises CoreHandlerError: For ordinary handler or stable-result conversion failure after error classification.
        """
        if self._shutdown:
            raise CoreShutdownError("Core is shut down.")

        token = str(command.name).strip().lower()
        if not token:
            raise CoreDispatchError("Command name cannot be blank.")

        with self._state_lock:
            handler = self._command_handlers.get(token)
        if handler is None:
            raise CoreDispatchError(
                "Unknown command handler: {!r}".format(command.name)
            )

        self.emit_event(
            "command.started",
            {
                "command_id": command.command_id,
                "name": token,
                "correlation_id": command.correlation_id,
            },
        )

        with self._command_lock:
            try:
                result = handler(self, command)
            except Exception as exc:
                error_code, error_details = core_error_details(exc)
                self.emit_event(
                    "command.failed",
                    {
                        "command_id": command.command_id,
                        "name": token,
                        "error": str(exc),
                        "correlation_id": command.correlation_id,
                    },
                )
                raise CoreHandlerError(
                    "Command handler failed for {!r}: {}".format(token, exc),
                    code=error_code,
                    details=error_details,
                ) from exc
        with self._state_lock:
            description = self._command_descriptions.get(token)
        if description is not None and description.transport_stable:
            try:
                result = to_wire(result)
            except Exception as exc:
                error_code, error_details = core_error_details(exc)
                self.emit_event(
                    "command.failed",
                    {
                        "command_id": command.command_id,
                        "name": token,
                        "error": str(exc),
                        "correlation_id": command.correlation_id,
                    },
                )
                raise CoreHandlerError(
                    "Command result serialization failed for {!r}: {}".format(
                        token,
                        exc,
                    ),
                    code=error_code,
                    details=error_details,
                ) from exc

        write_payload = self._write_completed_payload(token, command)
        if write_payload is not None:
            self.emit_event("write.completed", write_payload)
        self.emit_event(
            "command.finished",
            {
                "command_id": command.command_id,
                "name": token,
                "correlation_id": command.correlation_id,
            },
        )
        return CoreCommandResult(
            ok=True,
            command_id=command.command_id,
            result=result,
            correlation_id=command.correlation_id,
        )

    def _write_completed_payload(
        self, token: str, command: CoreCommand
    ) -> dict[str, Any] | None:
        """
        Select write-completion event metadata by route prefix, without inspecting the command's actual effects.

        Metadata routes additionally copy a present item_id. Generic invoke adds
        target/method when extraction succeeds, but still returns basic metadata
        after extraction failure. Other selected families include job-submitting
        ingest/conversion/backup commands; their event need not mean job completion.

        Example:
            >>> command = CoreCommand("metadata.write", {"item_id": 7}, command_id="request-1")
            >>> CoreRuntime._write_completed_payload(None, "metadata.write", command)["item_id"]
            7


        :param token: Already-normalized command route; this helper stringifies but does not lowercase it.
        :param command: Envelope providing IDs and optional item/invoke metadata.
        :return: Event payload for selected route families, or None when no write event should be emitted.
        """
        payload: dict[str, Any] = {
            "command_id": command.command_id,
            "name": str(token),
            "correlation_id": command.correlation_id,
        }
        if str(token).startswith("metadata."):
            if isinstance(command.payload, Mapping):
                item_id = command.payload.get("item_id")
                if item_id is not None:
                    payload["item_id"] = item_id
            return payload
        if str(token).startswith(
            (
                "catalog.",
                "admin.",
                "storage.",
                "cache.",
                "read-source.",
                "database.",
                "schema.",
                "preferences.",
                "custom-fields.",
                "ingest.",
                "conversion.",
                "backup.",
                "maintenance.",
            )
        ):
            return payload
        if str(token) != "invoke":
            return None
        try:
            target, method, _args, _kwargs = self._extract_invoke_payload(
                command.payload
            )
        except Exception:
            return payload
        payload["target"] = target
        payload["method"] = method
        return payload

    def execute_query(self, query: CoreQuery) -> CoreQueryResult:
        """
        Execute a query under the same lock as commands and convert stable results without command lifecycle events.

        Query classification does not enforce read-only effects. Shutdown is tested
        before waiting on the handler lock, not afterward. A long-running wait query
        can therefore block other command/query handlers. Result conversion occurs
        outside that lock using metadata read after the handler returns.

        Example:
            >>> response = runtime.execute_query(CoreQuery("health"))  # doctest: +SKIP


        :param query: Envelope passed unchanged to the selected handler after normalized-name routing.
        :return: Successful query envelope with request/correlation IDs and the optionally wire-converted result.
        :raises CoreShutdownError: If shutdown is already flagged at the initial check.
        :raises CoreDispatchError: For a blank or unknown query route.
        :raises CoreHandlerError: For ordinary handler or stable-result conversion failure after error classification.
        """
        if self._shutdown:
            raise CoreShutdownError("Core is shut down.")

        token = str(query.name).strip().lower()
        if not token:
            raise CoreDispatchError("Query name cannot be blank.")

        with self._state_lock:
            handler = self._query_handlers.get(token)
        if handler is None:
            raise CoreDispatchError("Unknown query handler: {!r}".format(query.name))

        with self._command_lock:
            try:
                result = handler(self, query)
            except Exception as exc:
                error_code, error_details = core_error_details(exc)
                raise CoreHandlerError(
                    "Query handler failed for {!r}: {}".format(token, exc),
                    code=error_code,
                    details=error_details,
                ) from exc
        with self._state_lock:
            description = self._query_descriptions.get(token)
        if description is not None and description.transport_stable:
            try:
                result = to_wire(result)
            except Exception as exc:
                error_code, error_details = core_error_details(exc)
                raise CoreHandlerError(
                    "Query result serialization failed for {!r}: {}".format(
                        token,
                        exc,
                    ),
                    code=error_code,
                    details=error_details,
                ) from exc

        return CoreQueryResult(
            ok=True,
            query_id=query.query_id,
            result=result,
            correlation_id=query.correlation_id,
        )

    def invoke_command(
        self,
        *,
        target: str,
        method: str,
        args: tuple[Any, ...] = (),
        kwargs: Mapping[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> Any:
        """
        Build a generic invoke command with copied outer argument containers and return its result.

        The built-in invoke route is not transport-stable, so direct calls can
        retain process-owned result objects. Target/method resolution and validation
        happen inside the handler, and command failures follow execute_command's
        wrapping/events. Selecting a command adds no transaction or access policy.

        Example:
            >>> result = runtime.invoke_command(target="database", method="set_pref", args=("view", "list"))  # doctest: +SKIP


        :param target: Hosted target name or alias passed unchanged into the invoke payload.
        :param method: Method spelling passed to later invoke extraction.
        :param args: Positional target values materialized as a tuple.
        :param kwargs: Optional target keyword mapping copied into a dictionary.
        :param correlation_id: Optional caller token retained on the generated command envelope.
        :return: Generic invoke command's result without an additional wrapper-level wire conversion.
        """
        envelope = CoreCommand(
            name="invoke",
            payload={
                "target": target,
                "method": method,
                "args": tuple(args),
                "kwargs": dict(kwargs or {}),
            },
            correlation_id=correlation_id,
        )
        return self.execute_command(envelope).result

    def invoke_query(
        self,
        *,
        target: str,
        method: str,
        args: tuple[Any, ...] = (),
        kwargs: Mapping[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> Any:
        """
        Build a generic invoke query and unwrap its result without imposing a read-only policy.

        The default invoke registration skips wire conversion for direct calls.
        Choosing the query route does not inspect the target method's effects.

        Example:
            >>> row = runtime.invoke_query(target="database", method="get_row_from_id", args=("works", 1))  # doctest: +SKIP


        :param target: Hosted target name or alias retained in the invoke payload.
        :param method: Target method spelling resolved later by the invoke handler.
        :param args: Positional target values converted to a tuple.
        :param kwargs: Optional target keyword values shallow-copied into a dictionary.
        :param correlation_id: Optional caller token stored on the generated query envelope.
        :return: Generic invoke query result, including process-owned values for the default unstable route.
        """
        envelope = CoreQuery(
            name="invoke",
            payload={
                "target": target,
                "method": method,
                "args": tuple(args),
                "kwargs": dict(kwargs or {}),
            },
            correlation_id=correlation_id,
        )
        return self.execute_query(envelope).result

    def _target_bindings(self) -> list[tuple[str, tuple[str, ...], str, Any]]:
        """
        Enumerate the hosted library and available database/storage facades with canonical names and aliases.

        Database discovery uses only library.database, not CoreServices' possible
        library.db fallback. Database attribute errors propagate; all ordinary
        storage attribute errors suppress that target instead. Reading these
        properties can itself resolve services. The list is rebuilt on every call.

        Example:
            >>> bindings = runtime._target_bindings()  # doctest: +SKIP


        :return: Ordered canonical-name/alias-tuple/summary/object records for library, then optional database and storage.
        """
        bindings: list[tuple[str, tuple[str, ...], str, Any]] = [
            ("library", ("library", "lib"), "Hosted library facade.", self.library),
        ]
        db = getattr(self.library, "database", None)
        if db is not None:
            bindings.append(
                ("database", ("database", "db"), "Hosted database facade.", db)
            )
        try:
            storage = getattr(self.library, "storage", None)
        except Exception:
            storage = None
        if storage is not None:
            bindings.append(
                (
                    "storage",
                    ("storage", "stores", "store_manager"),
                    "Hosted storage facade.",
                    storage,
                )
            )
        return bindings

    def _normalize_target_filter(self, target: str | None) -> str | None:
        """
        Resolve a nonblank case-normalized target selector to a currently available canonical target name.

        None or blank text means no filter. Availability is read anew, so known
        spellings for an absent database/storage target still fail resolution.

        Example:
            >>> CoreRuntime._normalize_target_filter(None, " ") is None
            True


        :param target: Optional target/alias stringified, stripped, and lowercased.
        :return: Canonical available target name, or None for no requested filter.
        :raises CoreDispatchError: If nonblank text does not identify an available target.
        """
        if target is None:
            return None
        token = str(target).strip().lower()
        if not token:
            return None
        for canonical_name, aliases, _summary, _obj in self._target_bindings():
            if token == canonical_name or token in aliases:
                return canonical_name
        raise CoreDispatchError("Unknown target filter: {!r}".format(target))

    def _resolve_target(self, token: str) -> Any:
        """
        Resolve a target name or alias to the current hosted facade without caching it.

        Unlike the introspection filter helper, blank text is not an all-target
        sentinel here and fails resolution.

        Example:
            >>> database = runtime._resolve_target("db")  # doctest: +SKIP


        :param token: Target label stringified, stripped, and lowercased before matching.
        :return: Available library/database/storage object selected by canonical name or alias.
        :raises CoreDispatchError: If the normalized label has no available target binding.
        """
        key = str(token).strip().lower()
        for canonical_name, aliases, _summary, obj in self._target_bindings():
            if key == canonical_name or key in aliases:
                return obj
        raise CoreDispatchError("Unknown invoke target: {!r}".format(token))

    @staticmethod
    def _describe_parameter(parameter: inspect.Parameter) -> CoreParameterDescription:
        """
        Convert inspect.Parameter metadata without validating whether its shape implies a required call argument.

        Requiredness means only absence of a default, so variadic parameters can
        be advertised as required. Missing/default-None both render as None, with
        the required flag distinguishing them. Defaults are retained by reference
        until the description record renders them.

        Example:
            >>> parameter = inspect.Parameter("limit", inspect.Parameter.KEYWORD_ONLY, default=20, annotation=int)
            >>> description = CoreRuntime._describe_parameter(parameter)
            >>> description.name, description.kind, description.required, description.default
            ('limit', 'keyword_only', False, 20)


        :param parameter: Inspected callable parameter supplying name, kind, default, and annotation.
        :return: Description record with lowercase kind text and stringified annotation when present.
        """
        default = (
            None if parameter.default is inspect.Signature.empty else parameter.default
        )
        annotation = (
            None
            if parameter.annotation is inspect.Signature.empty
            else str(parameter.annotation)
        )
        return CoreParameterDescription(
            name=str(parameter.name),
            kind=str(parameter.kind.name).lower(),
            required=parameter.default is inspect.Signature.empty,
            default=default,
            annotation=annotation,
        )

    def _describe_target_method(
        self, method_name: str, method: Any
    ) -> CoreMethodDescription:
        """
        Describe callable documentation, name-based write classification, and any inspectable signature.

        Signature inspection failures produce empty parameter/return metadata.
        Every parameter named self is omitted regardless of actual binding; cls is
        not specially omitted. Docstring and later parameter-conversion failures
        are outside the signature catch block and can propagate.

        Example:
            >>> description = runtime._describe_target_method("get_row", database.get_row_from_id)  # doctest: +SKIP


        :param method_name: Advertised method label used by the shared write-name heuristic.
        :param method: Callable-like object inspected for cleaned docs and optional signature metadata.
        :return: Method description record without invoking the target method itself.
        """
        summary = self._doc_summary(method)
        description = inspect.getdoc(method) or ""
        parameters: tuple[CoreParameterDescription, ...] = ()
        return_annotation: str | None = None
        try:
            signature = inspect.signature(method)
        except Exception:
            signature = None
        if signature is not None:
            params = []
            for parameter in signature.parameters.values():
                if parameter.name == "self":
                    continue
                params.append(self._describe_parameter(parameter))
            parameters = tuple(params)
            if signature.return_annotation is not inspect.Signature.empty:
                return_annotation = str(signature.return_annotation)
        return CoreMethodDescription(
            name=str(method_name),
            write=looks_like_write_method(method_name),
            summary=summary,
            description=description,
            parameters=parameters,
            return_annotation=return_annotation,
        )

    def _describe_target(
        self,
        *,
        target_name: str,
        aliases: tuple[str, ...],
        summary: str,
        target_obj: Any,
    ) -> CoreTargetDescription:
        """
        Enumerate sorted nonprivate callable attributes and construct a hosted-target description.

        Attribute access is real and may have effects; ordinary getattr failures
        skip that attribute. Underscore-prefixed names and noncallables are omitted.
        Failed dir, docstring, or method-description operations are not swallowed.
        Omission from introspection is not an invoke authorization restriction.

        Example:
            >>> description = runtime._describe_target(target_name="database", aliases=("db",), summary="Database", target_obj=database)  # doctest: +SKIP


        :param target_name: Canonical advertised target name retained in the description.
        :param aliases: Ordered target aliases converted to a tuple.
        :param summary: Supplied target summary retained separately from the object's full docstring.
        :param target_obj: Hosted facade whose public callable attributes and documentation are inspected.
        :return: Target description with methods ordered by attribute name.
        """
        methods: list[CoreMethodDescription] = []
        for attribute_name in sorted(dir(target_obj)):
            if attribute_name.startswith("_"):
                continue
            try:
                attribute = getattr(target_obj, attribute_name)
            except Exception:
                continue
            if not callable(attribute):
                continue
            methods.append(self._describe_target_method(attribute_name, attribute))
        return CoreTargetDescription(
            name=target_name,
            aliases=tuple(aliases),
            summary=summary,
            description=inspect.getdoc(target_obj) or "",
            methods=tuple(methods),
        )

    def describe_api(
        self, *, include_targets: bool = True, target: str | None = None
    ) -> dict[str, Any]:
        """
        Snapshot sorted endpoint descriptions and combine them with identity, service status, and optional target introspection.

        Target validation occurs even when target output is disabled. The filter
        affects only target descriptions, not command/query lists. Endpoint maps
        are rendered under the state lock, but service/target inspection happens
        afterward, so this is not one atomic runtime-state snapshot. Description
        reads can resolve services and execute property getters.

        Example:
            >>> description = runtime.describe_api(include_targets=False)  # doctest: +SKIP


        :param include_targets: Truth-tested choice to include a targets key with dynamic method descriptions.
        :param target: Optional available target/alias filter; blank text and None mean all targets.
        :return: New API dictionary with identity, service descriptions, named endpoints, and optional targets.
        :raises CoreDispatchError: If a nonblank target selector does not resolve, even when include_targets is false.
        """
        normalized_target = self._normalize_target_filter(target)
        with self._state_lock:
            commands = [
                self._command_descriptions[name].to_dict()
                for name in sorted(self._command_descriptions)
            ]
            queries = [
                self._query_descriptions[name].to_dict()
                for name in sorted(self._query_descriptions)
            ]

        payload: dict[str, Any] = {
            "core_uuid": self.core_uuid,
            "core_version": self.core_version,
            "api_version": self.api_version,
            "services": self.services.describe(),
            "commands": commands,
            "queries": queries,
        }
        if include_targets:
            targets = []
            for target_name, aliases, summary, obj in self._target_bindings():
                if normalized_target is not None and target_name != normalized_target:
                    continue
                targets.append(
                    self._describe_target(
                        target_name=target_name,
                        aliases=aliases,
                        summary=summary,
                        target_obj=obj,
                    ).to_dict()
                )
            payload["targets"] = targets
        return payload

    @staticmethod
    def _extract_invoke_payload(
        payload: Mapping[str, Any],
    ) -> tuple[str, str, tuple[Any, ...], dict[str, Any]]:
        """
        Coerce generic invoke fields into target/method text, positional tuple, and keyword dictionary.

        Outer containers are copied before blank-name checks. Strings used as args
        become character tuples, and values such as None used as names stringify
        rather than count as absent. No argument-signature or target-access check
        is performed by this extraction step.

        Example:
            >>> CoreRuntime._extract_invoke_payload({"target": " db ", "method": "get_row", "args": [1]})
            ('db', 'get_row', (1,), {})


        :param payload: Mapping with target/method and optional args/kwargs fields.
        :return: Stripped target and method strings, positional tuple, and shallow keyword dictionary.
        :raises CoreDispatchError: If either stringified/stripped target or method is blank.
        """
        target = str(payload.get("target", "")).strip()
        method = str(payload.get("method", "")).strip()
        args = tuple(payload.get("args", ()))
        kwargs = dict(payload.get("kwargs", {}) or {})
        if not target:
            raise CoreDispatchError("Invoke payload missing `target`.")
        if not method:
            raise CoreDispatchError("Invoke payload missing `method`.")
        return target, method, args, kwargs

    def _invoke(self, payload: Mapping[str, Any]) -> Any:
        """
        Resolve a hosted facade and call its named attribute with extracted arguments.

        Only existence/callability is checked. Underscore-prefixed methods are not
        blocked, and choosing an invoke query rather than command does not constrain
        effects. Property lookup and target exceptions propagate to the enclosing
        dispatch wrapper; this helper itself adds no rollback or wire conversion.

        Example:
            >>> row = runtime._invoke({"target": "database", "method": "get_row_from_id", "args": ["works", 1]})  # doctest: +SKIP


        :param payload: Generic invoke mapping decoded by _extract_invoke_payload.
        :return: Target callable's result unchanged.
        :raises CoreDispatchError: If names are absent, target resolution fails, or the attribute is missing/noncallable.
        """
        target_name, method_name, args, kwargs = self._extract_invoke_payload(payload)
        target = self._resolve_target(target_name)
        method = getattr(target, method_name, None)
        if method is None or not callable(method):
            raise CoreDispatchError(
                "Target {!r} has no callable method {!r}.".format(
                    target_name, method_name
                )
            )
        return method(*args, **kwargs)

    def _handle_invoke_command(
        self, runtime: "CoreRuntime", command: CoreCommand
    ) -> Any:
        """
        Execute a command envelope's generic invoke payload on this bound runtime.

        Example:
            >>> result = runtime._handle_invoke_command(runtime, command)  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument; invocation uses the bound instance.
        :param command: Envelope carrying target, method, and optional target arguments.
        :return: Generic target invocation result before outer command processing.
        """
        del runtime
        return self._invoke(command.payload)

    def _handle_metadata_write_command(
        self, runtime: "CoreRuntime", command: CoreCommand
    ) -> dict[str, Any]:
        """
        Apply an item-centered metadata payload and any required cache reconciliation.

        Example:
            >>> receipt = runtime._handle_metadata_write_command(runtime, command)  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument; this bound instance owns the workflow.
        :param command: Envelope carrying item_id, values, and optional metadata-write policy fields.
        :return: Metadata write receipt augmented with cache reconciliation status.
        """
        del runtime
        return self._execute_metadata_write(command.payload)

    def _handle_metadata_tags_replace_command(
        self, runtime: "CoreRuntime", command: CoreCommand
    ) -> dict[str, Any]:
        """
        Force authoritative replacement of the tags field through the shared metadata workflow.

        Example:
            >>> receipt = runtime._handle_metadata_tags_replace_command(runtime, command)  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument; the bound runtime supplies services.
        :param command: Envelope with item_id and tags in values or the top-level payload.
        :return: Tags-replacement receipt with cache reconciliation status.
        """
        del runtime
        return self._execute_metadata_field_replace(command.payload, field_name="tags")

    def _handle_metadata_labels_replace_command(
        self, runtime: "CoreRuntime", command: CoreCommand
    ) -> dict[str, Any]:
        """
        Force authoritative replacement of the labels field using the shared item-centered write path.

        Example:
            >>> receipt = runtime._handle_metadata_labels_replace_command(runtime, command)  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument; this instance provides the database and services.
        :param command: Envelope with item_id and labels supplied in values or the outer payload.
        :return: Labels-replacement receipt augmented with cache status.
        """
        del runtime
        return self._execute_metadata_field_replace(
            command.payload, field_name="labels"
        )

    def _handle_metadata_genre_replace_command(
        self, runtime: "CoreRuntime", command: CoreCommand
    ) -> dict[str, Any]:
        """
        Replace only genre through the authoritative shared metadata-field adapter.

        Example:
            >>> receipt = runtime._handle_metadata_genre_replace_command(runtime, command)  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument in the common handler signature.
        :param command: Envelope carrying item_id and replacement genre values.
        :return: Genre-replacement write receipt with reconciliation metadata.
        """
        del runtime
        return self._execute_metadata_field_replace(command.payload, field_name="genre")

    def _handle_metadata_series_replace_command(
        self, runtime: "CoreRuntime", command: CoreCommand
    ) -> dict[str, Any]:
        """
        Replace the series field authoritatively while retaining other metadata policy options from the payload.

        Example:
            >>> receipt = runtime._handle_metadata_series_replace_command(runtime, command)  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument; this method uses its bound runtime.
        :param command: Envelope with item_id and series replacement values.
        :return: Series-replacement receipt with cache reconciliation status.
        """
        del runtime
        return self._execute_metadata_field_replace(
            command.payload, field_name="series"
        )

    def _handle_metadata_identifiers_replace_command(
        self,
        runtime: "CoreRuntime",
        command: CoreCommand,
    ) -> dict[str, Any]:
        """
        Apply authoritative identifier-field replacement through the common metadata write workflow.

        Example:
            >>> receipt = runtime._handle_metadata_identifiers_replace_command(runtime, command)  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument; services come from this bound instance.
        :param command: Envelope carrying item_id and identifiers in values or the outer payload.
        :return: Identifier-replacement receipt with cache status.
        """
        del runtime
        return self._execute_metadata_field_replace(
            command.payload, field_name="identifiers"
        )

    def _execute_metadata_field_replace(
        self, payload: Mapping[str, Any], *, field_name: str
    ) -> dict[str, Any]:
        """
        Force one requested metadata field and replace=true, preferring its nested value over a top-level alias.

        Existing values entries win even when None. Other nested values remain in
        the copied mapping, but fields is overridden to the selected one-element
        tuple. A missing field is rejected, rather than interpreted as an implicit
        request to clear data. Actual value validation belongs to the write workflow.

        Example:
            >>> receipt = runtime._execute_metadata_field_replace({"item_id": 7, "tags": []}, field_name="tags")  # doctest: +SKIP


        :param payload: Metadata request containing item identity, nested/outer replacement data, and optional policy fields.
        :param field_name: Metadata field forced into the downstream fields selection.
        :return: Shared metadata-write receipt for the explicit authoritative replacement.
        :raises CoreDispatchError: If neither nested values nor outer payload supplies the requested field.
        """
        values = dict(payload.get("values") or {})
        if field_name not in values:
            if field_name not in payload:
                raise CoreDispatchError(
                    "Metadata {!r} replace payload missing `{}`.".format(
                        field_name, field_name
                    )
                )
            values[field_name] = payload.get(field_name)
        return self._execute_metadata_write(
            {
                **dict(payload),
                "values": values,
                "fields": (field_name,),
                "replace": True,
            }
        )

    def _execute_metadata_write(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """
        Validate item/value presence, coerce write options, run the WEMI metadata workflow, and reconcile only changed receipts.

        Item IDs use int without a local positive-range check; replace/mark_dirty
        use Python truthiness. Kind and target-level falsey values default to liuxin
        and work. Unchanged writes still return cache configuration with reconciled
        false. Changed writes may have committed before reconciliation raises, and
        this adapter does not retry or undo them.

        Example:
            >>> receipt = runtime._execute_metadata_write({"item_id": 7, "values": {"title": "Example"}})  # doctest: +SKIP


        :param payload: Request requiring item_id and mapping-valued values, plus optional fields/kind/replace/target_level/mark_dirty.
        :return: Workflow receipt augmented or replaced with current cache reconciliation metadata.
        :raises CoreDispatchError: If item_id is absent or values is not a mapping.
        """
        from LiuXin_alpha.metadata.write_workflows import (
            write_wemi_metadata_values,
        )

        if "item_id" not in payload:
            raise CoreDispatchError("Metadata write payload missing `item_id`.")
        values = payload.get("values")
        if not isinstance(values, Mapping):
            raise CoreDispatchError(
                "Metadata write payload `values` must be an object."
            )
        result = write_wemi_metadata_values(
            self.library.database,
            item_id=int(payload["item_id"]),
            values=dict(values),
            fields=payload.get("fields"),
            kind=str(payload.get("kind") or "liuxin"),
            replace=bool(payload.get("replace", False)),
            target_level=str(payload.get("target_level") or "work"),
            mark_dirty=bool(payload.get("mark_dirty", True)),
        )
        if bool(result.get("changed", False)):
            return self.services.reconcile(result)
        return {
            **result,
            "cache": {
                "configured": self.services.cache is not None,
                "reconciled": False,
            },
        }

    def _handle_shutdown_command(
        self, runtime: "CoreRuntime", command: CoreCommand
    ) -> int:
        """
        Ignore command contents and request shutdown of this bound runtime.

        Example:
            >>> exit_code = runtime._handle_shutdown_command(runtime, CoreCommand("shutdown"))  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument.
        :param command: Ignored envelope; shutdown needs no payload fields here.
        :return: Process-style status from shutdown, normally zero.
        """
        del runtime, command
        return self.shutdown()

    def _handle_invoke_query(self, runtime: "CoreRuntime", query: CoreQuery) -> Any:
        """
        Execute a query envelope's generic invoke payload without checking target side effects.

        Example:
            >>> result = runtime._handle_invoke_query(runtime, query)  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument; target resolution uses this instance.
        :param query: Envelope carrying generic target/method/argument fields.
        :return: Target invocation result before outer query result processing.
        """
        del runtime
        return self._invoke(query.payload)

    def _handle_health_query(
        self, runtime: "CoreRuntime", query: CoreQuery
    ) -> dict[str, Any]:
        """
        Describe runtime identity, shutdown flag, service types/status, and sorted registered route names.

        This is not an active database/storage readiness probe. Service description
        can resolve lazy application preferences or raise. Handler dictionaries are
        inspected without acquiring the registration state lock in this method.

        Example:
            >>> status = runtime._handle_health_query(runtime, CoreQuery("health"))  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument; state comes from the bound instance.
        :param query: Ignored envelope, including its payload.
        :return: Current identity/service/registration mapping without a global atomic-state guarantee.
        """
        del runtime, query
        return {
            "core_uuid": self.core_uuid,
            "core_version": self.core_version,
            "api_version": self.api_version,
            "shutdown": self.is_shutdown,
            "services": self.services.describe(),
            "registered_command_handlers": sorted(self._command_handlers.keys()),
            "registered_query_handlers": sorted(self._query_handlers.keys()),
        }

    def _handle_api_describe_query(
        self, runtime: "CoreRuntime", query: CoreQuery
    ) -> dict[str, Any]:
        """
        Copy description options from a query, truth-convert inclusion, and delegate target validation/introspection.

        A nonempty string such as false still enables target output; this path does
        not use the HTTP GET route's explicit boolean-token parser.

        Example:
            >>> description = runtime._handle_api_describe_query(runtime, CoreQuery("api.describe", {"include_targets": False}))  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument in the common query-handler signature.
        :param query: Envelope carrying optional include_targets and target fields.
        :return: API description after boolean/string option coercion and normal target resolution.
        """
        del runtime
        payload = dict(query.payload or {})
        include_targets = bool(payload.get("include_targets", True))
        target = payload.get("target", None)
        return self.describe_api(
            include_targets=include_targets,
            target=None if target is None else str(target),
        )

    @staticmethod
    def _preview_value(value: Any, *, max_len: int = 400) -> str:
        """
        Render repr in full, then shorten displayed text with an ellipsis when it exceeds the requested character limit.

        This does not bound repr computation/allocation or sanitize its contents.
        Limits below three still produce a three-character ellipsis when truncation
        is required; the limit is not measured in UTF-8 bytes or display cells.

        Example:
            >>> CoreRuntime._preview_value("abcdef", max_len=6)
            "'ab..."
            >>> CoreRuntime._preview_value("x", max_len=0)
            '...'


        :param value: Arbitrary result value whose repr is used as preview text.
        :param max_len: Requested character budget, with space for three dots when truncating.
        :return: Full repr if within budget, otherwise its shortened prefix plus three dots.
        """
        text = repr(value)
        if len(text) <= max_len:
            return text
        return text[: max(0, max_len - 3)] + "..."

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        """
        Convert a non-None value to float, returning None for ordinary conversion failures.

        Negative and nonfinite floats are not rejected. Later wire conversion may
        still reject a nonfinite value that this helper accepts.

        Example:
            >>> CoreRuntime._safe_float("1.5"), CoreRuntime._safe_float("invalid")
            (1.5, None)


        :param value: Optional numeric-like value to convert without a range constraint.
        :return: Converted float, or None for absent/unconvertible values.
        """
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None

    @classmethod
    def _serialize_job_execution(cls, execution: Any) -> dict[str, Any] | None:
        """
        Describe execution flags, traceback/log paths, and a repr preview instead of exposing the raw result object.

        Missing attributes use false/empty defaults. Preview truncation limits
        displayed characters, not repr computation; traceback/log fields are not
        length-limited or sanitized here. Attribute access failures propagate.

        Example:
            >>> from types import SimpleNamespace
            >>> CoreRuntime._serialize_job_execution(SimpleNamespace(ok=True, result=7))["result_preview"]
            '7'


        :param execution: Optional attribute-based JobExecution-like value.
        :return: New execution-summary dictionary or None if no execution object was supplied.
        """
        if execution is None:
            return None
        return {
            "ok": bool(getattr(execution, "ok", False)),
            "timed_out": bool(getattr(execution, "timed_out", False)),
            "aborted": bool(getattr(execution, "aborted", False)),
            "traceback": str(getattr(execution, "traceback", "") or ""),
            "log_path": str(getattr(execution, "log_path", "") or ""),
            # Intentionally a preview for JSON safety over remote transports.
            "result_preview": cls._preview_value(getattr(execution, "result", None)),
        }

    @classmethod
    def _serialize_managed_job(cls, info: Any) -> dict[str, Any]:
        """
        Summarize a managed job's identity, state, timing, request shape, and optional execution preview.

        Request argument values/environment contents are omitted, but names, cwd,
        log/traceback text, and result repr can still expose supplied data. Argument
        counting materializes a tuple and keyword keys are stringified/sorted.
        Timings use permissive float conversion; missing values become None.
        This is not a validator or a guarantee that every returned float is wire-safe.

        Example:
            >>> from types import SimpleNamespace
            >>> payload = CoreRuntime._serialize_managed_job(SimpleNamespace(job_id="job-1", state="running"))
            >>> payload["job_id"], payload["state"], payload["execution"]
            ('job-1', 'running', None)


        :param info: Attribute-based managed-job snapshot; missing attributes receive descriptive defaults.
        :return: New summary dictionary with epoch-second timestamps, duration/timeout seconds, request shape, and execution summary.
        """
        request = getattr(info, "request", None)
        request_payload: dict[str, Any] = {}
        if request is not None:
            request_kwargs = getattr(request, "kwargs", {}) or {}
            request_payload = {
                "module_name": str(getattr(request, "module_name", "") or ""),
                "function_name": str(getattr(request, "function_name", "") or ""),
                "args_count": len(tuple(getattr(request, "args", ()) or ())),
                "kwargs_keys": sorted(str(key) for key in request_kwargs.keys()),
                "module_is_source_code": bool(
                    getattr(request, "module_is_source_code", False)
                ),
                "cwd": str(getattr(request, "cwd", "") or ""),
                "has_env": bool(getattr(request, "env", None)),
            }
        return {
            "job_id": str(getattr(info, "job_id", "") or ""),
            "label": str(getattr(info, "label", "") or ""),
            "retry_of_job_id": (
                None
                if getattr(info, "retry_of_job_id", None) in (None, "")
                else str(getattr(info, "retry_of_job_id"))
            ),
            "state": str(getattr(info, "state", "") or ""),
            "backend_name": str(getattr(info, "backend_name", "") or ""),
            "submitted_at": cls._safe_float(getattr(info, "submitted_at", None)),
            "started_at": cls._safe_float(getattr(info, "started_at", None)),
            "finished_at": cls._safe_float(getattr(info, "finished_at", None)),
            "duration_s": cls._safe_float(getattr(info, "duration_s", None)),
            "timeout_s": cls._safe_float(getattr(info, "timeout_s", None)),
            "no_output": bool(getattr(info, "no_output", False)),
            "log_path": str(getattr(info, "log_path", "") or ""),
            "request": request_payload,
            "execution": cls._serialize_job_execution(getattr(info, "execution", None)),
        }

    @staticmethod
    def _normalize_job_id(payload: Mapping[str, Any], *, field: str = "job_id") -> str:
        """
        Stringify and strip a selected job identifier, rejecting only missing or blank text.

        Explicit None becomes the literal string None rather than a missing value.
        This does not check UUID syntax or manager membership.

        Example:
            >>> CoreRuntime._normalize_job_id({"job_id": " job-1 "})
            'job-1'


        :param payload: Mapping containing the requested identifier field.
        :param field: Key to read, also used in the missing-identifier error message.
        :return: Nonblank stripped identifier text.
        :raises CoreDispatchError: If the selected field is absent or stringifies to blank text.
        """
        job_id = str(payload.get(field, "")).strip()
        if not job_id:
            raise CoreDispatchError("`{}` is required.".format(field))
        return job_id

    @staticmethod
    def _normalize_job_states(
        payload: Mapping[str, Any],
    ) -> set[JobState] | None:
        """
        Parse plural/singular job-state filters and reject tokens outside the fixed supported vocabulary.

        A present states key overrides state even when None. Strings accept comma
        or semicolon separators; only list/tuple/set containers are otherwise
        accepted. Tokens are stringified, stripped, lowercased, and deduplicated.
        None means no filter; blank input returns an empty set, which the current
        in-memory manager interprets as matching no states.

        Example:
            >>> sorted(CoreRuntime._normalize_job_states({"states": " RUNNING; failed,running "}))
            ['failed', 'running']
            >>> CoreRuntime._normalize_job_states({"state": ""})
            set()


        :param payload: Mapping with optional states or legacy state field.
        :return: Set of validated state strings, empty set for an empty filter, or None for no filter.
        :raises CoreDispatchError: For an unsupported container type or unknown normalized state tokens.
        """
        raw_states = payload.get("states", payload.get("state", None))
        if raw_states is None:
            return None
        raw_values: set[str] = set()
        if isinstance(raw_states, str):
            for part in raw_states.replace(";", ",").split(","):
                token = part.strip().lower()
                if token:
                    raw_values.add(token)
        elif isinstance(raw_states, (list, tuple, set)):
            for part in raw_states:
                token = str(part).strip().lower()
                if token:
                    raw_values.add(token)
        else:
            raise CoreDispatchError("`states` must be a string or sequence of strings.")
        allowed = {
            "pending",
            "running",
            "succeeded",
            "failed",
            "timed_out",
            "aborted",
            "cancelled",
        }
        invalid = raw_values - allowed
        if invalid:
            raise CoreDispatchError(
                "Unknown job states: {}.".format(", ".join(sorted(invalid)))
            )
        return {cast(JobState, value) for value in raw_values}

    @staticmethod
    def _normalize_optional_limit(
        payload: Mapping[str, Any], *, key: str = "limit"
    ) -> int | None:
        """
        Read an optional positive integer limit, using int coercion rather than strict integer-type validation.

        Missing/None means no limit. Values such as booleans or fractional numbers
        can be accepted according to int conversion before the minimum check.

        Example:
            >>> CoreRuntime._normalize_optional_limit({"limit": "20"}), CoreRuntime._normalize_optional_limit({})
            (20, None)


        :param payload: Mapping containing an optional limit-like field.
        :param key: Limit field name used for lookup and validation errors.
        :return: Integer of at least one, or None for a missing/None field.
        :raises CoreDispatchError: If conversion fails or the converted value is below one.
        """
        if key not in payload:
            return None
        raw = payload.get(key)
        if raw is None:
            return None
        try:
            value = int(raw)
        except Exception as exc:
            raise CoreDispatchError("`{}` must be an integer.".format(key)) from exc
        if value < 1:
            raise CoreDispatchError("`{}` must be >= 1.".format(key))
        return value

    @staticmethod
    def _normalize_offset(payload: Mapping[str, Any], *, key: str = "offset") -> int:
        """
        Read a nonnegative integer-coerced offset, treating missing/None as zero.

        Example:
            >>> CoreRuntime._normalize_offset({"offset": "3"}), CoreRuntime._normalize_offset({})
            (3, 0)


        :param payload: Mapping containing an optional offset field.
        :param key: Field name used for lookup and error text.
        :return: Converted offset of at least zero, with zero as the absence default.
        :raises CoreDispatchError: If int conversion fails or yields a negative value.
        """
        if key not in payload:
            return 0
        raw = payload.get(key)
        if raw is None:
            return 0
        try:
            value = int(raw)
        except Exception as exc:
            raise CoreDispatchError("`{}` must be an integer.".format(key)) from exc
        if value < 0:
            raise CoreDispatchError("`{}` must be >= 0.".format(key))
        return value

    @staticmethod
    def _normalize_wait_timeout(payload: Mapping[str, Any]) -> float | None:
        """
        Select timeout_s before legacy timeout and convert numeric values or recognized disabling tokens.

        A present timeout_s wins even when None. None/off/disable/disabled text
        means no supplied deadline. Numeric conversion accepts negative and
        nonfinite values without additional validation; blank text is invalid.

        Example:
            >>> CoreRuntime._normalize_wait_timeout({"timeout": "off"}) is None
            True
            >>> CoreRuntime._normalize_wait_timeout({"timeout_s": "2.5"})
            2.5


        :param payload: Mapping with optional preferred timeout_s or legacy timeout field.
        :return: Requested float timeout in seconds or None for absence/disabling tokens.
        :raises CoreDispatchError: If a non-disabled value cannot be converted to float.
        """
        if "timeout_s" in payload:
            raw = payload.get("timeout_s")
        elif "timeout" in payload:
            raw = payload.get("timeout")
        else:
            return None
        if raw is None:
            return None
        token = str(raw).strip().lower()
        if token in {"none", "off", "disable", "disabled"}:
            return None
        try:
            return float(raw)
        except Exception as exc:
            raise CoreDispatchError(
                "`timeout_s` must be a float, int, or none-like token."
            ) from exc

    def _handle_jobs_list_query(
        self, runtime: "CoreRuntime", query: CoreQuery
    ) -> dict[str, Any]:
        """
        Validate listing options, fetch the manager's full filtered snapshot list, then slice and serialize the requested window.

        Total counts matches before pagination, and manager order is preserved.
        Returned states is empty both for no filter and an explicitly empty filter,
        although those inputs have different selection semantics in the manager.

        Example:
            >>> payload = runtime._handle_jobs_list_query(runtime, CoreQuery("jobs.list", {"limit": 20}))  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument; this instance supplies the manager.
        :param query: Envelope carrying optional state(s), positive limit, and nonnegative offset.
        :return: Serialized job window plus pre-window total, effective pagination, and normalized state tokens.
        """
        del runtime
        payload = dict(query.payload or {})
        states = self._normalize_job_states(payload)
        limit = self._normalize_optional_limit(payload, key="limit")
        offset = self._normalize_offset(payload, key="offset")

        jobs = self.job_manager.list(states=states)
        total = len(jobs)
        if limit is None:
            window = jobs[offset:]
        else:
            window = jobs[offset : offset + limit]
        return {
            "jobs": [self._serialize_managed_job(info) for info in window],
            "total": total,
            "offset": offset,
            "limit": limit,
            "states": sorted(states) if states else [],
        }

    def _handle_jobs_get_query(
        self, runtime: "CoreRuntime", query: CoreQuery
    ) -> dict[str, Any]:
        """
        Fetch and serialize one job, translating a manager KeyError into an unknown-job dispatch failure.

        Example:
            >>> payload = runtime._handle_jobs_get_query(runtime, CoreQuery("jobs.get", {"job_id": "job-1"}))  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument in the common handler signature.
        :param query: Envelope with a job_id normalized to nonblank text.
        :return: Dictionary containing the serialized job under job.
        :raises CoreDispatchError: If the ID is blank or manager lookup raises KeyError.
        """
        del runtime
        payload = dict(query.payload or {})
        job_id = self._normalize_job_id(payload)
        try:
            info = self.job_manager.get(job_id)
        except KeyError as exc:
            raise CoreDispatchError("Unknown job id: {!r}".format(job_id)) from exc
        return {
            "job": self._serialize_managed_job(info),
        }

    def _handle_jobs_wait_query(
        self, runtime: "CoreRuntime", query: CoreQuery
    ) -> dict[str, Any]:
        """
        Delegate waiting to the manager and serialize the observed job, which may still be nonterminal after timeout.

        When dispatched normally, this wait holds the shared command/query handler
        lock. No second worker timeout or success-state assertion is added here.

        Example:
            >>> payload = runtime._handle_jobs_wait_query(runtime, CoreQuery("jobs.wait", {"job_id": "job-1", "timeout_s": 1}))  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument; the bound runtime supplies the manager.
        :param query: Envelope containing job_id and optional timeout_s/timeout policy.
        :return: Dictionary with the manager's observed job snapshot after waiting.
        :raises CoreDispatchError: For invalid identifier/timeout or a manager KeyError for the job.
        """
        del runtime
        payload = dict(query.payload or {})
        job_id = self._normalize_job_id(payload)
        timeout_s = self._normalize_wait_timeout(payload)
        try:
            info = self.job_manager.wait(job_id, timeout=timeout_s)
        except KeyError as exc:
            raise CoreDispatchError("Unknown job id: {!r}".format(job_id)) from exc
        return {
            "job": self._serialize_managed_job(info),
        }

    def _resolve_library_database_path(self) -> str | None:
        """
        Read a nonblank database_path from library.database.metadata without probing the path or using a db alias.

        Example:
            >>> path = runtime._resolve_library_database_path()  # doctest: +SKIP


        :return: Stripped database path/connection-selector text, or None for missing database/metadata/value.
        """
        db = getattr(self.library, "database", None)
        metadata = getattr(db, "metadata", None) if db is not None else None
        if not isinstance(metadata, Mapping):
            return None
        value = metadata.get("database_path")
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _resolve_library_db_type(self) -> str:
        """
        Read library.database.type as stripped text, defaulting to SQLite for missing or falsey values.

        This does not consult a library.db alias or validate a driver selector.

        Example:
            >>> db_type = runtime._resolve_library_db_type()  # doctest: +SKIP


        :return: Nonblank advertised driver type or the literal SQLite fallback.
        """
        db = getattr(self.library, "database", None)
        value = getattr(db, "type", None) if db is not None else None
        text = str(value or "").strip()
        return text or "SQLite"

    @staticmethod
    def _normalize_sync_job_kwargs(payload: Mapping[str, Any]) -> dict[str, Any]:
        """
        Require mapping-valued sync_kwargs when supplied and return a shallow mutable copy.

        Missing sync_kwargs becomes empty rather than failing immediately despite
        the endpoint's required-field metadata. Worker argument completeness is
        not validated by this helper.

        Example:
            >>> CoreRuntime._normalize_sync_job_kwargs({"sync_kwargs": {"mode": "local"}})
            {'mode': 'local'}


        :param payload: Outer command payload with optional sync_kwargs mapping.
        :return: New worker-keyword dictionary, empty if the field is absent.
        :raises CoreDispatchError: If an explicitly supplied sync_kwargs value is not a mapping.
        """
        raw = payload.get("sync_kwargs", {})
        if not isinstance(raw, Mapping):
            raise CoreDispatchError("`sync_kwargs` must be a mapping.")
        return dict(raw)

    def _handle_sync_store_start_command(
        self, runtime: "CoreRuntime", command: CoreCommand
    ) -> dict[str, Any]:
        """
        Fill blank database options, submit the importable sync worker, and emit submission metadata without waiting.

        Database fallback checks stringified blankness, so explicit None values
        do not trigger fallback. Worker keyword completeness is deferred to execution.
        Missing/None job_timeout_s becomes manager timeout -1, disabling the native
        backend deadline rather than selecting the manager's ordinary default.
        Other timeout values are float-coerced without range/finite checks.

        Labels prefer stripped label then job_label, with literal stringification
        meaning None can become a nonblank label. Submission is not undone if later
        event construction or outer result conversion fails. The event uses text
        none for an absent timeout while the returned result uses None.

        Example:
            >>> submitted = runtime._handle_sync_store_start_command(runtime, command)  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument; this runtime resolves database options and owns dispatch.
        :param command: Envelope with sync_kwargs and optional job_timeout_s/job_no_output/job_backend/label/job_label.
        :return: Job ID and effective label/backend/timeout/output choices, not the completed sync report.
        :raises CoreDispatchError: For invalid sync_kwargs shape or a blank path with no usable library fallback.
        """
        del runtime
        payload = dict(command.payload or {})
        sync_kwargs = self._normalize_sync_job_kwargs(payload)

        if not str(sync_kwargs.get("database_path", "")).strip():
            resolved_path = self._resolve_library_database_path()
            if not resolved_path:
                raise CoreDispatchError(
                    "sync.store.start requires `sync_kwargs.database_path` when core library database path is unavailable."
                )
            sync_kwargs["database_path"] = resolved_path
        if not str(sync_kwargs.get("db_type", "")).strip():
            sync_kwargs["db_type"] = self._resolve_library_db_type()

        timeout_value = payload.get("job_timeout_s", None)
        timeout_s = None if timeout_value is None else float(timeout_value)
        manager_timeout = -1.0 if timeout_s is None else float(timeout_s)

        no_output = bool(payload.get("job_no_output", False))
        backend = payload.get("job_backend", None)
        label = (
            str(payload.get("label", "")).strip()
            or str(payload.get("job_label", "")).strip()
            or None
        )

        request = JobRequest(
            module_name="LiuXin_alpha.core.workflow_jobs",
            function_name="run_sync_store_job",
            kwargs=sync_kwargs,
        )
        job_id = self.job_manager.submit(
            request,
            timeout=manager_timeout,
            no_output=no_output,
            backend=backend,
            label=label,
        )

        self.emit_event(
            "sync.store.job_submitted",
            {
                "job_id": job_id,
                "label": label or "",
                "backend": "" if backend is None else str(backend),
                "timeout_s": "none" if timeout_s is None else timeout_s,
                "no_output": no_output,
            },
        )
        return {
            "job_id": job_id,
            "label": label or "",
            "backend": "" if backend is None else str(backend),
            "timeout_s": None if timeout_s is None else timeout_s,
            "no_output": no_output,
        }

    def _handle_sync_store_cancel_command(
        self, runtime: "CoreRuntime", command: CoreCommand
    ) -> dict[str, Any]:
        """
        Request cancellation through the common helper while emitting the legacy sync-specific event type.

        Example:
            >>> result = runtime._handle_sync_store_cancel_command(runtime, command)  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument; this bound instance supplies the manager and events.
        :param command: Envelope with the job_id to normalize and cancel.
        :return: Cancellation acknowledgment and best-effort observed state without waiting for termination.
        """
        del runtime
        payload = dict(command.payload or {})
        job_id = self._normalize_job_id(payload)
        result = self._cancel_job_by_id(
            job_id=job_id, event_type="sync.store.job_cancel_requested"
        )
        return result

    def _cancel_job_by_id(self, *, job_id: str, event_type: str) -> dict[str, Any]:
        """
        Ask the manager to cancel, best-effort read the job state, and emit the chosen cancellation event.

        Cancel failures propagate before an event is emitted. Subsequent lookup or
        state-access failures become unknown; a true acknowledgment is not proof
        the job has stopped. The identifier is not normalized again by this helper.

        Example:
            >>> result = runtime._cancel_job_by_id(job_id="job-1", event_type="jobs.cancel_requested")  # doctest: +SKIP


        :param job_id: Job identifier forwarded unchanged to cancel and follow-up lookup.
        :param event_type: Event label used to announce the acknowledgment and observed state.
        :return: Job ID, boolean cancellation acknowledgment, and observed state text or unknown fallback.
        """
        cancelled = bool(self.job_manager.cancel(job_id))
        state = "unknown"
        try:
            info = self.job_manager.get(job_id)
            state = str(info.state)
        except Exception:
            state = "unknown"

        payload = {
            "job_id": job_id,
            "cancelled": cancelled,
            "state": state,
        }
        self.emit_event(event_type, payload)
        return payload

    def _handle_jobs_cancel_command(
        self, runtime: "CoreRuntime", command: CoreCommand
    ) -> dict[str, Any]:
        """
        Normalize a job identifier and request cancellation with the generic jobs.cancel_requested event.

        Example:
            >>> result = runtime._handle_jobs_cancel_command(runtime, command)  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument in the common command-handler signature.
        :param command: Envelope containing the nonblank job identifier to cancel.
        :return: Shared cancellation helper's acknowledgment and best-effort state mapping.
        """
        del runtime
        payload = dict(command.payload or {})
        job_id = self._normalize_job_id(payload)
        return self._cancel_job_by_id(job_id=job_id, event_type="jobs.cancel_requested")

    def _handle_jobs_retry_command(
        self,
        runtime: "CoreRuntime",
        command: CoreCommand,
    ) -> dict[str, Any]:
        """
        Ask the manager to retry a job, fetch its new snapshot, serialize it, and emit jobs.retried.

        Labels preserve whitespace; only None or empty text means no override.
        allow_succeeded uses truthiness. KeyError from retry or the follow-up get
        is reported against the original ID; ValueError from either becomes
        job_not_retryable. A new job can already exist if follow-up lookup,
        serialization, or event reporting fails; no compensating cancellation occurs.

        Example:
            >>> result = runtime._handle_jobs_retry_command(runtime, command)  # doctest: +SKIP


        :param runtime: Ignored dispatcher argument; this instance owns manager access and event delivery.
        :param command: Envelope containing job_id and optional label/allow_succeeded overrides.
        :return: New job ID, original retry_of_job_id, and serialized snapshot of the newly submitted job.
        :raises CoreDispatchError: For invalid/unknown ID or manager ValueError classified as job_not_retryable.
        """
        del runtime
        payload = dict(command.payload or {})
        job_id = self._normalize_job_id(payload)
        label_raw = payload.get("label")
        label = None if label_raw in (None, "") else str(label_raw)
        try:
            retried_job_id = self.job_manager.retry(
                job_id,
                label=label,
                allow_succeeded=bool(payload.get("allow_succeeded", False)),
            )
            info = self.job_manager.get(retried_job_id)
        except KeyError as exc:
            raise CoreDispatchError("Unknown job id: {!r}".format(job_id)) from exc
        except ValueError as exc:
            raise CoreDispatchError(str(exc), code="job_not_retryable") from exc
        result = {
            "job_id": retried_job_id,
            "retry_of_job_id": job_id,
            "job": self._serialize_managed_job(info),
        }
        self.emit_event("jobs.retried", result)
        return result


__all__ = [
    "CoreRuntime",
]
