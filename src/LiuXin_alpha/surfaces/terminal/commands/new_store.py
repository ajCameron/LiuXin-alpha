"""
Select a registered backend preset and interactively save a store configuration through Core.

Selectable presets are captured from the default registry at module import.
Local root prompting may create directories before any store write or later
cancellation; remote roots are retained as unvalidated text. Optional manager
refresh is a separate Core operation after the configuration has been saved.
"""

from __future__ import annotations

import time

from pathlib import Path
from typing import Any, Optional

from LiuXin_alpha.storage.backend_registry import (
    DEFAULT_BACKEND_REGISTRY,
    StorageBackendDescriptor,
)
from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


_StoreKindPreset = StorageBackendDescriptor


_STORE_KIND_PRESETS: tuple[StorageBackendDescriptor, ...] = tuple(
    DEFAULT_BACKEND_REGISTRY.iter_descriptors(user_selectable_only=True)
)


class NewStoreWizardCommand(TerminalCommandAPI):
    """
    Prompt for backend/location policy, save a new or existing store, and optionally refresh loaded stores.

    Existing root/name matches request update confirmation. A new configuration
    has no separate final confirmation after the read-only/online prompts.
    Directory creation, saving, and refreshing are not one atomic operation.

    Example:
        >>> NewStoreWizardCommand().usage
        'add store'
    """

    group = "add"
    name = "store"
    aliases = ("new-store", "new_store", "add-store", "add_store")
    summary = "Interactive wizard to create or update a store row."
    usage = "add store"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Gather and save store configuration, report it, then optionally request a clearing storage refresh.

        Backend/location helpers can create directories before the row is saved.
        Read-only and online choices are declarative metadata, not live probes.
        Refresh defaults to true and happens after saving; refresh or output
        failure does not undo the configuration or any created directories.

        Example:
            >>> NewStoreWizardCommand().execute(browser, [])  # doctest: +SKIP


        :param browser: Host supplying schema/search reads, prompts, Core storage operations, and output.
        :param args: Must be empty; configuration choices come from prompts.
        :return: ``True`` after saving and any requested refresh reporting.
        :raises ValueError: For arguments, missing stores table, invalid location/kind, or declined required action.
        """
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        if "stores" not in set(browser.db.get_tables()):
            raise ValueError("Database schema does not contain `stores` table.")

        browser.emit("New store wizard")
        browser.emit("----------------")

        preset = self._prompt_store_kind(browser)
        root_uri = self._prompt_root_uri(browser, preset)

        default_name = safe_path_to_name(root_uri) or preset.kind
        store_name = (
            browser.prompt_text("Store name", default=default_name).strip()
            or default_name
        )

        read_only = browser.prompt_yes_no(
            "Read-only store?",
            default=bool(preset.read_only_default),
        )
        online = browser.prompt_yes_no("Mark store online?", default=True)

        row = self._create_or_update_store_row(
            browser,
            preset=preset,
            root_uri=root_uri,
            store_name=store_name,
            read_only=bool(read_only),
            online=bool(online),
        )

        store_id = self._store_row_id(row)
        browser.emit(
            "Store saved: id={} name={!r} kind={} root_uri={}".format(
                store_id,
                row["store_name"],
                row["store_kind"],
                row["store_root_uri"],
            )
        )

        refresh = browser.prompt_yes_no(
            "Refresh storage manager now?",
            default=True,
        )
        if refresh:
            report = self._refresh_storage_manager(browser)
            browser.emit(
                "Storage bootstrap: discovered={} loaded={} skipped={} failed={}".format(
                    self._bootstrap_report_field(report, "discovered_rows", 0),
                    self._bootstrap_report_field(report, "loaded_stores", 0),
                    self._bootstrap_report_field(report, "skipped_rows", 0),
                    self._bootstrap_report_field(report, "failed_rows", 0),
                )
            )

        return True

    @staticmethod
    def _bootstrap_report_field(report: object, key: str, default=0):
        """
        Read a legacy bootstrap counter, falling back to its current configuration-oriented name.

        Exact legacy keys/attributes win even when their value is ``None``. Only
        discovered/skipped/failed row names have aliases; other names use the
        supplied default if absent. Attribute fallback expressions are evaluated
        eagerly, so property-access errors can propagate even with a legacy field.

        Example:
            >>> NewStoreWizardCommand._bootstrap_report_field({"loaded_stores": 2}, "loaded_stores")
            2
            >>> NewStoreWizardCommand._bootstrap_report_field({"failed_configurations": 1}, "failed_rows")
            1


        :param report: Dictionary or attribute-based bootstrap report.
        :param key: Legacy counter spelling to read, optionally mapped to a current name.
        :param default: Value returned when neither spelling is present.
        :return: Counter value as stored, or the supplied default, without numeric conversion.
        """
        current_names = {
            "discovered_rows": "discovered_configurations",
            "skipped_rows": "skipped_configurations",
            "failed_rows": "failed_configurations",
        }
        if isinstance(report, dict):
            return report.get(key, report.get(current_names.get(key, ""), default))
        return getattr(
            report,
            key,
            getattr(report, current_names.get(key, ""), default),
        )

    @staticmethod
    def _refresh_storage_manager(browser):
        """
        Request Core storage refresh with existing loaded state cleared and unwrap an optional report field.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> host.execute_core_command.return_value = {"report": {"loaded_stores": 2}}
            >>> NewStoreWizardCommand._refresh_storage_manager(host)
            {'loaded_stores': 2}


        :param browser: Host exposing named Core storage commands.
        :return: Response's report field when present, otherwise the original response.
        """
        result = browser.execute_core_command(
            "storage.refresh",
            payload={"clear_existing": True},
        )
        return (result or {}).get("report", result)

    def _prompt_store_kind(self, browser) -> _StoreKindPreset:
        """
        List the captured selectable presets and resolve one prompted number or kind identifier.

        The prompt default is the first numbered entry; an unresolvable response
        raises instead of reprompting.

        Example:
            >>> preset = NewStoreWizardCommand()._prompt_store_kind(browser)  # doctest: +SKIP


        :param browser: Host providing text output and the selection prompt.
        :return: Selected backend descriptor from the module's captured tuple.
        :raises ValueError: If the response does not identify a captured selectable preset.
        """
        browser.emit("Available store kinds:")
        for idx, preset in enumerate(_STORE_KIND_PRESETS, start=1):
            browser.emit("  {}. {} ({})".format(idx, preset.label, preset.kind))

        default_idx = 1
        selection = browser.prompt_text(
            "Store kind (number or id)",
            default=str(default_idx),
        ).strip()
        chosen = self._resolve_store_kind_selection(selection)
        if chosen is None:
            raise ValueError("Unknown store kind selection: {!r}".format(selection))
        return chosen

    def _resolve_store_kind_selection(self, raw: str) -> Optional[_StoreKindPreset]:
        """
        Resolve a one-based preset number or lowercase kind ID against the captured choices.

        Out-of-range numeric text still gets a kind-ID lookup. Registry changes
        after module import are not reflected in this tuple.

        Example:
            >>> NewStoreWizardCommand()._resolve_store_kind_selection("") is None
            True
            >>> NewStoreWizardCommand()._resolve_store_kind_selection("1") is _STORE_KIND_PRESETS[0]
            True


        :param raw: Selection text, stripped before numeric or case-normalized kind lookup.
        :return: Matching descriptor or ``None`` for blank/unknown selection.
        """
        text = str(raw).strip()
        if not text:
            return None
        try:
            idx = int(text)
            if 1 <= idx <= len(_STORE_KIND_PRESETS):
                return _STORE_KIND_PRESETS[idx - 1]
        except Exception:
            pass
        lowered = text.lower()
        for preset in _STORE_KIND_PRESETS:
            if preset.kind == lowered:
                return preset
        return None

    def _prompt_root_uri(self, browser, preset: _StoreKindPreset) -> str:
        """
        Prompt for a remote URI or local path, creating permitted missing directories along the way.

        Remote values need only be nonblank. Local paths expand home notation and
        resolve to absolute paths. Directory presets reject existing nondirectories;
        missing directories require confirmation, defaulting false for unmanaged
        existing drives. File presets may create parents but do not create the file.
        SquashFS specifically requires an existing regular file. Created directories
        are not removed if later validation or store saving fails.

        Example:
            >>> root = NewStoreWizardCommand()._prompt_root_uri(browser, preset)  # doctest: +SKIP


        :param browser: Host supplying path text and directory-creation confirmations.
        :param preset: Descriptor whose location type and kind select path validation rules.
        :return: Stripped remote URI or resolved local path string.
        :raises ValueError: For blank/unsupported locations, incompatible existing paths, or declined required creation.
        """
        prompt = "Store root URI/path"
        if preset.location_type == "remote":
            raw = browser.prompt_text(prompt, default="").strip()
            if not raw:
                raise ValueError("Store root URI cannot be empty.")
            return raw

        raw = browser.prompt_text(prompt, default="").strip()
        if not raw:
            raise ValueError("Store root path cannot be empty.")

        path = Path(raw).expanduser()
        if preset.location_type == "dir":
            if path.exists() and not path.is_dir():
                raise ValueError(
                    "Path exists but is not a directory: {!r}".format(str(path))
                )
            if not path.exists():
                create_default = preset.kind != "on_disk_existing_unmanaged_drive"
                create_it = browser.prompt_yes_no(
                    "Directory does not exist. Create it?",
                    default=create_default,
                )
                if not create_it:
                    raise ValueError("Directory does not exist: {!r}".format(str(path)))
                path.mkdir(parents=True, exist_ok=True)
            return str(path.resolve())

        if preset.location_type == "file":
            parent = path.parent
            if not parent.exists():
                create_parent = browser.prompt_yes_no(
                    "Parent directory does not exist. Create it?",
                    default=True,
                )
                if not create_parent:
                    raise ValueError(
                        "Parent directory does not exist: {!r}".format(str(parent))
                    )
                parent.mkdir(parents=True, exist_ok=True)

            if preset.kind == "squashfs_readonly":
                if not path.exists() or not path.is_file():
                    raise ValueError(
                        "SquashFS store requires an existing archive file: {!r}".format(
                            str(path)
                        )
                    )
            return str(path.resolve())

        raise ValueError(
            "Unsupported store location type: {!r}".format(preset.location_type)
        )

    def _create_or_update_store_row(
        self,
        browser,
        *,
        preset: _StoreKindPreset,
        root_uri: str,
        store_name: str,
        read_only: bool,
        online: bool,
    ):
        """
        Build timestamped configuration, request confirmation for a matching row, and delegate saving to Core.

        Root matches take precedence over name matches. The match is used for
        prompting, not passed as an explicit target ID; Core owns final save/upsert
        selection. New stores proceed without an additional confirmation.

        Example:
            >>> row = NewStoreWizardCommand()._create_or_update_store_row(  # doctest: +SKIP
            ...     browser, preset=preset, root_uri=root, store_name="Archive", read_only=True, online=True
            ... )


        :param browser: Host providing existing-store search, confirmation, and Core save dispatch.
        :param preset: Backend descriptor supplying kind/protocol/capability defaults.
        :param root_uri: Root string included in search and save payload.
        :param store_name: Display name used for fallback search and saving.
        :param read_only: Whether write/delete capabilities should be suppressed in the payload.
        :param online: Whether saved status should be online rather than offline.
        :return: Store object/payload returned by the Core save helper.
        :raises ValueError: If the user declines updating an existing match.
        """
        now_epk = int(time.time() * 1000)
        updates = self._build_store_payload(
            preset=preset,
            root_uri=root_uri,
            store_name=store_name,
            read_only=read_only,
            online=online,
            now_epk=now_epk,
        )

        existing = self._find_existing_store(
            browser, root_uri=root_uri, store_name=store_name
        )
        if existing is not None:
            browser.emit(
                "Existing store found: id={} name={!r} kind={} root_uri={}".format(
                    self._store_row_id(existing),
                    existing["store_name"],
                    existing["store_kind"],
                    existing["store_root_uri"],
                )
            )
            update_existing = browser.prompt_yes_no(
                "Update existing row?", default=True
            )
            if not update_existing:
                raise ValueError("Store wizard canceled: existing row not updated.")

        return self._save_store_row(browser, store_payload=updates)

    def _build_store_payload(
        self,
        *,
        preset: _StoreKindPreset,
        root_uri: str,
        store_name: str,
        read_only: bool,
        online: bool,
        now_epk: int,
    ) -> dict[str, Any]:
        """
        Build store metadata from a preset, masking random-write/delete capabilities for read-only configurations.

        Other capabilities come directly from the descriptor. Created and modified
        timestamps both receive the supplied time, including for an update payload;
        any preservation policy belongs to Core. No backend/path probe occurs here.

        Example:
            >>> payload = NewStoreWizardCommand()._build_store_payload(
            ...     preset=_STORE_KIND_PRESETS[0], root_uri="/archive", store_name="Archive",
            ...     read_only=True, online=False, now_epk=0
            ... )
            >>> payload["store_supports_random_write"], payload["store_online_status"]
            (0, 'offline')


        :param preset: Descriptor providing backend kind, protocol, and capability flags.
        :param root_uri: Root location text copied without validation.
        :param store_name: Store display name copied into the configuration.
        :param read_only: Read-only choice controlling the flag and write/delete capability masking.
        :param online: Declarative online/offline status choice.
        :param now_epk: Unix epoch milliseconds integer-converted into both timestamps.
        :return: New dictionary of store-prefixed fields ready for Core saving.
        """
        supports_random_write = bool(preset.supports_random_write and not read_only)
        supports_delete = bool(preset.supports_delete and not read_only)

        payload: dict[str, Any] = {
            "store_name": store_name,
            "store_kind": preset.kind,
            "store_access_protocol": preset.access_protocol,
            "store_root_uri": root_uri,
            "store_is_read_only": int(bool(read_only)),
            "store_online_status": "online" if online else "offline",
            "store_supports_folders": int(bool(preset.supports_folders)),
            "store_supports_hierarchical_list": int(
                bool(preset.supports_hierarchical_list)
            ),
            "store_supports_random_read": int(bool(preset.supports_random_read)),
            "store_supports_random_write": int(supports_random_write),
            "store_supports_delete": int(supports_delete),
            "store_supports_checksums": int(bool(preset.supports_checksums)),
            "store_supports_immutable_objects": int(
                bool(preset.supports_immutable_objects)
            ),
            "store_modified_timestamp_ep_k": int(now_epk),
            "store_created_timestamp_ep_k": int(now_epk),
        }
        return payload

    def _find_existing_store(self, browser, *, root_uri: str, store_name: str):
        """
        Return the first exact-root match, otherwise the first exact-name match.

        Multiple matches are not treated as ambiguous and no normalization is
        performed. Search failures propagate rather than triggering the next query.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> host.db.search.side_effect = [[], [{"store_id": 2}]]
            >>> NewStoreWizardCommand()._find_existing_store(host, root_uri="/archive", store_name="Archive")
            {'store_id': 2}


        :param browser: Host exposing store-field search.
        :param root_uri: Exact root value used in the first search.
        :param store_name: Exact name used only when root search yields no rows.
        :return: First matching row or ``None`` when both searches return no matches.
        """
        for column, value in (
            ("store_root_uri", root_uri),
            ("store_name", store_name),
        ):
            rows = browser.db.search("stores", column, value)
            if rows:
                return rows[0]
        return None

    def _save_store_row(self, browser, *, store_payload: dict[str, Any]):
        """
        Send a shallow copy of store configuration to Core and unwrap an optional store field.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> host.execute_core_command.return_value = {"store": {"store_id": 2}}
            >>> NewStoreWizardCommand()._save_store_row(host, store_payload={"store_name": "Archive"})
            {'store_id': 2}


        :param browser: Host supplying the named ``storage.store.save`` command.
        :param store_payload: Configuration field mapping copied into the command payload.
        :return: Store field when present, otherwise Core's original response.
        """
        result = browser.execute_core_command(
            "storage.store.save",
            payload={"store": dict(store_payload)},
        )
        return (result or {}).get("store", result)

    @staticmethod
    def _store_row_id(store_row) -> Optional[int]:
        """
        Read an integer store ID from mapping access, falling back to the row object's identity attribute.

        Lookup and conversion exceptions are swallowed independently for both
        representations; neither positivity nor identity consistency is checked.

        Example:
            >>> NewStoreWizardCommand._store_row_id({"store_id": "12"})
            12
            >>> NewStoreWizardCommand._store_row_id({}) is None
            True


        :param store_row: Mapping-like or attribute-based saved store result.
        :return: First usable integer identity, or ``None`` if both access paths fail or are absent.
        """
        for key in ("store_id",):
            try:
                value = store_row[key]
                if value is None:
                    continue
                return int(value)
            except Exception:
                continue
        try:
            if store_row.row_id is not None:
                return int(store_row.row_id)
        except Exception:
            pass
        return None
