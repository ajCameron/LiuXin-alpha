"""
Composite Digital Asset catalogue and resolution workflows.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import override

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class CompositeDigitalAssetMixin(_StorageManagerState):
    """
    Manage ordered logical assemblies of atomic Digital Assets.

    Composite records describe membership, names, and attributes while the
    member Assets continue to own byte identity and Replica placement.  All
    mutations validate member references and use revision preconditions; this
    component never concatenates or materialises member bytes.
    """

    @override
    def declare_composite_digital_asset(
        self,
        declaration: api.CompositeDigitalAssetDeclaration,
    ) -> api.CompositeDigitalAssetRecord:
        """
        Register an ordered Composite after validating every member.


        :param declaration:
        :return:
        """

        for member in declaration.members:
            self.get_digital_asset_record(member.digital_asset_id)
        with self._lock, self._metadata_transaction():
            composite_id = api.CompositeDigitalAssetID(
                self._allocate_metadata_id_locked("composite")
            )
            record = api.CompositeDigitalAssetRecord(
                composite_id,
                declaration.members,
                declaration.name,
                declaration.attributes,
                self._new_revision_locked(),
            )
            self._composites[composite_id] = record
            return record

    @override
    def get_composite_digital_asset_record(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
    ) -> api.CompositeDigitalAssetRecord:
        """
        Return one Composite record.


        :param composite_digital_asset_id:
        :return:
        """

        with self._lock:
            try:
                return self._composites[composite_digital_asset_id]
            except KeyError as error:
                raise api.CompositeDigitalAssetNotFound(
                    f"Composite Digital Asset {composite_digital_asset_id} is not registered."
                ) from error

    @override
    def replace_composite_digital_asset(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
        declaration: api.CompositeDigitalAssetDeclaration,
        *,
        if_revision: str | None = None,
    ) -> api.CompositeDigitalAssetRecord:
        """
        Replace Composite metadata and membership under revision control.


        :param composite_digital_asset_id:
        :param declaration:
        :param if_revision:
        :return:
        """

        for member in declaration.members:
            self.get_digital_asset_record(member.digital_asset_id)
        with self._lock, self._metadata_transaction():
            current = self._require_composite_locked(composite_digital_asset_id)
            self._check_revision(current.revision, if_revision)
            record = api.CompositeDigitalAssetRecord(
                composite_digital_asset_id,
                declaration.members,
                declaration.name,
                declaration.attributes,
                self._new_revision_locked(),
            )
            self._composites[composite_digital_asset_id] = record
            return record

    @override
    def iter_composite_digital_asset_records(
        self,
    ) -> Iterator[api.CompositeDigitalAssetRecord]:
        """
        Iterate over a stable Composite snapshot.


        :return:
        """

        with self._lock:
            records = tuple(self._composites[key] for key in sorted(self._composites))
        return iter(records)

    @override
    def forget_composite_digital_asset(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
        *,
        require_unlinked: bool = True,
        if_revision: str | None = None,
    ) -> bool:
        """
        Forget an unlinked Composite without touching member Assets.


        :param composite_digital_asset_id:
        :param require_unlinked:
        :param if_revision:
        :return:
        """

        with self._lock, self._metadata_transaction():
            current = self._composites.get(composite_digital_asset_id)
            if current is None:
                return False
            self._check_revision(current.revision, if_revision)
            if require_unlinked:
                if any(
                    kind == "composite_digital_asset"
                    and target_id == composite_digital_asset_id
                    for kind, target_id in self._item_targets.values()
                ):
                    raise api.StoragePreconditionFailed(
                        "Composite Digital Asset is still linked to an Item."
                    )
                if any(
                    source.composite_digital_asset_id == composite_digital_asset_id
                    for record in self._derivations.values()
                    for source in record.declaration.sources
                ):
                    raise api.StoragePreconditionFailed(
                        "Composite Digital Asset is still derivation provenance."
                    )
            del self._composites[composite_digital_asset_id]
            return True

    @override
    def resolve_composite_digital_asset(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
        *,
        preferred_store_ref: api.StoreUUID | None = None,
        require_verified: bool = False,
    ) -> tuple[api.CompositeDigitalAssetMemberResolution, ...]:
        """
        Resolve each readable Composite member without flattening context.


        :param composite_digital_asset_id:
        :param preferred_store_ref:
        :param require_verified:
        :return:
        """

        record = self.get_composite_digital_asset_record(composite_digital_asset_id)
        resolved: list[api.CompositeDigitalAssetMemberResolution] = []
        missing: list[api.DigitalAssetID] = []
        for membership in record.members:
            try:
                resolution = self.resolve_digital_asset(
                    membership.digital_asset_id,
                    preferred_store_ref=preferred_store_ref,
                    require_verified=require_verified,
                )
            except (api.DigitalAssetNotFound, api.NoReadableReplica):
                if membership.required:
                    missing.append(membership.digital_asset_id)
                continue
            resolved.append(
                api.CompositeDigitalAssetMemberResolution(
                    membership,
                    resolution,
                )
            )
        if missing:
            raise api.CompositeDigitalAssetIncomplete(
                "required member Assets are unavailable: "
                + ", ".join(str(value) for value in missing)
            )
        return tuple(resolved)

    @override
    def assess_composite_digital_asset(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
    ) -> api.CompositeDigitalAssetAvailabilityAssessment:
        """
        Assess member existence and current Replica readability.


        :param composite_digital_asset_id:
        :return:
        """

        record = self.get_composite_digital_asset_record(composite_digital_asset_id)
        resolved = readable = 0
        missing: list[api.DigitalAssetID] = []
        errors: list[str] = []
        required_members = tuple(
            membership for membership in record.members if membership.required
        )
        for membership in required_members:
            try:
                self.get_digital_asset_record(membership.digital_asset_id)
                resolved += 1
            except api.DigitalAssetNotFound as error:
                missing.append(membership.digital_asset_id)
                errors.append(str(error))
                continue
            try:
                self.select_replica(membership.digital_asset_id)
                readable += 1
            except api.NoReadableReplica as error:
                missing.append(membership.digital_asset_id)
                errors.append(str(error))
        return api.CompositeDigitalAssetAvailabilityAssessment(
            composite_digital_asset_id,
            len(required_members),
            resolved,
            readable,
            tuple(dict.fromkeys(missing)),
            tuple(errors),
        )


__all__ = ["CompositeDigitalAssetMixin"]
