"""Core endpoint declarations for content workflows operations."""

from __future__ import annotations

from typing import cast

from LiuXin_alpha.core.program_endpoints.common import (
    ProgramEndpointHandlers,
    ProgramEndpointRegistrar,
    field,
)


def install_queries(api: object, runtime: object) -> None:
    """Register this family's query endpoints."""

    handlers = cast(ProgramEndpointHandlers, api)
    registrar = cast(ProgramEndpointRegistrar, runtime)
    query = registrar.register_query_handler

    query(
                "ingest.formats",
                handlers.ingest_formats,
                summary="List recognised ebook and metadata ingest extensions.",
                tags=("ingest", "capabilities"),
            )

    query(
                "metadata.file.formats",
                handlers.metadata_file_formats,
                summary="List metadata file reader and writer support.",
                tags=("metadata", "files", "capabilities"),
            )

    query(
                "metadata.file.inspect",
                handlers.metadata_file_inspect,
                summary="Extract metadata from a local path or base64 file payload.",
                payload_fields=(
                    field("path", field_type="string"),
                    field("base64", field_type="string"),
                    field("file_type", field_type="string"),
                ),
                tags=("metadata", "files", "read"),
            )

    query(
                "metadata.online.sources",
                handlers.metadata_online_sources,
                summary="List configured online metadata and cover sources.",
                tags=("metadata", "online", "capabilities"),
            )

    query(
                "conversion.formats",
                handlers.conversion_formats,
                summary="List available conversion input and output formats.",
                tags=("conversion", "capabilities"),
            )

    query(
                "conversion.options",
                handlers.conversion_options,
                summary="Describe conversion options for an input/output pair.",
                payload_fields=(
                    field("input_path", required=True, field_type="string"),
                    field("output_path", required=True, field_type="string"),
                ),
                tags=("conversion", "capabilities"),
            )

def install_commands(api: object, runtime: object) -> None:
    """Register this family's command endpoints."""

    handlers = cast(ProgramEndpointHandlers, api)
    registrar = cast(ProgramEndpointRegistrar, runtime)
    command = registrar.register_command_handler

    command(
                "metadata.file.write",
                handlers.metadata_file_write,
                summary="Write metadata into a local path or base64 file payload.",
                payload_fields=(
                    field("path", field_type="string"),
                    field("base64", field_type="string"),
                    field("file_type", field_type="string"),
                    field("item_id", field_type="integer"),
                    field("metadata", field_type="object"),
                ),
                tags=("metadata", "files", "write"),
            )

    command(
                "metadata.identify.start",
                handlers.metadata_identify_start,
                summary="Submit online metadata identification as a managed job.",
                payload_fields=(
                    field("title", field_type="string|null"),
                    field("authors", field_type="array|null"),
                    field("identifiers", field_type="object|null"),
                    field("timeout_s", field_type="number"),
                    field("allowed_plugins", field_type="array|null"),
                ),
                tags=("metadata", "online", "jobs", "write"),
            )

    command(
                "metadata.covers.start",
                handlers.metadata_covers_start,
                summary="Submit online cover discovery as a managed job.",
                payload_fields=(
                    field("title", field_type="string|null"),
                    field("authors", field_type="array|null"),
                    field("identifiers", field_type="object|null"),
                    field("timeout_s", field_type="number"),
                ),
                tags=("metadata", "online", "jobs", "write"),
            )

    command(
                "ingest.disk.start",
                handlers.ingest_disk_start,
                summary="Submit local-disk ingestion as a managed job.",
                payload_fields=(
                    field("disk_path", required=True, field_type="string"),
                    field("store_name", field_type="string|null"),
                    field("ebook_extensions", field_type="array|null"),
                    field("source_label", field_type="string"),
                    field("compute_hash", field_type="boolean"),
                    field("follow_symlinks", field_type="boolean"),
                    field("attach_store_links", field_type="boolean"),
                    field("refresh_storage_manager", field_type="boolean"),
                ),
                tags=("ingest", "jobs", "write"),
            )

    command(
                "ingest.remote-html.start",
                handlers.ingest_remote_html_start,
                summary="Submit remote HTML source registration as a managed job.",
                payload_fields=(
                    field("kind", required=True, field_type="string"),
                    field("options", required=True, field_type="object"),
                ),
                tags=("ingest", "remote", "jobs", "write"),
            )

    command(
                "conversion.start",
                handlers.conversion_start,
                summary="Submit an ebook conversion as a managed job.",
                payload_fields=(
                    field("input_path", required=True, field_type="string"),
                    field("output_path", required=True, field_type="string"),
                    field("options", field_type="object"),
                ),
                tags=("conversion", "jobs", "write"),
            )
