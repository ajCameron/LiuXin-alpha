"""Calibre library fixtures with a small template cache.

Creates a template Calibre library (metadata.db + optional aux dbs) once per
session and then provisions per-test writable copies, reseeding library_id.uuid
so each copy has a unique identity.
"""

from __future__ import annotations

import shutil
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import (
    create_calibre_library_skeleton,
    calibre_metadata_schema_info,
)


@dataclass(frozen=True)
class ProvisionedCalibreLibrary:
    name: str
    root: Path
    metadata_db: Path
    notes_db: Optional[Path]
    fts_db: Optional[Path]
    library_uuid: str


class CalibreLibraryTemplateManager:
    def __init__(self, *, cache_dir: Path, regenerate: bool = False) -> None:
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.regenerate = regenerate
        self._templates_root = self.cache_dir / "templates" / "calibre_libraries"
        self._templates_root.mkdir(parents=True, exist_ok=True)

    def provision_blank_library(
        self,
        *,
        dst_dir: Path,
        name: str = "calibre_library",
        create_notes_db: bool = False,
        create_fts_db: bool = False,
        best_effort_aux_dbs: bool = True,
    ) -> ProvisionedCalibreLibrary:
        template_root = self._ensure_template(
            create_notes_db=create_notes_db,
            create_fts_db=create_fts_db,
            best_effort_aux_dbs=best_effort_aux_dbs,
        )

        dst_dir = Path(dst_dir)
        dst_dir.mkdir(parents=True, exist_ok=True)
        out_root = dst_dir / name
        if out_root.exists():
            shutil.rmtree(out_root)
        shutil.copytree(template_root, out_root)

        metadata_db = out_root / "metadata.db"
        if not metadata_db.exists():
            raise FileNotFoundError(f"Template copy missing metadata.db: {metadata_db}")

        # Reseed library identity so each provisioned library is unique.
        new_uuid = str(uuid.uuid4())
        conn = sqlite3.connect(str(metadata_db))
        try:
            conn.execute("DELETE FROM library_id")
            conn.execute("INSERT INTO library_id (uuid) VALUES (?)", (new_uuid,))
            conn.commit()
        finally:
            conn.close()

        notes_db = out_root / ".calnotes" / "notes.db"
        fts_db = out_root / "full-text-search.db"

        return ProvisionedCalibreLibrary(
            name=name,
            root=out_root,
            metadata_db=metadata_db,
            notes_db=notes_db if notes_db.exists() else None,
            fts_db=fts_db if fts_db.exists() else None,
            library_uuid=new_uuid,
        )

    def _ensure_template(
        self,
        *,
        create_notes_db: bool,
        create_fts_db: bool,
        best_effort_aux_dbs: bool,
    ) -> Path:
        info = calibre_metadata_schema_info()
        variant = f"notes={int(create_notes_db)}_fts={int(create_fts_db)}_be={int(best_effort_aux_dbs)}"
        key = f"uv{info.user_version}_{info.sha256[:10]}_{variant}"
        root = self._templates_root / key

        if root.exists() and not self.regenerate:
            return root

        if root.exists():
            shutil.rmtree(root)
        root.mkdir(parents=True, exist_ok=True)

        # Deterministic UUID for the template; provisioned copies will reseed it.
        template_uuid = "00000000-0000-0000-0000-000000000000"

        create_calibre_library_skeleton(
            root,
            overwrite=True,
            validate=True,
            ensure_library_uuid=True,
            library_uuid=template_uuid,
            create_notes_db=create_notes_db,
            create_fts_db=create_fts_db,
            best_effort_aux_dbs=best_effort_aux_dbs,
        )
        return root
