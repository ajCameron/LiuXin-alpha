"""
Create and clone reusable blank Calibre-library templates for isolated tests.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise calibre library templates through a consuming regression::

        python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_templates.py
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
    """
    Represent the ProvisionedCalibreLibrary state used by deterministic test-support operations.

    Example:
        Exercise ProvisionedCalibreLibrary through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_templates.py
    """
    name: str
    root: Path
    metadata_db: Path
    notes_db: Optional[Path]
    fts_db: Optional[Path]
    library_uuid: str


class CalibreLibraryTemplateManager:
    """
    Represent the CalibreLibraryTemplateManager state used by deterministic test-support operations.

    Example:
        Exercise CalibreLibraryTemplateManager through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_templates.py
    """
    def __init__(self, *, cache_dir: Path, regenerate: bool = False) -> None:
        """
        Initialize and validate the CalibreLibraryTemplateManager test-support state.

        Example:
            Exercise CalibreLibraryTemplateManager.  init   through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_templates.py


        :param cache_dir: Value supplied for cache dir under the deterministic fixture
            contract.
        :param regenerate: Value supplied for regenerate under the deterministic fixture
            contract.
        :return: None; completion is expressed through state changes or assertions.
        """
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
        """
        Provision blank library for deterministic fixture consumers.

        Example:
            Exercise CalibreLibraryTemplateManager.provision blank library through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_templates.py


        :param dst_dir: Destination directory that owns the provisioned fixture.
        :param name: Stable fixture, profile, member or field name.
        :param create_notes_db: Value supplied for create notes db under the deterministic
            fixture contract.
        :param create_fts_db: Value supplied for create fts db under the deterministic
            fixture contract.
        :param best_effort_aux_dbs: Value supplied for best effort aux dbs under the
            deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
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
        """
        Perform the ensure template step with deterministic fixture inputs.

        Example:
            Exercise CalibreLibraryTemplateManager. ensure template through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_templates.py


        :param create_notes_db: Value supplied for create notes db under the deterministic
            fixture contract.
        :param create_fts_db: Value supplied for create fts db under the deterministic
            fixture contract.
        :param best_effort_aux_dbs: Value supplied for best effort aux dbs under the
            deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
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
