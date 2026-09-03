# Catalog examples

These scripts create and exercise the WEMI catalog facade. By default each
uses a disposable SQLite database. Pass `--database /path/to/new.sqlite` to
retain the result; the requested path must not already exist.

- `catalog_crud_example.py` — Work/Expression/Manifestation/Item CRUD.
- `catalog_metadata_bundle_example.py` — related metadata and coherent bundles.
- `catalog_matching_example.py` — exact, scoped, identifier, and ambiguous matches.
- `catalog_mutations_example.py` — coordinated updates, rollback, and merging.
- `catalog_writers_example.py` — schema-selected writers and link views.
- `catalog_quickstart.sh` — run the complete non-network catalog tour.

```bash
bash examples/catalog/catalog_quickstart.sh
```
