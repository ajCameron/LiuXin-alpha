# Storage root shim removal — 2026-09-15

The user requested complete removal of the lazy exports in storage.__init__.
The initializer now contains only package documentation: no export map,
__getattr__, __dir__, __all__, eager replacement exports or compatibility aliases.
The sole concrete root caller now imports StoreContainer from storage.store_container;
the public SealedArtifactWorkflow example imports its implementation module.
The storage API guide explains direct imports. Ordinary real-subpackage imports
such as `from LiuXin_alpha.storage import api` use Python's package machinery.

Verification: **165 tests passed**, including six fresh-interpreter cases proving
root import isolation and owner imports after API, manager, reconcile, storage
ingest and top-level remote-HTML entry points. All four existing filesystem
integration test functions remain unchanged. Full configured formatting, annotation,
architecture/import-cycle, Ruff, basedpyright and mypy checks passed, including
37 invalid examples rejected by each type checker. Selected docstring audit,
normalizer, Ruff lint/format and whitespace checks also passed.

[Evidence](test-results/storage-root-shim-removal-2026-09-15.json) links exact
commands, terminal records and current source hashes. The before snapshot preserves
the three original files. This is a functional API cleanup; older doc-only proofs
retain their historical scope and hashes. No commit or publication was made.

The documentation inventory records two removed hooks and three added documented
test/helper functions: net +1 function, with file coverage unchanged at 853/2732.
The D081–D180 documentation batch remains paused after D095; D096 still requires
a new resume request. Preserve the rest of the preexisting dirty tree.
