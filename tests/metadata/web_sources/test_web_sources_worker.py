"""
Verify serializable metadata and cover worker inputs, outputs and cache handling.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test web sources worker through its owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_worker.py
"""
from __future__ import annotations

from pathlib import Path

from LiuXin_alpha.metadata.utils import calibreMetaInformation


def test_web_sources_worker_import_smoke() -> None:
    """
    Verify web sources worker import smoke.

    Example:
        Exercise test web sources worker import smoke through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_worker.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.worker as worker

    assert worker is not None


def test_merge_result_clears_equal_touched_fields(monkeypatch) -> None:
    """
    Verify merge result clears equal touched fields.

    Example:
        Exercise test merge result clears equal touched fields through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_worker.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.worker as worker

    class _Plugin:
        """
        Provide the Plugin test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test merge result clears equal touched fields.Plugin through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_worker.py
        """
        touched_fields = frozenset({"title", "identifier:isbn"})

    monkeypatch.setattr(worker, "_iter_identify_plugins", lambda: [_Plugin()])
    monkeypatch.setitem(worker.msprefs, "ignore_fields", [])

    old_mi = calibreMetaInformation("Same Title", ["Same Author"])
    old_mi.set_identifier("isbn", "9780306406157")
    new_mi = calibreMetaInformation("Same Title", ["Same Author"])
    new_mi.set_identifier("isbn", "9780306406157")

    out = worker.merge_result(old_mi, new_mi)
    assert out.title != "Same Title"
    assert out.get_identifiers().get("isbn")


def test_worker_main_writes_outputs(monkeypatch, tmp_path: Path) -> None:
    """
    Verify worker main writes outputs.

    Example:
        Exercise test worker main writes outputs through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_worker.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.worker as worker

    base_mi = calibreMetaInformation("Seed Title", ["Seed Author"])
    updated_mi = calibreMetaInformation("Updated Title", ["Updated Author"])

    monkeypatch.setattr(worker, "_metadata_from_opf_bytes", lambda raw, tdir: base_mi)
    monkeypatch.setattr(worker, "identify", lambda *args, **kwargs: [updated_mi])
    monkeypatch.setattr(worker, "merge_result", lambda old, new, ensure_fields=None: new)
    monkeypatch.setattr(worker, "metadata_to_opf", lambda _mi, default_lang=None: b"<opf/>")
    monkeypatch.setattr(
        worker,
        "download_cover",
        lambda *args, **kwargs: (object(), 100, 120, "jpeg", b"cover-bytes"),
    )

    failed_ids, failed_covers, all_failed = worker.main(
        do_identify=True,
        covers=True,
        metadata={1: b"<opf/>"},
        ensure_fields=None,
        tdir=str(tmp_path),
    )

    assert failed_ids == set()
    assert failed_covers == set()
    assert all_failed is False
    assert (tmp_path / "1.mi").read_bytes() == b"<opf/>"
    assert (tmp_path / "1.cover").read_bytes() == b"cover-bytes"
    assert (tmp_path / "1.log").exists()


def test_worker_main_marks_failed_identify(monkeypatch, tmp_path: Path) -> None:
    """
    Verify worker main marks failed identify.

    Example:
        Exercise test worker main marks failed identify through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_worker.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.worker as worker

    base_mi = calibreMetaInformation("Seed Title", ["Seed Author"])
    monkeypatch.setattr(worker, "_metadata_from_opf_bytes", lambda raw, tdir: base_mi)
    monkeypatch.setattr(worker, "identify", lambda *args, **kwargs: [])

    failed_ids, failed_covers, all_failed = worker.main(
        do_identify=True,
        covers=False,
        metadata={42: b"<opf/>"},
        ensure_fields=None,
        tdir=str(tmp_path),
    )

    assert failed_ids == {42}
    assert failed_covers == set()
    assert all_failed is True
    assert (tmp_path / "42.log").exists()


def test_worker_single_identify_returns_expected_tuple(monkeypatch) -> None:
    """
    Verify worker single identify returns expected tuple.

    Example:
        Exercise test worker single identify returns expected tuple through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_worker.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.worker as worker

    mi_1 = calibreMetaInformation("A", ["One"])
    mi_2 = calibreMetaInformation("B", ["Two"])
    mi_1.has_cached_cover_url = False
    mi_2.has_cached_cover_url = True

    monkeypatch.setattr(worker, "identify", lambda *args, **kwargs: [mi_1, mi_2])
    monkeypatch.setattr(worker, "metadata_to_opf", lambda mi: f"<opf>{mi.title}</opf>".encode("utf-8"))
    monkeypatch.setattr(worker, "dump_caches", lambda: {"cache": {"x": 1}})

    opfs, cover_flags, caches, log_dump = worker.single_identify("Title", ["Author"], {"isbn": "x"})
    assert opfs == [b"<opf>A</opf>", b"<opf>B</opf>"]
    assert cover_flags == [False, True]
    assert caches == {"cache": {"x": 1}}
    assert isinstance(log_dump, str)


def test_worker_single_covers_writes_cover_files(monkeypatch, tmp_path: Path) -> None:
    """
    Verify worker single covers writes cover files.

    Example:
        Exercise test worker single covers writes cover files through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_worker.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.worker as worker

    loaded = {}
    monkeypatch.setattr(worker, "load_caches", lambda caches: loaded.update(caches))

    class _Plugin:
        """
        Provide the Plugin test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test worker single covers writes cover files.Plugin through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_worker.py
        """
        def __init__(self, name: str, can_multi: bool):
            """
            Initialize the Plugin test double.

            Example:
                Exercise test worker single covers writes cover files.Plugin.init through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_worker.py


            :param name: Value supplied for name in the focused test operation.
            :param can_multi: Value supplied for can multi in the focused test operation.
            :return: None; the function records state or raises through its assertions.
            """
            self.name = name
            self.can_get_multiple_covers = can_multi

    p1 = _Plugin("SourceOne", False)
    p2 = _Plugin("SourceTwo", True)

    def _fake_run_download(log, results, abort, title=None, authors=None, identifiers=None, timeout=30, get_best_cover=False):
        """
        Perform the fake run download test-helper operation with deterministic inputs.

        Example:
            Exercise test worker single covers writes cover files.fake run download through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_worker.py


        :param log: Value supplied for log in the focused test operation.
        :param results: Value supplied for results in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param title: Value supplied for title in the focused test operation.
        :param authors: Value supplied for authors in the focused test operation.
        :param identifiers: Value supplied for identifiers in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :param get_best_cover: Value supplied for get best cover in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        results.put((p1, 120, 160, "jpeg", b"one"))
        results.put((p2, 200, 300, "png", b"two"))

    monkeypatch.setattr(worker, "run_download", _fake_run_download)

    log_dump = worker.single_covers("Title", ["Author"], {"isbn": "x"}, {"cache": {"ok": True}}, str(tmp_path))
    assert loaded == {"cache": {"ok": True}}
    assert isinstance(log_dump, str)

    covers = sorted(tmp_path.glob("*.cover"))
    assert len(covers) == 2
    for cover_path in covers:
        done_dir = tmp_path / (cover_path.name + ".done")
        assert done_dir.is_dir()
