"""Read-only single-file wrapper for URLs discovered via wget crawling."""

from __future__ import annotations

from LiuXin_alpha.storage.api.file_api import SingleFileAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus


class WgetHtmlReadOnlySingleFile(SingleFileAPI):
    """Represents one discovered URL from a wget-backed HTML crawl."""

    def __init__(
        self,
        file_url: str,
        *,
        store=None,
        exists_hint: bool = True,
    ) -> None:
        self._exists_hint = bool(exists_hint)
        super().__init__(
            file_url=file_url,
            file_status=None,
            store=None if store is None else str(getattr(store, "name", "")),
        )

    def recheck_status(self) -> SingleFileStatus:
        if self.file_status is None:
            exists_hint = self._exists_hint

            def _exists(_url: str) -> bool:
                return bool(exists_hint)

            def _size(_url: str) -> int:
                return 0

            def _hash(_url: str) -> str:
                return ""

            self.file_status = SingleFileStatus(
                url=self.file_url,
                check_exists_function=_exists,
                check_size_function=_size,
                check_hash_function=_hash,
            )
        else:
            self.file_status.recheck_self(all=True)
        return self.file_status

    def as_string(self) -> str:
        raise NotImplementedError("wget spider file wrappers do not download payloads.")

    def as_bytes(self) -> bytes:
        raise NotImplementedError("wget spider file wrappers do not download payloads.")


__all__ = ["WgetHtmlReadOnlySingleFile"]
