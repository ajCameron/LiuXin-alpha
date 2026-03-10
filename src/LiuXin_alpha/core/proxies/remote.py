"""HTTP-backed remote proxies for daemon-hosted core runtimes."""

from __future__ import annotations

import json
import urllib.error
import urllib.request

from typing import Any, Mapping

from LiuXin_alpha.core.proxies.local import looks_like_write_method


class RemoteProxyError(RuntimeError):
    """Raised when remote proxy requests fail."""


class _RemoteProxyBase:
    """Base remote proxy for one logical target."""

    def __init__(self, *, endpoint: str, target: str, timeout_seconds: float = 10.0) -> None:
        base = str(endpoint).strip().rstrip("/")
        if not base.startswith("http://") and not base.startswith("https://"):
            raise ValueError("Remote endpoint must be an HTTP URL, got {!r}".format(endpoint))
        self.endpoint = base
        self.target = str(target)
        self.timeout_seconds = float(timeout_seconds)

    def _url(self, suffix: str) -> str:
        return self.endpoint + suffix

    def _http_json(self, *, method: str, url: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
        body: bytes | None = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
        request = urllib.request.Request(url=url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            try:
                err_body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                err_body = str(exc)
            raise RemoteProxyError("HTTP {} for {} {}: {}".format(exc.code, method, url, err_body)) from exc
        except Exception as exc:
            raise RemoteProxyError("HTTP request failed for {} {}: {}".format(method, url, exc)) from exc

        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise RemoteProxyError("Non-JSON response from {} {}: {}".format(method, url, exc)) from exc
        if not isinstance(data, dict):
            raise RemoteProxyError("JSON response must be an object for {} {}.".format(method, url))
        return data

    def _invoke(self, *, method: str, call_args: tuple[Any, ...], call_kwargs: Mapping[str, Any], write: bool) -> Any:
        rpc_path = "/rpc/command" if write else "/rpc/query"
        payload = {
            "name": "invoke",
            "payload": {
                "target": self.target,
                "method": str(method),
                "args": list(call_args),
                "kwargs": dict(call_kwargs),
            },
        }
        response = self._http_json(method="POST", url=self._url(rpc_path), payload=payload)
        if not bool(response.get("ok", False)):
            raise RemoteProxyError("Remote call failed: {}".format(response.get("error")))
        return response.get("result")

    def query(self, method: str, *args: Any, **kwargs: Any) -> Any:
        return self._invoke(method=method, call_args=tuple(args), call_kwargs=kwargs, write=False)

    def command(self, method: str, *args: Any, **kwargs: Any) -> Any:
        return self._invoke(method=method, call_args=tuple(args), call_kwargs=kwargs, write=True)

    def __getattr__(self, method_name: str):
        def _caller(*args: Any, **kwargs: Any) -> Any:
            if looks_like_write_method(method_name):
                return self.command(method_name, *args, **kwargs)
            return self.query(method_name, *args, **kwargs)

        _caller.__name__ = "remote_proxy_{}".format(method_name)
        return _caller


class RemoteDatabaseProxy(_RemoteProxyBase):
    """Remote proxy for `database` target."""

    def __init__(self, *, endpoint: str, timeout_seconds: float = 10.0) -> None:
        super().__init__(endpoint=endpoint, target="database", timeout_seconds=timeout_seconds)


class RemoteStorageProxy(_RemoteProxyBase):
    """Remote proxy for `storage` target."""

    def __init__(self, *, endpoint: str, timeout_seconds: float = 10.0) -> None:
        super().__init__(endpoint=endpoint, target="storage", timeout_seconds=timeout_seconds)


class RemoteLibraryProxy(_RemoteProxyBase):
    """Top-level remote proxy for `library` target."""

    def __init__(self, *, endpoint: str, timeout_seconds: float = 10.0) -> None:
        super().__init__(endpoint=endpoint, target="library", timeout_seconds=timeout_seconds)
        self.database = RemoteDatabaseProxy(endpoint=endpoint, timeout_seconds=timeout_seconds)
        self.storage = RemoteStorageProxy(endpoint=endpoint, timeout_seconds=timeout_seconds)

    def health(self) -> Mapping[str, Any]:
        response = self._http_json(method="GET", url=self._url("/health"))
        if not bool(response.get("ok", False)):
            raise RemoteProxyError("Health request failed: {}".format(response.get("error")))
        return dict(response.get("result", {}))


__all__ = [
    "RemoteProxyError",
    "RemoteLibraryProxy",
    "RemoteDatabaseProxy",
    "RemoteStorageProxy",
]
