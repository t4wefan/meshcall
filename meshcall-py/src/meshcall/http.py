"""Internal HTTP v1 transport used by :class:`meshcall.Mesh` for ``http(s)://`` Router endpoints."""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncIterable, AsyncIterator, Mapping
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self, cast

import httpx
from pydantic import BaseModel, ValidationError

from meshcall.context import DEFAULT_RETRY, RetryPolicy
from meshcall.errors import MeshCallError, ProtocolError
from meshcall.router_auth import RouterCredentials
from meshcall.streams import RpcDuplexClient, RpcServerStream


@dataclass(frozen=True, slots=True)
class _Options:
    timeout: float | None = None
    idle_timeout: float | None = None
    metadata: Mapping[str, str] | None = None
    idempotency_key: str | None = None
    instance: str | None = None


def _merge_options(base: _Options, **overrides: Any) -> _Options:
    values: dict[str, Any] = {}
    for name in ("timeout", "idle_timeout", "metadata", "idempotency_key", "instance"):
        value = overrides.get(name)
        if value is None:
            values[name] = getattr(base, name)
        elif name == "metadata" and base.metadata:
            values[name] = {**base.metadata, **value}
        else:
            values[name] = value
    return _Options(**values)


def _validate(model: type[BaseModel | str | bytes], payload: Any, subject: str) -> Any:
    if model is str or model is bytes:
        if not isinstance(payload, model):
            raise ProtocolError(
                "Invalid response"
                if subject == "response"
                else "Invalid output stream item",
                details={
                    "source": "client",
                    "reason": "invalid_response"
                    if subject == "response"
                    else "invalid_output_item",
                },
            )
        return payload
    try:
        return cast(type[BaseModel], model).model_validate(payload)
    except ValidationError as exc:
        raise ProtocolError(
            "Invalid response"
            if subject == "response"
            else "Invalid output stream item",
            details={
                "source": "client",
                "reason": "invalid_response"
                if subject == "response"
                else "invalid_output_item",
                "violations": [
                    {
                        "path": ".".join(str(part) for part in error["loc"]),
                        "code": error["type"],
                        "message": error["msg"],
                    }
                    for error in exc.errors()
                ],
            },
        ) from None


def _error_from(status: int, body: Any) -> MeshCallError:
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict) and isinstance(error.get("code"), str):
        details = error.get("details")
        return MeshCallError(
            error["code"],
            str(error.get("message") or error["code"]),
            retryable=error.get("retryable") is True,
            details=details if isinstance(details, dict) else {},
        )
    return MeshCallError(
        "unavailable",
        f"HTTP {status} without a MeshCall error",
        retryable=status >= 500,
        details={"source": "client"},
    )


class _HttpTransport:
    """Internal HTTP v1 client transport used by :class:`Mesh`."""

    def __init__(
        self,
        url: str,
        *,
        credentials: RouterCredentials | None = None,
        token: str | None = None,
        headers: Mapping[str, str] | None = None,
        retry: RetryPolicy = DEFAULT_RETRY,
        timeout: float | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if credentials is not None and token is not None:
            raise ValueError("Pass credentials or token, not both")
        self.base = url.rstrip("/")
        if not self.base.startswith(("http://", "https://")):
            raise ValueError("HTTP transport needs an http(s):// Router URL")
        self.credentials = credentials or (
            RouterCredentials.bearer(token) if token is not None else None
        )
        self.headers = dict(headers or {})
        self.retry = retry
        self.timeout = timeout
        self._options = _Options()
        self._client = client or httpx.AsyncClient()
        self._owns_client = client is None
        self._closed = False

    # -- lifecycle ---------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        """HTTP has no long-lived connection; true until :meth:`close`."""
        return not self._closed

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    def with_options(
        self,
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        instance: str | None = None,
    ) -> Self:
        clone = object.__new__(type(self))
        clone.__dict__.update(self.__dict__)
        clone._options = _merge_options(
            self._options,
            timeout=timeout,
            idle_timeout=idle_timeout,
            metadata=metadata,
            idempotency_key=idempotency_key,
            instance=instance,
        )
        return clone

    # -- HTTP plumbing -----------------------------------------------------

    def _auth_header(self) -> dict[str, str]:
        credentials = self.credentials
        if credentials is None:
            return {}
        if credentials.token is not None:
            return {"Authorization": f"Bearer {credentials.token}"}
        if credentials.username is not None:
            pair = f"{credentials.username}:{credentials.password or ''}"
            return {
                "Authorization": "Basic " + base64.b64encode(pair.encode()).decode()
            }
        return {}

    def _headers(
        self,
        options: _Options,
        *,
        content_type: str | None = None,
    ) -> dict[str, str]:
        headers = {**self.headers, **self._auth_header()}
        if content_type is not None:
            headers["Content-Type"] = content_type
        if options.timeout is not None:
            headers["MeshCall-Timeout-Ms"] = str(max(1, round(options.timeout * 1000)))
        if options.idle_timeout is not None:
            headers["MeshCall-Idle-Timeout-Ms"] = str(
                max(1, round(options.idle_timeout * 1000))
            )
        if options.idempotency_key is not None:
            headers["Idempotency-Key"] = options.idempotency_key
        if options.instance is not None:
            headers["MeshCall-Instance"] = options.instance
        for key, value in (options.metadata or {}).items():
            headers["MeshCall-Meta-" + key] = value
        return headers

    def _url(self, service: str, method: str) -> str:
        return f"{self.base}/v1/call/{service}/{method}"

    def _request_timeout(self, options: _Options) -> float | None:
        return options.timeout if options.timeout is not None else self.timeout

    @staticmethod
    def _env(body: Any) -> Any:
        return json.loads(body) if isinstance(body, (str, bytes)) and body else None

    @staticmethod
    def _trailers(headers: Mapping[str, str]) -> dict[str, str]:
        prefix = "meshcall-trailer-"
        return {
            name[len(prefix) :]: value
            for name, value in headers.items()
            if name.lower().startswith(prefix)
        }

    # -- calls -------------------------------------------------------------

    async def call_unary(
        self,
        service: str,
        method: str,
        request: BaseModel,
        response_type: type[BaseModel],
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        idempotent: bool = False,
        instance: str | None = None,
    ) -> Any:
        options = _merge_options(
            self._options,
            timeout=timeout,
            idle_timeout=idle_timeout,
            metadata=metadata,
            idempotency_key=idempotency_key,
            instance=instance,
        )
        payload = request.model_dump(mode="json", by_alias=True, exclude_unset=True)
        may_retry = self.retry.enabled and (
            idempotent or options.idempotency_key is not None
        )
        attempt = 0
        while True:
            attempt += 1
            try:
                response = await self._client.post(
                    self._url(service, method),
                    json=payload,
                    headers=self._headers(options, content_type="application/json"),
                    timeout=self._request_timeout(options),
                )
                body = self._env(response.content)
                if response.status_code >= 400:
                    raise _error_from(response.status_code, body)
                return _validate(response_type, body, "response")
            except MeshCallError as exc:
                if (
                    not may_retry
                    or not exc.retryable
                    or exc.code not in self.retry.retry_codes
                    or attempt >= self.retry.max_attempts
                ):
                    if attempt > 1:
                        exc.details = {
                            **(exc.details or {}),
                            "attempts": attempt,
                        }
                    raise
            except httpx.HTTPError as exc:
                raise MeshCallError(
                    "unavailable",
                    f"HTTP request failed: {exc}",
                    retryable=True,
                    details={"source": "client"},
                ) from exc
            await asyncio.sleep(self.retry.delay(attempt))

    def call_server_stream(
        self,
        service: str,
        method: str,
        request: BaseModel,
        item_type: type[BaseModel | str | bytes],
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        idempotent: bool = False,
        instance: str | None = None,
    ) -> RpcServerStream[Any]:
        options = _merge_options(
            self._options,
            timeout=timeout,
            idle_timeout=idle_timeout,
            metadata=metadata,
            idempotency_key=idempotency_key,
            instance=instance,
        )
        return _HTTPServerStream(
            self,
            service,
            method,
            request,
            item_type,
            options,
        )

    async def call_client_stream(
        self,
        service: str,
        method: str,
        request: BaseModel,
        items: AsyncIterable[Any],
        input_type: type[BaseModel | str | bytes],
        response_type: type[BaseModel],
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        idempotent: bool = False,
        instance: str | None = None,
    ) -> Any:
        options = _merge_options(
            self._options,
            timeout=timeout,
            idle_timeout=idle_timeout,
            metadata=metadata,
            idempotency_key=idempotency_key,
            instance=instance,
        )

        async def body() -> AsyncIterator[bytes]:
            yield (
                json.dumps(
                    request.model_dump(mode="json", by_alias=True, exclude_unset=True)
                )
                + "\n"
            ).encode()
            async for item in items:
                dumped = _dump_item(input_type, item)
                yield (json.dumps(dumped) + "\n").encode()

        try:
            response = await self._client.post(
                self._url(service, method),
                content=body(),
                headers=self._headers(options, content_type="application/x-ndjson"),
                timeout=self._request_timeout(options),
            )
        except httpx.HTTPError as exc:
            raise MeshCallError(
                "unavailable",
                f"HTTP request failed: {exc}",
                retryable=True,
                details={"source": "client"},
            ) from exc
        payload = self._env(response.content)
        if response.status_code >= 400:
            raise _error_from(response.status_code, payload)
        return _validate(response_type, payload, "response")

    def call_duplex(
        self, service: str, method: str, *args: Any, **kwargs: Any
    ) -> RpcDuplexClient[Any, Any]:
        raise MeshCallError(
            "method_not_found",
            "duplex methods are WebSocket-only",
            retryable=False,
            details={"source": "client", "reason": "duplex_over_http"},
        )


def _dump_item(input_type: type[BaseModel | str | bytes], item: Any) -> Any:
    if isinstance(item, BaseModel):
        validated = (
            cast(type[BaseModel], input_type).model_validate(item)
            if input_type is not BaseModel
            else item
        )
        return validated.model_dump(mode="json", by_alias=True)
    return item


class _HTTPServerStream(RpcServerStream[Any]):
    """NDJSON envelopes read from one streaming HTTP response."""

    def __init__(
        self,
        router: _HttpTransport,
        service: str,
        method: str,
        request: BaseModel,
        item_type: type[BaseModel | str | bytes],
        options: _Options,
    ) -> None:
        self._router = router
        self._service = service
        self._method = method
        self._request = request
        self._item_type = item_type
        self._options = options
        self._context: Any = None
        self._response: httpx.Response | None = None
        self._lines: AsyncIterator[str] | None = None
        self._trailers: dict[str, str] = {}
        self._done = False

    async def _open(self) -> None:
        if self._response is not None:
            return
        self._context = self._router._client.stream(
            "POST",
            self._router._url(self._service, self._method),
            content=json.dumps(
                self._request.model_dump(mode="json", by_alias=True, exclude_unset=True)
            ),
            headers=self._router._headers(
                self._options, content_type="application/json"
            ),
            timeout=self._router._request_timeout(self._options),
        )
        response = await self._context.__aenter__()
        self._response = response
        if response.status_code >= 400:
            body = await response.aread()
            await self._context.__aexit__(None, None, None)
            self._response = None
            raise _error_from(response.status_code, _HttpTransport._env(body))
        self._lines = response.aiter_lines()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Any:
        if self._done:
            raise StopAsyncIteration
        await self._open()
        assert self._lines is not None
        while True:
            try:
                line = await self._lines.__anext__()
            except StopAsyncIteration:
                await self._finish()
                raise StopAsyncIteration from None
            if not line.strip():
                continue
            try:
                envelope = json.loads(line)
            except json.JSONDecodeError:
                await self._finish()
                raise ProtocolError(
                    "Invalid HTTP stream envelope",
                    details={"source": "client", "reason": "invalid_response"},
                ) from None
            if "item" in envelope:
                return _validate(self._item_type, envelope["item"], "output_item")
            if "error" in envelope:
                error = envelope["error"]
                await self._finish()
                raise MeshCallError(
                    str(error.get("code") or "internal"),
                    str(error.get("message") or "stream failed"),
                    retryable=error.get("retryable") is True,
                    details=error.get("details") or {},
                )
            end = envelope.get("end")
            if isinstance(end, dict):
                self._trailers = dict(end.get("trailers") or {})
                await self._finish()
                raise StopAsyncIteration
            await self._finish()
            raise ProtocolError(
                "Unknown HTTP stream envelope",
                details={"source": "client", "reason": "invalid_response"},
            )

    async def _finish(self) -> None:
        if self._done:
            return
        self._done = True
        if self._context is not None:
            await self._context.__aexit__(None, None, None)
            self._context = None
        self._response = None

    async def cancel(self, reason: str = "client_closed") -> None:
        await self._finish()

    @property
    def trailers(self) -> Mapping[str, str]:
        return self._trailers
