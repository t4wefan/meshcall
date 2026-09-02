"""Logging types used by the MeshCall server runtime."""

from __future__ import annotations

from typing import Any, Protocol, Self, overload


class RpcLogger(Protocol):
    """The logger shape MeshCall injects into services and RPC methods.

    MeshCall uses Loguru by default, but keeping this as a small protocol lets
    an application provide a compatible logger with ``RpcServer(logger=...)``.
    """

    def bind(self, **context: Any) -> Self: ...

    def opt(
        self,
        *,
        exception: bool | BaseException | None = None,
        record: bool = False,
        lazy: bool = False,
        colors: bool = False,
        raw: bool = False,
        capture: bool = True,
        depth: int = 0,
        ansi: bool = False,
    ) -> RpcLogger: ...

    @overload
    def log(
        self,
        level: str | int,
        message: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None: ...

    @overload
    def log(self, level: str | int, message: Any, /) -> None: ...

    def debug(self, *args: Any, **kwargs: Any) -> Any: ...

    def trace(self, *args: Any, **kwargs: Any) -> Any: ...

    def success(self, *args: Any, **kwargs: Any) -> Any: ...

    def info(self, *args: Any, **kwargs: Any) -> Any: ...

    def warning(self, *args: Any, **kwargs: Any) -> Any: ...

    def error(self, *args: Any, **kwargs: Any) -> Any: ...

    def exception(self, *args: Any, **kwargs: Any) -> Any: ...

    def critical(self, *args: Any, **kwargs: Any) -> Any: ...
