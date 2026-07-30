from __future__ import annotations

from collections.abc import AsyncIterable
from typing import Any

from pydantic import BaseModel

from meshcall.streams import RpcDuplexClient, RpcServerStream


class ClientBase:
    """Runtime surface used by generated service clients."""

    async def _unary(
        self,
        service: str,
        method: str,
        request: BaseModel,
        response_type: type[BaseModel],
    ) -> Any:
        raise NotImplementedError

    def _server_stream(
        self,
        service: str,
        method: str,
        request: BaseModel,
        item_type: type[BaseModel],
    ) -> RpcServerStream[Any]:
        raise NotImplementedError

    async def _client_stream(
        self,
        service: str,
        method: str,
        request: BaseModel,
        items: AsyncIterable[BaseModel],
        response_type: type[BaseModel],
    ) -> Any:
        raise NotImplementedError

    def _duplex(
        self,
        service: str,
        method: str,
        request: BaseModel,
        input_type: type[BaseModel],
        output_type: type[BaseModel],
        result_type: type[BaseModel] | None,
    ) -> RpcDuplexClient[Any, Any, Any]:
        raise NotImplementedError

