from collections.abc import AsyncIterator
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, Field

from meshcall import RpcInputStream, method, service


class PythonResult(BaseModel):
    message: str
    handled_by: str


class ItemStatus(StrEnum):
    READY = "ready"
    BUSY = "busy"


class PythonItem(BaseModel):
    value: int = Field(alias="itemValue")
    status: ItemStatus


class PythonTotal(BaseModel):
    total: int


@service(name="test.v1.PythonInteropService")
class PythonInteropService:
    @method()
    async def greet(
        self,
        name: str,
        repeat: Annotated[int, Field(ge=1, le=5)] = 1,
    ) -> PythonResult:
        return PythonResult(
            message=" ".join([f"Hello, {name}!"] * repeat),
            handled_by="python",
        )

    @method.server_stream()
    async def download(self, count: int) -> AsyncIterator[PythonItem]:
        for value in range(count):
            yield PythonItem.model_validate({"itemValue": value, "status": "ready"})

    @method.client_stream()
    async def upload(
        self, offset: int, items: RpcInputStream[PythonItem]
    ) -> PythonTotal:
        total = offset
        async for item in items:
            total += item.value
        return PythonTotal(total=total)
