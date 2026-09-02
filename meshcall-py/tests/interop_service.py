from typing import Annotated

from pydantic import BaseModel, Field

from meshcall import method, service


class PythonResult(BaseModel):
    message: str
    handled_by: str


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
