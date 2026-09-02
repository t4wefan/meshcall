from pydantic import BaseModel, Field

from meshcall import method, service


class PythonRequest(BaseModel):
    name: str
    repeat: int = Field(ge=1, le=5)


class PythonResult(BaseModel):
    message: str
    handled_by: str


@service(name="test.v1.PythonInteropService")
class PythonInteropService:
    @staticmethod
    @method()
    async def greet(request: PythonRequest) -> PythonResult:
        return PythonResult(
            message=" ".join([f"Hello, {request.name}!"] * request.repeat),
            handled_by="python",
        )
