from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from meshcall import query, service


class GreetingResult(BaseModel):
    message: str
    handled_by: str


@service("demo.v1.PythonGreetingService")
class PythonGreetingService:
    """A Python service consumed by the generated TypeScript client."""

    @query
    async def greet(
        self,
        name: str,
        repeat: Annotated[int, Field(ge=1, le=5)] = 1,
    ) -> GreetingResult:
        """Greet ``name`` ``repeat`` times and say which runtime answered."""
        return GreetingResult(
            message=" ".join(f"Hello, {name}!" for _ in range(repeat)),
            handled_by="python",
        )
