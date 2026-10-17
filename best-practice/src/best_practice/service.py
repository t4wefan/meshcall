"""The in-memory LLM-shaped service used by the best-practice demo.

The service shows the recommended v0.3 shape:

- ``@query`` / ``@mutates`` / ``@stream`` instead of one generic decorator;
- a dependency (``SessionUnit``) that stages this call's writes and commits
  them only when the call succeeds, with ``yield`` cleanup on error or
  cancellation, so a failed or cancelled turn leaves no trace;
- ``meta("x-caller-id")`` for the one thing that really comes from transport
  metadata (a header set by the calling gateway or CLI).
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Annotated
from uuid import uuid4

from pydantic import BaseModel

from meshcall import (
    RpcLogger,
    dependency,
    meta,
    mutates,
    query,
    service,
    stream,
)
from meshcall.errors import ErrorCode, MeshCallError

_TOKEN_PATTERN = re.compile(r"\w+|[^\w\s]", re.UNICODE)
_CHUNK_PATTERN = re.compile(r"\S+\s*", re.UNICODE)
_FAKE_TOKEN_DELAY_SECONDS = 0.06


class SessionInfo(BaseModel):
    id: str
    title: str
    message_count: int


class NewSessionResponse(BaseModel):
    session: SessionInfo


class ListSessionsResponse(BaseModel):
    sessions: list[SessionInfo]


class TokenCountResponse(BaseModel):
    count: int
    tokenizer: str


class PromptChunk(BaseModel):
    text: str


class PromptAssembly(BaseModel):
    text: str
    chunk_count: int


class ChatChunk(BaseModel):
    delta: str
    index: int


@dataclass
class _Session:
    id: str
    title: str
    messages: list[tuple[str, str]] = field(default_factory=list)

    def info(self) -> SessionInfo:
        return SessionInfo(
            id=self.id,
            title=self.title,
            message_count=len(self.messages),
        )


class SessionUnit:
    """One call's unit of work over the process-wide session table.

    Writes are staged here and applied by :meth:`commit` only when the call
    succeeds; :meth:`rollback` drops them. A dependency provider owns the
    lifecycle, so handlers never commit by hand.
    """

    def __init__(self, store: _SessionStore) -> None:
        self._store = store
        self._created: list[_Session] = []
        self._pending: list[tuple[str, str, str]] = []

    async def list(self) -> list[SessionInfo]:
        rows = await self._store.rows()
        return [session.info() for session in self._created + rows]

    async def get(self, session_id: str) -> _Session | None:
        for session in self._created:
            if session.id == session_id:
                return session
        for session in await self._store.rows():
            if session.id == session_id:
                return session
        return None

    def create(self, title: str) -> _Session:
        session = _Session(id=uuid4().hex, title=title)
        self._created.append(session)
        return session

    def append(self, session: _Session, role: str, text: str) -> None:
        self._pending.append((session.id, role, text))

    async def commit(self) -> None:
        await self._store.apply(self._created, self._pending)

    async def rollback(self) -> None:
        """Nothing was applied yet, so dropping the staged writes is enough."""
        self._created.clear()
        self._pending.clear()


class _SessionStore:
    """Process-local session table shared by every call of this instance."""

    def __init__(self) -> None:
        self._rows: dict[str, _Session] = {}
        self._lock = asyncio.Lock()

    async def rows(self) -> list[_Session]:
        async with self._lock:
            return list(self._rows.values())

    async def apply(
        self,
        created: list[_Session],
        pending: list[tuple[str, str, str]],
    ) -> None:
        async with self._lock:
            for session in created:
                self._rows[session.id] = session
            for session_id, role, text in pending:
                row = self._rows.get(session_id)
                if row is not None:
                    row.messages.append((role, text))


_STORE = _SessionStore()


@dependency
async def transaction() -> AsyncIterator[SessionUnit]:
    """One unit of work per call, committed on success and dropped on error."""
    unit = SessionUnit(_STORE)
    try:
        yield unit
    except BaseException:
        await unit.rollback()
        raise
    else:
        await unit.commit()


Tx = Annotated[SessionUnit, transaction._provider()]
CallerId = Annotated[str | None, meta("x-caller-id")]


@service("best_practice.v1.LlmService")
class LlmService:
    """A deterministic LLM facade with process-local session state."""

    @mutates
    async def new_session(
        self,
        title: str = "New session",
        *,
        tx: Tx,
        caller: CallerId,
        logger: RpcLogger,
    ) -> NewSessionResponse:
        """Create a session and return it; each call creates a new one."""
        normalized = title.strip() or "New session"
        session = tx.create(normalized)
        logger.info(  # noqa: PLE1205
            "created session id={} title={!r} caller={!r}",
            session.id,
            session.title,
            caller,
        )
        return NewSessionResponse(session=session.info())

    @query
    async def list_sessions(self, *, tx: Tx) -> ListSessionsResponse:
        """List the sessions held in this process."""
        return ListSessionsResponse(sessions=await tx.list())

    @query
    async def count_tokens(self, text: str, *, logger: RpcLogger) -> TokenCountResponse:
        """Count simple lexical tokens; this deliberately is not a model tokenizer."""
        count = len(_TOKEN_PATTERN.findall(text))
        logger.debug("counted {} tokens", count)  # noqa: PLE1205
        return TokenCountResponse(count=count, tokenizer="simple-regex")

    @mutates(idempotent=True)
    async def assemble_prompt(
        self,
        chunks: AsyncIterator[PromptChunk],
        *,
        logger: RpcLogger,
    ) -> PromptAssembly:
        """Collect prompt chunks to demonstrate client-to-server backpressure."""
        collected = [chunk async for chunk in chunks]
        logger.debug(  # noqa: PLE1205
            "assembled prompt from {} chunks",
            len(collected),
        )
        return PromptAssembly(
            text="".join(chunk.text for chunk in collected),
            chunk_count=len(collected),
        )

    @stream(read_only=False, idempotent=False)
    async def stream_chat(
        self,
        session_id: str,
        prompt: str,
        max_tokens: int = 64,
        *,
        tx: Tx,
        caller: CallerId,
        logger: RpcLogger,
    ) -> AsyncIterator[ChatChunk]:
        """Stream a deterministic reply for ``prompt`` into ``session_id``.

        The user message and the assistant reply are staged in the call's unit
        of work: they become visible together when the stream ends, and are
        dropped if the caller cancels the stream halfway.
        """
        if max_tokens <= 0:
            raise MeshCallError(
                ErrorCode.INVALID_ARGUMENT,
                "max_tokens must be positive",
            )
        session = await tx.get(session_id)
        if session is None:
            raise MeshCallError(
                ErrorCode.INVALID_ARGUMENT,
                f"Unknown session: {session_id}",
            )
        tx.append(session, "user", prompt)
        logger.info(  # noqa: PLE1205
            "streaming response for prompt={!r} caller={!r}",
            prompt,
            caller,
        )

        response = (
            f"I received: {prompt}\n"
            "This deterministic fake response is streamed from Python through MeshCall."
        )
        assistant = ""
        for index, delta in enumerate(_CHUNK_PATTERN.findall(response)):
            if index >= max_tokens:
                break
            await asyncio.sleep(_FAKE_TOKEN_DELAY_SECONDS)
            assistant += delta
            yield ChatChunk(delta=delta, index=index)

        tx.append(session, "assistant", assistant)
        logger.debug(  # noqa: PLE1205
            "streamed {} response chunks",
            len(assistant.split()),
        )
