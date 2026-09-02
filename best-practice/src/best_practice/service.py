"""The in-memory LLM-shaped service used by the best-practice demo."""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from uuid import uuid4

from pydantic import BaseModel

from meshcall import RpcInputStream, method, service
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


@service(name="best_practice.v1.LlmService")
class LlmService:
    """A deterministic LLM facade with process-local session state."""

    def __init__(self) -> None:
        self._sessions: dict[str, _Session] = {}
        self._lock = asyncio.Lock()

    @method()
    async def new_session(self, title: str = "New session") -> NewSessionResponse:
        session = _Session(
            id=uuid4().hex,
            title=title.strip() or "New session",
        )
        async with self._lock:
            self._sessions[session.id] = session
        return NewSessionResponse(session=session.info())

    @method()
    async def list_sessions(self) -> ListSessionsResponse:
        async with self._lock:
            sessions = [session.info() for session in self._sessions.values()]
        return ListSessionsResponse(sessions=sessions)

    @method()
    async def count_tokens(self, text: str) -> TokenCountResponse:
        """Count simple lexical tokens; this deliberately is not a model tokenizer."""
        return TokenCountResponse(
            count=len(_TOKEN_PATTERN.findall(text)),
            tokenizer="simple-regex",
        )

    @method.client_stream()
    async def assemble_prompt(
        self,
        items: RpcInputStream[PromptChunk],
    ) -> PromptAssembly:
        """Collect prompt chunks to demonstrate client-to-server backpressure."""
        chunks = [item async for item in items]
        return PromptAssembly(
            text="".join(item.text for item in chunks),
            chunk_count=len(chunks),
        )

    @method.server_stream()
    async def stream_chat(
        self,
        session_id: str,
        prompt: str,
        max_tokens: int = 64,
    ) -> AsyncIterator[ChatChunk]:
        if max_tokens <= 0:
            raise MeshCallError(
                ErrorCode.INVALID_ARGUMENT,
                "max_tokens must be positive",
            )
        async with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                raise MeshCallError(
                    ErrorCode.INVALID_ARGUMENT,
                    f"Unknown session: {session_id}",
                )
            session.messages.append(("user", prompt))

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

        async with self._lock:
            session = self._sessions.get(session_id)
            if session is not None:
                session.messages.append(("assistant", assistant))
