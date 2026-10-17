from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

import pytest
from pydantic import BaseModel

from meshcall import (
    ClientBase,
    ContractError,
    DuplexChannel,
    Router,
    RpcContext,
    StreamKind,
    dependency,
    get_service_contract,
    get_service_warnings,
    meta,
    mutates,
    query,
    serve,
    service,
    stream,
)


class FileStat(BaseModel):
    path: str
    size: int
    exists: bool
    owner: str
    tx_count: int


class TerminalIn(BaseModel):
    stdin: str = ""


class TerminalOut(BaseModel):
    stdout: str = ""


_lifecycle_events: list[str] = []


@dependency
async def get_db(ctx: RpcContext) -> AsyncIterator[dict[str, str]]:
    _lifecycle_events.append(f"db_open:{ctx.method}")
    try:
        yield {"conn": f"db-for-{ctx.method}"}
    finally:
        _lifecycle_events.append(f"db_close:{ctx.method}")


DB = Annotated[dict[str, str], get_db._provider()]


@dependency
async def get_audit(
    db: DB,
    user_id: str = meta("x-user-id", default="anon"),
) -> dict[str, str]:
    _lifecycle_events.append("audit_init")
    return {"conn": db["conn"], "user": user_id}


Audit = Annotated[dict[str, str], get_audit._provider()]


@service("workspace.v1", description="Remote workspace file & terminal operations.")
class Workspace:
    @query
    async def stat_file(
        self,
        path: str,
        db: DB,
        audit: Audit = get_audit,
        user_id: str = meta("x-user-id"),
        org_id: str | None = meta("x-org-id"),
    ) -> FileStat:
        """Return metadata for a single file."""
        assert db is not None
        assert db["conn"] == audit["conn"]
        tx_count = sum(1 for e in _lifecycle_events if e.startswith("db_open:"))
        owner = f"{user_id}@{org_id}" if org_id else user_id
        return FileStat(
            path=path,
            size=len(path),
            exists=True,
            owner=owner,
            tx_count=tx_count,
        )

    @stream
    async def tail_file(
        self,
        path: str,
        lines: int = 2,
        db: DB = get_db,
    ) -> AsyncIterator[str]:
        """Stream lines from a file."""
        for i in range(lines):
            yield f"{db['conn']}:{path}:{i}"

    @mutates(idempotent=True)
    async def upload_file(
        self,
        path: str,
        chunks: AsyncIterator[bytes],
        db: DB,
    ) -> FileStat:
        """Stream file chunks to the server."""
        total = 0
        async for chunk in chunks:
            total += len(chunk)
        return FileStat(
            path=path,
            size=total,
            exists=True,
            owner=db["conn"],
            tx_count=1,
        )

    @mutates
    async def open_terminal(
        self,
        cwd: str,
        channel: DuplexChannel[TerminalIn, TerminalOut],
        db: DB,
    ) -> None:
        """Interactive terminal session (stdin/stdout duplex)."""
        async for frame in channel:
            await channel.send(
                TerminalOut(stdout=f"[{db['conn']}:{cwd}] {frame.stdin}")
            )


class WorkspaceClient(ClientBase):
    service_name = "workspace.v1"

    async def stat_file(self, path: str) -> FileStat:
        req_cls = get_service_contract(Workspace)
        del req_cls
        from meshcall.contract import get_service_method_binding

        req_type = get_service_method_binding(Workspace, "stat_file").request_type
        return await self.call_unary(
            self.service_name,
            "stat_file",
            req_type(path=path),
            FileStat,
            idempotent=True,
        )

    def tail_file(self, path: str, lines: int = 2):
        from meshcall.contract import get_service_method_binding

        req_type = get_service_method_binding(Workspace, "tail_file").request_type
        return self.call_server_stream(
            self.service_name,
            "tail_file",
            req_type(path=path, lines=lines),
            str,
        )

    async def upload_file(self, path: str, chunks: AsyncIterator[bytes]) -> FileStat:
        from meshcall.contract import get_service_method_binding

        req_type = get_service_method_binding(Workspace, "upload_file").request_type
        return await self.call_client_stream(
            self.service_name,
            "upload_file",
            req_type(path=path),
            chunks,
            bytes,
            FileStat,
            idempotent=True,
        )

    def open_terminal(self, cwd: str):
        from meshcall.contract import get_service_method_binding

        req_type = get_service_method_binding(Workspace, "open_terminal").request_type
        return self.call_duplex(
            self.service_name,
            "open_terminal",
            req_type(cwd=cwd),
            TerminalIn,
            TerminalOut,
        )


def test_di_and_meta_stripped_from_contract_and_strict_clean() -> None:
    assert get_service_warnings(Workspace) == ()
    contract = get_service_contract(Workspace)
    by_name = {m.name: m for m in contract.methods}

    assert by_name["stat_file"].stream is StreamKind.UNARY
    assert by_name["stat_file"].parameters == ("path",)
    assert set(by_name["stat_file"].request.schema_.get("properties", {})) == {"path"}
    assert by_name["stat_file"].annotations.read_only
    assert by_name["stat_file"].annotations.idempotent

    assert by_name["tail_file"].stream is StreamKind.SERVER
    assert by_name["tail_file"].parameters == ("path", "lines")
    assert by_name["tail_file"].annotations.read_only

    assert by_name["upload_file"].stream is StreamKind.CLIENT
    assert by_name["upload_file"].parameters == ("path",)
    assert by_name["upload_file"].annotations.idempotent

    assert by_name["open_terminal"].stream is StreamKind.DUPLEX
    assert by_name["open_terminal"].parameters == ("cwd",)
    assert by_name["open_terminal"].duplex_reason == (
        "Interactive terminal session (stdin/stdout duplex)."
    )


def test_dependency_cycle_detected_at_service_definition() -> None:
    marker_a = dependency()
    marker_b = dependency()

    def fn_a(x: int = marker_b) -> int:
        return x

    def fn_b(y: int = marker_a) -> int:
        return y

    marker_a.provider = fn_a
    marker_b.provider = fn_b

    with pytest.raises(ContractError, match="Circular dependency"):

        @service("bad.v1", description="Cycle service.")
        class BadService:
            @query
            async def call(self, val: int = marker_a) -> FileStat:
                """Never runs."""
                raise AssertionError(val)

        del BadService


async def test_di_meta_and_four_shapes_end_to_end() -> None:
    from meshcall import InvalidArgumentError

    _lifecycle_events.clear()
    router = Router(listen="ws://127.0.0.1:0")
    async with (
        serve(Workspace(), router=router, access_log=False) as server,
        WorkspaceClient.connect(server.url) as ws,
    ):
        # Missing required metadata -> InvalidArgumentError
        with pytest.raises(InvalidArgumentError, match="x-user-id"):
            await ws.stat_file("/etc/hosts")

        # Unary with metadata + shared per-call dependency cache
        stat = await ws.with_options(
            metadata={"x-user-id": "alice", "x-org-id": "acme"}
        ).stat_file("/etc/hosts")
        assert stat.path == "/etc/hosts"
        assert stat.owner == "alice@acme"
        assert stat.tx_count == 1
        assert _lifecycle_events == [
            "db_open:stat_file",
            "audit_init",
            "db_close:stat_file",
        ]

        # Server stream with yield dependency cleanup
        _lifecycle_events.clear()
        lines = [line async for line in ws.tail_file("/var/log/app.log", lines=3)]
        assert lines == [
            "db-for-tail_file:/var/log/app.log:0",
            "db-for-tail_file:/var/log/app.log:1",
            "db-for-tail_file:/var/log/app.log:2",
        ]
        assert _lifecycle_events == [
            "db_open:tail_file",
            "db_close:tail_file",
        ]

        # Client stream with AsyncIterator[bytes]
        _lifecycle_events.clear()

        async def send_chunks() -> AsyncIterator[bytes]:
            yield b"hello "
            yield b"world"

        uploaded = await ws.upload_file("/tmp/out.bin", send_chunks())
        assert uploaded.size == 11
        assert uploaded.owner == "db-for-upload_file"
        assert _lifecycle_events == [
            "db_open:upload_file",
            "db_close:upload_file",
        ]

        # Duplex channel with @mutates and DuplexChannel
        _lifecycle_events.clear()
        async with ws.open_terminal("/home/alice") as term:
            await term.send(TerminalIn(stdin="pwd"))
            out = await anext(term)
            assert out.stdout == "[db-for-open_terminal:/home/alice] pwd"
            await term.close_send()
            assert [item async for item in term] == []
        assert _lifecycle_events == [
            "db_open:open_terminal",
            "db_close:open_terminal",
        ]
