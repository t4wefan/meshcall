"""Credential sources (docs/protocol.md → Credential sources).

A function the runtime asks for the credential before every connection it
opens, so an expiring token or tunnel credential is replaced without
rebuilding the Mesh.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import cast

from meshcall.errors import MeshCallError, UnauthenticatedError, UnavailableError

#: Returns the credential to connect to the Router it is given (its URL as
#: the Router lists it: ``wss://…``, ``unix:///…``, ``pipe://…``), sync or
#: async; called before every connection. A Mesh over one Router may ignore
#: the argument.
CredentialSource = Callable[[str], "str | Awaitable[str]"]

TUNNEL_CREDENTIAL_PREFIX = "mtun_"


class CredentialSourceError(ValueError):
    """A source returned a credential that cannot be used as configured (a
    tunnel credential given as a token, a plain one where an end-to-end key is
    pinned): a configuration error, never retried."""


def _source_failed(message: str) -> UnavailableError:
    return UnavailableError(
        message,
        retryable=True,
        details={"source": "client", "reason": "credential_source"},
    )


def router_name(uri: str | None, unix_path: Path | None, pipe: str | None) -> str:
    """How a Router endpoint is named to credential sources and in results."""
    if uri is not None:
        return uri
    if unix_path is not None:
        return f"unix://{unix_path}"
    return f"pipe://{pipe}"


async def ask_credential_source(
    source: CredentialSource,
    router: str,
    check: Callable[[str], None] | None = None,
) -> str:
    """Ask ``source`` for a credential to ``router`` and check it; a failing
    source fails this connection attempt as an unreachable Router would."""
    try:
        value = source(router)
        if inspect.isawaitable(value):
            value = await cast(Awaitable[str], value)
    except (MeshCallError, CredentialSourceError):
        raise
    except Exception as exc:
        raise _source_failed(f"The credential source failed: {exc}") from exc
    if not isinstance(value, str) or not value:
        raise _source_failed("The credential source returned no credential")
    if check is not None:
        try:
            check(value)
        except ValueError as exc:
            raise CredentialSourceError(str(exc)) from exc
    return value


def kinded_source(source: CredentialSource, kind: str) -> CredentialSource:
    """The source a Mesh gives its driver for a ``token=`` or ``tunnel=``
    function: what it returns is checked to be that kind of credential."""

    async def checked(router: str) -> str:
        value = source(router)
        if inspect.isawaitable(value):
            value = await cast(Awaitable[str], value)
        if not isinstance(value, str) or not value:
            return value
        tunnel = value.startswith(TUNNEL_CREDENTIAL_PREFIX)
        if kind == "tunnel" and not tunnel:
            raise CredentialSourceError(
                "The tunnel source returned no tunnel credential (mtun_…)"
            )
        if kind == "token" and tunnel:
            raise CredentialSourceError(
                "The token source returned a tunnel credential: pass the source as tunnel="
            )
        return value

    return checked


def is_router_refusal(error: BaseException) -> bool:
    """The Router refused the credential itself (HTTP 401 or close 4401)."""
    return isinstance(error, UnauthenticatedError) and error.source == "router"
