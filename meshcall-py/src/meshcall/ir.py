from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class StreamKind(StrEnum):
    UNARY = "unary"
    SERVER = "server_stream"
    CLIENT = "client_stream"
    DUPLEX = "duplex"


class BindingKind(StrEnum):
    INSTANCE = "instance"
    STATIC = "static"


class BalanceKind(StrEnum):
    ROUND_ROBIN = "round_robin"
    LEAST_INFLIGHT = "least_inflight"
    RANDOM = "random"
    STICKY = "sticky"
    DISABLED = "disabled"


class BalancePolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: BalanceKind = BalanceKind.ROUND_ROBIN
    key: str | None = None


class TypeRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    module: str
    qualname: str
    schema_: dict[str, Any] = Field(alias="schema")


class MethodAnnotations(BaseModel):
    """Semantic hints about a method; advisory for callers and tooling.

    ``read_only`` implies ``idempotent``. A method with no annotation may have
    side effects and must not be retried automatically without an idempotency
    key.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    idempotent: bool = False
    read_only: bool = False
    destructive: bool = False


class MethodContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    stream: StreamKind
    # The request is always one object whose properties are the method's
    # parameters, in declaration order.
    request: TypeRef
    parameters: tuple[str, ...] = ()
    response: TypeRef | None = None
    input_item: TypeRef | None = None
    output_item: TypeRef | None = None
    balance: BalancePolicy
    binding: BindingKind = BindingKind.INSTANCE
    description: str | None = None
    annotations: MethodAnnotations = MethodAnnotations()
    deprecated: str | None = None
    # Why the method needs the duplex shape; required for duplex, absent otherwise.
    duplex_reason: str | None = None


class ServiceContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    # Full, normalised name: exactly one version segment (``chat.v1``,
    # ``demo.v1.Greeting``, ``chat.v1.2.0``); ``chat`` becomes ``chat.v0``.
    name: str
    version: str = "0"
    source_module: str
    source_qualname: str
    balance: BalancePolicy
    description: str | None = None
    # Every call must name an instance: the instances hold distinct state.
    targeted: bool = False
    methods: tuple[MethodContract, ...]


class ContractDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    protocol: str = "meshcall/1"
    services: tuple[ServiceContract, ...]
