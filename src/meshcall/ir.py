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


class MethodContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    stream: StreamKind
    binding: BindingKind
    request: TypeRef
    response: TypeRef | None = None
    input_item: TypeRef | None = None
    output_item: TypeRef | None = None
    balance: BalancePolicy


class ServiceContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    source_module: str
    source_qualname: str
    balance: BalancePolicy
    methods: tuple[MethodContract, ...]


class ContractDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    protocol: str = "meshcall/1"
    services: tuple[ServiceContract, ...]
