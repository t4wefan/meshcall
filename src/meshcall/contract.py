from __future__ import annotations

import inspect
import re
from collections.abc import AsyncIterator as AsyncIteratorABC
from collections.abc import Callable
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    ParamSpec,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
)

from pydantic import BaseModel, TypeAdapter

from meshcall.errors import ContractError
from meshcall.ir import (
    BalanceKind,
    BalancePolicy,
    MethodContract,
    ServiceContract,
    StreamKind,
    TypeRef,
)
from meshcall.streams import RpcDuplex, RpcInputStream

ServiceT = TypeVar("ServiceT", bound=type[Any])
MethodParams = ParamSpec("MethodParams")
MethodReturnT = TypeVar("MethodReturnT")
_SERVICE_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_.]*$")


class Balance:
    @staticmethod
    def round_robin() -> BalancePolicy:
        return BalancePolicy(kind=BalanceKind.ROUND_ROBIN)

    @staticmethod
    def least_inflight() -> BalancePolicy:
        return BalancePolicy(kind=BalanceKind.LEAST_INFLIGHT)

    @staticmethod
    def random() -> BalancePolicy:
        return BalancePolicy(kind=BalanceKind.RANDOM)

    @staticmethod
    def sticky(key: str) -> BalancePolicy:
        if not key.startswith("request.") or len(key) == len("request."):
            raise ContractError("A sticky key must start with 'request.'")
        return BalancePolicy(kind=BalanceKind.STICKY, key=key)

    @staticmethod
    def disabled() -> BalancePolicy:
        return BalancePolicy(kind=BalanceKind.DISABLED)


@dataclass(frozen=True)
class MethodOptions:
    balance: BalancePolicy | None
    stream: StreamKind | None


class Method:
    """Decorator namespace for RPC method binding styles."""

    def __call__(
        self,
        *,
        balance: BalancePolicy | None = None,
    ) -> Callable[
        [Callable[MethodParams, MethodReturnT]],
        Callable[MethodParams, MethodReturnT],
    ]:
        """Legacy marker used together with an explicit @staticmethod."""

        def decorate(
            func: Callable[MethodParams, MethodReturnT],
        ) -> Callable[MethodParams, MethodReturnT]:
            _mark_method(func, balance=balance, stream=None)
            return func

        return decorate

    def unary(
        self,
        *,
        balance: BalancePolicy | None = None,
    ) -> MethodDeclaration:
        return MethodDeclaration(StreamKind.UNARY, balance)

    def server_stream(
        self,
        *,
        balance: BalancePolicy | None = None,
    ) -> MethodDeclaration:
        return MethodDeclaration(StreamKind.SERVER, balance)

    def client_stream(
        self,
        *,
        balance: BalancePolicy | None = None,
    ) -> MethodDeclaration:
        return MethodDeclaration(StreamKind.CLIENT, balance)

    def duplex(
        self,
        *,
        balance: BalancePolicy | None = None,
    ) -> MethodDeclaration:
        return MethodDeclaration(StreamKind.DUPLEX, balance)


class MethodDeclaration:
    """RPC shape and options awaiting a binding style."""

    def __init__(
        self,
        stream: StreamKind,
        balance: BalancePolicy | None,
    ) -> None:
        self.stream = stream
        self.balance = balance

    if TYPE_CHECKING:
        static = staticmethod
    else:

        @property
        def static(self) -> _StaticShape:
            return _StaticShape(self.stream, self.balance)


class _StaticShape:
    def __init__(
        self,
        stream: StreamKind,
        balance: BalancePolicy | None,
    ) -> None:
        self.stream = stream
        self.balance = balance

    def __call__(
        self,
        func: Callable[MethodParams, MethodReturnT],
    ) -> staticmethod[MethodParams, MethodReturnT]:
        _mark_method(func, balance=self.balance, stream=self.stream)
        return staticmethod(func)


method = Method()


def _mark_method(
    func: Callable[..., Any],
    *,
    balance: BalancePolicy | None,
    stream: StreamKind | None,
) -> None:
    existing = getattr(func, "__meshcall_method__", None)
    if existing is not None and not isinstance(existing, MethodOptions):
        raise ContractError(f"{func.__qualname__} has invalid RPC metadata")
    if isinstance(existing, MethodOptions):
        if existing.balance is not None and balance is not None:
            raise ContractError(f"{func.__qualname__} declares balance twice")
        if existing.stream is not None and stream is not None:
            raise ContractError(f"{func.__qualname__} declares its RPC shape twice")
        balance = balance or existing.balance
        stream = stream or existing.stream
    func.__meshcall_method__ = MethodOptions(  # type: ignore[attr-defined]
        balance=balance,
        stream=stream,
    )


def service(
    *,
    name: str | None = None,
    balance: BalancePolicy | None = None,
) -> Callable[[ServiceT], ServiceT]:
    def decorate(cls: ServiceT) -> ServiceT:
        service_name = name or f"{cls.__module__}.{cls.__qualname__}"
        if not _SERVICE_NAME.fullmatch(service_name):
            raise ContractError(f"Invalid service name: {service_name!r}")
        contract = extract_service_contract(
            cls,
            name=service_name,
            balance=balance or Balance.round_robin(),
        )
        cls.__meshcall_contract__ = contract
        return cls

    return decorate


def get_service_contract(cls: type[Any]) -> ServiceContract:
    contract = getattr(cls, "__meshcall_contract__", None)
    if not isinstance(contract, ServiceContract):
        raise ContractError(f"{cls.__qualname__} is not decorated with @service")
    return contract


def extract_service_contract(
    cls: type[Any],
    *,
    name: str,
    balance: BalancePolicy,
) -> ServiceContract:
    methods: list[MethodContract] = []
    for attribute_name, descriptor in cls.__dict__.items():
        func: Callable[..., Any] | None = None
        if isinstance(descriptor, staticmethod):
            func = descriptor.__func__
        elif hasattr(descriptor, "__meshcall_method__"):
            raise ContractError(
                f"{cls.__qualname__}.{attribute_name} must use "
                "@method.<shape>(...).static or @staticmethod"
            )

        if func is None:
            continue
        options = getattr(func, "__meshcall_method__", None)
        if not isinstance(options, MethodOptions):
            continue
        methods.append(
            _extract_method(
                func,
                name=attribute_name,
                balance=options.balance or balance,
                declared_stream=options.stream,
            )
        )

    if not methods:
        raise ContractError(f"Service {name!r} does not declare any RPC methods")

    return ServiceContract(
        name=name,
        source_module=cls.__module__,
        source_qualname=cls.__qualname__,
        balance=balance,
        methods=tuple(methods),
    )


def _extract_method(
    func: Callable[..., Any],
    *,
    name: str,
    balance: BalancePolicy,
    declared_stream: StreamKind | None,
) -> MethodContract:
    if not (inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func)):
        raise ContractError(f"RPC method {func.__qualname__} must be async")

    try:
        hints = get_type_hints(func, include_extras=True)
    except (NameError, TypeError) as exc:
        raise ContractError(
            f"Cannot resolve type annotations for {func.__qualname__}: {exc}"
        ) from exc

    signature = inspect.signature(func)
    request_type: Any | None = None
    stream_annotation: Any | None = None

    for parameter in signature.parameters.values():
        if parameter.kind not in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            raise ContractError(
                f"RPC method {func.__qualname__} cannot use *args or **kwargs"
            )
        if parameter.name not in hints:
            raise ContractError(
                f"Parameter {parameter.name!r} on {func.__qualname__} needs a type"
            )

        annotation = hints[parameter.name]
        origin = get_origin(annotation)
        if origin in (RpcInputStream, RpcDuplex):
            if stream_annotation is not None:
                raise ContractError(
                    f"RPC method {func.__qualname__} has more than one stream"
                )
            stream_annotation = annotation
        else:
            if request_type is not None:
                raise ContractError(
                    f"RPC method {func.__qualname__} must have one request model"
                )
            request_type = annotation

    if request_type is None:
        raise ContractError(f"RPC method {func.__qualname__} needs a request model")
    _require_model(request_type, f"request for {func.__qualname__}")

    if "return" not in hints:
        raise ContractError(f"RPC method {func.__qualname__} needs a return type")
    return_type = hints["return"]
    return_origin = get_origin(return_type)

    request_ref = _type_ref(request_type)
    if stream_annotation is None and return_origin is AsyncIteratorABC:
        output_args = get_args(return_type)
        if len(output_args) != 1:
            raise ContractError(f"Invalid stream result on {func.__qualname__}")
        output_type = output_args[0]
        _require_model(output_type, f"stream item for {func.__qualname__}")
        return _validate_declared_stream(
            MethodContract(
                name=name,
                stream=StreamKind.SERVER,
                request=request_ref,
                output_item=_type_ref(output_type),
                balance=balance,
            ),
            declared_stream=declared_stream,
            func=func,
        )

    if stream_annotation is None:
        _require_model(return_type, f"response for {func.__qualname__}")
        return _validate_declared_stream(
            MethodContract(
                name=name,
                stream=StreamKind.UNARY,
                request=request_ref,
                response=_type_ref(return_type),
                balance=balance,
            ),
            declared_stream=declared_stream,
            func=func,
        )

    stream_origin = get_origin(stream_annotation)
    stream_args = get_args(stream_annotation)
    if stream_origin is RpcInputStream:
        if len(stream_args) != 1:
            raise ContractError(f"Invalid input stream on {func.__qualname__}")
        input_type = stream_args[0]
        _require_model(input_type, f"input stream item for {func.__qualname__}")
        _require_model(return_type, f"response for {func.__qualname__}")
        return _validate_declared_stream(
            MethodContract(
                name=name,
                stream=StreamKind.CLIENT,
                request=request_ref,
                response=_type_ref(return_type),
                input_item=_type_ref(input_type),
                balance=balance,
            ),
            declared_stream=declared_stream,
            func=func,
        )

    if stream_origin is RpcDuplex:
        if len(stream_args) != 2:
            raise ContractError(f"Invalid duplex stream on {func.__qualname__}")
        input_type, output_type = stream_args
        _require_model(input_type, f"duplex input for {func.__qualname__}")
        _require_model(output_type, f"duplex output for {func.__qualname__}")
        response_ref = None
        if return_type not in (None, type(None)):
            _require_model(return_type, f"response for {func.__qualname__}")
            response_ref = _type_ref(return_type)
        return _validate_declared_stream(
            MethodContract(
                name=name,
                stream=StreamKind.DUPLEX,
                request=request_ref,
                response=response_ref,
                input_item=_type_ref(input_type),
                output_item=_type_ref(output_type),
                balance=balance,
            ),
            declared_stream=declared_stream,
            func=func,
        )

    raise ContractError(f"Unsupported stream annotation on {func.__qualname__}")


def _validate_declared_stream(
    contract: MethodContract,
    *,
    declared_stream: StreamKind | None,
    func: Callable[..., Any],
) -> MethodContract:
    if declared_stream is not None and contract.stream is not declared_stream:
        raise ContractError(
            f"RPC method {func.__qualname__} is declared as "
            f"{declared_stream.value}, but its signature implies "
            f"{contract.stream.value}"
        )
    return contract


def _require_model(annotation: Any, context: str) -> None:
    if not inspect.isclass(annotation) or not issubclass(annotation, BaseModel):
        raise ContractError(f"The {context} must be a Pydantic BaseModel")


def _type_ref(annotation: type[BaseModel]) -> TypeRef:
    if "<locals>" in annotation.__qualname__:
        raise ContractError(
            f"Type {annotation.__qualname__} is local and cannot be generated"
        )
    return TypeRef(
        module=annotation.__module__,
        qualname=annotation.__qualname__,
        schema=TypeAdapter(annotation).json_schema(),
    )
