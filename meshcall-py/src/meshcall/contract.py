from __future__ import annotations

import inspect
import re
from collections.abc import AsyncIterator as AsyncIteratorABC
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    ParamSpec,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
)

from pydantic import BaseModel, TypeAdapter, create_model

from meshcall.errors import ContractError
from meshcall.ir import (
    BalanceKind,
    BalancePolicy,
    BindingKind,
    MethodContract,
    RequestStyle,
    ServiceContract,
    StreamKind,
    TypeRef,
)
from meshcall.logging import RpcLogger
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
    binding: BindingKind | None


_NO_STREAM = object()
_NO_LOGGER = object()


@dataclass(frozen=True)
class MethodBinding:
    request_type: type[BaseModel]
    request_style: RequestStyle
    request_parameter: str | None
    request_fields: tuple[str, ...]
    parameter_names: tuple[str, ...]
    positional_only_parameters: frozenset[str]
    stream_parameter: str | None
    logger_parameter: str | None

    def arguments(
        self,
        request: BaseModel,
        stream: Any = _NO_STREAM,
        *,
        logger: Any = _NO_LOGGER,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        if self.request_style is RequestStyle.MODEL:
            if self.request_parameter is None:
                raise ContractError(
                    "Model-style RPC method is missing its request parameter"
                )
            values: dict[str, Any] = {self.request_parameter: request}
        else:
            values = {name: getattr(request, name) for name in self.request_fields}

        if self.stream_parameter is None:
            if stream is not _NO_STREAM:
                raise ContractError("RPC method does not declare a stream parameter")
        else:
            if stream is _NO_STREAM:
                raise ContractError("RPC method is missing its stream parameter")
            values[self.stream_parameter] = stream

        if self.logger_parameter is not None:
            if logger is _NO_LOGGER:
                raise ContractError("RPC method is missing its logger parameter")
            values[self.logger_parameter] = logger

        args: list[Any] = []
        kwargs: dict[str, Any] = {}
        for name in self.parameter_names:
            value = values[name]
            if name in self.positional_only_parameters:
                args.append(value)
            else:
                kwargs[name] = value
        return tuple(args), kwargs


class Method:
    """Decorator namespace for RPC shape and binding declarations."""

    def __call__(
        self,
        *,
        balance: BalancePolicy | None = None,
    ) -> MethodDeclaration:
        """Declare the default unary method and infer its binding from the class."""
        return MethodDeclaration(stream=None, balance=balance, binding=None)

    def unary(
        self,
        *,
        balance: BalancePolicy | None = None,
    ) -> MethodDeclaration:
        return MethodDeclaration(
            stream=StreamKind.UNARY,
            balance=balance,
            binding=BindingKind.INSTANCE,
        )

    def server_stream(
        self,
        *,
        balance: BalancePolicy | None = None,
    ) -> MethodDeclaration:
        return MethodDeclaration(
            stream=StreamKind.SERVER,
            balance=balance,
            binding=BindingKind.INSTANCE,
        )

    def client_stream(
        self,
        *,
        balance: BalancePolicy | None = None,
    ) -> MethodDeclaration:
        return MethodDeclaration(
            stream=StreamKind.CLIENT,
            balance=balance,
            binding=BindingKind.INSTANCE,
        )

    def duplex(
        self,
        *,
        balance: BalancePolicy | None = None,
    ) -> MethodDeclaration:
        """Declare the experimental duplex method shape."""
        return MethodDeclaration(
            stream=StreamKind.DUPLEX,
            balance=balance,
            binding=BindingKind.INSTANCE,
        )


class MethodDeclaration:
    """RPC declaration awaiting application to a function."""

    def __init__(
        self,
        *,
        stream: StreamKind | None,
        balance: BalancePolicy | None,
        binding: BindingKind | None,
    ) -> None:
        self.stream = stream
        self.balance = balance
        self.binding = binding

    if TYPE_CHECKING:
        static = staticmethod
    else:

        @property
        def static(self) -> _StaticShape:
            return _StaticShape(self.stream, self.balance)

    def __call__(
        self,
        func: Callable[MethodParams, MethodReturnT],
    ) -> Callable[MethodParams, MethodReturnT]:
        _mark_method(
            func,
            balance=self.balance,
            stream=self.stream,
            binding=self.binding,
        )
        return func


class _StaticShape:
    def __init__(
        self,
        stream: StreamKind | None,
        balance: BalancePolicy | None,
    ) -> None:
        self.stream = stream
        self.balance = balance

    def __call__(
        self,
        func: Callable[MethodParams, MethodReturnT],
    ) -> staticmethod[MethodParams, MethodReturnT]:
        _mark_method(
            func,
            balance=self.balance,
            stream=self.stream,
            binding=BindingKind.STATIC,
        )
        return staticmethod(func)


method = Method()


def _mark_method(
    func: Callable[..., Any],
    *,
    balance: BalancePolicy | None,
    stream: StreamKind | None,
    binding: BindingKind | None,
) -> None:
    existing = getattr(func, "__meshcall_method__", None)
    if existing is not None and not isinstance(existing, MethodOptions):
        raise ContractError(f"{func.__qualname__} has invalid RPC metadata")
    if isinstance(existing, MethodOptions):
        if existing.balance is not None and balance is not None:
            raise ContractError(f"{func.__qualname__} declares balance twice")
        if existing.stream is not None and stream is not None:
            raise ContractError(f"{func.__qualname__} declares its RPC shape twice")
        if existing.binding is not None and binding is not None:
            raise ContractError(f"{func.__qualname__} declares its binding twice")
        balance = balance or existing.balance
        stream = stream if stream is not None else existing.stream
        binding = binding or existing.binding
    func.__meshcall_method__ = MethodOptions(  # type: ignore[attr-defined]
        balance=balance,
        stream=stream,
        binding=binding,
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


def get_service_method_binding(
    cls: type[Any],
    method_name: str,
) -> MethodBinding:
    bindings = getattr(cls, "__meshcall_method_bindings__", None)
    if not isinstance(bindings, Mapping):
        raise ContractError(f"{cls.__qualname__} has no extracted RPC method bindings")
    binding = bindings.get(method_name)
    if not isinstance(binding, MethodBinding):
        raise ContractError(
            f"RPC method {cls.__qualname__}.{method_name} has no binding"
        )
    return binding


def extract_service_contract(
    cls: type[Any],
    *,
    name: str,
    balance: BalancePolicy,
) -> ServiceContract:
    methods: list[MethodContract] = []
    bindings: dict[str, MethodBinding] = {}
    for attribute_name, descriptor in cls.__dict__.items():
        func: Callable[..., Any] | None = None
        descriptor_binding: BindingKind | None = None
        if isinstance(descriptor, staticmethod):
            func = descriptor.__func__
            descriptor_binding = BindingKind.STATIC
        elif isinstance(descriptor, classmethod):
            if hasattr(descriptor.__func__, "__meshcall_method__"):
                raise ContractError(
                    f"{cls.__qualname__}.{attribute_name} cannot be a classmethod"
                )
        elif hasattr(descriptor, "__meshcall_method__"):
            func = descriptor
            descriptor_binding = BindingKind.INSTANCE

        if func is None:
            continue
        options = getattr(func, "__meshcall_method__", None)
        if not isinstance(options, MethodOptions):
            continue
        binding = options.binding or descriptor_binding
        if binding != descriptor_binding:
            raise ContractError(
                f"{cls.__qualname__}.{attribute_name} has inconsistent binding"
            )
        if binding is None:
            raise ContractError(
                f"{cls.__qualname__}.{attribute_name} does not declare a binding"
            )
        extracted = _extract_method(
            func,
            name=attribute_name,
            balance=options.balance or balance,
            declared_stream=options.stream,
            binding=binding,
            service_type=cls,
        )
        methods.append(extracted.contract)
        bindings[attribute_name] = extracted.binding

    if not methods:
        raise ContractError(f"Service {name!r} does not declare any RPC methods")

    cls.__meshcall_method_bindings__ = MappingProxyType(bindings)  # type: ignore[attr-defined]
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
    binding: BindingKind,
    service_type: type[Any],
) -> _ExtractedMethod:
    if not (inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func)):
        raise ContractError(f"RPC method {func.__qualname__} must be async")

    try:
        hints = get_type_hints(func, include_extras=True)
    except (NameError, TypeError) as exc:
        raise ContractError(
            f"Cannot resolve type annotations for {func.__qualname__}: {exc}"
        ) from exc

    parameters = list(inspect.signature(func).parameters.values())
    if binding is BindingKind.INSTANCE:
        if not parameters or parameters[0].kind not in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            raise ContractError(
                f"Instance RPC method {func.__qualname__} needs a receiver parameter"
            )
        parameters = parameters[1:]

    request_parameters: list[inspect.Parameter] = []
    stream_annotation: Any | None = None
    stream_parameter: inspect.Parameter | None = None
    logger_parameter: inspect.Parameter | None = None
    for parameter in parameters:
        if parameter.kind not in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            raise ContractError(
                f"RPC method {func.__qualname__} cannot use *args or **kwargs"
            )
        annotation = hints.get(parameter.name)
        origin = get_origin(annotation)
        if parameter.name == "logger" or annotation is RpcLogger:
            if origin in (RpcInputStream, RpcDuplex):
                raise ContractError(
                    f"RPC method {func.__qualname__} cannot use logger as a stream"
                )
            if logger_parameter is not None:
                raise ContractError(
                    f"RPC method {func.__qualname__} has more than one logger"
                )
            logger_parameter = parameter
            continue
        if parameter.name not in hints:
            raise ContractError(
                f"Parameter {parameter.name!r} on {func.__qualname__} needs a type"
            )

        if origin in (RpcInputStream, RpcDuplex):
            if stream_parameter is not None:
                raise ContractError(
                    f"RPC method {func.__qualname__} has more than one stream"
                )
            stream_annotation = annotation
            stream_parameter = parameter
        else:
            request_parameters.append(parameter)

    request_model = (
        _model_class(hints[request_parameters[0].name])
        if len(request_parameters) == 1
        else None
    )
    if request_model is not None:
        request_type = request_model
        request_style = RequestStyle.MODEL
        request_fields: tuple[str, ...] = ()
        request_parameter = request_parameters[0].name
    else:
        request_type = _create_request_model(
            service_type,
            name,
            request_parameters,
            hints,
            func,
        )
        request_style = RequestStyle.EXPANDED
        request_fields = tuple(parameter.name for parameter in request_parameters)
        request_parameter = None

    request_ref = _type_ref(request_type)
    method_binding = MethodBinding(
        request_type=request_type,
        request_style=request_style,
        request_parameter=request_parameter,
        request_fields=request_fields,
        parameter_names=tuple(parameter.name for parameter in parameters),
        positional_only_parameters=frozenset(
            parameter.name
            for parameter in parameters
            if parameter.kind is inspect.Parameter.POSITIONAL_ONLY
        ),
        stream_parameter=(
            stream_parameter.name if stream_parameter is not None else None
        ),
        logger_parameter=(
            logger_parameter.name if logger_parameter is not None else None
        ),
    )

    if "return" not in hints:
        raise ContractError(f"RPC method {func.__qualname__} needs a return type")
    return_type = hints["return"]
    return_origin = get_origin(return_type)

    if stream_annotation is None and return_origin is AsyncIteratorABC:
        output_args = get_args(return_type)
        if len(output_args) != 1:
            raise ContractError(f"Invalid stream result on {func.__qualname__}")
        output_type = output_args[0]
        _require_model(output_type, f"stream item for {func.__qualname__}")
        contract = MethodContract(
            name=name,
            stream=StreamKind.SERVER,
            binding=binding,
            request=request_ref,
            output_item=_type_ref(output_type),
            balance=balance,
            request_style=request_style,
            request_fields=request_fields,
        )
        return _validated_method(contract, declared_stream, func, method_binding)

    if stream_annotation is None:
        _require_model(return_type, f"response for {func.__qualname__}")
        contract = MethodContract(
            name=name,
            stream=StreamKind.UNARY,
            binding=binding,
            request=request_ref,
            response=_type_ref(return_type),
            balance=balance,
            request_style=request_style,
            request_fields=request_fields,
        )
        return _validated_method(contract, declared_stream, func, method_binding)

    stream_origin = get_origin(stream_annotation)
    stream_args = get_args(stream_annotation)
    if stream_origin is RpcInputStream:
        if len(stream_args) != 1:
            raise ContractError(f"Invalid input stream on {func.__qualname__}")
        input_type = stream_args[0]
        _require_model(input_type, f"input stream item for {func.__qualname__}")
        _require_model(return_type, f"response for {func.__qualname__}")
        contract = MethodContract(
            name=name,
            stream=StreamKind.CLIENT,
            binding=binding,
            request=request_ref,
            response=_type_ref(return_type),
            input_item=_type_ref(input_type),
            balance=balance,
            request_style=request_style,
            request_fields=request_fields,
        )
        return _validated_method(contract, declared_stream, func, method_binding)

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
        contract = MethodContract(
            name=name,
            stream=StreamKind.DUPLEX,
            binding=binding,
            request=request_ref,
            response=response_ref,
            input_item=_type_ref(input_type),
            output_item=_type_ref(output_type),
            balance=balance,
            request_style=request_style,
            request_fields=request_fields,
        )
        return _validated_method(contract, declared_stream, func, method_binding)

    raise ContractError(f"Unsupported stream annotation on {func.__qualname__}")


@dataclass(frozen=True)
class _ExtractedMethod:
    contract: MethodContract
    binding: MethodBinding


def _validated_method(
    contract: MethodContract,
    declared_stream: StreamKind | None,
    func: Callable[..., Any],
    binding: MethodBinding,
) -> _ExtractedMethod:
    if declared_stream is not None and contract.stream is not declared_stream:
        raise ContractError(
            f"RPC method {func.__qualname__} is declared as "
            f"{declared_stream.value}, but its signature implies "
            f"{contract.stream.value}"
        )
    return _ExtractedMethod(contract=contract, binding=binding)


def _create_request_model(
    service_type: type[Any],
    method_name: str,
    parameters: list[inspect.Parameter],
    hints: dict[str, Any],
    func: Callable[..., Any],
) -> type[BaseModel]:
    fields: dict[str, tuple[Any, Any]] = {
        parameter.name: (
            hints[parameter.name],
            ... if parameter.default is inspect.Parameter.empty else parameter.default,
        )
        for parameter in parameters
    }
    try:
        field_definitions: Any = fields
        return create_model(
            _request_model_name(service_type, method_name),
            __module__=service_type.__module__,
            **field_definitions,
        )
    except Exception as exc:
        raise ContractError(
            f"Cannot build request model for {func.__qualname__}: {exc}"
        ) from exc


def _model_class(annotation: Any) -> type[BaseModel] | None:
    if inspect.isclass(annotation) and issubclass(annotation, BaseModel):
        return annotation
    return None


def _request_model_name(service_type: type[Any], method_name: str) -> str:
    suffix = "".join(
        part[:1].upper() + part[1:] for part in method_name.split("_") if part
    )
    return f"{service_type.__name__}{suffix}Request"


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
