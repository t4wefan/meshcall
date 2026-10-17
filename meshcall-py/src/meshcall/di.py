from __future__ import annotations

import contextlib
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, Any, Self, get_args, get_origin, get_type_hints

from pydantic import TypeAdapter, ValidationError

from meshcall.context import RpcContext
from meshcall.errors import ContractError, InvalidArgumentError, invalid_argument
from meshcall.logging import RpcLogger

_UNSET = object()


class DependencyMarker:
    """Marker for a dependency-injected parameter or Annotated metadata."""

    def __init__(self, provider: Callable[..., Any] | None = None) -> None:
        if isinstance(provider, DependencyMarker):
            provider = provider.provider
        self.provider = provider

    def _provider(self) -> DependencyMarker:
        return self

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        # When used as @dependency without parentheses and then called directly,
        # or when used as @dependency() with parentheses on a function.
        if (
            self.provider is None
            and len(args) == 1
            and not kwargs
            and callable(args[0])
        ):
            return DependencyMarker(args[0])
        if self.provider is None:
            raise TypeError("DependencyMarker has no provider callable")
        return self.provider(*args, **kwargs)


def dependency(provider: Callable[..., Any] | None = None) -> Any:
    """Declare a dependency provider for service methods.

    May be used as:
    - ``@dependency`` on a sync/async function or generator
    - ``Annotated[T, dependency(provider)]``
    - ``Annotated[T, dependency()]`` (resolves ``T._provider()``)
    """
    return DependencyMarker(provider)


@dataclass(frozen=True)
class MetaMarker:
    """Marker for extracting a request metadata entry into a parameter."""

    key: str
    default: Any = _UNSET


def meta(key: str, *, default: Any = _UNSET) -> Any:
    """Declare a metadata-injected parameter (escape hatch for headers).

    Example: ``CallerId = Annotated[str | None, meta("x-user-id")]``
    """
    if not isinstance(key, str) or not key.strip():
        raise ContractError("meta() requires a non-empty metadata key")
    return MetaMarker(key=key.strip(), default=default)


@dataclass(frozen=True)
class MetaPlan:
    key: str
    target_type: Any
    has_default: bool
    default_value: Any

    def resolve(self, context: RpcContext) -> Any:
        raw = context.metadata.get(self.key)
        if raw is None:
            lowered = self.key.lower()
            for k, v in context.metadata.items():
                if k.lower() == lowered:
                    raw = v
                    break
        if raw is None:
            if self.has_default:
                return self.default_value
            raise InvalidArgumentError(
                f"Missing required metadata {self.key!r}",
                details={
                    "source": "server",
                    "reason": "missing_metadata",
                    "key": self.key,
                },
            )
        if self.target_type in (Any, str, object):
            return raw
        try:
            return TypeAdapter(self.target_type).validate_python(raw)
        except ValidationError as exc:
            raise invalid_argument(exc, "request") from None


@dataclass(frozen=True)
class DependencyParamSpec:
    name: str
    kind: str  # "context" | "logger" | "dependency" | "meta" | "default"
    plan: DependencyPlan | MetaPlan | None = None
    default_value: Any = None
    positional_only: bool = False


@dataclass(frozen=True)
class DependencyPlan:
    provider: Callable[..., Any]
    params: tuple[DependencyParamSpec, ...]


def unwrap_annotated(annotation: Any) -> tuple[Any, tuple[Any, ...]]:
    if get_origin(annotation) is Annotated:
        args = get_args(annotation)
        if args:
            return args[0], tuple(args[1:])
    return annotation, ()


def _allows_none(tp: Any) -> bool:
    if tp in (Any, None, type(None), object):
        return True
    args = get_args(tp)
    return any(arg is type(None) for arg in args)


def extract_injected_plan(
    parameter: inspect.Parameter,
    annotation: Any,
    owner_qualname: str,
    *,
    _visiting: frozenset[Callable[..., Any]] = frozenset(),
) -> DependencyPlan | MetaPlan | None:
    """Return a DependencyPlan or MetaPlan if ``parameter`` is injected, else None."""
    base_type, extras = unwrap_annotated(annotation)
    dep_marker: DependencyMarker | None = None
    meta_marker: MetaMarker | None = None

    for extra in extras:
        if isinstance(extra, DependencyMarker):
            dep_marker = extra
        elif isinstance(extra, MetaMarker):
            meta_marker = extra
        elif callable(extra) and hasattr(extra, "__meshcall_dependency__"):
            dep_marker = DependencyMarker(extra)

    default = parameter.default
    if isinstance(default, DependencyMarker):
        dep_marker = default
    elif isinstance(default, MetaMarker):
        meta_marker = default
    elif callable(default) and hasattr(default, "__meshcall_dependency__"):
        dep_marker = DependencyMarker(default)

    if meta_marker is not None:
        has_default = False
        default_val: Any = None
        if default is not inspect.Parameter.empty and not isinstance(
            default, (MetaMarker, DependencyMarker)
        ):
            has_default = True
            default_val = default
        elif meta_marker.default is not _UNSET:
            has_default = True
            default_val = meta_marker.default
        elif _allows_none(base_type):
            has_default = True
            default_val = None
        return MetaPlan(
            key=meta_marker.key,
            target_type=base_type,
            has_default=has_default,
            default_value=default_val,
        )

    if dep_marker is not None:
        provider = dep_marker.provider
        if provider is None:
            if inspect.isclass(base_type) and hasattr(base_type, "_provider"):
                provider_factory = getattr(base_type, "_provider", None)
                provider = provider_factory() if callable(provider_factory) else None
            elif inspect.isclass(base_type):
                provider = base_type
        if provider is None or not callable(provider):
            raise ContractError(
                f"Parameter {parameter.name!r} on {owner_qualname} declares a "
                "dependency without a callable provider"
            )
        return _compile_dependency_plan(
            provider,
            owner_qualname=owner_qualname,
            visiting=_visiting,
        )

    return None


def _compile_dependency_plan(
    provider: Callable[..., Any],
    *,
    owner_qualname: str,
    visiting: frozenset[Callable[..., Any]],
) -> DependencyPlan:
    if isinstance(provider, DependencyMarker):
        if provider.provider is None:
            raise ContractError(f"Dependency on {owner_qualname} has no provider")
        provider = provider.provider

    if provider in visiting:
        name = getattr(provider, "__qualname__", repr(provider))
        raise ContractError(
            f"Circular dependency detected while resolving {name} for {owner_qualname}"
        )

    next_visiting = visiting | {provider}
    try:
        hints = get_type_hints(provider, include_extras=True)
    except Exception:  # noqa: BLE001
        hints = {}

    sig = inspect.signature(provider)
    specs: list[DependencyParamSpec] = []
    for param in sig.parameters.values():
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue
        pos_only = param.kind is inspect.Parameter.POSITIONAL_ONLY
        ann = hints.get(param.name, param.annotation)
        base_ann, _ = unwrap_annotated(ann)
        if param.name in ("ctx", "context") or base_ann is RpcContext:
            specs.append(
                DependencyParamSpec(
                    name=param.name,
                    kind="context",
                    positional_only=pos_only,
                )
            )
            continue
        if param.name == "logger" or base_ann is RpcLogger:
            specs.append(
                DependencyParamSpec(
                    name=param.name,
                    kind="logger",
                    positional_only=pos_only,
                )
            )
            continue

        nested = extract_injected_plan(
            param,
            ann,
            getattr(provider, "__qualname__", owner_qualname),
            _visiting=next_visiting,
        )
        if nested is not None:
            specs.append(
                DependencyParamSpec(
                    name=param.name,
                    kind="meta" if isinstance(nested, MetaPlan) else "dependency",
                    plan=nested,
                    positional_only=pos_only,
                )
            )
            continue

        if param.default is not inspect.Parameter.empty:
            specs.append(
                DependencyParamSpec(
                    name=param.name,
                    kind="default",
                    default_value=param.default,
                    positional_only=pos_only,
                )
            )
            continue

        provider_name = getattr(provider, "__qualname__", repr(provider))
        raise ContractError(
            f"Dependency {provider_name} parameter {param.name!r} cannot be resolved; "
            "annotate it with dependency(...), meta(...), or RpcContext"
        )

    return DependencyPlan(provider=provider, params=tuple(specs))


class DependencyScope:
    """Per-call dependency resolution scope with caching and generator cleanup."""

    def __init__(self, *, context: RpcContext, logger: RpcLogger) -> None:
        self.context = context
        self.logger = logger
        self._cache: dict[Callable[..., Any], Any] = {}
        self._stack = contextlib.AsyncExitStack()

    async def __aenter__(self) -> Self:
        await self._stack.__aenter__()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool | None:
        return await self._stack.__aexit__(exc_type, exc, tb)  # type: ignore[arg-type]

    async def resolve(self, plan: DependencyPlan | MetaPlan) -> Any:
        if isinstance(plan, MetaPlan):
            return plan.resolve(self.context)
        if plan.provider in self._cache:
            return self._cache[plan.provider]

        # Resolve meta specs first so missing/invalid metadata fails fast before
        # acquiring resources in nested dependency providers.
        resolved_by_name: dict[str, Any] = {}
        for spec in plan.params:
            if spec.kind == "meta":
                assert spec.plan is not None
                resolved_by_name[spec.name] = await self.resolve(spec.plan)

        args: list[Any] = []
        kwargs: dict[str, Any] = {}
        for spec in plan.params:
            if spec.kind == "context":
                val = self.context
            elif spec.kind == "logger":
                val = self.logger
            elif spec.kind == "meta":
                val = resolved_by_name[spec.name]
            elif spec.kind == "dependency":
                assert spec.plan is not None
                val = await self.resolve(spec.plan)
            else:
                val = spec.default_value
            if spec.positional_only:
                args.append(val)
            else:
                kwargs[spec.name] = val

        fn = plan.provider
        if inspect.isasyncgenfunction(fn):
            cm = contextlib.asynccontextmanager(fn)(*args, **kwargs)
            value = await self._stack.enter_async_context(cm)
        elif inspect.isgeneratorfunction(fn):
            sync_cm = contextlib.contextmanager(fn)(*args, **kwargs)
            value = self._stack.enter_context(sync_cm)
        elif inspect.iscoroutinefunction(fn):
            value = await fn(*args, **kwargs)
        else:
            res = fn(*args, **kwargs)
            if inspect.isawaitable(res):
                value = await res
            else:
                value = res

        self._cache[plan.provider] = value
        return value
