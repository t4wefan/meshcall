"""Advisory contract warnings: quality, not correctness.

Hard rules (service name shape, parameter types, shape mismatches) raise
``ContractError``. Everything here is a *warning*: it never stops a human, it
carries a ``hint`` written as an instruction an agent can act on, and strict
mode (``MESHCALL_STRICT=1`` or ``python -W error::meshcall.MeshCallWarning``)
turns it into an error for CI and agents.

Each warning says how to silence it explicitly. Project-wide suppression is
``MESHCALL_IGNORE_WARNINGS=W001,W002``.
"""

from __future__ import annotations

import inspect
import os
import re
import warnings
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from meshcall.errors import ContractError
from meshcall.ir import MethodContract, ServiceContract

_MUTATING = re.compile(
    r"^(create|add|insert|update|set|put|patch|delete|remove|drop|clear|reset|"
    r"send|post|write|save|upload|cancel|revoke|issue|register|unregister|"
    r"start|stop|restart|apply|assign|move|rename|archive|restore|purge)(_|$)"
)
_READING = re.compile(
    r"^(get|list|read|fetch|find|search|query|count|describe|show|check|is|has|"
    r"exists|lookup|peek|preview|validate|estimate|stream|watch)(_|$)"
)


@dataclass(frozen=True)
class ContractWarning:
    """One advisory finding about a service contract."""

    code: str
    subject: str
    message: str
    hint: str
    silence: str
    location: str | None = None

    def render(self) -> str:
        where = f"\n  {self.location}" if self.location else ""
        return (
            f"{self.code} {self.subject}: {self.message}{where}"
            f"\n  hint: {self.hint}"
            f"\n  silence: {self.silence}"
        )


class MeshCallWarning(UserWarning):
    """Python ``warnings`` category for :class:`ContractWarning`."""

    def __init__(self, diagnostic: ContractWarning) -> None:
        super().__init__(diagnostic.render())
        self.diagnostic = diagnostic


def strict_mode() -> bool:
    return os.environ.get("MESHCALL_STRICT", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def ignored_codes() -> frozenset[str]:
    raw = os.environ.get("MESHCALL_IGNORE_WARNINGS", "")
    return frozenset(code.strip().upper() for code in raw.split(",") if code.strip())


def lint_contract(
    contract: ServiceContract,
    *,
    source: type[Any] | None = None,
) -> list[ContractWarning]:
    """Compute every advisory warning for ``contract``; pure, no side effects."""
    findings: list[ContractWarning] = []
    service_location = _location(source) if source is not None else None
    if contract.description is None:
        findings.append(
            ContractWarning(
                code="W001",
                subject=contract.name,
                message="service has no description",
                hint=(
                    "Add a one-line class docstring saying what the service is "
                    "for. It becomes the contract's description and the text "
                    "LLM callers and generated clients show."
                ),
                silence='@service(..., description="") to declare none on purpose',
                location=service_location,
            )
        )
    for method in contract.methods:
        subject = f"{contract.name}.{method.name}"
        location = (
            _location(getattr(source, method.name, None))
            if source is not None
            else None
        )
        findings.extend(_lint_method(method, subject, location))
        if contract.targeted and str(method.balance.kind) == "sticky":
            findings.append(
                ContractWarning(
                    code="W006",
                    subject=subject,
                    message="sticky balancing on a targeted service is never used",
                    hint=(
                        "Targeted services route by the instance id the caller "
                        "passes; the Router ignores the sticky key. Drop "
                        "Balance.sticky(...) or drop targeted=True."
                    ),
                    silence="remove the sticky policy",
                    location=location,
                )
            )
    return findings


def _lint_method(
    method: MethodContract, subject: str, location: str | None
) -> Iterable[ContractWarning]:
    annotations = method.annotations
    annotated = (
        annotations.idempotent or annotations.read_only or annotations.destructive
    )
    mutating = _MUTATING.match(method.name) is not None
    reading = _READING.match(method.name) is not None
    if method.description is None:
        yield ContractWarning(
            code="W001",
            subject=subject,
            message="method has no description",
            hint=(
                "Add a one-line docstring stating what the call does and when to "
                'use it, e.g. """Create a session and return it.""" Mention side '
                "effects. This becomes the tool description LLM callers see."
            ),
            silence='@method(description="") to declare none on purpose',
            location=location,
        )
    if mutating and not annotated:
        yield ContractWarning(
            code="W002",
            subject=subject,
            message="name suggests side effects but the method has no annotation",
            hint=(
                "Declare its semantics so retry policies and tools can reason "
                "about it: @method(idempotent=True) if repeating it is safe, "
                "@method(destructive=True) if it cannot be undone, or "
                "@method(read_only=True) if the name is misleading."
            ),
            silence="any of idempotent=, read_only=, destructive= on @method",
            location=location,
        )
    if annotations.read_only and mutating:
        yield ContractWarning(
            code="W003",
            subject=subject,
            message="read_only=True on a method whose name suggests mutation",
            hint=(
                "Either the name or the annotation is wrong. Rename the method "
                "(get_/list_/…) or drop read_only and declare idempotent= / "
                "destructive= instead."
            ),
            silence="fix the name or the annotation",
            location=location,
        )
    if annotations.destructive and reading:
        yield ContractWarning(
            code="W003",
            subject=subject,
            message="destructive=True on a method whose name suggests a read",
            hint=(
                "Either the name or the annotation is wrong. Rename the method "
                "to say what it changes, or drop destructive=."
            ),
            silence="fix the name or the annotation",
            location=location,
        )
    if method.deprecated == "":
        yield ContractWarning(
            code="W005",
            subject=subject,
            message="deprecated without naming a replacement",
            hint=(
                'Give callers somewhere to go: @method(deprecated="new_name"). '
                "Use a short sentence if there is no replacement."
            ),
            silence='@method(deprecated="no replacement")',
            location=location,
        )


def emit_contract_warnings(
    contract: ServiceContract, source: type[Any]
) -> list[ContractWarning]:
    """Lint, drop ignored codes, then warn or (strict) raise. Returns kept findings."""
    ignored = ignored_codes()
    findings = [
        w for w in lint_contract(contract, source=source) if w.code not in ignored
    ]
    if not findings:
        return []
    if strict_mode():
        rendered = "\n".join(f.render() for f in findings)
        raise ContractError(
            f"Strict mode: {len(findings)} contract warning(s) for {contract.name}\n"
            f"{rendered}"
        )
    for finding in findings:
        # stacklevel: emit → extract_service_contract → service.decorate → class body
        warnings.warn(MeshCallWarning(finding), stacklevel=4)
    return findings


def _location(obj: Any) -> str | None:
    try:
        target = inspect.unwrap(obj) if callable(obj) else obj
        file = inspect.getsourcefile(target)
        line = inspect.getsourcelines(target)[1]
    except (OSError, TypeError):
        return None
    return f"{file}:{line}" if file else None
