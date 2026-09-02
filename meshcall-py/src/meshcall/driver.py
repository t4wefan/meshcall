from __future__ import annotations

import threading
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

from meshcall.errors import DriverStateError
from meshcall.transport import FrameConnection

if TYPE_CHECKING:
    from meshcall.server import ServerRuntime


@dataclass(frozen=True)
class DriverBinding:
    owner_id: str
    generation: str


class ServerDriver(ABC):
    def __init__(self) -> None:
        self._binding_lock = threading.Lock()
        self._binding: DriverBinding | None = None

    def acquire_binding(self, owner_id: str) -> DriverBinding:
        with self._binding_lock:
            if self._binding is not None:
                raise DriverStateError(
                    f"Driver is already bound to {self._binding.owner_id}"
                )
            binding = DriverBinding(owner_id, uuid.uuid4().hex)
            self._binding = binding
            return binding

    def release_binding(self, binding: DriverBinding) -> None:
        with self._binding_lock:
            if self._binding != binding:
                raise DriverStateError("Cannot release a stale driver binding")
            self._binding = None

    def check_binding(self, binding: DriverBinding) -> None:
        with self._binding_lock:
            if self._binding != binding:
                raise DriverStateError("Driver binding is stale")

    @abstractmethod
    async def start(
        self,
        binding: DriverBinding,
        runtime: ServerRuntime,
    ) -> None: ...

    @abstractmethod
    async def stop(self, binding: DriverBinding) -> None: ...


class ClientDriver(ABC):
    @abstractmethod
    async def connect(self) -> FrameConnection: ...

    @abstractmethod
    async def close(self) -> None: ...
