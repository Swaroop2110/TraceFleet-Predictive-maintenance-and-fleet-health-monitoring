"""Plug-in registry with filesystem discovery.

This is the mechanism behind the build rule that adding a capability means
adding one file. A plug-in module declares itself with a decorator:

    @register_detector("tailgating_proxy")
    def detect(window, event, params, weight): ...

and the registry finds it by scanning its own package directory at import time.
No central list to edit, so a new detector needs no change to any existing file,
and deleting the file removes the capability cleanly.

Discovery is deliberately eager and happens once per package. Lazy loading would
mean a typo in a plug-in surfaces during the first Kafka batch instead of at
startup.
"""

from __future__ import annotations

import importlib
import pkgutil
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any, TypeVar

T = TypeVar("T")


class RegistryError(RuntimeError):
    """Raised for duplicate registrations or unknown lookups."""


@dataclass(frozen=True)
class Entry[T]:
    """A registered plug-in plus the metadata declared at its decorator."""

    key: str
    target: T
    module: str
    metadata: dict[str, Any] = field(default_factory=dict)


class Registry[T]:
    """Named collection of plug-ins discovered from a package directory."""

    def __init__(self, name: str, *, allow_override: bool = False) -> None:
        self.name = name
        self.allow_override = allow_override
        self._entries: dict[str, Entry[T]] = {}
        self._discovered: set[str] = set()
        self._lock = threading.RLock()

    # -- registration ----------------------------------------------------
    def register(self, key: str, **metadata: Any) -> Callable[[T], T]:
        """Decorator registering ``key``. Import-time side effect by design."""

        def decorator(target: T) -> T:
            self.add(key, target, **metadata)
            return target

        return decorator

    def add(self, key: str, target: T, **metadata: Any) -> None:
        with self._lock:
            existing = self._entries.get(key)
            if existing is not None and not self.allow_override:
                # Two plug-ins claiming one name means one silently shadows the
                # other, and which one wins depends on filesystem ordering.
                if existing.target is target:
                    return
                raise RegistryError(
                    f"{self.name}: {key!r} is already registered by {existing.module!r}; names must be unique"
                )
            module = getattr(target, "__module__", "<unknown>")
            self._entries[key] = Entry(key=key, target=target, module=module, metadata=metadata)

    def unregister(self, key: str) -> None:
        with self._lock:
            self._entries.pop(key, None)

    # -- lookup ----------------------------------------------------------
    def get(self, key: str) -> T:
        entry = self._entries.get(key)
        if entry is None:
            raise RegistryError(f"{self.name}: no plug-in named {key!r}; available: {sorted(self._entries)}")
        return entry.target

    def get_or_none(self, key: str) -> T | None:
        entry = self._entries.get(key)
        return entry.target if entry else None

    def entry(self, key: str) -> Entry[T] | None:
        return self._entries.get(key)

    def all(self) -> dict[str, T]:
        return {key: entry.target for key, entry in self._entries.items()}

    def entries(self) -> dict[str, Entry[T]]:
        return dict(self._entries)

    def keys(self) -> list[str]:
        return sorted(self._entries)

    def metadata(self) -> dict[str, dict[str, Any]]:
        """Plug-in inventory, used by the admin API and ``feature.py list``."""
        return {key: {"module": entry.module, **entry.metadata} for key, entry in sorted(self._entries.items())}

    def __contains__(self, key: object) -> bool:
        return key in self._entries

    def __len__(self) -> int:
        return len(self._entries)

    def __iter__(self) -> Iterator[Entry[T]]:
        return iter(self._entries.values())

    # -- discovery -------------------------------------------------------
    def discover(
        self,
        package_name: str,
        package_paths: list[str],
        *,
        skip: tuple[str, ...] = (),
    ) -> list[str]:
        """Import every sibling module so decorators run. Idempotent."""
        with self._lock:
            if package_name in self._discovered:
                return sorted(self._entries)
            self._discovered.add(package_name)

        for info in sorted(pkgutil.iter_modules(package_paths), key=lambda item: item.name):
            # ``_``-prefixed modules are shared helpers, not plug-ins.
            if info.name.startswith("_") or info.name in skip:
                continue
            importlib.import_module(f"{package_name}.{info.name}")
        return sorted(self._entries)

    def clear(self) -> None:
        """Reset. Tests use this to isolate registry state between cases."""
        with self._lock:
            self._entries.clear()
            self._discovered.clear()
