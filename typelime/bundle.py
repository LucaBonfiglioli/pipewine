from abc import ABCMeta
from dataclasses import dataclass
from typing import Any

from typing_extensions import Self


class BundleMeta(ABCMeta):
    def __new__(
        cls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        /,
        **kwds: Any,
    ):
        the_cls = super().__new__(cls, name, bases, namespace)
        if not kwds.get("_is_root"):
            the_cls = dataclass(the_cls)
        return the_cls


class Bundle[T](metaclass=BundleMeta, _is_root=True):
    def __init__(self, **kwargs: T) -> None: ...

    def __getattr__(self, name: str) -> T: ...

    def as_dict(self) -> dict[str, T]:
        return self.__dict__

    @classmethod
    def from_dict(cls, **data: T) -> Self:
        return cls(**data)
