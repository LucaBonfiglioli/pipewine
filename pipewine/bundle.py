from abc import ABCMeta
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Self, dataclass_transform


@dataclass_transform(kw_only_default=True)
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
            the_cls = dataclass(the_cls)  # type: ignore
        return the_cls


class Bundle[T](metaclass=BundleMeta, _is_root=True):
    def __init__(self, /, **kwargs: T) -> None:
        pass

    def as_dict(self) -> dict[str, T]:
        return self.__dict__

    @classmethod
    def from_dict(cls, **data: T) -> Self:
        return cls(**data)

    def __getstate__(self) -> dict[str, T]:
        return self.as_dict()

    def __setstate__(self, data: dict[str, T]) -> None:
        for k, v in data.items():
            setattr(self, k, v)


class DefaultBundle[T](Bundle[T]):
    def __init__(self, factory: Callable[[str], T], /, **kwargs: T) -> None:
        self._factory = factory
        self._data = kwargs

    def __getattr__(self, name: str) -> T:
        if name not in self._data:
            self._data[name] = self._factory(name)
        return self._data[name]

    def as_dict(self) -> dict[str, T]:
        return self._data
