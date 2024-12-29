from abc import ABC, ABCMeta, abstractmethod
from collections.abc import Iterable, KeysView
from typing import Any


class ParserRegistry:
    _registered_parsers: dict[str, type["Parser"]] = {}

    @classmethod
    def get(cls, key: str) -> type["Parser"] | None:
        return cls._registered_parsers.get(key)

    @classmethod
    def keys(cls) -> KeysView[str]:
        return cls._registered_parsers.keys()


class _ParserMeta(ABCMeta):
    def __new__(
        cls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        /,
        **kwargs: Any,
    ):
        the_cls: type["Parser"] = super().__new__(cls, name, bases, namespace, **kwargs)  # type: ignore
        extensions = the_cls.extensions()  # type: ignore
        if extensions is not None:
            ParserRegistry._registered_parsers.update({k: the_cls for k in extensions})
        return the_cls


class Parser[T: Any](ABC, metaclass=_ParserMeta):
    def __init__(self, type_: type[T] | None = None):
        super().__init__()
        self._type = type_

    @property
    def type_(self) -> type[T] | None:
        return self._type

    @abstractmethod
    def parse(self, data: bytes) -> T:
        pass

    @abstractmethod
    def dump(self, data: T) -> bytes:
        pass

    @classmethod
    @abstractmethod
    def extensions(cls) -> Iterable[str]:
        pass
