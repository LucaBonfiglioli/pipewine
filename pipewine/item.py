from abc import ABC, abstractmethod
from typing import Any, Self

from pipewine.parsers import Parser
from pipewine.storage import ReadStorage


class Item[T: Any](ABC):
    @abstractmethod
    def _get(self) -> T: ...

    @abstractmethod
    def _get_parser(self) -> Parser[T]: ...

    @abstractmethod
    def _is_shared(self) -> bool: ...

    @property
    def parser(self) -> Parser[T]:
        return self._get_parser()

    @property
    def is_shared(self) -> bool:
        return self._is_shared()

    def with_value(self, value: T) -> "MemoryItem[T]":
        return MemoryItem(value, self._get_parser(), shared=self.is_shared)

    def with_parser(self, parser: Parser[T]) -> "MemoryItem[T]":
        return MemoryItem(self(), parser, shared=self.is_shared)

    @abstractmethod
    def with_sharedness(self, shared: bool) -> Self: ...

    def __call__(self) -> T:
        return self._get()


class MemoryItem[T: Any](Item[T]):
    def __init__(self, value: T, parser: Parser[T], shared: bool = False) -> None:
        self._value = value
        self._parser = parser
        self._shared = shared

    def _get(self) -> T:
        return self._value

    def _get_parser(self) -> Parser[T]:
        return self._parser

    def _is_shared(self) -> bool:
        return self._shared

    def with_sharedness(self, shared: bool) -> Self:
        return type(self)(self._value, self._parser, shared=shared)


class StoredItem[T: Any](Item[T]):
    def __init__(
        self, storage: ReadStorage, parser: Parser[T], shared: bool = False
    ) -> None:
        self._storage = storage
        self._parser = parser
        self._shared = shared

    def _get(self) -> T:
        return self._parser.parse(self._storage.read())

    def _get_parser(self) -> Parser[T]:
        return self._parser

    def _is_shared(self) -> bool:
        return self._shared

    def with_sharedness(self, shared: bool) -> Self:
        return type(self)(self._storage, self._parser, shared=shared)

    @property
    def storage(self) -> ReadStorage:
        return self._storage


class CachedItem[T: Any](Item[T]):
    def __init__(self, source: Item[T], shared: bool | None = None) -> None:
        self._source = source
        self._cache = None
        self._shared = shared

    def _get(self) -> T:
        if self._cache is None:
            self._cache = self._source()
        return self._cache

    def _get_parser(self) -> Parser[T]:
        return self._source._get_parser()

    def _is_shared(self) -> bool:
        if self._shared is None:
            return self._source.is_shared
        return self._shared

    def with_sharedness(self, shared: bool) -> Self:
        return type(self)(self._source, shared=shared)

    @property
    def source(self) -> Item[T]:
        return self._source

    def source_recursive(self) -> Item[T]:
        source: Item[T] = self
        while isinstance(source, CachedItem):
            source = source.source
        return source
