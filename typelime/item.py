import typing as t
from abc import ABC, abstractmethod

from typelime.parsers import Parser
from typelime.storage import ReadStorage


class Item[T: t.Any](ABC):
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

    def with_data(self, data: T) -> "MemoryItem[T]":
        return MemoryItem(data, self._get_parser(), shared=self.is_shared)

    def __call__(self) -> T:
        return self._get()


class MemoryItem[T: t.Any](Item[T]):
    def __init__(self, data: T, parser: Parser[T], shared: bool = False) -> None:
        self._data = data
        self._parser = parser
        self._shared = shared

    def _get(self) -> T:
        return self._data

    def _get_parser(self) -> Parser[T]:
        return self._parser

    def _is_shared(self) -> bool:
        return self._shared


class StoredItem[T: t.Any](Item[T]):
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

    @property
    def storage(self) -> ReadStorage:
        return self._storage


class CachedItem[T: t.Any](Item[T]):
    def __init__(self, source: Item[T]) -> None:
        self._source = source
        self._cache = None

    def _get(self) -> T:
        if self._cache is None:
            self._cache = self._source()
        return self._cache

    def _get_parser(self) -> Parser[T]:
        return self._source._get_parser()

    def _is_shared(self) -> bool:
        return self._source.is_shared

    @property
    def source(self) -> Item[T]:
        return self._source

    def source_recursive(self) -> Item[T]:
        source: Item[T] = self
        while isinstance(source, CachedItem):
            source = source.source
        return source
