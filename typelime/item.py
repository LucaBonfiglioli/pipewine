import typing as t
from abc import ABC, abstractmethod

from typelime.parsers import Parser
from typelime.storage import Storage


class Item[T: t.Any](ABC):
    @abstractmethod
    def get(self) -> T:
        pass

    @abstractmethod
    def get_parser(self) -> Parser[T]:
        pass

    def with_data(self, data: T) -> "MemoryItem[T]":
        return MemoryItem(data, self.get_parser())

    def __call__(self) -> T:
        return self.get()


class StoredItem[T: t.Any](Item[T]):
    def __init__(self, storage: Storage, parser: Parser[T]) -> None:
        self._storage = storage
        self._parser = parser

    def get(self) -> T:
        return self._parser.parse(self._storage.read())

    def get_parser(self) -> Parser[T]:
        return self._parser


class MemoryItem[T: t.Any](Item[T]):
    def __init__(self, data: T, parser: Parser[T]) -> None:
        self._data = data
        self._parser = parser

    def get(self) -> T:
        return self._data

    def get_parser(self) -> Parser[T]:
        return self._parser
