"""`Item` base class and implementations to represent data items in Pipewine."""

from abc import ABC, abstractmethod
from typing import Any, Self

from pipewine.parsers import Parser
from pipewine.reader import Reader


class Item[T: Any](ABC):
    """Base class for all Pipewine items. An item holds a reference to a single,
    serializable unit of data of parameterized type `T`.

    It provides methods to access the data, the way it is parsed, and whether it is
    considered shared or not.

    Item instances are immutable, all methods that modify the item return instead a new
    object with the desired changes, applying no in-place modifications to the
    original object.

    Every subclass must implement the `_get`, `_get_parser`, `_is_shared` and
    `with_sharedness` methods to provide the item's functionality.
    """

    @abstractmethod
    def _get(self) -> T:
        """Return the data held by the item."""
        pass

    @abstractmethod
    def _get_parser(self) -> Parser[T]:
        """Return the parser used to parse the data."""
        pass

    @abstractmethod
    def _is_shared(self) -> bool:
        """Return whether the item is shared or not."""
        pass

    @property
    def parser(self) -> Parser[T]:
        """Return the parser used to parse the data."""
        return self._get_parser()

    @property
    def is_shared(self) -> bool:
        """Return whether the item is shared or not."""
        return self._is_shared()

    def with_value(self, value: T) -> "MemoryItem[T]":
        """Change the value referenced by the item, returning a new item with the new
        value. The returned item will always be an instance of `MemoryItem`, as it is
        the type of item that holds references to values stored directly in memory.

        Args:
            value (T): New value to reference.

        Returns:
            MemoryItem[T]: New item with the new value.
        """
        return MemoryItem(value, self._get_parser(), shared=self.is_shared)

    def with_parser(self, parser: Parser[T]) -> "MemoryItem[T]":
        """Change the parser used to parse the data, returning a new item with the new
        parser. The returned item will always be an instance of `MemoryItem`, because
        changing the parser requires the data to be loaded first and stored in memory.

        Args:
            parser (Parser[T]): New parser to use.

        Returns:
            MemoryItem[T]: New item with the new parser.
        """
        return MemoryItem(self(), parser, shared=self.is_shared)

    @abstractmethod
    def with_sharedness(self, shared: bool) -> Self:
        """Change the sharedness of the item, returning a new item with the new
        sharedness. Changing the sharedness of an item does not require the data to be
        loaded, so the returned item will always be of the same type as the original.

        Args:
            shared (bool): New sharedness to set.

        Returns:
            Self: New item with the new sharedness.
        """
        pass

    def __call__(self) -> T:
        """Return the data held by the item."""
        return self._get()


class MemoryItem[T: Any](Item[T]):
    """A `MemoryItem` is an `Item` that holds a reference to a value stored directly in
    memory. It is the most common type of item, as it is used to represent data that is
    already loaded and parsed, typically used to store results of computations or
    intermediate data.
    """

    def __init__(self, value: T, parser: Parser[T], shared: bool = False) -> None:
        """
        Args:
            value (T): The value associated with the item.
            parser (Parser[T]): The parser used to eventually serialize the value in
                case the item is later stored in a file.
            shared (bool, optional): The sharedness of the item. Defaults to False.
        """
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
    """A `StoredItem` is an `Item` that reads data from an external source, such as a
    file or a database. It holds a reference to a `Reader` and a `Parser` that are used
    to read and parse the data when it is requested.
    """

    def __init__(self, reader: Reader, parser: Parser[T], shared: bool = False) -> None:
        """
        Args:
            reader (Reader): The reader used to read the data from the external source.
            parser (Parser[T]): The parser used to parse the data read by the reader and
                to eventually serialize it in case the item is later stored somewhere
                else.
            shared (bool, optional): The sharedness of the item. Defaults to False.
        """
        self._reader = reader
        self._parser = parser
        self._shared = shared

    def _get(self) -> T:
        return self._parser.parse(self._reader.read())

    def _get_parser(self) -> Parser[T]:
        return self._parser

    def _is_shared(self) -> bool:
        return self._shared

    def with_sharedness(self, shared: bool) -> Self:
        return type(self)(self._reader, self._parser, shared=shared)

    @property
    def reader(self) -> Reader:
        """Return the reader used to read the data from the external source."""
        return self._reader


class CachedItem[T: Any](Item[T]):
    """A `CachedItem` is an `Item` that wraps another item and caches the value it
    returns when it is requested for the first time. Subsequent requests will return
    the cached value without calling the wrapped item again.
    """

    def __init__(self, source: Item[T], shared: bool | None = None) -> None:
        """
        Args:
            source (Item[T]): The item to wrap and cache.
            shared (bool | None, optional): The sharedness of the item. If `None`, the
                sharedness of the item is the same as the sharedness of the wrapped item.
                Defaults to None.
        """
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
        """Return the wrapped item."""
        return self._source

    @property
    def source_recursive(self) -> Item[T]:
        """Return the original source item, unwrapping any cached items in between."""
        source: Item[T] = self
        while isinstance(source, CachedItem):
            source = source.source
        return source
