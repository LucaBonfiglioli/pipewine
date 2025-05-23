import json
from pathlib import Path
from typing import Any

import pytest

from pipewine import (
    CachedItem,
    Item,
    Reader,
    JSONParser,
    MemoryItem,
    PickleParser,
    Parser,
    StoredItem,
    YAMLParser,
)


class TestMemoryItem:
    def test_get(self) -> None:
        value = 15
        item = MemoryItem(value, JSONParser(), shared=False)
        assert item() == value

    def test_get_parser(self) -> None:
        parser: JSONParser = JSONParser()
        item = MemoryItem(15, parser, shared=False)
        assert item.parser == parser

    @pytest.mark.parametrize("shared", [True, False])
    def test_shared(self, shared: bool) -> None:
        item = MemoryItem(15, JSONParser(), shared=shared)
        assert item.is_shared == shared

    @pytest.mark.parametrize("value", [50, "hello", 8.5])
    @pytest.mark.parametrize("parser", [JSONParser(), YAMLParser()])
    @pytest.mark.parametrize("shared", [True, False])
    def test_with_value(self, value: Any, parser: Parser, shared: bool) -> None:
        item = MemoryItem(10, parser, shared=shared)
        new_item = item.with_value(value)
        assert new_item() == value
        assert new_item.parser == parser
        assert new_item.is_shared == shared

    @pytest.mark.parametrize("parser", [JSONParser(), YAMLParser()])
    def test_with_parser(self, parser: Parser) -> None:
        item = MemoryItem(15, PickleParser())
        new_item = item.with_parser(parser)
        assert new_item.parser == parser

    @pytest.mark.parametrize("sharedness", [True, False])
    def test_with_sharedness(self, sharedness: bool) -> None:
        item = MemoryItem(15, PickleParser())
        new_item = item.with_sharedness(sharedness)
        assert new_item.is_shared == sharedness


class MockReader(Reader):
    def __init__(self, bytes_: bytes) -> None:
        super().__init__()
        self._bytes = bytes_
        self.read_called = 0

    def read(self) -> bytes:
        self.read_called += 1
        return self._bytes


class TestStoredItem:
    def test_get(self) -> None:
        parser: JSONParser = JSONParser()
        value = {"a": 10, "b": "hello", "c": [10, 20, 30]}
        reader = MockReader(parser.dump(value))
        item = StoredItem(reader, parser, shared=False)
        assert reader.read_called == 0
        assert item() == value
        assert reader.read_called == 1
        assert item() == value
        assert reader.read_called == 2
        assert item() == value
        assert reader.read_called == 3

    def test_reader(self) -> None:
        parser: JSONParser = JSONParser()
        value = {"a": 10, "b": "hello", "c": [10, 20, 30]}
        reader = MockReader(parser.dump(value))
        item = StoredItem(reader, parser, shared=False)
        assert isinstance(item.reader, MockReader)
        assert item.reader is reader

    def test_get_parser(self) -> None:
        parser: JSONParser = JSONParser()
        item = StoredItem(MockReader(b""), parser, shared=False)
        assert item.parser == parser

    @pytest.mark.parametrize("shared", [True, False])
    def test_shared(self, shared: bool) -> None:
        item: StoredItem = StoredItem(MockReader(b""), JSONParser(), shared=shared)
        assert item.is_shared == shared

    @pytest.mark.parametrize("sharedness", [True, False])
    def test_with_sharedness(self, sharedness: bool) -> None:
        item: StoredItem[dict] = StoredItem(MockReader(b""), JSONParser())
        new_item = item.with_sharedness(sharedness)
        assert new_item.is_shared == sharedness


class MockItem(MemoryItem):
    def __init__(self, value: Any, parser: Parser, shared: bool = False) -> None:
        super().__init__(value, parser, shared)
        self.get_called = 0

    def _get(self) -> Any:
        self.get_called += 1
        return super()._get()


class TestCachedItem:
    def test_get(self) -> None:
        source_item = MockItem(10, JSONParser(), shared=False)
        item = CachedItem(source_item)
        assert source_item.get_called == 0
        assert item() == 10
        assert source_item.get_called == 1
        assert item() == 10
        assert source_item.get_called == 1

    def test_get_parser(self) -> None:
        parser: JSONParser = JSONParser()
        source_item = MockItem(10, parser, shared=False)
        item = CachedItem(source_item)
        assert item.parser == parser

    @pytest.mark.parametrize("shared", [True, False])
    @pytest.mark.parametrize("cached_shared", [True, False, None])
    def test_shared(self, shared: bool, cached_shared: bool | None) -> None:
        source_item = MockItem(10, JSONParser(), shared=shared)
        item = CachedItem(source_item, shared=cached_shared)
        assert item.is_shared == (shared if (cached_shared is None) else cached_shared)

    @pytest.mark.parametrize("source_item", [MemoryItem(10, JSONParser())])
    def test_source(self, source_item: Item) -> None:
        item = CachedItem(source_item)
        assert item.source == source_item

    @pytest.mark.parametrize("source_item", [MemoryItem(10, JSONParser())])
    @pytest.mark.parametrize("n", [1, 2, 10])
    def test_source_recursive(self, source_item: Item, n: int) -> None:
        item: CachedItem = CachedItem(source_item)
        for _ in range(n - 1):
            item = CachedItem(item)

        assert item.source_recursive == source_item

    @pytest.mark.parametrize("sharedness", [True, False])
    def test_with_sharedness(self, sharedness: bool) -> None:
        item = CachedItem(MockItem(10, JSONParser()))
        new_item = item.with_sharedness(sharedness)
        assert new_item.is_shared == sharedness
