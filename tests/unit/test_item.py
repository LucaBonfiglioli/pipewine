import json
from pathlib import Path
from typing import Any

import pytest

from typelime import (
    CachedItem,
    Item,
    ReadStorage,
    JSONParser,
    MemoryItem,
    Parser,
    StoredItem,
    YAMLParser,
)


class TestMemoryItem:
    def test_get(self) -> None:
        data = 15
        item = MemoryItem(data, JSONParser(), shared=False)
        assert item() == data

    def test_get_parser(self) -> None:
        parser: JSONParser = JSONParser()
        item = MemoryItem(15, parser, shared=False)
        assert item.parser == parser

    @pytest.mark.parametrize("shared", [True, False])
    def test_shared(self, shared: bool) -> None:
        item = MemoryItem(15, JSONParser(), shared=shared)
        assert item.is_shared == shared

    @pytest.mark.parametrize("data", [50, "hello", 8.5])
    @pytest.mark.parametrize("parser", [JSONParser(), YAMLParser()])
    @pytest.mark.parametrize("shared", [True, False])
    def test_with_data(self, data: Any, parser: Parser, shared: bool) -> None:
        item = MemoryItem(10, parser, shared=shared)
        new_item = item.with_data(data)
        assert new_item() == data
        assert new_item.parser == parser
        assert new_item.is_shared == shared


class MockStorage(ReadStorage):
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
        data = {"a": 10, "b": "hello", "c": [10, 20, 30]}
        storage = MockStorage(parser.dump(data))
        item = StoredItem(storage, parser, shared=False)
        assert storage.read_called == 0
        assert item() == data
        assert storage.read_called == 1
        assert item() == data
        assert storage.read_called == 2
        assert item() == data
        assert storage.read_called == 3

    def test_storage(self) -> None:
        parser: JSONParser = JSONParser()
        data = {"a": 10, "b": "hello", "c": [10, 20, 30]}
        storage = MockStorage(parser.dump(data))
        item = StoredItem(storage, parser, shared=False)
        assert isinstance(item.storage, MockStorage)
        assert item.storage is storage

    def test_get_parser(self) -> None:
        parser: JSONParser = JSONParser()
        item = StoredItem(MockStorage(b""), parser, shared=False)
        assert item.parser == parser

    @pytest.mark.parametrize("shared", [True, False])
    def test_shared(self, shared: bool) -> None:
        item: StoredItem = StoredItem(MockStorage(b""), JSONParser(), shared=shared)
        assert item.is_shared == shared


class MockItem(MemoryItem):
    def __init__(self, data: Any, parser: Parser, shared: bool = False) -> None:
        super().__init__(data, parser, shared)
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
    def test_shared(self, shared: bool) -> None:
        source_item = MockItem(10, JSONParser(), shared=shared)
        item = CachedItem(source_item)
        assert item.is_shared == shared

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

        assert item.source_recursive() == source_item
