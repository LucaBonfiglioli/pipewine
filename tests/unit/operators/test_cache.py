import pickle
import random
from typing import Any, Literal, NamedTuple

import pytest

from pipewine import (
    Cache,
    CacheOp,
    Dataset,
    FIFOCache,
    LIFOCache,
    LRUCache,
    MemoCache,
    MRUCache,
    RRCache,
    TypelessSample,
)


class CacheCall(NamedTuple):
    op: Literal["get", "put", "clear"]
    args: list
    result: Any


class TestCache:
    def _test_cache(self, cache: Cache, calls: list[CacheCall]) -> None:
        for call in calls:
            if call.op == "get":
                result = cache.get(call.args[0])
            elif call.op == "put":
                result = cache.put(*call.args)  # type: ignore[func-returns-value]
            else:
                result = cache.clear()  # type: ignore[func-returns-value]

            assert result == call.result

        re_cache = pickle.loads(pickle.dumps(cache))
        assert isinstance(re_cache, Cache)


class TestMemoCache(TestCache):
    @pytest.mark.parametrize(
        "calls",
        [
            [
                CacheCall("get", ["a"], None),
                CacheCall("get", ["b"], None),
                CacheCall("clear", [], None),
            ],
            [
                CacheCall("put", ["a", 10], None),
                CacheCall("put", ["b", 20], None),
                CacheCall("put", ["c", 30], None),
                CacheCall("get", ["d"], None),
                CacheCall("get", ["b"], 20),
                CacheCall("put", ["a", 40], None),
                CacheCall("put", ["d", 50], None),
                CacheCall("get", ["a"], 40),
                CacheCall("put", ["e", 60], None),
                CacheCall("get", ["a"], 40),
                CacheCall("get", ["b"], 20),
                CacheCall("get", ["c"], 30),
                CacheCall("get", ["d"], 50),
                CacheCall("get", ["e"], 60),
                CacheCall("put", ["f", 1000], None),
                CacheCall("get", ["f"], 1000),
                CacheCall("get", ["a"], 40),
                CacheCall("put", ["c", 2000], None),
                CacheCall("put", ["a", 8], None),
                CacheCall("get", ["a"], 8),
                CacheCall("get", ["b"], 20),
                CacheCall("get", ["c"], 2000),
                CacheCall("get", ["d"], 50),
                CacheCall("get", ["e"], 60),
                CacheCall("get", ["f"], 1000),
                CacheCall("clear", [], None),
                CacheCall("get", ["a"], None),
                CacheCall("get", ["b"], None),
                CacheCall("get", ["c"], None),
                CacheCall("get", ["d"], None),
                CacheCall("get", ["e"], None),
            ],
        ],
    )
    def test_memo_cache(self, calls: list[CacheCall]) -> None:
        cache: MemoCache[str, int] = MemoCache()
        self._test_cache(cache, calls)


class _MockRandint:
    def __init__(self) -> None:
        self._count = 0

    def __call__(self, a: int, b: int) -> int:
        res = a + self._count
        self._count += 1
        if res == b:
            self._count = 0
        return res


class TestRRCache(TestCache):
    @pytest.mark.parametrize(
        ["maxlen", "calls"],
        [
            [
                1,
                [
                    CacheCall("get", ["a"], None),
                    CacheCall("put", ["b", 10], None),
                    CacheCall("get", ["b"], 10),
                    CacheCall("put", ["b", 20], None),
                    CacheCall("get", ["b"], 20),
                    CacheCall("put", ["a", 10], None),
                    CacheCall("get", ["a"], 10),
                    CacheCall("get", ["b"], None),
                    CacheCall("clear", [], None),
                    CacheCall("get", ["a"], None),
                    CacheCall("get", ["b"], None),
                ],
            ],
            [
                3,
                [
                    CacheCall("put", ["a", 10], None),
                    CacheCall("put", ["b", 20], None),
                    CacheCall("put", ["c", 30], None),
                    CacheCall("get", ["a"], 10),
                    CacheCall("get", ["b"], 20),
                    CacheCall("get", ["c"], 30),
                    CacheCall("put", ["d", 40], None),
                    CacheCall("get", ["a"], None),
                    CacheCall("get", ["d"], 40),
                    CacheCall("get", ["b"], 20),
                    CacheCall("get", ["c"], 30),
                    CacheCall("put", ["e", 50], None),
                    CacheCall("get", ["b"], None),
                    CacheCall("get", ["d"], 40),
                    CacheCall("get", ["e"], 50),
                    CacheCall("get", ["c"], 30),
                    CacheCall("put", ["f", 60], None),
                    CacheCall("get", ["c"], None),
                    CacheCall("get", ["d"], 40),
                    CacheCall("get", ["e"], 50),
                    CacheCall("get", ["f"], 60),
                    CacheCall("put", ["g", 70], None),
                    CacheCall("get", ["d"], None),
                    CacheCall("get", ["g"], 70),
                    CacheCall("get", ["e"], 50),
                    CacheCall("get", ["f"], 60),
                    CacheCall("put", ["h", 80], None),
                    CacheCall("get", ["e"], None),
                    CacheCall("get", ["g"], 70),
                    CacheCall("get", ["h"], 80),
                    CacheCall("get", ["f"], 60),
                ],
            ],
        ],
    )
    def test_rr_cache(
        self, monkeypatch: pytest.MonkeyPatch, maxlen: int, calls: list[CacheCall]
    ) -> None:
        monkeypatch.setattr(random, "randint", _MockRandint())
        cache: RRCache[str, int] = RRCache(maxlen)
        self._test_cache(cache, calls)


class TestFIFOCache(TestRRCache):
    @pytest.mark.parametrize(
        ["maxlen", "calls"],
        [
            [
                1,
                [
                    CacheCall("get", ["a"], None),
                    CacheCall("put", ["b", 10], None),
                    CacheCall("get", ["b"], 10),
                    CacheCall("put", ["b", 20], None),
                    CacheCall("get", ["b"], 20),
                    CacheCall("put", ["a", 10], None),
                    CacheCall("get", ["a"], 10),
                    CacheCall("get", ["b"], None),
                    CacheCall("clear", [], None),
                    CacheCall("get", ["a"], None),
                    CacheCall("get", ["b"], None),
                ],
            ],
            [
                3,
                [
                    CacheCall("put", ["a", 10], None),
                    CacheCall("put", ["b", 20], None),
                    CacheCall("put", ["c", 30], None),
                    CacheCall("get", ["a"], 10),
                    CacheCall("get", ["b"], 20),
                    CacheCall("get", ["c"], 30),
                    CacheCall("put", ["d", 40], None),
                    CacheCall("get", ["a"], None),
                    CacheCall("get", ["d"], 40),
                    CacheCall("get", ["b"], 20),
                    CacheCall("get", ["c"], 30),
                    CacheCall("put", ["e", 50], None),
                    CacheCall("get", ["b"], None),
                    CacheCall("get", ["d"], 40),
                    CacheCall("get", ["e"], 50),
                    CacheCall("get", ["c"], 30),
                    CacheCall("put", ["f", 60], None),
                    CacheCall("get", ["c"], None),
                    CacheCall("get", ["d"], 40),
                    CacheCall("get", ["e"], 50),
                    CacheCall("get", ["f"], 60),
                    CacheCall("put", ["g", 70], None),
                    CacheCall("get", ["d"], None),
                    CacheCall("get", ["g"], 70),
                    CacheCall("get", ["e"], 50),
                    CacheCall("get", ["f"], 60),
                    CacheCall("put", ["h", 80], None),
                    CacheCall("get", ["e"], None),
                    CacheCall("get", ["g"], 70),
                    CacheCall("get", ["h"], 80),
                    CacheCall("get", ["f"], 60),
                ],
            ],
        ],
    )
    def test_fifo_cache(
        self, monkeypatch: pytest.MonkeyPatch, maxlen: int, calls: list[CacheCall]
    ) -> None:
        monkeypatch.setattr(random, "randint", _MockRandint())
        cache: FIFOCache[str, int] = FIFOCache(maxlen)
        self._test_cache(cache, calls)


class TestLIFOCache(TestRRCache):
    @pytest.mark.parametrize(
        ["maxlen", "calls"],
        [
            [
                1,
                [
                    CacheCall("get", ["a"], None),
                    CacheCall("put", ["b", 10], None),
                    CacheCall("get", ["b"], 10),
                    CacheCall("put", ["b", 20], None),
                    CacheCall("get", ["b"], 20),
                    CacheCall("put", ["a", 10], None),
                    CacheCall("get", ["a"], 10),
                    CacheCall("get", ["b"], None),
                    CacheCall("clear", [], None),
                    CacheCall("get", ["a"], None),
                    CacheCall("get", ["b"], None),
                ],
            ],
            [
                3,
                [
                    CacheCall("put", ["a", 10], None),
                    CacheCall("put", ["b", 20], None),
                    CacheCall("put", ["c", 30], None),
                    CacheCall("get", ["a"], 10),
                    CacheCall("get", ["b"], 20),
                    CacheCall("get", ["c"], 30),
                    CacheCall("put", ["d", 40], None),
                    CacheCall("get", ["c"], None),
                    CacheCall("get", ["a"], 10),
                    CacheCall("get", ["b"], 20),
                    CacheCall("get", ["d"], 40),
                    CacheCall("put", ["e", 50], None),
                    CacheCall("get", ["d"], None),
                    CacheCall("get", ["a"], 10),
                    CacheCall("get", ["b"], 20),
                    CacheCall("get", ["e"], 50),
                    CacheCall("put", ["f", 60], None),
                    CacheCall("get", ["e"], None),
                    CacheCall("get", ["a"], 10),
                    CacheCall("get", ["b"], 20),
                    CacheCall("get", ["f"], 60),
                    CacheCall("put", ["g", 70], None),
                    CacheCall("get", ["f"], None),
                    CacheCall("get", ["a"], 10),
                    CacheCall("get", ["b"], 20),
                    CacheCall("get", ["g"], 70),
                    CacheCall("put", ["h", 80], None),
                    CacheCall("get", ["g"], None),
                    CacheCall("get", ["a"], 10),
                    CacheCall("get", ["b"], 20),
                    CacheCall("get", ["h"], 80),
                ],
            ],
        ],
    )
    def test_lifo_cache(
        self, monkeypatch: pytest.MonkeyPatch, maxlen: int, calls: list[CacheCall]
    ) -> None:
        monkeypatch.setattr(random, "randint", _MockRandint())
        cache: LIFOCache[str, int] = LIFOCache(maxlen)
        self._test_cache(cache, calls)


class TestLRUCache(TestCache):
    @pytest.mark.parametrize(
        ["maxlen", "calls"],
        [
            [
                5,
                [
                    CacheCall("get", ["a"], None),
                    CacheCall("get", ["b"], None),
                    CacheCall("clear", [], None),
                ],
            ],
            [
                5,
                [
                    CacheCall("put", ["a", 10], None),
                    CacheCall("put", ["b", 20], None),
                    CacheCall("put", ["c", 30], None),
                    CacheCall("get", ["d"], None),
                    CacheCall("get", ["b"], 20),
                    CacheCall("put", ["a", 40], None),
                    CacheCall("put", ["d", 50], None),
                    CacheCall("get", ["a"], 40),
                    CacheCall("put", ["e", 60], None),
                    CacheCall("get", ["a"], 40),
                    CacheCall("get", ["b"], 20),
                    CacheCall("get", ["c"], 30),
                    CacheCall("get", ["d"], 50),
                    CacheCall("get", ["e"], 60),
                    CacheCall("put", ["f", 1000], None),
                    CacheCall("get", ["f"], 1000),
                    CacheCall("get", ["a"], None),
                    CacheCall("put", ["c", 2000], None),
                    CacheCall("put", ["a", 8], None),
                    CacheCall("get", ["a"], 8),
                    CacheCall("get", ["b"], None),
                    CacheCall("get", ["c"], 2000),
                    CacheCall("get", ["d"], 50),
                    CacheCall("get", ["e"], 60),
                    CacheCall("get", ["f"], 1000),
                    CacheCall("clear", [], None),
                    CacheCall("get", ["a"], None),
                    CacheCall("get", ["b"], None),
                    CacheCall("get", ["c"], None),
                    CacheCall("get", ["d"], None),
                    CacheCall("get", ["e"], None),
                ],
            ],
        ],
    )
    def test_lru_cache(self, maxlen: int, calls: list[CacheCall]) -> None:
        cache: LRUCache[str, int] = LRUCache(maxlen)
        self._test_cache(cache, calls)


class TestMRUCache(TestCache):
    @pytest.mark.parametrize(
        ["maxlen", "calls"],
        [
            [
                5,
                [
                    CacheCall("get", ["a"], None),
                    CacheCall("get", ["b"], None),
                    CacheCall("clear", [], None),
                ],
            ],
            [
                5,
                [
                    CacheCall("put", ["a", 10], None),
                    CacheCall("put", ["b", 20], None),
                    CacheCall("put", ["c", 30], None),
                    CacheCall("get", ["d"], None),
                    CacheCall("get", ["b"], 20),
                    CacheCall("put", ["a", 40], None),
                    CacheCall("put", ["d", 50], None),
                    CacheCall("get", ["a"], 40),
                    CacheCall("put", ["e", 60], None),
                    CacheCall("get", ["a"], 40),
                    CacheCall("get", ["b"], 20),
                    CacheCall("get", ["c"], 30),
                    CacheCall("get", ["d"], 50),
                    CacheCall("get", ["e"], 60),
                    CacheCall("put", ["f", 1000], None),
                    CacheCall("get", ["f"], 1000),
                    CacheCall("get", ["e"], None),
                    CacheCall("get", ["a"], 40),
                    CacheCall("get", ["b"], 20),
                    CacheCall("get", ["c"], 30),
                    CacheCall("get", ["d"], 50),
                    CacheCall("put", ["c", 2000], None),
                    CacheCall("get", ["f"], 1000),
                    CacheCall("get", ["e"], None),
                    CacheCall("get", ["a"], 40),
                    CacheCall("get", ["b"], 20),
                    CacheCall("get", ["c"], 2000),
                    CacheCall("get", ["d"], 50),
                    CacheCall("put", ["e", 8], None),
                    CacheCall("get", ["e"], 8),
                    CacheCall("get", ["d"], None),
                    CacheCall("put", ["d", 15], None),
                    CacheCall("get", ["e"], None),
                    CacheCall("get", ["d"], 15),
                    CacheCall("clear", [], None),
                    CacheCall("get", ["a"], None),
                    CacheCall("get", ["b"], None),
                    CacheCall("get", ["c"], None),
                    CacheCall("get", ["d"], None),
                    CacheCall("get", ["e"], None),
                ],
            ],
        ],
    )
    def test_mru_cache(self, maxlen: int, calls: list[CacheCall]) -> None:
        cache: MRUCache[str, int] = MRUCache(maxlen)
        self._test_cache(cache, calls)


class MyDataset(Dataset[TypelessSample]):
    def __init__(self) -> None:
        super().__init__()
        self.getitem_called = 0

    def get_sample(self, idx: int) -> TypelessSample:
        self.getitem_called += 1
        return TypelessSample()

    def get_slice(self, idx: slice) -> Dataset[TypelessSample]:
        raise NotImplementedError()

    def size(self) -> int:
        return 10


class TestCacheOp:
    def test_call(self) -> None:
        op = CacheOp(MemoCache)
        dataset = MyDataset()
        cached = op(dataset)
        for _ in range(5):
            cached[0]
        assert dataset.getitem_called == 1

    def test_input_type(self) -> None:
        assert issubclass(CacheOp(MemoCache).input_type, Dataset)

    def test_output_type(self) -> None:
        assert issubclass(CacheOp(MemoCache).output_type, Dataset)
