from typing import Literal, NamedTuple, Any
import pytest
from typelime import Cache, MemoCache, LRUCache, Dataset, TypelessSample, CacheOp
import pickle


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
