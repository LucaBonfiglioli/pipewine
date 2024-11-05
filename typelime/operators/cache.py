from abc import ABC, abstractmethod
from threading import RLock

from typelime.dataset import Dataset, LazyDataset
from typelime.mappers import CacheMapper
from typelime.operators.base import DatasetOperator
from typelime.sample import Sample


class Cache[K, V](ABC):
    def __init__(self) -> None:
        self._lock = RLock()

    @abstractmethod
    def _clear(self) -> None: ...

    @abstractmethod
    def _get(self, key: K) -> V | None: ...

    @abstractmethod
    def _put(self, key: K, value: V) -> None: ...

    @abstractmethod
    def _params(self) -> tuple: ...

    def clear(self) -> None:
        with self._lock:
            self._clear()

    def get(self, key: K) -> V | None:
        with self._lock:
            return self._get(key)

    def put(self, key: K, value: V) -> None:
        with self._lock:
            self._put(key, value)


class MemoCache[K, V](Cache[K, V]):
    def __init__(self) -> None:
        super().__init__()
        self._memo: dict[K, V] = {}

    def _clear(self) -> None:
        self._memo.clear()

    def _get(self, key: K) -> V | None:
        return self._memo.get(key)

    def _put(self, key: K, value: V) -> None:
        self._memo[key] = value

    def _params(self) -> tuple:
        return tuple()


class LRUCache[K, V](Cache[K, V]):
    _PREV, _NEXT, _KEY, _VALUE = 0, 1, 2, 3

    def __init__(self, maxsize: int) -> None:
        super().__init__()
        self._maxsize = maxsize
        self._dll = []
        self._dll[:] = [self._dll, self._dll, None, None]
        self._mp: dict[K, list] = {}

    def _clear(self) -> None:
        self._mp.clear()
        self._dll[:] = [self._dll, self._dll, None, None]

    def _get(self, key: K) -> V | None:
        with self._lock:
            link = self._mp.get(key)
            if link is not None:
                link_prev, link_next, _key, value = link
                link_prev[self._NEXT] = link_next
                link_next[self._PREV] = link_prev
                last = self._dll[self._PREV]
                last[self._NEXT] = self._dll[self._PREV] = link
                link[self._PREV] = last
                link[self._NEXT] = self._dll
                return value

    def _put(self, key: K, value: V) -> None:
        if key in self._mp:
            self._mp[key][self._VALUE] = value
            self.get(key)  # Set key as mru
        elif len(self._mp) >= self._maxsize:
            oldroot = self._dll
            oldroot[self._KEY] = key
            oldroot[self._VALUE] = value
            self._dll = oldroot[self._NEXT]
            oldkey = self._dll[self._KEY]
            oldvalue = self._dll[self._VALUE]
            self._dll[self._KEY] = self._dll[self._VALUE] = None
            del self._mp[oldkey]
            self._mp[key] = oldroot
        else:
            last = self._dll[self._PREV]
            link = [last, self._dll, key, value]
            last[self._NEXT] = self._dll[self._PREV] = self._mp[key] = link

    def _params(self) -> tuple:
        return (self._maxsize,)


class CacheOp[T: Sample](DatasetOperator[Dataset[T], LazyDataset[T]]):
    class _CacheState:
        def __init__(self, dataset: Dataset[T], cache: Cache[int, T]) -> None:
            super().__init__()
            self._dataset = dataset
            self._cache = cache
            self._cache_mapper = CacheMapper[T]()

        def __call__(self, idx: int) -> T:
            result = self._cache.get(idx)
            if result is None:
                result = self._cache_mapper(idx, self._dataset[idx])
                self._cache.put(idx, result)
            return result

        def __getstate__(self):  # pragma: no cover
            return {
                "_dataset": self._dataset,
                "_cache": (type(self._cache), self._cache._params()),
            }

        def __setstate__(self, state):  # pragma: no cover
            self._dataset = state["_dataset"]
            cache_t, cache_prm = state["_cache"]
            self._cache = cache_t(*cache_prm)
            self._cache_mapper = CacheMapper()

    def apply(self, x: Dataset[T]) -> LazyDataset[T]:
        cachestate = CacheOp._CacheState(x, MemoCache[int, T]())
        return LazyDataset(len(x), cachestate)
