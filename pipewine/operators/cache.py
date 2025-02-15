import random
import weakref
from abc import ABC, abstractmethod
from collections import deque
from functools import partial
from threading import RLock
from typing import Any
from uuid import uuid4

from pipewine.dataset import Dataset, LazyDataset
from pipewine.grabber import Grabber, InheritedData
from pipewine.mappers import CacheMapper
from pipewine.operators.base import DatasetOperator
from pipewine.operators.functional import MapOp
from pipewine.sample import Sample


class Cache[K, V](ABC):
    def __init__(self) -> None:
        self._lock = RLock()

    @abstractmethod
    def _clear(self) -> None: ...

    @abstractmethod
    def _get(self, key: K) -> V | None: ...

    @abstractmethod
    def _put(self, key: K, value: V) -> None: ...

    def clear(self) -> None:
        with self._lock:
            self._clear()

    def get(self, key: K) -> V | None:
        with self._lock:
            return self._get(key)

    def put(self, key: K, value: V) -> None:
        with self._lock:
            self._put(key, value)

    def __getstate__(self) -> dict[str, Any]:
        data = {**self.__dict__}
        del data["_lock"]
        return data

    def __setstate__(self, data: dict[str, Any]) -> None:
        self._lock = RLock()
        for k, v in data.items():
            setattr(self, k, v)


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


class RRCache[K, V](Cache[K, V]):
    def __init__(self, maxsize: int = 32) -> None:
        super().__init__()
        self._mp: dict[K, V] = {}
        self._keys: list[K] = []
        self._maxsize = maxsize

    def _clear(self) -> None:
        self._mp.clear()
        self._keys.clear()

    def _get(self, key: K) -> V | None:
        return self._mp.get(key)

    def _put(self, key: K, value: V) -> None:
        if len(self._keys) < self._maxsize:
            self._keys.append(key)
        else:
            idx = random.randint(0, self._maxsize - 1)
            prev_k = self._keys[idx]
            self._keys[idx] = key
            del self._mp[prev_k]
        self._mp[key] = value


class FIFOCache[K, V](Cache[K, V]):
    def __init__(self, maxsize: int = 32) -> None:
        super().__init__()
        self._mp: dict[K, V] = {}
        self._keys: deque[K] = deque()
        self._maxsize = maxsize

    def _clear(self) -> None:
        self._mp.clear()
        self._keys.clear()

    def _get(self, key: K) -> V | None:
        return self._mp.get(key)

    def _put(self, key: K, value: V) -> None:
        if len(self._keys) < self._maxsize:
            self._keys.append(key)
        else:
            evicted = self._keys.popleft()
            self._keys.append(key)
            del self._mp[evicted]
        self._mp[key] = value


class LIFOCache[K, V](Cache[K, V]):
    def __init__(self, maxsize: int = 32) -> None:
        super().__init__()
        self._mp: dict[K, V] = {}
        self._keys: list[K] = []
        self._maxsize = maxsize

    def _clear(self) -> None:
        self._mp.clear()
        self._keys.clear()

    def _get(self, key: K) -> V | None:
        return self._mp.get(key)

    def _put(self, key: K, value: V) -> None:
        if len(self._keys) < self._maxsize:
            self._keys.append(key)
        else:
            evicted = self._keys[-1]
            self._keys[-1] = key
            del self._mp[evicted]
        self._mp[key] = value


class LRUCache[K, V](Cache[K, V]):
    _PREV, _NEXT, _KEY, _VALUE = 0, 1, 2, 3

    def __init__(self, maxsize: int = 32) -> None:
        super().__init__()
        self._maxsize = maxsize
        self._dll: list = []
        self._dll[:] = [self._dll, self._dll, None, None]
        self._mp: dict[K, list] = {}

    def _clear(self) -> None:
        self._mp.clear()
        self._dll[:] = [self._dll, self._dll, None, None]

    def _get(self, key: K) -> V | None:
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
        return None

    def _put(self, key: K, value: V) -> None:
        if key in self._mp:
            self._mp[key][self._VALUE] = value
            self._get(key)  # Set key as mru
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


class MRUCache[K, V](Cache[K, V]):
    _PREV, _NEXT, _KEY, _VALUE = 0, 1, 2, 3

    def __init__(self, maxsize: int = 32) -> None:
        super().__init__()
        self._maxsize = maxsize
        self._dll: list = []
        self._dll[:] = [self._dll, self._dll, None, None]
        self._mp: dict[K, list] = {}

    def _clear(self) -> None:
        self._mp.clear()
        self._dll[:] = [self._dll, self._dll, None, None]

    def _get(self, key: K) -> V | None:
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
        return None

    def _put(self, key: K, value: V) -> None:
        if key in self._mp:
            self._mp[key][self._VALUE] = value
            self._get(key)  # Set key as mru
        elif len(self._mp) >= self._maxsize:
            mru = self._dll[self._PREV]
            oldkey = mru[self._KEY]
            oldvalue = mru[self._VALUE]
            mru[self._KEY] = key
            mru[self._VALUE] = value
            del self._mp[oldkey]
            self._mp[key] = mru
        else:
            last = self._dll[self._PREV]
            link = [last, self._dll, key, value]
            last[self._NEXT] = self._dll[self._PREV] = self._mp[key] = link


class CacheOp(DatasetOperator[Dataset, Dataset]):
    def __init__(self, cache_type: type[Cache], **cache_params) -> None:
        super().__init__()
        self._cache_mapper: CacheMapper = CacheMapper()
        self._cache_type = cache_type
        self._cache_params = cache_params

    def _get_sample[T: Sample](self, dataset: Dataset[T], cache_id: str, idx: int) -> T:
        cache: Cache[int, T] = InheritedData.data[cache_id]
        result = cache.get(idx)
        if result is None:
            result = self._cache_mapper(idx, dataset[idx])
            cache.put(idx, result)
        return result

    def _finalize_cache(self, id_: str) -> None:
        if id_ in InheritedData.data:  # pragma: no branch
            del InheritedData.data[id_]

    def __call__[T: Sample](self, x: Dataset[T]) -> LazyDataset[T]:
        cache = self._cache_type(**self._cache_params)
        id_ = uuid4().hex
        InheritedData.data[id_] = cache
        dataset = LazyDataset(len(x), partial(self._get_sample, x, id_))
        weakref.finalize(dataset, self._finalize_cache, id_=id_)
        return dataset


class ItemCacheOp(DatasetOperator[Dataset, Dataset]):
    def __init__(self) -> None:
        super().__init__()
        self._map_op = MapOp(CacheMapper())

    def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
        return self._map_op(x)


class MemorizeEverythingOp(DatasetOperator[Dataset, Dataset]):
    def __init__(self, grabber: Grabber | None = None) -> None:
        super().__init__()
        self._grabber = grabber or Grabber()
        self._cache_mapper: CacheMapper = CacheMapper()

    def _get_sample(self, cache_id: str, idx: int) -> Sample:
        cache: Cache[int, Sample] = InheritedData.data[cache_id]
        result = cache.get(idx)
        assert result is not None
        return result

    def _finalize_cache(self, id_: str) -> None:
        if id_ in InheritedData.data:  # pragma: no branch
            del InheritedData.data[id_]

    def __call__(self, x: Dataset) -> Dataset:
        cache = MemoCache()
        id_ = uuid4().hex
        InheritedData.data[id_] = cache
        for i, sample in self.loop(x, self._grabber, "Caching"):
            cache.put(i, self._cache_mapper(i, sample))
        dataset = LazyDataset(len(x), partial(self._get_sample, id_))
        weakref.finalize(dataset, self._finalize_cache, id_=id_)
        return dataset
