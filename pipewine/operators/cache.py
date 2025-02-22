"""Operators for caching the results of other operators to avoid recomputation."""

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
    """Key-value cache abstraction with thread-safe operations on arbitrary keys and
    values.

    Subclasses must implement the `_clear`, `_get`, and `_put` methods to define the
    cache behavior and eviction policy. These methods are automatically made thread-safe
    by the `Cache` class, so there is no need to worry about acquiring and releasing
    locks when implementing them.
    """

    def __init__(self) -> None:
        """Initialize the locks necessary for thread-safety, always make sure to call
        this constructor when inheriting from this class.
        """
        self._lock = RLock()

    @abstractmethod
    def _clear(self) -> None:
        """Clear the cache, removing all key-value pairs."""
        pass

    @abstractmethod
    def _get(self, key: K) -> V | None:
        """Get the value associated with the given key.

        Args:
            key (K): Key to look up in the cache.

        Returns:
            V | None: Value associated with the key, or `None` if the key is not present
                in the cache.
        """
        pass

    @abstractmethod
    def _put(self, key: K, value: V) -> None:
        """Put a key-value pair in the cache.

        Args:
            key (K): Key to associate with the value.
            value (V): Value to store in the cache.
        """
        pass

    def clear(self) -> None:
        """Clear the cache, removing all key-value pairs."""
        with self._lock:
            self._clear()

    def get(self, key: K) -> V | None:
        """Get the value associated with the given key.

        Args:
            key (K): Key to look up in the cache.

        Returns:
            V | None: Value associated with the key, or `None` if the key is not present
                in the cache.
        """
        with self._lock:
            return self._get(key)

    def put(self, key: K, value: V) -> None:
        """Put a key-value pair in the cache.

        Args:
            key (K): Key to associate with the value.
            value (V): Value to store in the cache.
        """
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
    """Simple cache that stores key-value pairs in a dictionary, with no eviction policy
    or size limit, useful for memoization of functions with a bounded number of
    arguments.

    Avoid using this cache for large datasets or unbounded keys, as it will consume
    memory indefinitely.
    """

    def __init__(self) -> None:
        """Initialize the cache with an empty dictionary."""
        super().__init__()
        self._memo: dict[K, V] = {}

    def _clear(self) -> None:
        self._memo.clear()

    def _get(self, key: K) -> V | None:
        return self._memo.get(key)

    def _put(self, key: K, value: V) -> None:
        self._memo[key] = value


class RRCache[K, V](Cache[K, V]):
    """Random Replacement (RR) cache that evicts a random key-value pair when the cache
    is full and a new key-value pair is inserted. This cache is useful for scenarios
    where the order of access to the keys is not known or when no suitable eviction
    policy is particularly effective.
    """

    def __init__(self, maxsize: int = 32) -> None:
        """
        Args:
            maxsize (int, optional): Maximum number of key-value pairs to store in the
                cache. Defaults to 32.
        """

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
    """First-In-First-Out (FIFO) cache that evicts the least recently **inserted**
    key-value pair when the cache is full and a new key-value pair is inserted. This
    cache is useful for scenarios where the order of access to the keys is known and
    the oldest keys are likely to be the least useful.
    """

    def __init__(self, maxsize: int = 32) -> None:
        """
        Args:
            maxsize (int, optional): Maximum number of key-value pairs to store in the
                cache. Defaults to 32.
        """
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
    """Last-In-First-Out (LIFO) cache that evicts the most recently **inserted**
    key-value pair when the cache is full and a new key-value pair is inserted. This
    cache is useful for scenarios where the data is accessed in long repeated cycles,
    where the most recently inserted keys are likely to not going to be used again soon.
    """

    def __init__(self, maxsize: int = 32) -> None:
        """
        Args:
            maxsize (int, optional): Maximum number of key-value pairs to store in the
                cache. Defaults to 32.
        """
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
    """Least Recently Used (LRU) cache that evicts the least recently **accessed**
    key-value pair when the cache is full and a new key-value pair is inserted. This
    cache is useful for scenarios where the most recently accessed keys are likely to
    be the most useful in the near future.
    """

    _PREV, _NEXT, _KEY, _VALUE = 0, 1, 2, 3

    def __init__(self, maxsize: int = 32) -> None:
        """
        Args:
            maxsize (int, optional): Maximum number of key-value pairs to store in the
                cache. Defaults to 32.
        """
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
    """Most Recently Used (MRU) cache that evicts the most recently **accessed**
    key-value pair when the cache is full and a new key-value pair is inserted. This
    cache is useful for scenarios where the most recently accessed keys are likely not
    going to be accessed again soon.
    """

    _PREV, _NEXT, _KEY, _VALUE = 0, 1, 2, 3

    def __init__(self, maxsize: int = 32) -> None:
        """
        Args:
            maxsize (int, optional): Maximum number of key-value pairs to store in the
                cache. Defaults to 32.
        """

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
    """Operator that caches the results of another operator to avoid recomputation.
    See the "Cache" section in the documentation for more information.
    """

    def __init__(self, cache_type: type[Cache], **cache_params) -> None:
        """
        Args:
            cache_type (type[Cache]): Type of cache to use, must be a subclass of
                `Cache`.
            cache_params (Any): Additional parameters to pass to the cache constructor.
        """
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
    """Operator that caches the items of the samples in a dataset to avoid
    recomputation. Essentially the same as `MapOp(CacheMapper())`.
    """

    def __init__(self) -> None:
        """Initialize the operator with a `MapOp` that uses a `CacheMapper`."""
        super().__init__()
        self._map_op = MapOp(CacheMapper())

    def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
        return self._map_op(x)


class MemorizeEverythingOp(DatasetOperator[Dataset, Dataset]):
    """Operator that caches all samples in a dataset to avoid recomputation eagerly.
    This operator will block until all samples are computed and loaded in memory, so it
    may not be suitable for large datasets, or when memory is a concern.

    In these cases, consider using a *Checkpoint*, see the "Cache" section in the
    documentation for more information.
    """

    def __init__(self, grabber: Grabber | None = None) -> None:
        """
        Args:
            grabber (Grabber | None, optional): Grabber to use for loading the samples
                from the dataset. Defaults to None, in which case a new `Grabber` is
                created.
        """
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
