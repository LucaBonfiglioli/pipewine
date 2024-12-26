import math
import typing as t
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from functools import partial

from pipewine.sample import Sample


class Dataset[T: Sample](ABC, Sequence[T]):
    @abstractmethod
    def size(self) -> int:
        pass

    @abstractmethod
    def get_sample(self, idx: int) -> T:
        pass

    @abstractmethod
    def get_slice(self, idx: slice) -> "Dataset[T]":
        pass

    def __len__(self) -> int:
        return self.size()

    @t.overload
    def __getitem__(self, idx: int) -> T: ...
    @t.overload
    def __getitem__(self, idx: slice) -> "Dataset[T]": ...
    def __getitem__(self, idx: int | slice) -> T | "Dataset[T]":
        if isinstance(idx, int):
            if idx >= self.size():
                raise IndexError(idx)
            return self.get_sample(idx)
        else:
            return self.get_slice(idx)


class ListDataset[T: Sample](Dataset[T]):
    def __init__(self, samples: Sequence[T]) -> None:
        super().__init__()
        self._samples = samples

    def get_sample(self, idx: int) -> T:
        return self._samples[idx]

    def get_slice(self, idx: slice) -> "Dataset[T]":
        return self.__class__(self._samples[idx])

    def size(self) -> int:
        return len(self._samples)


class LazyDataset[T: Sample](Dataset[T]):
    def __init__(
        self,
        size: int,
        get_sample_fn: Callable[[int], T],
        index_fn: Callable[[int], int] | None = None,
    ) -> None:
        self._size = size
        self._get_sample_fn = get_sample_fn
        self._index_fn = index_fn

    def size(self) -> int:
        return self._size

    def get_sample(self, idx: int) -> T:
        return self._get_sample_fn(self._index_fn(idx) if self._index_fn else idx)

    def _slice_fn(self, step: int, start: int, x: int) -> int:
        return x * step + start

    def get_slice(self, idx: slice) -> Dataset[T]:
        start, stop, step = idx.indices(self.size())
        return LazyDataset(
            max(0, math.ceil((stop - start) / step)),
            self.get_sample,
            partial(self._slice_fn, step, start),
        )
