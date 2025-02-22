"""Base classes for Pipewine datasets."""

import math
from typing import overload
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from functools import partial

from pipewine.sample import Sample


class Dataset[T: Sample](ABC, Sequence[T]):
    """Base class for all Pipewine datasets. A dataset is a collection of samples
    that can be iterated over, sliced, and indexed, implementing the `Sequence`
    protocol.

    Subclasses must implement the `size`, `get_sample`, and `get_slice` abstract
    methods to define the dataset behavior.
    """

    @abstractmethod
    def size(self) -> int:
        """Return the number of samples in the dataset, same as `len()`."""
        pass

    @abstractmethod
    def get_sample(self, idx: int) -> T:
        """Return the sample at the given index."""
        pass

    @abstractmethod
    def get_slice(self, idx: slice) -> "Dataset[T]":
        """Return a new dataset containing the samples in the given slice."""
        pass

    def __len__(self) -> int:
        return self.size()

    @overload
    def __getitem__(self, idx: int) -> T: ...
    @overload
    def __getitem__(self, idx: slice) -> "Dataset[T]": ...
    def __getitem__(self, idx: int | slice) -> T | "Dataset[T]":
        if isinstance(idx, int):
            if idx >= self.size():
                raise IndexError(idx)
            return self.get_sample(idx)
        else:
            return self.get_slice(idx)


class ListDataset[T: Sample](Dataset[T]):
    """Simple dataset implementation that wraps a list of samples."""

    def __init__(self, samples: Sequence[T]) -> None:
        """
        Args:
            samples (Sequence[T]): List of samples to wrap in the dataset.
        """
        super().__init__()
        self._samples = samples

    def get_sample(self, idx: int) -> T:
        return self._samples[idx]

    def get_slice(self, idx: slice) -> "Dataset[T]":
        return self.__class__(self._samples[idx])

    def size(self) -> int:
        return len(self._samples)


class LazyDataset[T: Sample](Dataset[T]):
    """Dataset implementation that lazily generates samples on demand, calling a
    user-provided function to get the requested samples.
    """

    def __init__(
        self,
        size: int,
        get_sample_fn: Callable[[int], T],
        index_fn: Callable[[int], int] | None = None,
    ) -> None:
        """
        Args:
            size (int): Number of samples in the dataset.
            get_sample_fn (Callable[[int], T]): Function that returns the sample at
                the given index.
            index_fn (Callable[[int], int] | None, optional): Additional function that
                can be used to change the index before calling `get_sample_fn` with it.
                Defaults to None, in which case the index is passed as-is to
                `get_sample_fn`.
        """
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
