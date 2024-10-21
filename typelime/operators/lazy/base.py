import math
from abc import ABC, abstractmethod
from collections.abc import Callable
from functools import partial

from typelime.dataset import Dataset
from typelime.operators.base import AnyDataset
from typelime.operators.eager.base import _DatasetGenerator, _DatasetOperator
from typelime.sample import Sample


class _LazyDatasetWrapper[T: Sample](Dataset[T]):
    def __init__(
        self,
        get_sample: Callable[[int], T],
        size: int,
        index_fn: Callable[[int], int] | None = None,
    ) -> None:
        self._get_sample_fn = get_sample
        self._size = size
        self._index_fn = index_fn

    def size(self) -> int:
        return self._size

    def get_sample(self, idx: int) -> T:
        return self._get_sample_fn(self._index_fn(idx) if self._index_fn else idx)

    def get_slice(self, idx: slice) -> Dataset[T]:
        start, stop, step = idx.indices(self.size())
        return _LazyDatasetWrapper(
            self.get_sample,
            math.ceil((stop - start) / step),
            lambda x: x * step + start,
        )


class _LazyGenInterface[T_SAMPLE: Sample, T_STATE](ABC):
    @abstractmethod
    def prepare(self) -> T_STATE:
        pass

    @abstractmethod
    def size(self, state: T_STATE) -> int:
        pass

    @abstractmethod
    def get_sample(self, state: T_STATE, idx: int) -> T_SAMPLE:
        pass


class LazyDatasetGenerator[T_SAMPLE: Sample, T_STATE](
    _DatasetGenerator[Dataset[T_SAMPLE]], _LazyGenInterface[T_SAMPLE, T_STATE]
):
    def generate(self) -> Dataset[T_SAMPLE]:
        state = self.prepare()
        return _LazyDatasetWrapper(
            partial(self.get_sample, state), size=self.size(state)
        )


class _LazyOpInterface[T_IN: AnyDataset, T_SAMPLE_OUT: Sample, T_STATE](ABC):
    @abstractmethod
    def prepare(self, x: T_IN) -> T_STATE:
        pass

    @abstractmethod
    def size(self, state: T_STATE) -> int:
        pass

    @abstractmethod
    def get_sample(self, state: T_STATE, idx: int) -> T_SAMPLE_OUT:
        pass


class LazyManyToOneOperation[T_IN: AnyDataset, T_SAMPLE_OUT: Sample, T_STATE](
    _DatasetOperator[T_IN, Dataset[T_SAMPLE_OUT]],
    _LazyOpInterface[T_IN, T_SAMPLE_OUT, T_STATE],
):
    def apply(self, x: T_IN) -> Dataset[T_SAMPLE_OUT]:
        state = self.prepare(x)
        return _LazyDatasetWrapper(
            partial(self.get_sample, state=state), self.size(state)
        )
