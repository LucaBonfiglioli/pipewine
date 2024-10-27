from abc import ABC, abstractmethod
from functools import partial

from typelime._op_typing import AnyDataset
from typelime.dataset import Dataset, _LazyDatasetWrapper
from typelime.operators.base import DatasetOperator
from typelime.sample import Sample


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


class LazyDatasetOperator[T_IN: AnyDataset, T_SAMPLE_OUT: Sample, T_STATE](
    DatasetOperator[T_IN, Dataset[T_SAMPLE_OUT]],
    _LazyOpInterface[T_IN, T_SAMPLE_OUT, T_STATE],
):
    def apply(self, x: T_IN) -> Dataset[T_SAMPLE_OUT]:
        state = self.prepare(x)
        return _LazyDatasetWrapper(
            partial(self.get_sample, state=state), self.size(state)
        )
