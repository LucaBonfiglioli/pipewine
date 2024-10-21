from abc import ABC, abstractmethod
from functools import partial

from typelime.dataset import Dataset, _LazyDatasetWrapper
from typelime.generators.eager.base import _DatasetGenerator
from typelime.sample import Sample


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
