from abc import ABC, abstractmethod

from typelime._op_typing import AnyDataset
from typelime.dataset import Dataset, _LazyDatasetWrapper
from typelime.sample import Sample
from typelime.sources.base import DatasetSource


class DatasetSource[T: AnyDataset](ABC):
    @abstractmethod
    def generate(self) -> T:
        pass

    def __call__(self) -> T:
        return self.generate()


class _LazySourceInterface[T_SAMPLE: Sample](ABC):
    def _prepare(self) -> None:
        pass

    @abstractmethod
    def _size(self) -> int:
        pass

    @abstractmethod
    def _get_sample(self, idx: int) -> T_SAMPLE:
        pass


class LazyDatasetSource[T_SAMPLE: Sample](
    DatasetSource[Dataset[T_SAMPLE]], _LazySourceInterface[T_SAMPLE]
):
    def generate(self) -> Dataset[T_SAMPLE]:
        self._prepare()
        return _LazyDatasetWrapper(self._get_sample, size=self._size())
