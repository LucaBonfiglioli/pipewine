from abc import ABC, abstractmethod

from typelime._op_typing import AnyDataset, origin_type
from typelime._register import RegisterMeta
from typelime.dataset import Dataset, LazyDataset
from typelime.sample import Sample


class DatasetSourceMeta(RegisterMeta):
    def _type(self) -> str:
        return "source"


class DatasetSource[T: AnyDataset](ABC, metaclass=DatasetSourceMeta):
    @abstractmethod
    def __call__(self) -> T: ...

    @property
    def output_type(self):
        return origin_type(self.__call__.__annotations__["return"])


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
    def __call__(self) -> Dataset[T_SAMPLE]:
        self._prepare()
        return LazyDataset(self._size(), self._get_sample)
