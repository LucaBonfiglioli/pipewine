from abc import ABC, abstractmethod

from pipewine._op_typing import AnyDataset, origin_type
from pipewine._register import LoopCallbackMixin
from pipewine.dataset import Dataset, LazyDataset
from pipewine.sample import Sample
from inspect import get_annotations


class DatasetSource[T: AnyDataset](ABC, LoopCallbackMixin):
    @abstractmethod
    def __call__(self) -> T: ...

    @property
    def output_type(self):
        return origin_type(get_annotations(self.__call__, eval_str=True)["return"])


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
