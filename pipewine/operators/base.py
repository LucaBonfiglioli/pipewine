from abc import ABC, abstractmethod
from inspect import get_annotations

from pipewine._op_typing import AnyDataset, origin_type
from pipewine._register import LoopCallbackMixin
from pipewine.dataset import Dataset
from pipewine.sample import Sample


class DatasetOperator[T_IN: AnyDataset, T_OUT: AnyDataset](ABC, LoopCallbackMixin):
    @abstractmethod
    def __call__(self, x: T_IN) -> T_OUT: ...

    @property
    def input_type(self):
        return origin_type(get_annotations(self.__call__, eval_str=True)["x"])

    @property
    def output_type(self):
        return origin_type(get_annotations(self.__call__, eval_str=True)["return"])


class IdentityOp(DatasetOperator[Dataset, Dataset]):
    def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
        return x
