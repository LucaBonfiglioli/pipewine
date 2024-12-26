from abc import ABC, abstractmethod

from pipewine._op_typing import AnyDataset, origin_type
from pipewine._register import RegisterCallbackMixin
from pipewine.dataset import Dataset
from pipewine.sample import Sample


class DatasetOperator[T_IN: AnyDataset, T_OUT: AnyDataset](ABC, RegisterCallbackMixin):
    @abstractmethod
    def __call__(self, x: T_IN) -> T_OUT: ...

    @property
    def input_type(self):
        return origin_type(self.__call__.__annotations__["x"])

    @property
    def output_type(self):
        return origin_type(self.__call__.__annotations__["return"])


class IdentityOp(DatasetOperator[Dataset, Dataset]):
    def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
        return x
