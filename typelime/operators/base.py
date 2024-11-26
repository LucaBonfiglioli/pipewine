from abc import ABC, abstractmethod

from typelime._op_typing import AnyDataset, origin_type
from typelime._register import RegisterMeta


class DatasetOperatorMeta(RegisterMeta):
    def _type(self) -> str:
        return "operator"


class DatasetOperator[T_IN: AnyDataset, T_OUT: AnyDataset](
    ABC, metaclass=DatasetOperatorMeta
):
    @abstractmethod
    def __call__(self, x: T_IN) -> T_OUT: ...

    @property
    def input_type(self):
        return origin_type(self.__call__.__annotations__["x"])

    @property
    def output_type(self):
        return origin_type(self.__call__.__annotations__["return"])
