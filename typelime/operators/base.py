from abc import ABC, abstractmethod

from typelime._op_typing import AnyDataset
from typelime._setuppable import Setuppable


class DatasetOperator[T_IN: AnyDataset, T_OUT: AnyDataset](ABC, Setuppable):
    @abstractmethod
    def apply(self, x: T_IN) -> T_OUT:
        pass

    def __call__(self, x: T_IN) -> T_OUT:
        return self.apply(x)
