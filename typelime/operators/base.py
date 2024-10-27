from abc import ABC, abstractmethod

from typelime._op_typing import AnyDataset


class DatasetOperator[T_IN: AnyDataset, T_OUT: AnyDataset](ABC):
    @abstractmethod
    def apply(self, x: T_IN) -> T_OUT:
        pass

    def __call__(self, x: T_IN) -> T_OUT:
        return self.apply(x)
