from abc import ABC, abstractmethod

from typelime.operators.base import Setuppable, AnyDataset


class _DatasetGenerator[T: AnyDataset](ABC):
    @abstractmethod
    def generate(self) -> T:
        pass

    def __call__(self) -> T:
        return self.generate()


class EagerDatasetGenerator[T: AnyDataset](_DatasetGenerator[T], Setuppable):
    pass


class _DatasetOperator[T_IN: AnyDataset, T_OUT: AnyDataset](ABC):
    @abstractmethod
    def apply(self, x: T_IN) -> T_OUT:
        pass

    def __call__(self, x: T_IN) -> T_OUT:
        return self.apply(x)


class EagerDatasetOperation[T_IN: AnyDataset, T_OUT: AnyDataset](
    _DatasetOperator[T_IN, T_OUT], Setuppable
):
    pass
