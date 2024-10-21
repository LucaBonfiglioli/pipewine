from abc import ABC, abstractmethod

from typelime.operators.base import Setuppable
from typelime.operators.base import AnySample


class SampleOperator[T_IN: AnySample, T_OUT: AnySample](ABC, Setuppable):
    @abstractmethod
    def apply(self, x: T_IN) -> T_OUT:
        pass

    def __call__(self, x: T_IN) -> T_OUT:
        return self.apply(x)
