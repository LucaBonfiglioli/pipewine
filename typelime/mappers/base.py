from abc import ABC, abstractmethod

from typelime._setuppable import Setuppable
from typelime._op_typing import AnySample


class Mapper[T_IN: AnySample, T_OUT: AnySample](ABC, Setuppable):
    @abstractmethod
    def apply(self, x: T_IN) -> T_OUT:
        pass

    def __call__(self, x: T_IN) -> T_OUT:
        return self.apply(x)
