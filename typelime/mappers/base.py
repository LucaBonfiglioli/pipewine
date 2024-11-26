from abc import ABC, abstractmethod

from typelime._register import RegisterMeta
from typelime.sample import Sample


class MapperMeta(RegisterMeta):
    def _type(self) -> str:
        return "mapper"


class Mapper[T_IN: Sample, T_OUT: Sample](ABC, metaclass=MapperMeta):
    @abstractmethod
    def __call__(self, idx: int, x: T_IN) -> T_OUT: ...
