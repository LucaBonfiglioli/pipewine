from abc import ABC, abstractmethod

from typelime.sample import Sample


class Mapper[T_IN: Sample, T_OUT: Sample](ABC):
    @abstractmethod
    def __call__(self, idx: int, x: T_IN) -> T_OUT: ...
