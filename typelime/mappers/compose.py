from collections.abc import Iterable
from typing import cast

from typelime.mappers.base import Mapper
from typelime.sample import Sample


class ComposeMapper[T_IN: Sample, T_OUT: Sample](Mapper[T_IN, T_OUT]):
    def __init__(self, mappers: Iterable[Mapper]) -> None:
        self._mappers = mappers

    def apply(self, x: T_IN) -> T_OUT:
        temp = x
        for mapper in self._mappers:
            temp = mapper(temp)
        return cast(T_OUT, temp)
