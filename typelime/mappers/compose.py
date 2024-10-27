from typing import Any, TypeVarTuple, cast

from typelime.mappers.base import Mapper
from typelime.sample import Sample

Ts = TypeVarTuple("Ts")


class ComposeMapper[T_IN: Sample, T_OUT: Sample](Mapper[T_IN, T_OUT]):

    def __init__(
        self,
        mappers: (
            Mapper[T_IN, T_OUT]
            | tuple[Mapper[T_IN, T_OUT]]
            | tuple[Mapper[T_IN, Any], Mapper[Any, T_OUT]]
            | tuple[Mapper[T_IN, Any], *Ts, Mapper[Any, T_OUT]]
        ),
    ) -> None:
        super().__init__()
        if not isinstance(mappers, tuple):
            mappers_t = (mappers,)
        else:
            mappers_t = mappers
        self._mappers = mappers_t

    def apply(self, x: T_IN) -> T_OUT:
        temp = x
        for mapper in self._mappers:
            temp = mapper(temp)  # type: ignore
        return cast(T_OUT, temp)
