from typing import TypeVar, TypeVarTuple, cast

from pipewine.mappers.base import Mapper
from pipewine.sample import Sample

Ts = TypeVarTuple("Ts")

A = TypeVar("A", bound=Sample)
B = TypeVar("B", bound=Sample)


class ComposeMapper[T_IN: Sample, T_OUT: Sample](Mapper[T_IN, T_OUT]):
    def __init__(
        self,
        mappers: (
            Mapper[T_IN, T_OUT]
            | tuple[Mapper[T_IN, T_OUT]]
            | tuple[Mapper[T_IN, A], *Ts, Mapper[B, T_OUT]]
        ),
    ) -> None:
        super().__init__()
        if not isinstance(mappers, tuple):
            mappers_t = (mappers,)
        else:
            mappers_t = mappers  # type: ignore
        self._mappers = mappers_t

    def __call__(self, idx: int, x: T_IN) -> T_OUT:
        temp = x
        for mapper in self._mappers:
            temp = cast(Mapper, mapper)(idx, temp)
        return cast(T_OUT, temp)
