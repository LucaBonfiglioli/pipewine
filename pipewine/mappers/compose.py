"""Mappers that wrap or compose other mappers."""

from typing import TypeVar, TypeVarTuple, cast

from pipewine.mappers.base import Mapper
from pipewine.sample import Sample

Ts = TypeVarTuple("Ts")

A = TypeVar("A", bound=Sample)
B = TypeVar("B", bound=Sample)


class ComposeMapper[T_IN: Sample, T_OUT: Sample](Mapper[T_IN, T_OUT]):
    """Mapper that composes multiple mappers into a single mapper, calling each
    mapper in sequence, similar to function composition.

    When composing multiple mappers, the output type of each mapper must match the
    input type of the next mapper. This class is hinted in such a way that the type
    checker can infer the input and output types of the final composed mapper.
    """

    def __init__(
        self,
        mappers: (
            Mapper[T_IN, T_OUT]
            | tuple[Mapper[T_IN, T_OUT]]
            | tuple[Mapper[T_IN, A], *Ts, Mapper[B, T_OUT]]
        ),
    ) -> None:
        """
        Args:
            mappers (Mapper[T_IN, T_OUT] | tuple[Mapper, ...]): Mapper or tuple of
                mappers to compose. If a single mapper is provided, it is treated as a
                tuple with a single element.
        """
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
