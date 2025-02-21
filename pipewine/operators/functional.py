"""Operators that change behavior based on user-defined functions."""

from collections import defaultdict
from collections.abc import Callable
from functools import partial
from typing import Any, Protocol, TypeVar

from pipewine.dataset import Dataset, LazyDataset
from pipewine.grabber import Grabber
from pipewine.mappers import Mapper
from pipewine.operators.base import DatasetOperator
from pipewine.sample import Sample


class FilterOp[T: Sample](DatasetOperator[Dataset[T], Dataset[T]]):
    """Operator that keeps only or removes samples from a dataset based on a
    user-defined filter function.
    """

    def __init__(
        self,
        fn: Callable[[int, T], bool],
        negate: bool = False,
        grabber: Grabber | None = None,
    ) -> None:
        """
        Args:
            fn (Callable[[int, T], bool]): Function that takes the index and the sample
                and returns whether the sample should be kept or removed.
            negate (bool, optional): Whether to negate the filter function. Defaults to
                False.
            grabber (Grabber, optional): Grabber to use for grabbing samples. Defaults
                to None.
        """
        super().__init__()
        self._fn = fn
        self._grabber = grabber or Grabber()
        self._negate = negate

    def __call__(self, x: Dataset[T]) -> Dataset[T]:
        new_index = []
        for i, sample in self.loop(x, self._grabber, name="Filtering"):
            if self._fn(i, sample) ^ self._negate:
                new_index.append(i)
        return LazyDataset(len(new_index), x.get_sample, index_fn=new_index.__getitem__)


class GroupByOp[T: Sample](DatasetOperator[Dataset[T], dict[str, Dataset[T]]]):
    """Operator that groups samples in a dataset based on a user-defined grouping
    function, returning a mapping of datasets with a key for each unique value
    returned by the grouping function.
    """

    def __init__(
        self, fn: Callable[[int, T], str], grabber: Grabber | None = None
    ) -> None:
        """
        Args:
            fn (Callable[[int, T], str]): Function that takes the index and the sample
                and returns a string representing the group to which the sample belongs.
            grabber (Grabber, optional): Grabber to use for grabbing samples. Defaults
                to None.
        """
        super().__init__()
        self._fn = fn
        self._grabber = grabber or Grabber()

    def __call__(self, x: Dataset[T]) -> dict[str, Dataset[T]]:
        indexes: dict[str, list[int]] = defaultdict(list)
        for i, sample in self.loop(x, self._grabber, name="Computing index"):
            key = self._fn(i, sample)
            indexes[key].append(i)
        return {
            k: LazyDataset(len(index), x.get_sample, index_fn=index.__getitem__)
            for k, index in indexes.items()
        }


_T_contravariant = TypeVar("_T_contravariant", contravariant=True)


class SupportsDunderLT(Protocol[_T_contravariant]):
    """Protocol for types that support the less-than dunder method."""

    def __lt__(self, other: _T_contravariant, /) -> bool: ...


class SupportsDunderGT(Protocol[_T_contravariant]):
    """Protocol for types that support the greater-than dunder method."""

    def __gt__(self, other: _T_contravariant, /) -> bool: ...


ComparableT = SupportsDunderLT[Any] | SupportsDunderGT[Any]
"""Type alias for types that support the less-than and greater-than dunder methods."""


class SortOp[T: Sample](DatasetOperator[Dataset[T], Dataset[T]]):
    """Operator that sorts samples in a dataset based on a user-defined sorting
    function.
    """

    def __init__(
        self,
        fn: Callable[[int, T], ComparableT],
        reverse: bool = False,
        grabber: Grabber | None = None,
    ) -> None:
        """
        Args:
            fn (Callable[[int, T], ComparableT]): Function that takes the index and the
                sample and returns a comparable value to use for sorting.
            reverse (bool, optional): Whether to sort in reverse order. Defaults to False.
            grabber (Grabber, optional): Grabber to use for grabbing samples. Defaults
                to None.
        """
        super().__init__()
        self._fn = fn
        self._grabber = grabber or Grabber()
        self._reverse = reverse

    def __call__(self, x: Dataset[T]) -> Dataset[T]:
        keys: list[tuple[ComparableT, int]] = []
        for i, sample in self.loop(x, self._grabber, name="Computing keys"):
            keys.append((self._fn(i, sample), i))

        index = [x[1] for x in sorted(keys, reverse=self._reverse)]
        return LazyDataset(len(x), x.get_sample, index_fn=index.__getitem__)


class MapOp[T_IN: Sample, T_OUT: Sample](
    DatasetOperator[Dataset[T_IN], Dataset[T_OUT]]
):
    """Operator that applies a `Mapper` to each sample in a dataset."""

    def __init__(self, mapper: Mapper[T_IN, T_OUT]) -> None:
        """
        Args:
            mapper (Mapper[T_IN, T_OUT]): Mapper to apply to each sample.
        """
        super().__init__()
        self._mapper = mapper

    def _get_sample(self, x: Dataset[T_IN], idx: int) -> T_OUT:
        return self._mapper(idx, x[idx])

    def __call__(self, x: Dataset[T_IN]) -> Dataset[T_OUT]:
        return LazyDataset(len(x), partial(self._get_sample, x))
