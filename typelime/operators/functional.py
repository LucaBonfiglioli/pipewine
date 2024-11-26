from collections import defaultdict
from collections.abc import Callable, Mapping
from functools import partial
from typing import Any, Protocol, TypeVar

from typelime.dataset import Dataset, LazyDataset
from typelime.grabber import Grabber
from typelime.mappers import Mapper
from typelime.operators.base import DatasetOperator
from typelime.sample import Sample


class FilterOp[T: Sample](DatasetOperator[Dataset[T], Dataset[T]], title="filter"):
    def __init__(
        self,
        fn: Callable[[int, T], bool],
        negate: bool = False,
        grabber: Grabber[T] | None = None,
    ) -> None:
        super().__init__()
        self._fn = fn
        self._grabber = grabber or Grabber()
        self._negate = negate

    def __call__(self, x: Dataset[T]) -> Dataset[T]:
        new_index = []
        with self._grabber(x) as ctx:
            for i, sample in ctx:
                if self._fn(i, sample) ^ self._negate:
                    new_index.append(i)
        return LazyDataset(len(new_index), x.get_sample, index_fn=new_index.__getitem__)


class GroupByOp[T: Sample](
    DatasetOperator[Dataset[T], Mapping[str, Dataset[T]]], title="groupby"
):
    def __init__(
        self, fn: Callable[[int, T], str], grabber: Grabber[T] | None = None
    ) -> None:
        super().__init__()
        self._fn = fn
        self._grabber = grabber or Grabber()

    def __call__(self, x: Dataset[T]) -> Mapping[str, Dataset[T]]:
        indexes: dict[str, list[int]] = defaultdict(list)
        with self._grabber(x) as ctx:
            for i, sample in ctx:
                key = self._fn(i, sample)
                indexes[key].append(i)
        return {
            k: LazyDataset(len(index), x.get_sample, index_fn=index.__getitem__)
            for k, index in indexes.items()
        }


_T_contravariant = TypeVar("_T_contravariant", contravariant=True)


class SupportsDunderLT(Protocol[_T_contravariant]):
    def __lt__(self, other: _T_contravariant, /) -> bool: ...


class SupportsDunderGT(Protocol[_T_contravariant]):
    def __gt__(self, other: _T_contravariant, /) -> bool: ...


ComparableT = SupportsDunderLT[Any] | SupportsDunderGT[Any]


class SortOp[T: Sample](DatasetOperator[Dataset[T], Dataset[T]], title="sort"):
    def __init__(
        self,
        fn: Callable[[int, T], ComparableT],
        reverse: bool = False,
        grabber: Grabber | None = None,
    ) -> None:
        super().__init__()
        self._fn = fn
        self._grabber = grabber or Grabber()
        self._reverse = reverse

    def __call__(self, x: Dataset[T]) -> Dataset[T]:
        keys: list[tuple[ComparableT, int]] = []
        with self._grabber(x) as ctx:
            for i, sample in ctx:
                keys.append((self._fn(i, sample), i))

        index = [x[1] for x in sorted(keys, reverse=self._reverse)]
        return LazyDataset(len(x), x.get_sample, index_fn=index.__getitem__)


class MapOp[T_IN: Sample, T_OUT: Sample](
    DatasetOperator[Dataset[T_IN], Dataset[T_OUT]], title="map"
):
    def __init__(self, mapper: Mapper[T_IN, T_OUT]) -> None:
        self._mapper = mapper

    def _get_sample(self, x: Dataset[T_IN], idx: int) -> T_OUT:
        return self._mapper(idx, x[idx])

    def __call__(self, x: Dataset[T_IN]) -> Dataset[T_OUT]:
        return LazyDataset(len(x), partial(self._get_sample, x))
