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
    def __init__(
        self,
        fn: Callable[[int, T], bool],
        negate: bool = False,
        grabber: Grabber | None = None,
    ) -> None:
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
    def __init__(
        self, fn: Callable[[int, T], str], grabber: Grabber | None = None
    ) -> None:
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
    def __lt__(self, other: _T_contravariant, /) -> bool: ...


class SupportsDunderGT(Protocol[_T_contravariant]):
    def __gt__(self, other: _T_contravariant, /) -> bool: ...


ComparableT = SupportsDunderLT[Any] | SupportsDunderGT[Any]


class SortOp[T: Sample](DatasetOperator[Dataset[T], Dataset[T]]):
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
        for i, sample in self.loop(x, self._grabber, name="Sorting"):
            keys.append((self._fn(i, sample), i))

        index = [x[1] for x in sorted(keys, reverse=self._reverse)]
        return LazyDataset(len(x), x.get_sample, index_fn=index.__getitem__)


class MapOp[T_IN: Sample, T_OUT: Sample](
    DatasetOperator[Dataset[T_IN], Dataset[T_OUT]]
):
    def __init__(self, mapper: Mapper[T_IN, T_OUT]) -> None:
        super().__init__()
        self._mapper = mapper

    def _get_sample(self, x: Dataset[T_IN], idx: int) -> T_OUT:
        return self._mapper(idx, x[idx])

    def __call__(self, x: Dataset[T_IN]) -> Dataset[T_OUT]:
        return LazyDataset(len(x), partial(self._get_sample, x))
