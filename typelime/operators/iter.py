from collections.abc import Sequence
from functools import partial
from math import ceil

from typelime.dataset import Dataset, LazyDataset
from typelime.operators.base import DatasetOperator
from typelime.sample import Sample


class SliceOp(DatasetOperator[Dataset, Dataset], title="slice"):
    def __init__(
        self, start: int | None = None, stop: int | None = None, step: int | None = None
    ) -> None:
        super().__init__()
        self._start = start
        self._stop = stop
        self._step = step

    def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
        return x[slice(self._start or 0, self._stop or len(x), self._step or 1)]


class RepeatOp(DatasetOperator[Dataset, Dataset], title="repeat"):
    def __init__(self, times: int, interleave: bool = False) -> None:
        super().__init__()
        self._times = times
        self._interleave = interleave

    def _index_fn(self, orig_len: int, x: int) -> int:
        return x % orig_len

    def _index_fn_interleave(self, x: int) -> int:
        return x // self._times

    def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
        index_fn = (
            self._index_fn_interleave
            if self._interleave
            else partial(self._index_fn, len(x))
        )
        return LazyDataset(len(x) * self._times, x.get_sample, index_fn=index_fn)


class CycleOp(DatasetOperator[Dataset, Dataset], title="cycle"):
    def __init__(self, n: int) -> None:
        super().__init__()
        self._n = n

    def _index_fn(self, orig_len: int, x: int) -> int:
        return x % orig_len

    def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
        assert len(x) > 0
        return LazyDataset(
            self._n, x.get_sample, index_fn=partial(self._index_fn, len(x))
        )


class IndexOp(DatasetOperator[Dataset, Dataset], title="index"):
    def __init__(self, index: Sequence[int], negate: bool = False) -> None:
        super().__init__()
        self._index = index
        self._negate = negate

    def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
        index: Sequence[int]
        if self._negate:
            index = list(set(range(len(x))).difference(set(self._index)))
        else:
            index = self._index
        return LazyDataset(len(index), x.get_sample, index_fn=index.__getitem__)


class ReverseOp(DatasetOperator[Dataset, Dataset], title="reverse"):
    def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
        return x[::-1]


class PadOp[T: Sample](DatasetOperator[Dataset[T], Dataset[T]], title="pad"):
    def __init__(self, length: int, pad_with: T) -> None:
        super().__init__()
        self._length = length
        self._pad_with = pad_with

    def _get_sample(self, x: Dataset[T], idx: int) -> T:
        if idx < len(x):
            return x[idx]
        else:
            return self._pad_with

    def __call__(self, x: Dataset[T]) -> Dataset[T]:
        return LazyDataset(self._length, partial(self._get_sample, x))
