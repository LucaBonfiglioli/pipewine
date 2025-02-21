"""Operators with random behavior."""

from functools import partial
from random import shuffle

from pipewine.dataset import Dataset, LazyDataset
from pipewine.operators.base import DatasetOperator
from pipewine.sample import Sample


class ShuffleOp(DatasetOperator[Dataset, Dataset]):
    """Operator that shuffles the samples in a dataset in a random order."""

    def _index_fn(self, index: list[int], x: int) -> int:
        return index[x]

    def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
        idx = list(range(len(x)))
        shuffle(idx)
        return LazyDataset(len(x), x.get_sample, index_fn=partial(self._index_fn, idx))
