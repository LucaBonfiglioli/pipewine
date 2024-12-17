from functools import partial
from random import shuffle

from typelime.dataset import Dataset, LazyDataset
from typelime.operators.base import DatasetOperator
from typelime.sample import Sample


class ShuffleOp(DatasetOperator[Dataset, Dataset]):
    def _index_fn(self, index: list[int], x: int) -> int:
        return index[x]

    def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
        idx = list(range(len(x)))
        shuffle(idx)
        return LazyDataset(len(x), x.get_sample, index_fn=partial(self._index_fn, idx))
