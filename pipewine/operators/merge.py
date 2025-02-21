"""Operators for merging datasets."""

from bisect import bisect
from collections.abc import Sequence
from functools import partial

from pipewine.dataset import Dataset, LazyDataset
from pipewine.item import Item
from pipewine.operators.base import DatasetOperator
from pipewine.sample import Sample, TypelessSample


class CatOp(DatasetOperator[Sequence[Dataset], Dataset]):
    """Operator that concatenates multiple datasets into a single dataset."""

    def _get_sample[
        T: Sample
    ](self, datasets: Sequence[Dataset[T]], index: list[int], i: int) -> T:
        dataset_idx = bisect(index, i) - 1
        effective_i = i - index[dataset_idx]
        return datasets[dataset_idx][effective_i]

    def __call__[T: Sample](self, x: Sequence[Dataset[T]]) -> Dataset[T]:
        index = [0]
        for dataset in x:
            index.append(index[-1] + len(dataset))
        return LazyDataset(index[-1], partial(self._get_sample, x, index))


class ZipOp[T: Sample](DatasetOperator[Sequence[Dataset], Dataset[T]]):
    """Operator that zips multiple datasets into a single dataset by merging the items
    of individual samples.

    Input datasets must have the same length and the samples must have disjoint items.
    """

    def __init__(self, out_type: type[T] | None = None) -> None:
        """
        Args:
            out_type (type[T] | None, optional): Type of the output samples. Defaults
                to None (TypelessSample).
        """
        super().__init__()
        self._out_type = out_type or TypelessSample

    def _get_sample(self, datasets: Sequence[Dataset[Sample]], idx: int) -> T:
        data: dict[str, Item] = {}
        for dataset in datasets:
            data.update(dataset[idx].items())
        return self._out_type(**data)  # type: ignore

    def __call__(self, x: Sequence[Dataset[Sample]]) -> Dataset[T]:
        len0 = len(x[0])
        assert all(len(dataset) == len0 for dataset in x)
        return LazyDataset(len(x[0]), partial(self._get_sample, x))
