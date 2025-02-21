"""Operators for splitting datasets into multiple parts."""

from collections.abc import Sequence

from pipewine.dataset import Dataset
from pipewine.operators.base import DatasetOperator
from pipewine.sample import Sample


class BatchOp(DatasetOperator[Dataset, Sequence[Dataset]]):
    """Operator that splits a dataset into batches of a given size, except for the
    last batch which may be smaller.
    """

    def __init__(self, batch_size: int) -> None:
        """
        Args:
            batch_size (int): Size of the batches.
        """
        super().__init__()
        self._batch_size = batch_size
        assert self._batch_size > 0, "Batch size must be greater than 0."

    def __call__[T: Sample](self, x: Dataset[T]) -> Sequence[Dataset[T]]:
        start = 0
        batches = []
        while start < len(x):
            batches.append(x[start : start + self._batch_size])
            start += self._batch_size

        return batches


class ChunkOp(DatasetOperator[Dataset, Sequence[Dataset]]):
    """Operator that splits a dataset into a given number of chunks of approximately
    equal size. The difference in size between any two chunks is guaranteed to be
    at most 1.
    """

    def __init__(self, chunks: int) -> None:
        """
        Args:
            chunks (int): Number of chunks.
        """
        super().__init__()
        self._chunks = chunks
        assert self._chunks > 0, "Number of chunks must be greater than 0."

    def __call__[T: Sample](self, x: Dataset[T]) -> Sequence[Dataset[T]]:
        size_floor = len(x) // self._chunks
        sizes = [size_floor] * self._chunks
        remainder = len(x) % self._chunks
        for i in range(remainder):
            sizes[i] += 1
        start = 0
        chunks = []
        for size in sizes:
            chunks.append(x[start : start + size])
            start += size
        return chunks


class SplitOp(DatasetOperator[Dataset, Sequence[Dataset]]):
    """Operator that splits a dataset into multiple parts of given sizes. The sizes
    can be either integers or floats representing the proportions of the dataset to
    be included in each part.
    """

    def __init__(self, sizes: Sequence[int | float | None]) -> None:
        """
        Args:
            sizes (Sequence[int | float | None]): Sizes of the splits. If a size is an
                integer, it represents the number of samples in the split. If a size is
                a float, it represents the proportion of the dataset to be included in
                the split. If a size is None, it represents the remaining samples after
                all other sizes have been computed. At most one size can be None.
        """
        super().__init__()
        self._sizes = sizes
        all_ints = all(isinstance(x, int) for x in self._sizes if x is not None)
        all_floats = all(isinstance(x, float) for x in self._sizes if x is not None)
        assert all_ints or all_floats, "Sizes must be all int or all float, not mixed."
        self._null_idx = -1
        self._total: int | float = 0
        for i, size in enumerate(self._sizes):
            if size is None:
                if self._null_idx >= 0:
                    raise ValueError("At most one size can be None.")
                self._null_idx = i
            else:
                self._total += size

    def __call__[T: Sample](self, x: Dataset[T]) -> Sequence[Dataset[T]]:
        sizes: list[int] = []
        for size in self._sizes:
            if isinstance(size, float):
                int_size = int(size * len(x))
            elif size is None:
                int_size = len(x) - int(self._total)
            else:
                int_size = size
            sizes.append(int_size)

        start = 0
        splits = []
        for size in sizes:
            splits.append(x[start : start + size])
            start += size
        return splits
