"""Operators for manipulating the length and order of datasets."""

from collections.abc import Sequence
from functools import partial

from pipewine.dataset import Dataset, LazyDataset
from pipewine.operators.base import DatasetOperator
from pipewine.sample import Sample


class SliceOp(DatasetOperator[Dataset, Dataset]):
    """Operator that slices a dataset based on the start, stop, and step arguments,
    similar to the plain Python slicing.
    """

    def __init__(
        self, start: int | None = None, stop: int | None = None, step: int | None = None
    ) -> None:
        """
        Args:
            start (int, optional): Start index of the slice. Defaults to None.
            stop (int, optional): Stop index of the slice. Defaults to None.
            step (int, optional): Step of the slice. Defaults to None.
        """
        super().__init__()
        self._start = start
        self._stop = stop
        self._step = step

    def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
        return x[slice(self._start or 0, self._stop or len(x), self._step or 1)]


class RepeatOp(DatasetOperator[Dataset, Dataset]):
    """Operator that repeats a dataset a given number of times."""

    def __init__(self, times: int, interleave: bool = False) -> None:
        """
        Args:
            times (int): Number of times to repeat the dataset.
            interleave (bool, optional): Whether to interleave the repeated samples.
                Defaults to False.
        """
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


class CycleOp(DatasetOperator[Dataset, Dataset]):
    """Operator that repeats a dataset until a given length is reached."""

    def __init__(self, n: int) -> None:
        """
        Args:
            n (int): Length of the resulting dataset.
        """
        super().__init__()
        self._n = n

    def _index_fn(self, orig_len: int, x: int) -> int:
        return x % orig_len

    def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
        assert len(x) > 0
        return LazyDataset(
            self._n, x.get_sample, index_fn=partial(self._index_fn, len(x))
        )


class IndexOp(DatasetOperator[Dataset, Dataset]):
    """Operator that selects samples from a dataset based on their indices."""

    def __init__(self, index: Sequence[int], negate: bool = False) -> None:
        """
        Args:
            index (Sequence[int]): Indices of the samples to select.
            negate (bool, optional): Whether to negate the selection. Defaults to False.
        """
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


class ReverseOp(DatasetOperator[Dataset, Dataset]):
    """Operator that reverses the order of samples in a dataset."""

    def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
        return x[::-1]


class PadOp(DatasetOperator[Dataset, Dataset]):
    """Operator that pads a dataset to a given length by repeating a specified sample."""

    def __init__(self, length: int, pad_with: int = -1) -> None:
        """
        Args:
            length (int): Length of the resulting dataset.
            pad_with (int, optional): Index of the sample to use for padding. Defaults
                to -1.
        """
        super().__init__()
        self._length = length
        self._pad_with = pad_with

    def _get_sample[T: Sample](self, x: Dataset[T], idx: int) -> T:
        if idx < len(x):
            return x[idx]
        else:
            return x[self._pad_with]

    def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
        return LazyDataset(self._length, partial(self._get_sample, x))
