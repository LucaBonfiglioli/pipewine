from collections.abc import Sequence
from typelime import (
    LazyDataset,
    MemoryItem,
    PickleParser,
    Dataset,
    TypelessSample,
    BatchOp,
    ChunkOp,
    SplitOp,
)
import pytest


class MyDataset(LazyDataset[TypelessSample]):
    def __init__(
        self,
        size: int,
    ) -> None:
        super().__init__(size, self._make_sample)

    def _make_sample(self, idx: int) -> TypelessSample:
        return TypelessSample(number=MemoryItem(idx, parser=PickleParser()))


class TestAllSplitOp:
    def _test_op(
        self, datasets: Sequence[Dataset[TypelessSample]], expected: list[list[int]]
    ) -> None:
        for dataset, indices in zip(datasets, expected):
            found = [x["number"]() for x in dataset]
            assert found == indices


class TestBatchOp(TestAllSplitOp):
    @pytest.mark.parametrize(
        ["dataset_size", "batch_size", "expected"],
        [
            [0, 4, []],
            [1, 1, [[0]]],
            [4, 1, [[0], [1], [2], [3]]],
            [4, 4, [[0, 1, 2, 3]]],
            [4, 2, [[0, 1], [2, 3]]],
            [7, 3, [[0, 1, 2], [3, 4, 5], [6]]],
            [7, 9, [[0, 1, 2, 3, 4, 5, 6]]],
        ],
    )
    def test_op(
        self, dataset_size: int, batch_size: int, expected: list[list[int]]
    ) -> None:
        op = BatchOp(batch_size)
        dataset = MyDataset(dataset_size)
        self._test_op(op(dataset), expected)


class TestChunkOp(TestAllSplitOp):
    @pytest.mark.parametrize(
        ["dataset_size", "chunks", "expected"],
        [
            [0, 4, [[], [], [], []]],
            [1, 1, [[0]]],
            [4, 1, [[0, 1, 2, 3]]],
            [4, 4, [[0], [1], [2], [3]]],
            [4, 2, [[0, 1], [2, 3]]],
            [7, 3, [[0, 1, 2], [3, 4], [5, 6]]],
            [7, 9, [[0], [1], [2], [3], [4], [5], [6], [], []]],
        ],
    )
    def test_op(
        self, dataset_size: int, chunks: int, expected: list[list[int]]
    ) -> None:
        op = ChunkOp(chunks)
        dataset = MyDataset(dataset_size)
        self._test_op(op(dataset), expected)


class TestSplitOp(TestAllSplitOp):
    @pytest.mark.parametrize(
        ["dataset_size", "sizes", "expected"],
        [
            [0, [None], [[]]],
            [0, [10, None], [[], []]],
            [0, [0.5, None], [[], []]],
            [10, [3, 1, 2, 4], [[0, 1, 2], [3], [4, 5], [6, 7, 8, 9]]],
            [15, [3, 1, 2, 4], [[0, 1, 2], [3], [4, 5], [6, 7, 8, 9]]],
            [8, [3, 1, 2, 4], [[0, 1, 2], [3], [4, 5], [6, 7]]],
            [10, [3, 1, None, 4], [[0, 1, 2], [3], [4, 5], [6, 7, 8, 9]]],
            [10, [0.3, 0.5, None], [[0, 1, 2], [3, 4, 5, 6, 7], [8, 9]]],
        ],
    )
    def test_op(
        self,
        dataset_size: int,
        sizes: list[int | float | None],
        expected: list[list[int]],
    ) -> None:
        op = SplitOp(sizes)
        dataset = MyDataset(dataset_size)
        self._test_op(op(dataset), expected)

    def test_split_fails_more_than_one_none(self):
        with pytest.raises(ValueError):
            SplitOp([10, 20, None, 30, None])
