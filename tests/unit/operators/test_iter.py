from collections.abc import Sequence
from pipewine import (
    LazyDataset,
    MemoryItem,
    PickleParser,
    Dataset,
    TypelessSample,
    SliceOp,
    RepeatOp,
    CycleOp,
    IndexOp,
    ReverseOp,
    PadOp,
)
import pytest


class MyDataset(LazyDataset[TypelessSample]):
    def __init__(
        self,
        size: int,
    ) -> None:
        self._the_size = size
        super().__init__(size, self._make_sample)

    def _make_sample(self, idx: int) -> TypelessSample:
        if idx < 0:
            idx = self._the_size + idx
        return TypelessSample(number=MemoryItem(idx, parser=PickleParser()))


class TestAllIterOp:
    def _test_op(self, dataset: Dataset[TypelessSample], expected: list[int]) -> None:
        found = [x["number"]() for x in dataset]
        assert found == expected


class TestSliceOp(TestAllIterOp):
    @pytest.mark.parametrize(
        ["dataset_size", "start", "stop", "step", "expected"],
        [
            [0, None, None, None, []],
            [10, None, None, None, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]],
            [10, 5, None, None, [5, 6, 7, 8, 9]],
            [10, None, 6, None, [0, 1, 2, 3, 4, 5]],
            [10, None, None, 3, [0, 3, 6, 9]],
            [100, 30, 50, 4, [30, 34, 38, 42, 46]],
            [10, 12, None, None, []],
            [10, None, 14, 2, [0, 2, 4, 6, 8]],
        ],
    )
    def test_op(
        self,
        dataset_size: int,
        start: int | None,
        stop: int | None,
        step: int | None,
        expected: list[int],
    ) -> None:
        dataset = MyDataset(dataset_size)
        op = SliceOp(start=start, stop=stop, step=step)
        self._test_op(op(dataset), expected)


class TestRepeatOp(TestAllIterOp):
    @pytest.mark.parametrize(
        ["dataset_size", "times", "interleave", "expected"],
        [
            [3, 0, False, []],
            [3, 0, True, []],
            [3, 1, False, [0, 1, 2]],
            [3, 1, True, [0, 1, 2]],
            [0, 4, False, []],
            [0, 4, True, []],
            [3, 3, False, [0, 1, 2, 0, 1, 2, 0, 1, 2]],
            [3, 3, True, [0, 0, 0, 1, 1, 1, 2, 2, 2]],
            [3, 5, False, [0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2]],
            [3, 5, True, [0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2]],
        ],
    )
    def test_op(
        self, dataset_size: int, times: int, interleave: bool, expected: list[int]
    ) -> None:
        dataset = MyDataset(dataset_size)
        op = RepeatOp(times, interleave=interleave)
        self._test_op(op(dataset), expected)


class TestCycleOp(TestAllIterOp):
    @pytest.mark.parametrize(
        ["dataset_size", "n", "expected"],
        [
            [3, 0, []],
            [3, 1, [0]],
            [3, 3, [0, 1, 2]],
            [3, 10, [0, 1, 2, 0, 1, 2, 0, 1, 2, 0]],
        ],
    )
    def test_op(self, dataset_size: int, n: int, expected: list[int]) -> None:
        dataset = MyDataset(dataset_size)
        op = CycleOp(n)
        self._test_op(op(dataset), expected)


class TestIndexOp(TestAllIterOp):
    @pytest.mark.parametrize(
        ["dataset_size", "index", "negate", "expected"],
        [
            [0, [], False, []],
            [0, [], True, []],
            [5, [], False, []],
            [5, [], True, [0, 1, 2, 3, 4]],
            [10, [1, 4, 3], False, [1, 4, 3]],
            [10, [1, 4, 3], True, [0, 2, 5, 6, 7, 8, 9]],
        ],
    )
    def test_op(
        self, dataset_size: int, index: list[int], negate: bool, expected: list[int]
    ) -> None:
        dataset = MyDataset(dataset_size)
        op = IndexOp(index, negate=negate)
        self._test_op(op(dataset), expected)


class TestReverseOp(TestAllIterOp):
    @pytest.mark.parametrize(
        ["dataset_size", "expected"],
        [[0, []], [1, [0]], [5, [4, 3, 2, 1, 0]]],
    )
    def test_op(self, dataset_size: int, expected: list[int]) -> None:
        dataset = MyDataset(dataset_size)
        op = ReverseOp()
        self._test_op(op(dataset), expected)


class TestPadOp(TestAllIterOp):
    @pytest.mark.parametrize("pad_with", [-1])
    @pytest.mark.parametrize(
        ["dataset_size", "length", "expected"],
        [
            [4, 4, [0, 1, 2, 3]],
            [4, 2, [0, 1]],
            [4, 6, [0, 1, 2, 3, 3, 3]],
        ],
    )
    def test_op(
        self,
        dataset_size: int,
        length: int,
        pad_with: int,
        expected: list[int],
    ) -> None:
        dataset = MyDataset(dataset_size)
        op = PadOp(length, pad_with)
        self._test_op(op(dataset), expected)
