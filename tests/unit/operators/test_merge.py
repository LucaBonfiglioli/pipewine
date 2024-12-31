from pipewine import (
    CatOp,
    ZipOp,
    LazyDataset,
    MemoryItem,
    PickleParser,
    TypedSample,
    Item,
    Dataset,
)
import pytest


class NumberSample(TypedSample):
    number: Item[int]


class RangeDataset(LazyDataset[NumberSample]):
    def __init__(self, start: int, stop: int) -> None:
        super().__init__(stop - start, self._make_sample)
        self.start = start

    def _make_sample(self, idx: int) -> NumberSample:
        return NumberSample(number=MemoryItem(idx + self.start, parser=PickleParser()))


class TestCatOp:
    @pytest.mark.parametrize(
        "ranges",
        [
            [],
            [(10, 20)],
            [(10, 20), (50, 55), (23, 28), (20, 100)],
            [(10, 20), (5, 5), (20, 30)],
        ],
    )
    def test_call(self, ranges: list[tuple[int, int]]) -> None:
        expected = sum((list(range(a, b)) for a, b in ranges), start=[])
        op = CatOp()
        in_ = [RangeDataset(a, b) for a, b in ranges]
        out = op(in_)
        assert len(out) == len(expected)
        for sample, n in zip(out, expected):
            assert sample.number() == n


class TestZipOp:
    def test_call(self, dataset: Dataset) -> None:
        dataset_b = RangeDataset(40, 66)
        op = ZipOp()
        out = op((dataset, dataset_b))
        for x in out:
            x["metadata"]()
            x["number"]()
