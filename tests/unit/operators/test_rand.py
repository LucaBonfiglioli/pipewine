from pipewine import ShuffleOp, Dataset, MapOp, HashMapper


class TestShuffleOp:
    def test_call(self, dataset: Dataset) -> None:
        op = ShuffleOp()
        out = op(dataset)
        assert len(out) == len(dataset)

        hasher = MapOp(HashMapper())
        hashes_in = hasher(dataset)
        hashes_out = hasher(out)
        assert {x.hash() for x in hashes_in} == {x.hash() for x in hashes_out}
