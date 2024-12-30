from pipewine import IdentityOp, Dataset


class TestIdentityOp:
    def test_call(self, dataset: Dataset) -> None:
        op = IdentityOp()
        assert op(dataset) is dataset
