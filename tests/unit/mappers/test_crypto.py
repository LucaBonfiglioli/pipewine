import pytest
from pipewine import HashMapper, TypelessSample, MemoryItem, PickleParser, HashedSample


class TestHashMapper:
    @pytest.mark.parametrize(
        "sample",
        [
            TypelessSample(
                a=MemoryItem("item_a", PickleParser()),
                b=MemoryItem("item_b", PickleParser()),
                c=MemoryItem("item_c", PickleParser()),
                d=MemoryItem("item_d", PickleParser()),
            ),
        ],
    )
    @pytest.mark.parametrize("keys", [[], "a", ["a"], ["b", "a", "d"], None])
    def test_call(self, sample: TypelessSample, keys: list[str]) -> None:
        mapper = HashMapper(keys=keys)
        out = mapper(0, sample)
        assert isinstance(out, HashedSample)
        assert isinstance(out.hash(), str)

        if isinstance(keys, str):
            keys = [keys]

        if isinstance(keys, list):
            sample2 = sample
            for k in sample2:
                if k not in keys:
                    sample2 = sample2.with_values(**{k: sample2[k]() + "different"})
            out2 = mapper(0, sample2)
            assert out2.hash() == out.hash()

        for k in sample:
            if keys is None or k in keys:
                sample2 = sample.with_values(**{k: sample[k]() + "different"})
                out2 = mapper(0, sample2)
                assert out2.hash() != out.hash()

    @pytest.mark.parametrize("algo", ["WRONG", "", "shake_128", "shake_256"])
    def test_init_fail(self, algo: str) -> None:
        with pytest.raises(ValueError):
            HashMapper(algorithm=algo)
