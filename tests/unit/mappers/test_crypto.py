from typing import Any

import pytest

from pipewine import HashedSample, HashMapper


class TestHashMapper:
    @pytest.mark.parametrize(
        "data",
        [
            {
                "a": "item_a",
                "b": "item_b",
                "c": "item_c",
                "d": "item_d",
            },
        ],
    )
    @pytest.mark.parametrize("keys", [[], "a", ["a"], ["b", "a", "d"], None])
    def test_call(self, make_sample_fn, data: dict[str, Any], keys: list[str]) -> None:
        mapper = HashMapper(keys=keys)
        sample = make_sample_fn(data)
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
