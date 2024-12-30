from typing import Any

import pytest

from pipewine import (
    ConvertMapper,
    MemoryItem,
    Parser,
    PickleParser,
    Sample,
    ShareMapper,
    TypelessSample,
    YAMLParser,
)


class TestConvertMapper:
    @pytest.mark.parametrize(
        "sample",
        [
            TypelessSample(
                a=MemoryItem(10, PickleParser()),
                b=MemoryItem(20, PickleParser(), shared=True),
                c=MemoryItem(30, YAMLParser(int)),
            )
        ],
    )
    @pytest.mark.parametrize(
        ["parsers", "exp_type"],
        [
            [{}, {"a": PickleParser, "b": PickleParser, "c": YAMLParser}],
            [
                {"a": PickleParser()},
                {"a": PickleParser, "b": PickleParser, "c": YAMLParser},
            ],
            [
                {"b": YAMLParser()},
                {"a": PickleParser, "b": YAMLParser, "c": YAMLParser},
            ],
            [
                {"a": YAMLParser(), "d": PickleParser()},
                {"a": YAMLParser, "b": PickleParser, "c": YAMLParser},
            ],
        ],
    )
    def test_call(
        self,
        sample: Sample,
        parsers: dict[str, Parser],
        exp_type: dict[str, type[Parser]],
    ) -> None:
        mapper: ConvertMapper[Sample] = ConvertMapper(parsers)
        re_sample = mapper(0, sample)
        assert set(re_sample.keys()) == set(exp_type.keys())
        for k in re_sample:
            assert sample[k]() == re_sample[k]()
            assert type(re_sample[k].parser) is exp_type[k]


class TestShareMapper:
    @pytest.mark.parametrize(
        "sample",
        [
            TypelessSample(
                a=MemoryItem(10, PickleParser()),
                b=MemoryItem(20, PickleParser(), shared=True),
                c=MemoryItem(30, YAMLParser(int)),
            )
        ],
    )
    @pytest.mark.parametrize(
        ["share", "unshare", "exp_shared"],
        [
            [[], [], {"b"}],
            [["b"], [], {"b"}],
            [["b"], ["a", "c"], {"b"}],
            [["a"], [], {"a", "b"}],
            [["a", "c"], ["b"], {"a", "c"}],
            [["a", "c"], [], {"a", "b", "c"}],
        ],
    )
    def test_call(
        self,
        sample: Sample,
        share: list[str],
        unshare: list[str],
        exp_shared: set[str],
    ) -> None:
        mapper = ShareMapper(share, unshare)
        re_sample = mapper(0, sample)
        assert set(sample.keys()) == set(re_sample.keys())
        for k in sample:
            assert sample[k]() == re_sample[k]()
            assert re_sample[k].is_shared == (k in exp_shared)

    def test_raises(self) -> None:
        with pytest.raises(ValueError):
            ShareMapper(["a", "b", "c"], ["d", "b", "e"])
