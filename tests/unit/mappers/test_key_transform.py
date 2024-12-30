from typing import Any, Iterable

import pytest

from pipewine import (
    DuplicateItemMapper,
    FilterKeysMapper,
    FormatKeysMapper,
    RenameMapper,
)


class TestDuplicateItemMapper:
    @pytest.mark.parametrize(
        ["data", "src", "dst"],
        [[{"a": 10, "b": 20}, "b", "d"], [{"a": 10, "b": 20}, "b", "b"]],
    )
    def test_call(
        self, make_sample_fn, data: dict[str, Any], src: str, dst: str
    ) -> None:
        sample = make_sample_fn(data)
        val = sample[src]()
        mapper = DuplicateItemMapper(src, dst)
        re_sample = mapper(0, sample)
        re_val = re_sample[src]()
        re_val_copy = re_sample[dst]()
        assert val == re_val == re_val_copy


class TestFormatKeysMapper:
    @pytest.mark.parametrize(
        ["data", "fmt", "keys", "expected"],
        [
            [{}, "*", [], {}],
            [{}, "my_key", ["a", "b"], {}],
            [{}, "my_*_key", ["c", "d"], {}],
            [{"a": 10, "b": 20}, "my_*_key", ["a"], {"my_a_key": 10, "b": 20}],
            [
                {"a": 10, "b": 20},
                "my_*_key",
                ["a", "b"],
                {"my_a_key": 10, "my_b_key": 20},
            ],
            [{"a": 10, "b": 20}, "my_*_key", "b", {"a": 10, "my_b_key": 20}],
            [{"a": 10, "b": 20}, "my_*_key", None, {"my_a_key": 10, "my_b_key": 20}],
            [{"a": 10, "b": 20}, "***", "a", {"aaa": 10, "b": 20}],
        ],
    )
    def test_call(
        self,
        make_sample_fn,
        data: dict[str, Any],
        fmt: str,
        keys: str | Iterable[str] | None,
        expected: dict[str, Any],
    ) -> None:
        mapper = FormatKeysMapper(fmt, keys=keys)
        sample = make_sample_fn(data)
        re_sample = mapper(0, sample)
        assert set(re_sample.keys()) == set(expected.keys())
        for k, v in expected.items():
            assert re_sample[k]() == v


# Already tested in unit test for `Sample.remap`
class TestRenameMapper:
    @pytest.mark.parametrize(
        ["data", "renaming", "exclude", "expected"],
        [
            [{}, {}, False, {}],
            [{"a": 10, "b": 20}, {}, False, {"a": 10, "b": 20}],
            [{"a": 10, "b": 20}, {}, True, {}],
            [{"a": 10, "b": 20}, {"c": "d"}, False, {"a": 10, "b": 20}],
            [{"a": 10, "b": 20}, {"a": "c"}, False, {"c": 10, "b": 20}],
        ],
    )
    def test_call(
        self,
        make_sample_fn,
        data: dict[str, Any],
        renaming: dict[str, str],
        exclude: bool,
        expected: dict[str, Any],
    ) -> None:
        mapper = RenameMapper(renaming, exclude=exclude)
        sample = make_sample_fn(data)
        re_sample = mapper(0, sample)
        assert set(re_sample.keys()) == set(expected.keys())
        for k, v in expected.items():
            assert re_sample[k]() == v


# Already tested in unit test for `Sample.with_only` and `Sample.without`
class TestFilterKeysMapper:
    @pytest.mark.parametrize(
        ["data", "keys", "negate", "expected"],
        [
            [{}, [], False, {}],
            [{}, [], True, {}],
            [{"a": 10, "b": 20}, [], False, {}],
            [{"a": 10, "b": 20}, [], True, {"a": 10, "b": 20}],
            [{"a": 10, "b": 20}, ["c"], False, {}],
            [{"a": 10, "b": 20}, ["c"], True, {"a": 10, "b": 20}],
            [{"a": 10, "b": 20}, "a", False, {"a": 10}],
            [{"a": 10, "b": 20}, "a", True, {"b": 20}],
        ],
    )
    def test_call(
        self,
        make_sample_fn,
        data: dict[str, Any],
        keys: str | Iterable[str],
        negate: bool,
        expected: dict[str, Any],
    ) -> None:
        mapper = FilterKeysMapper(keys, negate=negate)
        sample = make_sample_fn(data)
        re_sample = mapper(0, sample)
        assert set(re_sample.keys()) == set(expected.keys())
        for k, v in expected.items():
            assert re_sample[k]() == v
