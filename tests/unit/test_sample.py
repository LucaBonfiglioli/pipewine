from contextlib import nullcontext
from typing import Any

import pytest

from pipewine import Item, JSONParser, MemoryItem, Sample, TypedSample, TypelessSample


class TestSample:
    def _test_get_item(self, sample: Sample, key: str, value: Any) -> None:
        assert sample[key]() == value

    def _test_len(self, sample: Sample, len_: int) -> None:
        assert len(sample) == len_

    def _test_iter(self, sample: Sample, keys: list[str]) -> None:
        assert len(sample) == len(keys)
        for x, y in zip(sorted(iter(sample)), sorted(keys)):
            assert x == y

    def _test_with_items(self, sample: Sample, items: dict[str, Item]) -> None:
        old_sample = sample
        sample = sample.with_items(**items)
        assert type(sample) is type(old_sample)
        for k, v in items.items():
            assert sample[k]() == v()
        for k, v in old_sample.items():
            if k not in items:
                assert sample[k]() == v()

    def _test_with_item(self, sample: Sample, key: str, item: Item) -> None:
        old_sample = sample
        sample = sample.with_item(key, item)
        assert type(sample) is type(old_sample)
        assert sample[key]() == item()
        for k, v in old_sample.items():
            if k != key:
                assert sample[k]() == v()

    def _test_with_values(self, sample: Sample, values: dict[str, Any]) -> None:
        old_sample = sample
        old_keys = set(sample.keys())
        new_keys = set(values.keys())
        cm: Any = nullcontext()
        if not new_keys.issubset(old_keys):
            cm = pytest.raises(Exception)
        with cm:
            old_type = type(sample)
            sample = sample.with_values(**values)
            assert type(sample) is old_type
            for k, v in values.items():
                assert sample[k]() == v
            for k, v in old_sample.items():
                if k not in values:
                    assert sample[k]() == v()

    def _test_with_value(self, sample: Sample, key: str, value: Any) -> None:
        old_sample = sample
        old_keys = set(sample.keys())
        cm: Any = nullcontext()
        if key not in old_keys:
            cm = pytest.raises(Exception)
        with cm:
            old_type = type(sample)
            sample = sample.with_value(key, value)
            assert type(sample) is old_type
            assert sample[key]() == value
            for k, v in old_sample.items():
                if k != key:
                    assert sample[k]() == v()

    def _test_without(self, sample: Sample, keys: list[str]) -> None:
        old_sample = sample
        sample = sample.without(*keys)
        for k in keys:
            assert k not in sample
        for k, v in old_sample.items():
            if k not in keys:
                assert sample[k]() == v()

    def _test_with_only(self, sample: Sample, keys: list[str]) -> None:
        old_sample = sample
        sample = sample.with_only(*keys)
        for k in keys:
            if k in old_sample:
                assert k in sample
        for k in old_sample:
            if k not in keys:
                assert k not in sample

    def _test_remap(
        self, sample: Sample, fromto: dict[str, str], exclude: bool
    ) -> None:
        old_sample = sample
        sample = sample.remap(fromto, exclude=exclude)
        for src, dst in fromto.items():
            if exclude:
                if src in old_sample:
                    assert sample[dst]() == old_sample[src]()
                else:
                    assert src not in sample
            else:
                if src in old_sample:
                    assert sample[dst]() == old_sample[src]()
                else:
                    assert src not in sample


class TestTypelessSample(TestSample):
    @pytest.mark.parametrize(
        ["sample", "key", "value"],
        [
            [
                TypelessSample(
                    a=MemoryItem(10, JSONParser()),
                    b=MemoryItem(20, JSONParser()),
                    c=MemoryItem(30, JSONParser()),
                    d=MemoryItem(40, JSONParser()),
                ),
                "c",
                30,
            ]
        ],
    )
    def test_get_item(self, sample: Sample, key: str, value: Any) -> None:
        self._test_get_item(sample, key, value)

    @pytest.mark.parametrize(
        ["sample", "len_"],
        [
            [
                TypelessSample(
                    a=MemoryItem(10, JSONParser()),
                    b=MemoryItem(20, JSONParser()),
                    c=MemoryItem(30, JSONParser()),
                    d=MemoryItem(40, JSONParser()),
                ),
                4,
            ],
            [TypelessSample(), 0],
            [TypelessSample(a=MemoryItem(4, JSONParser())), 1],
        ],
    )
    def test_size(self, sample: Sample, len_: int) -> None:
        self._test_len(sample, len_)

    @pytest.mark.parametrize(
        ["sample", "keys"],
        [
            [
                TypelessSample(
                    a=MemoryItem(10, JSONParser()),
                    b=MemoryItem(20, JSONParser()),
                    c=MemoryItem(30, JSONParser()),
                    d=MemoryItem(40, JSONParser()),
                ),
                ["a", "b", "c", "d"],
            ],
            [TypelessSample(), []],
            [
                TypelessSample(a=MemoryItem(4, JSONParser())),
                ["a"],
            ],
        ],
    )
    def test_iter(self, sample: Sample, keys: list[str]) -> None:
        self._test_iter(sample, keys)

    @pytest.mark.parametrize(
        "sample",
        [
            TypelessSample(
                a=MemoryItem(10, JSONParser()),
                b=MemoryItem(20, JSONParser()),
                c=MemoryItem(30, JSONParser()),
                d=MemoryItem(40, JSONParser()),
            ),
            TypelessSample(),
        ],
    )
    @pytest.mark.parametrize(
        "items",
        [
            {
                "a": MemoryItem(40, JSONParser()),
                "f": MemoryItem(303, JSONParser()),
            },
            {},
        ],
    )
    def test_with_items(self, sample: Sample, items: dict[str, Item]) -> None:
        self._test_with_items(sample, items)

    @pytest.mark.parametrize(
        "sample",
        [
            TypelessSample(
                a=MemoryItem(10, JSONParser()),
                b=MemoryItem(20, JSONParser()),
                c=MemoryItem(30, JSONParser()),
                d=MemoryItem(40, JSONParser()),
            ),
            TypelessSample(),
        ],
    )
    @pytest.mark.parametrize(
        ["key", "item"],
        [
            ["a", MemoryItem(40, JSONParser())],
            ["f", MemoryItem(303, JSONParser())],
        ],
    )
    def test_with_item(self, sample: Sample, key: str, item: Item) -> None:
        self._test_with_item(sample, key, item)

    @pytest.mark.parametrize(
        "sample",
        [
            TypelessSample(
                a=MemoryItem(10, JSONParser()),
                b=MemoryItem(20, JSONParser()),
                c=MemoryItem(30, JSONParser()),
                d=MemoryItem(40, JSONParser()),
            ),
            TypelessSample(),
        ],
    )
    @pytest.mark.parametrize("values", [{"a": 80, "f": 90}, {}, {"a": 90, "b": 40}])
    def test_with_values(self, sample: Sample, values: dict[str, Any]) -> None:
        self._test_with_values(sample, values)

    @pytest.mark.parametrize(
        "sample",
        [
            TypelessSample(
                a=MemoryItem(10, JSONParser()),
                b=MemoryItem(20, JSONParser()),
                c=MemoryItem(30, JSONParser()),
                d=MemoryItem(40, JSONParser()),
            ),
            TypelessSample(),
        ],
    )
    @pytest.mark.parametrize(["key", "value"], [["a", 80], ["f", 90]])
    def test_with_value(self, sample: Sample, key: str, value: Any) -> None:
        self._test_with_value(sample, key, value)

    @pytest.mark.parametrize(
        "sample",
        [
            TypelessSample(
                a=MemoryItem(10, JSONParser()),
                b=MemoryItem(20, JSONParser()),
                c=MemoryItem(30, JSONParser()),
                d=MemoryItem(40, JSONParser()),
            ),
            TypelessSample(),
        ],
    )
    @pytest.mark.parametrize("keys", [[], ["a"], ["a", "c"], ["a", "b", "c", "d", "e"]])
    def test_without(self, sample: Sample, keys: list[str]) -> None:
        self._test_without(sample, keys)

    @pytest.mark.parametrize(
        "sample",
        [
            TypelessSample(
                a=MemoryItem(10, JSONParser()),
                b=MemoryItem(20, JSONParser()),
                c=MemoryItem(30, JSONParser()),
                d=MemoryItem(40, JSONParser()),
            ),
            TypelessSample(),
        ],
    )
    @pytest.mark.parametrize("keys", [[], ["a"], ["a", "c"], ["a", "b", "c", "d", "e"]])
    def test_with_only(self, sample: Sample, keys: list[str]) -> None:
        self._test_with_only(sample, keys)

    @pytest.mark.parametrize(
        "sample",
        [
            TypelessSample(
                a=MemoryItem(10, JSONParser()),
                b=MemoryItem(20, JSONParser()),
                c=MemoryItem(30, JSONParser()),
                d=MemoryItem(40, JSONParser()),
            ),
            TypelessSample(),
        ],
    )
    @pytest.mark.parametrize(
        "fromto",
        [
            {},
            {"g": "h"},
            {"a": "b"},
            {"a": "A", "b": "B"},
            {"a": "A", "b": "B", "acb": "ACB"},
        ],
    )
    @pytest.mark.parametrize("exclude", [True, False])
    def test_remap(self, sample: Sample, fromto: dict[str, str], exclude: bool) -> None:
        self._test_remap(sample, fromto, exclude)


class MyTypedSample(TypedSample):
    a: Item[int]
    b: Item[int]
    c: Item[int]
    d: Item[int]


class MyEmptyTypedSample(TypedSample):
    pass


class MySingleItemTypedSample(TypedSample):
    a: Item[int]


class TestTypedSample(TestSample):
    @pytest.mark.parametrize(
        ["sample", "key", "value"],
        [
            [
                MyTypedSample(
                    a=MemoryItem(10, JSONParser()),
                    b=MemoryItem(20, JSONParser()),
                    c=MemoryItem(30, JSONParser()),
                    d=MemoryItem(40, JSONParser()),
                ),
                "c",
                30,
            ]
        ],
    )
    def test_get_item(self, sample: Sample, key: str, value: Any) -> None:
        self._test_get_item(sample, key, value)

    @pytest.mark.parametrize(
        ["sample", "len_"],
        [
            [
                MyTypedSample(
                    a=MemoryItem(10, JSONParser()),
                    b=MemoryItem(20, JSONParser()),
                    c=MemoryItem(30, JSONParser()),
                    d=MemoryItem(40, JSONParser()),
                ),
                4,
            ],
            [MyEmptyTypedSample(), 0],
            [MySingleItemTypedSample(a=MemoryItem(4, JSONParser())), 1],
        ],
    )
    def test_size(self, sample: Sample, len_: int) -> None:
        self._test_len(sample, len_)

    @pytest.mark.parametrize(
        ["sample", "keys"],
        [
            [
                MyTypedSample(
                    a=MemoryItem(10, JSONParser()),
                    b=MemoryItem(20, JSONParser()),
                    c=MemoryItem(30, JSONParser()),
                    d=MemoryItem(40, JSONParser()),
                ),
                ["a", "b", "c", "d"],
            ],
            [MyEmptyTypedSample(), []],
            [MySingleItemTypedSample(a=MemoryItem(4, JSONParser())), ["a"]],
        ],
    )
    def test_iter(self, sample: Sample, keys: list[str]) -> None:
        self._test_iter(sample, keys)

    @pytest.mark.parametrize(
        "sample",
        [
            MyTypedSample(
                a=MemoryItem(10, JSONParser()),
                b=MemoryItem(20, JSONParser()),
                c=MemoryItem(30, JSONParser()),
                d=MemoryItem(40, JSONParser()),
            ),
        ],
    )
    @pytest.mark.parametrize(
        "items",
        [
            {"a": MemoryItem(40, JSONParser())},
            {
                "a": MemoryItem(40, JSONParser()),
                "b": MemoryItem(80, JSONParser()),
                "c": MemoryItem(120, JSONParser()),
                "d": MemoryItem(160, JSONParser()),
            },
        ],
    )
    def test_with_items(self, sample: Sample, items: dict[str, Item]) -> None:
        self._test_with_items(sample, items)

    @pytest.mark.parametrize(
        "sample",
        [
            MyTypedSample(
                a=MemoryItem(10, JSONParser()),
                b=MemoryItem(20, JSONParser()),
                c=MemoryItem(30, JSONParser()),
                d=MemoryItem(40, JSONParser()),
            ),
        ],
    )
    def test_typeless(self, sample: Sample) -> None:
        tl = sample.typeless()
        assert isinstance(tl, TypelessSample)
        for k in sample.keys():
            assert sample.get(k) == tl.get(k)
