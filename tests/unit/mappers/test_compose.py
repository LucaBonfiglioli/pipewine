import numpy as np
import pytest

from pipewine import (
    ComposeMapper,
    Item,
    Mapper,
    MemoryItem,
    NumpyNpyParser,
    PickleParser,
    TypedSample,
)


class MySample1(TypedSample):
    a: Item[int]
    b: Item[float]
    c: Item[np.ndarray]


class MySample2(TypedSample):
    a: Item[int]
    b: Item[np.ndarray]


class MySample3(TypedSample):
    a: Item[str]


class MySample4(TypedSample):
    b: Item[dict]


class Mapper1_2(Mapper[MySample1, MySample2]):
    def __call__(self, idx: int, x: MySample1) -> MySample2:
        return MySample2(a=x.a, b=x.c)


class Mapper2_3(Mapper[MySample2, MySample3]):
    def __call__(self, idx: int, x: MySample2) -> MySample3:
        return MySample3(a=MemoryItem(str(x.a()), PickleParser()))


class Mapper3_4(Mapper[MySample3, MySample4]):
    def __call__(self, idx: int, x: MySample3) -> MySample4:
        return MySample4(b=MemoryItem({"value": x.a()}, PickleParser()))


class TestComposeMapper:
    @pytest.mark.parametrize(
        "sample",
        [
            MySample1(
                a=MemoryItem(10, PickleParser()),
                b=MemoryItem(25.4, PickleParser()),
                c=MemoryItem(np.arange(10), NumpyNpyParser()),
            )
        ],
    )
    def test_call(self, sample: MySample1) -> None:
        mapper = ComposeMapper(Mapper1_2())
        sample2 = mapper(0, sample)
        assert isinstance(sample2, MySample2)
        assert sample2.a() == sample.a()
        assert (sample2.b() == sample.c()).all()

        mapper = ComposeMapper((Mapper1_2(), Mapper2_3()))
        sample3 = mapper(0, sample)
        assert isinstance(sample3, MySample3)
        assert sample3.a() == str(sample.a())

        mapper = ComposeMapper((Mapper1_2(), Mapper2_3(), Mapper3_4()))
        sample4 = mapper(0, sample)
        assert isinstance(sample4, MySample4)
        assert sample4.b()["value"] == str(sample.a())
