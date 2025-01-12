import pickle
from typing import Any

import pytest

from pipewine import Bundle


class EmptyBundle(Bundle[int]):
    pass


class BundleA(Bundle[int | float | str]):
    a: int
    b: str
    c: int
    d: float


class BundleB(Bundle[None]):
    a: None


class TestBundle:
    def test_creation(self) -> None:
        bundle = BundleA(a=10, b="hello", c=5, d=10.2)
        assert bundle.a == 10
        assert bundle.b == "hello"
        assert bundle.c == 5
        assert bundle.d == 10.2

    @pytest.mark.parametrize(
        ["cls", "params"],
        [
            [BundleA, {"a": 12, "b": "alice", "c": -12, "d": 1231.0}],
            [BundleB, {"a": None}],
            [EmptyBundle, {}],
        ],
    )
    def test_as_dict(self, cls: type[Bundle], params: dict[str, Any]) -> None:
        assert cls(**params).as_dict() == params

    @pytest.mark.parametrize(
        ["cls", "params"],
        [
            [BundleA, {"a": 12, "b": "alice", "c": -12, "d": 1231.0}],
            [BundleB, {"a": None}],
            [EmptyBundle, {}],
        ],
    )
    def test_from_dict(self, cls: type[Bundle], params: dict[str, Any]) -> None:
        assert cls.from_dict(**params) == cls(**params)

    @pytest.mark.parametrize(
        "bundle",
        [
            BundleA(a=12, b="alice", c=-12, d=1231.0),
            BundleB(a=None),
            EmptyBundle(),
        ],
    )
    def test_pickle(self, bundle: Bundle) -> None:
        assert bundle == pickle.loads(pickle.dumps(bundle))
