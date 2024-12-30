from collections.abc import Sequence

import pytest

from pipewine import Grabber
from pipewine._register import RegisterCallbackMixin


class MyCallback:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int, Sequence]] = []

    def __call__(self, name: str, idx: int, seq: Sequence) -> None:
        self.calls.append((name, idx, seq))


class TestRegisterCallbackMixin:
    @pytest.mark.parametrize("name", ["name", None])
    def test_loop(self, name: str | None) -> None:
        obj = RegisterCallbackMixin()
        cb = MyCallback()
        obj.register_callback(cb)
        seq = range(10)
        grabber = Grabber()
        for _ in obj.loop(seq, grabber, name=name):
            pass
        assert len(cb.calls) == len(seq)
        for name, idx, seq_ in cb.calls:
            assert isinstance(name, str)
            assert seq_ is seq
