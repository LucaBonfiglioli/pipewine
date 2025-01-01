from collections.abc import Sequence

import pytest

from pipewine import Grabber
from pipewine._register import LoopCallbackMixin


class MyCallback:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def __call__(self, name: str, idx: int) -> None:
        self.calls.append((name, idx))


class TestRegisterCallbackMixin:
    @pytest.mark.parametrize("name", ["name", None])
    def test_loop(self, name: str | None) -> None:
        obj = LoopCallbackMixin()
        cb = MyCallback()
        obj.register_on_iter(cb)
        seq = range(10)
        grabber = Grabber()
        for _ in obj.loop(seq, grabber, name=name):
            pass
        assert len(cb.calls) == len(seq)
        for name, idx in cb.calls:
            assert isinstance(name, str)
