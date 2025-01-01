from collections.abc import Sequence

import pytest

from pipewine import Grabber
from pipewine._register import LoopCallbackMixin


class MyEnterCallback:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def __call__(self, name: str, total: int) -> None:
        self.calls.append((name, total))


class MyIterCallback:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def __call__(self, name: str, idx: int) -> None:
        self.calls.append((name, idx))


class MyExitCallback:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(self, name: str) -> None:
        self.calls.append(name)


class TestRegisterCallbackMixin:
    @pytest.mark.parametrize("name", ["name", None])
    def test_loop(self, name: str | None) -> None:
        obj = LoopCallbackMixin()
        on_enter = MyEnterCallback()
        on_iter = MyIterCallback()
        on_exit = MyExitCallback()
        obj.register_on_enter(on_enter)
        obj.register_on_iter(on_iter)
        obj.register_on_exit(on_exit)
        seq = range(10)
        grabber = Grabber()
        for _ in obj.loop(seq, grabber, name=name):
            pass
        assert len(on_iter.calls) == len(seq)
        for name, idx in on_iter.calls:
            assert isinstance(name, str)
        assert len(on_enter.calls) == 1
        assert on_enter.calls[0] == (name, 10)
        assert len(on_exit.calls) == 1
        assert on_exit.calls[0] == name
