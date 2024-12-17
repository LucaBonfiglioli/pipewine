from collections.abc import Callable, Generator, Sequence
from typing import TypeVar
from uuid import uuid1

from typelime.grabber import Grabber

T = TypeVar("T")

CallbackType = Callable[[str, int, Sequence[T]], None]


class RegisterCallbackMixin:
    def __init__(self) -> None:
        super().__init__()
        self._callbacks: list[CallbackType] = []

    def register_callback(self, cb: CallbackType) -> None:
        self._callbacks.append(cb)

    def loop[
        T
    ](self, seq: Sequence[T], grabber: Grabber, name: str | None = None) -> Generator[
        tuple[int, T]
    ]:
        if name is None:
            name = self.__class__.__name__ + uuid1().hex
        with grabber(seq) as ctx:
            for i, x in ctx:
                for cb in self._callbacks:
                    cb(name, i, seq)
                yield i, x
