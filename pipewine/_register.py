from collections.abc import Callable, Generator, Sequence
from functools import partial
from typing import TypeVar
from uuid import uuid1

from pipewine.grabber import Grabber

T = TypeVar("T")


class LoopCallbackMixin:
    def __init__(self) -> None:
        super().__init__()
        self._on_start_cb: Callable[[str, int], None] | None = None
        self._on_iter_cb: Callable[[str, int], None] | None = None
        self._on_end_cb: Callable[[str], None] | None = None

    def register_on_enter(self, cb: Callable[[str, int], None] | None) -> None:
        self._on_start_cb = cb

    def register_on_iter(self, cb: Callable[[str, int], None] | None) -> None:
        self._on_iter_cb = cb

    def register_on_exit(self, cb: Callable[[str], None] | None) -> None:
        self._on_end_cb = cb

    def loop[
        T
    ](self, seq: Sequence[T], grabber: Grabber, name: str | None = None) -> Generator[
        tuple[int, T]
    ]:
        if name is None:
            name = self.__class__.__name__ + uuid1().hex
        if self._on_start_cb is not None:
            self._on_start_cb(name, len(seq))

        iter_cb = partial(self._on_iter_cb, name) if self._on_iter_cb else None
        with grabber(seq, callback=iter_cb) as ctx:
            for i, x in ctx:
                yield i, x
        if self._on_end_cb is not None:
            self._on_end_cb(name)
