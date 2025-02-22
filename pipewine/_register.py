"""Private module that contains the `LoopCallbackMixin` class, which is a mixin class
for adding callback hooks on loop start, iteration, and end, used for custom behavior
in `DatasetOperator`, `DatasetSource`, and `DatasetSink` classes.
"""

from collections.abc import Callable, Generator, Sequence
from functools import partial
from typing import TypeVar
from uuid import uuid1

from pipewine.grabber import Grabber

T = TypeVar("T")


class LoopCallbackMixin:
    """Mixin class for adding callback hooks on loop start, iteration, and end;
    allowing for custom behavior to be injected into the loop, such as progress
    tracking, logging, etc.

    Every `DatasetOperator`, `DatasetSource`, and `DatasetSink` class automatically
    inherits from this mixin.
    """

    def __init__(self) -> None:
        super().__init__()
        self._on_start_cb: Callable[[str, int], None] | None = None
        self._on_iter_cb: Callable[[str, int], None] | None = None
        self._on_end_cb: Callable[[str], None] | None = None

    def register_on_enter(self, cb: Callable[[str, int], None] | None) -> None:
        """Register a callback to be called when the loop starts.

        Args:
            cb (Callable[[str, int], None] | None): Callback function to be called
                when the loop starts. The callback function should accept two
                arguments: the name of the loop, and the total number of iterations
                to be performed.
        """
        self._on_start_cb = cb

    def register_on_iter(self, cb: Callable[[str, int], None] | None) -> None:
        """Register a callback to be called on every iteration of the loop.

        Args:
            cb (Callable[[str, int], None] | None): Callback function to be called
                on every iteration of the loop. The callback function should accept
                two arguments: the name of the loop, and the current iteration index.
        """
        self._on_iter_cb = cb

    def register_on_exit(self, cb: Callable[[str], None] | None) -> None:
        """Register a callback to be called when the loop ends.

        Args:
            cb (Callable[[str], None] | None): Callback function to be called when
                the loop ends. The callback function should accept one argument: the
                name of the loop.
        """
        self._on_end_cb = cb

    def loop[
        T
    ](
        self, seq: Sequence[T], grabber: Grabber | None = None, name: str | None = None
    ) -> Generator[tuple[int, T]]:
        """Loop over a sequence of items, yielding the index and item.

        Args:
            seq (Sequence[T]): Sequence of items to loop over.
            grabber (Grabber | None, optional): Grabber to use for iterating over the
                sequence with multi-processing parallelism.
                If `None`, a new `Grabber` instance will be created default settings
                (no parallelism). Defaults to `None`.
            name (str | None, optional): Name of the loop. If `None`, a unique name
                will be generated. Defaults to `None`.
        """
        if name is None:
            name = self.__class__.__name__ + uuid1().hex
        if self._on_start_cb is not None:
            self._on_start_cb(name, len(seq))

        iter_cb = partial(self._on_iter_cb, name) if self._on_iter_cb else None
        grabber_ = grabber or Grabber()
        with grabber_(seq, callback=iter_cb) as ctx:
            for i, x in ctx:
                yield i, x
        if self._on_end_cb is not None:
            self._on_end_cb(name)
