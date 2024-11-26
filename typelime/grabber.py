from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import nullcontext
from typing import ContextManager, cast

import billiard.context
import billiard.pool


class Grabber[T]:
    def __init__(
        self, num_workers: int = 0, prefetch: int = 2, keep_order: bool = False
    ) -> None:
        super().__init__()
        self.num_workers = num_workers
        self.prefetch = prefetch
        self.keep_order = keep_order

    def __call__(
        self,
        seq: Sequence[T],
        size: int | None = None,
        *,
        worker_init_fn: tuple[Callable, Sequence] | None = None,
    ) -> "_GrabContext[T]":
        return _GrabContext(self, seq, size=size, worker_init_fn=worker_init_fn)

    def grab_all(
        self,
        seq: Sequence[T],
        *,
        track_fn: Callable[[Iterable], Iterable] | None = None,
        elem_fn: Callable[[T, int], None] | None = None,
        size: int | None = None,
        grab_context_manager: ContextManager | None = None,
        worker_init_fn: Callable | tuple[Callable, Sequence] | None = None,
    ):
        if track_fn is None:
            track_fn = lambda x: x  # noqa: E731
        if elem_fn is None:
            elem_fn = lambda x, y: None  # noqa: E731

        grab_cm = (
            nullcontext() if grab_context_manager is None else grab_context_manager
        )
        worker_init = (
            (worker_init_fn, ()) if callable(worker_init_fn) else worker_init_fn
        )

        with grab_cm:
            ctx = self(seq, size=size, worker_init_fn=worker_init)
            with ctx as grabber_iterator:
                for idx, elem in track_fn(grabber_iterator):
                    elem_fn(elem, idx)


class _GrabWorker[T]:
    def __init__(self, seq: Sequence[T]):
        self._seq = seq

    def _worker_fn_elem_and_index(self, idx) -> tuple[int, T]:
        return idx, self._seq[idx]


class _GrabContext[T]:
    def __init__(
        self,
        grabber: Grabber[T],
        seq: Sequence[T],
        size: int | None,
        worker_init_fn: tuple[Callable, Sequence] | None,
    ):
        self._grabber = grabber
        self._seq = seq
        self._size = size
        self._pool = None
        self._worker_init_fn = (None, ()) if worker_init_fn is None else worker_init_fn

    @staticmethod
    def wrk_init(user_init_fn):  # pragma: no cover
        if user_init_fn[0] is not None:
            user_init_fn[0](*user_init_fn[1])

    def __enter__(self) -> Iterator[tuple[int, T]]:
        if self._grabber.num_workers == 0:
            self._pool = None
            worker = _GrabWorker(self._seq)
            return (worker._worker_fn_elem_and_index(i) for i in range(len(self._seq)))

        self._pool = billiard.pool.Pool(
            self._grabber.num_workers if self._grabber.num_workers > 0 else None,
            initializer=_GrabContext.wrk_init,
            initargs=(self._worker_init_fn,),
            context=billiard.context.SpawnContext(),
        )
        runner = cast(billiard.pool.Pool, self._pool).__enter__()

        worker = _GrabWorker(self._seq)
        fn = worker._worker_fn_elem_and_index

        if self._grabber.keep_order:
            return runner.imap(
                fn,
                range(len(self._seq) if self._size is None else self._size),
                chunksize=self._grabber.prefetch,
            )  # type: ignore
        return runner.imap_unordered(
            fn,
            range(len(self._seq) if self._size is None else self._size),
            chunksize=self._grabber.prefetch,
        )  # type: ignore

    def __exit__(self, exc_type, exc_value, traceback):
        if self._pool is not None:
            self._pool.__exit__(exc_type, exc_value, traceback)
