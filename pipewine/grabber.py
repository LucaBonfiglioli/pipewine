from collections.abc import Callable, Iterator, Sequence
from multiprocessing import Pool


class _GrabWorker[T]:
    def __init__(
        self, seq: Sequence[T], callback: Callable[[int], None] | None = None
    ) -> None:
        self._seq = seq
        self._callback = callback

    def _worker_fn_elem_and_index(self, idx: int) -> tuple[int, T]:
        if self._callback is not None:
            self._callback(idx)
        return idx, self._seq[idx]


class _GrabContext[T]:
    def __init__(
        self,
        num_workers: int,
        prefetch: int,
        keep_order: bool,
        seq: Sequence[T],
        callback: Callable[[int], None] | None,
        worker_init_fn: tuple[Callable, Sequence] | None,
    ):
        self._num_workers = num_workers
        self._prefetch = prefetch
        self._keep_order = keep_order
        self._seq = seq
        self._pool = None
        self._callback = callback
        self._worker_init_fn = (None, ()) if worker_init_fn is None else worker_init_fn

    @staticmethod
    def wrk_init(user_init_fn):  # pragma: no cover
        if user_init_fn[0] is not None:
            user_init_fn[0](*user_init_fn[1])

    def __enter__(self) -> Iterator[tuple[int, T]]:
        worker = _GrabWorker(self._seq, callback=self._callback)
        if self._num_workers == 0:
            self._pool = None
            return (worker._worker_fn_elem_and_index(i) for i in range(len(self._seq)))

        self._pool = Pool(
            self._num_workers if self._num_workers > 0 else None,
            initializer=_GrabContext.wrk_init,
            initargs=(self._worker_init_fn,),
        )
        pool = self._pool.__enter__()

        fn = worker._worker_fn_elem_and_index
        if self._keep_order:
            return pool.imap(fn, range(len(self._seq)), chunksize=self._prefetch)
        return pool.imap_unordered(fn, range(len(self._seq)), chunksize=self._prefetch)

    def __exit__(self, exc_type, exc_value, traceback):
        if self._pool is not None:
            self._pool.__exit__(exc_type, exc_value, traceback)


class Grabber:
    def __init__(
        self, num_workers: int = 0, prefetch: int = 2, keep_order: bool = False
    ) -> None:
        super().__init__()
        self.num_workers = num_workers
        self.prefetch = prefetch
        self.keep_order = keep_order

    def __call__[
        T
    ](
        self,
        seq: Sequence[T],
        *,
        callback: Callable[[int], None] | None = None,
        worker_init_fn: tuple[Callable, Sequence] | None = None,
    ) -> _GrabContext[T]:
        return _GrabContext(
            self.num_workers,
            self.prefetch,
            self.keep_order,
            seq,
            callback=callback,
            worker_init_fn=worker_init_fn,
        )
