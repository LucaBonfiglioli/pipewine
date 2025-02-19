"""Multiprocessing utilities for iterating over a sequence with parallelism."""

from collections.abc import Callable, Iterator, Sequence
from multiprocessing.pool import Pool
from multiprocessing import get_context
from typing import Any
from signal import SIGINT, SIG_IGN, signal


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


class InheritedData:
    """Data that is inherited by all subprocesses at creation time. This is a
    workaround to allow arbitrary data to be shared between the main and child process
    using inheritance.

    This is crucial to pass things like sockets, file descriptors, and other resources
    that can only be shared between processes through inheritance.

    Note: The data stored in this class is only shared at the time of the subprocess
    creation, and it is not updated if the data changes in the main or child process, as
    it is not shared memory nor managed by any synchronization mechanism.
    """

    data: dict[str, Any] = {}
    """Dict-like container for all the data that is inherited by the subprocesses."""


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
        self._pool: Pool | None = None
        self._callback = callback
        self._worker_init_fn = (None, ()) if worker_init_fn is None else worker_init_fn

    @staticmethod
    def wrk_init(static_data: dict[str, Any], user_init_fn):  # pragma: no cover
        signal(SIGINT, SIG_IGN)
        InheritedData.data = static_data
        if user_init_fn[0] is not None:
            user_init_fn[0](*user_init_fn[1])

    def __enter__(self) -> Iterator[tuple[int, T]]:
        worker = _GrabWorker(self._seq, callback=self._callback)
        if self._num_workers == 0:
            self._pool = None
            return (worker._worker_fn_elem_and_index(i) for i in range(len(self._seq)))

        self._pool = get_context("spawn").Pool(
            self._num_workers if self._num_workers > 0 else None,
            initializer=_GrabContext.wrk_init,
            initargs=(
                InheritedData.data,
                self._worker_init_fn,
            ),
        )
        pool = self._pool.__enter__()

        fn = worker._worker_fn_elem_and_index
        if self._keep_order:
            return pool.imap(fn, range(len(self._seq)), chunksize=self._prefetch)
        return pool.imap_unordered(fn, range(len(self._seq)), chunksize=self._prefetch)

    def __exit__(self, exc_type, exc_value, traceback):
        if self._pool is not None:
            self._pool.__exit__(exc_type, exc_value, traceback)
            self._pool = None


class Grabber:
    """Grabber utility for iterating over a sequence using parallelism."""

    def __init__(
        self, num_workers: int = 0, prefetch: int = 2, keep_order: bool = True
    ) -> None:
        """
        Args:
            num_workers (int, optional): Number of worker processes to use for
                parallelism. If 0, no parallelism is used. Defaults to 0.
            prefetch (int, optional): Number of elements to prefetch in each worker
                process. Defaults to 2.
            keep_order (bool, optional): Whether to keep the order of the elements in
                the sequence. If `True`, the elements are yielded in the same order as they
                appear in the sequence. If `False`, the elements are yielded in the
                order they are processed. Defaults to `True`.
        """
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
        """Create a context manager for iterating over a sequence with parallelism.

        Note: only the call to the `__getitem__` method is actually parallelized, the
        rest of the iteration is done in the main process, making this useful when
        iterating over `LazyDataset` instances, that may perform expensive operations
        when fetching the samples.

        Args:
            seq (Sequence[T]): Sequence of elements to iterate over.
            callback (Callable[[int], None] | None, optional): Optional callback
                function to be called on each iteration. The callback function should
                accept the index of the current element as its only argument. Defaults
                to `None`.
            worker_init_fn (tuple[Callable, Sequence] | None, optional): Optional tuple
                containing a function and a sequence of arguments to be called in each
                worker process before starting the iteration. Defaults to `None`.

        Returns:
            _GrabContext[T]: Context manager for iterating over the sequence with
                parallelism.

        Examples:
            ```python
            seq = list(range(10))
            grabber = Grabber(num_workers=4, prefetch=4)
            with grabber(seq) as it:
                for idx, elem in it:
                    print(idx, elem)
            ```
        """
        return _GrabContext(
            self.num_workers,
            self.prefetch,
            self.keep_order,
            seq,
            callback=callback,
            worker_init_fn=worker_init_fn,
        )
