from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import nullcontext
from typing import ContextManager

import billiard.context
import billiard.pool

from typelime.dataset import Dataset
from typelime.sample import Sample


class Grabber[T: Sample]:
    def __init__(
        self, num_workers: int = 0, prefetch: int = 2, keep_order: bool = False
    ) -> None:
        super().__init__()
        self.num_workers = num_workers
        self.prefetch = prefetch
        self.keep_order = keep_order

    def __call__(
        self,
        dataset: Dataset[T],
        size: int | None = None,
        *,
        worker_init_fn: tuple[Callable, Sequence] | None = None,
    ) -> "_GrabContext[T]":
        return _GrabContext(self, dataset, size=size, worker_init_fn=worker_init_fn)

    def grab_all(
        self,
        dataset: Dataset[T],
        *,
        track_fn: Callable[[Iterable], Iterable] | None = None,
        sample_fn: Callable[[T, int], None] | None = None,
        size: int | None = None,
        grab_context_manager: ContextManager | None = None,
        worker_init_fn: Callable | tuple[Callable, Sequence] | None = None,
    ):
        if track_fn is None:
            track_fn = lambda x: x  # noqa: E731
        if sample_fn is None:
            sample_fn = lambda x, y: None  # noqa: E731
        if grab_context_manager is None:
            grab_context_manager = nullcontext()

        if isinstance(worker_init_fn, Callable):
            worker_init_fn = (worker_init_fn, ())

        with grab_context_manager:
            ctx = self(
                dataset,
                size=size,
                worker_init_fn=worker_init_fn,
            )
            with ctx as grabber_iterator:
                for idx, sample in track_fn(grabber_iterator):
                    sample_fn(sample, idx)


class _GrabWorker:
    def __init__(self, dataset: Dataset):
        self._dataset = dataset

    def _worker_fn_sample_and_index(self, idx) -> tuple[int, Sample]:
        return idx, self._dataset[idx]


class _GrabContext[T: Sample]:
    def __init__(
        self,
        grabber: Grabber[T],
        dataset: Dataset[T],
        size: int | None,
        worker_init_fn: tuple[Callable, Sequence] | None,
    ):
        self._grabber = grabber
        self._dataset = dataset
        self._size = size
        self._pool = None
        self._worker_init_fn = (None, ()) if worker_init_fn is None else worker_init_fn

    @staticmethod
    def wrk_init(user_init_fn):
        if user_init_fn[0] is not None:
            user_init_fn[0](*user_init_fn[1])

    def __enter__(self) -> Iterator[tuple[int, T]]:
        if self._grabber.num_workers == 0:
            self._pool = None
            return enumerate(iter(self._dataset))

        self._pool = billiard.pool.Pool(
            self._grabber.num_workers if self._grabber.num_workers > 0 else None,
            initializer=_GrabContext.wrk_init,
            initargs=(self._worker_init_fn,),
            # context=billiard.context.SpawnContext(),
        )
        runner = self._pool.__enter__()

        worker = _GrabWorker(self._dataset)
        fn = worker._worker_fn_sample_and_index

        if self._grabber.keep_order:
            return runner.imap(
                fn,
                range(len(self._dataset) if self._size is None else self._size),
                chunksize=self._grabber.prefetch,
            )  # type: ignore
        return runner.imap_unordered(
            fn,
            range(len(self._dataset) if self._size is None else self._size),
            chunksize=self._grabber.prefetch,
        )  # type: ignore

    def __exit__(self, exc_type, exc_value, traceback):
        if self._pool is not None:
            self._pool.__exit__(exc_type, exc_value, traceback)
