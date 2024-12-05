from collections.abc import Sequence
from typing import Any

import pytest

from typelime import Grabber


class RaisingSequence(Sequence[int]):
    def __init__(self, exception: Exception) -> None:
        super().__init__()
        self._exception = exception

    def __getitem__(self, x: int | slice) -> Sequence[int] | int:  # type: ignore
        if x == 3:
            raise self._exception
        return 4

    def __len__(self) -> int:
        return 10


class TestGrabber:
    @pytest.mark.parametrize("sequence", [list(range(100))])
    @pytest.mark.parametrize("workers", [0, 2, 4, 8])
    @pytest.mark.parametrize("prefetch", [1, 5])
    @pytest.mark.parametrize("keep_order", [True, False])
    @pytest.mark.parametrize(
        ["pass_track_fn", "pass_elem_fn"], [[True, True], [False, False]]
    )
    def test_grab_all(
        self,
        sequence: Sequence,
        workers: int,
        prefetch: int,
        keep_order: bool,
        pass_track_fn: bool,
        pass_elem_fn: bool,
    ) -> None:
        _track_fn_called = 0
        _elem_fn_called = 0

        def _track_fn(x):
            nonlocal _track_fn_called
            _track_fn_called += 1
            return x

        def _elem_fn(i, x):
            nonlocal _elem_fn_called
            _elem_fn_called += 1
            return x

        grabber: Grabber = Grabber(
            num_workers=workers, prefetch=prefetch, keep_order=keep_order
        )
        kwargs: dict[str, Any] = {}
        if pass_track_fn:
            kwargs["track_fn"] = _track_fn
        if pass_elem_fn:
            kwargs["elem_fn"] = _elem_fn
        grabber.grab_all(sequence, **kwargs)  # type: ignore
        if pass_track_fn:
            assert _track_fn_called == 1
        if pass_elem_fn:
            assert _elem_fn_called == len(sequence)

    @pytest.mark.parametrize("exception", [Exception("curse")])
    @pytest.mark.parametrize("workers", [0, 4])
    def test_grab_all_fails(self, exception: Exception, workers: int) -> None:
        seq = RaisingSequence(exception)
        grabber: Grabber = Grabber(workers)
        with pytest.raises(type(exception)):
            grabber.grab_all(seq)
