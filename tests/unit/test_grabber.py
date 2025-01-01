from collections.abc import Sequence
from typing import overload

import pytest

from pipewine import Grabber


class RaisingSequence(Sequence[int]):
    def __init__(self, exception: Exception) -> None:
        super().__init__()
        self._exception = exception

    @overload
    def __getitem__(self, x: int, /) -> int: ...

    @overload
    def __getitem__(self, x: slice, /) -> Sequence[int]: ...

    def __getitem__(self, x: int | slice, /) -> int | Sequence[int]:  # type: ignore
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
    def test_call(
        self, sequence: Sequence, workers: int, prefetch: int, keep_order: bool
    ) -> None:
        grabber: Grabber = Grabber(
            num_workers=workers, prefetch=prefetch, keep_order=keep_order
        )
        with grabber(sequence) as ctx:
            for _ in ctx:
                pass

    @pytest.mark.parametrize("workers", [0, 2, 4, 8])
    @pytest.mark.parametrize("prefetch", [1, 5])
    @pytest.mark.parametrize("keep_order", [True, False])
    @pytest.mark.parametrize("exc", [RuntimeError(), ValueError()])
    def test_call_raises(
        self, workers: int, prefetch: int, keep_order: bool, exc: Exception
    ) -> None:
        grabber = Grabber(num_workers=workers, prefetch=prefetch, keep_order=keep_order)
        seq = RaisingSequence(exc)
        with grabber(seq) as ctx:
            with pytest.raises(type(exc)):
                for _ in ctx:
                    pass
