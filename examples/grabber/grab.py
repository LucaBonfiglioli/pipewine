import os
import time
from collections.abc import Sequence
from typing import overload

from pipewine import Grabber


class SlowSequence(Sequence[int]):
    """Return the numbers in range [A, B), slowly."""

    def __init__(self, start: int, stop: int) -> None:
        self._start = start
        self._stop = stop

    def __len__(self) -> int:
        return self._stop - self._start

    @overload
    def __getitem__(self, idx: int) -> int: ...
    @overload
    def __getitem__(self, idx: slice) -> "SlowSequence": ...
    def __getitem__(self, idx: int | slice) -> "SlowSequence | int":
        # Let's not care about slicing for now
        if isinstance(idx, slice):
            raise NotImplementedError()

        # Raise if index is out of bounds
        if idx >= len(self):
            raise IndexError(idx)

        # Simulate some slow operation and return
        time.sleep(0.1)
        return idx + self._start


# Callback function that simply prints to stdout
def my_callback(index: int) -> None:
    print(f"Callback {index} invoked by process {os.getpid()}")


def main_no_grabber() -> None:
    sequence = SlowSequence(100, 200)

    t1 = time.perf_counter()
    total = 0
    for i, x in enumerate(sequence):
        my_callback(i)
        total += x
    t2 = time.perf_counter()
    print("Time (s):", t2 - t1)
    print("Total:", total)


def main_with_grabber() -> None:
    sequence = SlowSequence(100, 200)

    t1 = time.perf_counter()
    total = 0
    grabber = Grabber(num_workers=5)
    with grabber(sequence, callback=my_callback) as par_sequence:
        for i, x in par_sequence:
            total += x
    t2 = time.perf_counter()
    print("Time (s):", t2 - t1)
    print("Total:", total)


if __name__ == "__main__":
    main_no_grabber()
    main_with_grabber()
