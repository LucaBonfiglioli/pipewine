import os
import time
from dataclasses import dataclass
from functools import partial
from multiprocessing.pool import Pool
from typing import cast

import pytest

from pipewine import Event, EventQueue, SharedMemoryEventQueue


@dataclass
class MyEvent(Event):
    pid: int
    data: int


class TestSharedMemoryEventQueue:
    def _worker(self, queue: EventQueue, num: int) -> int:
        time.sleep(0.01)
        queue.emit(MyEvent(os.getpid(), num))
        return num

    @pytest.mark.parametrize("nproc", [1, 2, 4, 8])
    def test_queue(self, nproc: int) -> None:
        n = 100
        queue = SharedMemoryEventQueue()
        queue.start()
        pool = Pool(nproc)
        list(pool.imap(partial(self._worker, queue), range(n)))
        pool.close()
        pool.join()
        events: list[MyEvent] = []
        while (event := queue.capture()) is not None:
            events.append(cast(MyEvent, event))
        assert len({x.pid for x in events}) == nproc
        assert {x.data for x in events} == set(range(n))
        queue.close()

    def test_queue_wrong_state(self) -> None:
        queue = SharedMemoryEventQueue()
        with pytest.raises(RuntimeError):
            queue.emit(MyEvent(os.getpid(), 42))

        with pytest.raises(RuntimeError):
            queue.capture()
