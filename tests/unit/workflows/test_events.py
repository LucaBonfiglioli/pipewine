import os
import time
from dataclasses import dataclass
from functools import partial
from multiprocessing import get_context
from typing import cast

import pytest

from pipewine.workflows import Event, EventQueue, ProcessSharedEventQueue
from pipewine.grabber import InheritedData


@dataclass
class MyEvent(Event):
    pid: int
    data: int


class TestProcessSharedEventQueue:
    def _worker(self, queue: EventQueue, num: int) -> int:
        time.sleep(0.01)
        queue.emit(MyEvent(os.getpid(), num))
        return num

    def _init(self, data) -> None:
        InheritedData.data = data

    @pytest.mark.parametrize("nproc", [1, 2, 4, 8])
    @pytest.mark.parametrize("blocking", [True, False])
    def test_queue(self, nproc: int, blocking: bool) -> None:
        n = 100
        queue = ProcessSharedEventQueue()
        queue.start()
        pool = get_context("spawn").Pool(
            nproc, initializer=self._init, initargs=(InheritedData.data,)
        )
        list(pool.imap(partial(self._worker, queue), range(n)))
        pool.close()
        pool.join()
        events: list[MyEvent] = []
        while (
            event := (
                queue.capture_blocking(timeout=0.1) if blocking else queue.capture()
            )
        ) is not None:
            events.append(cast(MyEvent, event))
        assert len({x.pid for x in events}) == nproc
        assert {x.data for x in events} == set(range(n))
        queue.close()

        # Repeat close to assert nothing happens if called twice
        queue.close()

    @pytest.mark.parametrize("blocking", [True, False])
    def test_queue_wrong_state(self, blocking: bool) -> None:
        queue = ProcessSharedEventQueue()
        with pytest.raises(RuntimeError):
            queue.emit(MyEvent(os.getpid(), 42))

        with pytest.raises(RuntimeError):
            if blocking:
                queue.capture_blocking(timeout=0.5)
            else:
                queue.capture()
