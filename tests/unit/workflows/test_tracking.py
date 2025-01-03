import os
import time
from typing import no_type_check

import pytest
import curses

from pipewine.workflows import (
    CursesTracker,
    EventQueue,
    SharedMemoryEventQueue,
    TaskCompleteEvent,
    TaskStartEvent,
    TaskUpdateEvent,
    Event,
)


class MyEvent(Event):
    pass


@pytest.fixture()
@no_type_check
def event_queue() -> EventQueue:
    queue = SharedMemoryEventQueue()
    queue.start()
    yield queue
    queue.close()


def noop(*args, **kwargs):
    pass


class TestCursesTracker:
    @pytest.mark.parametrize("size", [(24, 80), (40, 120), (10, 20)])
    def test_curses_tracker(
        self, monkeypatch, event_queue: EventQueue, size: tuple[int, int]
    ) -> None:
        monkeypatch.setattr(curses, "cbreak", noop)
        monkeypatch.setattr(curses, "nocbreak", noop)
        monkeypatch.setattr(curses, "endwin", noop)
        monkeypatch.setattr(os, "get_terminal_size", lambda: (size[1], size[0]))
        tracker = CursesTracker()
        tracker.attach(event_queue)
        time.sleep(0.2)

        for _ in range(5):
            event_queue.emit(MyEvent())  # should be ignored

        tasks = [
            ("my_task_group/my_task_0", 100),
            ("my_task_group/my_task_1", 10),
            ("my_task_group/my_task_2", 50),
        ]
        for task, total in tasks:
            event_queue.emit(TaskStartEvent(task, total))
            time.sleep(0.1)
            for x in range(total):
                event_queue.emit(TaskUpdateEvent(task, x))
                time.sleep(0.01)
            event_queue.emit(TaskCompleteEvent(task))
            time.sleep(0.1)

        event_queue.emit(MyEvent())  # should be ignored

        tracker.detach()
