import curses
import os
import time
from typing import no_type_check

import pytest

from pipewine.workflows import (
    CursesTracker,
    Event,
    EventQueue,
    SharedMemoryEventQueue,
    TaskCompleteEvent,
    TaskStartEvent,
    TaskUpdateEvent,
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


class TestCursesTracker:
    def test_curses_tracker(self, event_queue: EventQueue) -> None:
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
