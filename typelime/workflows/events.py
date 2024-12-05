from abc import ABC, abstractmethod
from queue import Empty, Queue
from typing import Any


class Event:
    pass


class EventQueue(ABC):
    @abstractmethod
    def emit(self, event: Event) -> None: ...

    @abstractmethod
    def capture(self) -> Event | None: ...

    @abstractmethod
    def close(self) -> None: ...


class InMemoryEventQueue(EventQueue):
    def __init__(self) -> None:
        super().__init__()
        self._queue: Queue[Event] = Queue()
        self._closed = False

    def emit(self, event: Event) -> None:
        if self._closed:
            raise RuntimeError("Queue is closed")
        self._queue.put(event)

    def capture(self) -> Event | None:
        if self._closed:
            raise RuntimeError("Queue is closed")
        try:
            return self._queue.get_nowait()
        except Empty:
            return None

    def close(self) -> None:
        self._closed = True

    def __getstate__(self) -> dict[str, Any]:
        data = {**self.__dict__}
        del data["_queue"]
        return data

    def __setstate__(self, data: dict[str, Any]) -> None:
        self._queue = Queue()
        for k, v in data.items():
            setattr(self, k, v)
