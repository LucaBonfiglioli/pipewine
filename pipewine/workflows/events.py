from abc import ABC, abstractmethod
from multiprocessing import Manager
from queue import Empty, Queue
from typing import Any


class Event:
    pass


class EventQueue(ABC):
    @abstractmethod
    def start(self) -> None: ...

    @abstractmethod
    def emit(self, event: Event) -> None: ...

    @abstractmethod
    def capture(self) -> Event | None: ...

    @abstractmethod
    def close(self) -> None: ...


class SharedMemoryEventQueue(EventQueue):
    def __init__(self) -> None:
        super().__init__()
        self._mp_q: Queue | None = None

    def start(self) -> None:
        self._manager = Manager()
        self._mp_q = self._manager.Queue()

    def emit(self, event: Event) -> None:
        if self._mp_q is None:
            raise RuntimeError("Queue is closed")
        self._mp_q.put(event)

    def capture(self) -> Event | None:
        if self._mp_q is None:
            raise RuntimeError("Queue is closed")
        try:
            return self._mp_q.get_nowait()
        except Empty:
            return None

    def close(self) -> None:
        self._manager.shutdown()
        self._mp_q = None

    def __getstate__(self) -> dict[str, Any]:
        data = {**self.__dict__}
        del data["_manager"]
        return data
