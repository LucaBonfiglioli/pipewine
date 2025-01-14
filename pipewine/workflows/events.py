from abc import ABC, abstractmethod
from multiprocessing import Queue, get_context
from queue import Empty
from typing import Any, cast
from uuid import uuid1

from pipewine.grabber import InheritedData


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


class ProcessSharedEventQueue(EventQueue):
    def __init__(self) -> None:
        super().__init__()
        self._mp_q: Queue | None = None
        self._id = uuid1().hex

    def start(self) -> None:
        self._mp_q = get_context("spawn").Queue()
        self._mp_q.cancel_join_thread()
        InheritedData.data[self._id] = self._mp_q

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
        if self._mp_q is not None:
            self._mp_q.close()
            del InheritedData.data[self._id]
        self._mp_q = None

    def __getstate__(self) -> dict[str, Any]:  # pragma: no cover
        data = {**self.__dict__}
        del data["_mp_q"]
        return data

    def __setstate__(self, data: dict[str, Any]) -> None:
        self._id = data["_id"]
        self._mp_q = cast(Queue, InheritedData.data[self._id])
        self._mp_q.cancel_join_thread()
