from abc import ABC, abstractmethod
from pathlib import Path


class Storage(ABC):
    @abstractmethod
    def read(self) -> bytes:
        pass

    @abstractmethod
    def write(self, data: bytes) -> None:
        pass


class LocalFileStorage(Storage):
    def __init__(self, path: Path):
        self._path = path

    def read(self) -> bytes:
        with open(self._path, "rb") as fp:
            result = fp.read()
        return result

    def write(self, data: bytes) -> None:
        with open(self._path, "wb") as fp:
            fp.write(data)
