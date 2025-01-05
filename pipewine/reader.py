from abc import ABC, abstractmethod
from pathlib import Path


class Reader(ABC):
    @abstractmethod
    def read(self) -> bytes:
        pass


class LocalFileReader(Reader):
    def __init__(self, path: Path):
        self._path = path

    def read(self) -> bytes:
        with open(self._path, "rb") as fp:
            result = fp.read()
        return result

    @property
    def path(self) -> Path:
        return self._path
