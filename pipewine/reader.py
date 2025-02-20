"""`Reader` base class and implementations used by Pipewine `Item` instances to read 
data.
"""

from abc import ABC, abstractmethod
from pathlib import Path


class Reader(ABC):
    """Base class for all Pipewine readers. A reader is an object that reads data from
    a source and returns it as a byte string.

    All subclasses must implement the `read` method.
    """

    @abstractmethod
    def read(self) -> bytes:
        """Read data from the source and return it as a byte string."""
        pass


class LocalFileReader(Reader):
    """Reader implementation that reads data from a local file."""

    def __init__(self, path: Path):
        """
        Args:
            path (Path): Path to the file to read.
        """
        self._path = path

    def read(self) -> bytes:
        with open(self._path, "rb") as fp:
            result = fp.read()
        return result

    @property
    def path(self) -> Path:
        """Return the path to the file being read."""
        return self._path
