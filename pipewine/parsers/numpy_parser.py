"""Parser for arbitrary NumPy arrays."""

import io
from collections.abc import Iterable

import numpy as np

from pipewine.parsers.base import Parser


class NumpyNpyParser(Parser[np.ndarray]):
    """Parser for NumPy arrays saved in the `.npy` format."""

    def parse(self, data: bytes) -> np.ndarray:
        buffer = io.BytesIO(data)
        return np.load(buffer)

    def dump(self, data: np.ndarray) -> bytes:
        buffer = io.BytesIO()
        np.save(buffer, data)
        buffer.seek(0)
        return buffer.read()

    @classmethod
    def extensions(cls) -> Iterable[str]:
        return ["npy"]
