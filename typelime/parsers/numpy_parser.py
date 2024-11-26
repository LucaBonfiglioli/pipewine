import io
from collections.abc import Iterable

import numpy as np

from typelime.parsers.base import Parser


class NumpyNpyParser(Parser[np.ndarray]):
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
        return [".npy"]
