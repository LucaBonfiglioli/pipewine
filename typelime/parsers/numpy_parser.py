import io
from collections.abc import Iterable

import numpy as np

from typelime.parsers.base import Parser


class NumpyNpyParser(Parser[np.ndarray]):
    def parse(self, data: bytes) -> np.ndarray:
        return np.load(data)

    def dump(self, data: np.ndarray) -> bytes:
        buffer = io.BytesIO()
        np.save(buffer, data)
        return buffer.read()

    @classmethod
    def extensions(cls) -> Iterable[str]:
        return [".npy"]
