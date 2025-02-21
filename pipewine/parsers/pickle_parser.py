"""Parsers for arbitrary python objects using the `pickle` library."""

import pickle
from collections.abc import Iterable

from pipewine.parsers.base import Parser


class PickleParser[T](Parser[T]):
    """Parser for arbitrary python objects using the `pickle` library."""

    def parse(self, data: bytes) -> T:
        return pickle.loads(data)

    def dump(self, data: T) -> bytes:
        return pickle.dumps(data)

    @classmethod
    def extensions(cls) -> Iterable[str]:
        return ["pkl"]
