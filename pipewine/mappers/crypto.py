"""Mappers for cryptographic operations."""

import hashlib
import pickle
from collections.abc import Sequence, Iterable
from typing import Any

from pipewine.item import Item, MemoryItem
from pipewine.mappers.base import Mapper
from pipewine.parsers import YAMLParser
from pipewine.sample import Sample, TypedSample


class HashedSample(TypedSample):
    """Sample type to represent the hash of a sample."""

    hash: Item[str]
    """The hash of the sample."""


class HashMapper(Mapper[Sample, HashedSample]):
    """Compute the hash of a sample based on the selected items."""

    def __init__(
        self, algorithm: str = "sha256", keys: str | Sequence[str] | None = None
    ) -> None:
        """
        Args:
            algorithm (str, optional): Hash algorithm to use, must be one of the
                algorithms available in `hashlib.algorithms_available`. Defaults to
                "sha256".
            keys (str | Sequence[str] | None, optional): Keys of the sample to use for
                computing the hash. If a string is provided, it is treated as a single
                key. If a sequence is provided, it is treated as a list of keys. If
                `None` is provided, all keys in the sample are used. Defaults to `None`.

        Raises:
            ValueError: If the provided algorithm is not available in `hashlib` or if it
                requires parameters. Currently, the only two algorithms that require
                parameters (and thus are not supported) are "shake_128" and "shake_256".
        """
        super().__init__()
        algorithms_with_parameters = ["shake_128", "shake_256"]
        if (
            algorithm not in hashlib.algorithms_available
            or algorithm in algorithms_with_parameters
        ):
            raise ValueError(f"Invalid algorithm: {algorithm}")
        self._algorithm = algorithm
        self._keys = keys

    def __call__(self, idx: int, x: Sample) -> HashedSample:
        hash_ = self._compute_sample_hash(x)
        return HashedSample(hash=MemoryItem(hash_, YAMLParser(type_=str)))

    def _compute_item_hash(self, data: Any) -> str:
        return hashlib.new(self._algorithm, pickle.dumps(data)).hexdigest()

    def _compute_sample_hash(self, sample: Sample) -> str:
        keys: Iterable[str]
        if isinstance(self._keys, str):
            keys = [self._keys]
        elif isinstance(self._keys, Sequence):
            keys = self._keys
        else:
            keys = sorted(list(sample.keys()))
        return "".join([self._compute_item_hash(sample[k]()) for k in keys])  # type: ignore
