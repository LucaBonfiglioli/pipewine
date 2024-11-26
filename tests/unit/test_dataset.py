import contextlib
import random
import string
from typing import Any

import numpy as np
import pytest
from pydantic import BaseModel

from typelime import (
    Dataset,
    Item,
    JSONParser,
    LazyDataset,
    ListDataset,
    MemoryItem,
    NumpyNpyParser,
    Sample,
    TypedSample,
)


class MyMetadata(BaseModel):
    name: str
    age: int
    email: str


class MySample(TypedSample):
    image: Item[np.ndarray]
    metadata: Item[MyMetadata]

    @classmethod
    def generate(cls) -> "MySample":
        name = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
        return MySample(
            image=MemoryItem(np.random.rand(10, 10, 3), NumpyNpyParser()),
            metadata=MemoryItem(
                MyMetadata(name=name, age=20, email=f"{name}@example.com"),
                JSONParser(),
            ),
        )


class TestDataset:
    def _test_size(self, dataset: Dataset, size: int) -> None:
        assert len(dataset) == size

    def _test_getitem_single(
        self, dataset: Dataset, idx: int, sample: Sample | None
    ) -> None:
        cm: Any = contextlib.nullcontext()
        if sample is None:
            cm = pytest.raises(IndexError)
        with cm:
            assert dataset[idx] == sample

    def _test_getitem_slice(
        self, dataset: Dataset, slice_: slice, exp: list[Sample]
    ) -> None:
        sliced = dataset[slice_]
        assert len(sliced) == len(exp)
        for i, (x, y) in enumerate(zip(sliced, exp)):
            assert x == y, i


class TestLazyDataset(TestDataset):
    @pytest.mark.parametrize(
        ["dataset", "size"],
        [
            [LazyDataset(0, lambda x: MySample.generate()), 0],
            [LazyDataset(10, lambda x: MySample.generate()), 10],
        ],
    )
    def test_size(self, dataset: Dataset, size: int) -> None:
        self._test_size(dataset, size)

    @pytest.mark.parametrize(
        "samples",
        [
            [],
            [MySample.generate()],
            [MySample.generate() for _ in range(4)],
            [MySample.generate() for _ in range(10)],
        ],
    )
    @pytest.mark.parametrize("idx", list(range(15)))
    def test_getitem_single(self, samples: list[Sample], idx: int) -> None:
        dataset = LazyDataset(len(samples), lambda x: samples[x])
        sample = samples[idx] if idx < len(samples) else None
        return self._test_getitem_single(dataset, idx, sample)

    @pytest.mark.parametrize(
        "samples",
        [
            [],
            [MySample.generate()],
            [MySample.generate() for _ in range(4)],
            [MySample.generate() for _ in range(10)],
        ],
    )
    @pytest.mark.parametrize(
        "slice_",
        [
            slice(0, 0, 1),
            slice(1, 0, 1),
            slice(1, 5, 1),
            slice(12, 213, 2),
            slice(0, 20, 4),
            slice(0, 20, -2),
        ],
    )
    def test_getitem_slice(self, samples: list[Sample], slice_: slice) -> None:
        dataset = LazyDataset(len(samples), lambda x: samples[x])
        samples_ = samples[slice_]
        return self._test_getitem_slice(dataset, slice_, samples_)


class TestListDataset(TestDataset):
    @pytest.mark.parametrize(
        ["dataset", "size"],
        [
            [ListDataset([]), 0],
            [ListDataset([MySample.generate() for _ in range(10)]), 10],
        ],
    )
    def test_size(self, dataset: Dataset, size: int) -> None:
        self._test_size(dataset, size)

    @pytest.mark.parametrize(
        "samples",
        [
            [],
            [MySample.generate()],
            [MySample.generate() for _ in range(4)],
            [MySample.generate() for _ in range(10)],
        ],
    )
    @pytest.mark.parametrize("idx", list(range(15)))
    def test_getitem_single(self, samples: list[Sample], idx: int) -> None:
        dataset = ListDataset(samples)
        sample = samples[idx] if idx < len(samples) else None
        return self._test_getitem_single(dataset, idx, sample)

    @pytest.mark.parametrize(
        "samples",
        [
            [],
            [MySample.generate()],
            [MySample.generate() for _ in range(4)],
            [MySample.generate() for _ in range(10)],
        ],
    )
    @pytest.mark.parametrize(
        "slice_",
        [
            slice(0, 0, 1),
            slice(1, 0, 1),
            slice(1, 5, 1),
            slice(12, 213, 2),
            slice(0, 20, 4),
            slice(0, 20, -2),
        ],
    )
    def test_getitem_slice(self, samples: list[Sample], slice_: slice) -> None:
        dataset = ListDataset(samples)
        samples = samples[slice_]
        return self._test_getitem_slice(dataset, slice_, samples)
