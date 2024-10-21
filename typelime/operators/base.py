from collections.abc import Mapping, Sequence

from typing_extensions import Self

from typelime.bundle import Bundle
from typelime.dataset import Dataset
from typelime.sample import Sample


class Setuppable:
    def setup(self) -> None:
        pass

    def teardown(self) -> None:
        pass

    def __enter__(self) -> Self:
        self.setup()
        return self

    def __exit__(self) -> None:
        self.teardown()


AnyDataset = (
    Dataset
    | tuple[Dataset, ...]
    | Sequence[Dataset]
    | Mapping[str, Dataset]
    | Bundle[Dataset]
)

AnySample = (
    Sample
    | tuple[Sample, ...]
    | Sequence[Sample]
    | Mapping[str, Sample]
    | Bundle[Sample]
)
