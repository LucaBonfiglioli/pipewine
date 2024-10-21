from collections.abc import Mapping, Sequence

from typelime.bundle import Bundle
from typelime.dataset import Dataset
from typelime.sample import Sample


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
