from collections.abc import Mapping, Sequence

from typelime.bundle import Bundle
from typelime.dataset import Dataset
from types import GenericAlias

AnyDataset = (
    Dataset | tuple[Dataset, ...] | list[Dataset] | dict[str, Dataset] | Bundle[Dataset]
)


def origin_type(annotation) -> type:
    if isinstance(annotation, GenericAlias):
        return annotation.__origin__
    else:
        return annotation
