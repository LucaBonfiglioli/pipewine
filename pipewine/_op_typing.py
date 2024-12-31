from collections.abc import Mapping, Sequence

from pipewine.bundle import Bundle
from pipewine.dataset import Dataset
from types import GenericAlias

AnyDataset = (
    Dataset
    | tuple[Dataset, ...]
    | Sequence[Dataset]
    | Mapping[str, Dataset]
    | Bundle[Dataset]
)


def origin_type(annotation) -> type:
    if isinstance(annotation, GenericAlias):
        return annotation.__origin__
    else:
        return annotation
