from collections.abc import Mapping, Sequence
from types import GenericAlias
from typing import Any, TypeVar

from pipewine.bundle import Bundle
from pipewine.dataset import Dataset
from pipewine.sample import Sample, TypelessSample

AnyDataset = (
    Dataset
    | tuple[Dataset, ...]
    | Sequence[Dataset]
    | Mapping[str, Dataset]
    | Bundle[Dataset]
)


def origin_type(annotation: Any) -> type:
    if isinstance(annotation, TypeVar):
        return origin_type(annotation.__bound__)
    elif isinstance(annotation, GenericAlias):
        type_ = annotation.__origin__
    else:
        type_ = annotation
    if not isinstance(type_, type):
        raise ValueError(
            "`origin_type` can only be called on annotations whose origin is a "
            f"concrete type known at dev time and instance of `type`, got '{type_}' "
            f"of type '{type(type_)}'."
        )
    return type_
