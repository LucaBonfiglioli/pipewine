"""Private module for typing utilities used by the `pipewine` package."""

from collections import defaultdict
from collections.abc import Mapping, Sequence
from inspect import getmro
from types import GenericAlias, NoneType, UnionType
from typing import Any, Optional, TypeVar, Union, get_origin

from pipewine.bundle import Bundle
from pipewine.dataset import Dataset
from pipewine.sample import Sample, TypedSample, TypelessSample

AnyDataset = (
    Dataset
    | tuple[Dataset, ...]
    | Sequence[Dataset]
    | Mapping[str, Dataset]
    | Bundle[Dataset]
)
"""Type alias for any dataset type, which can be:

- A single dataset.
- A tuple of datasets.
- A sequence of datasets.
- A mapping of dataset names to datasets.
- A bundle of datasets.
"""


def _mro_lca(*cls_list: type) -> type:  # pragma: no cover
    mros = [list(getmro(cls)) for cls in cls_list]
    track = defaultdict(int)
    while mros:
        for mro in mros:
            cur = mro.pop(0)
            track[cur] += 1
            if track[cur] == len(cls_list):
                return cur
            if len(mro) == 0:
                mros.remove(mro)

    raise ValueError(f"Could not determine LCA for types {cls_list}.")


def origin_type(annotation: Any) -> type:
    if annotation is None:
        return type(None)
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


def get_sample_type_from_sample_annotation(sample_annotation: Any) -> type[Sample]:
    if sample_annotation is None:
        return TypelessSample
    if isinstance(sample_annotation, type) and issubclass(sample_annotation, Sample):
        return (
            sample_annotation
            if issubclass(sample_annotation, TypedSample)
            and sample_annotation is not TypedSample
            else TypelessSample
        )
    if isinstance(sample_annotation, TypeVar):
        return get_sample_type_from_sample_annotation(sample_annotation.__bound__)
    origin = get_origin(sample_annotation)
    if origin in [Optional, Union, UnionType]:
        types = [
            get_sample_type_from_sample_annotation(x)
            for x in sample_annotation.__args__
            if x is not NoneType
        ]
        return get_sample_type_from_sample_annotation(_mro_lca(*types))
    raise ValueError(f"Sample annotation '{sample_annotation}' is not supported.")


def get_sample_type_from_dataset_annotation(dataset_annotation: Any) -> type[Sample]:
    if dataset_annotation is None or (
        isinstance(dataset_annotation, type) and issubclass(dataset_annotation, Dataset)
    ):
        return TypelessSample
    if isinstance(dataset_annotation, TypeVar):
        return get_sample_type_from_dataset_annotation(dataset_annotation.__bound__)
    origin = get_origin(dataset_annotation)
    if origin in [Optional, Union, UnionType]:
        types = [
            get_sample_type_from_dataset_annotation(x)
            for x in dataset_annotation.__args__
            if x is not NoneType
        ]
        return _mro_lca(*types)
    if isinstance(dataset_annotation, GenericAlias):
        sample_annotation = dataset_annotation.__args__[0]
        return get_sample_type_from_sample_annotation(sample_annotation)
    raise ValueError(f"Dataset annotation '{dataset_annotation}' is not supported.")
