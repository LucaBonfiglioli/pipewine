from contextlib import nullcontext
from typing import Optional, TypeVar, Union
import pytest

from pipewine._op_typing import (
    origin_type,
    get_sample_type_from_dataset_annotation,
    get_sample_type_from_sample_annotation,
)
from pipewine.dataset import Dataset, LazyDataset, ListDataset
from pipewine.item import Item
from pipewine.sample import Sample, TypedSample, TypelessSample


T = TypeVar("T", bound=str)


@pytest.mark.parametrize(
    ["annotation", "expected"],
    [
        [int, int],
        [float, float],
        [list[str], list],
        [list[dict[str, tuple]], list],
        [T, str],
        [None, type(None)],
        [10, "fail"],
    ],
)
def test_origin_type(annotation, expected) -> None:
    cm = pytest.raises(ValueError) if expected == "fail" else nullcontext()
    with cm:
        assert origin_type(annotation) == expected


class MySampleA(TypedSample):
    a: Item[dict]
    b: Item[dict]
    c: Item[dict]


class MySampleB(TypedSample):
    a: Item[dict]


class MySampleC(MySampleB):
    b: Item[dict]


class MySampleD(MySampleB):
    c: Item[list]


S1 = TypeVar("S1", bound=MySampleC)
S2 = TypeVar("S2", bound=MySampleD)


@pytest.mark.parametrize(
    ["annotation", "expected"],
    [
        [None, TypelessSample],
        [Sample, TypelessSample],
        [TypedSample, TypelessSample],  # It only makes sense to use specific sub-types!
        [MySampleA, MySampleA],
        [Optional[MySampleA], MySampleA],
        [MySampleD | None, MySampleD],
        [MySampleD | MySampleC, MySampleB],
        [Union[MySampleC, MySampleD], MySampleB],
        [Union[MySampleA, MySampleD], TypelessSample],
        [S1, MySampleC],
        [Optional[S1], MySampleC],  # type: ignore
        [Optional[S2] | MySampleC, MySampleB],  # type: ignore
        [Optional[S1 | S2], MySampleB],  # type: ignore
        [10, "fail"],
        [int, "fail"],
        [list[S1], "fail"],  # type: ignore
        [MySampleA | list, "fail"],
    ],
)
def test_get_sample_type_from_sample_annotation(annotation, expected) -> None:
    cm = pytest.raises(ValueError) if expected == "fail" else nullcontext()
    with cm:
        assert get_sample_type_from_sample_annotation(annotation) == expected


D1 = TypeVar("D1", bound=Dataset[MySampleC])
D2 = TypeVar("D2", bound=LazyDataset[MySampleD])


@pytest.mark.parametrize(
    ["annotation", "expected"],
    [
        [None, TypelessSample],
        [Dataset, TypelessSample],
        [LazyDataset, TypelessSample],
        [ListDataset, TypelessSample],
        [D1, MySampleC],
        [D2, MySampleD],
        [Dataset[MySampleA | MySampleB], TypelessSample],
        [Dataset[MySampleD | MySampleC], MySampleB],
        [Dataset[MySampleD | MySampleC] | Dataset[MySampleB], MySampleB],
        [Optional[Dataset[MySampleB]], MySampleB],
        [Optional[Dataset[MySampleC] | D2], MySampleB],  # type: ignore
        [10, "fail"],
        [int, "fail"],
        [MySampleB, "fail"],
        [Dataset[MySampleA] | int, "fail"],
    ],
)
def test_get_sample_type_from_dataset_annotation(annotation, expected) -> None:
    cm = pytest.raises(ValueError) if expected == "fail" else nullcontext()
    with cm:
        assert get_sample_type_from_dataset_annotation(annotation) == expected
