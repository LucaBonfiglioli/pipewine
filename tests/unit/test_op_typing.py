from inspect import get_annotations

import pytest

from pipewine._op_typing import origin_type


class Container[T: str]:
    a: int
    b: float
    c: list[int]
    d: list[dict[str, tuple]]
    e: T
    f: "Container"


@pytest.mark.parametrize(
    ["annotation", "expected"],
    [
        [get_annotations(Container)["a"], int],
        [get_annotations(Container)["b"], float],
        [get_annotations(Container)["c"], list],
        [get_annotations(Container)["d"], list],
        [get_annotations(Container)["e"], str],
        [get_annotations(Container, eval_str=True)["f"], Container],
    ],
)
def test_origin_type(annotation, expected) -> None:
    assert origin_type(annotation) == expected
