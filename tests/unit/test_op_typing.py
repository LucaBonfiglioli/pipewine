from typelime._op_typing import origin_type
import pytest


class Container:
    a: int
    b: float
    c: list[int]
    d: list[dict[str, tuple]]


@pytest.mark.parametrize(
    ["annotation", "expected"],
    [
        [Container.__annotations__["a"], int],
        [Container.__annotations__["b"], float],
        [Container.__annotations__["c"], list],
        [Container.__annotations__["d"], list],
    ],
)
def test_origin_type(annotation, expected) -> None:
    assert origin_type(annotation) == expected
