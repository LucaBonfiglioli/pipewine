from collections.abc import Callable
from typing import Any

import pytest

from pipewine import MemoryItem, PickleParser, TypelessSample


def make_sample(data: dict[str, Any]) -> TypelessSample:
    return TypelessSample(**{k: MemoryItem(v, PickleParser()) for k, v in data.items()})


@pytest.fixture
def make_sample_fn() -> Callable[[dict[str, Any]], TypelessSample]:
    return make_sample
