from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pytest
from pydantic import BaseModel

from pipewine import Item, TypedSample, Dataset, UnderfolderSource


@pytest.fixture
def sample_data() -> Path:
    return Path(__file__).parents[0] / "sample_data"


@dataclass
class UnderfolderFixture:
    folder: Path
    size: int
    type_: type


class Sample0Metadata(BaseModel):
    letter: str
    color: str


class Sample0(TypedSample):
    image: Item[np.ndarray]
    metadata: Item[Sample0Metadata]
    shared: Item


@pytest.fixture(
    params=[
        ("underfolder_0", 26, Sample0),
    ]
)
def underfolder(request, sample_data: Path) -> UnderfolderFixture:
    folder, size, type_ = request.param
    return UnderfolderFixture(
        folder=sample_data / "underfolders" / folder,
        size=size,
        type_=type_,
    )


@pytest.fixture()
def dataset(underfolder: UnderfolderFixture) -> Dataset:
    return UnderfolderSource(folder=underfolder.folder)()
