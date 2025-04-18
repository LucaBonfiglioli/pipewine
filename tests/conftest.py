import curses
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pytest
from pydantic import BaseModel

from pipewine import Dataset, Item, TypedSample, UnderfolderSource


@pytest.fixture
def sample_data() -> Path:
    return Path(__file__).parents[0] / "sample_data"


@dataclass
class UnderfolderFixture:
    folder: Path
    size: int
    type_: type


@dataclass
class ImagesFolderFixture:
    folder: Path
    recursive: bool
    size: int


class LetterMetadata(BaseModel):
    letter: str
    color: str


class LetterSample(TypedSample):
    image: Item[np.ndarray]
    metadata: Item[LetterMetadata]
    shared: Item


@pytest.fixture(
    params=[
        ("underfolder_0", 26, LetterSample),
    ]
)
def underfolder(request, sample_data: Path) -> UnderfolderFixture:
    folder, size, type_ = request.param
    return UnderfolderFixture(
        folder=sample_data / "underfolders" / folder,
        size=size,
        type_=type_,
    )


@pytest.fixture(
    params=[
        ("folder_0", True, 26),
        ("folder_0", False, 26),
        ("folder_1", True, 26),
        ("folder_1/vowels", True, 5),
        ("folder_1/vowels", False, 5),
    ]
)
def images_folder(request, sample_data: Path) -> ImagesFolderFixture:
    folder, recur, size = request.param
    return ImagesFolderFixture(
        folder=sample_data / "images_folders" / folder, recursive=recur, size=size
    )


@pytest.fixture()
def dataset(underfolder: UnderfolderFixture) -> Dataset:
    return UnderfolderSource(folder=underfolder.folder)()


@pytest.fixture()
def letter_dataset(sample_data: Path) -> Dataset[LetterSample]:
    return UnderfolderSource(
        sample_data / "underfolders" / "underfolder_0", sample_type=LetterSample
    )()


def noop(*args, **kwargs):
    pass


@pytest.fixture(autouse=True)
def mock_curses(monkeypatch):
    monkeypatch.setattr(curses, "cbreak", noop)
    monkeypatch.setattr(curses, "nocbreak", noop)
    monkeypatch.setattr(curses, "endwin", noop)


@pytest.fixture(autouse=True)
def clear_static_data():
    from pipewine.grabber import InheritedData

    InheritedData.data.clear()
