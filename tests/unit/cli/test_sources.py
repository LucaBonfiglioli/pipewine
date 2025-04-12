from pathlib import Path

import pytest

from pipewine import (
    Grabber,
    ImagesFolderSource,
    Item,
    Sample,
    TypedSample,
    UnderfolderSource,
)
from pipewine.cli.sources import images_folder, underfolder


class MySample(TypedSample):
    alice: Item[dict]
    bob: Item[dict]


@pytest.mark.parametrize("path", ["folder_A", "folder_B"])
@pytest.mark.parametrize("sample_type", [Sample, MySample])
def test_underfolder(path: str, sample_type: type[Sample]) -> None:
    source = underfolder(path, Grabber(), sample_type)
    assert isinstance(source, UnderfolderSource)
    assert source.folder == Path(path)
    assert source.sample_type == sample_type


@pytest.mark.parametrize("path", ["folder_A", "folder_B"])
@pytest.mark.parametrize("sample_type", [Sample, MySample])
@pytest.mark.parametrize("recursive", [True, False])
def test_images_folder(path: str, recursive: bool, sample_type: type[Sample]) -> None:
    text = path
    if recursive:
        text = text + ",recursive"
    source = images_folder(text, Grabber(), sample_type)
    assert isinstance(source, ImagesFolderSource)
    assert source.folder == Path(path)
    assert source.is_recursive == recursive
