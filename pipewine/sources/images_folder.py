"""Source that reads the dataset from a folder of images."""

from pathlib import Path

import numpy as np

from pipewine.dataset import Dataset, LazyDataset
from pipewine.item import Item, StoredItem
from pipewine.parsers import ParserRegistry
from pipewine.reader import LocalFileReader
from pipewine.sample import TypedSample
from pipewine.sources.base import DatasetSource


class ImageSample(TypedSample):
    """Sample that contains an image and its label."""

    image: Item[np.ndarray]
    """The image data."""


_image_extensions = {".bmp", ".png", ".jpeg", ".jpg", ".jfif", ".jpe", ".tiff", ".tif"}


class ImagesFolderSource(DatasetSource[Dataset[ImageSample]]):
    """Source that reads the dataset from a folder of images."""

    def __init__(self, folder: Path, recursive: bool = False) -> None:
        """
        Args:
            folder (Path): Path to the folder where the dataset is stored.
            recursive (bool, optional): Whether to search for images recursively in the
                folder. Defaults to False.
        """
        super().__init__()
        self._folder = folder
        self._recursive = recursive
        self._found_files = []

    @property
    def folder(self) -> Path:
        """Path to the folder where the dataset is stored."""
        return self._folder

    @property
    def is_recursive(self) -> bool:
        """Whether to search for images recursively in the folder."""
        return self._recursive

    def _prepare(self) -> None:
        if not self._folder.exists() or not self._folder.is_dir():
            raise NotADirectoryError(self._folder)
        if self._recursive:
            _files_unsorted = list(self._folder.rglob("*"))
        else:
            _files_unsorted = list(self._folder.iterdir())

        _files_unsorted = [f for f in _files_unsorted if f.suffix in _image_extensions]
        self._found_files = sorted(_files_unsorted)

    def _get_sample(self, idx: int) -> ImageSample:
        file = self._found_files[idx]
        ptype = ParserRegistry.get(file.suffix[1:])
        assert ptype is not None, f"No parser found for file {file}"
        image = StoredItem(LocalFileReader(file), ptype(), shared=False)
        return ImageSample(image=image)

    def __call__(self) -> Dataset[ImageSample]:
        self._prepare()
        return LazyDataset(len(self._found_files), self._get_sample)
