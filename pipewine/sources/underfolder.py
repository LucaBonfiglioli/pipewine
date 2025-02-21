"""Source that reads the dataset from file system using Pipewine "Underfolder" format."""

import os
import warnings
from inspect import get_annotations
from itertools import chain
from pathlib import Path

from pipewine._op_typing import origin_type
from pipewine.dataset import Dataset, LazyDataset
from pipewine.item import StoredItem
from pipewine.parsers import ParserRegistry
from pipewine.reader import LocalFileReader
from pipewine.sample import Sample, TypedSample, TypelessSample
from pipewine.sources.base import DatasetSource


class UnderfolderSource[T: Sample](DatasetSource[Dataset[T]]):
    """Source that reads the dataset from file system using Pipewine "Underfolder" format."""

    def __init__(self, folder: Path, sample_type: type[T] | None = None) -> None:
        """
        Args:
            folder (Path): Path to the folder where the dataset is stored.
            sample_type (type[T] | None, optional): Type of the samples to produce.
                Defaults to None (TypelessSample).
        """
        super().__init__()
        self._folder = folder
        self._root_files: dict[str, Path] = {}
        self._root_items: dict[str, StoredItem] = {}
        self._sample_files: list[dict[str, Path]] = []
        self._sample_type = sample_type or TypelessSample

    @classmethod
    def data_path(cls, root_folder: Path) -> Path:
        """The path to the data folder inside the root folder."""
        return root_folder / "data"

    @property
    def sample_type(self) -> type[T] | type[TypelessSample]:
        """Type of the samples produced by this source."""
        return self._sample_type

    @property
    def folder(self) -> Path:
        """Path to the folder where the dataset is stored."""
        return self._folder

    @property
    def data_folder(self) -> Path:
        """Path to the data folder inside the root folder."""
        return self.data_path(self.folder)

    def _extract_key(self, name: str) -> str:
        return name.partition(".")[0]

    def _extract_id_key(self, name: str) -> tuple[int, str] | None:
        id_key_split = name.partition("_")
        if not id_key_split[2]:
            warnings.warn(
                f"{self.__class__}: cannot parse file name '{name}' as <id>_<key>.<ext>"
            )
            return None
        try:
            return (int(id_key_split[0]), self._extract_key(id_key_split[2]))
        except ValueError:
            warnings.warn(
                f"{self.__class__}: file name '{name}' does not start with an integer"
            )
            return None

    def _scan_root_files(self) -> None:
        if not self._folder.exists():
            raise NotADirectoryError(f"Folder '{self._folder}' does not exist.")

        root_items: dict[str, Path] = {}
        with os.scandir(str(self._folder)) as it:
            for entry in it:
                if entry.is_file():
                    key = self._extract_key(entry.name)
                    if key:
                        root_items[key] = Path(entry.path)
        self._root_files = root_items

    def _scan_sample_files(self) -> None:
        data_folder = self.data_folder
        if not data_folder.exists():
            raise NotADirectoryError(f"Folder '{data_folder}' does not exist.")

        sample_files: list[dict[str, Path]] = []
        with os.scandir(str(data_folder)) as it:
            for entry in it:
                if entry.is_file():
                    id_key = self._extract_id_key(entry.name)
                    if id_key:
                        sample_files.extend(
                            ({} for _ in range(id_key[0] - len(sample_files) + 1))
                        )
                        sample_files[id_key[0]][id_key[1]] = Path(entry.path)
        self._sample_files = sample_files

    def _prepare(self) -> None:
        self._scan_root_files()
        self._scan_sample_files()

    def _size(self) -> int:
        return len(self._sample_files)

    def _get_item(self, k: str, v: Path) -> StoredItem | None:
        maybe_root = self._root_items.get(k)
        if maybe_root is not None:
            return maybe_root
        ext = v.suffix[1:]
        parser_type = ParserRegistry.get(ext)
        if parser_type is None:
            warnings.warn(
                f"No parser found for extension '{ext}', make sure the extension "
                "is correct and/or implement a custom Parser for it.",
            )
            return None
        reader = LocalFileReader(v)
        annotated_type = None
        if issubclass(self.sample_type, TypedSample):
            annotation = get_annotations(self.sample_type, eval_str=True).get(k)
            if (
                annotation is not None
                and hasattr(annotation, "__args__")
                and len(annotation.__args__) > 0
            ):
                annotated_type = origin_type(annotation.__args__[0])
        parser = parser_type(type_=annotated_type)
        if k in self._root_files:
            result = StoredItem(reader, parser, shared=True)
            self._root_items[k] = result
        else:
            result = StoredItem(reader, parser, shared=False)
        return result

    def _get_sample(self, idx: int) -> T:
        data = {}
        for k, v in chain(self._sample_files[idx].items(), self._root_files.items()):
            item = self._get_item(k, v)
            if item is not None:
                data[k] = item
        return self.sample_type(**data)  # type: ignore

    def __call__(self) -> Dataset[T]:
        self._prepare()
        return LazyDataset(self._size(), self._get_sample)
