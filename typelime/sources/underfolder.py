import os
import warnings
from pathlib import Path

from typelime._op_typing import _RetrieveGeneric
from typelime.item import StoredItem
from typelime.parsers import ParserRegistry
from typelime.sample import Sample, TypelessSample
from typelime.sources.base import LazyDatasetSource
from typelime.storage import LocalFileStorage


class UnderfolderSource[T: Sample](LazyDatasetSource[T], _RetrieveGeneric):
    def __init__(self, folder: Path) -> None:
        self._folder = folder
        self._root_items: dict[str, Path] = {}
        self._samples: list[dict[str, Path]] = []

    @classmethod
    def data_path(cls, root_folder: Path) -> Path:
        return root_folder / "data"

    @property
    def sample_type(self) -> type[T]:
        return self._genargs[0]

    @property
    def folder(self) -> Path:
        return self._folder

    @property
    def data_folder(self) -> Path:
        return self.data_path(self.folder)

    def _extract_key(self, name: str) -> str:
        return name.partition(".")[0]

    def _extract_id_key(self, name: str) -> tuple[int, str] | None:
        id_key_split = name.partition("_")
        if not id_key_split[2]:
            warnings.warn(
                f"{self.__class__}: cannot parse file name {name} as <id>_<key>.<ext>"
            )
            return None
        try:
            return (int(id_key_split[0]), self._extract_key(id_key_split[2]))
        except ValueError:
            warnings.warn(
                f"{self.__class__}: file name `{name}` does not start with an integer"
            )
            return None

    def _scan_root_files(self):
        if not self._folder.exists():
            raise RuntimeError(f"Folder {self._folder} does not exist.")

        root_items: dict[str, Path] = {}
        with os.scandir(str(self._folder)) as it:
            for entry in it:
                if entry.is_file():
                    key = self._extract_key(entry.name)
                    if key:
                        root_items[key] = Path(entry.path)
        self._root_items = root_items

    def _scan_sample_files(self):
        data_folder = self.data_folder
        if not data_folder.exists():
            raise NotADirectoryError(f"Folder {data_folder} does not exist.")

        samples: list[dict[str, Path]] = []
        with os.scandir(str(data_folder)) as it:
            for entry in it:
                if entry.is_file():
                    id_key = self._extract_id_key(entry.name)
                    if id_key:
                        samples.extend(
                            ({} for _ in range(id_key[0] - len(samples) + 1))
                        )
                        samples[id_key[0]][id_key[1]] = Path(entry.path)
        self._samples = samples

    def _prepare(self) -> None:
        self._scan_root_files()
        self._scan_sample_files()

    def _size(self) -> int:
        return len(self._samples)

    def _get_sample(self, idx: int) -> T:
        data = {}
        for k, v in self._samples[idx].items():
            ext = v.suffix
            parser_type = ParserRegistry.get(ext)
            if parser_type is None:
                warnings.warn(
                    f"No parser found for extension {ext}, make sure the extension "
                    "is correct and/or implement a custom Parser for it.",
                )
                continue
            storage = LocalFileStorage(v)
            annotated_type = None
            if self.sample_type is not None:
                annotation = self.sample_type.__annotations__.get(k)
                if annotation is not None and len(annotation.__args__) > 0:
                    annotated_type = annotation.__args__[0]
            parser = parser_type(type_=annotated_type)
            data[k] = StoredItem(storage, parser)
        if self.sample_type is None:
            return TypelessSample(data)  # type: ignore
        else:
            return self.sample_type(**data)
