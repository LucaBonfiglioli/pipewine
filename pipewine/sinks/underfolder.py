import os
import shutil
from enum import Enum
from pathlib import Path

from pipewine.dataset import Dataset
from pipewine.grabber import Grabber
from pipewine.mappers import Mapper
from pipewine.operators import MapOp
from pipewine.sample import Sample
from pipewine.sinks.base import DatasetSink
from pipewine.sinks.fs_utils import CopyPolicy, write_item_to_file


class OverwritePolicy(str, Enum):
    """How to handle cases where pipewine needs to overwrite existing data."""

    FORBID = "FORBID"
    """Most strict policy: always fail in case there is something saved in the 
    destination path, even an empty folder. Ensures no data loss, but may crash the 
    program unwantedly.
    """

    ALLOW_IF_EMPTY = "ALLOW_IF_EMPTY"
    """Allow the overwrite only in case the destination path is an empty folder."""

    ALLOW_NEW_FILES = "ALLOW_NEW_FILES"
    """Only allow the creation of new files without deleting/modifying existing ones.
    Prevents data loss at the individual file level, but may render the dataset
    unreadable or change its format.
    """

    OVERWRITE_FILES = "OVERWRITE_FILES"
    """Delete only conflicting files before writing. This may result in data loss and
    make the dataset unreadable or change its format. Use at your own risk.
    """

    OVERWRITE = "OVERWRITE"
    """Weakest policy: completely delete and rewrite the folder. This will result in
    major data loss but ensures that the final dataset is readable and with the expected
    format. Use at your own risk.
    """


class _WriterMapper[T: Sample](Mapper[T, T]):
    def __init__(
        self,
        folder: Path,
        data_folder: Path,
        zfill: int,
        exclude: set[str],
        overwrite_policy: OverwritePolicy,
        copy_policy: CopyPolicy,
    ) -> None:
        super().__init__()
        self._folder = folder
        self._data_folder = data_folder
        self._zfill = zfill
        self._exclude = exclude
        self._overwrite_policy = overwrite_policy
        self._copy_policy = copy_policy

    def __call__(self, idx: int, x: T) -> T:
        prefix = str(idx).zfill(self._zfill)
        fname_fmt = "{prefix}_{key}.{ext}"
        for k, item in x.items():
            if k in self._exclude:
                continue
            ext = next(iter(item.parser.extensions()))
            fname = fname_fmt.format(prefix=prefix, key=k, ext=ext)
            fpath = self._data_folder / fname
            if fpath.is_file():
                if self._overwrite_policy != OverwritePolicy.OVERWRITE_FILES:
                    raise FileExistsError(
                        f"File {fpath} already exists and policy "
                        f"{self._overwrite_policy} is used. Either change the destination "
                        "path or set a weaker policy."
                    )
                else:
                    fpath.unlink()

            write_item_to_file(item, fpath, self._copy_policy)

        return x


class UnderfolderSink(DatasetSink[Dataset]):
    def __init__(
        self,
        folder: Path,
        grabber: Grabber | None = None,
        overwrite_policy: OverwritePolicy = OverwritePolicy.FORBID,
        copy_policy: CopyPolicy = CopyPolicy.HARD_LINK,
    ) -> None:
        super().__init__()
        self._folder = folder
        self._grabber = grabber or Grabber()
        self._overwrite_policy = overwrite_policy
        self._copy_policy = copy_policy

    def __call__[T: Sample](self, data: Dataset[T]) -> None:
        if len(data) == 0:
            return
        if self._folder.exists():
            if self._overwrite_policy == OverwritePolicy.FORBID:
                raise FileExistsError(
                    f"Folder {self._folder} already exists and policy "
                    f"{self._overwrite_policy} is used. Either change the destination "
                    "path or set a weaker policy."
                )

            elif self._overwrite_policy == OverwritePolicy.OVERWRITE:
                shutil.rmtree(self._folder, ignore_errors=True)

        self._folder.mkdir(parents=True, exist_ok=True)

        with os.scandir(self._folder) as it:
            if any(it) and self._overwrite_policy == OverwritePolicy.ALLOW_IF_EMPTY:
                raise FileExistsError(
                    f"Folder {self._folder} is not empty and policy "
                    f"{self._overwrite_policy} is used. Either change the destination"
                    "path or set a weaker policy."
                )

        inner_folder = self._folder / "data"
        inner_folder.mkdir(parents=True, exist_ok=True)
        best_zfill = len(str(len(data) - 1))

        root_items: set[str] = set()
        data0 = data[0]
        for k, item in data0.items():
            if item.is_shared:
                root_items.add(k)
                ext = next(iter(item.parser.extensions()))
                fpath = self._folder / f"{k}.{ext}"
                if fpath.is_file():
                    if self._overwrite_policy != OverwritePolicy.OVERWRITE_FILES:
                        raise FileExistsError(
                            f"File {fpath} already exists and policy "
                            f"{self._overwrite_policy} is used. Either change the "
                            "destination path or set a weaker policy."
                        )
                    else:
                        fpath.unlink()
                write_item_to_file(item, fpath, self._copy_policy)

        writer: _WriterMapper[T] = _WriterMapper(
            self._folder,
            inner_folder,
            best_zfill,
            root_items,
            self._overwrite_policy,
            self._copy_policy,
        )
        data = MapOp(writer)(data)

        for _ in self.loop(data, self._grabber, name="Writing"):
            pass