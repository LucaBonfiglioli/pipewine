import contextlib
from pipewine import (
    UnderfolderSink,
    ListDataset,
    Dataset,
    TypelessSample,
    Grabber,
    OverwritePolicy,
    MemoryItem,
    NumpyNpyParser,
    JSONParser,
)
from pathlib import Path
import pytest
import numpy as np


@pytest.fixture
def dataset() -> ListDataset:
    return ListDataset(
        [
            TypelessSample(
                image=MemoryItem(np.array([0, 2, 3, 4, 5, 6, 7]), NumpyNpyParser()),
                metadata=MemoryItem(
                    {"alice": 123, "bob": 124, "charlie": 125}, JSONParser()
                ),
                shared_stuff=MemoryItem({"author": "luca"}, JSONParser(), shared=True),
            ),
            TypelessSample(
                image=MemoryItem(
                    10 + np.array([0, 2, 3, 4, 5, 6, 7]), NumpyNpyParser()
                ),
                metadata=MemoryItem(
                    {"alice": 221, "bob": 224, "charlie": 225}, JSONParser()
                ),
                shared_stuff=MemoryItem({"author": "luca"}, JSONParser(), shared=True),
            ),
            TypelessSample(
                image=MemoryItem(
                    20 + np.array([0, 2, 3, 4, 5, 6, 7]), NumpyNpyParser()
                ),
                metadata=MemoryItem(
                    {"alice": 323, "bob": 324, "charlie": 325}, JSONParser()
                ),
                shared_stuff=MemoryItem({"author": "luca"}, JSONParser(), shared=True),
            ),
            TypelessSample(
                image=MemoryItem(
                    30 + np.array([0, 2, 3, 4, 5, 6, 7]), NumpyNpyParser()
                ),
                metadata=MemoryItem(
                    {"alice": 423, "bob": 424, "charlie": 425}, JSONParser()
                ),
                shared_stuff=MemoryItem({"author": "luca"}, JSONParser(), shared=True),
            ),
        ]
    )


class TestUnderfolderSink:
    @pytest.mark.parametrize("grabber", [None, Grabber(4)])
    @pytest.mark.parametrize(
        "overwrite_policy",
        [
            OverwritePolicy.FORBID,
            OverwritePolicy.ALLOW_IF_EMPTY,
            OverwritePolicy.ALLOW_NEW_FILES,
            OverwritePolicy.OVERWRITE_FILES,
            OverwritePolicy.OVERWRITE,
        ],
    )
    def test_call(
        self,
        tmp_path: Path,
        dataset: Dataset,
        grabber: Grabber,
        overwrite_policy: OverwritePolicy,
    ) -> None:
        folder = tmp_path / "folder"
        sink = UnderfolderSink(
            folder, grabber=grabber, overwrite_policy=overwrite_policy
        )
        sink(dataset)
        assert folder.is_dir()
        data_folder = folder / "data"
        assert data_folder.is_dir()
        for i, sample in enumerate(dataset):
            for k, v in sample.items():
                ext = next(iter(v.parser.extensions()))
                if v.is_shared:
                    assert (folder / (k + ext)).is_file()
                else:
                    zfill = len(str(len(dataset) - 1))
                    fmt_idx = str(i).zfill(zfill)
                    assert (data_folder / (fmt_idx + "_" + k + ext)).is_file()

    def test_input_type(self) -> None:
        sink = UnderfolderSink(Path("my_folder"))
        assert issubclass(sink.input_type, Dataset)

    @pytest.mark.parametrize(
        ["overwrite_policy", "fails"],
        [
            [OverwritePolicy.FORBID, True],
            [OverwritePolicy.ALLOW_IF_EMPTY, False],
            [OverwritePolicy.ALLOW_NEW_FILES, False],
            [OverwritePolicy.OVERWRITE_FILES, False],
            [OverwritePolicy.OVERWRITE, False],
        ],
    )
    def test_fail_folder_already_exists(
        self,
        tmp_path: Path,
        dataset: Dataset,
        overwrite_policy: OverwritePolicy,
        fails: bool,
    ) -> None:
        folder = tmp_path / "folder"
        folder.mkdir(parents=True)
        cm = pytest.raises(FileExistsError) if fails else contextlib.nullcontext()
        with cm:
            UnderfolderSink(folder, overwrite_policy=overwrite_policy)(dataset)

    @pytest.mark.parametrize(
        ["overwrite_policy", "fails"],
        [
            [OverwritePolicy.FORBID, True],
            [OverwritePolicy.ALLOW_IF_EMPTY, True],
            [OverwritePolicy.ALLOW_NEW_FILES, False],
            [OverwritePolicy.OVERWRITE_FILES, False],
            [OverwritePolicy.OVERWRITE, False],
        ],
    )
    def test_fail_folder_not_empty(
        self,
        tmp_path: Path,
        dataset: Dataset,
        overwrite_policy: OverwritePolicy,
        fails: bool,
    ) -> None:
        folder = tmp_path / "folder"
        folder.mkdir(parents=True)
        with open(folder / "my_file", "w") as fp:
            fp.write("Hello")
        cm = pytest.raises(FileExistsError) if fails else contextlib.nullcontext()
        with cm:
            UnderfolderSink(folder, overwrite_policy=overwrite_policy)(dataset)

    @pytest.mark.parametrize(
        ["overwrite_policy", "fails"],
        [
            [OverwritePolicy.FORBID, True],
            [OverwritePolicy.ALLOW_IF_EMPTY, True],
            [OverwritePolicy.ALLOW_NEW_FILES, True],
            [OverwritePolicy.OVERWRITE_FILES, False],
            [OverwritePolicy.OVERWRITE, False],
        ],
    )
    def test_fail_folder_root_file_exists(
        self,
        tmp_path: Path,
        dataset: Dataset,
        overwrite_policy: OverwritePolicy,
        fails: bool,
    ) -> None:
        folder = tmp_path / "folder"
        folder.mkdir(parents=True)
        with open(folder / "shared_stuff.json", "w") as fp:
            fp.write("Hello")
        cm = pytest.raises(FileExistsError) if fails else contextlib.nullcontext()
        with cm:
            UnderfolderSink(folder, overwrite_policy=overwrite_policy)(dataset)

    @pytest.mark.parametrize(
        ["overwrite_policy", "fails"],
        [
            [OverwritePolicy.FORBID, True],
            [OverwritePolicy.ALLOW_IF_EMPTY, True],
            [OverwritePolicy.ALLOW_NEW_FILES, True],
            [OverwritePolicy.OVERWRITE_FILES, False],
            [OverwritePolicy.OVERWRITE, False],
        ],
    )
    def test_fail_folder_item_file_exists(
        self,
        tmp_path: Path,
        dataset: Dataset,
        overwrite_policy: OverwritePolicy,
        fails: bool,
    ) -> None:
        folder = tmp_path / "folder"
        (folder / "data").mkdir(parents=True)
        with open(folder / "data" / "2_image.npy", "w") as fp:
            fp.write("World")
        cm = pytest.raises(FileExistsError) if fails else contextlib.nullcontext()
        with cm:
            UnderfolderSink(folder, overwrite_policy=overwrite_policy)(dataset)

    def test_empty_dataset(self, tmp_path: Path) -> None:
        folder = tmp_path / "folder"
        UnderfolderSink(folder)(ListDataset([]))
        assert not folder.is_dir()
