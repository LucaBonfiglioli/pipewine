from pathlib import Path

import pytest

from pipewine import (
    UnderfolderSource,
    TypelessSample,
    Dataset,
    Sample,
    StoredItem,
    LocalFileReader,
)
import shutil


@pytest.fixture
def clone_uf(underfolder, tmp_path: Path):
    clone_folder = tmp_path / "uf"
    shutil.copytree(underfolder.folder, clone_folder)
    underfolder.folder = clone_folder
    return underfolder


class TestUnderfolderSource:
    def test_folder(self, underfolder) -> None:
        source: UnderfolderSource = UnderfolderSource(underfolder.folder)
        assert source.folder == underfolder.folder

    def test_data_folder(self, underfolder) -> None:
        source: UnderfolderSource = UnderfolderSource(underfolder.folder)
        assert source.data_folder == underfolder.folder / "data"

    @pytest.mark.parametrize("pass_type", [True, False])
    def test_sample_type(self, underfolder, pass_type: bool) -> None:
        type_ = underfolder.type_ if pass_type else None
        source: UnderfolderSource = UnderfolderSource(
            underfolder.folder, sample_type=type_
        )
        expected_type = type_ or TypelessSample
        assert source.sample_type == expected_type

    def test_output_type(self, underfolder) -> None:
        source: UnderfolderSource = UnderfolderSource(underfolder.folder)
        assert issubclass(source.output_type, Dataset)

    def test_call(self, underfolder) -> None:
        source: UnderfolderSource = UnderfolderSource(underfolder.folder)
        assert isinstance(source(), Dataset)

    def test_call_fail_no_directory(self, tmp_path: Path) -> None:
        source: UnderfolderSource = UnderfolderSource(tmp_path / "NONEXISTING")
        with pytest.raises(NotADirectoryError):
            source()

    def test_call_fail_no_data_directory(self, clone_uf) -> None:
        shutil.rmtree(UnderfolderSource.data_path(clone_uf.folder))
        source: UnderfolderSource = UnderfolderSource(clone_uf.folder)
        with pytest.raises(NotADirectoryError):
            source()

    @pytest.mark.parametrize(
        "subdirectory", ["a_directory", "image.png", ".stuff", "0123_image.png"]
    )
    def test_call_with_subdirectories(self, clone_uf, subdirectory: str) -> None:
        Path(clone_uf.folder / subdirectory).mkdir(parents=True, exist_ok=True)
        source: UnderfolderSource = UnderfolderSource(
            clone_uf.folder, sample_type=clone_uf.type_
        )
        assert isinstance(source(), Dataset)

    @pytest.mark.parametrize(
        "subdirectory", ["a_directory", "image.png", ".stuff", "0123_image.png"]
    )
    def test_call_with_subdirectories_in_data(
        self, clone_uf, subdirectory: str
    ) -> None:
        (UnderfolderSource.data_path(clone_uf.folder) / subdirectory).mkdir(
            parents=True, exist_ok=True
        )
        source: UnderfolderSource = UnderfolderSource(
            clone_uf.folder, sample_type=clone_uf.type_
        )
        assert isinstance(source(), Dataset)

    @pytest.mark.parametrize(
        "file", [".txt", "image.png", "stuff", "_image.png", "______"]
    )
    def test_call_with_wrong_format_in_data(self, clone_uf, file: str) -> None:
        with open(UnderfolderSource.data_path(clone_uf.folder) / file, "w") as fp:
            fp.write("hello")
        source: UnderfolderSource = UnderfolderSource(
            clone_uf.folder, sample_type=clone_uf.type_
        )
        with pytest.warns():
            assert isinstance(source(), Dataset)

    @pytest.mark.parametrize("extra_file_name", ["somefile", ".stuff", "_some_file"])
    def test_call_with_extra_root_item(self, clone_uf, extra_file_name: str) -> None:
        with open(clone_uf.folder / extra_file_name, "w") as fp:
            fp.write("hello")

        source: UnderfolderSource = UnderfolderSource(
            clone_uf.folder, sample_type=clone_uf.type_
        )
        assert isinstance(source(), Dataset)

    def test_len(self, underfolder) -> None:
        source: UnderfolderSource = UnderfolderSource(underfolder.folder)
        assert len(source()) == underfolder.size

    @pytest.mark.parametrize("pass_type", [True, False])
    def test_get_sample(self, underfolder, pass_type: bool) -> None:
        sample_type = underfolder.type_ if pass_type else None
        source: UnderfolderSource[Sample] = UnderfolderSource(
            underfolder.folder, sample_type=sample_type
        )
        expected_type = underfolder.type_ if pass_type else TypelessSample
        dataset = source()
        for i, sample in enumerate(dataset):
            assert isinstance(sample, expected_type)
            for k, v in sample.items():
                assert isinstance(v, StoredItem)
                assert isinstance(v.reader, LocalFileReader)
                fname = v.reader.path.stem
                if v.is_shared:
                    assert fname == k
                else:
                    f_idx, _, f_key = fname.partition("_")
                    assert int(f_idx) == i
                    assert f_key == k

    @pytest.mark.parametrize("pass_type", [True, False])
    def test_get_sample_unknown_extension(self, clone_uf, pass_type: bool) -> None:
        fpath = UnderfolderSource.data_path(clone_uf.folder) / "00000_unknown.unknown"
        with open(fpath, "w") as fp:
            fp.write("hello")
        sample_type = clone_uf.type_ if pass_type else None
        source: UnderfolderSource[Sample] = UnderfolderSource(
            clone_uf.folder, sample_type=sample_type
        )
        expected_type = clone_uf.type_ if pass_type else TypelessSample
        dataset = source()
        with pytest.warns():
            for i, sample in enumerate(dataset):
                assert isinstance(sample, expected_type)
