from pathlib import Path

import pytest

from pipewine import Dataset, ImagesFolderSource, ImageSample


class TestImagesFolderSource:
    def test_folder(self, images_folder) -> None:
        source: ImagesFolderSource = ImagesFolderSource(images_folder.folder)
        assert source.folder == images_folder.folder

    def test_is_recursive(self, images_folder) -> None:
        source: ImagesFolderSource = ImagesFolderSource(
            images_folder.folder, recursive=images_folder.recursive
        )
        assert source.is_recursive == images_folder.recursive

    def test_call(self, images_folder) -> None:
        source: ImagesFolderSource = ImagesFolderSource(
            images_folder.folder, recursive=images_folder.recursive
        )
        assert isinstance(source(), Dataset)
        assert isinstance(source()[0], ImageSample)

    def test_call_fail_no_directory(self, tmp_path: Path) -> None:
        source: ImagesFolderSource = ImagesFolderSource(tmp_path / "NONEXISTING")
        with pytest.raises(NotADirectoryError):
            source()

    def test_len(self, images_folder) -> None:
        source: ImagesFolderSource = ImagesFolderSource(
            images_folder.folder, recursive=images_folder.recursive
        )
        assert len(source()) == images_folder.size
