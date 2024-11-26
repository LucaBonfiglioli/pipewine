from pathlib import Path

import pytest

from typelime import LocalFileReadStorage


class TestLocalFileReadStorage:
    @pytest.mark.parametrize("path", [Path("."), Path("/some/path")])
    def test_path(self, path: Path) -> None:
        fs = LocalFileReadStorage(path)
        assert fs.path == path

    def test_read(self, tmp_path: Path) -> None:
        path = tmp_path / "a_file"
        the_bytes = "some bytes".encode()
        with open(path, "wb") as fp:
            fp.write(the_bytes)

        assert LocalFileReadStorage(path).read() == the_bytes

    def test_read_fail(self) -> None:
        path = Path("/nonexistingpath")
        fs = LocalFileReadStorage(path)
        with pytest.raises(Exception):
            fs.read()
