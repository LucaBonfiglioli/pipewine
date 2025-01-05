from pathlib import Path

import pytest

from pipewine import LocalFileReader


class TestLocalFileReader:
    @pytest.mark.parametrize("path", [Path("."), Path("/some/path")])
    def test_path(self, path: Path) -> None:
        fs = LocalFileReader(path)
        assert fs.path == path

    def test_read(self, tmp_path: Path) -> None:
        path = tmp_path / "a_file"
        the_bytes = "some bytes".encode()
        with open(path, "wb") as fp:
            fp.write(the_bytes)

        assert LocalFileReader(path).read() == the_bytes

    def test_read_fail(self) -> None:
        path = Path("/nonexistingpath")
        fs = LocalFileReader(path)
        with pytest.raises(Exception):
            fs.read()
