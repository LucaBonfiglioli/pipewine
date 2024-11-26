import numpy as np
import pytest

from typelime import BmpParser, ImageParser, JpegParser, PngParser, TiffParser


class TestImageParser:
    def _test_parse(
        self, array: np.ndarray, parser: ImageParser, lossy: bool = False
    ) -> None:
        bytes_ = parser.dump(array)
        assert isinstance(bytes_, bytes)
        re_array = parser.parse(bytes_)
        assert array.dtype == re_array.dtype
        assert array.shape == re_array.shape
        if not lossy:
            assert np.allclose(array, re_array)


class TestBmpParser(TestImageParser):
    @pytest.mark.parametrize(
        "image",
        [
            np.arange(50 * 70, dtype=np.uint8).reshape(50, 70),
            np.arange(100 * 30, dtype=np.uint8).reshape(100, 30),
            np.arange(50 * 70 * 3, dtype=np.uint8).reshape(50, 70, 3),
        ],
    )
    def test_parse(self, image: np.ndarray) -> None:
        self._test_parse(image, BmpParser())


class TestPngParser(TestImageParser):
    @pytest.mark.parametrize(
        "image",
        [
            np.arange(50 * 70, dtype=np.uint8).reshape(50, 70),
            np.arange(100 * 30, dtype=np.uint8).reshape(100, 30),
            np.arange(50 * 70, dtype=np.uint16).reshape(50, 70),
            np.arange(100 * 30, dtype=np.uint16).reshape(100, 30),
            np.arange(50 * 70 * 3, dtype=np.uint8).reshape(50, 70, 3),
            np.arange(50 * 70 * 4, dtype=np.uint8).reshape(50, 70, 4),
        ],
    )
    def test_parse(self, image: np.ndarray) -> None:
        self._test_parse(image, PngParser())


class TestJpegParser(TestImageParser):
    @pytest.mark.parametrize(
        "image",
        [
            np.arange(50 * 70, dtype=np.uint8).reshape(50, 70),
            np.arange(100 * 30, dtype=np.uint8).reshape(100, 30),
            np.arange(50 * 70 * 3, dtype=np.uint8).reshape(50, 70, 3),
        ],
    )
    def test_parse(self, image: np.ndarray) -> None:
        self._test_parse(image, JpegParser(), lossy=True)


class TestTiffParser(TestImageParser):
    @pytest.mark.parametrize(
        "image",
        [
            np.arange(50 * 70, dtype=np.uint8).reshape(50, 70),
            np.arange(100 * 30, dtype=np.uint8).reshape(100, 30),
            np.arange(50 * 70, dtype=np.uint16).reshape(50, 70),
            np.arange(100 * 30, dtype=np.uint16).reshape(100, 30),
            np.arange(50 * 70 * 3, dtype=np.uint8).reshape(50, 70, 3),
            np.arange(50 * 70 * 4, dtype=np.uint8).reshape(50, 70, 4),
            np.arange(5 * 10 * 7 * 10 * 4, dtype=np.float64).reshape(5, 10, 7, 10, 4),
        ],
    )
    def test_parse(self, image: np.ndarray) -> None:
        self._test_parse(image, TiffParser())
