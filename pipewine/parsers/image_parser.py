"""Parsers for image data."""

import io
from collections.abc import Iterable, Mapping
from typing import Any

import imageio.v3 as iio
import numpy as np
import tifffile

from pipewine.parsers.base import Parser


class ImageParser(Parser[np.ndarray]):
    """Base class for image data parsers that use the `imageio` library.

    Subclasses should only need to implement the `extensions` method to specify the
    file extensions that the parser can handle.

    Optionally, the `_save_options` method can be implemented to provide additional
    options to the `imwrite` function when dumping data, e.g., compression level for PNG
    files.
    """

    def parse(self, data: bytes) -> np.ndarray:
        return np.array(iio.imread(data, extension="." + next(iter(self.extensions()))))

    def dump(self, data: np.ndarray) -> bytes:
        ext = next(iter(self.extensions()))
        return iio.imwrite(
            "<bytes>",
            data,
            extension="." + ext,
            **self._save_options(),
        )

    def _save_options(self) -> Mapping[str, Any]:
        """Additional options to pass to the `imwrite` function when dumping data."""
        return {}


class BmpParser(ImageParser):
    """Parser for BMP image data."""

    @classmethod
    def extensions(cls) -> Iterable[str]:
        return ["bmp"]


class PngParser(ImageParser):
    """Parser for PNG image data."""

    @classmethod
    def extensions(cls) -> Iterable[str]:
        return ["png"]

    def _save_options(self) -> Mapping[str, Any]:
        return {"compress_level": 4}


class JpegParser(ImageParser):
    """Parser for JPEG image data."""

    def _save_options(self) -> Mapping[str, Any]:
        return {"quality": 80}

    @classmethod
    def extensions(cls) -> Iterable[str]:
        return ["jpeg", "jpg", "jfif", "jpe"]


class TiffParser(ImageParser):
    """Parser for TIFF image data."""

    def _save_options(self) -> Mapping[str, Any]:
        return {"compression": "zlib", "photometric": True}

    @classmethod
    def extensions(cls) -> Iterable[str]:
        return ["tiff", "tif"]

    def parse(self, data: bytes) -> np.ndarray:
        return np.array(tifffile.imread(io.BytesIO(data)))

    def dump(self, data: np.ndarray) -> bytes:
        buffer = io.BytesIO()
        tifffile.imwrite(buffer, data, **self._save_options())
        buffer.seek(0)
        return buffer.read()
