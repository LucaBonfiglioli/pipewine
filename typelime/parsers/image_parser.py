import io
from collections.abc import Iterable, Mapping
from typing import Any

import imageio.v3 as iio
import numpy as np
import tifffile

from typelime.parsers.base import Parser


class ImageParser(Parser[np.ndarray]):
    def parse(self, data: bytes) -> np.ndarray:
        return np.array(iio.imread(data, extension=next(iter(self.extensions()))))

    def dump(self, data: np.ndarray) -> bytes:
        ext = next(iter(self.extensions()))
        return iio.imwrite(
            "<bytes>",
            data,
            extension=ext,
            **self._save_options(),
        )

    def _save_options(self) -> Mapping[str, Any]:
        return {}


class BmpParser(ImageParser):
    @classmethod
    def extensions(cls) -> Iterable[str]:
        return [".bmp"]


class PngParser(ImageParser):
    @classmethod
    def extensions(cls) -> Iterable[str]:
        return [".png"]

    def _save_options(self) -> Mapping[str, Any]:
        return {"compress_level": 4}


class JpegParser(ImageParser):
    def _save_options(self) -> Mapping[str, Any]:
        return {"quality": 80}

    @classmethod
    def extensions(cls) -> Iterable[str]:
        return [".jpeg", ".jpg", ".jfif", ".jpe"]


class TiffParser(ImageParser):
    def _save_options(self) -> Mapping[str, Any]:
        return {"compression": "zlib"}

    @classmethod
    def extensions(cls) -> Iterable[str]:
        return [".tiff", ".tif"]

    def parse(self, data: bytes) -> np.ndarray:
        return np.array(tifffile.imread(io.BytesIO(data)))

    def dump(self, data: np.ndarray) -> bytes:
        buffer = io.BytesIO()
        tifffile.imwrite(buffer, data, **self._save_options())
        return buffer.read()
