import numpy as np
import pytest
import math

from pipewine import NumpyNpyParser


class TestNumpyNpyParser:
    @pytest.mark.parametrize(
        "dtype",
        [
            np.uint8,
            np.uint16,
            np.uint32,
            np.uint64,
            np.int8,
            np.int16,
            np.int32,
            np.int64,
            np.float16,
            np.float32,
            np.float64,
            np.float128,
        ],
    )
    @pytest.mark.parametrize(
        "shape", [[], [1], [10], [10, 0], [10, 10], [10, 3, 4], [2, 3, 4, 5, 6]]
    )
    def test_parse(self, shape: list[int], dtype: np.dtype) -> None:
        array = np.arange(math.prod(shape), dtype=dtype).reshape(shape)
        parser = NumpyNpyParser()
        bytes_ = parser.dump(array)
        print(bytes_)
        assert isinstance(bytes_, bytes)
        re_array = parser.parse(bytes_)
        assert array.dtype == re_array.dtype
        assert array.shape == re_array.shape
        assert np.allclose(array, re_array)
